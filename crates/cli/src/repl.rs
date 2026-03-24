//! REPL loop with rustyline.
//!
//! Interactive mode: prompt, meta-commands, history, TAB completion.
//! Pipe mode: read lines from stdin, execute each.

use std::io::{self, BufRead};

use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context, Editor, Helper};

use strata_executor::{Command, Output};

use crate::commands::build_repl_cmd;
use crate::format::{
    format_diff, format_error, format_fork_info, format_merge_info, format_multi_output,
    format_multi_versioned_output, format_output, format_versioned_output, OutputMode,
};
use crate::parse::{
    check_meta_command, matches_to_action, BranchOp, CliAction, MetaCommand, Primitive,
};
use crate::state::SessionState;

/// Run the interactive REPL.
pub fn run_repl(state: &mut SessionState, mode: OutputMode, db_path: &str) {
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .build();

    let helper = StrataHelper::new();
    let mut rl: Editor<StrataHelper, _> = Editor::with_config(config).unwrap();
    rl.set_helper(Some(helper));

    // Load history
    let history_path = history_file();
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    print_welcome(db_path);

    loop {
        let prompt = state.prompt();
        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                // Check meta-commands first
                if let Some(meta) = check_meta_command(trimmed) {
                    match meta {
                        MetaCommand::Quit => break,
                        MetaCommand::Clear => {
                            // ANSI clear screen
                            print!("\x1B[2J\x1B[1;1H");
                        }
                        MetaCommand::Help { command } => {
                            print_help(command.as_deref());
                        }
                        MetaCommand::Use { branch, space } => match state.set_branch(&branch) {
                            Ok(()) => {
                                if let Some(s) = space {
                                    state.set_space(&s);
                                } else {
                                    state.set_space("default");
                                }
                            }
                            Err(e) => {
                                eprintln!("{}", format_error(&e, mode));
                            }
                        },
                    }
                    continue;
                }

                // Tokenize with shlex (respects quotes)
                let tokens = match shlex::split(trimmed) {
                    Some(t) => t,
                    None => {
                        eprintln!("(error) Invalid quoting");
                        continue;
                    }
                };

                if tokens.is_empty() {
                    continue;
                }

                // Parse via clap
                let cmd = build_repl_cmd();
                let matches = match cmd.try_get_matches_from(tokens) {
                    Ok(m) => m,
                    Err(e) => {
                        // clap error — show help text
                        eprintln!("{}", e);
                        continue;
                    }
                };

                execute_action(&matches, state, mode);
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C — just show new prompt
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D — exit
                break;
            }
            Err(err) => {
                eprintln!("(error) {:?}", err);
                break;
            }
        }
    }

    // Save history
    if let Some(ref path) = history_path {
        let _ = rl.save_history(path);
    }
}

/// Run in pipe mode: read lines from stdin, execute each.
pub fn run_pipe(state: &mut SessionState, mode: OutputMode) -> i32 {
    let stdin = io::stdin();
    let mut exit_code = 0;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let tokens = match shlex::split(trimmed) {
            Some(t) => t,
            None => {
                eprintln!("(error) Invalid quoting: {}", trimmed);
                exit_code = 1;
                continue;
            }
        };

        if tokens.is_empty() {
            continue;
        }

        let cmd = build_repl_cmd();
        let matches = match cmd.try_get_matches_from(tokens) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("{}", e);
                exit_code = 1;
                continue;
            }
        };

        if !execute_action(&matches, state, mode) {
            exit_code = 1;
        }
    }

    exit_code
}

/// Execute a parsed action. Returns true on success, false on error.
fn execute_action(matches: &clap::ArgMatches, state: &mut SessionState, mode: OutputMode) -> bool {
    match matches_to_action(matches, state) {
        Ok(CliAction::Execute(cmd)) => match state.execute(cmd) {
            Ok(output) => {
                let formatted = format_output(&output, mode);
                if !formatted.is_empty() {
                    println!("{}", formatted);
                }
                true
            }
            Err(e) => {
                eprintln!("{}", format_error(&e, mode));
                false
            }
        },
        Ok(CliAction::BranchOp(op)) => match op {
            BranchOp::Fork { destination } => match state.fork_branch(&destination) {
                Ok(info) => {
                    println!("{}", format_fork_info(&info, mode));
                    true
                }
                Err(e) => {
                    eprintln!("{}", format_error(&e, mode));
                    false
                }
            },
            BranchOp::Diff { branch_a, branch_b } => {
                match state.diff_branches(&branch_a, &branch_b) {
                    Ok(diff) => {
                        println!("{}", format_diff(&diff, mode));
                        true
                    }
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        false
                    }
                }
            }
            BranchOp::Merge { source, strategy } => match state.merge_branch(&source, strategy) {
                Ok(info) => {
                    println!("{}", format_merge_info(&info, mode));
                    true
                }
                Err(e) => {
                    eprintln!("{}", format_error(&e, mode));
                    false
                }
            },
        },
        Ok(CliAction::Meta(_)) => {
            // Meta-commands should have been handled before reaching here
            true
        }
        Ok(CliAction::MultiPut {
            branch,
            space,
            pairs,
        }) => {
            let mut outputs = Vec::new();
            for (key, value) in pairs {
                match state.execute(Command::KvPut {
                    branch: branch.clone(),
                    space: space.clone(),
                    key,
                    value,
                }) {
                    Ok(output) => outputs.push(output),
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        return false;
                    }
                }
            }
            let formatted = format_multi_output(&outputs, mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            true
        }
        Ok(CliAction::MultiGet {
            branch,
            space,
            keys,
            with_version,
        }) => {
            let mut outputs = Vec::new();
            for key in keys {
                match state.execute(Command::KvGet {
                    branch: branch.clone(),
                    space: space.clone(),
                    key,
                    as_of: None,
                }) {
                    Ok(output) => outputs.push(output),
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        return false;
                    }
                }
            }
            let formatted = format_multi_versioned_output(&outputs, mode, with_version);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            true
        }
        Ok(CliAction::MultiDel {
            branch,
            space,
            keys,
        }) => {
            let mut outputs = Vec::new();
            for key in keys {
                match state.execute(Command::KvDelete {
                    branch: branch.clone(),
                    space: space.clone(),
                    key,
                }) {
                    Ok(output) => outputs.push(output),
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        return false;
                    }
                }
            }
            let formatted = format_multi_output(&outputs, mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            true
        }
        Ok(CliAction::ListAll {
            branch,
            space,
            prefix,
            primitive,
        }) => {
            let mut all_keys = Vec::new();
            let mut cursor: Option<String> = None;

            loop {
                let output = match primitive {
                    Primitive::Kv => state.execute(Command::KvList {
                        branch: branch.clone(),
                        space: space.clone(),
                        prefix: prefix.clone(),
                        cursor: cursor.clone(),
                        limit: Some(1000),
                        as_of: None,
                    }),
                    Primitive::Json => state.execute(Command::JsonList {
                        branch: branch.clone(),
                        space: space.clone(),
                        prefix: prefix.clone(),
                        cursor: cursor.clone(),
                        limit: 1000,
                        as_of: None,
                    }),
                    Primitive::State => {
                        // State list doesn't have pagination, just execute once
                        match state.execute(Command::StateList {
                            branch: branch.clone(),
                            space: space.clone(),
                            prefix: prefix.clone(),
                            as_of: None,
                        }) {
                            Ok(output) => {
                                let formatted = format_output(&output, mode);
                                if !formatted.is_empty() {
                                    println!("{}", formatted);
                                }
                                return true;
                            }
                            Err(e) => {
                                eprintln!("{}", format_error(&e, mode));
                                return false;
                            }
                        }
                    }
                };

                match output {
                    Ok(Output::Keys(keys)) => {
                        all_keys.extend(keys);
                        break;
                    }
                    Ok(Output::JsonListResult {
                        keys, cursor: next, ..
                    }) => {
                        all_keys.extend(keys);
                        if next.is_none() {
                            break;
                        }
                        cursor = next;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        return false;
                    }
                }
            }

            let formatted = format_output(&Output::Keys(all_keys), mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            true
        }
        Ok(CliAction::GetWithVersion {
            command,
            with_version,
        }) => match state.execute(command) {
            Ok(output) => {
                let formatted = format_versioned_output(&output, mode, with_version);
                if !formatted.is_empty() {
                    println!("{}", formatted);
                }
                true
            }
            Err(e) => {
                eprintln!("{}", format_error(&e, mode));
                false
            }
        },
        Err(e) => {
            eprintln!("(error) {}", e);
            false
        }
    }
}

fn print_welcome(db_path: &str) {
    let version = env!("CARGO_PKG_VERSION");
    eprintln!("Strata v{version} — type 'help' for commands, 'quit' to exit.");
    eprintln!("Database: {db_path}");
    eprintln!();
    eprintln!("  Quick start:");
    eprintln!("    kv put greeting \"hello world\"    Store a value");
    eprintln!("    kv get greeting                   Retrieve it");
    eprintln!("    search \"hello\"                    Search across all data");
    eprintln!();
}

fn history_file() -> Option<String> {
    std::env::var("HOME")
        .ok()
        .map(|h| format!("{}/.strata_history", h))
}

fn print_help(command: Option<&str>) {
    if let Some(cmd) = command {
        // Show help for a specific command
        let cli = build_repl_cmd();
        match cli.try_get_matches_from(vec![cmd, "--help"]) {
            Ok(_) => {}
            Err(e) => println!("{}", e),
        }
    } else {
        println!("Available commands:");
        println!("  kv          Key-value operations (put, get, del, list, history)");
        println!("  json        JSON document operations (set, get, del, list, history)");
        println!("  event       Event log operations (append, get, list, len)");
        println!("  state       State cell operations (set, get, del, init, cas, list, history)");
        println!("  vector      Vector store operations (upsert, get, del, search, create, ...)");
        println!(
            "  graph       Graph operations (add-node, add-edge, neighbors, bfs, ontology, ...)"
        );
        println!("  branch      Branch operations (create, info, list, fork, diff, merge, ...)");
        println!("  space       Space operations (list, create, del, exists)");
        println!("  begin       Begin a transaction");
        println!("  commit      Commit a transaction");
        println!("  rollback    Rollback a transaction");
        println!("  txn         Transaction info (info, active)");
        println!("  ping        Ping the database");
        println!("  info        Database information");
        println!("  flush       Flush writes to disk");
        println!("  compact     Trigger compaction");
        println!("  search      Search across primitives");
        println!("  config      Configuration operations (set, get, list)");
        println!();
        println!("Meta-commands:");
        println!("  use <branch> [space]   Switch branch/space context");
        println!("  help [command]         Show help");
        println!("  quit / exit            Exit REPL");
        println!("  clear                  Clear screen");
    }
}

// =========================================================================
// TAB Completion
// =========================================================================

/// Known top-level commands for TAB completion.
const TOP_LEVEL_COMMANDS: &[&str] = &[
    "kv", "json", "event", "state", "vector", "graph", "branch", "space", "begin", "commit",
    "rollback", "txn", "ping", "info", "health", "init", "flush", "compact", "search", "config", "use", "help",
    "quit", "exit", "clear",
];

/// Known subcommands for each top-level command.
fn subcommands_for(cmd: &str) -> &'static [&'static str] {
    match cmd {
        "kv" => &["put", "get", "del", "list", "history"],
        "json" => &["set", "get", "del", "list", "history"],
        "event" => &["append", "get", "list", "len"],
        "state" => &["set", "get", "del", "init", "cas", "list", "history"],
        "vector" => &[
            "upsert",
            "get",
            "del",
            "search",
            "create",
            "drop",
            "del-collection",
            "collections",
            "stats",
            "batch-upsert",
        ],
        "graph" => &[
            "create",
            "delete",
            "list",
            "info",
            "add-node",
            "get-node",
            "remove-node",
            "list-nodes",
            "add-edge",
            "remove-edge",
            "neighbors",
            "bulk-insert",
            "bfs",
            "ontology",
            "analytics",
        ],
        "branch" => &[
            "create", "info", "get", "list", "exists", "del", "fork", "diff", "merge", "export",
            "import", "validate",
        ],
        "space" => &["list", "create", "del", "exists"],
        "txn" => &["info", "active"],
        "config" => &["set", "get", "list"],
        _ => &[],
    }
}

/// Known sub-subcommands for nested commands (3rd level).
fn sub_subcommands_for(cmd: &str, sub: &str) -> &'static [&'static str] {
    match (cmd, sub) {
        ("graph", "ontology") => &[
            "define", "get", "list", "delete", "freeze", "status", "summary",
        ],
        ("graph", "analytics") => &["wcc", "cdlp", "pagerank", "lcc", "sssp"],
        _ => &[],
    }
}

struct StrataHelper;

impl StrataHelper {
    fn new() -> Self {
        Self
    }
}

impl Helper for StrataHelper {}
impl Validator for StrataHelper {}
impl Highlighter for StrataHelper {}
impl Hinter for StrataHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Completer for StrataHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_to_pos = &line[..pos];
        let parts: Vec<&str> = line_to_pos.split_whitespace().collect();

        // Determine if we're completing a partial word or starting a new word
        let trailing_space = line_to_pos.ends_with(' ');

        if parts.is_empty() || (parts.len() == 1 && !trailing_space) {
            // Completing top-level command
            let prefix = parts.first().copied().unwrap_or("");
            let start = pos - prefix.len();
            let candidates: Vec<Pair> = TOP_LEVEL_COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|cmd| Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                })
                .collect();
            Ok((start, candidates))
        } else if parts.len() == 1 && trailing_space {
            // Just typed the top-level command, completing subcommand
            let subs = subcommands_for(parts[0]);
            let candidates: Vec<Pair> = subs
                .iter()
                .map(|s| Pair {
                    display: s.to_string(),
                    replacement: s.to_string(),
                })
                .collect();
            Ok((pos, candidates))
        } else if parts.len() == 2 && !trailing_space {
            // Completing partial subcommand
            let subs = subcommands_for(parts[0]);
            let prefix = parts[1];
            let start = pos - prefix.len();
            let candidates: Vec<Pair> = subs
                .iter()
                .filter(|s| s.starts_with(prefix))
                .map(|s| Pair {
                    display: s.to_string(),
                    replacement: s.to_string(),
                })
                .collect();
            Ok((start, candidates))
        } else if parts.len() == 2 && trailing_space {
            // Just typed "graph ontology ", completing sub-subcommand
            let sub_subs = sub_subcommands_for(parts[0], parts[1]);
            if !sub_subs.is_empty() {
                let candidates: Vec<Pair> = sub_subs
                    .iter()
                    .map(|s| Pair {
                        display: s.to_string(),
                        replacement: s.to_string(),
                    })
                    .collect();
                return Ok((pos, candidates));
            }
            Ok((pos, vec![]))
        } else if parts.len() == 3 && !trailing_space {
            // Completing partial sub-subcommand
            let sub_subs = sub_subcommands_for(parts[0], parts[1]);
            if !sub_subs.is_empty() {
                let prefix = parts[2];
                let start = pos - prefix.len();
                let candidates: Vec<Pair> = sub_subs
                    .iter()
                    .filter(|s| s.starts_with(prefix))
                    .map(|s| Pair {
                        display: s.to_string(),
                        replacement: s.to_string(),
                    })
                    .collect();
                return Ok((start, candidates));
            }
            Ok((pos, vec![]))
        } else {
            Ok((pos, vec![]))
        }
    }
}
