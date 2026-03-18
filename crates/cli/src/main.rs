//! Strata CLI — Redis-inspired CLI for the Strata database.
//!
//! Two modes:
//! - **Shell mode**: `strata [flags] COMMAND` — single command, exit
//! - **REPL mode**: `strata [flags]` — interactive prompt (if stdin is TTY)
//! - **Pipe mode**: `echo "kv put k v" | strata` — line-by-line from stdin

mod commands;
mod format;
mod parse;
mod repl;
mod state;
mod value;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process;
use std::{env, fs};

use strata_executor::{AccessMode, Command, OpenOptions, Output, Strata};

use commands::build_cli;
use format::{
    format_diff, format_error, format_fork_info, format_merge_info, format_multi_output,
    format_multi_versioned_output, format_output, format_versioned_output, OutputMode,
};
use parse::{matches_to_action, BranchOp, CliAction, Primitive};
use state::SessionState;

fn main() {
    let cli = build_cli();
    let matches = cli.get_matches();

    // Handle subcommands that don't need a database.
    if matches.subcommand_name() == Some("setup") {
        run_setup();
        return;
    }
    if let Some(("uninstall", sub)) = matches.subcommand() {
        run_uninstall(sub.get_flag("yes"));
        return;
    }

    // Determine output mode
    let output_mode = if matches.get_flag("json") {
        OutputMode::Json
    } else if matches.get_flag("raw") {
        OutputMode::Raw
    } else {
        OutputMode::Human
    };

    // Open database
    let db = match open_database(&matches) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    // Initial branch/space
    let initial_branch = matches
        .get_one::<String>("branch")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let initial_space = matches
        .get_one::<String>("space")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    let mut state = SessionState::new(db, initial_branch, initial_space);

    // Dispatch mode
    if matches.subcommand().is_some() {
        // Shell mode: parse, execute, format, exit
        let exit_code = run_shell_mode(&matches, &mut state, output_mode);
        process::exit(exit_code);
    } else if std::io::stdin().is_terminal() {
        // REPL mode
        repl::run_repl(&mut state, output_mode);
    } else {
        // Pipe mode
        let exit_code = repl::run_pipe(&mut state, output_mode);
        process::exit(exit_code);
    }
}

fn open_database(matches: &clap::ArgMatches) -> Result<Strata, String> {
    let read_only = matches.get_flag("read-only");
    let use_cache = matches.get_flag("cache");
    let follower = matches.get_flag("follower");

    if use_cache {
        Strata::cache().map_err(|e| format!("Failed to open cache database: {}", e))
    } else {
        let default_path = default_db_path();
        let path = matches
            .get_one::<String>("db")
            .map(|s| s.to_string())
            .unwrap_or(default_path);

        let mut opts = OpenOptions::new();

        if read_only || follower {
            opts = opts.access_mode(AccessMode::ReadOnly);
        }
        if follower {
            opts = opts.follower(true);
        }

        Strata::open_with(path, opts).map_err(|e| format!("Failed to open database: {}", e))
    }
}

fn run_shell_mode(matches: &clap::ArgMatches, state: &mut SessionState, mode: OutputMode) -> i32 {
    match matches_to_action(matches, state) {
        Ok(CliAction::Execute(cmd)) => match state.execute(cmd) {
            Ok(output) => {
                let formatted = format_output(&output, mode);
                if !formatted.is_empty() {
                    println!("{}", formatted);
                }
                0
            }
            Err(e) => {
                eprintln!("{}", format_error(&e, mode));
                1
            }
        },
        Ok(CliAction::BranchOp(op)) => match op {
            BranchOp::Fork { destination } => match state.fork_branch(&destination) {
                Ok(info) => {
                    println!("{}", format_fork_info(&info, mode));
                    0
                }
                Err(e) => {
                    eprintln!("{}", format_error(&e, mode));
                    1
                }
            },
            BranchOp::Diff { branch_a, branch_b } => {
                match state.diff_branches(&branch_a, &branch_b) {
                    Ok(diff) => {
                        println!("{}", format_diff(&diff, mode));
                        0
                    }
                    Err(e) => {
                        eprintln!("{}", format_error(&e, mode));
                        1
                    }
                }
            }
            BranchOp::Merge { source, strategy } => match state.merge_branch(&source, strategy) {
                Ok(info) => {
                    println!("{}", format_merge_info(&info, mode));
                    0
                }
                Err(e) => {
                    eprintln!("{}", format_error(&e, mode));
                    1
                }
            },
        },
        Ok(CliAction::Meta(_)) => {
            eprintln!("(error) Meta-commands are only available in REPL mode");
            1
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
                        return 1;
                    }
                }
            }
            let formatted = format_multi_output(&outputs, mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            0
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
                        return 1;
                    }
                }
            }
            let formatted = format_multi_versioned_output(&outputs, mode, with_version);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            0
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
                        return 1;
                    }
                }
            }
            let formatted = format_multi_output(&outputs, mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            0
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
                                return 0;
                            }
                            Err(e) => {
                                eprintln!("{}", format_error(&e, mode));
                                return 1;
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
                        return 1;
                    }
                }
            }

            let formatted = format_output(&Output::Keys(all_keys), mode);
            if !formatted.is_empty() {
                println!("{}", formatted);
            }
            0
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
                0
            }
            Err(e) => {
                eprintln!("{}", format_error(&e, mode));
                1
            }
        },
        Err(e) => {
            eprintln!("(error) {}", e);
            1
        }
    }
}

/// Default database path: `~/Documents/Strata`.
/// Falls back to `.strata` if `$HOME` is not set.
fn default_db_path() -> String {
    match env::var("HOME") {
        Ok(home) => {
            let path = PathBuf::from(home).join("Documents").join("Strata");
            path.to_string_lossy().to_string()
        }
        Err(_) => ".strata".to_string(),
    }
}

fn run_uninstall(skip_confirm: bool) {
    let home = match env::var("HOME") {
        Ok(h) => PathBuf::from(h),
        Err(_) => {
            eprintln!("Could not determine home directory.");
            process::exit(1);
        }
    };

    let strata_dir = home.join(".strata");
    let legacy_model_dir = home.join(".stratadb");
    let history_file = home.join(".strata_history");

    // Show what will be removed
    eprintln!("This will remove:");
    eprintln!();
    if strata_dir.exists() {
        eprintln!("  ~/.strata/          Binary, models, and configuration");
    }
    if legacy_model_dir.exists() {
        eprintln!("  ~/.stratadb/        Legacy model files");
    }
    if history_file.exists() {
        eprintln!("  ~/.strata_history   REPL history");
    }
    eprintln!("  PATH entries        From shell configuration files");
    eprintln!();
    eprintln!("Note: Per-project database directories (.strata/ in your projects)");
    eprintln!("will NOT be removed. Delete those manually if needed.");
    eprintln!();

    if !skip_confirm {
        eprint!("Continue? [y/N] ");
        io::stderr().flush().unwrap();
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer).is_err() || !answer.trim().eq_ignore_ascii_case("y") {
            eprintln!("Aborted.");
            return;
        }
    }

    // Remove ~/.strata/ (binary + models + config)
    if strata_dir.exists() {
        match fs::remove_dir_all(&strata_dir) {
            Ok(()) => eprintln!("  Removed ~/.strata/"),
            Err(e) => eprintln!("  Warning: could not remove ~/.strata/: {}", e),
        }
    }

    // Remove ~/.stratadb/ (legacy model dir)
    if legacy_model_dir.exists() {
        match fs::remove_dir_all(&legacy_model_dir) {
            Ok(()) => eprintln!("  Removed ~/.stratadb/"),
            Err(e) => eprintln!("  Warning: could not remove ~/.stratadb/: {}", e),
        }
    }

    // Remove ~/.strata_history
    if history_file.exists() {
        match fs::remove_file(&history_file) {
            Ok(()) => eprintln!("  Removed ~/.strata_history"),
            Err(e) => eprintln!("  Warning: could not remove ~/.strata_history: {}", e),
        }
    }

    // Clean PATH entries from shell configs
    let strata_path_marker = home.join(".strata/bin").to_string_lossy().to_string();
    let shell_configs = [
        home.join(".zshrc"),
        home.join(".bashrc"),
        home.join(".bash_profile"),
        home.join(".profile"),
        home.join(".config/fish/config.fish"),
    ];

    for config_path in &shell_configs {
        if !config_path.exists() {
            continue;
        }
        let contents = match fs::read_to_string(config_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if !contents.contains(&strata_path_marker) {
            continue;
        }

        // Remove the "# Strata" comment and the line following it
        let filtered: Vec<&str> = contents
            .lines()
            .collect::<Vec<_>>()
            .windows(1)
            .filter_map(|w| {
                let line = w[0];
                if line.trim() == "# Strata" || line.contains(&strata_path_marker) {
                    None
                } else {
                    Some(line)
                }
            })
            .collect();

        // Trim trailing blank lines that were left behind
        let mut result: Vec<&str> = filtered.into_iter().collect();
        while result.last() == Some(&"") {
            result.pop();
        }
        let mut new_contents = result.join("\n");
        new_contents.push('\n');

        match fs::write(config_path, &new_contents) {
            Ok(()) => eprintln!("  Cleaned PATH from {}", config_path.display()),
            Err(e) => eprintln!(
                "  Warning: could not update {}: {}",
                config_path.display(),
                e
            ),
        }
    }

    eprintln!();
    eprintln!("Strata has been uninstalled. Restart your shell to apply PATH changes.");
}

fn run_setup() {
    #[cfg(feature = "embed")]
    {
        eprintln!("Downloading MiniLM-L6-v2 embedding model...");
        match strata_intelligence::embed::download::ensure_model() {
            Ok(path) => {
                eprintln!("Model files ready at {}", path.display());
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                process::exit(1);
            }
        }
    }

    #[cfg(not(feature = "embed"))]
    {
        eprintln!("The 'embed' feature is not enabled. Rebuild with --features embed");
        process::exit(1);
    }
}
