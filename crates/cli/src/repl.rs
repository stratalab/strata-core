//! Interactive and piped command execution.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use clap::CommandFactory;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::json;
use strata_executor::ipc::Connection;
use strata_executor::{Command, Output};

use crate::context::CommandContext;
use crate::line::{self, SessionLine};
use crate::options::{Cli, Format};
use crate::render::{render_error, render_value};
use crate::{execute_parsed_command, CliError};

pub(crate) fn run_repl(
    connection: &Connection,
    context: &mut CommandContext,
    format: Format,
) -> Result<(), CliError> {
    let mut editor = DefaultEditor::new().map_err(|error| CliError::usage(error.to_string()))?;
    let history = history_path();
    if let Some(path) = history.as_ref() {
        let _ = editor.load_history(path);
    }
    print_banner(connection, format);

    // Ctrl+C follows shell convention (#2998): the first press prints the
    // escape hint, a second consecutive press exits; Ctrl+D always exits.
    let mut interrupted = false;
    loop {
        match editor.readline(&context.prompt(&connection.default_branch())) {
            Ok(line) => {
                interrupted = false;
                let _ = editor.add_history_entry(line.as_str());
                // A failed line reports and keeps the session (#2998): a typo
                // must never terminate an interactive REPL.
                match handle_line(connection, context, &line, format) {
                    LineOutcome::Exit => break,
                    LineOutcome::Continue | LineOutcome::Failed => {}
                }
            }
            Err(ReadlineError::Interrupted) => {
                if interrupted {
                    break;
                }
                interrupted = true;
                eprintln!("(press Ctrl+C again to exit — or type `exit`; Ctrl+D also quits)");
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(CliError::usage(error.to_string())),
        }
    }

    if let Some(path) = history.as_ref() {
        let _ = editor.save_history(path);
    }
    Ok(())
}

/// A short orientation when an interactive human session opens (#2998): what
/// this database holds and what to try. Scripted formats stay chrome-free, and
/// a failed describe never blocks the session.
fn print_banner(connection: &Connection, format: Format) {
    for line in banner_for(connection, format).unwrap_or_default() {
        println!("{line}");
    }
}

/// The banner lines for an interactive session, or `None` when the session is
/// scripted (non-human format) or describe fails — the decision logic, kept
/// out of the stdio glue so it is unit-testable.
fn banner_for(connection: &Connection, format: Format) -> Option<Vec<String>> {
    if format != Format::Human {
        return None;
    }
    let Ok(Output::Described(describe)) = connection.execute(Command::Describe { branch: None })
    else {
        return None;
    };
    let value = serde_json::to_value(&describe).ok()?;
    Some(banner_lines(&value))
}

/// The banner content, as pure data -> lines (unit-tested).
fn banner_lines(describe: &serde_json::Value) -> Vec<String> {
    use serde_json::Value;
    let count = |value: &Value, field: &str| value.get(field).and_then(Value::as_u64).unwrap_or(0);
    let list_len = |field: &str| {
        describe
            .get(field)
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };

    let version = describe
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("?");
    let target = describe
        .get("target")
        .and_then(Value::as_str)
        .unwrap_or("?");
    let branch = describe
        .get("branch")
        .and_then(Value::as_str)
        .unwrap_or("default");

    let empty = serde_json::json!({});
    let primitives = describe.get("primitives").unwrap_or(&empty);
    let kv = count(primitives, "kv_count");
    let json = count(primitives, "json_count");
    let events = count(primitives, "event_count");
    let collections = primitives
        .get("vector_collections")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let graphs = primitives
        .get("graphs")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);

    let mut lines = vec![format!("StrataDB {version} · {target} · branch {branch}")];
    if kv == 0 && json == 0 && events == 0 && collections == 0 && graphs == 0 {
        lines.push("empty database — write something to create it as you go".to_owned());
        lines.push(
            "Try: kv put greeting hello · json set doc '{\"a\": 1}' · help    exit (or Ctrl+D) to quit"
                .to_owned(),
        );
    } else {
        lines.push(format!(
            "{} branches · {} spaces · {kv} kv · {json} json · {events} events · {collections} vector collections · {graphs} graphs",
            list_len("branches"),
            list_len("spaces"),
        ));
        lines.push(
            "Try: describe · kv list · branch list · help    exit (or Ctrl+D) to quit".to_owned(),
        );
    }
    lines
}

/// Report a failed line without killing the session. Executor errors keep the
/// structured envelope; parse errors are printed once (clap's rendered message
/// already leads with `error:`, so it is not re-prefixed — the `error: error:`
/// doubling this replaces).
fn report_line_error(error: &CliError, format: Format) {
    match error {
        CliError::Executor(executor_error) => render_error(executor_error.status(), format),
        other => eprintln!("{}", prefixed_error_line(&other.to_string())),
    }
}

fn prefixed_error_line(message: &str) -> String {
    if message.trim_start().starts_with("error:") {
        message.to_owned()
    } else {
        format!("error: {message}")
    }
}

/// Runs a piped session: one line per command until stdin closes. Returns
/// whether any line failed, for the exit code.
pub(crate) fn run_pipe(
    connection: &Connection,
    context: &mut CommandContext,
    format: Format,
) -> Result<bool, CliError> {
    let mut saw_error = false;
    for line in io::stdin().lock().lines() {
        let line = line?;
        match handle_line(connection, context, &line, format) {
            LineOutcome::Continue | LineOutcome::Exit => {}
            LineOutcome::Failed => saw_error = true,
        }
    }
    Ok(saw_error)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineOutcome {
    Continue,
    Exit,
    /// The line was refused or failed and has been reported; the session
    /// goes on, and a piped session exits non-zero at its end.
    Failed,
}

/// Runs one line and reports its failure, if any. The line's own output
/// format — `--json`, `--raw`, `--output-format` — wins over the session's
/// for its answer and for its error alike (#3326); a line that does not
/// parse has no format of its own and reports in the session's. Both loops,
/// interactive and piped, come through here, so a failed line is reported
/// once, the same way (#3328).
fn handle_line(
    connection: &Connection,
    context: &mut CommandContext,
    line: &str,
    session_format: Format,
) -> LineOutcome {
    let parsed = match parse_line(line) {
        Ok(parsed) => parsed,
        Err(error) => return failed_line(&error, session_format),
    };
    let format = match &parsed {
        ReplLine::Command(command) => command.format.unwrap_or(session_format),
        _ => session_format,
    };
    match run_parsed_line(connection, context, parsed, format) {
        Ok(outcome) => outcome,
        Err(error) => failed_line(&error, format),
    }
}

fn failed_line(error: &CliError, format: Format) -> LineOutcome {
    report_line_error(error, format);
    LineOutcome::Failed
}

fn run_parsed_line(
    connection: &Connection,
    context: &mut CommandContext,
    parsed: ReplLine,
    format: Format,
) -> Result<LineOutcome, CliError> {
    match parsed {
        ReplLine::Empty => Ok(LineOutcome::Continue),
        ReplLine::Exit => Ok(LineOutcome::Exit),
        ReplLine::Clear => {
            print!("\x1b[2J\x1b[H");
            let _ = io::stdout().flush();
            Ok(LineOutcome::Continue)
        }
        ReplLine::Help => {
            Cli::command().print_long_help().map_err(CliError::from)?;
            println!();
            Ok(LineOutcome::Continue)
        }
        ReplLine::Use { branch, space } => {
            validate_context(connection, &branch, space.as_deref())?;
            context.set_branch(branch.clone());
            context.set_space(space.clone());
            render_value(
                &json!({
                    "type": "context",
                    "data": {
                        "branch": branch,
                        "space": space.unwrap_or_else(|| context.space_or_default().to_owned())
                    }
                }),
                format,
            )?;
            Ok(LineOutcome::Continue)
        }
        ReplLine::Command(line) => {
            let scope = context.scope_with_overrides(line.branch, line.space);
            execute_parsed_command(
                connection,
                line.command,
                &scope,
                format,
                crate::render::Channel::Transcript,
            )?;
            Ok(LineOutcome::Continue)
        }
    }
}

/// The REPL's own verbs (`quit`/`exit`, `clear`, a lone `help`, `use`) are
/// read here; every other line is a command line, read by the grammar the
/// playground shares (`SessionLine`).
fn parse_line(line: &str) -> Result<ReplLine, CliError> {
    let Some(words) = line::words(line)? else {
        return Ok(ReplLine::Empty);
    };
    match words[0].as_str() {
        "quit" | "exit" => return Ok(ReplLine::Exit),
        "clear" => return Ok(ReplLine::Clear),
        "help" if words.len() == 1 => return Ok(ReplLine::Help),
        "use" => return parse_use(&words),
        _ => {}
    }
    Ok(ReplLine::Command(Box::new(SessionLine::parse(words)?)))
}

fn parse_use(words: &[String]) -> Result<ReplLine, CliError> {
    match words {
        [_, branch_space] => {
            if let Some((branch, space)) = branch_space.split_once('/') {
                if branch.is_empty() || space.is_empty() {
                    return Err(CliError::usage("usage: use <branch>/<space>"));
                }
                Ok(ReplLine::Use {
                    branch: branch.to_owned(),
                    space: Some(space.to_owned()),
                })
            } else {
                Ok(ReplLine::Use {
                    branch: branch_space.clone(),
                    space: None,
                })
            }
        }
        [_, branch, space] => Ok(ReplLine::Use {
            branch: branch.clone(),
            space: Some(space.clone()),
        }),
        _ => Err(CliError::usage("usage: use <branch> [space]")),
    }
}

fn validate_context(
    connection: &Connection,
    branch: &str,
    space: Option<&str>,
) -> Result<(), CliError> {
    let _ = connection.execute(Command::BranchGet {
        branch: branch.to_owned(),
    })?;
    if let Some(space) = space {
        let output = connection.execute(Command::SpaceExists {
            branch: Some(branch.to_owned()),
            space: space.to_owned(),
        })?;
        match output {
            Output::Bool(true) => {}
            Output::Bool(false) => {
                return Err(CliError::usage(format!(
                    "space `{space}` does not exist on branch `{branch}`"
                )));
            }
            _ => {
                return Err(CliError::usage(
                    "space existence check returned an unexpected output",
                ))
            }
        }
    }
    Ok(())
}

fn history_path() -> Option<PathBuf> {
    std::env::var_os("STRATA_HISTORY")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".strata_history"))
        })
}

/// One REPL line: a verb of the REPL's own, or a command line.
enum ReplLine {
    Empty,
    Exit,
    Clear,
    Help,
    Use {
        branch: String,
        space: Option<String>,
    },
    // Boxed: a `TopCommand` is large next to the verbs.
    Command(Box<SessionLine>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_summarizes_a_populated_database() {
        let describe = serde_json::json!({
            "version": "1.1.0", "target": "durable_local", "branch": "default",
            "branches": ["default", "risky"], "spaces": ["default"],
            "primitives": {"kv_count": 264, "json_count": 783, "event_count": 0,
                "vector_collections": [{"name": "embeddings"}], "graphs": []}
        });
        let lines = banner_lines(&describe);
        assert_eq!(lines[0], "StrataDB 1.1.0 · durable_local · branch default");
        assert_eq!(
            lines[1],
            "2 branches · 1 spaces · 264 kv · 783 json · 0 events · 1 vector collections · 0 graphs"
        );
        assert!(lines[2].starts_with("Try: describe"), "got {}", lines[2]);
    }

    #[test]
    fn banner_guides_an_empty_database_toward_a_first_write() {
        let describe = serde_json::json!({
            "version": "1.1.0", "target": "cache", "branch": "default",
            "branches": ["default"], "spaces": ["default"],
            "primitives": {"kv_count": 0, "json_count": 0, "event_count": 0,
                "vector_collections": [], "graphs": []}
        });
        let lines = banner_lines(&describe);
        assert_eq!(
            lines[1],
            "empty database — write something to create it as you go"
        );
        assert!(
            lines[2].contains("kv put greeting hello"),
            "the empty-state suggestion must be a first write: {}",
            lines[2]
        );
    }

    #[test]
    fn banner_is_for_human_sessions_only_and_reflects_the_store() {
        // Kills the format-guard mutants in `banner_for`: Human gets real
        // lines, scripted formats get nothing.
        let connection =
            Connection::cache(strata_executor::Executor::open_cache().expect("cache opens"));
        let lines = banner_for(&connection, Format::Human).expect("human sessions get a banner");
        assert!(lines[0].starts_with("StrataDB "), "{}", lines[0]);
        assert_eq!(
            banner_for(&connection, Format::Json),
            None,
            "scripted formats stay chrome-free"
        );
    }

    #[test]
    fn a_single_nonzero_inventory_is_not_an_empty_database() {
        // Kills the `&&`→`||` mutants in the empty-state condition: each shape
        // has exactly ONE non-zero inventory, and every one must take the
        // stats branch, never the empty-database greeting.
        for primitives in [
            serde_json::json!({"kv_count": 5, "json_count": 0, "event_count": 0,
                "vector_collections": [], "graphs": []}),
            serde_json::json!({"kv_count": 0, "json_count": 5, "event_count": 0,
                "vector_collections": [], "graphs": []}),
            serde_json::json!({"kv_count": 0, "json_count": 0, "event_count": 5,
                "vector_collections": [], "graphs": []}),
            serde_json::json!({"kv_count": 0, "json_count": 0, "event_count": 0,
                "vector_collections": [{"name": "e"}], "graphs": []}),
            serde_json::json!({"kv_count": 0, "json_count": 0, "event_count": 0,
                "vector_collections": [], "graphs": ["g"]}),
        ] {
            let describe = serde_json::json!({
                "version": "1", "target": "cache", "branch": "default",
                "branches": ["default"], "spaces": ["default"],
                "primitives": primitives
            });
            let lines = banner_lines(&describe);
            assert!(
                lines[1].contains("branches ·"),
                "one non-zero inventory must show stats, got: {}",
                lines[1]
            );
        }
    }

    #[test]
    fn clap_errors_are_not_double_prefixed() {
        // clap's rendered message already leads with `error:` — printing it
        // verbatim replaces the historical `error: error:` doubling.
        assert_eq!(
            prefixed_error_line("error: unrecognized subcommand 'putt'"),
            "error: unrecognized subcommand 'putt'"
        );
        assert_eq!(
            prefixed_error_line("space `x` does not exist"),
            "error: space `x` does not exist"
        );
    }

    #[test]
    fn parses_use_branch_and_space() {
        let use_line = |line: &str| match parse_line(line).expect(line) {
            ReplLine::Use { branch, space } => (branch, space),
            _ => panic!("{line}: expected a use verb"),
        };
        assert_eq!(
            use_line("use main docs"),
            ("main".to_owned(), Some("docs".to_owned()))
        );
        assert_eq!(
            use_line("use main/docs"),
            ("main".to_owned(), Some("docs".to_owned()))
        );
        assert_eq!(use_line("use main"), ("main".to_owned(), None));
        for bad in ["use", "use main/", "use /docs", "use a b c"] {
            assert!(
                matches!(parse_line(bad), Err(CliError::Usage(_))),
                "{bad}: a malformed use is usage, not a command"
            );
        }
    }

    #[test]
    fn parses_repl_command() {
        let ReplLine::Command(line) = parse_line("kv put a b").expect("parse") else {
            panic!("expected executor command");
        };
        assert!(matches!(line.command, crate::options::TopCommand::Kv(_)));
        assert_eq!(line.format, None, "no flag is no choice");
    }

    #[test]
    fn repl_verbs_are_the_repl_s_and_a_flag_alone_is_refused() {
        // The REPL's verbs never reach the shared grammar (`exit` would be
        // "not a strata command" there), and a line of flags with no command
        // is an error, not a silent no-op.
        assert!(matches!(parse_line("exit").expect("parse"), ReplLine::Exit));
        assert!(matches!(parse_line("quit").expect("parse"), ReplLine::Exit));
        assert!(matches!(
            parse_line("clear").expect("parse"),
            ReplLine::Clear
        ));
        assert!(matches!(parse_line("help").expect("parse"), ReplLine::Help));
        // `help <command>` is clap's own help subcommand, answered as usage
        // text — only a lone `help` is the REPL's verb.
        assert!(matches!(parse_line("help kv"), Err(CliError::Usage(_))));
        assert!(matches!(
            parse_line("  # comment").expect("parse"),
            ReplLine::Empty
        ));
        assert!(parse_line("--json").is_err());
    }

    #[test]
    fn validate_context_accepts_an_existing_branch_and_rejects_a_missing_space() {
        let connection =
            Connection::cache(strata_executor::Executor::open_cache().expect("cache opens"));
        // The store default branch exists, with no space override.
        validate_context(&connection, strata_executor::DEFAULT_BRANCH, None)
            .expect("the default branch is a valid `use` target");
        // A space that was never created on that branch is refused.
        validate_context(
            &connection,
            strata_executor::DEFAULT_BRANCH,
            Some("never-created"),
        )
        .expect_err("a missing space must be rejected");
    }
}
