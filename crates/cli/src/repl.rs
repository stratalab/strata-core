//! Interactive and piped command execution.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::json;
use strata_executor::ipc::Connection;
use strata_executor::{Command, Output};

use crate::context::CommandContext;
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
                    Ok(LineOutcome::Exit) => break,
                    Ok(LineOutcome::Continue) => {}
                    Err(error) => report_line_error(&error, format),
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

pub(crate) fn run_pipe(
    connection: &Connection,
    context: &mut CommandContext,
    format: Format,
) -> Result<bool, CliError> {
    let mut saw_error = false;
    for line in io::stdin().lock().lines() {
        let line = line?;
        match handle_line(connection, context, &line, format) {
            Ok(LineOutcome::Continue | LineOutcome::Exit) => {}
            Err(error) => {
                saw_error = true;
                eprintln!("error: {error}");
            }
        }
    }
    Ok(saw_error)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineOutcome {
    Continue,
    Exit,
}

fn handle_line(
    connection: &Connection,
    context: &mut CommandContext,
    line: &str,
    format: Format,
) -> Result<LineOutcome, CliError> {
    match parse_line(line)? {
        ParsedLine::Empty => Ok(LineOutcome::Continue),
        ParsedLine::Exit => Ok(LineOutcome::Exit),
        ParsedLine::Clear => {
            print!("\x1b[2J\x1b[H");
            let _ = io::stdout().flush();
            Ok(LineOutcome::Continue)
        }
        ParsedLine::Help => {
            Cli::command().print_long_help().map_err(CliError::from)?;
            println!();
            Ok(LineOutcome::Continue)
        }
        ParsedLine::Use { branch, space } => {
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
        ParsedLine::Command(cli) => {
            let scope = context.scope_with_overrides(cli.branch, cli.space);
            let Some(command) = cli.command else {
                return Ok(LineOutcome::Continue);
            };
            execute_parsed_command(connection, command, &scope, format)?;
            Ok(LineOutcome::Continue)
        }
    }
}

fn parse_line(line: &str) -> Result<ParsedLine, CliError> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(ParsedLine::Empty);
    }

    let words = shlex::split(trimmed)
        .ok_or_else(|| CliError::usage("could not parse command line quoting"))?;
    if words.is_empty() {
        return Ok(ParsedLine::Empty);
    }

    match words[0].as_str() {
        "quit" | "exit" => return Ok(ParsedLine::Exit),
        "clear" => return Ok(ParsedLine::Clear),
        "help" if words.len() == 1 => return Ok(ParsedLine::Help),
        "use" => return parse_use(&words),
        _ => {}
    }

    let mut argv = Vec::with_capacity(words.len() + 1);
    argv.push("strata".to_owned());
    argv.extend(words);
    Cli::try_parse_from(argv)
        .map(|cli| ParsedLine::Command(Box::new(cli)))
        .map_err(|error| CliError::usage(error.to_string()))
}

fn parse_use(words: &[String]) -> Result<ParsedLine, CliError> {
    match words {
        [_, branch_space] => {
            if let Some((branch, space)) = branch_space.split_once('/') {
                if branch.is_empty() || space.is_empty() {
                    return Err(CliError::usage("usage: use <branch>/<space>"));
                }
                Ok(ParsedLine::Use {
                    branch: branch.to_owned(),
                    space: Some(space.to_owned()),
                })
            } else {
                Ok(ParsedLine::Use {
                    branch: branch_space.clone(),
                    space: None,
                })
            }
        }
        [_, branch, space] => Ok(ParsedLine::Use {
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

enum ParsedLine {
    Empty,
    Exit,
    Clear,
    Help,
    Use {
        branch: String,
        space: Option<String>,
    },
    Command(Box<Cli>),
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
        let ParsedLine::Use { branch, space } = parse_line("use main docs").expect("parse") else {
            panic!("expected use command");
        };
        assert_eq!(branch, "main");
        assert_eq!(space.as_deref(), Some("docs"));
    }

    #[test]
    fn parses_repl_command() {
        let ParsedLine::Command(cli) = parse_line("kv put a b").expect("parse") else {
            panic!("expected executor command");
        };
        assert!(cli.command.is_some());
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
