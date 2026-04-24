use std::io::{self, BufRead};

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::app::{CliApp, ReplOutcome};
use crate::parse;

fn process_line(app: &mut CliApp, trimmed: &str) -> Result<ReplOutcome, String> {
    let request = parse::parse_repl_line(trimmed, app.context())?;
    app.execute_request(request)
}

fn print_outcome(outcome: ReplOutcome) {
    match outcome {
        ReplOutcome::Continue { rendered } => {
            if let Some(rendered) = rendered {
                if !rendered.is_empty() {
                    println!("{rendered}");
                }
            }
        }
        ReplOutcome::Clear => {
            print!("\x1B[2J\x1B[1;1H");
        }
        ReplOutcome::Exit => {}
    }
}

/// Run the interactive CLI.
pub(crate) fn run_repl(app: &mut CliApp) -> i32 {
    let mut editor = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(error) => {
            eprintln!("Failed to initialize line editor: {error}");
            return 1;
        }
    };

    if let Some(history_path) = history_path() {
        let _ = editor.load_history(&history_path);
    }

    loop {
        match editor.readline(&app.context().prompt()) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(trimmed);

                match process_line(app, trimmed) {
                    Ok(ReplOutcome::Exit) => break,
                    Ok(outcome) => print_outcome(outcome),
                    Err(error) => eprintln!("{error}"),
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(error) => {
                eprintln!("Readline error: {error}");
                return 1;
            }
        }
    }

    if let Some(history_path) = history_path() {
        let _ = editor.save_history(&history_path);
    }

    0
}

/// Run in stdin-driven pipe mode.
pub(crate) fn run_pipe(app: &mut CliApp) -> i32 {
    let stdin = io::stdin();
    run_pipe_reader(app, stdin.lock())
}

fn run_pipe_reader<R: BufRead>(app: &mut CliApp, reader: R) -> i32 {
    let mut exit_code = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                eprintln!("Failed to read stdin: {error}");
                return 1;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        match process_line(app, trimmed) {
            Ok(ReplOutcome::Exit) => break,
            Ok(outcome) => print_outcome(outcome),
            Err(error) => {
                eprintln!("{error}");
                exit_code = 1;
            }
        }
    }

    exit_code
}

fn history_path() -> Option<String> {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .map(|path| path.join(".strata_history"))
        .map(|path| path.to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use strata_executor::{Command, Output, Session, Strata};

    use super::{process_line, run_pipe_reader};
    use crate::app::{CliApp, ReplOutcome};
    use crate::context::Context;
    use crate::render::RenderMode;

    fn cache_app() -> CliApp {
        let db = Strata::cache().expect("cache db should open");
        let default_branch = db.current_branch().to_string();
        let session = db.session();
        let context = Context::new(
            default_branch.clone(),
            default_branch,
            "default".to_string(),
        );
        CliApp::new_for_test(db, session, context, RenderMode::Human)
    }

    fn kv_get(session: &mut Session, branch: &str, space: &str) -> Output {
        session
            .execute(Command::KvGet {
                branch: Some(branch.into()),
                space: Some(space.to_string()),
                key: "k".to_string(),
                as_of: None,
            })
            .expect("kv get should succeed")
    }

    #[test]
    fn process_line_handles_help_and_quit_meta_commands() {
        let mut app = cache_app();

        match process_line(&mut app, "help kv").expect("help should parse") {
            ReplOutcome::Continue { rendered } => {
                let rendered = rendered.expect("help should render");
                assert!(rendered.contains("kv"));
            }
            _ => panic!("expected continue outcome"),
        }

        assert!(matches!(
            process_line(&mut app, "quit").expect("quit should parse"),
            ReplOutcome::Exit
        ));
    }

    #[test]
    fn process_line_use_updates_context_only_after_validation() {
        let mut app = cache_app();

        app.execute_command(Command::BranchCreate {
            branch_id: Some("feature".into()),
            metadata: None,
        })
        .expect("branch create should succeed");

        assert!(matches!(
            process_line(&mut app, "use feature analytics").expect("use should succeed"),
            ReplOutcome::Continue { .. }
        ));
        assert_eq!(app.context().branch(), "feature");
        assert_eq!(app.context().space(), "analytics");

        let error = match process_line(&mut app, "use missing") {
            Ok(_) => panic!("missing branch should fail"),
            Err(error) => error,
        };
        assert!(error.contains("Branch not found"));
        assert_eq!(app.context().branch(), "feature");
    }

    #[test]
    fn process_line_use_preserves_current_space_when_omitted() {
        let mut app = cache_app();

        app.execute_command(Command::SpaceCreate {
            branch: Some(app.context().branch().into()),
            space: "analytics".to_string(),
        })
        .expect("space create should succeed");
        app.execute_command(Command::BranchCreate {
            branch_id: Some("feature".into()),
            metadata: None,
        })
        .expect("branch create should succeed");

        assert!(matches!(
            process_line(&mut app, "use default analytics").expect("explicit use should succeed"),
            ReplOutcome::Continue { .. }
        ));
        assert_eq!(app.context().space(), "analytics");

        assert!(matches!(
            process_line(&mut app, "use feature").expect("implicit-space use should succeed"),
            ReplOutcome::Continue { .. }
        ));
        assert_eq!(app.context().branch(), "feature");
        assert_eq!(app.context().space(), "analytics");
    }

    #[test]
    fn run_pipe_reader_skips_blank_and_comment_lines() {
        let mut app = cache_app();
        let input = Cursor::new("# comment\n\nping\n");

        assert_eq!(run_pipe_reader(&mut app, input), 0);
    }

    #[test]
    fn run_pipe_reader_accumulates_failures_but_continues() {
        let mut app = cache_app();
        let input = Cursor::new("use missing\nkv put k 1\n");

        assert_eq!(run_pipe_reader(&mut app, input), 1);
        let branch = app.context().branch().to_string();
        let space = app.context().space().to_string();
        assert!(matches!(
            kv_get(app.session_mut(), &branch, &space),
            Output::MaybeVersioned(Some(_))
        ));
    }

    #[test]
    fn run_pipe_reader_stops_after_quit() {
        let mut app = cache_app();
        let input = Cursor::new("quit\nuse missing\n");

        assert_eq!(run_pipe_reader(&mut app, input), 0);
        assert_eq!(app.context().branch(), "default");
    }

    #[test]
    fn run_pipe_reader_reports_failure_before_quit() {
        let mut app = cache_app();
        let input = Cursor::new("use missing\nquit\nkv put k 1\n");

        assert_eq!(run_pipe_reader(&mut app, input), 1);
        assert_eq!(app.context().branch(), "default");
    }
}
