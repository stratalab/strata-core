use std::ffi::OsString;
use std::io::IsTerminal;

use clap::ArgMatches;
use strata_executor::{Command, Session, Strata};

use crate::admin;
use crate::context::Context;
use crate::init;
use crate::open::{ensure_branch_exists, open_cli, OpenRequest};
use crate::parse;
use crate::render::{render_error, render_output, RenderMode};
use crate::repl;
use crate::request::{CliRequest, MetaCommand};

pub(crate) struct CliApp {
    db: Strata,
    session: Session,
    context: Context,
    render_mode: RenderMode,
}

impl CliApp {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        db: Strata,
        session: Session,
        context: Context,
        render_mode: RenderMode,
    ) -> Self {
        Self {
            db,
            session,
            context,
            render_mode,
        }
    }

    #[cfg(test)]
    pub(crate) fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }

    pub(crate) fn execute_command(&mut self, command: Command) -> Result<Option<String>, String> {
        let output = self
            .session
            .execute(command.clone())
            .map_err(|error| render_error(&error, self.render_mode))?;
        self.context.note_success(&command, &output);
        let rendered = render_output(&output, self.render_mode);
        if rendered.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rendered))
        }
    }

    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    pub(crate) fn execute_request(&mut self, request: CliRequest) -> Result<ReplOutcome, String> {
        match request {
            CliRequest::Execute(command) => self
                .execute_command(command)
                .map(|rendered| ReplOutcome::Continue { rendered }),
            CliRequest::Meta(meta) => self.execute_meta(meta),
        }
    }

    fn execute_meta(&mut self, meta: MetaCommand) -> Result<ReplOutcome, String> {
        match meta {
            MetaCommand::Quit => Ok(ReplOutcome::Exit),
            MetaCommand::Clear => Ok(ReplOutcome::Clear),
            MetaCommand::Help { command } => Ok(ReplOutcome::Continue {
                rendered: Some(parse::help_text(command.as_deref(), true)),
            }),
            MetaCommand::Use { branch, space } => {
                ensure_branch_exists(&mut self.session, &branch)?;
                self.context.set_branch(branch);
                if let Some(space) = space {
                    self.context.set_space(space);
                }
                Ok(ReplOutcome::Continue { rendered: None })
            }
        }
    }

    pub(crate) fn close(self) -> Result<(), String> {
        let Self {
            db,
            session,
            render_mode,
            ..
        } = self;
        drop(session);
        db.close()
            .map_err(|error| render_error(&error, render_mode))
    }
}

pub(crate) enum ReplOutcome {
    Continue { rendered: Option<String> },
    Clear,
    Exit,
}

pub(crate) fn run(args: impl IntoIterator<Item = OsString>) -> i32 {
    let cli = parse::build_cli();
    let matches = match cli.try_get_matches_from(args) {
        Ok(matches) => matches,
        Err(error) => {
            eprintln!("{error}");
            return 2;
        }
    };

    if let Some(code) = run_local_command(&matches) {
        return code;
    }

    let render_mode = if matches.get_flag("json") {
        RenderMode::Json
    } else if matches.get_flag("raw") {
        RenderMode::Raw
    } else {
        RenderMode::Human
    };

    let opened = match open_cli(OpenRequest::from_matches(&matches)) {
        Ok(opened) => opened,
        Err(error) => {
            eprintln!("{error}");
            return 1;
        }
    };

    let mut app = CliApp {
        db: opened.db,
        session: opened.session,
        context: opened.context,
        render_mode,
    };

    let code = if matches.subcommand().is_some() {
        run_shell(&matches, &mut app)
    } else if std::io::stdin().is_terminal() {
        repl::run_repl(&mut app)
    } else {
        repl::run_pipe(&mut app)
    };

    match app.close() {
        Ok(()) => code,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

fn run_local_command(matches: &clap::ArgMatches) -> Option<i32> {
    let (name, submatches) = matches.subcommand()?;
    match name {
        "init" => {
            if let Err(error) = reject_local_open_flags(
                matches,
                "init",
                LocalCommandPolicy {
                    allow_db: true,
                    allow_branch: false,
                    allow_space: false,
                    allow_render: false,
                    allow_read_only: false,
                    allow_follower: false,
                    allow_cache: false,
                },
            ) {
                eprintln!("{error}");
                return Some(2);
            }
            let non_interactive = submatches.get_flag("non-interactive");
            let default_path = matches
                .get_one::<String>("db")
                .map_or("~/Documents/Strata", String::as_str);
            Some(match init::run_init(default_path, non_interactive) {
                Ok(()) => 0,
                Err(error) => {
                    eprintln!("{error}");
                    1
                }
            })
        }
        "up" => {
            if let Err(error) = reject_local_open_flags(
                matches,
                "up",
                LocalCommandPolicy {
                    allow_db: true,
                    allow_branch: false,
                    allow_space: false,
                    allow_render: false,
                    allow_read_only: false,
                    allow_follower: false,
                    allow_cache: false,
                },
            ) {
                eprintln!("{error}");
                return Some(2);
            }
            let foreground = submatches.get_flag("foreground");
            let db_path = matches
                .get_one::<String>("db")
                .map_or(".strata", String::as_str);
            Some(admin::run_up(db_path, foreground))
        }
        "down" => {
            if let Err(error) = reject_local_open_flags(
                matches,
                "down",
                LocalCommandPolicy {
                    allow_db: true,
                    allow_branch: false,
                    allow_space: false,
                    allow_render: false,
                    allow_read_only: false,
                    allow_follower: false,
                    allow_cache: false,
                },
            ) {
                eprintln!("{error}");
                return Some(2);
            }
            let db_path = matches
                .get_one::<String>("db")
                .map_or(".strata", String::as_str);
            Some(admin::run_down(db_path))
        }
        "uninstall" => {
            if let Err(error) = reject_local_open_flags(
                matches,
                "uninstall",
                LocalCommandPolicy {
                    allow_db: false,
                    allow_branch: false,
                    allow_space: false,
                    allow_render: false,
                    allow_read_only: false,
                    allow_follower: false,
                    allow_cache: false,
                },
            ) {
                eprintln!("{error}");
                return Some(2);
            }
            Some(admin::run_uninstall(submatches.get_flag("yes")))
        }
        _ => None,
    }
}

struct LocalCommandPolicy {
    allow_db: bool,
    allow_branch: bool,
    allow_space: bool,
    allow_render: bool,
    allow_read_only: bool,
    allow_follower: bool,
    allow_cache: bool,
}

fn reject_local_open_flags(
    matches: &ArgMatches,
    command: &str,
    policy: LocalCommandPolicy,
) -> Result<(), String> {
    if !policy.allow_db && matches.get_one::<String>("db").is_some() {
        return Err(format!("--db is not supported with `{command}`"));
    }
    if !policy.allow_branch && matches.get_one::<String>("branch").is_some() {
        return Err(format!("--branch is not supported with `{command}`"));
    }
    if !policy.allow_space && matches.get_one::<String>("space").is_some() {
        return Err(format!("--space is not supported with `{command}`"));
    }
    if !policy.allow_render {
        if matches.get_flag("json") {
            return Err(format!("--json is not supported with `{command}`"));
        }
        if matches.get_flag("raw") {
            return Err(format!("--raw is not supported with `{command}`"));
        }
    }
    if !policy.allow_read_only && matches.get_flag("read-only") {
        return Err(format!("--read-only is not supported with `{command}`"));
    }
    if !policy.allow_follower && matches.get_flag("follower") {
        return Err(format!("--follower is not supported with `{command}`"));
    }
    if !policy.allow_cache && matches.get_flag("cache") {
        return Err(format!("--cache is not supported with `{command}`"));
    }
    Ok(())
}

fn run_shell(matches: &clap::ArgMatches, app: &mut CliApp) -> i32 {
    match parse::matches_to_request(matches, app.context()) {
        Ok(CliRequest::Execute(command)) => match app.execute_command(command) {
            Ok(Some(rendered)) => {
                println!("{rendered}");
                0
            }
            Ok(None) => 0,
            Err(error) => {
                eprintln!("{error}");
                1
            }
        },
        Ok(CliRequest::Meta(_)) => unreachable!("shell mode cannot produce REPL meta commands"),
        Err(error) => {
            eprintln!("{error}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use strata_executor::Strata;
    use tempfile::tempdir;

    use super::run;
    use crate::context::Context;
    use crate::open::{open_cli, OpenRequest};
    use crate::render::RenderMode;

    fn run_args(args: &[&str]) -> i32 {
        run(args.iter().map(OsString::from))
    }

    #[test]
    fn shell_success_returns_zero() {
        assert_eq!(run_args(&["strata", "--cache", "ping"]), 0);
    }

    #[test]
    fn shell_parse_failure_returns_two() {
        assert_eq!(run_args(&["strata", "--cache", "not-a-command"]), 2);
    }

    #[test]
    fn shell_open_failure_returns_one() {
        assert_eq!(
            run_args(&["strata", "--cache", "--branch", "missing", "ping"]),
            1
        );
    }

    #[test]
    fn shell_executor_failure_returns_one() {
        assert_eq!(
            run_args(&["strata", "--cache", "--read-only", "kv", "put", "k", "1"]),
            1
        );
    }

    #[test]
    fn init_rejects_open_mode_flags() {
        assert_eq!(run_args(&["strata", "init", "--cache"]), 2);
        assert_eq!(run_args(&["strata", "init", "--read-only"]), 2);
    }

    #[test]
    fn up_rejects_non_db_global_flags() {
        assert_eq!(run_args(&["strata", "up", "--cache"]), 2);
        assert_eq!(run_args(&["strata", "up", "--follower"]), 2);
    }

    #[test]
    fn uninstall_rejects_database_open_flags() {
        assert_eq!(run_args(&["strata", "uninstall", "--db", "./db"]), 2);
        assert_eq!(run_args(&["strata", "uninstall", "--json"]), 2);
    }

    #[test]
    fn close_shuts_down_disk_backed_app_cleanly() {
        let temp = tempdir().expect("tempdir should succeed");
        let opened = open_cli(OpenRequest::new_for_test(
            Some(temp.path().to_path_buf()),
            false,
            false,
            false,
            None,
            None,
        ))
        .expect("open should succeed");

        let app = super::CliApp {
            db: opened.db,
            session: opened.session,
            context: opened.context,
            render_mode: RenderMode::Human,
        };

        app.close().expect("close should succeed");
        Strata::open(temp.path())
            .and_then(Strata::close)
            .expect("database should reopen after close");
    }

    #[test]
    fn new_for_test_close_shuts_down_cache_handle() {
        let db = Strata::cache().expect("cache open should succeed");
        let default_branch = db.current_branch().to_string();
        let session = db.session();
        let context = Context::new(
            default_branch.clone(),
            default_branch,
            "default".to_string(),
        );
        let app = super::CliApp::new_for_test(db, session, context, RenderMode::Human);
        app.close().expect("close should succeed");
    }

    #[test]
    fn run_closes_disk_backed_database_before_returning() {
        let temp = tempdir().expect("tempdir should succeed");
        let db_arg = temp.path().to_string_lossy().to_string();

        assert_eq!(run_args(&["strata", "--db", &db_arg, "ping"]), 0);
        Strata::open(temp.path())
            .and_then(Strata::close)
            .expect("database should reopen after run returns");
    }
}
