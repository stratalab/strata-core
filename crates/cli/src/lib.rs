//! Handwritten CLI layer over `strata-executor`.

#![deny(unsafe_code)]
#![allow(clippy::missing_const_for_fn)]
// The parser-only build (no `native`, e.g. the wasm playground) reuses the
// command grammar + renderer but not the host machinery, so some argument
// helpers and flag types go unexercised there. That is expected, not a defect.
#![cfg_attr(not(feature = "native"), allow(dead_code))]

// The parser-only build reads a line through `line::SessionLine` (#3326);
// `Cli` itself is parsed here only by the binary's entry point and the tests.
#[cfg(any(feature = "native", test))]
use clap::Parser;
use strata_executor::{Command, Executor, ExecutorError, GraphPropertyDef};

#[cfg(feature = "native")]
use serde_json::Value;
#[cfg(feature = "native")]
use std::ffi::OsString;
#[cfg(feature = "native")]
use std::io::IsTerminal;
#[cfg(feature = "native")]
use std::path::PathBuf;
#[cfg(feature = "native")]
use strata_executor::ipc::{Connection, SessionAccess};
#[cfg(feature = "native")]
use strata_executor::IpcMode;

#[cfg(feature = "native")]
mod agents;
#[cfg(test)]
mod arg_spec;
mod catalog;
#[cfg(test)]
mod catalog_guard;
mod changelog;
#[cfg(test)]
mod command_examples;
mod context;
#[cfg(feature = "native")]
mod doctor;
#[cfg(feature = "native")]
mod guidance;
#[cfg(feature = "native")]
mod init;
mod input;
mod line;
#[cfg(feature = "native")]
mod mcp;
#[cfg(feature = "native")]
mod open;
mod options;
mod render;
#[cfg(feature = "native")]
mod repl;
mod report;
mod table;
#[cfg(feature = "native")]
mod uninstall;
#[cfg(feature = "native")]
mod update;
mod wall_clock;

#[cfg(feature = "native")]
use context::CommandContext;
use context::Scope;
use input::{
    bytes_argument, cursor_argument, parse_filter_argument, parse_json_argument,
    parse_optional_filter_argument, parse_optional_json_argument, parse_relaxed_json_argument,
    parse_vector_argument,
};
#[cfg(any(feature = "native", test))]
use options::Cli;
use options::{
    ArrowCommand, BranchCommand, CloneProgressFormat, CommandCommand, ConfigCommand, EventCommand,
    GraphCommand, GraphOntologyCommand, HubCommand, JsonCommand, KvCommand, SpaceCommand,
    VectorCollectionCommand, VectorCommand,
};
#[cfg(feature = "native")]
use render::{print_output, render_error, render_value};

// The wasm-safe CLI surface, re-exported for embedded consumers (the browser
// playground): render an Output or error to the CLI's own display string with
// no host I/O. `command_from_line` (below) turns a CLI line into a Command
// plus the output format its flags chose; `run_line` runs one against an
// embedded executor and returns what the binary would have printed.
pub use options::Format;
pub use render::{error_to_string, render_output, value_to_string, Invocation, Rendered};

/// Runs the CLI and returns a process exit code.
#[cfg(feature = "native")]
pub fn run<I, T>(args: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    // Capture the executor's boundary error logs (reference_id + code + source
    // chain) to stderr so the reference id shown in an error message correlates
    // to a real, inspectable line (ERR-2). stdout stays clean for command
    // output. Ignored if a subscriber is already installed (e.g. in tests).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::ERROR)
        .try_init();
    match Cli::try_parse_from(args) {
        Ok(cli) => {
            let format = cli.output_format();
            match execute(cli) {
                Ok(exit_code) => exit_code,
                Err(CliError::Executor(error)) => {
                    render_error(error.status(), format);
                    1
                }
                Err(error) => {
                    eprintln!("error: {error}");
                    2
                }
            }
        }
        Err(error) => {
            // clap routes --help/--version to stdout with exit 0 and genuine
            // parse errors to stderr with exit 2; error.print() honors that
            // split. Install scripts verify with `strata --version`, so the
            // success path must be a success.
            let exit = if error.use_stderr() { 2 } else { 0 };
            // A failure to print has nowhere left to report.
            let _ = error.print();
            exit
        }
    }
}

/// The MCP server owns the process: the caller opens the target (explicit
/// path / `STRATA_DB` / `--cache`; refusal otherwise, like any one-shot
/// command); this applies the session scope and serves stdio until the
/// client closes stdin.
#[cfg(feature = "native")]
fn serve_mcp(connection: Connection, context: &CommandContext) -> Result<i32, CliError> {
    let scope = context.scope_with_overrides(None, None);
    if let Some(branch) = scope.branch.as_deref() {
        connection.set_default_branch(branch);
    }
    if let Some(space) = scope.space.as_deref() {
        connection.set_default_space(space.to_owned());
    }
    // Force the session scope to apply before we touch stdio: a reserved or
    // malformed branch/space must fail loudly here (the connection validates
    // the scope when it runs a command), not silently mid-stream.
    connection.execute(Command::Ping {})?;
    let exit = mcp::serve(&connection)?;
    connection.close()?;
    Ok(exit)
}

#[cfg(feature = "native")]
#[allow(clippy::too_many_lines)]
fn execute(cli: Cli) -> Result<i32, CliError> {
    let format = cli.output_format();
    let durability = cli.durability.map(options::DurabilityArg::mode);
    let ipc = cli.ipc.map(options::IpcArg::mode);
    let access = if cli.read_only {
        SessionAccess::Read
    } else {
        SessionAccess::ReadWrite
    };
    let command = cli.command;
    let mut context = CommandContext::new(cli.branch, cli.space);

    if let Some(command) = command {
        if let Some(name) = deferred_top_command(&command) {
            return Err(deferred_command(name));
        }
        if matches!(command, options::TopCommand::Doctor) {
            // Doctor takes an *optional* database target, so it resolves the
            // target itself instead of going through open_executor's refusal.
            let (report, healthy) = doctor::run_doctor(cli.cache, cli.db, cli.db_path)?;
            render_value(&report, format)?;
            return Ok(i32::from(!healthy));
        }
        if let options::TopCommand::Uninstall(ref args) = command {
            // A host command: it removes the installation, so a database
            // target is a usage error, never something to open (#2995).
            if cli.db.is_some() || cli.db_path.is_some() || cli.cache {
                return Err(CliError::usage(
                    "`uninstall` removes the Strata installation; it does not take a database target",
                ));
            }
            let value = uninstall::run_uninstall(args.yes)?;
            render_value(&value, format)?;
            return Ok(0);
        }
        #[cfg(feature = "inference")]
        if let options::TopCommand::Inference(ref args) = command {
            // A host command like `update`: it replaces the binary and takes no
            // database target, so it must not reach the connection below.
            if matches!(args.command, options::InferenceCommand::InstallLocal) {
                if update::rejects_db_target(cli.db.is_some(), cli.db_path.is_some(), cli.cache) {
                    return Err(CliError::usage(
                        "`inference install-local` changes the Strata binary; \
                         it does not take a database target",
                    ));
                }
                let value = update::run_install_local()?;
                render_value(&value, format)?;
                return Ok(0);
            }
        }
        if let options::TopCommand::Update(ref args) = command {
            // A host command: it replaces the binary, not a database target.
            if update::rejects_db_target(cli.db.is_some(), cli.db_path.is_some(), cli.cache) {
                return Err(CliError::usage(
                    "`update` updates the Strata binary; it does not take a database target",
                ));
            }
            let value = update::run_update(args.check, args.version.clone())?;
            let update_available = value
                .get("data")
                .and_then(|data| data.get("update_available"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            render_value(&value, format)?;
            // `--check` signals via exit code so scripts can gate on it
            // (`if ! strata update --check; then …`); a real update exits 0.
            return Ok(update::check_exit_code(args.check, update_available));
        }
        let command = match command {
            options::TopCommand::Agents(args) => return agents::run(&args.command, format),
            // #3094: answers from the binary itself, so it needs no database
            // and no open — the point is that a binary of unknown provenance
            // can say what is in it.
            options::TopCommand::Changelog { version } => {
                return changelog::run(version.as_deref())
            }
            other => other,
        };
        if matches!(
            command,
            options::TopCommand::Mcp(options::McpArgs {
                command: options::McpCommand::Serve,
            })
        ) {
            let opened = open::open_connection(
                cli.cache,
                cli.db,
                cli.db_path,
                durability,
                // A long-lived owner other processes attach to.
                ipc.unwrap_or(IpcMode::Host),
                access,
                open::OpenIntent::OneShot,
            )?;
            return serve_mcp(opened.connection, &context);
        }
        if matches!(command, options::TopCommand::Start) {
            return run_ipc_start(
                cli.cache,
                cli.db,
                cli.db_path,
                durability,
                ipc,
                access,
                format,
            );
        }
        if matches!(command, options::TopCommand::Stop) {
            return run_ipc_stop(cli.cache, cli.db, cli.db_path, cli.read_only, format);
        }
        if let TopLevelAction::NoDatabase(value) = top_level_without_database(&command)? {
            render_value(&value, format)?;
            return Ok(0);
        }

        if let options::TopCommand::Clone(args) = command {
            return run_clone(args, format);
        }
        if let options::TopCommand::Hub(args) = command {
            return run_hub(args, format);
        }

        let opened = open::open_connection(
            cli.cache,
            cli.db,
            cli.db_path,
            durability,
            // A one-shot holds the lock briefly: broker to an owner, don't host.
            ipc.unwrap_or(IpcMode::Client),
            access,
            one_shot_intent(&command),
        )?;
        let connection = opened.connection;
        if let Some(branch) = context.scope_with_overrides(None, None).branch.as_deref() {
            connection.set_default_branch(branch);
        }

        let scope = context.scope_with_overrides(None, None);
        execute_parsed_command(
            &connection,
            command,
            &scope,
            format,
            render::Channel::OneShot,
        )?;
        connection.close()?;
        return Ok(0);
    }

    let interactive = std::io::stdin().is_terminal();
    let intent = if interactive {
        open::OpenIntent::Interactive
    } else {
        open::OpenIntent::Pipe
    };
    let session_ipc = ipc.unwrap_or(default_session_ipc(interactive));
    let opened = open::open_connection(
        cli.cache,
        cli.db,
        cli.db_path,
        durability,
        session_ipc,
        access,
        intent,
    )?;
    if opened.implicit_cache {
        // Bare interactive invocation: an ephemeral session, stated plainly
        // so nobody discovers volatility after typing data in (first-run D2).
        eprintln!(
            "strata {} — in-memory session (nothing persisted; run with a path to keep data)",
            env!("CARGO_PKG_VERSION")
        );
        // Standing in a directory full of datasets and getting a scratch
        // session is the #3000 trap — name what is actually here.
        if let Ok(cwd) = std::env::current_dir() {
            if let Some(notice) = contained_datasets_notice(&open::strata_databases_in(&cwd)) {
                eprintln!("{notice}");
            }
        }
        eprintln!("type `help` for commands  |  agents: run `strata agents guide`");
        eprintln!("skills for coding agents: npx skills add stratalab/strata-agent-skills");
    }
    if let Some(path) = opened.implicit_cwd.as_deref() {
        // The git model (#3000): a bare interactive open inside a dataset
        // opened THAT dataset — say so before the first prompt.
        eprintln!(
            "opened the Strata database in the current directory ({})",
            path.display()
        );
    }
    let connection = opened.connection;
    if let Some(branch) = context.scope_with_overrides(None, None).branch.as_deref() {
        connection.set_default_branch(branch);
    }

    let saw_pipe_error = if interactive {
        repl::run_repl(&connection, &mut context, format)?;
        false
    } else {
        repl::run_pipe(&connection, &mut context, format)?
    };
    connection.close()?;
    Ok(i32::from(saw_pipe_error))
}

/// The default multi-process mode for a session open when `--ipc` is unset: an
/// interactive REPL is a long-lived owner others attach to (Host); a piped
/// stream is a one-shot-like client that brokers to an owner but does not host.
#[cfg(feature = "native")]
const fn default_session_ipc(interactive: bool) -> IpcMode {
    if interactive {
        IpcMode::Host
    } else {
        IpcMode::Client
    }
}

/// Clone creates a NEW database from a hub dataset; it never touches a session
/// database, so it runs on its own ephemeral cache executor.
#[cfg(feature = "native")]
fn run_clone(args: options::CloneArgs, format: options::Format) -> Result<i32, CliError> {
    let mut executor = Executor::open_cache()?;
    let dataset = args.dataset;
    let dest = args
        .dest
        .unwrap_or_else(|| PathBuf::from(format!("{dataset}.strata")));
    let dest = dest.display().to_string();
    let (invocation, output) = match args.progress {
        Some(CloneProgressFormat::Jsonl) => {
            if format != options::Format::Json {
                return Err(CliError::usage("`--progress jsonl` requires `--json`"));
            }
            let mut progress_error = None;
            let mut on_progress = |event| {
                if progress_error.is_none() {
                    progress_error = print_output(
                        &event,
                        &Invocation::none(),
                        options::Format::Json,
                        render::Channel::OneShot,
                    )
                    .err();
                }
            };
            let output = executor.execute_hub_clone_with_progress(
                &dataset,
                args.branch.as_deref(),
                &dest,
                args.hub,
                &mut on_progress,
            )?;
            if let Some(error) = progress_error {
                return Err(error);
            }
            // `--progress jsonl` requires `--json`, which reads no declaration.
            (Invocation::none(), output)
        }
        None => {
            let command = Command::HubClone {
                dataset,
                branch: args.branch,
                dest,
                hub_url: args.hub,
            };
            let invocation = Invocation::of(&command, format)?;
            (invocation, executor.execute(command)?)
        }
    };
    print_output(&output, &invocation, format, render::Channel::OneShot)?;
    executor.close()?;
    Ok(0)
}

/// Hub browse commands never touch a session database; they use the executor
/// command boundary so every frontend shares the same resolver and envelopes.
#[cfg(feature = "native")]
fn run_hub(args: options::HubArgs, format: options::Format) -> Result<i32, CliError> {
    let mut executor = Executor::open_cache()?;
    let command = match args.command {
        HubCommand::Info { hub } => Command::HubInfo { hub_url: hub },
        HubCommand::ListDatasets(args) => Command::HubListDatasets {
            hub_url: args.hub,
            tasks: args.tasks,
            tags: args.tags,
            primitives: args.primitives,
            license: args.license,
            size_min_bytes: args.size_min_bytes,
            size_max_bytes: args.size_max_bytes,
            sort: args.sort.map(Into::into),
            limit: args.limit,
            offset: args.offset,
        },
        HubCommand::GetDataset { name, hub } => Command::HubGetDataset { name, hub_url: hub },
        HubCommand::ListRefs { dataset, hub } => Command::HubListRefs {
            dataset,
            hub_url: hub,
        },
        HubCommand::ListYanked { since, hub } => Command::HubListYanked {
            since,
            hub_url: hub,
        },
    };
    let invocation = Invocation::of(&command, format)?;
    let output = executor.execute(command)?;
    print_output(&output, &invocation, format, render::Channel::OneShot)?;
    executor.close()?;
    Ok(0)
}

/// `strata start <db>` — open a durable database as a broker owner and block,
/// keeping the socket alive so other processes can attach, until the hosting is
/// stopped (`strata stop`, or `ipc stop`). A foreground "keep an owner alive"
/// wrapper — never a server: it hosts Strata's multi-process substrate, the
/// moral equivalent of `SQLite`'s file lock, and exits cleanly on stop.
#[cfg(feature = "native")]
fn run_ipc_start(
    cache: bool,
    db_flag: Option<PathBuf>,
    db_path: Option<PathBuf>,
    durability: Option<strata_executor::DurabilityMode>,
    ipc: Option<IpcMode>,
    access: SessionAccess,
    format: options::Format,
) -> Result<i32, CliError> {
    if cache {
        return Err(CliError::usage(
            "`strata start` hosts a durable database; `--cache` is single-process and hosts nothing",
        ));
    }
    // start exists to host; an explicit `--ipc client|off` contradicts that.
    if matches!(ipc, Some(IpcMode::Client | IpcMode::Off)) {
        return Err(CliError::usage(
            "`strata start` hosts the broker socket; it is incompatible with `--ipc client|off`",
        ));
    }
    let opened = open::open_connection(
        false,
        db_flag,
        db_path,
        durability,
        IpcMode::Host,
        access,
        open::OpenIntent::OneShot,
    )?;
    let connection = opened.connection;
    if !connection.is_hosting() {
        // Host mode either brokered to an existing owner (a Remote handle) or
        // won the lock but could not bind the socket — either way this process
        // is not the host it was asked to be, so there is nothing to keep alive.
        let reason = if connection.is_local() {
            "could not bind the broker socket for this database"
        } else {
            "another process already owns this database (run `strata ipc status` to inspect it)"
        };
        connection.close()?;
        return Err(CliError::usage(reason));
    }
    render_value(&started_report(&connection), format)?;
    // The process now blocks, so the readiness report must reach stdout before
    // the wait — a human banner rendered with `print!` would otherwise sit in
    // the buffer until exit, and a supervising script waits on this line.
    std::io::Write::flush(&mut std::io::stdout())?;
    block_until_unhosted(&connection);
    connection.close()?;
    Ok(0)
}

/// `strata stop <db>` — tell a durable database's broker owner to stop hosting.
/// Brokers to the owner as a client and forwards `ipc_stop`: a `strata start`
/// owner then exits, while an interactive owner (a REPL, `mcp serve`) simply
/// stops brokering and keeps running. Forgiving — a database with no owner
/// reports `stopped: false` rather than failing, and never creates a database.
#[cfg(feature = "native")]
fn run_ipc_stop(
    cache: bool,
    db_flag: Option<PathBuf>,
    db_path: Option<PathBuf>,
    read_only: bool,
    format: options::Format,
) -> Result<i32, CliError> {
    if cache {
        return Err(CliError::usage(
            "`strata stop` stops a durable database's broker owner; `--cache` has none",
        ));
    }
    if read_only {
        // Stopping the owner's hosting is write-classified; a read-only stop
        // is a contradiction, refused rather than silently ignored.
        return Err(CliError::usage(
            "`strata stop` sends a write-classified command; it is incompatible with `--read-only`",
        ));
    }
    let path = resolve_durable_target(db_flag, db_path)?;
    if !path.exists() {
        // Nothing to stop. Report it plainly without opening (and thereby
        // creating) a database at a path that does not exist.
        render_value(
            &serde_json::json!({
                "type": "ipc_stop",
                "data": { "stopped": false, "database": path.display().to_string() },
            }),
            format,
        )?;
        return Ok(0);
    }
    let connection = Connection::open_durable_local_brokered(
        &path,
        strata_executor::DurableLocalOpenOptions::new(),
        IpcMode::Client,
        SessionAccess::ReadWrite,
    )?;
    let command = Command::IpcStop {};
    let invocation = Invocation::of(&command, format)?;
    let output = connection.execute(command)?;
    print_output(&output, &invocation, format, render::Channel::OneShot)?;
    connection.close()?;
    Ok(0)
}

/// Blocks while a `strata start` owner is actively hosting, polling the live
/// hosting state so an `ipc_stop` (in-process or brokered from `strata stop`)
/// ends the wait promptly. A timing-only loop — carved out of the mutation gate.
#[cfg(feature = "native")]
fn block_until_unhosted(connection: &Connection) {
    const POLL: std::time::Duration = std::time::Duration::from_millis(200);
    while connection.hosting_active() {
        std::thread::sleep(POLL);
    }
}

/// The readiness report `strata start` prints once it is hosting: the socket,
/// owner pid, and client count, drawn from the same `ipc_status` a client would
/// see. Doubles as the signal a supervising script waits for before attaching.
#[cfg(feature = "native")]
fn started_report(connection: &Connection) -> Value {
    let data = connection
        .execute(Command::IpcStatus {})
        .ok()
        .and_then(|output| serde_json::to_value(&output).ok())
        .and_then(|mut value| value.get_mut("data").map(Value::take))
        .unwrap_or(Value::Null);
    serde_json::json!({ "type": "ipc_started", "data": data })
}

/// Resolves the durable database target for `start`/`stop` from the explicit
/// flag/positional path, then `STRATA_DB`. These commands act on a specific
/// database, so an unspecified target is a usage error — never an implicit cwd.
#[cfg(feature = "native")]
fn resolve_durable_target(
    db_flag: Option<PathBuf>,
    db_path: Option<PathBuf>,
) -> Result<PathBuf, CliError> {
    match (db_flag, db_path) {
        (Some(_), Some(_)) => Err(CliError::usage(
            "provide either `--db <path>` or positional database path, not both",
        )),
        (Some(path), None) | (None, Some(path)) => Ok(path),
        (None, None) => open::env_database_path().ok_or_else(|| {
            CliError::usage(
                "`strata start`/`stop` require a database path (a positional path, `--db`, or STRATA_DB)",
            )
        }),
    }
}

/// The one dispatch for a parsed command, one-shot or mid-session. `scope` is
/// the branch/space this command runs under (the session's, or the command's
/// own overrides).
#[cfg(feature = "native")]
// A flat top-level command dispatch, like its family-level siblings.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_parsed_command(
    connection: &Connection,
    command: options::TopCommand,
    scope: &Scope,
    format: options::Format,
    channel: render::Channel,
) -> Result<(), CliError> {
    if let Some(name) = deferred_top_command(&command) {
        return Err(deferred_command(name));
    }
    // The executor owns branch and space session context (CLI-4). Resolve the
    // current scope onto the connection so every path — including `command run`,
    // which executes raw JSON without per-command scope injection — honors
    // --branch/--space and the REPL `use` context uniformly.
    let connection_branch = connection.default_branch();
    let branch = scope.branch.clone().unwrap_or(connection_branch);
    connection.set_default_branch(branch);
    let space = scope
        .space
        .clone()
        .unwrap_or_else(|| strata_executor::DEFAULT_SPACE.to_owned());
    connection.set_default_space(space);
    // Every command's declaration is read before it runs, so the render never
    // touches the executor again; the two closures differ only in whether a
    // model-availability refusal gets the download offer.
    let run = |command: Command| -> Result<(Invocation, strata_executor::Output), CliError> {
        let invocation = Invocation::of(&command, format)?;
        Ok((invocation, connection.execute(command)?))
    };
    let run_offering_download =
        |command: Command| -> Result<(Invocation, strata_executor::Output), CliError> {
            let invocation = Invocation::of(&command, format)?;
            #[cfg(feature = "inference")]
            let output = execute_with_download_offer(connection, command, format)?;
            #[cfg(not(feature = "inference"))]
            let output = connection.execute(command)?;
            Ok((invocation, output))
        };
    let (invocation, output) = match command {
        options::TopCommand::Ping => run(Command::Ping {})?,
        options::TopCommand::Remote => run(Command::RemoteGet {})?,
        options::TopCommand::Clone(_) | options::TopCommand::Hub(_) => {
            unreachable!("host-only hub commands are dispatched before a session database opens")
        }
        options::TopCommand::Init => {
            let value = init::run_init()?;
            render_value(&value, format)?;
            return Ok(());
        }
        options::TopCommand::Uninstall(_) | options::TopCommand::Update(_) => {
            // Host commands (change the installation, not a database); the
            // one-shot path owns them, so this in-session arm is defensive.
            return Err(CliError::usage(
                "this is a host command; run it outside of a database session",
            ));
        }
        options::TopCommand::Doctor => {
            // Inside a session the database is already open and evidently
            // working, so report installation checks only; the process exit
            // code is unaffected — the session stays alive either way.
            let (report, _healthy) = doctor::run_doctor(false, None, None)?;
            render_value(&report, format)?;
            return Ok(());
        }
        options::TopCommand::Agents(args) => {
            // Exit code is only meaningful for the one-shot path; agents
            // commands never fail healthily inside a session.
            let _exit = agents::run(&args.command, format)?;
            return Ok(());
        }
        options::TopCommand::Changelog { version } => {
            let _exit = changelog::run(version.as_deref())?;
            return Ok(());
        }
        options::TopCommand::Mcp(_) => {
            return Err(CliError::usage(
                "`mcp serve` runs as a one-shot command (it owns stdio), not inside a session",
            ));
        }
        options::TopCommand::Info => run(Command::Info {
            branch: scope.branch.clone(),
        })?,
        options::TopCommand::Health => run(Command::Health {
            branch: scope.branch.clone(),
        })?,
        options::TopCommand::Metrics => run(Command::Metrics {
            branch: scope.branch.clone(),
        })?,
        options::TopCommand::Describe => run(Command::Describe {
            branch: scope.branch.clone(),
        })?,
        options::TopCommand::Config(args) => run(config_command(args.command))?,
        options::TopCommand::Ipc(args) => match args.command {
            options::IpcSubcommand::Status => run(Command::IpcStatus {})?,
            options::IpcSubcommand::Stop => run(Command::IpcStop {})?,
        },
        options::TopCommand::Branch(args) => run(branch_command(args.command)?)?,
        options::TopCommand::Space(args) => run(space_command(args.command, scope))?,
        options::TopCommand::Kv(args) => run(kv_command(args.command, scope)?)?,
        options::TopCommand::Json(args) => run(json_command(args.command, scope)?)?,
        // `--text` loads the collection's recorded model, so it meets the
        // same refusal `inference embed` meets and gets the same offer.
        options::TopCommand::Vector(command) => {
            run_offering_download(vector_command(command.command, scope)?)?
        }
        options::TopCommand::Event(args) => run(event_command(args.command, scope)?)?,
        options::TopCommand::Graph(args) => run(graph_command(args.command, scope)?)?,
        options::TopCommand::Arrow(args) => run(arrow_command(args.command, scope))?,
        #[cfg(feature = "inference")]
        options::TopCommand::Inference(args) => {
            run_offering_download(inference_command(args.command)?)?
        }
        options::TopCommand::Command(args) => run(raw_command(args.command)?)?,
        options::TopCommand::Search(_)
        | options::TopCommand::Recipe(_)
        | options::TopCommand::Txn(_)
        | options::TopCommand::Begin
        | options::TopCommand::Commit
        | options::TopCommand::Rollback
        | options::TopCommand::Flush
        | options::TopCommand::Compact
        | options::TopCommand::Up(_)
        | options::TopCommand::Down(_) => unreachable!("deferred top commands handled above"),
        options::TopCommand::Start | options::TopCommand::Stop => {
            unreachable!("start/stop own their open and are handled before a session opens")
        }
    };

    print_output(&output, &invocation, format, channel)?;
    Ok(())
}

/// The implicit-cache notice's dataset line (#3000): `None` when the
/// directory holds no datasets — the caller must print nothing (the mutant
/// that dropped the emptiness guard would have indexed an empty list).
#[cfg(feature = "native")]
fn contained_datasets_notice(found: &[String]) -> Option<String> {
    let first = found.first()?;
    Some(format!(
        "Strata datasets in this directory: {} — open one with `strata ./{first}`",
        found.join(", ")
    ))
}

fn deferred_top_command(command: &options::TopCommand) -> Option<&'static str> {
    match command {
        options::TopCommand::Search(_) => Some("search"),
        options::TopCommand::Recipe(_) => Some("recipe"),
        options::TopCommand::Txn(_) => Some("txn"),
        options::TopCommand::Begin => Some("begin"),
        options::TopCommand::Commit => Some("commit"),
        options::TopCommand::Rollback => Some("rollback"),
        options::TopCommand::Flush => Some("flush"),
        options::TopCommand::Compact => Some("compact"),
        options::TopCommand::Up(_) => Some("up"),
        options::TopCommand::Down(_) => Some("down"),
        _ => None,
    }
}

fn deferred_command(name: &str) -> CliError {
    CliError::usage(format!(
        "`{name}` is recognized from the old CLI, but is not available in the V1 CLI surface yet"
    ))
}

#[cfg(feature = "native")]
enum TopLevelAction {
    NeedsDatabase,
    NoDatabase(Value),
}

#[cfg(feature = "native")]
fn top_level_without_database(command: &options::TopCommand) -> Result<TopLevelAction, CliError> {
    match command {
        options::TopCommand::Init => Ok(TopLevelAction::NoDatabase(init::run_init()?)),
        options::TopCommand::Config(args) => match &args.command {
            ConfigCommand::Set { key, value } => {
                Ok(TopLevelAction::NoDatabase(user_config_set(key, value)?))
            }
            ConfigCommand::Unset { key } => Ok(TopLevelAction::NoDatabase(user_config_unset(key)?)),
            ConfigCommand::Path => Ok(TopLevelAction::NoDatabase(user_config_path()?)),
            ConfigCommand::Show => Ok(TopLevelAction::NoDatabase(user_config_show())),
            ConfigCommand::GetKey { key } if is_user_config_key(key) => {
                Ok(TopLevelAction::NoDatabase(user_config_get(key)?))
            }
            _ => Ok(TopLevelAction::NeedsDatabase),
        },
        options::TopCommand::Command(args) => match &args.command {
            CommandCommand::Print { json, file } => {
                let command = raw_command_from_sources(json.as_deref(), file.as_ref())?;
                let value = serde_json::to_value(command)?;
                Ok(TopLevelAction::NoDatabase(value))
            }
            CommandCommand::Run { .. } => Ok(TopLevelAction::NeedsDatabase),
        },
        _ => Ok(TopLevelAction::NeedsDatabase),
    }
}

fn config_command(command: ConfigCommand) -> Command {
    match command {
        ConfigCommand::Get => Command::ConfigGet {},
        ConfigCommand::GetKey { key } => Command::ConfigureGetKey { key },
        // User-config subcommands are handled before a database opens
        // (top_level_without_database); reaching here is a dispatch bug.
        ConfigCommand::Set { .. }
        | ConfigCommand::Unset { .. }
        | ConfigCommand::Path
        | ConfigCommand::Show => unreachable!("user-config subcommands run without a database"),
    }
}

/// The settings a `[providers.<name>]` section stores, in the order the
/// usage line lists them.
#[cfg(all(feature = "native", feature = "inference"))]
const PROVIDER_SETTINGS: [strata_hub::ProviderSetting; 2] = [
    strata_hub::ProviderSetting::ApiKey,
    strata_hub::ProviderSetting::BaseUrl,
];

/// The canonical provider name and setting behind a `<provider>.api_key` or
/// `<provider>.base_url` config key, validated against the known cloud
/// providers, or `None`.
#[cfg(all(feature = "native", feature = "inference"))]
fn provider_setting_target(key: &str) -> Option<(&'static str, strata_hub::ProviderSetting)> {
    let (provider, field) = key.rsplit_once('.')?;
    let setting = PROVIDER_SETTINGS
        .into_iter()
        .find(|setting| setting.field() == field)?;
    let info = strata_executor::inference_provider_key_info(provider)?;
    Some((info.provider, setting))
}

#[cfg(all(feature = "native", not(feature = "inference")))]
fn provider_setting_target(_key: &str) -> Option<(&'static str, strata_hub::ProviderSetting)> {
    None
}

/// Whether `key` is a user-config key handled without opening a database
/// (`hub.url`, a `<provider>.api_key`, or a `<provider>.base_url`).
#[cfg(feature = "native")]
fn is_user_config_key(key: &str) -> bool {
    key == "hub.url" || provider_setting_target(key).is_some()
}

/// Mask a secret to a short non-secret prefix, e.g. `sk-ant-****`.
#[cfg(feature = "native")]
fn redact_key(value: &str) -> String {
    let prefix: String = value.chars().take(7).collect();
    format!("{prefix}****")
}

/// The settable user-config keys, for the usage line: `hub.url` and, in a
/// build with inference, each cloud provider's settings — derived from the
/// executor's provider table so a provider added there is listed here.
#[cfg(feature = "native")]
fn settable_config_keys() -> String {
    let mut keys = vec!["hub.url".to_owned()];
    #[cfg(feature = "inference")]
    for info in strata_executor::INFERENCE_CLOUD_PROVIDER_KEYS {
        for setting in PROVIDER_SETTINGS {
            keys.push(format!("{}.{}", info.provider, setting.field()));
        }
    }
    keys.join(", ")
}

#[cfg(feature = "native")]
fn unknown_config_key(key: &str) -> CliError {
    CliError::usage(format!(
        "unknown config key `{key}`; settable keys: {}",
        settable_config_keys()
    ))
}

/// `strata config set <key> <value>` — writes the global user config.
/// `hub.url` sets the hub; `<provider>.api_key` stores a cloud API key (0600,
/// never echoed back in plaintext); `<provider>.base_url` redirects the
/// provider's requests (#3270). The provider's own environment variable
/// (`OPENAI_API_KEY`, `OPENAI_BASE_URL`, …) overrides a stored value.
#[cfg(feature = "native")]
fn user_config_set(key: &str, value: &str) -> Result<serde_json::Value, CliError> {
    if key == "hub.url" {
        let path = strata_hub::write_global_hub_url(value)
            .map_err(|error| CliError::usage(error.to_string()))?;
        return Ok(serde_json::json!({
            "key": key,
            "value": value,
            "path": path.display().to_string(),
        }));
    }
    if let Some((provider, setting)) = provider_setting_target(key) {
        let path = strata_hub::global_config_path().ok_or_else(|| {
            CliError::usage("the platform exposes no user config directory to store the setting in")
        })?;
        strata_hub::write_provider_setting(&path, provider, setting, value)
            .map_err(|error| CliError::usage(error.to_string()))?;
        let path = path.display().to_string();
        return Ok(match setting {
            strata_hub::ProviderSetting::ApiKey => serde_json::json!({
                "key": key,
                "value": redact_key(value),
                "path": path,
                "note": "API key stored (env var overrides it)",
            }),
            strata_hub::ProviderSetting::BaseUrl => serde_json::json!({
                "key": key,
                "value": value,
                "path": path,
                "note": "base URL stored (env var overrides it)",
            }),
        });
    }
    Err(unknown_config_key(key))
}

#[cfg(feature = "native")]
fn user_config_unset(key: &str) -> Result<serde_json::Value, CliError> {
    if key == "hub.url" {
        let path = strata_hub::unset_global_hub_url()
            .map_err(|error| CliError::usage(error.to_string()))?;
        return Ok(serde_json::json!({
            "key": key,
            "unset": true,
            "path": path.map(|path| path.display().to_string()),
        }));
    }
    if let Some((provider, setting)) = provider_setting_target(key) {
        // `path` names the file only when one existed to remove the field from,
        // as for `hub.url`; no config directory means no file either.
        let path = strata_hub::global_config_path()
            .map(|path| {
                strata_hub::unset_provider_setting(&path, provider, setting)
                    .map(|existed| existed.then(|| path.display().to_string()))
            })
            .transpose()
            .map_err(|error| CliError::usage(error.to_string()))?
            .flatten();
        return Ok(serde_json::json!({
            "key": key,
            "unset": true,
            "path": path,
        }));
    }
    Err(unknown_config_key(key))
}

#[cfg(feature = "native")]
fn user_config_get(key: &str) -> Result<serde_json::Value, CliError> {
    if key == "hub.url" {
        let value = strata_hub::read_global_hub_url()
            .map_err(|error| CliError::usage(error.to_string()))?;
        return Ok(serde_json::json!({
            "key": "hub.url",
            "value": value.map(|url| url.to_string()),
        }));
    }
    if let Some((provider, setting)) = provider_setting_target(key) {
        let value = strata_hub::global_config_path()
            .map_or(Ok(None), |path| {
                strata_hub::read_provider_setting(&path, provider, setting)
            })
            .map_err(|error| CliError::usage(error.to_string()))?;
        // Never surface a raw key — report set/unset with a redacted preview.
        // A base URL is not a secret and reads back as stored.
        let shown = match setting {
            strata_hub::ProviderSetting::ApiKey => value.as_deref().map(redact_key),
            strata_hub::ProviderSetting::BaseUrl => value.clone(),
        };
        return Ok(serde_json::json!({
            "key": key,
            "set": value.is_some(),
            "value": shown,
        }));
    }
    Err(unknown_config_key(key))
}

/// Runs a command that may load a model, offering the download when the model
/// is not on disk (D8).
///
/// Loading a model never downloads on its own — a silent multi-hundred-megabyte
/// fetch is not something a caller can consent to mid-operation, and until this
/// landed `embed` and `rank` did exactly that while `generate` refused.
///
/// So the decision moves here, where the CLI knows who is asking:
///
/// - **A person at a terminal** is offered the download, with its size, and
///   answers.
/// - **Anything else** — `--json`, a pipe, an agent — gets the refusal, which
///   already names `strata inference models pull <model>`. An agent cannot
///   answer a prompt, and blocking one on a hidden fetch is the failure this
///   exists to prevent.
#[cfg(all(feature = "native", feature = "inference"))]
fn execute_with_download_offer(
    connection: &Connection,
    command: Command,
    format: options::Format,
) -> Result<strata_executor::Output, CliError> {
    execute_offering_download(
        connection,
        command,
        format,
        std::io::stdin().is_terminal(),
        &mut |error, pull_spec| {
            ask_for_download(error, pull_spec, std::io::stdin().lock(), std::io::stderr())
        },
    )
}

/// Shows the refusal and asks the question on `prompt`; the answer is one
/// line of `answers`. True only for an explicit `y`.
///
/// The streams are parameters so the answer logic has a test without a
/// terminal; the process's stdin and stderr are supplied by
/// [`execute_with_download_offer`].
#[cfg(all(feature = "native", feature = "inference"))]
fn ask_for_download(
    error: &ExecutorError,
    pull_spec: &str,
    mut answers: impl std::io::BufRead,
    mut prompt: impl std::io::Write,
) -> bool {
    // Rationale: the prompt is advisory. If it cannot be written or flushed
    // the question is lost, not the answer — the read below still decides,
    // and reporting the failure would go to the same broken stream.
    let _ = writeln!(prompt, "{error}")
        .and_then(|()| write!(prompt, "\nDownload {pull_spec} now? [y/N] "))
        .and_then(|()| prompt.flush());
    let mut answer = String::new();
    answers.read_line(&mut answer).is_ok() && answer.trim().eq_ignore_ascii_case("y")
}

/// The offer loop with who-is-asking and the question as inputs, so a test
/// can run it against the fake runtime without a terminal: refuse → ask →
/// pull → retry.
#[cfg(all(feature = "native", feature = "inference"))]
fn execute_offering_download(
    connection: &Connection,
    command: Command,
    format: options::Format,
    interactive: bool,
    ask: &mut dyn FnMut(&ExecutorError, &str) -> bool,
) -> Result<strata_executor::Output, CliError> {
    let error = match connection.execute(command.clone()) {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    let answer = error.inference_availability();
    let Some(pull_spec) = offerable_download(interactive, format, &command, answer.as_ref()) else {
        return Err(error.into());
    };
    let pull_spec = pull_spec.to_owned();
    if !ask(&error, &pull_spec) {
        return Err(error.into());
    }
    connection.execute(Command::InferenceModelsPull {
        model: pull_spec.clone(),
    })?;
    eprintln!("pulled {pull_spec}; retrying");
    Ok(connection.execute(command)?)
}

/// What a failed command may offer to download (D8): the pull spec, or
/// nothing.
///
/// A pure decision so it has a truth table. Every condition guards a
/// different mistake:
///
/// - **A terminal.** An agent cannot answer a prompt; blocking one on a hidden
///   fetch is the failure D8 exists to prevent.
/// - **Human output.** `--json` output is parsed. A prompt in the middle of it
///   corrupts the stream even when a human is watching.
/// - **The resolver's answer is `not_downloaded`**, read from the refusal's
///   details rather than from its code or from the command. The answer says
///   which model, so a `vector --text` refusal — whose model came from the
///   collection's record, not the command line — is offered too, and it says
///   what a pull accepts, so an uncatalogued name is never offered (#3226).
///   Any other answer is not fixed by downloading, and offering would be a
///   wrong suggestion rather than no suggestion.
/// - **Not a pull.** A refused pull carries the same answer, and running it
///   again would meet the same refusal.
#[cfg(all(feature = "native", feature = "inference"))]
fn offerable_download<'a>(
    interactive: bool,
    format: options::Format,
    command: &Command,
    answer: Option<&'a strata_executor::InferenceAvailabilityDetails>,
) -> Option<&'a str> {
    if !interactive
        || !matches!(format, options::Format::Human)
        || matches!(command, Command::InferenceModelsPull { .. })
    {
        return None;
    }
    let answer = answer?;
    if answer.availability != strata_executor::InferenceAvailabilityKind::NotDownloaded {
        return None;
    }
    answer.pull_spec.as_deref()
}

/// How a one-shot command opens its target (R9 of #3261). An `inference`
/// command works on models, not on a database's data, so with no target it
/// runs in an ephemeral cache session — `strata inference status` answers
/// from any directory, the way `strata doctor` does. Every other command
/// keeps the refusal: agents never write to an implicit location.
#[cfg(feature = "native")]
fn one_shot_intent(command: &options::TopCommand) -> open::OpenIntent {
    match command {
        #[cfg(feature = "inference")]
        options::TopCommand::Inference(_) => open::OpenIntent::InferenceOneShot,
        _ => open::OpenIntent::OneShot,
    }
}

#[cfg(feature = "native")]
fn user_config_path() -> Result<serde_json::Value, CliError> {
    let path = strata_hub::global_config_path()
        .ok_or_else(|| CliError::usage("the platform exposes no user config directory"))?;
    Ok(serde_json::json!({ "path": path.display().to_string() }))
}

/// `strata config show` — the resolved hub URL and which layer supplied
/// it, the first thing to ask for when strata talks to the wrong hub.
#[cfg(feature = "native")]
fn user_config_show() -> serde_json::Value {
    match strata_hub::resolve_hub_url(&strata_hub::HubUrlInputs::from_process(None)) {
        Ok(resolved) => serde_json::json!({
            "hub.url": resolved.url.to_string(),
            "source": resolved.source.to_string(),
        }),
        Err(error) => serde_json::json!({
            "hub.url": serde_json::Value::Null,
            "detail": error.to_string(),
        }),
    }
}

fn branch_command(command: BranchCommand) -> Result<Command, CliError> {
    Ok(match command {
        BranchCommand::List => Command::BranchList {},
        BranchCommand::Get { branch } => Command::BranchGet { branch },
        BranchCommand::Create { branch } => Command::BranchCreate { branch },
        BranchCommand::Fork {
            source,
            branch,
            version,
            timestamp,
        } => match (version, timestamp) {
            (Some(version), None) => Command::BranchForkAtVersion {
                source,
                branch,
                version,
            },
            (None, Some(timestamp)) => Command::BranchForkAtTimestamp {
                source,
                branch,
                timestamp,
            },
            (None, None) | (Some(_), Some(_)) => Command::BranchForkCurrent { source, branch },
        },
        BranchCommand::Delete { branch } => Command::BranchDelete { branch },
        // #3112 S5: `branch diff` reaches a different wire field
        // (`at_timestamp`, not `as_of`), so it has no `as_of_time` counterpart
        // to offer yet — giving it one is a wire change, not a CLI change.
        BranchCommand::Diff {
            branch_a,
            branch_b,
            as_of,
        } => Command::BranchDiff {
            branch_a,
            branch_b,
            at_timestamp: as_of,
        },
        BranchCommand::Merge {
            source,
            target,
            strategy,
        } => Command::BranchMerge {
            source,
            target,
            strategy: strategy.into(),
        },
        BranchCommand::Preview {
            source,
            target,
            strategy,
        } => Command::BranchPreview {
            source,
            target,
            strategy: strategy.into(),
        },
        BranchCommand::Tag(_) => return Err(deferred_command("branch tag")),
        BranchCommand::Note(_) => return Err(deferred_command("branch note")),
    })
}

fn space_command(command: SpaceCommand, scope: &Scope) -> Command {
    match command {
        SpaceCommand::List => Command::SpaceList {
            branch: scope.branch.clone(),
        },
        SpaceCommand::Create { space } => Command::SpaceCreate {
            branch: scope.branch.clone(),
            space,
        },
        SpaceCommand::Exists { space } => Command::SpaceExists {
            branch: scope.branch.clone(),
            space,
        },
        SpaceCommand::Delete { space, force } => Command::SpaceDelete {
            branch: scope.branch.clone(),
            space,
            force,
        },
    }
}

/// #3112 S5: parses `--as-of-time` into the UTC epoch microseconds the wire
/// carries. Refusals name a working spelling rather than just rejecting.
///
/// Ungated: the command mappings that call it are compiled for the browser
/// target too, where a `--as-of-time` value still has to become an instant.
fn as_of_time_micros(input: Option<&str>) -> Result<Option<u64>, CliError> {
    input
        .map(|text| {
            crate::wall_clock::parse_instant(text)
                .map_err(|reason| CliError::usage(format!("invalid --as-of-time: {reason}")))
        })
        .transpose()
}

fn kv_command(command: KvCommand, scope: &Scope) -> Result<Command, CliError> {
    Ok(match command {
        KvCommand::Put { key, value, file } => Command::KvPut {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key: bytes(key),
            value: bytes_argument(value.as_deref(), file.as_ref())?,
        },
        KvCommand::Get {
            key,
            as_of,
            as_of_time,
        } => Command::KvGet {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key: bytes(key),
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        KvCommand::Delete { key } => Command::KvDelete {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key: bytes(key),
        },
        KvCommand::List {
            prefix,
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::KvList {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix: prefix.map(bytes),
            cursor: cursor.as_deref().map(cursor_argument).transpose()?,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        KvCommand::Scan {
            start,
            cursor,
            limit,
        } => Command::KvScan {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            // A cursor continues from the first unreturned row, so it maps to
            // the inclusive scan start (clap rejects --start with --cursor).
            start: match cursor {
                Some(cursor) => Some(cursor_argument(&cursor)?),
                None => start.map(bytes),
            },
            limit,
        },
        KvCommand::Exists { key } => Command::KvExists {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key: bytes(key),
        },
        KvCommand::History { key } => Command::KvHistory {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key: bytes(key),
        },
        KvCommand::Count { prefix } => Command::KvCount {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix: prefix.map(bytes),
            as_of: None,
            as_of_time: None,
        },
        KvCommand::Sample { prefix, count } => Command::KvSample {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix: prefix.map(bytes),
            count,
        },
    })
}

// A flat command-mapping table, like its vector/graph/inference siblings above.
#[allow(clippy::too_many_lines)]
fn json_command(command: JsonCommand, scope: &Scope) -> Result<Command, CliError> {
    Ok(match command {
        JsonCommand::Set {
            key,
            path,
            value,
            file,
        } => Command::JsonSet {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key,
            path,
            value: parse_relaxed_json_argument(value.as_deref(), file.as_ref(), "json value")?,
        },
        JsonCommand::Get {
            key,
            path,
            as_of,
            as_of_time,
        } => Command::JsonGet {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key,
            path,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        JsonCommand::Delete { key, path } => Command::JsonDelete {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key,
            path,
        },
        JsonCommand::List {
            prefix,
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::JsonList {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix,
            cursor,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        JsonCommand::Scan {
            start,
            cursor,
            limit,
        } => Command::JsonScan {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            // A cursor continues from the first unreturned document, so it maps
            // to the inclusive scan start (clap rejects --start with --cursor).
            start: cursor.or(start),
            limit,
        },
        JsonCommand::Exists { key } => Command::JsonExists {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key,
        },
        JsonCommand::History { key } => Command::JsonHistory {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            key,
        },
        JsonCommand::Count { prefix } => Command::JsonCount {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix,
            as_of: None,
            as_of_time: None,
        },
        JsonCommand::Sample { prefix, count } => Command::JsonSample {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            prefix,
            count,
        },
        JsonCommand::Index {
            command:
                options::JsonIndexCommand::Create {
                    name,
                    field_path,
                    index_type,
                },
        } => Command::JsonCreateIndex {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            name,
            field_path,
            index_type: index_type.into(),
        },
        JsonCommand::Index {
            command: options::JsonIndexCommand::Drop { name },
        } => Command::JsonDropIndex {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            name,
        },
        JsonCommand::Index {
            command: options::JsonIndexCommand::List,
        } => Command::JsonListIndexes {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
        },
    })
}

#[allow(clippy::too_many_lines)]
fn vector_command(command: VectorCommand, scope: &Scope) -> Result<Command, CliError> {
    Ok(match command {
        VectorCommand::Collection { command } => vector_collection_command(command, scope),
        VectorCommand::Upsert {
            collection,
            key,
            vector,
            text,
            metadata,
            file,
            metadata_file,
        } => Command::VectorUpsert {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
            // Clap already refuses `--text` alongside a vector or a file, so
            // an empty vector here means the text form was chosen.
            vector: if text.is_some() {
                Vec::new()
            } else {
                parse_vector_argument(vector.as_deref(), file.as_ref(), "vector")?
            },
            text,
            metadata: parse_optional_json_argument(
                metadata.as_deref(),
                metadata_file.as_ref(),
                "vector metadata",
            )?,
        },
        VectorCommand::Get {
            collection,
            key,
            as_of,
            as_of_time,
        } => Command::VectorGet {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        VectorCommand::History { collection, key } => Command::VectorHistory {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
        },
        VectorCommand::Exists { collection, key } => Command::VectorExists {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
        },
        VectorCommand::Keys {
            collection,
            prefix,
            cursor,
            limit,
        } => Command::VectorListKeys {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            prefix,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        },
        VectorCommand::Scan {
            collection,
            start,
            cursor,
            limit,
        } => Command::VectorScan {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            // A cursor continues from the first unreturned key, so it maps to
            // the inclusive scan start (clap rejects --start with --cursor).
            start: cursor.or(start),
            limit,
        },
        VectorCommand::UpdateMetadata {
            collection,
            key,
            patch,
            file,
        } => Command::VectorUpdateMetadata {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
            patch: parse_json_argument(patch.as_deref(), file.as_ref(), "metadata patch")?,
        },
        VectorCommand::Delete { collection, key } => Command::VectorDelete {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            key,
        },
        VectorCommand::DeleteAll { collection } => Command::VectorDeleteAll {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
        },
        VectorCommand::DeleteByFilter {
            collection,
            filter,
            filter_file,
        } => Command::VectorDeleteByFilter {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            filter: parse_filter_argument(filter.as_deref(), filter_file.as_ref())?,
        },
        VectorCommand::Query {
            collection,
            query,
            text,
            file,
            k,
            filter,
            filter_file,
            as_of,
            as_of_time,
            diagnostics,
        } => {
            let command_filter =
                parse_optional_filter_argument(filter.as_deref(), filter_file.as_ref())?;
            if diagnostics {
                if text.is_some() {
                    return Err(CliError::usage(
                        "`--text` is not supported with `--diagnostics`; \
                         index diagnostics take an explicit query vector",
                    ));
                }
                Command::VectorIndexQuery {
                    branch: scope.branch.clone(),
                    space: scope.space.clone(),
                    collection,
                    query: parse_vector_argument(query.as_deref(), file.as_ref(), "query vector")?,
                    k,
                    filter: command_filter,
                    as_of,
                    as_of_time: as_of_time_micros(as_of_time.as_deref())?,
                }
            } else {
                Command::VectorQuery {
                    branch: scope.branch.clone(),
                    space: scope.space.clone(),
                    collection,
                    query: if text.is_some() {
                        Vec::new()
                    } else {
                        parse_vector_argument(query.as_deref(), file.as_ref(), "query vector")?
                    },
                    text,
                    k,
                    filter: command_filter,
                    as_of,
                    as_of_time: as_of_time_micros(as_of_time.as_deref())?,
                }
            }
        }
        VectorCommand::Count { collection } => Command::VectorCount {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            as_of: None,
            as_of_time: None,
        },
        VectorCommand::Sample { collection, count } => Command::VectorSample {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            count,
        },
    })
}

fn vector_collection_command(command: VectorCollectionCommand, scope: &Scope) -> Command {
    match command {
        VectorCollectionCommand::Create {
            collection,
            dimension,
            metric,
            embedding_model,
        } => Command::VectorCreateCollection {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
            dimension,
            metric: metric.into(),
            embedding_model,
        },
        VectorCollectionCommand::Delete { collection } => Command::VectorDeleteCollection {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
        },
        VectorCollectionCommand::List => Command::VectorListCollections {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
        },
        VectorCollectionCommand::Stats { collection } => Command::VectorCollectionStats {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            collection,
        },
        VectorCollectionCommand::SetEmbeddingModel { collection, model } => {
            Command::VectorSetEmbeddingModel {
                branch: scope.branch.clone(),
                space: scope.space.clone(),
                collection,
                model,
            }
        }
    }
}

// A flat command-mapping table, like its vector/graph/json siblings.
#[allow(clippy::too_many_lines)]
fn event_command(command: EventCommand, scope: &Scope) -> Result<Command, CliError> {
    Ok(match command {
        EventCommand::Append {
            event_type,
            payload,
            file,
        } => Command::EventAppend {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            event_type,
            payload: parse_json_argument(payload.as_deref(), file.as_ref(), "event payload")?,
        },
        EventCommand::Get {
            sequence,
            as_of,
            as_of_time,
        } => Command::EventGet {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            sequence,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        EventCommand::Exists { sequence } => Command::EventExists {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            sequence,
        },
        EventCommand::Count { as_of, as_of_time } => Command::EventCount {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        EventCommand::List {
            event_type,
            limit,
            after_sequence,
            as_of,
            as_of_time,
        } => Command::EventList {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            event_type,
            limit,
            after_sequence,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        EventCommand::Types { as_of, as_of_time } => Command::EventListTypes {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        EventCommand::ByType {
            event_type,
            limit,
            after_sequence,
            as_of,
            as_of_time,
        } => Command::EventList {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            event_type: Some(event_type),
            limit,
            after_sequence,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        EventCommand::Range {
            start_seq,
            end_seq,
            limit,
            direction,
            event_type,
        } => Command::EventRange {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            start_seq,
            end_seq,
            limit,
            direction: direction.into(),
            event_type,
        },
        EventCommand::RangeTime {
            start_ts,
            end_ts,
            limit,
            direction,
            event_type,
        } => Command::EventRangeByTime {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            start_ts,
            end_ts,
            limit,
            direction: direction.into(),
            event_type,
        },
        EventCommand::VerifyChain => Command::EventVerifyChain {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
        },
    })
}

#[allow(clippy::too_many_lines)]
fn graph_command(command: GraphCommand, scope: &Scope) -> Result<Command, CliError> {
    Ok(match command {
        GraphCommand::Create { graph } => Command::GraphCreate {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
        },
        GraphCommand::Delete { graph } => Command::GraphDelete {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
        },
        GraphCommand::List {
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::GraphList {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            cursor,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Meta {
            graph,
            as_of,
            as_of_time,
        } => Command::GraphGetMeta {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::AddNode {
            graph,
            node_id,
            properties,
            properties_file,
            object_type,
        } => Command::GraphAddNode {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            node_id,
            properties: parse_optional_json_argument(
                properties.as_deref(),
                properties_file.as_ref(),
                "node properties",
            )?,
            binding: None,
            object_type,
        },
        GraphCommand::GetNode {
            graph,
            node_id,
            as_of,
            as_of_time,
        } => Command::GraphGetNode {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            node_id,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::RemoveNode { graph, node_id } => Command::GraphRemoveNode {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            node_id,
        },
        GraphCommand::ListNodes {
            graph,
            prefix,
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::GraphListNodes {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            prefix,
            cursor,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Sample { graph, count } => Command::GraphSample {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            count,
        },
        GraphCommand::AddEdge {
            graph,
            src,
            edge_type,
            dst,
            weight,
            properties,
            properties_file,
        } => Command::GraphAddEdge {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            src,
            edge_type,
            dst,
            weight,
            properties: parse_optional_json_argument(
                properties.as_deref(),
                properties_file.as_ref(),
                "edge properties",
            )?,
        },
        GraphCommand::GetEdge {
            graph,
            src,
            edge_type,
            dst,
            as_of,
            as_of_time,
        } => Command::GraphGetEdge {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            src,
            edge_type,
            dst,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::RemoveEdge {
            graph,
            src,
            edge_type,
            dst,
        } => Command::GraphRemoveEdge {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            src,
            edge_type,
            dst,
        },
        GraphCommand::Neighbors {
            graph,
            node_id,
            direction,
            edge_type,
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::GraphNeighbors {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            node_id,
            direction: direction.into(),
            edge_type,
            cursor,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::NodesByType {
            graph,
            object_type,
            cursor,
            limit,
            as_of,
            as_of_time,
        } => Command::GraphNodesByType {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            object_type,
            cursor,
            limit,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Ontology(args) => graph_ontology_command(args.command, scope)?,
        GraphCommand::Wcc {
            graph,
            as_of,
            as_of_time,
        } => Command::GraphWcc {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Lcc {
            graph,
            as_of,
            as_of_time,
        } => Command::GraphLcc {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Sssp {
            graph,
            source,
            direction,
            as_of,
            as_of_time,
        } => Command::GraphSssp {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            source,
            direction: Some(direction.into()),
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Pagerank {
            graph,
            damping,
            max_iterations,
            tolerance,
            personalization,
            as_of,
            as_of_time,
        } => Command::GraphPagerank {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            damping,
            max_iterations,
            tolerance,
            personalization: personalization
                .as_deref()
                .map(parse_personalization)
                .transpose()?,
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::Cdlp {
            graph,
            max_iterations,
            direction,
            as_of,
            as_of_time,
        } => Command::GraphCdlp {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            max_iterations,
            direction: Some(direction.into()),
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphCommand::BulkInsert {
            graph,
            data,
            file,
            chunk_size,
        } => {
            let payload =
                parse_json_argument(data.as_deref(), file.as_ref(), "bulk-insert payload")?;
            let payload: BulkInsertPayload = serde_json::from_value(payload).map_err(|error| {
                CliError::usage(format!(
                    "bulk-insert payload must be {{\"nodes\": [...], \"edges\": [...]}}: {error}"
                ))
            })?;
            Command::GraphBulkInsert {
                branch: scope.branch.clone(),
                space: scope.space.clone(),
                graph,
                nodes: payload.nodes,
                edges: payload.edges,
                chunk_size,
            }
        }
        GraphCommand::Bfs {
            graph,
            start,
            max_depth,
            max_nodes,
            edge_types,
            direction,
            as_of,
            as_of_time,
        } => Command::GraphBfs {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            start,
            max_depth,
            max_nodes,
            edge_types: if edge_types.is_empty() {
                None
            } else {
                Some(edge_types)
            },
            direction: Some(direction.into()),
            budget: None,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
    })
}

#[derive(serde::Deserialize)]
struct BulkInsertPayload {
    #[serde(default)]
    nodes: Vec<strata_executor::GraphBulkNode>,
    #[serde(default)]
    edges: Vec<strata_executor::GraphBulkEdge>,
}

fn parse_personalization(raw: &str) -> Result<std::collections::BTreeMap<String, f64>, CliError> {
    serde_json::from_str(raw).map_err(|error| {
        CliError::usage(format!(
            "personalization must be a JSON object mapping node ids to weights: {error}"
        ))
    })
}

fn graph_ontology_command(
    command: GraphOntologyCommand,
    scope: &Scope,
) -> Result<Command, CliError> {
    Ok(match command {
        GraphOntologyCommand::DefineObjectType {
            graph,
            name,
            properties,
            properties_file,
        } => Command::GraphDefineObjectType {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            name,
            properties: parse_type_properties(properties.as_deref(), properties_file.as_ref())?,
        },
        GraphOntologyCommand::DefineLinkType {
            graph,
            name,
            source,
            target,
            cardinality,
            properties,
            properties_file,
        } => Command::GraphDefineLinkType {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            name,
            source,
            target,
            cardinality,
            properties: parse_type_properties(properties.as_deref(), properties_file.as_ref())?,
        },
        GraphOntologyCommand::DeleteObjectType { graph, name } => Command::GraphDeleteObjectType {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            name,
        },
        GraphOntologyCommand::DeleteLinkType { graph, name } => Command::GraphDeleteLinkType {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            name,
        },
        GraphOntologyCommand::Freeze { graph } => Command::GraphFreezeOntology {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
        },
        GraphOntologyCommand::Get {
            graph,
            as_of,
            as_of_time,
        } => Command::GraphGetOntology {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
        GraphOntologyCommand::Summary {
            graph,
            as_of,
            as_of_time,
        } => Command::GraphOntologySummary {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            graph,
            as_of,
            as_of_time: as_of_time_micros(as_of_time.as_deref())?,
        },
    })
}

/// Parses the ontology type-properties JSON argument into the wire map.
fn parse_type_properties(
    properties: Option<&str>,
    properties_file: Option<&std::path::PathBuf>,
) -> Result<std::collections::BTreeMap<String, GraphPropertyDef>, CliError> {
    let Some(value) = parse_optional_json_argument(properties, properties_file, "type properties")?
    else {
        return Ok(std::collections::BTreeMap::new());
    };
    serde_json::from_value(value).map_err(|error| {
        CliError::usage(format!(
            "type properties must map names to {{value_type, required}}: {error}"
        ))
    })
}

fn arrow_command(command: ArrowCommand, scope: &Scope) -> Command {
    match command {
        ArrowCommand::Import {
            file_path,
            format,
            target,
            key_column,
            value_column,
            collection,
            graph,
        } => Command::ArrowImport {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            file_path,
            format: format.map(Into::into),
            target: target.into(),
            key_column,
            value_column,
            collection,
            graph,
        },
        ArrowCommand::Export {
            primitive,
            format,
            path,
            prefix,
            limit,
            collection,
            graph,
            event_type,
        } => Command::ArrowExport {
            branch: scope.branch.clone(),
            space: scope.space.clone(),
            primitive: primitive.into(),
            format: format.into(),
            path,
            prefix,
            limit,
            collection,
            graph,
            event_type,
        },
    }
}

/// Builds executor commands for the inference family. Inference is
/// database-independent (models are process state), so no scope is injected.
#[cfg(feature = "inference")]
#[allow(
    clippy::too_many_lines,
    reason = "one flat match over the inference verbs; the Generate arm assembles the chat body inline"
)]
fn inference_command(command: options::InferenceCommand) -> Result<Command, CliError> {
    use options::{InferenceCommand as Inf, InferenceModelsCommand as Models};
    Ok(match command {
        Inf::Models(args) => match args.command {
            Models::List => Command::InferenceModelsList {},
            Models::Local => Command::InferenceModelsLocal {},
            Models::Pull { model } => Command::InferenceModelsPull { model },
        },
        Inf::Capability { model } => Command::InferenceModelCapability { model },
        Inf::Generate {
            model,
            prompt,
            system,
            messages,
            messages_json,
            json_body,
            max_tokens,
            temperature,
            top_k,
            top_p,
            min_p,
            repeat_penalty,
            frequency_penalty,
            presence_penalty,
            seed,
            stop_sequences,
            stop_tokens,
            response_format,
            grammar,
            n_ctx,
            n_gpu_layers,
            chat_format,
            tools_json,
            tool_choice,
            response_schema,
            response_schema_name,
            logprobs,
            top_logprobs,
        } => {
            let request = if let Some(body) = json_body {
                serde_json::from_str::<strata_executor::InferenceChatRequest>(&body)
                    .map_err(|error| CliError::usage(format!("invalid --json-body: {error}")))?
            } else {
                let mut turns: Vec<strata_executor::InferenceChatMessage> = Vec::new();
                if let Some(system) = system {
                    turns.push(strata_executor::InferenceChatMessage::new(
                        strata_executor::InferenceRole::System,
                        system,
                    ));
                }
                for spec in &messages {
                    turns.push(parse_chat_message(spec)?);
                }
                if let Some(json) = messages_json {
                    let parsed: Vec<strata_executor::InferenceChatMessage> =
                        serde_json::from_str(&json).map_err(|error| {
                            CliError::usage(format!("invalid --messages-json: {error}"))
                        })?;
                    turns.extend(parsed);
                }

                let mut request = strata_executor::InferenceChatRequest::default();
                if turns.is_empty() {
                    match prompt {
                        Some(prompt) => request.prompt = Some(prompt),
                        None => {
                            return Err(CliError::usage(
                                "provide a prompt, --system/--message, or --json-body".to_owned(),
                            ))
                        }
                    }
                } else {
                    if let Some(prompt) = prompt {
                        turns.push(strata_executor::InferenceChatMessage::new(
                            strata_executor::InferenceRole::User,
                            prompt,
                        ));
                    }
                    request.messages = Some(turns);
                }

                request.max_tokens = max_tokens;
                request.temperature = temperature;
                request.top_p = top_p;
                request.top_k = top_k;
                request.min_p = min_p;
                request.repeat_penalty = repeat_penalty;
                request.frequency_penalty = frequency_penalty;
                request.presence_penalty = presence_penalty;
                request.seed = seed;
                request.stop = (!stop_sequences.is_empty()).then_some(stop_sequences);
                request.stop_token_ids = (!stop_tokens.is_empty()).then_some(stop_tokens);
                request.grammar = grammar;
                request.response_format = response_format.map(|format| match format {
                    options::ResponseFormatArg::Text => {
                        strata_executor::InferenceResponseFormat::Text
                    }
                    options::ResponseFormatArg::JsonObject => {
                        strata_executor::InferenceResponseFormat::JsonObject
                    }
                });
                // --response-schema takes precedence over --response-format.
                if let Some(schema_json) = response_schema {
                    let schema: serde_json::Value = serde_json::from_str(&schema_json)
                        .map_err(|e| CliError::usage(format!("invalid --response-schema: {e}")))?;
                    request.response_format =
                        Some(strata_executor::InferenceResponseFormat::JsonSchema {
                            json_schema: strata_executor::InferenceJsonSchemaSpec {
                                name: response_schema_name.unwrap_or_else(|| "response".to_owned()),
                                description: None,
                                schema,
                                strict: None,
                            },
                        });
                }
                if let Some(tools_json) = tools_json {
                    let tools: Vec<strata_executor::InferenceTool> =
                        serde_json::from_str(&tools_json)
                            .map_err(|e| CliError::usage(format!("invalid --tools-json: {e}")))?;
                    request.tools = (!tools.is_empty()).then_some(tools);
                }
                if let Some(choice) = tool_choice {
                    request.tool_choice = Some(parse_tool_choice(&choice));
                }
                if logprobs || top_logprobs.is_some() {
                    request.logprobs = Some(true);
                    request.top_logprobs = top_logprobs;
                }
                if n_ctx.is_some() || n_gpu_layers.is_some() || chat_format.is_some() {
                    request.model_config = Some(strata_executor::InferenceModelConfig {
                        n_ctx,
                        n_gpu_layers,
                        chat_format,
                        ..Default::default()
                    });
                }
                request
            };
            Command::InferenceGenerate { model, request }
        }
        Inf::Tokenize {
            model,
            text,
            special,
        } => Command::InferenceTokenize {
            model,
            text,
            add_special: special,
        },
        Inf::Detokenize { model, ids } => Command::InferenceDetokenize { model, ids },
        Inf::Embed {
            model,
            inputs,
            dimensions,
            normalize,
            input_type,
        } => {
            let input = if inputs.len() == 1 {
                strata_executor::InferenceEmbedInput::One(
                    inputs.into_iter().next().expect("one input present"),
                )
            } else {
                strata_executor::InferenceEmbedInput::Many(inputs)
            };
            Command::InferenceEmbed {
                model,
                request: strata_executor::InferenceEmbeddingsRequest {
                    input,
                    dimensions,
                    normalize: normalize.then_some(true),
                    input_type: input_type.map(|kind| match kind {
                        options::InputTypeArg::Query => strata_executor::InferenceInputType::Query,
                        options::InputTypeArg::Document => {
                            strata_executor::InferenceInputType::Document
                        }
                    }),
                    instruction: None,
                },
            }
        }
        Inf::Rank {
            model,
            query,
            passages,
        } => Command::InferenceRank {
            model,
            request: strata_executor::InferenceRankRequest { query, passages },
        },
        Inf::Unload { model } => Command::InferenceUnload { model },
        Inf::CacheStatus => Command::InferenceCacheStatus {},
        Inf::Status => Command::InferenceStatus {},
        // Handled before the connection is opened: it replaces the binary
        // rather than executing against a database, so it has no wire command.
        Inf::InstallLocal => {
            return Err(CliError::usage(
                "`inference install-local` is a host command and is handled \
                 before dispatch; reaching here is a routing bug",
            ))
        }
    })
}

/// Parses a `--message "role:content"` spec into a chat message.
#[cfg(feature = "inference")]
fn parse_chat_message(spec: &str) -> Result<strata_executor::InferenceChatMessage, CliError> {
    let (role, content) = spec.split_once(':').ok_or_else(|| {
        CliError::usage(format!("--message must be \"role:content\", got {spec:?}"))
    })?;
    let role = match role.trim() {
        "system" => strata_executor::InferenceRole::System,
        "user" => strata_executor::InferenceRole::User,
        "assistant" => strata_executor::InferenceRole::Assistant,
        "tool" => strata_executor::InferenceRole::Tool,
        other => {
            return Err(CliError::usage(format!(
                "unknown message role {other:?} (expected system|user|assistant|tool)"
            )))
        }
    };
    Ok(strata_executor::InferenceChatMessage::new(
        role,
        content.to_owned(),
    ))
}

/// Parse a `--tool-choice` value: `auto` | `none` | `required` (case-insensitive)
/// select a mode; anything else names a function to force.
#[cfg(feature = "inference")]
fn parse_tool_choice(value: &str) -> strata_executor::InferenceToolChoice {
    use strata_executor::{
        InferenceNamedToolChoice, InferenceToolChoice, InferenceToolChoiceFunction,
        InferenceToolChoiceMode,
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => InferenceToolChoice::Mode(InferenceToolChoiceMode::Auto),
        "none" => InferenceToolChoice::Mode(InferenceToolChoiceMode::None),
        "required" => InferenceToolChoice::Mode(InferenceToolChoiceMode::Required),
        _ => InferenceToolChoice::Named(InferenceNamedToolChoice::Function {
            function: InferenceToolChoiceFunction {
                name: value.trim().to_owned(),
            },
        }),
    }
}

// Test gates are stacked, never `all(test, …)`: cargo-mutants recognises the
// literal `#[cfg(test)]` and skips the module, but does not look inside
// `all(...)`, so a combined gate has it mutate the module's helper fns as
// product code.
#[cfg(test)]
#[cfg(feature = "inference")]
mod config_key_tests {
    use super::{
        is_user_config_key, provider_setting_target, redact_key, settable_config_keys,
        unknown_config_key,
    };
    use strata_hub::ProviderSetting;

    #[test]
    fn redacts_to_a_non_secret_prefix() {
        // Real keys are long, so only a public-ish prefix is ever revealed.
        assert_eq!(
            redact_key("sk-ant-api03-averylongsecretvalue"),
            "sk-ant-****"
        );
        assert_eq!(redact_key("AIzaSyD-averylongsecretvalue"), "AIzaSyD****");
    }

    #[test]
    fn provider_setting_targets_are_validated() {
        for (provider, setting) in [
            ("openai", ProviderSetting::ApiKey),
            ("anthropic", ProviderSetting::ApiKey),
            ("google", ProviderSetting::ApiKey),
            ("openai", ProviderSetting::BaseUrl),
            ("anthropic", ProviderSetting::BaseUrl),
            ("google", ProviderSetting::BaseUrl),
        ] {
            let key = format!("{provider}.{}", setting.field());
            assert_eq!(
                provider_setting_target(&key),
                Some((provider, setting)),
                "{key}"
            );
        }
        // The provider name is canonicalized the way `inference status` names it.
        assert_eq!(
            provider_setting_target("OpenAI.base_url"),
            Some(("openai", ProviderSetting::BaseUrl))
        );
        for key in [
            "bogus.api_key",
            "bogus.base_url",
            "openai.model",
            "openai",
            "api_key",
            ".api_key",
            "openai.api_key.extra",
        ] {
            assert_eq!(provider_setting_target(key), None, "{key}");
        }
    }

    #[test]
    fn user_config_keys_recognized() {
        assert!(is_user_config_key("hub.url"));
        assert!(is_user_config_key("openai.api_key"));
        assert!(is_user_config_key("google.base_url"));
        assert!(!is_user_config_key("nonsense"));
        assert!(!is_user_config_key("hub.base_url"));
    }

    /// The usage line lists every key `strata config set` accepts and no
    /// other — each listed key is a target, each target is listed.
    #[test]
    fn the_usage_line_lists_exactly_the_settable_keys() {
        let listed = settable_config_keys();
        let keys: Vec<&str> = listed.split(", ").collect();
        assert_eq!(keys[0], "hub.url");
        for key in &keys[1..] {
            assert!(
                provider_setting_target(key).is_some(),
                "{key} is listed but not settable"
            );
        }
        for info in strata_executor::INFERENCE_CLOUD_PROVIDER_KEYS {
            for setting in [ProviderSetting::ApiKey, ProviderSetting::BaseUrl] {
                let key = format!("{}.{}", info.provider, setting.field());
                assert!(
                    keys.contains(&key.as_str()),
                    "{key} is settable but not listed"
                );
            }
        }
        let error = unknown_config_key("bogus.key").to_string();
        assert!(error.contains("`bogus.key`"), "{error}");
        assert!(error.contains(&listed), "{error}");
    }
}

// Stacked gate: see `config_key_tests`.
#[cfg(test)]
#[cfg(feature = "inference")]
mod inference_command_tests {
    use super::{inference_command, options, parse_chat_message};
    use serde_json::json;

    fn generate(
        prompt: Option<&str>,
        system: Option<&str>,
        messages: Vec<&str>,
        json_body: Option<&str>,
    ) -> options::InferenceCommand {
        options::InferenceCommand::Generate {
            model: "m".to_owned(),
            prompt: prompt.map(str::to_owned),
            system: system.map(str::to_owned),
            messages: messages.into_iter().map(str::to_owned).collect(),
            messages_json: None,
            json_body: json_body.map(str::to_owned),
            max_tokens: None,
            temperature: None,
            top_k: None,
            top_p: None,
            min_p: None,
            repeat_penalty: None,
            frequency_penalty: None,
            presence_penalty: None,
            seed: None,
            stop_sequences: vec![],
            stop_tokens: vec![],
            response_format: None,
            grammar: None,
            n_ctx: None,
            n_gpu_layers: None,
            chat_format: None,
            tools_json: None,
            tool_choice: None,
            response_schema: None,
            response_schema_name: None,
            logprobs: false,
            top_logprobs: None,
        }
    }

    fn body(command: options::InferenceCommand) -> serde_json::Value {
        let command = inference_command(command).expect("builds");
        serde_json::to_value(&command).expect("serializes")["request"].clone()
    }

    #[test]
    fn system_plus_prompt_becomes_messages() {
        let req = body(generate(Some("hello"), Some("be terse"), vec![], None));
        assert_eq!(
            req["messages"][0],
            json!({"role": "system", "content": "be terse"})
        );
        assert_eq!(
            req["messages"][1],
            json!({"role": "user", "content": "hello"})
        );
        assert!(req.get("prompt").is_none());
    }

    #[test]
    fn prompt_only_is_raw_completion() {
        let req = body(generate(Some("once upon"), None, vec![], None));
        assert_eq!(req["prompt"], "once upon");
        assert!(req.get("messages").is_none());
    }

    #[test]
    fn json_body_overrides_everything() {
        let req = body(generate(
            None,
            None,
            vec![],
            Some(r#"{"messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#),
        ));
        assert_eq!(req["max_tokens"], 5);
        assert_eq!(req["messages"][0]["content"], "hi");
    }

    #[test]
    fn no_input_is_a_usage_error() {
        let err = inference_command(generate(None, None, vec![], None)).unwrap_err();
        assert!(err.to_string().contains("prompt"), "got: {err}");
    }

    fn generate_with(f: impl FnOnce(&mut options::InferenceCommand)) -> options::InferenceCommand {
        let mut cmd = generate(Some("hi"), None, vec![], None);
        f(&mut cmd);
        cmd
    }

    #[test]
    fn tools_and_tool_choice_are_built() {
        let cmd = generate_with(|c| {
            if let options::InferenceCommand::Generate {
                tools_json,
                tool_choice,
                ..
            } = c
            {
                *tools_json = Some(
                    r#"[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]"#
                        .to_owned(),
                );
                *tool_choice = Some("required".to_owned());
            }
        });
        let req = body(cmd);
        assert_eq!(req["tools"][0]["type"], "function");
        assert_eq!(req["tools"][0]["function"]["name"], "get_weather");
        assert_eq!(req["tool_choice"], "required");
    }

    #[test]
    fn named_tool_choice_forces_function() {
        let cmd = generate_with(|c| {
            if let options::InferenceCommand::Generate { tool_choice, .. } = c {
                *tool_choice = Some("get_weather".to_owned());
            }
        });
        let req = body(cmd);
        assert_eq!(req["tool_choice"]["type"], "function");
        assert_eq!(req["tool_choice"]["function"]["name"], "get_weather");
    }

    #[test]
    fn response_schema_sets_json_schema_format() {
        let cmd = generate_with(|c| {
            if let options::InferenceCommand::Generate {
                response_schema,
                response_schema_name,
                ..
            } = c
            {
                *response_schema = Some(r#"{"type":"object"}"#.to_owned());
                *response_schema_name = Some("person".to_owned());
            }
        });
        let req = body(cmd);
        assert_eq!(req["response_format"]["type"], "json_schema");
        assert_eq!(req["response_format"]["json_schema"]["name"], "person");
        assert_eq!(
            req["response_format"]["json_schema"]["schema"]["type"],
            "object"
        );
    }

    #[test]
    fn logprobs_flag_is_built() {
        let cmd = generate_with(|c| {
            if let options::InferenceCommand::Generate {
                logprobs,
                top_logprobs,
                ..
            } = c
            {
                *logprobs = true;
                *top_logprobs = Some(3);
            }
        });
        let req = body(cmd);
        assert_eq!(req["logprobs"], true);
        assert_eq!(req["top_logprobs"], 3);
    }

    #[test]
    fn message_roles_parse_and_reject() {
        assert!(parse_chat_message("user:hi").is_ok());
        assert!(parse_chat_message("assistant:sure").is_ok());
        assert!(parse_chat_message("no-colon").is_err());
        assert!(parse_chat_message("wizard:hi").is_err());
    }

    #[test]
    fn embed_single_and_batch() {
        let one = body(options::InferenceCommand::Embed {
            model: "m".to_owned(),
            inputs: vec!["a".to_owned()],
            dimensions: None,
            normalize: false,
            input_type: None,
        });
        assert_eq!(one["input"], "a");

        let many = body(options::InferenceCommand::Embed {
            model: "m".to_owned(),
            inputs: vec!["a".to_owned(), "b".to_owned()],
            dimensions: Some(256),
            normalize: true,
            input_type: Some(options::InputTypeArg::Query),
        });
        assert_eq!(many["input"], json!(["a", "b"]));
        assert_eq!(many["dimensions"], 256);
        assert_eq!(many["normalize"], true);
        assert_eq!(many["input_type"], "query");
    }
}

fn raw_command(command: CommandCommand) -> Result<Command, CliError> {
    match command {
        CommandCommand::Run { json, file } => {
            raw_command_from_sources(json.as_deref(), file.as_ref())
        }
        CommandCommand::Print { .. } => Err(CliError::usage(
            "`command print` validates a command without executing it and is handled before open",
        )),
    }
}

fn raw_command_from_sources(
    json: Option<&str>,
    file: Option<&std::path::PathBuf>,
) -> Result<Command, CliError> {
    let text = match (json, file) {
        (Some(_), Some(_)) => {
            return Err(CliError::usage(
                "provide either `--command-json <json>` or `--file <path>`, not both",
            ));
        }
        (Some(json), None) => json.to_owned(),
        (None, Some(path)) => input::read_text_file(path)?,
        (None, None) => {
            return Err(CliError::usage(
                "raw command execution requires `--command-json <json>` or `--file <path>`",
            ));
        }
    };
    Ok(serde_json::from_str(&text)?)
}

fn bytes(value: String) -> strata_executor::Bytes {
    strata_executor::Bytes::new(value.into_bytes())
}

/// A CLI line parsed for an embedded session: the executor command it names
/// and the output format its flags selected (`--json`, `--raw`,
/// `--output-format`), so the caller renders the result the way the binary
/// would.
#[derive(Debug)]
#[non_exhaustive]
pub struct ParsedLine {
    /// The command to execute.
    pub command: Command,
    /// The output format the line's flags chose, or `None` when they chose
    /// nothing — the caller falls back to its session's format.
    pub format: Option<Format>,
}

/// Parses a single CLI line (e.g. `--json kv get greeting`) into an executor
/// [`Command`] and its output [`Format`], reusing the exact clap grammar and
/// argument handling the `strata` binary uses (one `SessionLine`, shared
/// with the REPL — `crates/cli/src/line.rs`). Wasm-safe and host-free: it
/// opens no database and performs no I/O. `branch`/`space` supply the
/// session scope for commands that omit their own `--branch`/`--space`.
///
/// The `Err` string is human-readable text to display in place of running a
/// command: a clap parse error, `--help`/`--version` output, or a note that the
/// command needs a host environment (`mcp`, `init`, `clone`, `start`/`stop`, …)
/// and is unavailable in an embedded session. A blank line or a `#` comment
/// is `Err` of the empty string: nothing to run, nothing to show.
pub fn command_from_line(
    line: &str,
    branch: Option<String>,
    space: Option<String>,
) -> Result<ParsedLine, String> {
    let Some(words) = line::words(line).map_err(|error| error.to_string())? else {
        return Err(String::new());
    };
    let parsed = line::SessionLine::parse(words).map_err(|error| error.to_string())?;
    // A per-command --branch/--space (global clap flags) overrides the session
    // scope the caller supplies.
    let scope = Scope {
        branch: parsed.branch.or(branch),
        space: parsed.space.or(space),
    };
    let command = command_to_executor(parsed.command, &scope).map_err(|error| error.to_string())?;
    Ok(ParsedLine {
        command,
        format: parsed.format,
    })
}

/// Runs one CLI line against an embedded `executor` and returns what the
/// `strata` binary would have written for it — stdout, then stderr, as one
/// string, in the format the line's flags chose. The executor's default
/// branch and space are the session scope. Wasm-safe: this is the browser
/// playground's whole path, and the output-contract matrix pins it equal to
/// the binary's channels (`crates/cli/tests/output_contract.rs`).
///
/// Parse errors, `--help`, and host-only refusals are returned as the text to
/// display (see [`command_from_line`]); command failures come back as the
/// CLI's error line. The only `Err` is a rendering failure.
///
/// # Errors
///
/// Returns the renderer's error when the executor's output cannot be
/// serialized for display.
pub fn run_line(executor: &mut Executor, line: &str) -> Result<String, CliError> {
    let branch = Some(executor.default_branch().to_owned());
    let space = Some(executor.default_space().to_owned());
    let ParsedLine { command, format } = match command_from_line(line, branch, space) {
        Ok(parsed) => parsed,
        Err(message) => return Ok(message),
    };
    // The line's own format over the session's (#3326); the playground's
    // session is a human one.
    let format = format.unwrap_or(Format::Human);
    let invocation = Invocation::of(&command, format)?;
    match executor.execute(command) {
        Ok(output) => Ok(render_output(&output, &invocation, format)?.stdout_then_stderr()),
        Err(error) => Ok(render::error_line(error.status(), format)),
    }
}

/// Maps a parsed top-level command to an executor [`Command`] for the embedded
/// surface — the data primitives plus read-only status commands. Host commands
/// (filesystem, sockets, or model providers) are refused with a plain message.
fn command_to_executor(command: options::TopCommand, scope: &Scope) -> Result<Command, CliError> {
    use options::TopCommand as Top;
    if let Some(name) = deferred_top_command(&command) {
        return Err(deferred_command(name));
    }
    Ok(match command {
        Top::Ping => Command::Ping {},
        Top::Remote => Command::RemoteGet {},
        Top::Info => Command::Info {
            branch: scope.branch.clone(),
        },
        Top::Health => Command::Health {
            branch: scope.branch.clone(),
        },
        Top::Metrics => Command::Metrics {
            branch: scope.branch.clone(),
        },
        Top::Describe => Command::Describe {
            branch: scope.branch.clone(),
        },
        Top::Config(args) => {
            if matches!(
                args.command,
                ConfigCommand::Get | ConfigCommand::GetKey { .. }
            ) {
                config_command(args.command)
            } else {
                return Err(CliError::usage(
                    "`config set`/`unset`/`show`/`path` manage on-disk user config and are not available in an embedded session",
                ));
            }
        }
        Top::Branch(args) => branch_command(args.command)?,
        Top::Space(args) => space_command(args.command, scope),
        Top::Kv(args) => kv_command(args.command, scope)?,
        Top::Json(args) => json_command(args.command, scope)?,
        Top::Vector(args) => vector_command(args.command, scope)?,
        Top::Event(args) => event_command(args.command, scope)?,
        Top::Graph(args) => graph_command(args.command, scope)?,
        Top::Arrow(args) => arrow_command(args.command, scope),
        Top::Command(args) => raw_command(args.command)?,
        Top::Hub(_) => {
            return Err(CliError::usage(
                "`hub` needs a host environment and is not available in an embedded session",
            ));
        }
        _ => {
            return Err(CliError::usage(
                "that command needs a host environment (filesystem, sockets, or model providers) and is not available in an embedded session",
            ));
        }
    })
}

/// CLI error. Public because the wasm-safe surface (`output_to_string`,
/// `value_to_string`) returns it; `#[non_exhaustive]` per the error contract.
#[derive(Debug)]
#[non_exhaustive]
pub enum CliError {
    /// Invalid usage: a bad argument, an unsupported command, or a refusal.
    Usage(String),
    /// A filesystem or stdio error from a host operation.
    Io(std::io::Error),
    /// A JSON parse or serialization error.
    Json(serde_json::Error),
    /// An error returned by the executor while running a command.
    Executor(Box<ExecutorError>),
}

impl CliError {
    fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => formatter.write_str(message),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Executor(error) => write!(formatter, "{}", error.message()),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ExecutorError> for CliError {
    fn from(value: ExecutorError) -> Self {
        Self::Executor(Box::new(value))
    }
}

// Stacked gate: see `config_key_tests`.
#[cfg(test)]
#[cfg(feature = "native")]
mod tests {
    use super::*;

    #[test]
    fn dataset_notice_is_silent_for_an_empty_directory() {
        // Kills the emptiness-guard mutant (#3001 gate find): no datasets ⇒
        // no notice; datasets ⇒ all named, first one in the command hint.
        assert_eq!(contained_datasets_notice(&[]), None);
        let found = vec!["alpha".to_owned(), "zeta".to_owned()];
        let notice = contained_datasets_notice(&found).expect("datasets get a notice");
        assert!(
            notice.contains("alpha, zeta") && notice.contains("`strata ./alpha`"),
            "{notice}"
        );
    }
    use options::{
        ArrowCommand, CliArrowExportPrimitive, CliArrowFormat, CommandCommand, KvCommand,
        TopCommand,
    };
    use std::path::PathBuf;

    /// Every deferred verb must keep producing its named refusal — kills
    /// arm-deletion mutants in `deferred_top_command` (`uninstall` left this
    /// list when #2995 implemented it).
    #[test]
    fn deferred_verbs_stay_mapped_to_their_refusals() {
        for verb in [
            "search", "recipe", "txn", "begin", "commit", "rollback", "flush", "compact", "up",
            "down",
        ] {
            let cli = Cli::try_parse_from(["strata", verb]).expect("deferred verb parses");
            let command = cli.command.expect("verb present");
            assert_eq!(deferred_top_command(&command), Some(verb), "verb {verb}");
        }
    }

    #[test]
    fn parses_direct_database_path_before_subcommand() {
        let cli = Cli::parse_from(["strata", "./db", "kv", "put", "hello", "world"]);
        assert_eq!(cli.db_path, Some(PathBuf::from("./db")));
        assert!(matches!(
            cli.command,
            Some(TopCommand::Kv(options::KvArgs {
                command: KvCommand::Put { key, value: Some(value), file: None },
            })) if key == "hello" && value == "world"
        ));
    }

    #[test]
    fn parses_db_flag_and_scope() {
        let cli = Cli::parse_from([
            "strata", "--db", "./db", "--branch", "feature", "--space", "app", "kv", "get", "hello",
        ]);
        assert_eq!(cli.db, Some(PathBuf::from("./db")));
        assert_eq!(cli.branch.as_deref(), Some("feature"));
        assert_eq!(cli.space.as_deref(), Some("app"));
    }

    #[test]
    fn parses_no_command_for_shell_mode() {
        let cli = Cli::parse_from(["strata", "--cache"]);
        assert!(cli.cache);
        assert!(cli.command.is_none());
    }

    #[test]
    fn parses_output_flags() {
        let cli = Cli::parse_from(["strata", "--json", "--cache", "ping"]);
        assert_eq!(cli.output_format(), options::Format::Json);

        let cli = Cli::parse_from(["strata", "--raw", "--cache", "ping"]);
        assert_eq!(cli.output_format(), options::Format::Raw);
    }

    #[test]
    fn parses_clone_progress_jsonl() {
        let cli = Cli::parse_from([
            "strata",
            "--json",
            "clone",
            "titanic",
            "--branch",
            "main",
            "--progress",
            "jsonl",
        ]);
        assert_eq!(cli.output_format(), options::Format::Json);
        assert!(matches!(
            cli.command,
            Some(TopCommand::Clone(options::CloneArgs {
                dataset,
                branch: Some(branch),
                progress: Some(CloneProgressFormat::Jsonl),
                ..
            })) if dataset == "titanic" && branch == "main"
        ));
    }

    #[test]
    fn parses_hub_list_datasets_filters() {
        let cli = Cli::parse_from([
            "strata",
            "--json",
            "hub",
            "list-datasets",
            "--hub",
            "https://hub.example.test",
            "--task",
            "classification",
            "--tag",
            "tabular",
            "--primitive",
            "kv",
            "--sort",
            "downloads",
            "--limit",
            "20",
            "--offset",
            "40",
        ]);
        assert_eq!(cli.output_format(), options::Format::Json);
        assert!(matches!(
            cli.command,
            Some(TopCommand::Hub(options::HubArgs {
                command: HubCommand::ListDatasets(options::HubListDatasetsArgs {
                    hub: Some(hub),
                    tasks,
                    tags,
                    primitives,
                    sort: Some(options::HubDatasetSortArg::Downloads),
                    limit: Some(20),
                    offset: Some(40),
                    ..
                }),
            })) if hub == "https://hub.example.test"
                && tasks == vec!["classification"]
                && tags == vec!["tabular"]
                && primitives == vec!["kv"]
        ));
    }

    #[test]
    fn top_level_hub_command_executes_through_the_host_runner() {
        let cli = Cli::parse_from(["strata", "hub", "info", "--hub", "not-a-url"]);
        let error = execute(cli).expect_err("bad hub URL surfaces through run_hub");
        let CliError::Executor(error) = error else {
            panic!("expected executor error, got {error:?}");
        };
        assert_eq!(error.status().code(), "invalid_argument.executor.hub_url");
    }

    #[test]
    fn parses_arrow_file_format_without_colliding_with_output_format() {
        let cli = Cli::parse_from([
            "strata",
            "--json",
            "--cache",
            "arrow",
            "export",
            "--primitive",
            "kv",
            "--format",
            "jsonl",
            "out.jsonl",
        ]);
        assert_eq!(cli.output_format(), options::Format::Json);
        assert!(matches!(
            cli.command,
            Some(TopCommand::Arrow(options::ArrowArgs {
                command: ArrowCommand::Export {
                    primitive: CliArrowExportPrimitive::Kv,
                    format: CliArrowFormat::Jsonl,
                    path,
                    ..
                },
            })) if path == "out.jsonl"
        ));
    }

    #[test]
    fn parses_raw_command_json_without_colliding_with_output_json() {
        let cli = Cli::parse_from([
            "strata",
            "--json",
            "--cache",
            "command",
            "run",
            "--command-json",
            r#"{"type":"ping"}"#,
        ]);
        assert_eq!(cli.output_format(), options::Format::Json);
        assert!(matches!(
            cli.command,
            Some(TopCommand::Command(options::CommandArgs {
                command: CommandCommand::Run {
                    json: Some(json),
                    file: None,
                },
            })) if json == r#"{"type":"ping"}"#
        ));
    }

    #[test]
    fn parses_delete_alias() {
        let cli = Cli::parse_from(["strata", "--cache", "kv", "del", "hello"]);
        assert!(matches!(
            cli.command,
            Some(TopCommand::Kv(options::KvArgs {
                command: KvCommand::Delete { key },
            })) if key == "hello"
        ));
    }

    #[test]
    fn kv_put_reads_file_value() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let path = temp.path().join("value.bin");
        std::fs::write(&path, b"from-file").expect("write value");
        let command = kv_command(
            KvCommand::Put {
                key: "hello".to_owned(),
                value: None,
                file: Some(path),
            },
            &Scope::default(),
        )
        .expect("kv command");

        let Command::KvPut { value, .. } = command else {
            panic!("expected kv put");
        };
        assert_eq!(value.as_slice(), b"from-file");
    }

    #[test]
    fn deferred_top_level_command_returns_usage_error() {
        assert_eq!(run(["strata", "--cache", "search"]), 2);
    }

    #[test]
    fn command_from_line_maps_each_supported_family() {
        let cmd = |line: &str| command_from_line(line, None, None).expect(line).command;
        assert!(matches!(cmd("ping"), Command::Ping {}));
        assert!(matches!(cmd("remote"), Command::RemoteGet {}));
        assert!(matches!(cmd("info"), Command::Info { .. }));
        assert!(matches!(cmd("health"), Command::Health { .. }));
        assert!(matches!(cmd("metrics"), Command::Metrics { .. }));
        assert!(matches!(cmd("describe"), Command::Describe { .. }));
        assert!(matches!(cmd("config get"), Command::ConfigGet {}));
        assert!(matches!(cmd("branch list"), Command::BranchList {}));
        assert!(matches!(cmd("space list"), Command::SpaceList { .. }));
        assert!(matches!(cmd("kv put a b"), Command::KvPut { .. }));
        assert!(matches!(cmd("json list"), Command::JsonList { .. }));
        assert!(matches!(
            cmd("vector collection list"),
            Command::VectorListCollections { .. }
        ));
        assert!(matches!(cmd("event count"), Command::EventCount { .. }));
        assert!(matches!(cmd("graph list"), Command::GraphList { .. }));
        assert!(matches!(
            cmd("arrow export --primitive kv --format jsonl out.jsonl"),
            Command::ArrowExport { .. }
        ));
        assert!(matches!(
            cmd(r#"command run --command-json '{"type":"ping"}'"#),
            Command::Ping {}
        ));
    }

    #[test]
    fn command_from_line_refuses_host_and_deferred_commands() {
        // Host-only commands (filesystem/sockets/process) are refused, not run.
        assert!(command_from_line("mcp serve", None, None).is_err());
        assert!(command_from_line("init", None, None).is_err());
        let error = command_from_line("hub info", None, None).expect_err("hub is host-only");
        assert!(
            error.contains("`hub` needs a host environment"),
            "unexpected hub refusal: {error}"
        );
        // Config reads are allowed; config mutations are host-only.
        assert!(command_from_line("config get", None, None).is_ok());
        assert!(command_from_line("config set hub.url http://example", None, None).is_err());
        // Deferred (old-CLI) commands are refused.
        assert!(command_from_line("search foo", None, None).is_err());
        // A bad flag on a real command surfaces the clap parse error text.
        assert!(command_from_line("kv get k --nope", None, None).is_err());
    }

    #[test]
    fn command_from_line_applies_session_scope_and_per_command_override() {
        // The session scope fills in when the command omits --branch.
        let Command::KvGet { branch, .. } =
            command_from_line("kv get k", Some("feature".to_owned()), None)
                .expect("kv get")
                .command
        else {
            panic!("expected kv get");
        };
        assert_eq!(branch.as_deref(), Some("feature"));

        // A per-command --branch overrides the supplied session scope.
        let Command::KvGet { branch, .. } =
            command_from_line("kv get k --branch other", Some("feature".to_owned()), None)
                .expect("kv get override")
                .command
        else {
            panic!("expected kv get");
        };
        assert_eq!(branch.as_deref(), Some("other"));
    }

    #[test]
    fn command_from_line_carries_the_output_format_its_flags_chose() {
        // #3312: the format is part of the parse, so an embedded session
        // renders `--json`/`--raw` the way the binary does instead of
        // dropping the flag on the floor; #3326: a line that chose nothing
        // says so, and the session decides.
        let format = |line: &str| command_from_line(line, None, None).expect(line).format;
        assert_eq!(format("kv get k"), None);
        assert_eq!(format("--json kv get k"), Some(Format::Json));
        assert_eq!(format("kv get k --json"), Some(Format::Json));
        assert_eq!(format("--raw kv get k"), Some(Format::Raw));
        // #3345: human has a name too, so a line in a `--json` session can
        // ask for it instead of inheriting the session.
        assert_eq!(format("--human kv get k"), Some(Format::Human));
        // Q5 (#3314 S4): the hidden flag is gone; so is the format it named.
        assert!(command_from_line("--output-format pretty kv get k", None, None).is_err());
        // Conflicting flags are a parse error, as on the command line.
        assert!(command_from_line("--json --raw kv get k", None, None).is_err());
    }

    #[test]
    fn line_refusal_names_each_session_argument() {
        // #3327: the truth table on the shared helper — every session
        // argument the grammar accepts on a line is named back, by kind.
        use options::LineRefusal;
        let refusal = |line: &str| {
            let argv = std::iter::once("strata").chain(line.split_whitespace());
            Cli::try_parse_from(argv).expect(line).line_refusal()
        };
        let flag = |line: &str| match refusal(line) {
            Some(LineRefusal::SessionFlag { flag, .. }) => flag,
            other => panic!("{line}: expected a session-flag refusal, got {other:?}"),
        };
        // (Session flags are not clap globals, so after the verb they are
        // already an unknown-argument parse error; only the leading position
        // parses and needs refusing.)
        assert_eq!(flag("--db /elsewhere kv get k"), "--db");
        assert_eq!(flag("--cache kv get k"), "--cache");
        assert_eq!(flag("--durability always kv put k v"), "--durability");
        assert_eq!(flag("--ipc client kv get k"), "--ipc");
        assert_eq!(flag("--read-only kv put k v"), "--read-only");
        // A session flag with no command at all is still the flag's refusal.
        assert_eq!(flag("--cache"), "--cache");
        // The positional database path: a command behind it is a path
        // refusal; a lone word is a typo'd verb.
        assert_eq!(
            refusal("/elsewhere kv get k"),
            Some(LineRefusal::DatabasePath("/elsewhere".to_owned()))
        );
        assert_eq!(
            refusal("foo"),
            Some(LineRefusal::NotACommand("foo".to_owned()))
        );
        // Direction control: per-command globals and plain commands pass.
        for line in [
            "kv get k",
            "--branch feature kv get k",
            "--space s kv get k",
            "--json kv get k",
            "--raw kv get k",
            "--human kv get k",
            "--json",
        ] {
            assert_eq!(refusal(line), None, "{line} must not be refused");
        }
    }

    #[test]
    fn line_refusals_name_the_argument_and_the_new_session() {
        // Each refusal's text carries what the reader needs: the argument
        // that was refused and, for a session argument, the invocation that
        // opens a session with it.
        use options::LineRefusal;
        let flag = LineRefusal::SessionFlag {
            flag: "--db",
            usage: "--db <PATH>",
        }
        .to_string();
        assert!(
            flag.contains("`--db`") && flag.contains("`strata --db <PATH> <command>`"),
            "{flag}"
        );
        let path = LineRefusal::DatabasePath("/elsewhere".to_owned()).to_string();
        assert!(
            path.contains("`/elsewhere`") && path.contains("`strata /elsewhere <command>`"),
            "{path}"
        );
        let word = LineRefusal::NotACommand("foo".to_owned()).to_string();
        assert!(word.contains("`foo`") && word.contains("`help`"), "{word}");
    }

    #[test]
    fn command_from_line_refuses_session_arguments() {
        // #3327 at the call site: the playground's parser returns the refusal
        // as the text to display, naming the argument, instead of dropping
        // it and parsing a command that would run against the session.
        for (line, named) in [
            ("--db /elsewhere kv get k", "`--db`"),
            ("--cache kv get k", "`--cache`"),
            ("--durability always kv put k v", "`--durability`"),
            ("--ipc off kv get k", "`--ipc`"),
            ("--read-only kv put k v", "`--read-only`"),
            ("/elsewhere kv get k", "`/elsewhere`"),
            ("foo", "`foo`"),
        ] {
            let error = command_from_line(line, None, None).expect_err(line);
            assert!(
                error.contains(named),
                "{line}: refusal must name {named}: {error}"
            );
        }
        // Direction control: the per-command globals still parse.
        assert!(
            command_from_line("--branch feature --space s --json kv get k", None, None).is_ok()
        );
    }

    #[test]
    fn run_line_refuses_session_arguments_instead_of_answering_from_the_session() {
        // The user-visible defect: `--db /elsewhere kv get greeting` answered
        // `hello` from the session's database; `--read-only kv put` wrote.
        let mut executor = Executor::open_cache().expect("cache executor opens");
        let run = |executor: &mut Executor, line: &str| run_line(executor, line).expect(line);
        assert_eq!(
            run(&mut executor, "kv put greeting hello"),
            "created greeting\n"
        );
        let refused = run(&mut executor, "--db /elsewhere kv get greeting");
        assert!(refused.contains("`--db`"), "{refused}");
        let refused = run(&mut executor, "--read-only kv put greeting changed");
        assert!(refused.contains("`--read-only`"), "{refused}");
        assert_eq!(
            run(&mut executor, "kv get greeting"),
            "hello\n",
            "the refused write must not have happened"
        );
        let refused = run(&mut executor, "descrbe");
        assert!(refused.contains("`descrbe`"), "{refused}");
    }

    #[test]
    fn run_line_renders_in_the_format_the_line_chose() {
        // The playground's path: each format's channel text, as the binary
        // would print it (stdout ⧺ stderr; JSON envelopes newline-terminated).
        let mut executor = Executor::open_cache().expect("cache executor opens");
        let run = |executor: &mut Executor, line: &str| run_line(executor, line).expect(line);
        assert_eq!(
            run(&mut executor, "kv put greeting hello"),
            "created greeting\n"
        );
        // #3116: the stored value and nothing else — `run_line` answers one
        // command the way the binary does, and a file must hold the bytes
        // alone. A shared stream (the REPL, a pipe) adds its own separator.
        assert_eq!(run(&mut executor, "--raw kv get greeting"), "hello");
        // A missed write is its stderr feedback line after an empty stdout,
        // in human and raw alike; `--json` keeps the envelope on stdout.
        assert_eq!(run(&mut executor, "kv delete nope"), "no such key: nope\n");
        assert_eq!(
            run(&mut executor, "--raw kv delete nope"),
            "no such key: nope\n"
        );
        let json = run(&mut executor, "--json kv delete nope");
        let envelope: serde_json::Value = serde_json::from_str(&json).expect("compact JSON");
        assert_eq!(envelope["data"]["effect"]["kind"], "not_found");
        let json = run(&mut executor, "--json kv get greeting");
        assert!(json.ends_with('\n'), "JSON is newline-terminated: {json:?}");
        let envelope: serde_json::Value = serde_json::from_str(&json).expect("compact JSON");
        assert_eq!(envelope["type"], "kv_versioned_value");
        assert_eq!(envelope["data"]["value"]["value"], "aGVsbG8=");
        // A command failure is the error line in the chosen format, not an Err.
        let missing = run(&mut executor, "--json branch get nope");
        let envelope: serde_json::Value = serde_json::from_str(&missing).expect("error envelope");
        assert_eq!(envelope["error"]["code"], "not_found.engine.branch");
        let missing = run(&mut executor, "branch get nope");
        assert!(
            missing.starts_with("not_found.engine.branch:") && missing.ends_with('\n'),
            "human error line: {missing:?}"
        );
        // Parse errors and host-only refusals are the text to display.
        assert!(run(&mut executor, "kv put").contains("Usage: strata kv put"));
        assert!(run(&mut executor, "hub info").contains("needs a host environment"));
    }

    #[test]
    fn interactive_sessions_default_to_host_piped_to_client() {
        // The default session mode when `--ipc` is unset: a TTY REPL hosts so
        // other processes can attach; a piped stream brokers as a client and
        // never hosts. (TTY detection can't be faked in-process, so the policy
        // is pinned here rather than through the binary.)
        assert_eq!(default_session_ipc(true), IpcMode::Host);
        assert_eq!(default_session_ipc(false), IpcMode::Client);
    }

    /// The MCP entry applies the session scope before it ever touches
    /// stdio: a reserved session branch must fail the serve loudly (and a
    /// stubbed exit code would sail past this expectation).
    #[test]
    fn serve_mcp_rejects_a_reserved_session_branch_before_serving() {
        let connection = Connection::cache(Executor::open_cache().expect("cache executor opens"));
        let context = CommandContext::new(Some("_reserved".to_owned()), None);
        assert!(
            serve_mcp(connection, &context).is_err(),
            "a reserved session branch must refuse the MCP serve"
        );
    }

    #[test]
    fn run_executes_durable_kv_round_trip() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let db = temp.path().to_string_lossy().to_string();

        assert_eq!(
            run(["strata", "--db", db.as_str(), "kv", "put", "hello", "world"]),
            0
        );
        assert_eq!(
            run(["strata", "--db", db.as_str(), "kv", "get", "hello"]),
            0
        );
    }

    /// The answer at the terminal: an explicit `y` — either case, surrounding
    /// whitespace tolerated — and nothing else. `n`, `yes`, an empty line and
    /// a closed stdin all decline. The refusal and the question reach the
    /// prompt stream first, and a prompt stream that cannot be written loses
    /// the question, not the answer.
    #[cfg(all(feature = "native", feature = "inference"))]
    #[test]
    fn only_an_explicit_yes_at_the_terminal_accepts_the_download() {
        use super::ask_for_download;

        struct Broken;
        impl std::io::Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }

        let refusal = ExecutorError::new("inference.missing_model", "miniLM is not on disk");
        let mut prompt = Vec::new();
        assert!(ask_for_download(
            &refusal,
            "miniLM",
            &b"y\n"[..],
            &mut prompt
        ));
        let prompt = String::from_utf8(prompt).expect("the prompt is text");
        let (shown, question) = prompt
            .split_once("\nDownload miniLM now? [y/N] ")
            .unwrap_or_else(|| panic!("no question in {prompt:?}"));
        assert!(shown.contains(refusal.code()), "{shown:?}");
        assert_eq!(question, "");

        for yes in ["Y\n", "  y  \n", "y"] {
            assert!(
                ask_for_download(&refusal, "miniLM", yes.as_bytes(), Vec::new()),
                "{yes:?} is a yes"
            );
        }
        for no in ["n\n", "N\n", "yes\n", "\n", ""] {
            assert!(
                !ask_for_download(&refusal, "miniLM", no.as_bytes(), Vec::new()),
                "{no:?} is a no"
            );
        }
        assert!(ask_for_download(&refusal, "miniLM", &b"y\n"[..], Broken));
        assert!(!ask_for_download(&refusal, "miniLM", &b"n\n"[..], Broken));
    }

    /// D8's truth table: every condition guards a different mistake.
    #[cfg(all(feature = "native", feature = "inference"))]
    #[test]
    fn the_download_offer_needs_a_terminal_human_output_and_a_model_to_pull() {
        use super::offerable_download;
        use crate::options::Format;
        use strata_executor::{Command, InferenceAvailabilityDetails, InferenceAvailabilityKind};

        let answer = |availability: InferenceAvailabilityKind,
                      pull_spec: Option<&str>|
         -> InferenceAvailabilityDetails {
            serde_json::from_value(serde_json::json!({
                "model": "miniLM",
                "provider": "local",
                "availability": availability,
                "pull_spec": pull_spec,
            }))
            .expect("a resolver answer")
        };
        let not_downloaded = answer(InferenceAvailabilityKind::NotDownloaded, Some("miniLM"));
        let tokenize = Command::InferenceTokenize {
            model: "miniLM".to_owned(),
            text: "hi".to_owned(),
            add_special: false,
        };

        // The one case that offers, and it names what a pull accepts.
        assert_eq!(
            offerable_download(true, Format::Human, &tokenize, Some(&not_downloaded)),
            Some("miniLM")
        );

        // Not a terminal: an agent cannot answer, so it must get the refusal
        // (which already names the pull command) instead of a hidden fetch.
        assert_eq!(
            offerable_download(false, Format::Human, &tokenize, Some(&not_downloaded)),
            None
        );

        // Machine-readable output: a prompt would corrupt the stream even with
        // a human watching.
        for format in [Format::Json, Format::Raw] {
            assert_eq!(
                offerable_download(true, format, &tokenize, Some(&not_downloaded)),
                None,
                "{format:?} is parsed, so it must not be interrupted"
            );
        }

        // A refused pull carries the same answer; running it again would
        // meet the same refusal.
        let pull = Command::InferenceModelsPull {
            model: "miniLM".to_owned(),
        };
        assert_eq!(
            offerable_download(true, Format::Human, &pull, Some(&not_downloaded)),
            None
        );

        // Any other answer is not fixed by downloading. Offering here would be
        // a wrong suggestion, which is worse than none — and for a name the
        // catalog does not know, an offer to pull it (#3226).
        for availability in [
            InferenceAvailabilityKind::Ready,
            InferenceAvailabilityKind::NotInCatalog,
            InferenceAvailabilityKind::PathMissing,
            InferenceAvailabilityKind::TaskNotSupported,
            InferenceAvailabilityKind::LocalExecutionNotBuilt,
            InferenceAvailabilityKind::ProviderNotBuilt,
            InferenceAvailabilityKind::NetworkDisabled,
            InferenceAvailabilityKind::KeyMissing,
        ] {
            let other = answer(availability, Some("miniLM"));
            assert_eq!(
                offerable_download(true, Format::Human, &tokenize, Some(&other)),
                None,
                "{availability:?} is not fixed by downloading"
            );
        }

        // An answer with nothing to pull, and a failure with no answer at all
        // (a provider timeout, a branch that does not exist).
        let unpullable = answer(InferenceAvailabilityKind::NotDownloaded, None);
        assert_eq!(
            offerable_download(true, Format::Human, &tokenize, Some(&unpullable)),
            None
        );
        assert_eq!(
            offerable_download(true, Format::Human, &tokenize, None),
            None
        );
    }

    /// A world where `miniLM` is catalogued but not on disk, for the offer
    /// loop's call-site tests.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    fn undownloaded_world() -> Connection {
        use strata_executor::FakeInferenceService;

        Connection::cache(
            Executor::open_cache()
                .expect("cache executor opens")
                .with_inference_runtime(FakeInferenceService::new().with_undownloaded("miniLM")),
        )
    }

    /// A command that loads `model` and nothing else.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    fn tokenize_with(model: &str) -> strata_executor::Command {
        strata_executor::Command::InferenceTokenize {
            model: model.to_owned(),
            text: "hi".to_owned(),
            add_special: false,
        }
    }

    /// The code of the executor refusal the offer loop handed back.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    fn refusal_code(error: CliError) -> String {
        match error {
            CliError::Executor(error) => error.code().to_owned(),
            other => panic!("not an executor refusal: {other:?}"),
        }
    }

    /// The answer a test gives in place of a person at the terminal when no
    /// question may be asked.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    fn never_asked(error: &ExecutorError, _: &str) -> bool {
        panic!("asked without a terminal: {}", error.code())
    }

    /// The offer loop end to end against the fake runtime: a person at a
    /// terminal meets the refusal, one question, the pull and the retry; the
    /// answer decides; no terminal meets the refusal alone.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    #[test]
    fn the_offer_pulls_the_model_the_refusal_names_and_retries() {
        use super::execute_offering_download;
        use crate::options::Format;
        use strata_executor::Output;

        let connection = undownloaded_world();
        let tokenize = tokenize_with("miniLM");

        // Not a terminal: the refusal, and no question.
        let error = execute_offering_download(
            &connection,
            tokenize.clone(),
            Format::Human,
            false,
            &mut never_asked,
        )
        .expect_err("not on disk");
        assert_eq!(refusal_code(error), "inference.missing_model");

        // A terminal, and the answer is no: the refusal, asked once.
        let mut asked = Vec::new();
        let mut decline = |error: &ExecutorError, pull_spec: &str| -> bool {
            asked.push((error.code().to_owned(), pull_spec.to_owned()));
            false
        };
        let error = execute_offering_download(
            &connection,
            tokenize.clone(),
            Format::Human,
            true,
            &mut decline,
        )
        .expect_err("declined");
        assert_eq!(refusal_code(error), "inference.missing_model");
        assert_eq!(
            asked,
            [("inference.missing_model".to_owned(), "miniLM".to_owned())]
        );

        // A terminal, and the answer is yes: pulled, retried, done — and the
        // model is present for everything after.
        let mut asked = Vec::new();
        let mut accept = |_: &ExecutorError, pull_spec: &str| -> bool {
            asked.push(pull_spec.to_owned());
            true
        };
        let output = execute_offering_download(
            &connection,
            tokenize.clone(),
            Format::Human,
            true,
            &mut accept,
        )
        .expect("pulled and retried");
        assert!(matches!(output, Output::InferenceTokenIds(_)), "{output:?}");
        assert_eq!(asked, ["miniLM"]);
        let output =
            execute_offering_download(&connection, tokenize, Format::Human, true, &mut never_asked)
                .expect("present now");
        assert!(matches!(output, Output::InferenceTokenIds(_)), "{output:?}");
    }

    /// #3235: the size the refusal quotes is the size `models list` shows for
    /// the same bytes — one formatter, read at both sites, not a decimal one
    /// in the refusal and a binary one in the table. The fake lists only its
    /// own zero-sized models, so the table row is built from the byte count
    /// the refusal itself reports.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    #[test]
    fn the_refusal_quotes_the_size_models_list_shows() {
        use super::execute_offering_download;
        use crate::options::Format;

        let error = execute_offering_download(
            &undownloaded_world(),
            tokenize_with("miniLM"),
            Format::Human,
            false,
            &mut never_asked,
        )
        .expect_err("not on disk");
        let CliError::Executor(refusal) = error else {
            panic!("not an executor refusal: {error:?}")
        };
        assert_eq!(refusal.code(), "inference.missing_model");
        let size_bytes = refusal
            .inference_availability()
            .and_then(|details| details.size_bytes)
            .expect("a catalogued variant has a size");

        // Through the renderer the binary uses: `models list` is a
        // `display: bespoke` command, so its table comes from its own arm.
        let listed: strata_executor::Output = serde_json::from_value(serde_json::json!({
            "type": "inference_models",
            "data": { "items": [{
                "name": "miniLM", "task": "embed", "architecture": "bert",
                "default_quant": "f16", "is_local": false, "runnable": true,
                "size_bytes": size_bytes, "embedding_dim": 384,
                "hf_repo": "sentence-transformers/all-MiniLM-L6-v2", "local_path": null
            }], "cursor": null, "has_more": false }
        }))
        .expect("the wire deserializes");
        let table = crate::render::render_output(
            &listed,
            &crate::render::Invocation::none(),
            Format::Human,
        )
        .expect("renders")
        .stdout
        .text();
        let size = table.trim_end().rsplit('\t').next().expect("a size column");
        assert!(
            size.ends_with(" MB"),
            "a catalogued size, not `-`: {table:?}"
        );
        assert!(
            refusal.message().contains(&format!("({size}, ")),
            "the refusal quotes {size:?}: {:?}",
            refusal.message()
        );
    }

    /// The prompt a person reads before answering names only commands the
    /// clap tree parses — the refusal's own `strata inference models pull …`
    /// line, and anything else it quotes (#3261, the `strata models list`
    /// class of #3256). Checked on the real refusal the offer loop shows,
    /// rendered by the same function that shows it.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    #[test]
    fn the_prompt_names_only_commands_the_clap_tree_parses() {
        use super::{ask_for_download, execute_offering_download};
        use crate::options::tests::{commands_that_do_not_parse, named_commands};
        use crate::options::Format;

        let mut prompt = Vec::new();
        let mut decline = |error: &ExecutorError, pull_spec: &str| -> bool {
            ask_for_download(error, pull_spec, &b"n\n"[..], &mut prompt)
        };
        let error = execute_offering_download(
            &undownloaded_world(),
            tokenize_with("miniLM"),
            Format::Human,
            true,
            &mut decline,
        )
        .expect_err("declined");
        assert_eq!(refusal_code(error), "inference.missing_model");

        let prompt = String::from_utf8(prompt).expect("the prompt is text");
        let named = named_commands(&prompt);
        assert!(
            named.contains(&"strata inference models pull miniLM".to_owned()),
            "the prompt names the pull: {named:?}\n{prompt}"
        );
        let failures = commands_that_do_not_parse(
            named
                .into_iter()
                .map(|command| ("prompt".to_owned(), command)),
        );
        assert!(
            failures.is_empty(),
            "the prompt names commands the clap tree does not parse:\n{}\n{prompt}",
            failures.join("\n")
        );
    }

    /// The offer keys on the resolver's answer, not the command: `vector
    /// --text` loads the collection's recorded model and gets the same offer,
    /// and a name the catalog does not know gets none.
    #[cfg(all(feature = "native", feature = "inference", feature = "testkit"))]
    #[test]
    fn the_offer_names_the_collection_model_and_never_an_uncatalogued_one() {
        use super::execute_offering_download;
        use crate::options::Format;
        use strata_executor::{Command, Output, VectorDistanceMetric};

        // `vector --text`: the model comes from the collection's record, not
        // the command line, and the offer still names it.
        let connection = undownloaded_world();
        connection
            .execute(Command::VectorCreateCollection {
                branch: None,
                space: None,
                collection: "docs".to_owned(),
                dimension: 8,
                metric: VectorDistanceMetric::Cosine,
                embedding_model: Some("miniLM".to_owned()),
            })
            .expect("collection records the model");
        let upsert = Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            vector: Vec::new(),
            text: Some("a document to embed".to_owned()),
            metadata: None,
        };
        let mut asked = Vec::new();
        let mut accept = |error: &ExecutorError, pull_spec: &str| -> bool {
            asked.push((error.code().to_owned(), pull_spec.to_owned()));
            true
        };
        let output =
            execute_offering_download(&connection, upsert, Format::Human, true, &mut accept)
                .expect("pulled and retried");
        assert!(
            matches!(output, Output::VectorWriteResult { .. }),
            "{output:?}"
        );
        assert_eq!(
            asked,
            [("inference.missing_model".to_owned(), "miniLM".to_owned())]
        );

        // A name the catalog does not know is refused without a question:
        // there is nothing a pull would accept (#3226).
        let error = execute_offering_download(
            &connection,
            tokenize_with("nope"),
            Format::Human,
            true,
            &mut never_asked,
        )
        .expect_err("unknown");
        assert_eq!(refusal_code(error), "inference.unknown_model");
    }

    /// A one-shot `inference` command falls back to the ephemeral cache when
    /// no target is named; every other one-shot keeps the refusal (R9).
    #[cfg(all(feature = "native", feature = "inference"))]
    #[test]
    fn only_an_inference_one_shot_may_run_without_a_target() {
        use open::OpenIntent;

        let command = |line: &str| {
            Cli::try_parse_from(line.split(' '))
                .expect("parses")
                .command
                .expect("a command")
        };
        assert_eq!(
            one_shot_intent(&command("strata inference status")),
            OpenIntent::InferenceOneShot
        );
        assert_eq!(
            one_shot_intent(&command("strata inference models list")),
            OpenIntent::InferenceOneShot
        );
        // Data commands, and `vector upsert --text` that embeds on the way
        // in, keep the refusal: they write somewhere.
        for line in [
            "strata kv get k",
            "strata vector upsert docs d1 --text hello",
            "strata admin ping",
        ] {
            assert_eq!(
                one_shot_intent(&command(line)),
                OpenIntent::OneShot,
                "{line}"
            );
        }
    }
}
