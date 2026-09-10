//! Database open helpers.

use std::path::{Path, PathBuf};

use strata_executor::ipc::{Connection, SessionAccess};
use strata_executor::{Executor, IpcMode};

use crate::CliError;

/// How the invocation intends to use the database (first-run D2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenIntent {
    /// A single command: refuses to run without an explicit target.
    OneShot,
    /// A single `inference` command (R9 of #3261): it works on models, not on
    /// a database's data, so with no target it runs in an ephemeral cache
    /// session instead of refusing — `strata inference status` answers from
    /// any directory. An explicit target is honored exactly as for
    /// [`OneShot`](Self::OneShot); the current directory is never opened.
    InferenceOneShot,
    /// An interactive TTY session: falls back to an ephemeral cache session.
    Interactive,
    /// A piped (non-TTY) session: refuses like one-shot commands — an agent
    /// streaming commands must never write to an implicit location.
    Pipe,
}

/// An opened connection plus how its target was chosen.
pub(crate) struct OpenedConnection {
    pub(crate) connection: Connection,
    /// True when a bare invocation fell back to cache mode; an interactive
    /// caller prints the nothing-is-persisted banner (an inference one-shot
    /// has nothing to announce: it persisted nothing).
    pub(crate) implicit_cache: bool,
    /// Set when a bare interactive invocation opened the database the current
    /// directory IS (#3000, the git model); the caller announces the path.
    pub(crate) implicit_cwd: Option<PathBuf>,
}

/// Resolves the database target: explicit flag/path, then the `STRATA_DB`
/// environment variable, then intent-specific fallback. A one-shot or piped
/// invocation with no target refuses with a teaching error instead of opening
/// the current directory implicitly — agents run commands from arbitrary
/// directories, and an accidental durable database in cwd is data loss
/// waiting to happen.
///
/// `ipc` selects the multi-process access policy for a durable open; cache
/// opens ignore it (cache mode is single-process by construction). `access`
/// is the session access for a durable open — `Read` requires a durable
/// target (an ephemeral cache session has nothing to protect).
pub(crate) fn open_connection(
    cache: bool,
    db_flag: Option<PathBuf>,
    db_path: Option<PathBuf>,
    durability: Option<strata_executor::DurabilityMode>,
    ipc: IpcMode,
    access: SessionAccess,
    intent: OpenIntent,
) -> Result<OpenedConnection, CliError> {
    if cache {
        if db_flag.is_some() || db_path.is_some() {
            return Err(CliError::usage(
                "`--cache` cannot be combined with `--db` or a database path",
            ));
        }
        return Ok(OpenedConnection {
            connection: Connection::cache(Executor::open_cache()?),
            implicit_cache: false,
            implicit_cwd: None,
        });
    }

    let mut implicit_cwd = None;
    let path = match (db_flag, db_path) {
        (Some(_), Some(_)) => {
            return Err(CliError::usage(
                "provide either `--db <path>` or positional database path, not both",
            ));
        }
        (Some(path), None) | (None, Some(path)) => Some(path),
        (None, None) => env_database_path().or_else(|| {
            // The git model (#3000): a bare interactive session standing
            // inside an EXISTING database opens it. Detection requires the
            // full durable layout, so this can never create anything — the
            // documented accidental-database hazard stays closed, and
            // one-shot/pipe intents keep refusing (their hint names the cwd
            // instead): agents never write to an implicit location.
            let target =
                implicit_interactive_target(intent, std::env::current_dir().ok().as_deref())?;
            implicit_cwd = Some(target.clone());
            Some(target)
        }),
    };

    if let Some(path) = path {
        let mut options = strata_executor::DurableLocalOpenOptions::new();
        if let Some(mode) = durability {
            options = options.with_durability(mode);
        }
        return Ok(OpenedConnection {
            connection: Connection::open_durable_local_brokered(path, options, ipc, access)?,
            implicit_cache: false,
            implicit_cwd,
        });
    }

    if durability.is_some() {
        return Err(CliError::usage(
            "`--durability` requires a durable database (a path or STRATA_DB)",
        ));
    }
    if access == SessionAccess::Read {
        return Err(CliError::usage(
            "`--read-only` requires a durable database (a path or STRATA_DB)",
        ));
    }

    match intent {
        OpenIntent::Interactive | OpenIntent::InferenceOneShot => Ok(OpenedConnection {
            connection: Connection::cache(Executor::open_cache()?),
            implicit_cache: true,
            implicit_cwd: None,
        }),
        OpenIntent::OneShot | OpenIntent::Pipe => {
            Err(no_database_refusal(std::env::current_dir().ok().as_deref()))
        }
    }
}

/// The bare interactive target (#3000): the current directory, exactly when
/// the session is interactive and the directory already IS a Strata database.
fn implicit_interactive_target(intent: OpenIntent, cwd: Option<&Path>) -> Option<PathBuf> {
    if intent != OpenIntent::Interactive {
        return None;
    }
    cwd.filter(|dir| is_strata_database_dir(dir))
        .map(Path::to_path_buf)
}

/// An existing Strata database directory is unmistakable: the durable layout's
/// top-level objects. Requiring all three means detection never guesses — and
/// can never create anything.
pub(crate) fn is_strata_database_dir(dir: &Path) -> bool {
    dir.join("manifest").is_dir() && dir.join("wal").is_dir() && dir.join("meta").is_dir()
}

/// Immediate child directories of `dir` that are Strata databases, sorted,
/// capped so a huge directory cannot flood a hint line.
pub(crate) fn strata_databases_in(dir: &Path) -> Vec<String> {
    const CAP: usize = 8;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<String> = entries
        .flatten()
        .filter(|entry| is_strata_database_dir(&entry.path()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    found.truncate(CAP);
    found
}

/// The bare one-shot/pipe refusal, aware of where it is standing (#3000): the
/// generic hint, plus the cwd dataset (or the datasets the cwd contains) when
/// that is evidently what the caller meant.
fn no_database_refusal(cwd: Option<&Path>) -> CliError {
    let mut message = String::from(
        "[invalid_argument.cli.no_database]: no database specified\n  hint: pass a path (strata ./mydb kv put …), set STRATA_DB, or use --cache for ephemeral",
    );
    if let Some(cwd) = cwd {
        if is_strata_database_dir(cwd) {
            message.push_str(
                "\n  note: the current directory is a Strata database — run `strata . <command>` to use it",
            );
        } else {
            let found = strata_databases_in(cwd);
            if !found.is_empty() {
                use std::fmt::Write as _;
                // Writing to a String is infallible.
                let _ = write!(
                    message,
                    "\n  note: Strata datasets here: {} — try `strata ./{} <command>`",
                    found.join(", "),
                    found[0]
                );
            }
        }
    }
    CliError::usage(message)
}

/// Reads the `STRATA_DB` fallback database target (empty means unset).
pub(crate) fn env_database_path() -> Option<PathBuf> {
    std::env::var_os("STRATA_DB")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{
        implicit_interactive_target, is_strata_database_dir, no_database_refusal,
        strata_databases_in, OpenIntent,
    };
    use std::fs;

    fn make_dataset(dir: &std::path::Path) {
        for object in ["manifest", "wal", "meta"] {
            fs::create_dir_all(dir.join(object)).expect("layout dir");
        }
    }

    #[test]
    fn detection_requires_the_full_layout() {
        let root = tempfile::tempdir().expect("tmp");
        let dataset = root.path().join("db");
        make_dataset(&dataset);
        assert!(is_strata_database_dir(&dataset));

        // Any missing layout object breaks detection — never guess.
        for object in ["manifest", "wal", "meta"] {
            let partial = root.path().join(format!("partial-{object}"));
            make_dataset(&partial);
            fs::remove_dir(partial.join(object)).expect("remove");
            assert!(
                !is_strata_database_dir(&partial),
                "must not detect without {object}/"
            );
        }

        // A FILE named like a layout dir does not count.
        let fake = root.path().join("fake");
        fs::create_dir_all(fake.join("wal")).expect("wal");
        fs::create_dir_all(fake.join("meta")).expect("meta");
        fs::write(fake.join("manifest"), b"not a dir").expect("file");
        assert!(!is_strata_database_dir(&fake));
    }

    #[test]
    fn implicit_target_is_interactive_only() {
        let root = tempfile::tempdir().expect("tmp");
        make_dataset(root.path());
        assert_eq!(
            implicit_interactive_target(OpenIntent::Interactive, Some(root.path())),
            Some(root.path().to_path_buf())
        );
        // Agents keep the refusal: never an implicit write target. An
        // inference one-shot falls back to cache, never to the cwd (R9).
        for intent in [
            OpenIntent::OneShot,
            OpenIntent::Pipe,
            OpenIntent::InferenceOneShot,
        ] {
            assert_eq!(
                implicit_interactive_target(intent, Some(root.path())),
                None,
                "{intent:?}"
            );
        }
        // A non-database cwd yields nothing even interactively.
        let plain = tempfile::tempdir().expect("tmp");
        assert_eq!(
            implicit_interactive_target(OpenIntent::Interactive, Some(plain.path())),
            None
        );
    }

    #[test]
    fn contained_datasets_are_listed_sorted_and_capped() {
        let root = tempfile::tempdir().expect("tmp");
        for name in ["zeta", "alpha", "not-a-db"] {
            let child = root.path().join(name);
            if name == "not-a-db" {
                fs::create_dir_all(&child).expect("dir");
            } else {
                make_dataset(&child);
            }
        }
        assert_eq!(strata_databases_in(root.path()), vec!["alpha", "zeta"]);
    }

    #[test]
    fn the_refusal_names_the_cwd_dataset_or_its_children() {
        let root = tempfile::tempdir().expect("tmp");
        make_dataset(root.path());
        let inside = no_database_refusal(Some(root.path())).to_string();
        assert!(
            inside.contains("current directory is a Strata database"),
            "{inside}"
        );

        let parent = tempfile::tempdir().expect("tmp");
        make_dataset(&parent.path().join("sales"));
        let above = no_database_refusal(Some(parent.path())).to_string();
        assert!(above.contains("Strata datasets here: sales"), "{above}");

        let plain = tempfile::tempdir().expect("tmp");
        let bare = no_database_refusal(Some(plain.path())).to_string();
        assert!(!bare.contains("note:"), "{bare}");
    }
}
