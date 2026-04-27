use std::path::PathBuf;

use clap::ArgMatches;
use strata_executor::{AccessMode, Command, OpenOptions, Output, Session, Strata};

use crate::context::Context;

pub(crate) struct OpenRequest {
    db_path: Option<PathBuf>,
    cache: bool,
    follower: bool,
    read_only: bool,
    initial_branch: Option<String>,
    initial_space: Option<String>,
}

pub(crate) struct OpenedCli {
    pub(crate) db: Strata,
    pub(crate) session: Session,
    pub(crate) context: Context,
}

impl OpenRequest {
    pub(crate) fn from_matches(matches: &ArgMatches) -> Self {
        Self {
            db_path: matches.get_one::<String>("db").map(PathBuf::from),
            cache: matches.get_flag("cache"),
            follower: matches.get_flag("follower"),
            read_only: matches.get_flag("read-only"),
            initial_branch: matches.get_one::<String>("branch").cloned(),
            initial_space: matches.get_one::<String>("space").cloned(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        db_path: Option<PathBuf>,
        cache: bool,
        follower: bool,
        read_only: bool,
        initial_branch: Option<String>,
        initial_space: Option<String>,
    ) -> Self {
        Self {
            db_path,
            cache,
            follower,
            read_only,
            initial_branch,
            initial_space,
        }
    }
}

pub(crate) fn open_cli(request: OpenRequest) -> Result<OpenedCli, String> {
    let db = if request.cache {
        Strata::cache().map_err(|error| format!("Failed to open cache database: {error}"))?
    } else {
        let mut options = OpenOptions::new();
        if request.read_only || request.follower {
            options = options.access_mode(AccessMode::ReadOnly);
        }
        if request.follower {
            options = options.follower(true);
        }

        let path = request.db_path.unwrap_or_else(|| PathBuf::from(".strata"));
        Strata::open_with(path, options)
            .map_err(|error| format!("Failed to open database: {error}"))?
    };

    let default_branch = db.current_branch().to_string();
    let branch = request
        .initial_branch
        .unwrap_or_else(|| default_branch.clone());
    let space = request
        .initial_space
        .unwrap_or_else(|| "default".to_string());

    let mut session = if request.cache && request.read_only {
        Session::new_with_mode(db.database(), AccessMode::ReadOnly)
    } else {
        db.session()
            .map_err(|error| format!("Failed to create session: {error}"))?
    };
    if !request.follower {
        ensure_branch_exists(&mut session, &branch)?;
    }
    let context = Context::new(default_branch, branch, space);

    Ok(OpenedCli {
        db,
        session,
        context,
    })
}

pub(crate) fn ensure_branch_exists(session: &mut Session, branch: &str) -> Result<(), String> {
    match session
        .execute(Command::BranchExists {
            branch: branch.into(),
        })
        .map_err(|error| error.to_string())?
    {
        Output::Bool(true) => Ok(()),
        Output::Bool(false) => Err(format!("Branch not found: {branch}")),
        other => Err(format!(
            "Unexpected output when checking branch existence: {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{open_cli, OpenRequest};
    use strata_executor::{Command, Error, Session, Strata, Value};

    fn kv_put(session: &mut Session, branch: &str, space: &str) -> Result<(), Error> {
        session.execute(Command::KvPut {
            branch: Some(branch.into()),
            space: Some(space.to_string()),
            key: "k".to_string(),
            value: Value::Int(1),
        })?;
        Ok(())
    }

    #[test]
    fn cache_read_only_rejects_writes() {
        let mut opened = open_cli(OpenRequest {
            db_path: None,
            cache: true,
            follower: false,
            read_only: true,
            initial_branch: None,
            initial_space: None,
        })
        .expect("cache open should succeed");

        let error = kv_put(
            &mut opened.session,
            opened.context.branch(),
            opened.context.space(),
        )
        .expect_err("write should be rejected");
        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn disk_read_only_rejects_writes() {
        let temp = tempdir().expect("tempdir should succeed");
        let db = Strata::open(temp.path()).expect("db create should succeed");
        db.close().expect("db close should succeed");

        let mut opened = open_cli(OpenRequest {
            db_path: Some(temp.path().to_path_buf()),
            cache: false,
            follower: false,
            read_only: true,
            initial_branch: None,
            initial_space: None,
        })
        .expect("read-only open should succeed");

        let error = kv_put(
            &mut opened.session,
            opened.context.branch(),
            opened.context.space(),
        )
        .expect_err("write should be rejected");
        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn follower_open_rejects_writes() {
        let temp = tempdir().expect("tempdir should succeed");
        let db = Strata::open(temp.path()).expect("db create should succeed");
        db.close().expect("db close should succeed");

        let mut opened = open_cli(OpenRequest {
            db_path: Some(temp.path().to_path_buf()),
            cache: false,
            follower: true,
            read_only: false,
            initial_branch: None,
            initial_space: None,
        })
        .expect("follower open should succeed");

        let error = kv_put(
            &mut opened.session,
            opened.context.branch(),
            opened.context.space(),
        )
        .expect_err("write should be rejected");
        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn missing_initial_branch_is_rejected() {
        let result = open_cli(OpenRequest {
            db_path: None,
            cache: true,
            follower: false,
            read_only: false,
            initial_branch: Some("missing".to_string()),
            initial_space: None,
        });
        let error = match result {
            Ok(_) => panic!("missing branch should fail"),
            Err(error) => error,
        };
        assert!(error.contains("Branch not found"));
    }
}
