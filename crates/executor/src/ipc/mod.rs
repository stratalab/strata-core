//! Transparent IPC for concurrent multi-process access.
//!
//! When a database is already locked by another process, Strata can connect
//! to a running IPC server via Unix domain socket instead of failing. This
//! enables workflows like CLI + AI agent, Python script + Node SDK.
//!
//! # Architecture
//!
//! - [`IpcServer`] holds the database and listens on `strata.sock`
//! - [`IpcClient`] connects to the socket and sends commands
//! - [`Backend`] enum routes commands to either local executor or IPC client

pub mod client;
pub mod protocol;
pub mod server;
pub mod wire;

pub use client::IpcClient;
pub use server::IpcServer;

use std::path::PathBuf;
use std::sync::Mutex;

use strata_security::AccessMode;

use crate::{Command, Executor, Output, Result};

/// Backend for command execution — either local (in-process) or IPC (remote).
pub(crate) enum Backend {
    /// Direct in-process execution via the Executor.
    Local {
        /// The local executor.
        executor: Executor,
    },
    /// Remote execution via Unix domain socket IPC.
    Ipc {
        /// The IPC client connection (mutex for interior mutability).
        client: Mutex<IpcClient>,
        /// The access mode for this connection.
        access_mode: AccessMode,
        /// The data directory path.
        data_dir: PathBuf,
    },
}

impl Backend {
    /// Execute a command through the appropriate backend.
    pub(crate) fn execute(&self, cmd: Command) -> Result<Output> {
        match self {
            Backend::Local { executor } => executor.execute(cmd),
            Backend::Ipc { client, .. } => {
                let mut c = client.lock().unwrap_or_else(|e| e.into_inner());
                c.execute(cmd)
            }
        }
    }

    /// Execute a command that bypasses the system branch guard.
    ///
    /// For IPC, the command already has the branch field set to `_system_`,
    /// so normal `execute()` on the server side works. However, the server's
    /// Session uses execute (not execute_internal), so system branch commands
    /// are routed through regular execute which includes the guard.
    ///
    /// For local backend, this calls `execute_internal` directly.
    pub(crate) fn execute_internal(&self, cmd: Command) -> Result<Output> {
        match self {
            Backend::Local { executor } => executor.execute_internal(cmd),
            Backend::Ipc { client, .. } => {
                // The server session calls execute(), which has the system
                // branch guard. For IPC clients to use system branch, the
                // server would need a privileged path. For now, route through
                // normal execute — system branch ops over IPC will be rejected.
                let mut c = client.lock().unwrap_or_else(|e| e.into_inner());
                c.execute(cmd)
            }
        }
    }

    /// Get a reference to the local executor, if this is a local backend.
    ///
    /// # Panics
    ///
    /// Panics if called on an IPC backend.
    pub(crate) fn executor(&self) -> &Executor {
        match self {
            Backend::Local { executor } => executor,
            Backend::Ipc { .. } => {
                panic!("executor() not available on IPC backend — use execute_cmd() instead")
            }
        }
    }

    /// Check if this is a local backend.
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Backend::Local { .. })
    }
}
