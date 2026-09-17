//! Live state of a hosted IPC socket, as seen by the `ipc_status` command.
//!
//! When a `Connection` opens a durable store as a host (see [`crate::IpcMode`])
//! it starts an `IpcServer` and injects an [`IpcHostState`] into the underlying
//! [`crate::Executor`], so the executor's `ipc_status` handler can report the
//! socket path, the owner pid, and the live client count uniformly — for an
//! in-process caller and for a remote client whose request is forwarded here
//! over the wire. A non-hosting executor (a client, a cache, or an
//! `IpcMode::Off` open) holds none, which the handler reports as `hosting:
//! false`.
//!
//! This lives in the always-compiled part of the crate (not the unix-only `ipc`
//! module) because the executor field that holds it is always compiled; the
//! unix transport is what populates it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::SessionAccess;

/// Display identity of one connected IPC client, as its hello introduced it.
/// A protocol-1 (pre-hello) connection appears anonymous. Display metadata
/// only — the `0600` socket's same-user check is the security boundary.
#[derive(Clone, Debug)]
pub struct IpcClientEntry {
    /// Client-reported product name (`strata-vscode`, `strata`), if any.
    pub name: Option<String>,
    /// Client-reported version, if any.
    pub version: Option<String>,
    /// Client-reported process id, if any.
    pub pid: Option<u64>,
    /// The session access this connection was granted.
    pub access: SessionAccess,
    /// The negotiated wire protocol revision (1 = legacy, no hello).
    pub protocol: u32,
}

/// Live registry of connected IPC clients, shared between the server (which
/// registers, upgrades on hello, and deregisters via RAII) and `ipc_status`
/// (which snapshots it). Keys are per-connection ids in accept order, so
/// snapshots are stable.
#[derive(Clone, Debug, Default)]
pub struct IpcClientRegistry {
    inner: Arc<Mutex<BTreeMap<u64, IpcClientEntry>>>,
}

impl IpcClientRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a connection (called by the server's connection guard).
    #[cfg(feature = "ipc")]
    pub(crate) fn register(&self, id: u64, entry: IpcClientEntry) {
        self.lock().insert(id, entry);
    }

    /// Replace a connection's entry (a hello upgrading the anonymous record).
    #[cfg(feature = "ipc")]
    pub(crate) fn update(&self, id: u64, entry: IpcClientEntry) {
        self.lock().insert(id, entry);
    }

    /// Forget a connection (guard drop on any exit path).
    #[cfg(feature = "ipc")]
    pub(crate) fn deregister(&self, id: u64) {
        self.lock().remove(&id);
    }

    /// The number of connected clients.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no clients are connected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// A point-in-time copy of the connected clients, in accept order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<IpcClientEntry> {
        self.lock().values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, IpcClientEntry>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A handle onto a running IPC host's reportable state.
///
/// `socket_path` and `owner_pid` are fixed at host start; `clients` is the
/// live client registry shared with the server, so a status read always
/// reflects the current connections. `stop_signal` is the server's shutdown
/// flag — setting it stops the listener (`ipc_stop`).
#[derive(Clone, Debug)]
pub struct IpcHostState {
    socket_path: PathBuf,
    owner_pid: u32,
    clients: IpcClientRegistry,
    stop_signal: Arc<AtomicBool>,
}

impl IpcHostState {
    /// Build a host-state handle from a server's fixed facts, its live client
    /// registry, and its shutdown flag.
    #[must_use]
    pub fn new(
        socket_path: PathBuf,
        owner_pid: u32,
        clients: IpcClientRegistry,
        stop_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            socket_path,
            owner_pid,
            clients,
            stop_signal,
        }
    }

    /// Signal the hosting server to stop accepting connections (`ipc_stop`).
    /// The listener exits on its next poll; the socket files are unlinked when
    /// the owning `Connection` is later closed or dropped.
    pub fn request_stop(&self) {
        self.stop_signal.store(true, Ordering::SeqCst);
    }

    /// The socket this owner is hosting on.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// The hosting owner's process id.
    #[must_use]
    pub const fn owner_pid(&self) -> u32 {
        self.owner_pid
    }

    /// The number of clients currently connected to the host.
    #[must_use]
    pub fn client_count(&self) -> u64 {
        u64::try_from(self.clients.len()).unwrap_or(u64::MAX)
    }

    /// A point-in-time copy of the connected clients.
    #[must_use]
    pub fn clients(&self) -> Vec<IpcClientEntry> {
        self.clients.snapshot()
    }
}
