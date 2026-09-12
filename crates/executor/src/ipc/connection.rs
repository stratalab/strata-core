//! `Connection` — the transport-transparent handle every frontend opens.
//!
//! A connection is either `Local` (this process won the writer lock and holds
//! the one `Executor`, optionally hosting a socket for others) or `Remote`
//! (another process owns the store and we speak to it over its socket). Both
//! present the same `execute(Command) -> Result<Output, ExecutorError>`, so callers
//! never branch on which they got.
//!
//! `open_durable_local_brokered` is the open dance: try to win the lock; on
//! contention, connect to the owner's socket; a bounded retry rides the
//! ~250ms window where an owner is starting up or shutting down (so the lock
//! or the socket is briefly in flux) before either result becomes final.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    Command, DurableLocalOpenOptions, Executor, ExecutorError, IpcMode, Output, DEFAULT_BRANCH,
    DEFAULT_SPACE,
};

use super::client::{ConnectError, IpcClient};
use super::dispatch::read_only_rejection;
use super::protocol::SessionAccess;
use super::resolve_connect;
use super::server::IpcServer;

/// The engine's lock-contention wire code — the signal that another process
/// owns the store (see the RFC trace: `local_fs` EWOULDBLOCK folds to this).
const LOCK_CONTENTION_CODE: &str = "unavailable.engine.persistence";
/// Poll step while riding the owner start-up / shut-down window.
const OPEN_RETRY_STEP: Duration = Duration::from_millis(25);
/// Number of poll steps (≈ the ~250ms owner-close window, doubled for headroom).
const OPEN_RETRY_STEPS: u32 = 20;

/// A transport-transparent database connection.
pub struct Connection {
    inner: ConnectionInner,
    scope: Mutex<Scope>,
    /// The access this connection was opened with. For a remote connection the
    /// owner's dispatch gate is the authority and this drives the courtesy
    /// pre-rejection; for a local connection it is the only gate there is
    /// (a true read-only *open* is a separate engine feature).
    access: SessionAccess,
}

struct Scope {
    branch: String,
    space: String,
}

enum ConnectionInner {
    /// This process owns the store. The server (when present) shares this exact
    /// `Arc<Mutex<Executor>>`, so in-process and socket clients serialize
    /// through one executor; dropping the server unlinks the socket.
    Local {
        executor: Arc<Mutex<Executor>>,
        server: Option<IpcServer>,
    },
    /// Another process owns the store; we are a client of its socket.
    Remote { client: Mutex<IpcClient> },
}

impl Connection {
    /// Open a durable store with transparent multi-process brokering, driven by
    /// `ipc` (a transport policy the executor owns; the engine `options` stay
    /// IPC-agnostic):
    ///
    /// - [`IpcMode::Host`]: win the lock and host a socket so other processes
    ///   can broker to us (a REPL, `mcp serve`, `strata start`, a long-lived SDK
    ///   app). On contention, broker to the existing owner instead.
    /// - [`IpcMode::Client`]: win the lock but do not host (a one-shot command
    ///   that holds the lock briefly); on contention, broker as a client.
    /// - [`IpcMode::Off`]: opt out entirely — a raw local open with no server
    ///   and no fallback (hardened single-process deployments).
    ///
    /// `access` is the session access this connection lives under. `Read`
    /// rejects every write-classified command: enforced by the owner's
    /// dispatch gate when brokered, and by this connection's own execute
    /// chokepoint when local (a courtesy until a true read-only engine open
    /// exists).
    ///
    /// # Errors
    ///
    /// The underlying open error, the owner's capacity refusal, or the
    /// lock-contention error when the store is owned but no socket is
    /// reachable within the retry budget.
    pub fn open_durable_local_brokered(
        path: impl Into<PathBuf>,
        options: DurableLocalOpenOptions,
        ipc: IpcMode,
        access: SessionAccess,
    ) -> Result<Self, ExecutorError> {
        let path = path.into();
        match ipc {
            // Opt out: a raw single-process open, no socket and no fallback.
            IpcMode::Off => {
                let executor = Executor::open_durable_local_with_options(&path, options)?;
                Ok(Self::local(executor, None, access))
            }
            // Host: win the lock and host a socket others can broker to.
            IpcMode::Host => Self::open_brokered(&path, options, true, access),
            // Client: win the lock but do not host; broker only on contention.
            IpcMode::Client => Self::open_brokered(&path, options, false, access),
        }
    }

    fn open_brokered(
        path: &Path,
        options: DurableLocalOpenOptions,
        host: bool,
        access: SessionAccess,
    ) -> Result<Self, ExecutorError> {
        // Quiet open: a lost-lock loss that then brokers to the owner must not
        // cross the logging boundary as an engine failure (#3071). Only a
        // definitive failure is converted to a logged `ExecutorError` below.
        match Executor::open_durable_local_quiet(path, options.clone()) {
            Ok(executor) => Ok(Self::local_hosting(path, executor, host, access)),
            // Contention: the store is already owned. Ride the owner start-up /
            // shut-down window, brokering to its socket if one appears.
            Err(error) if error.code() == LOCK_CONTENTION_CODE => {
                Self::broker_to_owner(path, options, host, access)
            }
            // A non-contention open error never brokers — surface it (this is a
            // genuine failure, so it logs at the executor boundary).
            Err(error) => Err(crate::ExecutorError::from(error)),
        }
    }

    /// The store was owned when we tried to open it. Ride the short, fixed owner
    /// start-up / shut-down window (bounded poll iterations, not a wall clock):
    /// an owner may be binding its socket (we become a client) or releasing the
    /// lock (we become the owner). Whichever resolves first wins; if neither does
    /// within the window, a final open returns the definitive lock error.
    ///
    /// This is a deliberately racy retry loop — its per-iteration branches are
    /// timing-dependent and convergent, so it is carved out of the mutation gate.
    fn broker_to_owner(
        path: &Path,
        options: DurableLocalOpenOptions,
        host: bool,
        access: SessionAccess,
    ) -> Result<Self, ExecutorError> {
        // A capacity refusal is remembered across the window: slots may free
        // as clients disconnect, so we keep riding — but if the window closes
        // with the store still locked, the refusal is the truthful error to
        // surface, not the lock-contention fallback.
        let mut at_capacity: Option<crate::ExecutorError> = None;
        for _ in 0..OPEN_RETRY_STEPS {
            if let Some(socket) = resolve_connect(path) {
                match IpcClient::connect(&socket, access) {
                    Ok(client) => return Ok(Self::remote(client, access)),
                    Err(ConnectError::AtCapacity(error)) => at_capacity = Some(error),
                    // Any other connect failure (a socket mid-teardown, a
                    // refused hello) keeps riding the window; the lock probe
                    // below decides how this open ends.
                    Err(ConnectError::Io(error)) => {
                        tracing::debug!("IPC connect probe failed; still riding: {error}");
                    }
                }
            }
            match Executor::open_durable_local_quiet(path, options.clone()) {
                Ok(executor) => return Ok(Self::local_hosting(path, executor, host, access)),
                Err(error) if error.code() == LOCK_CONTENTION_CODE => {
                    std::thread::sleep(OPEN_RETRY_STEP);
                }
                Err(error) => return Err(crate::ExecutorError::from(error)),
            }
        }
        // The window closed with the store still locked: this contention did
        // NOT recover, so the failure is genuine and crosses the logging
        // boundary (a capacity refusal, if seen, is the truthful error).
        match Executor::open_durable_local_quiet(path, options) {
            Ok(executor) => Ok(Self::local_hosting(path, executor, host, access)),
            Err(error) if error.code() == LOCK_CONTENTION_CODE => {
                Err(at_capacity.unwrap_or_else(|| crate::ExecutorError::from(error)))
            }
            Err(error) => Err(crate::ExecutorError::from(error)),
        }
    }

    fn local_hosting(path: &Path, executor: Executor, host: bool, access: SessionAccess) -> Self {
        let scope = Scope {
            branch: executor.default_branch().to_owned(),
            space: executor.default_space().to_owned(),
        };
        let executor = Arc::new(Mutex::new(executor));
        // Hosting is best-effort: a bind failure leaves the store fully usable
        // in-process, only unable to broker (mirrors the fork-manifest arm's
        // health-debt posture).
        let server = if host {
            match IpcServer::start(path, executor.clone()) {
                Ok(server) => {
                    // Inject the live host state so `ipc_status` (in-process or
                    // forwarded from a client) can report the socket, pid, and
                    // connected-client count.
                    let state = crate::IpcHostState::new(
                        server.socket_path().to_path_buf(),
                        std::process::id(),
                        server.clients(),
                        server.stop_signal(),
                    );
                    executor
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .set_ipc_host_state(state);
                    Some(server)
                }
                Err(error) => {
                    tracing::warn!(
                        "IPC server not started (store usable in-process only): {error}"
                    );
                    None
                }
            }
        } else {
            None
        };
        Self {
            inner: ConnectionInner::Local { executor, server },
            scope: Mutex::new(scope),
            access,
        }
    }

    fn local(executor: Executor, server: Option<IpcServer>, access: SessionAccess) -> Self {
        let scope = Scope {
            branch: executor.default_branch().to_owned(),
            space: executor.default_space().to_owned(),
        };
        Self {
            inner: ConnectionInner::Local {
                executor: Arc::new(Mutex::new(executor)),
                server,
            },
            scope: Mutex::new(scope),
            access,
        }
    }

    /// Wrap a cache (non-durable, single-process) executor as a local
    /// connection. Cache mode has no writer lock and no socket, so there is
    /// nothing to broker — this is always a plain local handle, letting every
    /// frontend hold one `Connection` type regardless of the open target.
    /// Cache handles are full-access; a read-only view of an ephemeral store
    /// no other process can reach has nothing to protect.
    #[must_use]
    pub fn cache(executor: Executor) -> Self {
        Self::local(executor, None, SessionAccess::ReadWrite)
    }

    fn remote(client: IpcClient, access: SessionAccess) -> Self {
        Self {
            inner: ConnectionInner::Remote {
                client: Mutex::new(client),
            },
            scope: Mutex::new(Scope {
                branch: DEFAULT_BRANCH.to_owned(),
                space: DEFAULT_SPACE.to_owned(),
            }),
            access,
        }
    }

    /// Whether this process owns the store (vs is a client of another owner).
    #[must_use]
    pub fn is_local(&self) -> bool {
        matches!(self.inner, ConnectionInner::Local { .. })
    }

    /// Whether this connection is hosting a socket for other processes.
    #[must_use]
    pub fn is_hosting(&self) -> bool {
        matches!(
            &self.inner,
            ConnectionInner::Local {
                server: Some(_),
                ..
            }
        )
    }

    /// Whether this connection is *actively* hosting right now. `is_hosting`
    /// reports whether a server was started; this also accounts for an
    /// `ipc_stop` (in-process or brokered from another process) having signaled
    /// that server to shut down. A headless `strata start` owner polls this to
    /// block until its hosting is stopped, then exits.
    #[must_use]
    pub fn hosting_active(&self) -> bool {
        match &self.inner {
            ConnectionInner::Local {
                server: Some(server),
                ..
            } => !server.is_stopped(),
            _ => false,
        }
    }

    /// The owner's hello, when this connection brokered to a remote owner that
    /// speaks protocol revision 2 — its release, IDL stamps, granted access,
    /// and pid. `None` for local and cache handles (there is no remote owner
    /// to describe) and for a pre-hello owner (implicit protocol 1).
    #[must_use]
    pub fn server_hello(&self) -> Option<super::ServerHello> {
        match &self.inner {
            ConnectionInner::Remote { client } => client
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .server_hello()
                .cloned(),
            ConnectionInner::Local { .. } => None,
        }
    }

    /// Set this connection's default branch. Applied to every subsequent
    /// command (locally by setting the executor default under the lock,
    /// remotely by sending it in the request envelope).
    pub fn set_default_branch(&self, branch: impl Into<String>) {
        self.scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .branch = branch.into();
    }

    /// Set this connection's default space.
    pub fn set_default_space(&self, space: impl Into<String>) {
        self.scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .space = space.into();
    }

    /// This connection's current default branch (the store's default at open,
    /// or the last value set via [`Self::set_default_branch`]).
    #[must_use]
    pub fn default_branch(&self) -> String {
        self.scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .branch
            .clone()
    }

    /// This connection's current default space.
    #[must_use]
    pub fn default_space(&self) -> String {
        self.scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .space
            .clone()
    }

    /// Execute a command, returning the same typed result whether the store is
    /// local or remote.
    ///
    /// # Errors
    ///
    /// The command's own error, the read-only rejection on a `Read`-access
    /// connection submitting a write, or a transport error if a remote
    /// owner's connection fails.
    pub fn execute(&self, command: Command) -> Result<Output, ExecutorError> {
        // One chokepoint for both transports. Remotely this is a courtesy
        // (the owner's dispatch gate is the authority and would reject the
        // same way); locally it is the only gate there is.
        if self.access == SessionAccess::Read && command.is_write() {
            return Err(read_only_rejection(&command));
        }
        let (branch, space) = {
            let scope = self
                .scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (scope.branch.clone(), scope.space.clone())
        };
        match &self.inner {
            ConnectionInner::Local { executor, .. } => {
                let mut executor = executor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                executor.set_default_branch(branch)?;
                executor.set_default_space(space)?;
                executor.execute(command)
            }
            ConnectionInner::Remote { client } => {
                let output = client
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .execute(Some(&branch), Some(&space), &command)?;
                // The owner answered `ipc_status` as an owner would; from a
                // remote client's vantage it is not the owner. Every other
                // command passes through untouched.
                Ok(match output {
                    Output::IpcStatus(mut status) => {
                        status.is_owner = false;
                        Output::IpcStatus(status)
                    }
                    other => other,
                })
            }
        }
    }

    /// Close the connection: for a local owner, drop the server (unlink the
    /// socket) and close the executor; for a remote client, drop the socket.
    ///
    /// # Errors
    ///
    /// The executor close error, for a local owner.
    pub fn close(self) -> Result<(), ExecutorError> {
        match self.inner {
            ConnectionInner::Local { executor, server } => {
                drop(server); // stop the listener and unlink the socket first
                if let Ok(mutex) = Arc::try_unwrap(executor) {
                    let mut executor = mutex
                        .into_inner()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    executor.close()?;
                }
                Ok(())
            }
            ConnectionInner::Remote { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    use super::{Connection, SessionAccess};
    use crate::{Command, DurableLocalOpenOptions, Executor, IpcMode, Output};

    /// Counts ERROR-level tracing events on the current thread — the seam the
    /// broker open dance must stay quiet on for a recovered lock contention.
    struct ErrorCounter(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> Layer<S> for ErrorCounter {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::ERROR {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn ipc_status(conn: &Connection) -> crate::types::AdminIpcStatus {
        match conn
            .execute(Command::IpcStatus {})
            .expect("ipc_status runs")
        {
            Output::IpcStatus(status) => status,
            other => panic!("expected ipc_status output, got {other:?}"),
        }
    }

    fn ipc_stop(conn: &Connection) -> crate::types::AdminIpcStop {
        match conn.execute(Command::IpcStop {}).expect("ipc_stop runs") {
            Output::IpcStop(result) => result,
            other => panic!("expected ipc_stop output, got {other:?}"),
        }
    }

    #[test]
    fn brokered_open_attaches_across_path_spellings() {
        // The owner and a client may spell the same store differently (a
        // short symlink vs the deep canonical path). Socket resolution must
        // agree for both — even when one spelling overflows `sun_path` — so
        // the client attaches instead of being stranded on the lock error.
        let root = tempfile::tempdir().expect("tmp");
        let dir = root.path().join("db");
        std::fs::create_dir_all(&dir).expect("db dir");
        // A second spelling of the SAME directory, dot-padded past `sun_path`
        // (`/.` segments resolve to the identical location, so only the
        // spelled length differs — the absolute-vs-relative hazard in
        // miniature).
        let mut padded = dir.clone().into_os_string();
        while padded.len() < 120 {
            padded.push("/.");
        }
        let padded = std::path::PathBuf::from(padded);

        let owner = Connection::open_durable_local_brokered(
            &dir,
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open via the short spelling");
        let client = Connection::open_durable_local_brokered(
            &padded,
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open via the over-long spelling");
        assert!(!client.is_local(), "the client attached over the broker");

        client.close().expect("client close");
        owner.close().expect("owner close");
    }

    #[test]
    fn ipc_stop_halts_hosting_and_is_idempotent() {
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        assert!(ipc_status(&owner).hosting, "hosting before stop");

        assert!(ipc_stop(&owner).stopped, "the running host is stopped");
        assert!(
            !ipc_status(&owner).hosting,
            "the host reports it is no longer hosting after stop"
        );
        assert!(!ipc_stop(&owner).stopped, "stopping again is a no-op");

        owner.close().expect("owner close");
    }

    #[test]
    fn a_client_ipc_stop_stops_the_owner_hosting() {
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(!client.is_local(), "second opener brokered as a client");

        // The client's `ipc_stop` forwards to the owner, which stops hosting.
        assert!(ipc_stop(&client).stopped, "the client stopped the owner");
        assert!(
            !ipc_status(&owner).hosting,
            "the owner stopped hosting at the client's request"
        );

        // Stopping actually signals the server (not just clears the state): the
        // owner's handler sees the shutdown flag and drops this client, so the
        // client's next command loses its transport.
        let mut kicked = false;
        for _ in 0..200 {
            if client.execute(Command::IpcStatus {}).is_err() {
                kicked = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(kicked, "the client is dropped once the owner stops hosting");

        owner.close().expect("owner close");
    }

    #[test]
    fn ipc_status_reports_hosting_and_flips_is_owner_for_a_client() {
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        let owner_status = ipc_status(&owner);
        assert!(owner_status.is_owner, "the host owns the store");
        assert!(owner_status.hosting, "the host is hosting a socket");
        assert!(
            owner_status.socket_path.is_some(),
            "the host reports its socket"
        );
        assert_eq!(owner_status.owner_pid, Some(u64::from(std::process::id())));
        assert_eq!(owner_status.client_count, 0, "no clients connected yet");

        // A client brokers in; its status forwards to the owner but reports the
        // client's own ownership (false), and the live client count now sees it.
        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(!client.is_local(), "second opener brokered as a client");

        let client_status = ipc_status(&client);
        assert!(!client_status.is_owner, "a client is not the owner");
        assert!(client_status.hosting, "the owner it reached is hosting");
        assert_eq!(
            client_status.owner_pid,
            Some(u64::from(std::process::id())),
            "reports the same-process owner's pid"
        );
        assert_eq!(client_status.client_count, 1, "the live client is counted");
        assert_eq!(
            ipc_status(&owner).client_count,
            1,
            "the owner sees its client"
        );

        // When the client leaves, its handler thread exits and the live count
        // returns to zero (the decrement half of the connection guard).
        client.close().expect("client close");
        let mut count = u64::MAX;
        for _ in 0..200 {
            count = ipc_status(&owner).client_count;
            if count == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            count, 0,
            "the client count returns to zero after the client leaves"
        );
        owner.close().expect("owner close");
    }

    #[test]
    fn hosting_active_reflects_the_live_stop_signal() {
        // A host is actively hosting until `ipc_stop` signals its server to
        // shut down; `hosting_active` then reports false even though the server
        // struct is still held (`is_hosting` alone would stay true). This is the
        // exact predicate `strata start` blocks on.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        assert!(owner.is_hosting(), "the owner started a server");
        assert!(owner.hosting_active(), "and it is actively hosting");

        assert!(ipc_stop(&owner).stopped, "stop halts the hosting");
        assert!(
            owner.is_hosting(),
            "the server struct is still held after stop"
        );
        assert!(
            !owner.hosting_active(),
            "but it is no longer actively hosting once stopped"
        );
        owner.close().expect("owner close");
    }

    #[test]
    fn hosting_active_is_false_for_non_hosting_connections() {
        // A client-mode local open (won the lock, no socket), an opt-out open,
        // and a cache handle all host nothing, so none is actively hosting.
        let dir = tempfile::tempdir().expect("tmp");
        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(client.is_local() && !client.hosting_active());
        client.close().expect("close");

        let cache = Connection::cache(Executor::open_cache().expect("cache"));
        assert!(!cache.hosting_active(), "cache mode hosts nothing");
        cache.close().expect("close");
    }

    #[test]
    fn ipc_status_on_a_cache_connection_is_not_hosting() {
        let conn = Connection::cache(Executor::open_cache().expect("cache"));
        let status = ipc_status(&conn);
        assert!(status.is_owner, "a cache handle trivially owns its store");
        assert!(!status.hosting, "cache mode hosts nothing");
        assert!(status.socket_path.is_none());
        assert_eq!(status.owner_pid, None);
        assert_eq!(status.client_count, 0);
        conn.close().expect("close");
    }

    #[test]
    fn cache_connection_exposes_and_updates_its_default_scope() {
        // The cache constructor yields a plain local handle, and the scope
        // getters read the store default then track set_default_* — the CLI
        // relies on this for its prompt and one-shot scope resolution.
        let conn = Connection::cache(Executor::open_cache().expect("cache"));
        assert!(conn.is_local() && !conn.is_hosting(), "cache never hosts");
        assert_eq!(conn.default_branch(), crate::DEFAULT_BRANCH);
        assert_eq!(conn.default_space(), crate::DEFAULT_SPACE);
        conn.set_default_branch("feature");
        conn.set_default_space("docs");
        assert_eq!(conn.default_branch(), "feature");
        assert_eq!(conn.default_space(), "docs");
        conn.close().expect("close");
    }

    fn kv_put(key: &str, value: &str) -> Command {
        serde_json::from_value(serde_json::json!({
            "type": "kv_put", "key": key, "value": value,
        }))
        .expect("kv_put command")
    }

    fn kv_get(key: &str) -> Command {
        serde_json::from_value(serde_json::json!({ "type": "kv_get", "key": key }))
            .expect("kv_get command")
    }

    #[test]
    fn opt_out_opens_a_plain_local_connection() {
        let dir = tempfile::tempdir().expect("tmp");
        let conn = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Off,
            SessionAccess::ReadWrite,
        )
        .expect("open");
        assert!(conn.is_local());
        assert!(!conn.is_hosting(), "opt-out never hosts");
        conn.close().expect("close");
    }

    #[test]
    fn client_mode_winning_the_lock_opens_local_without_hosting() {
        // Client on an uncontended store wins the lock and opens Local — but,
        // unlike Host, it must NOT host a socket (a one-shot has no business
        // spinning up a listener). This pins the host=false in the Client arm.
        let dir = tempfile::tempdir().expect("tmp");
        let conn = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("open");
        assert!(conn.is_local(), "an uncontended client wins the lock");
        assert!(!conn.is_hosting(), "client never hosts a socket");
        conn.close().expect("close");
    }

    #[test]
    fn ipc_status_reports_client_identities_from_both_vantages() {
        // The status-bar contract: the owner (and any client asking through
        // it) can see WHO is attached — the identity each hello introduced,
        // its granted access, and its protocol revision.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        let reader = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::Read,
        )
        .expect("reader open");
        assert!(!reader.is_local(), "brokered as a client");

        let status = ipc_status(&owner);
        assert_eq!(status.client_count, 1);
        assert_eq!(status.clients.len(), 1, "count and list agree: {status:?}");
        let client = &status.clients[0];
        assert_eq!(client.protocol, 2);
        assert_eq!(client.access, SessionAccess::Read);
        assert_eq!(client.pid, Some(u64::from(std::process::id())));
        assert!(
            client.name.is_some(),
            "the Rust client introduces its executable name"
        );
        assert_eq!(client.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));

        // The same list is visible to the client itself (forwarded to the
        // owner, reported back over the wire).
        let remote_view = ipc_status(&reader);
        assert_eq!(remote_view.clients.len(), 1);
        assert_eq!(remote_view.clients[0].access, SessionAccess::Read);

        // A departed client leaves no entry.
        reader.close().expect("reader close");
        let mut remaining = usize::MAX;
        for _ in 0..200 {
            remaining = ipc_status(&owner).clients.len();
            if remaining == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(remaining, 0, "gone clients leave no identity entries");

        owner.close().expect("owner close");
    }

    #[test]
    fn a_read_access_local_open_gates_writes_at_the_connection() {
        // No engine read-only open exists yet, so for a local connection the
        // execute chokepoint IS the gate: writes rejected, reads served.
        let dir = tempfile::tempdir().expect("tmp");
        let conn = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Off,
            SessionAccess::Read,
        )
        .expect("read-access open");

        let error = conn
            .execute(kv_put("aGk=", "dg=="))
            .expect_err("a write on a read-access connection is rejected");
        assert_eq!(error.code(), "access_denied.executor.read_only_session");

        let got = conn.execute(kv_get("aGk=")).expect("reads still serve");
        let got = serde_json::to_value(&got).expect("json");
        assert_eq!(got["data"]["found"], false, "and nothing was written");
        conn.close().expect("close");
    }

    #[test]
    fn a_read_access_brokered_client_is_gated_and_the_owner_stays_writable() {
        // The gate keys on each session, not the store: a read-access client
        // brokered to a read-write owner is rejected on writes while the
        // owner keeps full access.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        let reader = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::Read,
        )
        .expect("reader open");
        assert!(!reader.is_local(), "second opener brokered as a client");
        assert_eq!(
            reader.server_hello().expect("protocol 2").granted_access,
            SessionAccess::Read,
            "the owner granted the declared read access"
        );

        let error = reader
            .execute(kv_put("aGk=", "dg=="))
            .expect_err("a write on a read session is rejected");
        assert_eq!(error.code(), "access_denied.executor.read_only_session");

        owner
            .execute(kv_put("aGk=", "b3duZXI="))
            .expect("owner writes");
        let seen = reader.execute(kv_get("aGk=")).expect("reader reads");
        let seen = serde_json::to_value(&seen).expect("json");
        assert_eq!(
            seen["data"]["value"]["value"], "b3duZXI=",
            "the read session observes the owner's writes"
        );

        reader.close().expect("reader close");
        owner.close().expect("owner close");
    }

    #[test]
    fn an_owner_at_capacity_yields_the_capacity_error_not_the_lock_error() {
        // The open dance rides the retry window on capacity refusals (a slot
        // may free), but when the window closes with the store still locked
        // it must surface the truthful capacity error — not the misleading
        // "no socket reachable" lock fallback.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        // Saturate the owner's connection cap with idle raw connections and
        // gate on the live client count the owner itself reports.
        let socket = crate::ipc::resolve_connect(dir.path()).expect("owner socket");
        let held: Vec<std::os::unix::net::UnixStream> = (0..crate::ipc::server::MAX_CONNECTIONS)
            .map(|_| std::os::unix::net::UnixStream::connect(&socket).expect("saturate"))
            .collect();
        for _ in 0..500 {
            if ipc_status(&owner).client_count >= crate::ipc::server::MAX_CONNECTIONS as u64 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            ipc_status(&owner).client_count,
            crate::ipc::server::MAX_CONNECTIONS as u64,
            "the cap is saturated"
        );

        let refused = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        );
        let Err(error) = refused else {
            panic!("an open against a saturated owner must fail");
        };
        assert_eq!(error.code(), "resource_exhausted.executor.ipc_connections");

        drop(held);
        owner.close().expect("owner close");
    }

    #[test]
    fn a_brokered_client_learns_the_owner_hello_and_a_local_owner_has_none() {
        // The hello is how an out-of-process surface (a status bar, doctor)
        // learns what it attached to: protocol revision, release, IDL stamps.
        // In-repo owner and client share one build, so the stamps must match
        // exactly here — a mismatch would mean the hello lies about its build.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        assert!(
            owner.server_hello().is_none(),
            "a local owner has no remote owner to describe"
        );

        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(!client.is_local(), "second opener brokered as a client");

        let hello = client
            .server_hello()
            .expect("a protocol-2 owner said hello");
        assert_eq!(hello.protocol, 2);
        assert_eq!(hello.release, env!("CARGO_PKG_VERSION"));
        assert_eq!(hello.idl.schema_version, "strata.idl.v1");
        assert_eq!(hello.idl.generator_version, "strata-executor-idl.1");
        assert_eq!(hello.owner_pid, std::process::id(), "same-process owner");
        assert!(hello.capabilities.is_empty(), "no capabilities exist yet");

        client.close().expect("client close");
        owner.close().expect("owner close");
    }

    #[test]
    fn first_opener_hosts_and_second_opener_brokers_through_it() {
        let dir = tempfile::tempdir().expect("tmp");

        // Owner: a long-lived host.
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");
        assert!(
            owner.is_local() && owner.is_hosting(),
            "owner hosts a socket"
        );

        // Owner writes a value.
        let put = owner
            .execute(kv_put("aGk=", "b3duZXI="))
            .expect("owner put");
        assert_eq!(
            serde_json::to_value(&put).expect("json")["type"],
            "write_result"
        );

        // Second opener finds the lock held and becomes a client.
        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(
            !client.is_local(),
            "second opener brokered as a remote client"
        );

        // The client reads the owner's write, and its own write is visible to
        // the owner — one store, two processes.
        let got = client.execute(kv_get("aGk=")).expect("client get");
        let got = serde_json::to_value(&got).expect("json");
        assert_eq!(
            got["data"]["value"]["value"], "b3duZXI=",
            "client sees owner's write"
        );

        client
            .execute(kv_put("Ynll", "Y2xpZW50"))
            .expect("client put");
        let owner_sees = owner.execute(kv_get("Ynll")).expect("owner get");
        let owner_sees = serde_json::to_value(&owner_sees).expect("json");
        assert_eq!(
            owner_sees["data"]["value"]["value"], "Y2xpZW50",
            "owner sees client's write"
        );

        client.close().expect("client close");
        owner.close().expect("owner close");
    }

    #[test]
    fn a_remote_command_after_the_owner_is_gone_is_a_transport_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        let client = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        )
        .expect("client open");
        assert!(!client.is_local(), "brokered as a client");

        // The owner exits; the client's next command loses its transport and
        // surfaces the registered, in-doubt transport error.
        owner.close().expect("owner close");
        let error = client
            .execute(kv_get("aGk="))
            .expect_err("a command with no owner fails");
        assert_eq!(error.code(), "unavailable.executor.ipc_transport");
        assert_eq!(error.class(), crate::ExecutorErrorClass::Unavailable);
        // #3244: the site restated the row's retry flag and had drifted from
        // it; the whole row is what the caller must see.
        let entry = crate::public_error_code_entry("unavailable.executor.ipc_transport")
            .expect("ipc_transport is registered");
        assert_eq!(error.public_class(), entry.class);
        assert_eq!(error.retry_policy(), entry.retry_policy);
        assert_eq!(error.commit_outcome(), entry.commit_outcome);
        assert_eq!(error.suggested_fix(), entry.suggested_fix);
    }

    #[test]
    fn set_default_branch_scopes_subsequent_commands() {
        // A local connection's default branch/space must actually apply to
        // later commands (a no-op setter would silently target the wrong
        // branch).
        let dir = tempfile::tempdir().expect("tmp");
        let conn = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Off,
            SessionAccess::ReadWrite,
        )
        .expect("open");

        let created: serde_json::Value = serde_json::to_value(
            conn.execute(
                serde_json::from_value(serde_json::json!({
                    "type": "branch_create", "branch": "feature",
                }))
                .expect("cmd"),
            )
            .expect("create branch"),
        )
        .expect("json");
        assert_eq!(created["type"], "branch");

        conn.set_default_branch("feature");
        conn.execute(kv_put("aw==", "dg=="))
            .expect("put on default scope");
        // The write must be visible on `feature` (the connection default),
        // and absent on the store default.
        let on_feature = read_branch(&conn, Some("feature"), "aw==");
        assert_eq!(on_feature["data"]["value"]["value"], "dg==");
        let on_default = read_branch(&conn, Some(crate::DEFAULT_BRANCH), "aw==");
        assert_eq!(on_default["data"]["found"], false);

        conn.close().expect("close");
    }

    #[test]
    fn set_default_space_applies_to_subsequent_commands() {
        // A no-op space setter would silently leave commands on the store
        // default. Observe the setter took effect via validation: an over-long
        // default space (rejected by ProductSpace) must surface on the NEXT
        // command, proving `execute` applied the connection's space.
        let dir = tempfile::tempdir().expect("tmp");
        let conn = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Off,
            SessionAccess::ReadWrite,
        )
        .expect("open");

        conn.set_default_space("s".repeat(usize::from(u16::MAX) + 1));
        let Err(error) = conn.execute(kv_put("aw==", "dg==")) else {
            panic!("an over-long default space must be rejected when applied");
        };
        assert_eq!(error.code(), "invalid_argument.engine.product_space");
        conn.close().expect("close");
    }

    /// Read a key on an explicit branch (bypasses the connection scope).
    fn read_branch(conn: &Connection, branch: Option<&str>, key: &str) -> serde_json::Value {
        let command = serde_json::from_value(serde_json::json!({
            "type": "kv_get", "branch": branch, "key": key,
        }))
        .expect("kv_get");
        serde_json::to_value(conn.execute(command).expect("get")).expect("json")
    }

    #[test]
    fn a_non_contention_open_error_propagates_without_brokering() {
        // A regular file (not a database directory) fails with a permanent,
        // non-lock error. The brokered open must surface THAT error, not treat
        // it as contention and try to connect to a nonexistent owner.
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("not-a-db");
        std::fs::write(&path, b"plain file").expect("write file");
        let refused = Connection::open_durable_local_brokered(
            &path,
            // Brokering ON (Client) — the fallback path, which must NOT swallow
            // a non-contention error as if it were a busy owner.
            DurableLocalOpenOptions::new(),
            IpcMode::Client,
            SessionAccess::ReadWrite,
        );
        let Err(error) = refused else {
            panic!("a regular file is not a database");
        };
        assert_eq!(error.code(), "invalid_argument.engine.persistence");
        assert_ne!(error.code(), super::LOCK_CONTENTION_CODE);
    }

    #[test]
    fn opt_out_client_refuses_a_held_store_instead_of_brokering() {
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        // IpcMode::Off must not fall back to a client — it takes the raw lock
        // path and fails with the lock-contention error.
        let refused = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Off,
            SessionAccess::ReadWrite,
        );
        let Err(error) = refused else {
            panic!("opt-out should refuse a held store, not broker");
        };
        assert_eq!(error.code(), super::LOCK_CONTENTION_CODE);

        owner.close().expect("owner close");
    }

    #[test]
    fn a_client_brokering_to_an_owner_does_not_log_a_lost_lock_as_an_error() {
        // #3071: a one-shot (IpcMode::Client) against a hosted database loses
        // the direct lock to the owner and brokers to it. That lost attempt is
        // expected contention with a designed recovery, so it must NOT surface
        // as an engine error at ERROR level on the success path.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        let errors = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(ErrorCounter(errors.clone()));
        let client = tracing::subscriber::with_default(subscriber, || {
            Connection::open_durable_local_brokered(
                dir.path(),
                DurableLocalOpenOptions::new(),
                IpcMode::Client,
                SessionAccess::ReadWrite,
            )
        })
        .expect("client brokers to the owner");

        assert!(
            !client.is_local(),
            "the client lost the lock and brokered to the owner"
        );
        assert_eq!(
            errors.load(Ordering::SeqCst),
            0,
            "a recovered lock contention must not log at ERROR (#3071)"
        );

        drop(client);
        owner.close().expect("owner close");
    }

    #[test]
    fn a_definitive_contention_refusal_still_logs_at_error() {
        // Direction control for #3071: the fix silences only *recovered*
        // contention (the Client broker path). A definitive refusal —
        // IpcMode::Off against a held store, which has no fallback — must still
        // cross the logging boundary so genuine contention stays diagnosable
        // (#3005). The lost lock carries an OS-error source chain, so the
        // boundary log fires.
        let dir = tempfile::tempdir().expect("tmp");
        let owner = Connection::open_durable_local_brokered(
            dir.path(),
            DurableLocalOpenOptions::new(),
            IpcMode::Host,
            SessionAccess::ReadWrite,
        )
        .expect("owner open");

        let errors = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(ErrorCounter(errors.clone()));
        let refused = tracing::subscriber::with_default(subscriber, || {
            Connection::open_durable_local_brokered(
                dir.path(),
                DurableLocalOpenOptions::new(),
                IpcMode::Off,
                SessionAccess::ReadWrite,
            )
        });

        let Err(error) = refused else {
            panic!("opt-out against a held store must refuse, not broker");
        };
        assert_eq!(error.code(), super::LOCK_CONTENTION_CODE);
        assert!(
            errors.load(Ordering::SeqCst) >= 1,
            "a definitive contention refusal must still log at ERROR (#3005)"
        );

        owner.close().expect("owner close");
    }
}
