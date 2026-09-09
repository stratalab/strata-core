//! The owner's IPC server: a thread-per-connection Unix-domain-socket listener
//! over the one `Executor` that holds the store's writer lock.
//!
//! Every connection's requests serialize through a shared `Mutex<Executor>` —
//! correct because the engine is single-session by construction (the
//! `Database` is non-`Clone` and every operation takes `&mut self`), so there
//! is exactly one executor per store and the mutex is the natural serialization
//! point. The socket and pid files are `0600`; on drop the server unlinks them.

use std::io::{self, BufReader, BufWriter};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::Executor;

use std::sync::atomic::AtomicU64;

use super::dispatch::execute_wire_request;
use super::protocol::{self, HelloFrame, ServerHello, SessionAccess, SubscribeFrame, WireRequest};
use super::{pid_path, resolve_binding, wire};

/// Concurrent handler-thread cap. Excess connections are refused with a
/// structured rejection frame rather than queued — the client's open dance
/// retries, and an embedded store never has a legitimate fan-out this wide.
pub(crate) const MAX_CONNECTIONS: usize = 128;
/// Write timeout for the capacity-rejection frame: the listener thread writes
/// it inline and must not stall on a slow or dead peer.
const REJECT_WRITE_TIMEOUT: Duration = Duration::from_millis(500);
/// Handler read timeout: bounds how long a handler blocks before re-checking
/// the shutdown flag, so `shutdown()` returns promptly.
const HANDLER_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// Read timeout for a version-tick-subscribed connection: each timeout tick
/// doubles as the watermark poll, so this bounds notification latency. The
/// watermark is a lock-free atomic — polling never touches the executor lane.
const NOTIFY_POLL: Duration = Duration::from_millis(150);
/// Listener idle poll when no connection is pending.
const ACCEPT_POLL: Duration = Duration::from_millis(50);

/// A running IPC server. Dropping it stops the listener and unlinks the socket
/// and pid files.
pub struct IpcServer {
    socket_path: PathBuf,
    pointer_path: Option<PathBuf>,
    pid_path: PathBuf,
    shutdown: Arc<AtomicBool>,
    /// Live registry of connected clients — registered/deregistered by each
    /// handler thread's RAII guard, upgraded with identity by hellos, and read
    /// by `ipc_status` through the injected host state.
    clients: crate::IpcClientRegistry,
    listener_handle: Option<JoinHandle<()>>,
}

impl IpcServer {
    /// Bind the owner's socket for `data_dir` and start serving `executor`.
    ///
    /// The caller has already won the writer lock (it holds the live
    /// `Executor`); this only adds the transport. A stale socket from a crashed
    /// prior owner is removed first.
    ///
    /// # Errors
    ///
    /// Any bind / permission / spawn failure. Callers treat this as best-effort
    /// (the store is still usable in-process; only brokering is unavailable).
    pub fn start(data_dir: &Path, executor: Arc<Mutex<Executor>>) -> io::Result<Self> {
        // Capture the owner's baseline session scope BEFORE any connection can
        // mutate it, so a request that omits branch/space resets to the store
        // default rather than inheriting a previous request's scope.
        let baseline = {
            let executor = executor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::new(Baseline {
                branch: executor.default_branch().to_owned(),
                space: executor.default_space().to_owned(),
            })
        };
        let binding = resolve_binding(data_dir);
        if let Some(parent) = binding.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if binding.socket.exists() {
            std::fs::remove_file(&binding.socket)?;
        }

        let listener = UnixListener::bind(&binding.socket)?;
        std::fs::set_permissions(&binding.socket, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;

        if let Some(pointer) = &binding.pointer {
            write_owner_file(pointer, binding.socket.to_string_lossy().as_bytes())?;
        }
        let pid_file = pid_path(data_dir);
        write_owner_file(&pid_file, std::process::id().to_string().as_bytes())?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let listener_shutdown = shutdown.clone();
        let clients = crate::IpcClientRegistry::new();
        let listener_clients = clients.clone();
        let listener_handle =
            thread::Builder::new()
                .name("ipc-listener".into())
                .spawn(move || {
                    listener_loop(
                        listener,
                        executor,
                        baseline,
                        listener_shutdown,
                        listener_clients,
                    );
                })?;

        Ok(Self {
            socket_path: binding.socket,
            pointer_path: binding.pointer,
            pid_path: pid_file,
            shutdown,
            clients,
            listener_handle: Some(listener_handle),
        })
    }

    /// The socket path this owner is listening on.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// The live client registry, shared with the handler threads.
    #[must_use]
    pub fn clients(&self) -> crate::IpcClientRegistry {
        self.clients.clone()
    }

    /// The number of currently connected clients.
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// The shutdown flag; setting it stops the listener (`ipc_stop`).
    #[must_use]
    pub fn stop_signal(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    /// Whether shutdown has been signaled (via `shutdown()`/`stop_signal()`, or
    /// an `ipc_stop` that flipped the shared flag). Unlike the presence of the
    /// server itself, this reflects the *live* state: a headless `strata start`
    /// owner polls it to learn when its hosting was stopped so it can exit.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Stop the listener, wait for it, and unlink the socket, pointer, and pid
    /// files. Idempotent; `Drop` calls the same cleanup.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
        self.cleanup_files();
    }

    fn cleanup_files(&self) {
        let _ = std::fs::remove_file(&self.socket_path);
        if let Some(pointer) = &self.pointer_path {
            let _ = std::fs::remove_file(pointer);
        }
        let _ = std::fs::remove_file(&self.pid_path);
    }

    /// Stop an owner identified by its pid file: send `SIGTERM`, wait briefly
    /// for exit, then remove the pid, socket, and pointer files. Idempotent.
    ///
    /// # Errors
    ///
    /// Filesystem errors reading the pid file; a missing pid file is success
    /// (no owner to stop).
    pub fn stop(data_dir: &Path) -> io::Result<()> {
        let pid_file = pid_path(data_dir);
        if let Ok(contents) = std::fs::read_to_string(&pid_file) {
            if let Ok(pid) = contents.trim().parse::<i32>() {
                // Send SIGTERM without an unsafe `libc::kill` — this crate is
                // `#![deny(unsafe_code)]`; `kill(1)` is a portable equivalent.
                let _ = std::process::Command::new("kill")
                    .arg("-TERM")
                    .arg(pid.to_string())
                    .status();
                for _ in 0..20 {
                    if !process_is_alive(pid) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
            let _ = std::fs::remove_file(&pid_file);
        }
        for name in [super::SOCKET_NAME, super::POINTER_NAME] {
            let _ = std::fs::remove_file(data_dir.join(name));
        }
        Ok(())
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
        self.cleanup_files();
    }
}

fn write_owner_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    std::fs::write(path, bytes)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

/// `kill -0` liveness probe (no unsafe): exit status 0 means the process
/// exists (or we may lack permission, which for a same-user pid means alive).
fn process_is_alive(pid: i32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// The owner's session scope at start — the reset target for requests that
/// omit branch/space.
struct Baseline {
    branch: String,
    space: String,
}

// The listener thread's entry point owns its resources for the thread's whole
// life (the listener, the shared executor/baseline handles, the shutdown flag),
// so by-value is correct even though `accept` only borrows.
#[allow(clippy::needless_pass_by_value)]
fn listener_loop(
    listener: UnixListener,
    executor: Arc<Mutex<Executor>>,
    baseline: Arc<Baseline>,
    shutdown: Arc<AtomicBool>,
    clients: crate::IpcClientRegistry,
) {
    let mut handlers: Vec<JoinHandle<()>> = Vec::new();
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                handlers.retain(|handle| !handle.is_finished());
                if handlers.len() >= MAX_CONNECTIONS {
                    tracing::warn!(
                        "IPC connection limit reached ({MAX_CONNECTIONS}); refusing connection"
                    );
                    reject_at_capacity(stream);
                    continue;
                }
                stream.set_nonblocking(false).ok();
                stream.set_read_timeout(Some(HANDLER_READ_TIMEOUT)).ok();
                let executor = executor.clone();
                let baseline = baseline.clone();
                let shutdown = shutdown.clone();
                let clients = clients.clone();
                match thread::Builder::new()
                    .name("ipc-handler".into())
                    .spawn(move || {
                        handle_connection(stream, &executor, &baseline, &shutdown, clients);
                    }) {
                    Ok(handle) => handlers.push(handle),
                    Err(error) => tracing::warn!("failed to spawn IPC handler: {error}"),
                }
            }
            Err(ref error) if is_transient_accept_error(error.kind()) => {
                thread::sleep(ACCEPT_POLL);
            }
            Err(error) => {
                if !shutdown.load(Ordering::SeqCst) {
                    tracing::warn!("IPC accept error: {error}");
                }
                break;
            }
        }
    }
    for handle in handlers {
        let _ = handle.join();
    }
}

/// Refuse a connection over the cap with a structured rejection frame, then
/// close it. Without the frame, a refused client watches a silent drop hang
/// until its own timeout; with it, the client learns why and can retry once a
/// slot frees.
fn reject_at_capacity(stream: UnixStream) {
    // Best-effort throughout: the peer may already be gone, and the listener
    // must keep accepting either way — a failed step just means the peer
    // learns nothing extra before the close it was getting anyway.
    let _ = stream.set_write_timeout(Some(REJECT_WRITE_TIMEOUT));
    let error = crate::ExecutorError::new(
        "resource_exhausted.executor.ipc_connections",
        format!(
            "the store owner is at its IPC connection capacity ({MAX_CONNECTIONS}); \
             retry once another client disconnects"
        ),
    );
    let payload = serde_json::to_string(&serde_json::json!({ "error": error }))
        .unwrap_or_else(|_| static_wire_request_error());
    let mut writer = BufWriter::new(stream);
    let _ = wire::write_frame(&mut writer, payload.as_bytes());
}

/// Monotonic per-connection id source, for registry keys in accept order.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// RAII client registration: registers an anonymous protocol-1 entry for this
/// handler thread's whole life and deregisters on every exit path (EOF,
/// timeout, error, or panic), so `ipc_status` never reports a stale
/// connection. A hello upgrades the entry in place via [`Self::introduce`].
struct ConnectionGuard {
    clients: crate::IpcClientRegistry,
    id: u64,
}

impl ConnectionGuard {
    fn enter(clients: crate::IpcClientRegistry) -> Self {
        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        clients.register(
            id,
            crate::IpcClientEntry {
                name: None,
                version: None,
                pid: None,
                access: SessionAccess::ReadWrite,
                protocol: 1,
            },
        );
        Self { clients, id }
    }

    /// Upgrade this connection's registry entry with what its hello declared.
    fn introduce(&self, entry: crate::IpcClientEntry) {
        self.clients.update(self.id, entry);
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.clients.deregister(self.id);
    }
}

fn handle_connection(
    stream: UnixStream,
    executor: &Arc<Mutex<Executor>>,
    baseline: &Baseline,
    shutdown: &AtomicBool,
    clients: crate::IpcClientRegistry,
) {
    let connected = ConnectionGuard::enter(clients);
    let Ok(read_half) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(stream);

    let mut awaiting_first_frame = true;
    // Whether this connection negotiated protocol revision 2 (hello accepted):
    // responses then carry the request's correlation id in a transport frame.
    let mut correlated = false;
    // The session's granted access. Full access until a hello declares
    // otherwise — a protocol-1 connection has no way to narrow itself.
    let mut access = SessionAccess::ReadWrite;
    // Version-tick subscription state, once this connection subscribes.
    let mut ticks: Option<TickState> = None;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let frame = match wire::read_frame(&mut reader) {
            Ok(frame) => frame,
            // A read timeout re-checks the shutdown flag and loops; anything
            // else (a clean EOF disconnect included) ends this connection.
            // For a subscribed connection each timeout is also the watermark
            // poll — the lowered NOTIFY_POLL timeout bounds tick latency.
            Err(ref e) if is_read_retry_error(e.kind()) => {
                if let Some(state) = &mut ticks {
                    if !state.push_if_advanced(&mut writer) {
                        break;
                    }
                }
                continue;
            }
            Err(_) => break,
        };

        // A hello is only meaningful as the connection's first frame: it
        // negotiates protocol revision 2 before any command. A first frame
        // that is instead a bare request keeps the connection on the implicit
        // protocol 1 — transitional acceptance, to be removed before the
        // release train once every in-family client sends a hello (#2872,
        // design doc §4.1). A hello arriving later is not sniffed and falls
        // through as a malformed request envelope.
        if std::mem::take(&mut awaiting_first_frame) && protocol::frame_is_hello(&frame) {
            match serve_hello(&frame) {
                Ok(outcome) => {
                    if wire::write_frame(&mut writer, outcome.response.as_bytes()).is_err() {
                        break;
                    }
                    correlated = true;
                    access = outcome.access;
                    // The hello introduced this connection: upgrade its
                    // anonymous registry entry so `ipc_status` can name it.
                    connected.introduce(crate::IpcClientEntry {
                        name: outcome.client.as_ref().map(|c| c.name.clone()),
                        version: outcome.client.as_ref().and_then(|c| c.version.clone()),
                        pid: outcome.client.as_ref().and_then(|c| c.pid).map(u64::from),
                        access: outcome.access,
                        protocol: protocol::PROTOCOL_VERSION,
                    });
                    continue;
                }
                Err(refusal) => {
                    // Best-effort refusal: the connection is closing over a
                    // protocol violation either way; a failed write of the
                    // refusal changes nothing for the server.
                    let _ = wire::write_frame(&mut writer, refusal.as_bytes());
                    break;
                }
            }
        }

        // A subscription frame is transport-level: it never dispatches through
        // the executor (and so is never gated — a read-only observer is
        // exactly who subscribes). Protocol-revision-2 connections only; on
        // protocol 1 it falls through as a malformed request envelope.
        if correlated && protocol::frame_is_subscribe(&frame) {
            let response = serve_subscribe(&frame, executor, &mut ticks);
            if ticks.is_some() {
                // Prompt ticks need a short poll; best-effort — a failure just
                // leaves the slower shutdown-check cadence in place.
                let _ = reader.get_ref().set_read_timeout(Some(NOTIFY_POLL));
            }
            if wire::write_frame(&mut writer, response.as_bytes()).is_err() {
                break;
            }
            continue;
        }

        let received_at = std::time::Instant::now();
        let response = if correlated {
            serve_one_correlated(executor, baseline, &frame, access, received_at)
        } else {
            serve_one(executor, baseline, &frame, access, received_at)
        };
        if wire::write_frame(&mut writer, response.as_bytes()).is_err() {
            break;
        }
        // A write this connection just dispatched advances the watermark; a
        // subscribed connection hears about it right away, not a poll later.
        if let Some(state) = &mut ticks {
            if !state.push_if_advanced(&mut writer) {
                break;
            }
        }
    }
}

/// A connection's version-tick subscription: the shared store-state watermark
/// plus the last value pushed. Coalescing is inherent — only the latest value
/// at each check is pushed, so a slow client gets fewer ticks, never a
/// backlog.
struct TickState {
    watermark: Arc<AtomicU64>,
    last_seen: u64,
}

impl TickState {
    /// Push one `{"notify":{"event":"version","version":N}}` frame if the
    /// watermark advanced past the last push. Returns false when the write
    /// fails (the connection is dead and the handler should exit).
    fn push_if_advanced(&mut self, writer: &mut BufWriter<UnixStream>) -> bool {
        let now = self.watermark.load(Ordering::Relaxed);
        if now == self.last_seen {
            return true;
        }
        self.last_seen = now;
        let frame = format!("{{\"notify\":{{\"event\":\"version\",\"version\":{now}}}}}");
        wire::write_frame(writer, frame.as_bytes()).is_ok()
    }
}

/// Serve a subscription frame: accept the supported event intersection, arm
/// the tick state when `version` is among them, and ack with the accepted set.
/// The baseline watermark is captured at subscribe time — the subscriber
/// re-reads current state anyway, so the first tick is the first *change*.
fn serve_subscribe(
    frame: &[u8],
    executor: &Arc<Mutex<Executor>>,
    ticks: &mut Option<TickState>,
) -> String {
    let request: SubscribeFrame = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(error) => {
            return correlate(
                None,
                &wire_request_error(&format!("malformed subscription frame: {error}")),
            )
        }
    };
    let Some(id) = request.id else {
        return correlate(
            None,
            &wire_request_error("a protocol-revision-2 subscription requires a correlation id"),
        );
    };
    let accepted: Vec<&str> = request
        .subscribe
        .events
        .iter()
        .filter(|event| event.as_str() == protocol::EVENT_VERSION)
        .map(String::as_str)
        .collect();
    if accepted.contains(&protocol::EVENT_VERSION) {
        let watermark = executor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state_version_handle();
        *ticks = Some(TickState {
            last_seen: watermark.load(Ordering::Relaxed),
            watermark,
        });
    }
    correlate(
        Some(id),
        &serde_json::to_string(
            &serde_json::json!({ "type": "ipc_subscribed", "data": { "events": accepted } }),
        )
        .unwrap_or_else(|error| serialize_hello_failure(&error)),
    )
}

/// An accepted hello: the response envelope to write, plus what the
/// connection learned about itself — the access it enforces from now on and
/// the identity for the client registry.
struct HelloOutcome {
    response: String,
    access: SessionAccess,
    client: Option<protocol::ClientIdentity>,
}

/// Serve a hello first frame: strict parse, protocol check, capability grant.
/// `Err` is a refusal envelope and the connection closes (a client that
/// cannot hello correctly has nothing safe to say next).
fn serve_hello(frame: &[u8]) -> Result<HelloOutcome, String> {
    let request: HelloFrame = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(error) => return Err(ipc_hello_error(&format!("malformed hello frame: {error}"))),
    };
    let hello = request.hello;
    if hello.protocol != protocol::PROTOCOL_VERSION {
        return Err(ipc_hello_error(&format!(
            "unsupported protocol revision {}; this owner speaks revision {}",
            hello.protocol,
            protocol::PROTOCOL_VERSION
        )));
    }
    // Declared access is granted as requested and enforced at the dispatch
    // gate for the connection's whole life; the identity feeds the client
    // registry `ipc_status` reports from.
    tracing::debug!(client = ?hello.client, access = ?hello.access, "IPC hello");
    let capabilities: Vec<String> = hello
        .capabilities
        .iter()
        .filter(|name| protocol::SUPPORTED_CAPABILITIES.contains(&name.as_str()))
        .cloned()
        .collect();
    let granted = hello.access;
    let response = ServerHello {
        protocol: protocol::PROTOCOL_VERSION,
        release: env!("CARGO_PKG_VERSION").to_owned(),
        idl: protocol::build_idl_stamps(),
        granted_access: granted,
        capabilities,
        owner_pid: std::process::id(),
    };
    Ok(HelloOutcome {
        response: serde_json::to_string(
            &serde_json::json!({ "type": "ipc_hello", "data": response }),
        )
        .unwrap_or_else(|error| serialize_hello_failure(&error)),
        access: granted,
        client: hello.client,
    })
}

/// Last-resort envelope for a hello response that fails to serialize —
/// practically unreachable with plain-data fields, mirrored on the dispatch
/// path's serialize fallback.
fn serialize_hello_failure(error: &serde_json::Error) -> String {
    let status = crate::ExecutorError::new(
        "internal.executor.wire_response",
        format!("hello response serialization failed: {error}"),
    );
    serde_json::to_string(&serde_json::json!({ "error": status }))
        .unwrap_or_else(|_| static_wire_request_error())
}

fn ipc_hello_error(detail: &str) -> String {
    let error = crate::ExecutorError::new(
        "invalid_argument.executor.ipc_hello",
        format!("IPC hello refused: {detail}"),
    );
    serde_json::to_string(&serde_json::json!({ "error": error }))
        .unwrap_or_else(|_| static_wire_request_error())
}

/// Serve one frame on the implicit protocol 1: decode and dispatch, answering
/// with the bare executor envelope. A correlation id on a protocol-1 request
/// is ignored rather than rejected — there is no response frame to echo it in,
/// and a pre-hello owner never knew the field existed.
fn serve_one(
    executor: &Arc<Mutex<Executor>>,
    baseline: &Baseline,
    frame: &[u8],
    access: SessionAccess,
    received_at: std::time::Instant,
) -> String {
    let request: WireRequest = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(error) => return wire_request_error(&error.to_string()),
    };
    dispatch_request(executor, baseline, &request, access, received_at)
}

/// Serve one frame on protocol revision 2: the request must carry a
/// correlation id, and the response is `{"id", "payload"}` with the untouched
/// executor envelope as the payload. `id` is `null` only when the request's
/// own id could not be read.
fn serve_one_correlated(
    executor: &Arc<Mutex<Executor>>,
    baseline: &Baseline,
    frame: &[u8],
    access: SessionAccess,
    received_at: std::time::Instant,
) -> String {
    let request: WireRequest = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(error) => return correlate(None, &wire_request_error(&error.to_string())),
    };
    let Some(id) = request.id else {
        return correlate(
            None,
            &wire_request_error("a protocol-revision-2 request requires a correlation id"),
        );
    };
    correlate(
        Some(id),
        &dispatch_request(executor, baseline, &request, access, received_at),
    )
}

/// Wrap an executor response envelope in a protocol-revision-2 response frame.
fn correlate(id: Option<u64>, payload: &str) -> String {
    match serde_json::value::RawValue::from_string(payload.to_owned()) {
        Ok(raw) => serde_json::to_string(&protocol::WireResponseFrame { id, payload: &raw })
            .unwrap_or_else(|error| {
                serialize_failure_frame(
                    id,
                    &format!("response frame serialization failed: {error}"),
                )
            }),
        // Unreachable with the well-formed JSON every response path produces;
        // kept total so a correlated connection always receives a frame.
        Err(error) => {
            serialize_failure_frame(id, &format!("response payload was not JSON: {error}"))
        }
    }
}

/// Last-resort correlated frame, hand-built so even the serialize-failure
/// path answers in the shape a protocol-revision-2 client is parsing.
fn serialize_failure_frame(id: Option<u64>, detail: &str) -> String {
    let id = id.map_or("null".to_owned(), |id| id.to_string());
    let message = detail.replace(['"', '\\'], "'");
    format!(
        "{{\"id\":{id},\"payload\":{{\"error\":{{\"class\":\"internal\",\
         \"code\":\"internal.executor.wire_response\",\"message\":\"{message}\"}}}}}}"
    )
}

/// Decode one parsed request's scope and dispatch it — with a panic guard so a
/// single misbehaving command cannot take down the owner. The scope
/// application and dispatch happen under one lock hold, so concurrent
/// connections cannot interleave between them.
fn dispatch_request(
    executor: &Arc<Mutex<Executor>>,
    baseline: &Baseline,
    request: &WireRequest,
    access: SessionAccess,
    received_at: std::time::Instant,
) -> String {
    // Each request fully determines the scope: its own branch/space, or the
    // owner's baseline when omitted — never a previous request's leftover.
    let branch = request.branch.as_deref().unwrap_or(&baseline.branch);
    let space = request.space.as_deref().unwrap_or(&baseline.space);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut executor = executor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Deadline shed: the budget may have expired while this request
        // waited for the execution lane (the lock above). Checked after
        // acquisition and before dispatch, so expired work is dropped
        // instead of piling onto a busy lane for a caller that gave up.
        if let Some(budget_ms) = request.deadline_ms {
            if received_at.elapsed() >= std::time::Duration::from_millis(budget_ms) {
                return Err(deadline_shed(budget_ms));
            }
        }
        executor.set_default_branch(branch.to_owned())?;
        executor.set_default_space(space.to_owned())?;
        Ok::<String, crate::ExecutorError>(execute_wire_request(
            &mut executor,
            request.command.get(),
            access,
        ))
    }));
    match outcome {
        Ok(Ok(response)) => response,
        // A rejected session scope (e.g. an invalid branch name) is a clean
        // error envelope, not a transport failure.
        Ok(Err(error)) => serde_json::to_string(&serde_json::json!({ "error": error }))
            .unwrap_or_else(|_| wire_request_error("session scope rejected")),
        Err(_) => internal_panic_error(),
    }
}

/// A transient `accept()` outcome (the listener's non-blocking poll expiring)
/// versus a real listener failure. Transient → re-poll; anything else ends the
/// listener. Extracted as a pure predicate so its classification is unit-tested
/// directly (the accept loop itself is OS-timing-driven).
fn is_transient_accept_error(kind: io::ErrorKind) -> bool {
    kind == io::ErrorKind::WouldBlock
}

/// A read error that should keep the connection alive (the per-request read
/// timeout firing) versus one that ends it (a clean EOF or hard error).
/// Extracted as a pure predicate for the same reason.
fn is_read_retry_error(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}

fn wire_request_error(detail: &str) -> String {
    let error = crate::ExecutorError::new(
        "invalid_argument.executor.wire_request",
        format!("malformed IPC request envelope: {detail}"),
    );
    serde_json::to_string(&serde_json::json!({ "error": error }))
        .unwrap_or_else(|_| static_wire_request_error())
}

/// The registered rejection for a request whose deadline expired before
/// dispatch. Retryable by policy (`same_request`): the same request may well
/// succeed once the lane is less busy.
fn deadline_shed(budget_ms: u64) -> crate::ExecutorError {
    crate::ExecutorError::new(
        "unavailable.executor.ipc_deadline",
        format!("the request's {budget_ms}ms deadline expired before dispatch"),
    )
}

fn internal_panic_error() -> String {
    let error = crate::ExecutorError::new(
        "internal.executor.wire_response",
        "the IPC handler panicked while serving a command",
    );
    serde_json::to_string(&serde_json::json!({ "error": error }))
        .unwrap_or_else(|_| static_wire_request_error())
}

fn static_wire_request_error() -> String {
    "{\"error\":{\"class\":\"invalid_argument\",\"code\":\"invalid_argument.executor.wire_request\",\
     \"message\":\"malformed IPC request envelope\"}}"
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::IpcServer;
    use crate::ipc::protocol::WireRequestOwned;
    use crate::ipc::wire;
    use crate::Executor;
    use crate::SessionAccess;
    use std::io::{BufReader, BufWriter};
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};

    /// Send a legacy (protocol 1) wire command with optional scope and read
    /// the bare response envelope.
    fn round_trip(
        sock: &std::path::Path,
        branch: Option<&str>,
        command: &str,
    ) -> serde_json::Value {
        let stream = UnixStream::connect(sock).expect("connect");
        let mut writer = BufWriter::new(stream.try_clone().expect("clone"));
        let mut reader = BufReader::new(stream);
        let raw = serde_json::value::RawValue::from_string(command.to_owned()).expect("raw");
        let request = WireRequestOwned {
            id: None,
            deadline_ms: None,
            branch,
            space: None,
            command: &raw,
        };
        let payload = serde_json::to_vec(&request).expect("serialize");
        wire::write_frame(&mut writer, &payload).expect("write");
        let response = wire::read_frame(&mut reader).expect("read");
        serde_json::from_slice(&response).expect("decode response")
    }

    #[test]
    fn process_is_alive_distinguishes_a_live_pid_from_a_dead_one() {
        // Our own process is alive; i32::MAX is not a running pid.
        let own = i32::try_from(std::process::id()).expect("pid fits i32");
        assert!(super::process_is_alive(own), "this process is alive");
        assert!(
            !super::process_is_alive(i32::MAX),
            "i32::MAX is not a running process"
        );
    }

    #[test]
    fn transient_accept_errors_are_exactly_would_block() {
        use std::io::ErrorKind;
        // Only a poll-timeout (WouldBlock) re-polls; every other kind ends the
        // listener. A wrong predicate would either spin on real failures or drop
        // the listener on a benign timeout.
        assert!(super::is_transient_accept_error(ErrorKind::WouldBlock));
        assert!(!super::is_transient_accept_error(ErrorKind::TimedOut));
        assert!(!super::is_transient_accept_error(
            ErrorKind::ConnectionAborted
        ));
        assert!(!super::is_transient_accept_error(ErrorKind::Other));
    }

    #[test]
    fn read_retry_errors_are_the_timeout_kinds_only() {
        use std::io::ErrorKind;
        // A read timeout (WouldBlock OR TimedOut, platform-dependent) keeps the
        // connection alive; a clean EOF or hard error ends it. `&&` instead of
        // `||`, or dropping either kind, would strand paused clients.
        assert!(super::is_read_retry_error(ErrorKind::WouldBlock));
        assert!(super::is_read_retry_error(ErrorKind::TimedOut));
        assert!(!super::is_read_retry_error(ErrorKind::UnexpectedEof));
        assert!(!super::is_read_retry_error(ErrorKind::BrokenPipe));
    }

    #[test]
    fn dropping_the_server_without_an_explicit_shutdown_still_cleans_up() {
        // RAII: an owner dropped without an explicit shutdown() — e.g. a panic
        // unwinds its scope — must still unlink its socket and pid file, or a
        // crashed owner would strand a dead socket that blocks reconnection.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let sock;
        let pid = dir.path().join("strata.pid");
        {
            let server = IpcServer::start(dir.path(), executor).expect("start");
            sock = server.socket_path().to_path_buf();
            assert!(sock.exists(), "socket present while the owner is alive");
            assert!(pid.exists(), "pid file present while the owner is alive");
            // `server` drops here — no explicit shutdown().
        }
        assert!(!sock.exists(), "socket unlinked on drop");
        assert!(!pid.exists(), "pid file unlinked on drop");
    }

    #[test]
    fn is_stopped_tracks_the_shutdown_signal() {
        // A live host reports not-stopped; setting the stop signal (the exact
        // flag `ipc_stop` flips) makes it report stopped, without dropping the
        // server. `strata start` polls this to learn when to exit.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        assert!(!server.is_stopped(), "a running host is not stopped");
        server
            .stop_signal()
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            server.is_stopped(),
            "flipping the shared stop signal is observed as stopped"
        );
        server.shutdown();
        assert!(server.is_stopped(), "an explicit shutdown reports stopped");
    }

    #[test]
    fn start_writes_an_owner_pid_file_and_shutdown_removes_it() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let pid_file = dir.path().join("strata.pid");
        let recorded = std::fs::read_to_string(&pid_file).expect("pid file written");
        assert_eq!(
            recorded.trim(),
            std::process::id().to_string(),
            "pid file records our pid"
        );
        server.shutdown();
        assert!(!pid_file.exists(), "pid file removed on shutdown");
    }

    #[test]
    fn stop_removes_the_socket_and_pid_files_for_a_dead_owner() {
        // A crashed owner leaves stale files with a dead pid; stop() clears them.
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::write(dir.path().join("strata.pid"), i32::MAX.to_string()).expect("pid");
        std::fs::write(dir.path().join("strata.sock"), b"").expect("sock");
        IpcServer::stop(dir.path()).expect("stop");
        assert!(!dir.path().join("strata.pid").exists(), "pid removed");
        assert!(!dir.path().join("strata.sock").exists(), "socket removed");
    }

    #[test]
    fn a_well_formed_json_frame_that_is_not_a_request_returns_a_wire_request_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let sock = server.socket_path().to_path_buf();

        // Valid JSON, but not a `{branch,space,command}` envelope.
        let stream = UnixStream::connect(&sock).expect("connect");
        let mut writer = BufWriter::new(stream.try_clone().expect("clone"));
        let mut reader = BufReader::new(stream);
        wire::write_frame(&mut writer, b"[1,2,3]").expect("write");
        let response = wire::read_frame(&mut reader).expect("read");
        let response: serde_json::Value = serde_json::from_slice(&response).expect("decode");
        assert_eq!(
            response["error"]["code"], "invalid_argument.executor.wire_request",
            "a non-envelope frame is a wire_request error: {response}"
        );

        server.shutdown();
    }

    #[test]
    fn a_connection_idle_past_the_read_timeout_is_still_served() {
        // The handler's read timeout must CONTINUE (re-check shutdown) rather
        // than drop the connection — a client that pauses longer than
        // HANDLER_READ_TIMEOUT then sends a command must still get a response.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let sock = server.socket_path().to_path_buf();

        let stream = UnixStream::connect(&sock).expect("connect");
        let mut writer = BufWriter::new(stream.try_clone().expect("clone"));
        let mut reader = BufReader::new(stream);
        std::thread::sleep(super::HANDLER_READ_TIMEOUT + std::time::Duration::from_millis(500));

        let raw = serde_json::value::RawValue::from_string("{\"type\":\"ping\"}".to_owned())
            .expect("raw");
        let request = WireRequestOwned {
            id: None,
            deadline_ms: None,
            branch: None,
            space: None,
            command: &raw,
        };
        wire::write_frame(&mut writer, &serde_json::to_vec(&request).expect("ser")).expect("write");
        let response = wire::read_frame(&mut reader).expect("handler continued past the timeout");
        let response: serde_json::Value = serde_json::from_slice(&response).expect("decode");
        assert_eq!(response["type"], "pong", "served after an idle pause");

        server.shutdown();
    }

    /// One raw connection with reader/writer halves, for multi-frame tests.
    struct RawConn {
        writer: BufWriter<UnixStream>,
        reader: BufReader<UnixStream>,
    }

    impl RawConn {
        fn connect(sock: &std::path::Path) -> Self {
            let stream = UnixStream::connect(sock).expect("connect");
            Self {
                writer: BufWriter::new(stream.try_clone().expect("clone")),
                reader: BufReader::new(stream),
            }
        }

        fn send(&mut self, payload: &[u8]) -> serde_json::Value {
            wire::write_frame(&mut self.writer, payload).expect("write");
            let response = wire::read_frame(&mut self.reader).expect("read");
            serde_json::from_slice(&response).expect("decode response")
        }

        /// A legacy (protocol 1) request: no correlation id.
        fn send_command(&mut self, command: &str) -> serde_json::Value {
            let raw = serde_json::value::RawValue::from_string(command.to_owned()).expect("raw");
            let request = WireRequestOwned {
                id: None,
                deadline_ms: None,
                branch: None,
                space: None,
                command: &raw,
            };
            self.send(&serde_json::to_vec(&request).expect("serialize"))
        }

        /// A protocol-revision-2 request carrying a correlation id.
        fn send_correlated(&mut self, id: u64, command: &str) -> serde_json::Value {
            let raw = serde_json::value::RawValue::from_string(command.to_owned()).expect("raw");
            let request = WireRequestOwned {
                id: Some(id),
                deadline_ms: None,
                branch: None,
                space: None,
                command: &raw,
            };
            self.send(&serde_json::to_vec(&request).expect("serialize"))
        }
    }

    #[test]
    fn a_connection_over_the_cap_is_refused_with_a_structured_frame() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let sock = server.socket_path().to_path_buf();

        // Saturate the cap with idle connections and wait until every handler
        // is live — accepts are asynchronous, so the shared client count is
        // the only honest gate before probing the cap.
        let held: Vec<UnixStream> = (0..super::MAX_CONNECTIONS)
            .map(|_| UnixStream::connect(&sock).expect("saturating connect"))
            .collect();
        for _ in 0..500 {
            if server.client_count() >= super::MAX_CONNECTIONS {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            server.client_count(),
            super::MAX_CONNECTIONS,
            "all saturating connections have live handlers"
        );

        // The next connection is refused with the registered frame — written
        // unprompted, before any client frame — and then closed.
        let over = UnixStream::connect(&sock).expect("connect over the cap");
        let mut reader = BufReader::new(over);
        let frame = wire::read_frame(&mut reader).expect("the rejection frame arrives");
        let value: serde_json::Value = serde_json::from_slice(&frame).expect("decode");
        assert_eq!(
            value["error"]["code"], "resource_exhausted.executor.ipc_connections",
            "refused with the registered capacity code: {value}"
        );
        assert_eq!(value["error"]["class"], "resource_exhausted");
        assert_eq!(value["error"]["retry_policy"], "same_request");
        assert!(
            wire::read_frame(&mut reader).is_err(),
            "closed after the refusal"
        );

        drop(held);
        server.shutdown();
    }

    #[test]
    fn a_hello_first_frame_negotiates_protocol_2_and_commands_follow() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let hello = conn.send(b"{\"hello\":{\"protocol\":2}}");
        assert_eq!(hello["type"], "ipc_hello", "hello answered: {hello}");
        assert_eq!(hello["data"]["protocol"], 2);
        assert_eq!(hello["data"]["release"], env!("CARGO_PKG_VERSION"));
        assert_eq!(hello["data"]["idl"]["schema_version"], "strata.idl.v1");
        assert_eq!(
            hello["data"]["idl"]["generator_version"],
            "strata-executor-idl.1"
        );
        assert_eq!(hello["data"]["granted_access"], "read_write");
        assert_eq!(
            hello["data"]["owner_pid"],
            u64::from(std::process::id()),
            "the owner reports its own pid"
        );
        assert_eq!(
            hello["data"]["capabilities"],
            serde_json::json!([]),
            "nothing granted from an empty want-list"
        );

        // The same connection then serves commands, correlated: the response
        // is an {"id","payload"} frame echoing the request id, with the
        // classic executor envelope untouched inside.
        let pong = conn.send_correlated(1, "{\"type\":\"ping\"}");
        assert_eq!(pong["id"], 1, "the response echoes the request id");
        assert_eq!(
            pong["payload"]["type"], "pong",
            "the payload is the classic envelope: {pong}"
        );

        server.shutdown();
    }

    #[test]
    fn an_unsupported_protocol_revision_is_refused_and_the_connection_closes() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let refusal = conn.send(b"{\"hello\":{\"protocol\":99}}");
        assert_eq!(
            refusal["error"]["code"], "invalid_argument.executor.ipc_hello",
            "refused with the registered hello code: {refusal}"
        );
        assert_eq!(refusal["error"]["class"], "invalid_argument");

        // The refusal closes the connection: a further exchange fails — the
        // write may already see a broken pipe, or the read reaches EOF —
        // rather than hanging or being served.
        let followup = wire::write_frame(&mut conn.writer, b"{\"command\":{\"type\":\"ping\"}}")
            .and_then(|()| wire::read_frame(&mut conn.reader));
        assert!(
            followup.is_err(),
            "a refused connection serves nothing further"
        );

        server.shutdown();
    }

    #[test]
    fn a_malformed_hello_is_refused_not_guessed() {
        // deny_unknown_fields is the hello's evolution contract: a field this
        // owner does not know cannot be silently absorbed, because the client
        // chose protocol semantics the owner would then be faking.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let refusal = conn.send(b"{\"hello\":{\"protocol\":2,\"surprise\":true}}");
        assert_eq!(
            refusal["error"]["code"],
            "invalid_argument.executor.ipc_hello"
        );

        server.shutdown();
    }

    #[test]
    fn capability_grants_are_the_supported_intersection() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let hello = conn
            .send(b"{\"hello\":{\"protocol\":2,\"capabilities\":[\"notify.version\",\"bogus\"]}}");
        assert_eq!(hello["type"], "ipc_hello", "probing is not an error");
        assert_eq!(
            hello["data"]["capabilities"],
            serde_json::json!(["notify.version"]),
            "the supported capability is granted; the unknown one is ignored"
        );

        server.shutdown();
    }

    /// Open a hello'd connection subscribed to version ticks.
    fn subscribed_conn(sock: &std::path::Path, access: &str) -> RawConn {
        let mut conn = RawConn::connect(sock);
        let hello = format!("{{\"hello\":{{\"protocol\":2,\"access\":\"{access}\"}}}}");
        assert_eq!(conn.send(hello.as_bytes())["type"], "ipc_hello");
        let ack = conn.send(b"{\"id\":1,\"subscribe\":{\"events\":[\"version\"]}}");
        assert_eq!(ack["id"], 1);
        assert_eq!(ack["payload"]["type"], "ipc_subscribed", "acked: {ack}");
        assert_eq!(
            ack["payload"]["data"]["events"],
            serde_json::json!(["version"])
        );
        conn
    }

    /// Blocking-read one frame and decode it (used for expected pushes).
    fn read_push(conn: &mut RawConn) -> serde_json::Value {
        let frame = wire::read_frame(&mut conn.reader).expect("push arrives");
        serde_json::from_slice(&frame).expect("push decodes")
    }

    /// One owner-local write, never crossing the socket.
    fn owner_local_put(executor: &Arc<Mutex<Executor>>, value: &str) {
        let put: crate::Command = serde_json::from_value(
            serde_json::json!({ "type": "kv_put", "key": "aGk=", "value": value }),
        )
        .expect("command");
        executor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .execute(put)
            .expect("owner-local write");
    }

    #[test]
    fn a_subscriber_is_ticked_by_its_own_writes_immediately() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = subscribed_conn(server.socket_path(), "read_write");

        let put = conn.send_correlated(
            2,
            "{\"type\":\"kv_put\",\"key\":\"aGk=\",\"value\":\"dg==\"}",
        );
        assert_eq!(put["payload"]["type"], "write_result");
        // The tick follows the response on the same connection — pushed after
        // the serve, not a poll interval later.
        let push = read_push(&mut conn);
        assert_eq!(push["notify"]["event"], "version", "a tick: {push}");
        assert_eq!(push["notify"]["version"], 1, "one write so far");

        server.shutdown();
    }

    #[test]
    fn owner_local_writes_reach_a_read_only_subscriber() {
        // The headline scenario: the owner process writes in-process (never
        // crossing the socket), and a read-only observer hears about it. The
        // watermark lives at the executor choke point, so no write path can
        // bypass it.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor.clone()).expect("start");
        let mut conn = subscribed_conn(server.socket_path(), "read");

        owner_local_put(&executor, "dg==");

        let push = read_push(&mut conn);
        assert_eq!(push["notify"]["event"], "version");
        assert_eq!(
            push["notify"]["version"], 1,
            "the local write ticked: {push}"
        );

        server.shutdown();
    }

    #[test]
    fn ticks_are_coalesced_and_strictly_increasing() {
        // Lossy latest-wins semantics: a burst of writes may surface as fewer
        // frames (typically one), every frame carries a newer value than the
        // last, and the final observed value is the final watermark.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor.clone()).expect("start");
        let mut conn = subscribed_conn(server.socket_path(), "read");

        for _ in 0..5u8 {
            owner_local_put(&executor, "dg==");
        }

        let mut last = 0u64;
        loop {
            let push = read_push(&mut conn);
            let version = push["notify"]["version"].as_u64().expect("version");
            assert!(
                version > last,
                "ticks are strictly increasing (got {version} after {last})"
            );
            last = version;
            if version == 5 {
                break;
            }
        }

        server.shutdown();
    }

    #[test]
    fn an_unsubscribed_connection_receives_no_pushes() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor.clone()).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        owner_local_put(&executor, "dg==");

        // No subscription — nothing may arrive. A bounded read must time out.
        conn.reader
            .get_ref()
            .set_read_timeout(Some(std::time::Duration::from_millis(500)))
            .expect("set timeout");
        assert!(
            wire::read_frame(&mut conn.reader).is_err(),
            "an unsubscribed connection is never pushed to"
        );

        server.shutdown();
    }

    #[test]
    fn a_subscription_with_only_unknown_events_acks_empty_and_never_ticks() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor.clone()).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let ack = conn.send(b"{\"id\":1,\"subscribe\":{\"events\":[\"weather\"]}}");
        assert_eq!(
            ack["payload"]["data"]["events"],
            serde_json::json!([]),
            "unknown events are ignored, mirroring capability grants: {ack}"
        );

        owner_local_put(&executor, "dg==");
        conn.reader
            .get_ref()
            .set_read_timeout(Some(std::time::Duration::from_millis(500)))
            .expect("set timeout");
        assert!(
            wire::read_frame(&mut conn.reader).is_err(),
            "an empty subscription never ticks"
        );

        server.shutdown();
    }

    #[test]
    fn a_request_within_its_deadline_executes_normally() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let response =
            conn.send(b"{\"id\":1,\"deadline_ms\":60000,\"command\":{\"type\":\"ping\"}}");
        assert_eq!(response["id"], 1);
        assert_eq!(
            response["payload"]["type"], "pong",
            "a live budget executes"
        );

        server.shutdown();
    }

    #[test]
    fn an_already_expired_deadline_is_shed_with_the_registered_code() {
        // deadline_ms: 0 is "already expired" by construction (elapsed >= 0),
        // making the shed deterministic without sleeps or contention.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let shed = conn.send(
            b"{\"id\":1,\"deadline_ms\":0,\
              \"command\":{\"type\":\"kv_put\",\"key\":\"aGk=\",\"value\":\"dg==\"}}",
        );
        assert_eq!(shed["id"], 1, "the shed is correlated");
        assert_eq!(
            shed["payload"]["error"]["code"], "unavailable.executor.ipc_deadline",
            "shed with the registered code: {shed}"
        );
        assert_eq!(shed["payload"]["error"]["retry_policy"], "same_request");
        assert_eq!(shed["payload"]["error"]["commit_outcome"], "not_started");

        // Shed before dispatch: the write never happened.
        let get = conn.send_correlated(2, "{\"type\":\"kv_get\",\"key\":\"aGk=\"}");
        assert_eq!(
            get["payload"]["data"]["found"], false,
            "nothing was written by the shed request"
        );

        server.shutdown();
    }

    #[test]
    fn a_deadline_that_expires_waiting_for_the_lane_is_shed() {
        // The motivating case: the lane is busy (here: the test holds the
        // executor lock, standing in for another connection's long command),
        // the client's budget expires during the wait, and the request is
        // shed at lock acquisition instead of executing for a caller that
        // has already given up.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor.clone()).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let lane = executor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        wire::write_frame(
            &mut conn.writer,
            b"{\"id\":1,\"deadline_ms\":50,\"command\":{\"type\":\"ping\"}}",
        )
        .expect("write while the lane is held");
        // Hold the lane well past the request's 50ms budget, then release.
        std::thread::sleep(std::time::Duration::from_millis(200));
        drop(lane);

        let frame = wire::read_frame(&mut conn.reader).expect("shed response");
        let shed: serde_json::Value = serde_json::from_slice(&frame).expect("decode");
        assert_eq!(
            shed["payload"]["error"]["code"], "unavailable.executor.ipc_deadline",
            "the expired wait was shed, not executed: {shed}"
        );

        server.shutdown();
    }

    #[test]
    fn a_legacy_request_deadline_is_honored_with_a_bare_envelope() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let shed = conn.send(b"{\"deadline_ms\":0,\"command\":{\"type\":\"ping\"}}");
        assert_eq!(
            shed["error"]["code"], "unavailable.executor.ipc_deadline",
            "bare envelope on protocol 1: {shed}"
        );
        assert!(shed.get("id").is_none(), "no frame wrapper on protocol 1");

        server.shutdown();
    }

    #[test]
    fn the_client_registry_names_hello_clients_and_keeps_legacy_anonymous() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");

        // A legacy connection: first frame is a bare request, no hello.
        let mut legacy = RawConn::connect(server.socket_path());
        assert_eq!(legacy.send_command("{\"type\":\"ping\"}")["type"], "pong");

        // A hello'd connection introducing itself with identity + read access.
        let mut named = RawConn::connect(server.socket_path());
        let hello = named.send(
            b"{\"hello\":{\"protocol\":2,\"access\":\"read\",\
              \"client\":{\"name\":\"strata-vscode\",\"version\":\"0.1.0\",\"pid\":4242}}}",
        );
        assert_eq!(hello["type"], "ipc_hello");

        let clients = server.clients().snapshot();
        assert_eq!(clients.len(), 2, "both connections are registered");
        let anon = clients
            .iter()
            .find(|c| c.protocol == 1)
            .expect("the legacy connection appears");
        assert!(
            anon.name.is_none() && anon.version.is_none() && anon.pid.is_none(),
            "a pre-hello connection is anonymous"
        );
        assert_eq!(anon.access, SessionAccess::ReadWrite);
        let named_entry = clients
            .iter()
            .find(|c| c.protocol == 2)
            .expect("the hello'd connection appears");
        assert_eq!(named_entry.name.as_deref(), Some("strata-vscode"));
        assert_eq!(named_entry.version.as_deref(), Some("0.1.0"));
        assert_eq!(named_entry.pid, Some(4242));
        assert_eq!(named_entry.access, SessionAccess::Read);

        // Disconnects deregister on every exit path (the RAII guard).
        drop(named);
        drop(legacy);
        for _ in 0..200 {
            if server.client_count() == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(server.client_count(), 0, "gone clients leave no entries");

        server.shutdown();
    }

    #[test]
    fn a_subscription_without_an_id_is_refused() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let refused = conn.send(b"{\"subscribe\":{\"events\":[\"version\"]}}");
        assert_eq!(refused["id"], serde_json::Value::Null);
        assert_eq!(
            refused["payload"]["error"]["code"],
            "invalid_argument.executor.wire_request"
        );

        server.shutdown();
    }

    #[test]
    fn a_read_session_write_is_rejected_and_reads_still_serve() {
        // The access a hello declares is enforced for the connection's whole
        // life: writes are rejected with the registered read-only code, reads
        // pass, and the rejection does not end the connection.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let hello = conn.send(b"{\"hello\":{\"protocol\":2,\"access\":\"read\"}}");
        assert_eq!(hello["data"]["granted_access"], "read");

        let put = conn.send_correlated(
            1,
            "{\"type\":\"kv_put\",\"key\":\"aGk=\",\"value\":\"dg==\"}",
        );
        assert_eq!(
            put["payload"]["error"]["code"], "access_denied.executor.read_only_session",
            "a write on a read session is rejected: {put}"
        );
        assert_eq!(put["payload"]["error"]["class"], "access_denied");

        let get = conn.send_correlated(2, "{\"type\":\"kv_get\",\"key\":\"aGk=\"}");
        assert_eq!(
            get["payload"]["data"]["found"], false,
            "the gate held before dispatch (nothing written) and reads serve"
        );

        server.shutdown();
    }

    #[test]
    fn a_correlated_request_without_an_id_is_refused_with_a_null_id_frame() {
        // Protocol 2 requires the id: without one the client cannot correlate
        // the answer, so the refusal itself says which request it refuses the
        // only way it can — id null, in a frame the client is parsing.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );
        let refused = conn.send_command("{\"type\":\"ping\"}");
        assert_eq!(refused["id"], serde_json::Value::Null);
        assert_eq!(
            refused["payload"]["error"]["code"],
            "invalid_argument.executor.wire_request"
        );
        assert_eq!(
            conn.send_correlated(1, "{\"type\":\"ping\"}")["payload"]["type"],
            "pong",
            "the connection survives the refusal"
        );

        server.shutdown();
    }

    #[test]
    fn pipelined_requests_are_answered_in_order_with_their_own_ids() {
        // The protocol permits writing ahead: two frames sent back-to-back are
        // answered in request order, each response carrying its request's id.
        // (One-in-flight is client discipline, not a server rule.)
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());
        assert_eq!(
            conn.send(b"{\"hello\":{\"protocol\":2}}")["type"],
            "ipc_hello"
        );

        let first = frame_for(7, "{\"type\":\"ping\"}");
        let second = frame_for(8, "{\"type\":\"kv_count\"}");
        wire::write_frame(&mut conn.writer, &first).expect("write first");
        wire::write_frame(&mut conn.writer, &second).expect("write second");

        let responses: Vec<serde_json::Value> = (0..2)
            .map(|_| {
                let bytes = wire::read_frame(&mut conn.reader).expect("read");
                serde_json::from_slice(&bytes).expect("decode")
            })
            .collect();
        assert_eq!(responses[0]["id"], 7, "first in, first answered");
        assert_eq!(responses[0]["payload"]["type"], "pong");
        assert_eq!(responses[1]["id"], 8);
        assert_eq!(responses[1]["payload"]["type"], "uint");

        server.shutdown();
    }

    /// Serialize one correlated request frame without sending it.
    fn frame_for(id: u64, command: &str) -> Vec<u8> {
        let raw = serde_json::value::RawValue::from_string(command.to_owned()).expect("raw");
        serde_json::to_vec(&WireRequestOwned {
            id: Some(id),
            deadline_ms: None,
            branch: None,
            space: None,
            command: &raw,
        })
        .expect("serialize")
    }

    #[test]
    fn a_stray_id_on_a_legacy_connection_is_ignored_not_echoed() {
        // Pins protocol-1 semantics exactly: no hello means no response frame,
        // even when the request smuggles an id — a legacy connection's reply
        // is the bare envelope, byte-compatible with every pre-hello client.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        let response = conn.send_correlated(42, "{\"type\":\"ping\"}");
        assert_eq!(response["type"], "pong", "bare envelope: {response}");
        assert!(
            response.get("id").is_none(),
            "no frame wrapper on protocol 1"
        );

        server.shutdown();
    }

    #[test]
    fn a_hello_after_the_first_frame_is_a_malformed_request_envelope() {
        // The hello negotiates a connection, not a request — mid-connection it
        // is just a frame with no `command`, and the connection survives it.
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let mut conn = RawConn::connect(server.socket_path());

        assert_eq!(conn.send_command("{\"type\":\"ping\"}")["type"], "pong");
        let late_hello = conn.send(b"{\"hello\":{\"protocol\":2}}");
        assert_eq!(
            late_hello["error"]["code"],
            "invalid_argument.executor.wire_request"
        );
        assert_eq!(
            conn.send_command("{\"type\":\"ping\"}")["type"],
            "pong",
            "the connection continues after the stray hello"
        );

        server.shutdown();
    }

    #[test]
    fn serves_commands_and_error_envelopes_over_the_socket() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let sock = server.socket_path().to_path_buf();

        // A put then a get round-trip the same value.
        let put = round_trip(
            &sock,
            None,
            "{\"type\":\"kv_put\",\"key\":\"aGk=\",\"value\":\"dGhlcmU=\"}",
        );
        assert_eq!(put["type"], "write_result", "put succeeded: {put}");
        let get = round_trip(&sock, None, "{\"type\":\"kv_get\",\"key\":\"aGk=\"}");
        assert_eq!(get["type"], "kv_versioned_value");
        assert_eq!(get["data"]["value"]["value"], "dGhlcmU=");

        // A malformed command comes back as a registered error envelope.
        let bad = round_trip(&sock, None, "{\"type\":\"nope\"}");
        assert_eq!(
            bad["error"]["code"],
            "invalid_argument.executor.wire_request"
        );

        server.shutdown();
        assert!(!sock.exists(), "socket removed on shutdown");
    }

    #[test]
    fn per_request_branch_scope_is_applied() {
        let dir = tempfile::tempdir().expect("tmp");
        let executor = Arc::new(Mutex::new(Executor::open_cache().expect("cache")));
        let mut server = IpcServer::start(dir.path(), executor).expect("start");
        let sock = server.socket_path().to_path_buf();

        // Create a branch, then a put/get scoped to it via the request envelope
        // (no explicit branch on the command) must target that branch.
        let created = round_trip(
            &sock,
            None,
            "{\"type\":\"branch_create\",\"branch\":\"feature\"}",
        );
        assert_eq!(created["type"], "branch", "branch created: {created}");
        round_trip(
            &sock,
            Some("feature"),
            "{\"type\":\"kv_put\",\"key\":\"aw==\",\"value\":\"dg==\"}",
        );
        let on_feature = round_trip(
            &sock,
            Some("feature"),
            "{\"type\":\"kv_get\",\"key\":\"aw==\"}",
        );
        assert_eq!(
            on_feature["data"]["value"]["value"], "dg==",
            "value on feature branch"
        );
        let on_main = round_trip(&sock, None, "{\"type\":\"kv_get\",\"key\":\"aw==\"}");
        assert_eq!(
            on_main["data"]["found"], false,
            "key absent on the default branch"
        );

        server.shutdown();
    }
}
