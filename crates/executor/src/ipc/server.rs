//! IPC server that listens on a Unix domain socket.
//!
//! The server holds the database and handles commands from multiple clients.
//! Each client connection gets its own `Session` for transaction isolation.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use strata_engine::Database;
use strata_security::AccessMode;

use crate::Session;

use super::protocol::{self, Request, Response};
use super::wire;

/// IPC server that listens on a Unix domain socket and dispatches
/// commands to the database via per-connection sessions.
pub struct IpcServer {
    socket_path: PathBuf,
    pid_path: PathBuf,
    shutdown: Arc<AtomicBool>,
    listener_handle: Option<JoinHandle<()>>,
}

impl IpcServer {
    /// Start the IPC server, binding to `<data_dir>/strata.sock`.
    ///
    /// Spawns a listener thread that accepts connections. Each connection
    /// gets a dedicated handler thread with its own `Session`.
    pub fn start(
        data_dir: &Path,
        db: Arc<Database>,
        access_mode: AccessMode,
    ) -> io::Result<Self> {
        let socket_path = data_dir.join("strata.sock");
        let pid_path = data_dir.join("strata.pid");

        // Remove stale socket if it exists
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let listener = UnixListener::bind(&socket_path)?;
        // Restrict socket to owner-only access. This is an embedded database
        // IPC mechanism, not a network server — only the owning user's
        // processes should be able to connect.
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;

        // Write PID file (owner-only permissions)
        std::fs::write(&pid_path, std::process::id().to_string())?;
        std::fs::set_permissions(&pid_path, std::fs::Permissions::from_mode(0o600))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let socket_path_clone = socket_path.clone();

        let listener_handle = thread::Builder::new()
            .name("ipc-listener".into())
            .spawn(move || {
                Self::listener_loop(
                    listener,
                    db,
                    access_mode,
                    shutdown_clone,
                    socket_path_clone,
                );
            })?;

        Ok(Self {
            socket_path,
            pid_path,
            shutdown,
            listener_handle: Some(listener_handle),
        })
    }

    /// The socket path this server is listening on.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Signal the server to stop and wait for it to finish.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
    }

    /// Stop a server by PID file. Sends SIGTERM to the process and cleans up.
    pub fn stop(data_dir: &Path) -> io::Result<()> {
        let pid_path = data_dir.join("strata.pid");
        let socket_path = data_dir.join("strata.sock");

        if pid_path.exists() {
            let pid_str = std::fs::read_to_string(&pid_path)?;
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                // Send SIGTERM
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                // Brief wait for process to exit
                for _ in 0..20 {
                    unsafe {
                        if libc::kill(pid, 0) != 0 {
                            break;
                        }
                    }
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            }
            let _ = std::fs::remove_file(&pid_path);
        }

        if socket_path.exists() {
            let _ = std::fs::remove_file(&socket_path);
        }

        Ok(())
    }

    /// Maximum number of concurrent handler threads.
    const MAX_CONNECTIONS: usize = 128;

    fn listener_loop(
        listener: UnixListener,
        db: Arc<Database>,
        access_mode: AccessMode,
        shutdown: Arc<AtomicBool>,
        _socket_path: PathBuf,
    ) {
        let mut handler_threads: Vec<JoinHandle<()>> = Vec::new();

        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    // Reap finished threads before checking capacity
                    handler_threads.retain(|h| !h.is_finished());

                    if handler_threads.len() >= Self::MAX_CONNECTIONS {
                        tracing::warn!(
                            "IPC connection limit reached ({}), rejecting connection",
                            Self::MAX_CONNECTIONS,
                        );
                        drop(stream);
                        continue;
                    }

                    // Set a read timeout so handler threads can check the
                    // shutdown flag periodically instead of blocking forever.
                    stream.set_nonblocking(false).ok();
                    stream.set_read_timeout(Some(std::time::Duration::from_secs(2))).ok();
                    let db = db.clone();
                    let shutdown = shutdown.clone();
                    match thread::Builder::new()
                        .name("ipc-handler".into())
                        .spawn(move || {
                            Self::handle_connection(stream, db, access_mode, shutdown);
                        })
                    {
                        Ok(handle) => {
                            handler_threads.push(handle);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to spawn IPC handler thread: {}", e);
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    if !shutdown.load(Ordering::SeqCst) {
                        tracing::warn!("IPC accept error: {}", e);
                    }
                    break;
                }
            }
        }

        // Wait for all handler threads to finish on shutdown
        for handle in handler_threads {
            let _ = handle.join();
        }
    }

    fn handle_connection(
        stream: std::os::unix::net::UnixStream,
        db: Arc<Database>,
        access_mode: AccessMode,
        shutdown: Arc<AtomicBool>,
    ) {
        let stream_clone = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut reader = io::BufReader::new(stream_clone);
        let mut writer = io::BufWriter::new(stream);
        let mut session = Session::new_with_mode(db, access_mode);

        loop {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            let frame = match wire::read_frame(&mut reader) {
                Ok(f) => f,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(ref e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    // Read timeout — check shutdown flag and loop
                    continue;
                }
                Err(_) => break,
            };

            let request: Request = match protocol::decode(&frame) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("IPC decode error: {}", e);
                    break;
                }
            };

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.execute(request.command)
            }));

            let result = match result {
                Ok(r) => r,
                Err(e) => {
                    let msg = if let Some(s) = e.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    tracing::error!("IPC handler panicked: {}", msg);
                    Err(crate::Error::Internal {
                        reason: format!("Server handler panicked: {}", msg),
                    })
                }
            };

            let response = Response {
                id: request.id,
                result,
            };

            let payload = match protocol::encode(&response) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("IPC encode error: {}", e);
                    break;
                }
            };

            if wire::write_frame(&mut writer, &payload).is_err() {
                break;
            }
        }
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_file(&self.pid_path);
    }
}
