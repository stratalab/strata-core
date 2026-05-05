use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use strata_engine::{AccessMode, Database};

use crate::{Error, Session};

use super::protocol::{self, Request, Response};
use super::wire;

/// IPC server that listens on a Unix domain socket and serves executor commands.
pub struct IpcServer {
    socket_path: PathBuf,
    pid_path: PathBuf,
    shutdown: Arc<AtomicBool>,
    listener_handle: Option<JoinHandle<()>>,
}

impl IpcServer {
    /// Start a server bound to `<data_dir>/strata.sock`.
    pub fn start(data_dir: &Path, db: Arc<Database>, access_mode: AccessMode) -> io::Result<Self> {
        let socket_path = data_dir.join("strata.sock");
        let pid_path = data_dir.join("strata.pid");

        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let listener = UnixListener::bind(&socket_path)?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;

        std::fs::write(&pid_path, std::process::id().to_string())?;
        std::fs::set_permissions(&pid_path, std::fs::Permissions::from_mode(0o600))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let listener_shutdown = shutdown.clone();
        let listener_handle = thread::Builder::new()
            .name("ipc-listener".into())
            .spawn(move || Self::listener_loop(listener, db, access_mode, listener_shutdown))?;

        Ok(Self {
            socket_path,
            pid_path,
            shutdown,
            listener_handle: Some(listener_handle),
        })
    }

    /// Return the socket path for the running server.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Signal the server to stop and wait for the listener thread to exit.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
        self.cleanup_files();
    }

    /// Stop a running server using the PID and socket files in `data_dir`.
    pub fn stop(data_dir: &Path) -> io::Result<()> {
        let pid_path = data_dir.join("strata.pid");
        let socket_path = data_dir.join("strata.sock");

        if pid_path.exists() {
            let pid = std::fs::read_to_string(&pid_path)?
                .trim()
                .parse::<i32>()
                .ok();
            if let Some(pid) = pid {
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                for _ in 0..20 {
                    let alive = unsafe { libc::kill(pid, 0) == 0 };
                    if !alive {
                        break;
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

    const MAX_CONNECTIONS: usize = 128;

    fn listener_loop(
        listener: UnixListener,
        db: Arc<Database>,
        access_mode: AccessMode,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut handlers = Vec::new();

        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    handlers.retain(|handle: &JoinHandle<()>| !handle.is_finished());
                    if handlers.len() >= Self::MAX_CONNECTIONS {
                        tracing::warn!(
                            "IPC connection limit reached ({}), rejecting connection",
                            Self::MAX_CONNECTIONS
                        );
                        drop(stream);
                        continue;
                    }

                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));

                    let handler_db = db.clone();
                    let handler_shutdown = shutdown.clone();
                    match thread::Builder::new()
                        .name("ipc-handler".into())
                        .spawn(move || {
                            Self::handle_connection(
                                stream,
                                handler_db,
                                access_mode,
                                handler_shutdown,
                            );
                        }) {
                        Ok(handle) => handlers.push(handle),
                        Err(error) => tracing::warn!("Failed to spawn IPC handler thread: {error}"),
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(50));
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

    fn handle_connection(
        stream: std::os::unix::net::UnixStream,
        db: Arc<Database>,
        access_mode: AccessMode,
        shutdown: Arc<AtomicBool>,
    ) {
        let stream_clone = match stream.try_clone() {
            Ok(stream_clone) => stream_clone,
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
                Ok(frame) => frame,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(_) => break,
            };

            let request: Request = match protocol::decode(&frame) {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!("IPC decode error: {error}");
                    break;
                }
            };

            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.execute(request.command)
            })) {
                Ok(result) => result,
                Err(payload) => {
                    let message = if let Some(text) = payload.downcast_ref::<&str>() {
                        text.to_string()
                    } else if let Some(text) = payload.downcast_ref::<String>() {
                        text.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    tracing::error!("IPC handler panicked: {message}");
                    Err(Error::Internal {
                        reason: format!("Server handler panicked: {message}"),
                        hint: Some(
                            "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                                .to_string(),
                        ),
                    })
                }
            };

            let response = Response {
                id: request.id,
                result,
            };
            let payload = match protocol::encode(&response) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!("IPC encode error: {error}");
                    break;
                }
            };

            if wire::write_frame(&mut writer, &payload).is_err() {
                break;
            }
        }
    }

    fn cleanup_files(&self) {
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_file(&self.pid_path);
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
