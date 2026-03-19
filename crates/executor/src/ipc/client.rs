//! IPC client that connects to a Strata server via Unix domain socket.

use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{Command, Output, Result};

use super::protocol::{self, Request, Response};
use super::wire;

/// Client that communicates with an IPC server over a Unix domain socket.
pub struct IpcClient {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    socket_path: PathBuf,
    next_id: u64,
}

impl IpcClient {
    /// Connect to a server at the given socket path with a 1-second timeout.
    pub fn connect(socket_path: &Path) -> io::Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;

        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);

        Ok(Self {
            reader,
            writer,
            socket_path: socket_path.to_path_buf(),
            next_id: 1,
        })
    }

    /// Execute a command over IPC.
    pub fn execute(&mut self, cmd: Command) -> Result<Output> {
        let request = Request {
            id: self.next_id,
            command: cmd,
        };
        self.next_id += 1;

        let payload = protocol::encode(&request).map_err(|e| crate::Error::Internal {
            reason: format!("IPC encode error: {}", e),
        })?;

        wire::write_frame(&mut self.writer, &payload).map_err(|e| crate::Error::Io {
            reason: format!("IPC write error: {}", e),
        })?;

        let response_frame = wire::read_frame(&mut self.reader).map_err(|e| crate::Error::Io {
            reason: format!("IPC read error: {}", e),
        })?;

        let response: Response =
            protocol::decode(&response_frame).map_err(|e| crate::Error::Internal {
                reason: format!("IPC decode error: {}", e),
            })?;

        if response.id != request.id {
            return Err(crate::Error::Internal {
                reason: format!(
                    "IPC response ID mismatch: expected {}, got {}",
                    request.id, response.id
                ),
            });
        }

        response.result
    }

    /// Open a new connection to the same socket path.
    pub fn connect_new(&self) -> io::Result<Self> {
        Self::connect(&self.socket_path)
    }

    /// Return the socket path this client is connected to.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}
