use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{Command, Error, Output, Result};

use super::protocol::{self, Request, Response};
use super::wire;

/// Client connection for executor IPC.
pub(crate) struct IpcClient {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    socket_path: PathBuf,
    next_id: u64,
}

impl IpcClient {
    pub(crate) fn connect(socket_path: &Path) -> io::Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;

        Ok(Self {
            reader: BufReader::new(stream.try_clone()?),
            writer: BufWriter::new(stream),
            socket_path: socket_path.to_path_buf(),
            next_id: 1,
        })
    }

    pub(crate) fn execute(&mut self, command: Command) -> Result<Output> {
        let request = Request {
            id: self.next_id,
            command,
        };
        self.next_id += 1;

        let payload = protocol::encode(&request).map_err(|error| Error::Internal {
            reason: format!("IPC encode error: {error}"),
            hint: Some(
                "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                    .to_string(),
            ),
        })?;

        wire::write_frame(&mut self.writer, &payload).map_err(|error| Error::Io {
            reason: format!("IPC write error: {error}"),
            hint: None,
        })?;

        let frame = wire::read_frame(&mut self.reader).map_err(|error| Error::Io {
            reason: format!("IPC read error: {error}"),
            hint: None,
        })?;

        let response: Response = protocol::decode(&frame).map_err(|error| Error::Internal {
            reason: format!("IPC decode error: {error}"),
            hint: Some(
                "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                    .to_string(),
            ),
        })?;

        if response.id != request.id {
            return Err(Error::Internal {
                reason: format!(
                    "IPC response ID mismatch: expected {}, got {}",
                    request.id, response.id
                ),
                hint: Some(
                    "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                        .to_string(),
                ),
            });
        }

        response.result
    }

    pub(crate) fn connect_new(&self) -> io::Result<Self> {
        Self::connect(&self.socket_path)
    }
}
