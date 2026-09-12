//! IPC client: speaks the framed wire to a store owner's socket and
//! reconstructs the same `ExecutorResult<Output>` an in-process executor
//! returns, so a remote connection is transport-transparent to callers.

use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use serde_json::value::RawValue;

use crate::{Command, ErrorStatus, ExecutorError, Output};

use super::protocol::{
    self, ClientIdentity, HelloFrame, HelloRequest, ServerHello, SessionAccess, WireRequestOwned,
    WireResponseFrameRef,
};
use super::wire;

/// Client read timeout: a command may run arbitrarily long on the owner, so
/// this is generous; it exists so a dead owner does not hang the client
/// forever.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Client write timeout: framing a request should never block for long.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a connect attempt failed — the broker's open dance treats these
/// differently: a capacity refusal is a definitive, typed answer from a live
/// owner worth surfacing to the caller, while a transport failure is just one
/// bad probe inside the bounded retry window.
#[derive(Debug)]
pub(crate) enum ConnectError {
    /// The owner answered with the registered capacity-rejection frame.
    AtCapacity(ExecutorError),
    /// Any other transport or handshake failure.
    Io(io::Error),
}

/// The capacity-rejection code a refused connection carries (see
/// `reject_at_capacity` on the server side).
const AT_CAPACITY_CODE: &str = "resource_exhausted.executor.ipc_connections";

/// A connected IPC client. One outstanding request at a time (synchronous
/// request/response), so framing plus stream ordering is the only correlation
/// needed.
pub(crate) struct IpcClient {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    /// The owner's hello, when it speaks protocol revision 2. `None` against a
    /// pre-hello owner (the connection then runs the implicit protocol 1).
    server_hello: Option<ServerHello>,
    /// Correlation id of the most recent protocol-revision-2 request. The next
    /// request uses the increment, and the response frame must echo it back.
    last_id: u64,
}

impl IpcClient {
    /// Connect to a store owner listening at `socket_path` and introduce
    /// ourselves with a protocol-revision-2 hello declaring `access`.
    pub(crate) fn connect(socket_path: &Path, access: SessionAccess) -> Result<Self, ConnectError> {
        let stream = UnixStream::connect(socket_path).map_err(ConnectError::Io)?;
        stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .map_err(ConnectError::Io)?;
        stream
            .set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(ConnectError::Io)?;
        let mut client = Self {
            reader: BufReader::new(stream.try_clone().map_err(ConnectError::Io)?),
            writer: BufWriter::new(stream),
            server_hello: None,
            last_id: 0,
        };
        client.server_hello = client.hello(access)?;
        Ok(client)
    }

    /// The hello exchange. `Ok(Some)` is a protocol-revision-2 owner;
    /// `Ok(None)` is a pre-hello owner that answered the hello frame with a
    /// malformed-envelope error and kept the connection — we downgrade to the
    /// implicit protocol 1 rather than stranding the user on a skewed pair.
    /// A capacity rejection is a typed refusal; any other refusal or transport
    /// failure fails the connect.
    fn hello(&mut self, access: SessionAccess) -> Result<Option<ServerHello>, ConnectError> {
        let request = HelloRequest {
            protocol: protocol::PROTOCOL_VERSION,
            idl: Some(protocol::build_idl_stamps()),
            client: Some(ClientIdentity {
                name: process_name(),
                version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                pid: Some(std::process::id()),
            }),
            access,
            capabilities: Vec::new(),
        };
        let payload = serde_json::to_vec(&HelloFrame { hello: request })
            .map_err(|error| ConnectError::Io(io::Error::other(error)))?;
        // An owner at capacity writes its rejection frame and closes without
        // ever reading our hello — so a failed hello WRITE (broken pipe) may
        // coexist with a readable rejection already in the socket buffer.
        // Read before judging the write.
        let write_result = wire::write_frame(&mut self.writer, &payload);
        let response = match wire::read_frame(&mut self.reader) {
            Ok(response) => response,
            Err(read_error) => {
                return Err(ConnectError::Io(write_result.err().unwrap_or(read_error)));
            }
        };
        let value: serde_json::Value = serde_json::from_slice(&response)
            .map_err(|error| ConnectError::Io(io::Error::other(error)))?;

        if value.get("type").and_then(serde_json::Value::as_str) == Some("ipc_hello") {
            let hello: ServerHello = serde_json::from_value(value["data"].clone())
                .map_err(|error| ConnectError::Io(io::Error::other(error)))?;
            return Ok(Some(hello));
        }
        if let Some(error) = value.get("error") {
            let code = error.get("code").and_then(serde_json::Value::as_str);
            if code == Some("invalid_argument.executor.wire_request") {
                return Ok(None);
            }
            if code == Some(AT_CAPACITY_CODE) {
                let status: ErrorStatus = serde_json::from_value(error.clone())
                    .map_err(|error| ConnectError::Io(io::Error::other(error)))?;
                return Err(ConnectError::AtCapacity(ExecutorError::from_status(status)));
            }
            return Err(ConnectError::Io(io::Error::other(format!(
                "the store owner refused the IPC hello: {error}"
            ))));
        }
        Err(ConnectError::Io(io::Error::other(
            "unexpected response to the IPC hello (neither ipc_hello nor an error envelope)",
        )))
    }

    /// The owner's hello, when this connection negotiated protocol revision 2.
    pub(crate) fn server_hello(&self) -> Option<&ServerHello> {
        self.server_hello.as_ref()
    }

    /// Send `command` (with the caller's session scope) and reconstruct the
    /// typed result. A transport failure surfaces as a registered
    /// `unavailable.executor.ipc_transport` error (the command's fate is
    /// in-doubt).
    ///
    /// On a protocol-revision-2 connection the request carries a correlation
    /// id and the response arrives in an `{"id","payload"}` frame; the echoed
    /// id must match, or the stream ordering this client depends on has been
    /// violated and the response cannot be trusted. On the implicit protocol 1
    /// both directions are the bare shapes.
    pub(crate) fn execute(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        command: &Command,
    ) -> Result<Output, ExecutorError> {
        let command_json = serde_json::to_string(command).map_err(|error| {
            ExecutorError::new(
                "internal.executor.wire_response",
                format!("could not serialize command for IPC: {error}"),
            )
        })?;
        let raw = RawValue::from_string(command_json).map_err(transport_error)?;
        let correlated = self.server_hello.is_some();
        let id = correlated.then(|| {
            self.last_id = self.last_id.wrapping_add(1);
            self.last_id
        });
        let request = WireRequestOwned {
            id,
            deadline_ms: None,
            branch,
            space,
            command: &raw,
        };
        let payload = serde_json::to_vec(&request).map_err(transport_error)?;

        wire::write_frame(&mut self.writer, &payload).map_err(transport_error)?;
        let response = wire::read_frame(&mut self.reader).map_err(transport_error)?;

        let Some(sent_id) = id else {
            return decode_wire_response(&response);
        };
        let frame: WireResponseFrameRef =
            serde_json::from_slice(&response).map_err(transport_error)?;
        if frame.id != Some(sent_id) {
            return Err(transport_error(format!(
                "correlation id mismatch: sent {sent_id}, response echoed {:?}",
                frame.id
            )));
        }
        decode_wire_response(frame.payload.get().as_bytes())
    }
}

/// Turn a response envelope (`{"type","data"}` or `{"error":…}`) back into a
/// typed `ExecutorResult<Output>` — the inverse of the server's encode step, so
/// the remote result is indistinguishable from a local one.
fn decode_wire_response(bytes: &[u8]) -> Result<Output, ExecutorError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(transport_error)?;
    if let Some(error) = value.get("error") {
        let status: ErrorStatus = serde_json::from_value(error.clone()).map_err(transport_error)?;
        return Err(ExecutorError::from_status(status));
    }
    serde_json::from_value::<Output>(value).map_err(transport_error)
}

fn transport_error(error: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::new(
        "unavailable.executor.ipc_transport",
        format!("IPC transport failure: {error}"),
    )
}

/// The connecting process's display name for the hello identity — the
/// executable stem (`strata` for the CLI). Display metadata only; the socket's
/// same-user permission check is the security boundary.
fn process_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::IpcClient;
    use crate::ipc::wire;
    use std::io::{BufReader, BufWriter};
    use std::os::unix::net::UnixListener;

    /// A hand-rolled pre-hello owner: answers every frame the way the
    /// protocol-1 server did — a `WireRequest` with `ping` gets a pong
    /// envelope; anything else (including a hello frame it has never heard
    /// of) gets the malformed-envelope error — and keeps the connection open.
    /// A frame smuggling a correlation `id` is answered with an error too:
    /// after downgrading, the client must speak pure protocol 1.
    fn spawn_legacy_owner(dir: &std::path::Path) -> std::path::PathBuf {
        let sock = dir.join("strata.sock");
        let listener = UnixListener::bind(&sock).expect("bind fake owner");
        std::thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut writer = BufWriter::new(stream);
            while let Ok(frame) = wire::read_frame(&mut reader) {
                let value: Result<serde_json::Value, _> = serde_json::from_slice(&frame);
                let is_pure_protocol_1_ping = value
                    .as_ref()
                    .ok()
                    .filter(|v| v.get("id").is_none())
                    .and_then(|v| v.get("command"))
                    .and_then(|c| c.get("type"))
                    .and_then(|t| t.as_str())
                    == Some("ping");
                let response = if is_pure_protocol_1_ping {
                    "{\"type\":\"pong\",\"data\":{\"version\":\"0.0.0-legacy\"}}".to_owned()
                } else {
                    "{\"error\":{\"class\":\"invalid_argument\",\
                     \"code\":\"invalid_argument.executor.wire_request\",\
                     \"message\":\"malformed IPC request envelope\"}}"
                        .to_owned()
                };
                if wire::write_frame(&mut writer, response.as_bytes()).is_err() {
                    break;
                }
            }
        });
        sock
    }

    /// A hand-rolled protocol-revision-2 owner that answers the hello
    /// correctly, then echoes the WRONG correlation id on every response —
    /// the transport-corruption case the client must refuse to trust.
    fn spawn_miscorrelating_owner(dir: &std::path::Path) -> std::path::PathBuf {
        let sock = dir.join("strata.sock");
        let listener = UnixListener::bind(&sock).expect("bind fake owner");
        std::thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut writer = BufWriter::new(stream);
            let mut helloed = false;
            while let Ok(frame) = wire::read_frame(&mut reader) {
                let response = if helloed {
                    "{\"id\":999999,\"payload\":{\"type\":\"pong\",\
                     \"data\":{\"version\":\"0.0.0-test\"}}}"
                        .to_owned()
                } else {
                    helloed = true;
                    let _ = frame; // the hello's contents are irrelevant here
                    "{\"type\":\"ipc_hello\",\"data\":{\"protocol\":2,\
                     \"release\":\"0.0.0-test\",\
                     \"idl\":{\"schema_version\":\"strata.idl.v1\",\
                     \"generator_version\":\"strata-executor-idl.1\"},\
                     \"granted_access\":\"read_write\",\"capabilities\":[],\
                     \"owner_pid\":1}}"
                        .to_owned()
                };
                if wire::write_frame(&mut writer, response.as_bytes()).is_err() {
                    break;
                }
            }
        });
        sock
    }

    /// A hand-rolled owner at capacity: writes the rejection frame the moment
    /// a connection arrives (never reading the client's hello) and closes.
    fn spawn_at_capacity_owner(dir: &std::path::Path) -> std::path::PathBuf {
        let sock = dir.join("strata.sock");
        let listener = UnixListener::bind(&sock).expect("bind fake owner");
        std::thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut writer = BufWriter::new(stream);
            // Built with the real constructor so this fake cannot drift from
            // the envelope the listener's reject_at_capacity actually writes.
            let error = crate::ExecutorError::new(
                "resource_exhausted.executor.ipc_connections",
                "the store owner is at its IPC connection capacity",
            );
            let rejection =
                serde_json::to_string(&serde_json::json!({ "error": error })).expect("serialize");
            let _ = wire::write_frame(&mut writer, rejection.as_bytes());
            // The stream drops here: rejection then close, like the listener.
        });
        sock
    }

    #[test]
    fn a_capacity_refusal_surfaces_as_a_typed_connect_error() {
        // The refusal races our hello write (the owner never reads it), so
        // this also exercises the read-before-judging-the-write path.
        let dir = tempfile::tempdir().expect("tmp");
        let sock = spawn_at_capacity_owner(dir.path());

        let error = match IpcClient::connect(&sock, crate::ipc::SessionAccess::ReadWrite) {
            Err(super::ConnectError::AtCapacity(error)) => error,
            Err(other) => panic!("expected the typed capacity refusal, got {other:?}"),
            Ok(_) => panic!("a refused connection must not connect"),
        };
        assert_eq!(error.code(), "resource_exhausted.executor.ipc_connections");
    }

    #[test]
    fn a_miscorrelated_response_is_a_transport_error_not_a_result() {
        // If the echoed id does not match the sent id, the stream ordering the
        // client depends on has been violated — returning the payload anyway
        // could hand a caller some other request's answer. The registered
        // in-doubt transport error is the only honest result.
        let dir = tempfile::tempdir().expect("tmp");
        let sock = spawn_miscorrelating_owner(dir.path());

        let mut client = IpcClient::connect(&sock, crate::ipc::SessionAccess::ReadWrite)
            .expect("hello succeeds");
        assert!(client.server_hello().is_some(), "protocol 2 negotiated");

        let command: crate::Command =
            serde_json::from_value(serde_json::json!({ "type": "ping" })).expect("ping");
        let error = client
            .execute(None, None, &command)
            .expect_err("a miscorrelated response must not be trusted");
        assert_eq!(error.code(), "unavailable.executor.ipc_transport");
    }

    #[test]
    fn a_pre_hello_owner_downgrades_the_client_to_protocol_1() {
        // Skew tolerance: a stale long-lived owner (old `strata start`) that
        // predates the hello answers it with a malformed-envelope error and
        // keeps serving. The new client must downgrade and keep working — a
        // hard failure would strand every database owned by an old process.
        let dir = tempfile::tempdir().expect("tmp");
        let sock = spawn_legacy_owner(dir.path());

        let mut client = IpcClient::connect(&sock, crate::ipc::SessionAccess::ReadWrite)
            .expect("connect despite the old owner");
        assert!(
            client.server_hello().is_none(),
            "no hello info on a downgraded (protocol 1) connection"
        );

        let command: crate::Command =
            serde_json::from_value(serde_json::json!({ "type": "ping" })).expect("ping");
        let output = client
            .execute(None, None, &command)
            .expect("commands still run on protocol 1");
        assert_eq!(
            serde_json::to_value(&output).expect("json")["type"],
            "pong",
            "the downgraded connection serves commands"
        );
    }
}
