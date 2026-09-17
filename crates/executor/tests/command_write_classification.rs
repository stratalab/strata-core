//! Pins the runtime write classification to the authored IDL, and the
//! read-only session gate to both.
//!
//! `Command::is_write()` is the classification the IPC read-only gate
//! dispatches on; the IDL `access` facet is the authored truth every SDK and
//! doc surface reads. These tests hold them together for every command in the
//! generated catalog — with zero exceptions — and then prove the gate over a
//! real socket session: every write-class command is rejected on a read
//! session, and no read-class command ever trips the gate.

use std::path::PathBuf;
// Named only by `Session::open`, which needs `ipc` + unix.
#[cfg(all(feature = "ipc", unix))]
use std::path::Path;

use strata_executor::Command;

/// A catalog entry paired with its decoded request fixture.
struct CatalogCommand {
    id: String,
    /// Read only by the socket-session test, which needs `ipc` + unix.
    #[cfg_attr(not(all(feature = "ipc", unix)), allow(dead_code))]
    family: String,
    access: String,
    fixture_json: String,
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Load every command from the generated index with its request fixture,
/// skipping families whose `Command` variants are compiled out of this build.
/// The skip is loud: the returned counts let each test assert full coverage
/// when the features are on.
fn load_catalog() -> (Vec<CatalogCommand>, usize) {
    let index: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(manifest_dir().join("idl/v1/generated/command-index.json"))
            .expect("read generated command index"),
    )
    .expect("parse generated command index");
    let fixtures_root = manifest_dir().join("tests/fixtures");

    let mut commands = Vec::new();
    let mut skipped = 0usize;
    for entry in index["commands"].as_array().expect("commands array") {
        let family = entry["family"].as_str().expect("family").to_owned();
        if family == "inference" && !cfg!(feature = "inference") {
            skipped += 1;
            continue;
        }
        let fixture_rel = entry["fixtures"]["request"]
            .as_str()
            .expect("every cataloged command has a request fixture");
        commands.push(CatalogCommand {
            id: entry["id"].as_str().expect("id").to_owned(),
            family,
            access: entry["access"].as_str().expect("access").to_owned(),
            fixture_json: std::fs::read_to_string(fixtures_root.join(fixture_rel))
                .unwrap_or_else(|error| panic!("read fixture {fixture_rel}: {error}")),
        });
    }
    (commands, skipped)
}

#[test]
fn is_write_matches_the_idl_access_facet_for_every_command() {
    let (commands, skipped) = load_catalog();
    assert!(
        commands.len() + skipped >= 127,
        "the catalog kept its breadth (saw {} + {skipped} skipped)",
        commands.len()
    );
    if cfg!(feature = "inference") {
        assert_eq!(skipped, 0, "nothing is skipped when all families compile");
    }

    for command in &commands {
        let decoded: Command =
            serde_json::from_str(&command.fixture_json).unwrap_or_else(|error| {
                panic!("fixture for {} decodes as a Command: {error}", command.id)
            });
        assert_eq!(
            decoded.is_write(),
            command.access == "write",
            "{}: Command::is_write() must match the IDL access facet (`{}`) — \
             change them in lockstep",
            command.id,
            command.access,
        );
    }
}

/// Minimal wire framing (4-byte big-endian length + payload), hand-rolled so
/// this test speaks to the owner exactly as an external client would.
#[cfg(all(feature = "ipc", unix))]
mod raw_wire {
    use std::io::{Read, Write};

    pub(crate) fn write_frame(stream: &mut impl Write, payload: &[u8]) {
        let len = u32::try_from(payload.len()).expect("frame fits u32");
        stream.write_all(&len.to_be_bytes()).expect("write length");
        stream.write_all(payload).expect("write payload");
        stream.flush().expect("flush");
    }

    pub(crate) fn read_frame(stream: &mut impl Read) -> Vec<u8> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("read length");
        let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut payload).expect("read payload");
        payload
    }
}

/// One protocol-revision-2 session over a real socket, declared with the
/// given access.
#[cfg(all(feature = "ipc", unix))]
struct Session {
    stream: std::os::unix::net::UnixStream,
    next_id: u64,
}

#[cfg(all(feature = "ipc", unix))]
impl Session {
    fn open(socket: &Path, access: &str) -> Self {
        let mut stream = std::os::unix::net::UnixStream::connect(socket).expect("connect");
        let hello = format!("{{\"hello\":{{\"protocol\":2,\"access\":\"{access}\"}}}}");
        raw_wire::write_frame(&mut stream, hello.as_bytes());
        let reply: serde_json::Value =
            serde_json::from_slice(&raw_wire::read_frame(&mut stream)).expect("hello reply");
        assert_eq!(reply["type"], "ipc_hello", "hello accepted: {reply}");
        assert_eq!(reply["data"]["granted_access"], access);
        Self { stream, next_id: 0 }
    }

    /// Send one command (raw wire JSON) and return the response payload.
    fn send(&mut self, command_json: &str) -> serde_json::Value {
        self.next_id += 1;
        let request = serde_json::json!({
            "id": self.next_id,
            "command": serde_json::from_str::<serde_json::Value>(command_json)
                .expect("fixture is JSON"),
        });
        raw_wire::write_frame(
            &mut self.stream,
            serde_json::to_string(&request)
                .expect("serialize")
                .as_bytes(),
        );
        let frame: serde_json::Value =
            serde_json::from_slice(&raw_wire::read_frame(&mut self.stream)).expect("response");
        assert_eq!(frame["id"], self.next_id, "correlated");
        frame["payload"].clone()
    }
}

// Drives the IPC server directly; the socket transport is `ipc` + unix.
#[cfg(all(feature = "ipc", unix))]
#[test]
fn a_read_session_rejects_every_cataloged_write_and_gates_no_read() {
    let (commands, _) = load_catalog();
    let dir = tempfile::tempdir().expect("tmp");
    // Read-class commands run for real on the read session, and one of them
    // (`arrow.export`) writes its export file to the working directory — pin
    // the CWD to the tempdir so the sweep leaves nothing behind. (The other
    // test in this binary uses only absolute paths, so this is safe even
    // under parallel execution.)
    std::env::set_current_dir(dir.path()).expect("enter tempdir");
    let executor = std::sync::Arc::new(std::sync::Mutex::new(
        strata_executor::Executor::open_cache().expect("cache executor"),
    ));
    let mut server =
        strata_executor::ipc::IpcServer::start(dir.path(), executor).expect("start server");
    let mut session = Session::open(server.socket_path(), "read");

    let mut writes_rejected = 0usize;
    for command in &commands {
        let payload = session.send(&command.fixture_json);
        if command.access == "write" {
            assert_eq!(
                payload["error"]["code"], "access_denied.executor.read_only_session",
                "{}: a write on a read session is gated: {payload}",
                command.id,
            );
            writes_rejected += 1;
        } else {
            // A read may legitimately fail (missing state, feature-gated
            // backend) — but never with the read-only gate's code.
            assert_ne!(
                payload["error"]["code"].as_str(),
                Some("access_denied.executor.read_only_session"),
                "{}: a read-class command must never trip the read-only gate",
                command.id,
            );
        }
    }
    assert!(
        writes_rejected >= 40,
        "the sweep exercised the write catalog ({writes_rejected} rejections)"
    );

    // The same fixtures on a read-write session are not gated — the gate
    // keys on the session, not the command.
    let mut writable = Session::open(server.socket_path(), "read_write");
    for command in commands.iter().filter(|c| c.family == "kv") {
        let payload = writable.send(&command.fixture_json);
        assert_ne!(
            payload["error"]["code"].as_str(),
            Some("access_denied.executor.read_only_session"),
            "{}: a read-write session is never gated",
            command.id,
        );
    }

    server.shutdown();
}
