//! `strata start` / `strata stop`: the headless broker-owner lifecycle pair.
//!
//! `strata start <db>` opens a durable database as a broker owner and blocks,
//! keeping the socket alive so other processes can attach, until `strata stop`
//! (or `ipc stop`) stops the hosting and it exits cleanly. These are real,
//! cross-process invocations of the `strata` binary — the multi-process claims
//! only mean something because every actor is a separate OS process.

#![deny(unsafe_code)]

use std::io::BufRead;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_strata")
}

fn strata(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .env_remove("STRATA_DB")
        .output()
        .expect("run strata binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn db_arg(dir: &Path) -> String {
    dir.join("db").to_string_lossy().into_owned()
}

/// Spawns `strata --db <db> --json start` and blocks until it prints its
/// readiness report, returning the running child and the parsed report. The
/// child is now hosting a broker socket and blocking until stopped.
fn spawn_start_host(db: &str) -> (Child, serde_json::Value) {
    let mut child = Command::new(bin())
        .args(["--db", db, "--json", "start"])
        .env_remove("STRATA_DB")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn strata start");
    let mut reader = std::io::BufReader::new(child.stdout.take().expect("start stdout"));
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("start prints a readiness report before it blocks");
    let report: serde_json::Value = serde_json::from_str(line.trim())
        .unwrap_or_else(|error| panic!("start readiness line is JSON ({error}): {line:?}"));
    (child, report)
}

#[test]
fn start_hosts_a_database_and_stop_stops_it_and_it_exits() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    // Seed the store so it exists durably before the host opens it.
    assert!(
        strata(&["--db", &db, "kv", "put", "seed", "1"])
            .status
            .success(),
        "seed"
    );

    // `strata start` becomes the broker owner and reports it is hosting.
    let (mut host, report) = spawn_start_host(&db);
    assert_eq!(
        report["type"], "ipc_started",
        "readiness envelope: {report}"
    );
    assert_eq!(
        report["data"]["hosting"], true,
        "start reports it is hosting: {report}"
    );
    assert_eq!(
        report["data"]["is_owner"], true,
        "start owns the store it hosts: {report}"
    );
    assert!(
        report["data"]["socket_path"].is_string(),
        "start reports its socket: {report}"
    );

    // A separate one-shot process now BROKERS to the host instead of being
    // refused on the writer lock — one store, two processes.
    let brokered = strata(&["--db", &db, "kv", "put", "contender", "1"]);
    assert!(
        brokered.status.success(),
        "a second process brokers to the start host: stderr={}",
        stderr(&brokered)
    );

    // `strata stop` brokers to the host and tells it to stop hosting.
    let stop = strata(&["--db", &db, "--json", "stop"]);
    assert!(stop.status.success(), "stop: stderr={}", stderr(&stop));
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&stop).trim()).expect("stop --json parses");
    assert_eq!(parsed["type"], "ipc_stop", "typed envelope: {parsed}");
    assert_eq!(
        parsed["data"]["stopped"], true,
        "stop halted the running host: {parsed}"
    );

    // Once its hosting is stopped, the start process exits cleanly on its own.
    let status = wait_with_timeout(&mut host).expect("start exits after being stopped");
    assert_eq!(status.code(), Some(0), "a stopped host exits cleanly");

    // With the owner gone, the store is free: a fresh process opens it directly
    // and the pre-stop durable data survived.
    let read = strata(&["--db", &db, "kv", "get", "contender"]);
    assert!(read.status.success(), "post-stop read: {}", stderr(&read));
    assert_eq!(stdout(&read).trim(), "1", "the brokered write survived");
}

#[test]
fn brokered_durability_flag_is_refused_not_silently_ignored() {
    // #3395: durability is fixed when the owner opens the database, so a brokered
    // client cannot change it. Asking for one and being silently given the
    // owner's mode is the failure this refuses — a caller who wanted a synced
    // commit must not be told it happened when it did not.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert!(strata(&["--db", &db, "kv", "put", "seed", "1"])
        .status
        .success());
    // The start host owns the store at its default (standard) durability.
    let (mut host, _report) = spawn_start_host(&db);

    let refused = strata(&["--db", &db, "--durability", "always", "kv", "put", "k", "v"]);
    assert!(
        !refused.status.success(),
        "a brokered --durability must be refused, not silently ignored: stdout={}",
        stdout(&refused)
    );
    assert!(
        stderr(&refused).contains("running owner") && stderr(&refused).contains("--durability"),
        "the refusal names the flag and the owner-governs reason: {}",
        stderr(&refused)
    );

    // The same client brokers fine without the flag.
    let ok = strata(&["--db", &db, "kv", "put", "k", "v"]);
    assert!(
        ok.status.success(),
        "a brokered write without --durability succeeds: {}",
        stderr(&ok)
    );

    let stop = strata(&["--db", &db, "--json", "stop"]);
    assert!(stop.status.success(), "stop: {}", stderr(&stop));
    wait_with_timeout(&mut host).expect("host exits after stop");
}

#[test]
fn stop_on_a_missing_database_reports_not_stopped_and_creates_nothing() {
    let dir = tempfile::tempdir().expect("tmp");
    let missing = dir.path().join("no-such-db");
    let stop = strata(&["--db", &missing.to_string_lossy(), "--json", "stop"]);
    assert!(
        stop.status.success(),
        "stop is forgiving: {}",
        stderr(&stop)
    );
    let parsed: serde_json::Value =
        serde_json::from_str(stdout(&stop).trim()).expect("stop --json parses");
    assert_eq!(parsed["type"], "ipc_stop");
    assert_eq!(
        parsed["data"]["stopped"], false,
        "there was no owner to stop: {parsed}"
    );
    assert!(
        !missing.exists(),
        "stop must not create a database as a side effect"
    );
}

#[test]
fn start_refuses_cache_mode() {
    // A cache database is single-process by construction and hosts nothing, so
    // there is no owner to keep alive.
    let refused = strata(&["--cache", "start"]);
    assert_eq!(
        refused.status.code(),
        Some(2),
        "start --cache is a usage error: {}",
        stderr(&refused)
    );
}

#[test]
fn start_refuses_an_incompatible_ipc_mode() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    // start exists to host; both non-hosting modes contradict that outright.
    for mode in ["client", "off"] {
        let refused = strata(&["--db", &db, "--ipc", mode, "start"]);
        assert_eq!(
            refused.status.code(),
            Some(2),
            "start --ipc {mode} is a usage error: {}",
            stderr(&refused)
        );
    }
}

#[test]
fn stop_refuses_cache_mode() {
    // A cache database has no broker owner to stop.
    let refused = strata(&["--cache", "stop"]);
    assert_eq!(
        refused.status.code(),
        Some(2),
        "stop --cache is a usage error: {}",
        stderr(&refused)
    );
}

#[test]
fn start_and_stop_require_a_database_target() {
    // Neither a positional path, `--db`, nor STRATA_DB: both refuse rather than
    // acting on an implicit current directory.
    for verb in ["start", "stop"] {
        let refused = strata(&[verb]);
        assert_eq!(
            refused.status.code(),
            Some(2),
            "`strata {verb}` with no target is a usage error: {}",
            stderr(&refused)
        );
    }
}

#[test]
fn start_refuses_when_another_process_already_owns_the_store() {
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    assert!(
        strata(&["--db", &db, "kv", "put", "seed", "1"])
            .status
            .success(),
        "seed"
    );

    // A first host owns the store; a second `strata start` cannot become a
    // second owner — Host mode brokers it in as a client, so it refuses.
    let (mut host, _report) = spawn_start_host(&db);
    let refused = strata(&["--db", &db, "start"]);
    assert_eq!(
        refused.status.code(),
        Some(2),
        "a second start refuses an already-owned store: {}",
        stderr(&refused)
    );
    assert!(
        stderr(&refused).contains("already owns"),
        "the refusal names the already-owned cause (not the bind-failure one): {}",
        stderr(&refused)
    );

    // Tear the host down for a clean exit.
    let _ = strata(&["--db", &db, "stop"]);
    let _ = wait_with_timeout(&mut host);
}

#[test]
fn a_read_only_client_is_gated_across_processes_and_reads_still_work() {
    // Cross-process read-only: a writable host owns the database; a separate
    // `--read-only` process brokering through it is rejected on writes by the
    // OWNER's dispatch gate and still serves reads. Also pins the local
    // (unbrokered) `--read-only` open the same way.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());

    // Local, unbrokered: the connection's own gate.
    let put = strata(&["--db", &db, "--read-only", "--json", "kv", "put", "k", "v"]);
    assert_ne!(put.status.code(), Some(0), "a read-only write fails");
    let combined = format!("{}{}", stdout(&put), stderr(&put));
    assert!(
        combined.contains("access_denied.executor.read_only_session"),
        "the registered code names the refusal: {combined}"
    );

    // Seed a value with a writable one-shot, then attach read-only through a
    // running host.
    let seeded = strata(&["--db", &db, "--json", "kv", "put", "k", "v"]);
    assert_eq!(seeded.status.code(), Some(0), "{}", stderr(&seeded));
    let (mut host, _report) = spawn_start_host(&db);

    let brokered_put = strata(&["--db", &db, "--read-only", "--json", "kv", "put", "k", "w"]);
    assert_ne!(
        brokered_put.status.code(),
        Some(0),
        "the owner's gate rejects a brokered read-only write"
    );
    let combined = format!("{}{}", stdout(&brokered_put), stderr(&brokered_put));
    assert!(
        combined.contains("access_denied.executor.read_only_session"),
        "rejected by the registered code, not a transport error: {combined}"
    );

    let get = strata(&["--db", &db, "--read-only", "--json", "kv", "get", "k"]);
    assert_eq!(
        get.status.code(),
        Some(0),
        "a brokered read-only read serves: {}",
        stderr(&get)
    );
    assert!(
        stdout(&get).contains("\"found\":true") || stdout(&get).contains("\"found\": true"),
        "the seeded value is visible read-only: {}",
        stdout(&get)
    );

    let _ = strata(&["--db", &db, "stop"]);
    let _ = wait_with_timeout(&mut host);
}

/// Minimal wire framing (4-byte big-endian length + payload), hand-rolled so
/// these tests speak to a durable host exactly as an out-of-repo client (the
/// VS Code extension) will — no crate internals.
mod raw_wire {
    use std::io::{Read, Write};

    pub(crate) fn send(stream: &mut std::os::unix::net::UnixStream, payload: &[u8]) {
        let len = u32::try_from(payload.len()).expect("frame fits u32");
        stream.write_all(&len.to_be_bytes()).expect("write length");
        stream.write_all(payload).expect("write payload");
        stream.flush().expect("flush");
    }

    pub(crate) fn recv(stream: &mut std::os::unix::net::UnixStream) -> serde_json::Value {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("read length");
        let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut payload).expect("read payload");
        serde_json::from_slice(&payload).expect("frame decodes")
    }
}

/// Attach a raw protocol-revision-2 socket session to a started host, using
/// the socket path its readiness report published.
fn attach_raw(report: &serde_json::Value, access: &str) -> std::os::unix::net::UnixStream {
    let socket = report["data"]["socket_path"]
        .as_str()
        .expect("the readiness report names the socket");
    let mut stream =
        std::os::unix::net::UnixStream::connect(socket).expect("connect to the host socket");
    let hello = format!("{{\"hello\":{{\"protocol\":2,\"access\":\"{access}\"}}}}");
    raw_wire::send(&mut stream, hello.as_bytes());
    let reply = raw_wire::recv(&mut stream);
    assert_eq!(reply["type"], "ipc_hello", "hello accepted: {reply}");
    stream
}

#[test]
fn a_durable_host_ticks_a_read_only_subscriber_across_processes() {
    // The extension's exact topology, durable end-to-end: a real `strata
    // start` owner on a durable database, a read-only raw-socket subscriber
    // (this test process), and a separate one-shot writer process brokering
    // through the owner. The subscriber must hear about the write.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    let seeded = strata(&["--db", &db, "--json", "kv", "put", "hi", "there"]);
    assert_eq!(seeded.status.code(), Some(0), "{}", stderr(&seeded));

    let (mut host, report) = spawn_start_host(&db);
    let mut subscriber = attach_raw(&report, "read");
    raw_wire::send(
        &mut subscriber,
        b"{\"id\":1,\"subscribe\":{\"events\":[\"version\"]}}",
    );
    let ack = raw_wire::recv(&mut subscriber);
    assert_eq!(ack["payload"]["type"], "ipc_subscribed", "acked: {ack}");

    // A separate process writes, brokered through the durable owner.
    let put = strata(&["--db", &db, "--json", "kv", "put", "hi", "again"]);
    assert_eq!(put.status.code(), Some(0), "{}", stderr(&put));

    let push = raw_wire::recv(&mut subscriber);
    assert_eq!(push["notify"]["event"], "version", "a tick arrived: {push}");
    assert!(
        push["notify"]["version"].as_u64().unwrap_or(0) >= 1,
        "the cross-process write advanced the durable store's watermark"
    );

    let _ = strata(&["--db", &db, "stop"]);
    let _ = wait_with_timeout(&mut host);
}

#[test]
fn a_durable_host_sheds_an_expired_deadline_over_the_wire() {
    // Durable end-to-end deadline shed: deadline_ms 0 is expired by
    // construction, so the durable owner must answer with the registered
    // shed code — correlated, and without executing the write.
    let dir = tempfile::tempdir().expect("tmp");
    let db = db_arg(dir.path());
    // The CLI takes plain-text keys/values; on the wire they are base64
    // ("hi" = aGk=, "there" = dGhlcmU=).
    let seeded = strata(&["--db", &db, "--json", "kv", "put", "hi", "there"]);
    assert_eq!(seeded.status.code(), Some(0), "{}", stderr(&seeded));

    let (mut host, report) = spawn_start_host(&db);
    let mut session = attach_raw(&report, "read_write");

    raw_wire::send(
        &mut session,
        b"{\"id\":1,\"deadline_ms\":0,\
          \"command\":{\"type\":\"kv_put\",\"key\":\"aGk=\",\"value\":\"bmV3\"}}",
    );
    let shed = raw_wire::recv(&mut session);
    assert_eq!(shed["id"], 1, "the shed is correlated");
    assert_eq!(
        shed["payload"]["error"]["code"], "unavailable.executor.ipc_deadline",
        "the durable owner sheds expired work: {shed}"
    );

    // The shed happened before dispatch: the durable store still holds the
    // seeded value.
    raw_wire::send(
        &mut session,
        b"{\"id\":2,\"command\":{\"type\":\"kv_get\",\"key\":\"aGk=\"}}",
    );
    let get = raw_wire::recv(&mut session);
    assert_eq!(
        get["payload"]["data"]["value"]["value"], "dGhlcmU=",
        "the seeded value survived the shed write: {get}"
    );

    let _ = strata(&["--db", &db, "stop"]);
    let _ = wait_with_timeout(&mut host);
}

/// Waits up to a few seconds for `child` to exit, returning its status. Kills it
/// on timeout so a hung host never wedges the suite.
fn wait_with_timeout(child: &mut Child) -> Option<std::process::ExitStatus> {
    for _ in 0..300 {
        match child.try_wait().expect("try_wait") {
            Some(status) => return Some(status),
            None => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}
