//! The `STRATA_HUB_TIMEOUT_SECONDS` override reaches the TCP connect budget,
//! not only the per-request budget: under a sanitizer the whole process can
//! stall past the 10s connect default while a loopback round trip is healthy.
//!
//! One test per binary: the override is process-global environment.

#![cfg(target_os = "linux")]

use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use strata_hub::{ClientError, ClientTransport};

/// A loopback listener whose accept queue is full. Linux drops SYNs to a
/// listener with a full accept queue, so a client connect to it stalls until
/// the client's connect budget expires. The queue is filled by connecting
/// until a probe connect times out, which also proves the stall is real.
fn listener_that_stalls_connects() -> (TcpListener, Vec<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    let addr = listener.local_addr().expect("listener address");
    let mut queued = Vec::new();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(250)) {
            Ok(stream) => queued.push(stream),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => break,
            Err(error) => panic!("filling the accept queue failed: {error}"),
        }
        assert!(queued.len() < 8192, "the accept queue never filled");
    }
    (listener, queued)
}

#[test]
fn connect_budget_follows_the_hub_timeout_override() {
    // Above the client's 10s connect default, so only an override that
    // reaches the connect budget keeps the stalled connect alive this long.
    const OVERRIDE_SECS: u64 = 12;
    std::env::set_var("STRATA_HUB_TIMEOUT_SECONDS", OVERRIDE_SECS.to_string());

    let (listener, _queued) = listener_that_stalls_connects();
    let base = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let transport =
        ClientTransport::new(url::Url::parse(&base).expect("url")).expect("transport builds");

    let started = Instant::now();
    let error = transport
        .info()
        .expect_err("a connect that never completes cannot succeed");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, ClientError::Timeout { .. }),
        "expected a timeout, got {error:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(OVERRIDE_SECS - 1),
        "the connect gave up after {elapsed:?}, inside the {OVERRIDE_SECS}s override"
    );
}
