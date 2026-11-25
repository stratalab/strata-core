//! A `ClientTransport` (built once per executor hub command) bridges its
//! async client on the calling thread: building one and making a call
//! starts no runtime worker pool.
//!
//! One test per binary: it counts the process's threads.

#![cfg(target_os = "linux")]

use std::net::TcpListener;

use strata_hub::{ClientError, ClientTransport};

fn live_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("procfs lists this process's threads")
        .count()
}

#[test]
fn transport_calls_run_without_spawning_threads() {
    // A loopback port with nothing listening: the call fails fast at
    // connect, after the full request path has run through the runtime.
    let closed_port = TcpListener::bind("127.0.0.1:0")
        .expect("listener binds")
        .local_addr()
        .expect("listener address")
        .port();
    let base = format!("http://127.0.0.1:{closed_port}");

    let before = live_threads();
    let transport =
        ClientTransport::new(url::Url::parse(&base).expect("url")).expect("transport builds");
    let error = transport
        .info()
        .expect_err("nothing listens on the closed port");
    let after = live_threads();

    assert!(
        matches!(error, ClientError::Network { .. }),
        "expected a connect failure, got {error:?}"
    );
    assert_eq!(after, before, "the transport started threads");
}
