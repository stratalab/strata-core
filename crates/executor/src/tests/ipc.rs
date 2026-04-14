//! IPC integration tests.

use crate::ipc::client::IpcClient;
use crate::ipc::protocol::{self, Request, Response};
use crate::ipc::server::IpcServer;
use crate::ipc::wire;
use crate::{Command, Output, Session, Value};
use strata_engine::database::search_only_primary_spec;
use strata_engine::Database;
use strata_security::AccessMode;

// =========================================================================
// Wire protocol tests
// =========================================================================

#[test]
fn wire_round_trip_various_sizes() {
    for size in [0, 1, 100, 4096, 65536] {
        let data = vec![0xABu8; size];
        let mut buf = Vec::new();
        wire::write_frame(&mut buf, &data).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let result = wire::read_frame(&mut cursor).unwrap();
        assert_eq!(result.len(), size);
        assert_eq!(result, data);
    }
}

// =========================================================================
// Protocol encode/decode tests
// =========================================================================

#[test]
fn protocol_ping_round_trip() {
    let req = Request {
        id: 1,
        command: Command::Ping,
    };
    let bytes = protocol::encode(&req).unwrap();
    let decoded: Request = protocol::decode(&bytes).unwrap();
    assert_eq!(decoded.id, 1);
    assert!(matches!(decoded.command, Command::Ping));
}

#[test]
fn protocol_kv_put_round_trip() {
    let req = Request {
        id: 42,
        command: Command::KvPut {
            branch: None,
            space: None,
            key: "hello".to_string(),
            value: Value::String("world".into()),
        },
    };
    let bytes = protocol::encode(&req).unwrap();
    let decoded: Request = protocol::decode(&bytes).unwrap();
    assert_eq!(decoded.id, 42);
    match decoded.command {
        Command::KvPut { key, value, .. } => {
            assert_eq!(key, "hello");
            assert_eq!(value, Value::String("world".into()));
        }
        _ => panic!("expected KvPut"),
    }
}

#[test]
fn protocol_response_ok_round_trip() {
    let resp = Response {
        id: 1,
        result: Ok(Output::Pong {
            version: "0.6.0".into(),
        }),
    };
    let bytes = protocol::encode(&resp).unwrap();
    let decoded: Response = protocol::decode(&bytes).unwrap();
    assert!(decoded.result.is_ok());
}

#[test]
fn protocol_response_err_round_trip() {
    let resp = Response {
        id: 2,
        result: Err(crate::Error::KeyNotFound {
            key: "missing".into(),
            hint: None,
        }),
    };
    let bytes = protocol::encode(&resp).unwrap();
    let decoded: Response = protocol::decode(&bytes).unwrap();
    assert!(decoded.result.is_err());
}

// =========================================================================
// Server + Client integration tests
// =========================================================================

fn setup_server() -> (tempfile::TempDir, IpcServer) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_runtime(search_only_primary_spec(dir.path())).unwrap();

    // Ensure default branch
    {
        let executor = crate::Executor::new(db.clone());
        executor
            .execute(Command::BranchCreate {
                branch_id: Some("default".to_string()),
                metadata: None,
            })
            .ok(); // ignore if exists
    }

    let server = IpcServer::start(dir.path(), db, AccessMode::ReadWrite).unwrap();
    (dir, server)
}

#[test]
fn ipc_ping() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client = IpcClient::connect(&socket_path).unwrap();
    let result = client.execute(Command::Ping).unwrap();
    match result {
        Output::Pong { version } => assert!(!version.is_empty()),
        _ => panic!("expected Pong"),
    }

    server.shutdown();
}

#[test]
fn ipc_kv_put_get() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client = IpcClient::connect(&socket_path).unwrap();

    // Put
    let result = client
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "greeting".to_string(),
            value: Value::String("hello".into()),
        })
        .unwrap();
    match result {
        Output::WriteResult { key, version } => {
            assert_eq!(key, "greeting");
            assert!(version > 0);
        }
        _ => panic!("expected WriteResult"),
    }

    // Get
    let result = client
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "greeting".to_string(),
            as_of: None,
        })
        .unwrap();
    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::String("hello".into()));
        }
        _ => panic!("expected MaybeVersioned(Some)"),
    }

    server.shutdown();
}

#[test]
fn ipc_transaction() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client = IpcClient::connect(&socket_path).unwrap();

    // Begin transaction
    let result = client
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    assert!(matches!(result, Output::TxnBegun));

    // Put within transaction
    client
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn_key".to_string(),
            value: Value::Int(42),
        })
        .unwrap();

    // Commit
    let result = client.execute(Command::TxnCommit).unwrap();
    match result {
        Output::TxnCommitted { version } => assert!(version > 0),
        _ => panic!("expected TxnCommitted"),
    }

    // Verify data persisted
    let result = client
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "txn_key".to_string(),
            as_of: None,
        })
        .unwrap();
    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::Int(42));
        }
        _ => panic!("expected value after commit"),
    }

    server.shutdown();
}

#[test]
fn ipc_multiple_clients() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client1 = IpcClient::connect(&socket_path).unwrap();
    let mut client2 = IpcClient::connect(&socket_path).unwrap();

    // Client 1 writes
    client1
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "from_client1".to_string(),
            value: Value::String("one".into()),
        })
        .unwrap();

    // Client 2 reads
    let result = client2
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "from_client1".to_string(),
            as_of: None,
        })
        .unwrap();
    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::String("one".into()));
        }
        _ => panic!("client2 should see client1's write"),
    }

    server.shutdown();
}

#[test]
fn ipc_connect_new() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let client1 = IpcClient::connect(&socket_path).unwrap();
    let mut client2 = client1.connect_new().unwrap();

    let result = client2.execute(Command::Ping).unwrap();
    assert!(matches!(result, Output::Pong { .. }));

    server.shutdown();
}

#[test]
fn ipc_session_has_no_local_executor() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let client = IpcClient::connect(&socket_path).unwrap();
    let mut session = Session::new_ipc(client);

    assert!(matches!(session, Session::Ipc(_)));
    assert!(session.executor().is_none());
    assert!(!session.in_transaction());
    assert!(matches!(session.execute(Command::Ping).unwrap(), Output::Pong { .. }));

    server.shutdown();
}

#[test]
fn ipc_session_tracks_remote_transaction_state() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let client = IpcClient::connect(&socket_path).unwrap();
    let mut session = Session::new_ipc(client);

    assert!(!session.in_transaction());

    assert!(matches!(
        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .unwrap(),
        Output::TxnBegun
    ));
    assert!(session.in_transaction());
    assert!(matches!(
        session.execute(Command::TxnIsActive).unwrap(),
        Output::Bool(true)
    ));

    assert!(matches!(
        session.execute(Command::TxnRollback).unwrap(),
        Output::TxnAborted
    ));
    assert!(!session.in_transaction());
    assert!(matches!(
        session.execute(Command::TxnIsActive).unwrap(),
        Output::Bool(false)
    ));

    server.shutdown();
}

#[test]
fn ipc_server_shutdown_cleans_up() {
    let (dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();
    let pid_path = dir.path().join("strata.pid");

    assert!(socket_path.exists());
    assert!(pid_path.exists());

    server.shutdown();
    // Drop cleans up
    drop(server);

    assert!(!socket_path.exists());
    assert!(!pid_path.exists());
}

#[test]
fn ipc_stale_socket_cleanup() {
    let dir = tempfile::tempdir().unwrap();

    // Create a stale socket file
    let socket_path = dir.path().join("strata.sock");
    std::fs::write(&socket_path, "stale").unwrap();

    // Server should clean up stale socket and start successfully
    let db = Database::open_runtime(search_only_primary_spec(dir.path())).unwrap();
    let executor = crate::Executor::new(db.clone());
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("default".to_string()),
            metadata: None,
        })
        .ok();

    let mut server = IpcServer::start(dir.path(), db, AccessMode::ReadWrite).unwrap();

    let mut client = IpcClient::connect(&socket_path).unwrap();
    let result = client.execute(Command::Ping).unwrap();
    assert!(matches!(result, Output::Pong { .. }));

    server.shutdown();
}

// =========================================================================
// Deeper edge-case tests
// =========================================================================

#[test]
fn wire_truncated_length() {
    // Only 2 bytes instead of 4 — should fail with UnexpectedEof
    let buf = vec![0u8, 1];
    let mut cursor = std::io::Cursor::new(buf);
    let err = wire::read_frame(&mut cursor).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn wire_truncated_payload() {
    // Header says 10 bytes but only 3 follow
    let mut buf = Vec::new();
    buf.extend_from_slice(&10u32.to_be_bytes());
    buf.extend_from_slice(&[1, 2, 3]);
    let mut cursor = std::io::Cursor::new(buf);
    let err = wire::read_frame(&mut cursor).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn wire_multiple_frames_sequential() {
    let mut buf = Vec::new();
    wire::write_frame(&mut buf, b"first").unwrap();
    wire::write_frame(&mut buf, b"second").unwrap();
    wire::write_frame(&mut buf, b"third").unwrap();

    let mut cursor = std::io::Cursor::new(buf);
    assert_eq!(wire::read_frame(&mut cursor).unwrap(), b"first");
    assert_eq!(wire::read_frame(&mut cursor).unwrap(), b"second");
    assert_eq!(wire::read_frame(&mut cursor).unwrap(), b"third");
    // No more frames
    assert!(wire::read_frame(&mut cursor).is_err());
}

#[test]
fn protocol_decode_garbage_fails() {
    let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB];
    let result: std::result::Result<Request, _> = protocol::decode(&garbage);
    assert!(result.is_err());
}

#[test]
fn ipc_concurrent_writers() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let threads: Vec<_> = (0..5)
        .map(|i| {
            let path = socket_path.clone();
            std::thread::spawn(move || {
                let mut client = IpcClient::connect(&path).unwrap();
                for j in 0..10 {
                    let key = format!("thread{}_{}", i, j);
                    client
                        .execute(Command::KvPut {
                            branch: None,
                            space: None,
                            key: key.clone(),
                            value: Value::Int((i * 10 + j) as i64),
                        })
                        .unwrap();
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    // Verify all keys are readable
    let mut client = IpcClient::connect(&socket_path).unwrap();
    for i in 0..5 {
        for j in 0..10 {
            let key = format!("thread{}_{}", i, j);
            let result = client
                .execute(Command::KvGet {
                    branch: None,
                    space: None,
                    key,
                    as_of: None,
                })
                .unwrap();
            match result {
                Output::MaybeVersioned(Some(vv)) => {
                    assert_eq!(vv.value, Value::Int((i * 10 + j) as i64));
                }
                _ => panic!("expected value for thread{}_{}", i, j),
            }
        }
    }

    server.shutdown();
}

#[test]
fn ipc_transaction_rollback() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client = IpcClient::connect(&socket_path).unwrap();

    // Begin + put + rollback
    client
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    client
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "rollback_key".to_string(),
            value: Value::String("should_not_exist".into()),
        })
        .unwrap();
    let result = client.execute(Command::TxnRollback).unwrap();
    assert!(matches!(result, Output::TxnAborted));

    // Key should not exist
    let result = client
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "rollback_key".to_string(),
            as_of: None,
        })
        .unwrap();
    match result {
        Output::MaybeVersioned(None) | Output::Maybe(None) => {}
        _ => panic!("expected None after rollback"),
    }

    server.shutdown();
}

#[test]
fn ipc_client_disconnect_doesnt_crash_server() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    // Connect and immediately drop client
    {
        let _client = IpcClient::connect(&socket_path).unwrap();
    }

    // Server should still work
    std::thread::sleep(std::time::Duration::from_millis(100));
    let mut client = IpcClient::connect(&socket_path).unwrap();
    let result = client.execute(Command::Ping).unwrap();
    assert!(matches!(result, Output::Pong { .. }));

    server.shutdown();
}

#[test]
fn ipc_malformed_frame_doesnt_crash_server() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    // Send garbage directly on the socket
    {
        let mut stream = std::os::unix::net::UnixStream::connect(&socket_path).unwrap();
        use std::io::Write;
        // Write a valid length header but garbage payload
        let len_bytes = 5u32.to_be_bytes();
        stream.write_all(&len_bytes).unwrap();
        stream.write_all(b"TRASH").unwrap();
        stream.flush().unwrap();
        // Drop — server handler should log and close this connection
    }

    std::thread::sleep(std::time::Duration::from_millis(100));

    // Server should still accept new clients
    let mut client = IpcClient::connect(&socket_path).unwrap();
    let result = client.execute(Command::Ping).unwrap();
    assert!(matches!(result, Output::Pong { .. }));

    server.shutdown();
}

#[test]
fn ipc_large_value() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    let mut client = IpcClient::connect(&socket_path).unwrap();

    // 1 MB value
    let large_value = "x".repeat(1024 * 1024);
    client
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "big_key".to_string(),
            value: Value::String(large_value.clone()),
        })
        .unwrap();

    let result = client
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "big_key".to_string(),
            as_of: None,
        })
        .unwrap();
    match result {
        Output::MaybeVersioned(Some(vv)) => {
            assert_eq!(vv.value, Value::String(large_value));
        }
        _ => panic!("expected large value back"),
    }

    server.shutdown();
}

#[test]
fn ipc_rapid_connect_disconnect() {
    let (_dir, mut server) = setup_server();
    let socket_path = server.socket_path().to_path_buf();

    // Rapidly connect and disconnect 20 times
    for _ in 0..20 {
        let mut client = IpcClient::connect(&socket_path).unwrap();
        client.execute(Command::Ping).unwrap();
        drop(client);
    }

    // Server should still work
    let mut client = IpcClient::connect(&socket_path).unwrap();
    let result = client.execute(Command::Ping).unwrap();
    assert!(matches!(result, Output::Pong { .. }));

    server.shutdown();
}
