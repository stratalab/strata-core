//! Adversarial regressions for public engine APIs.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

use strata_core::{BranchId, Value};
use strata_engine::database::OpenSpec;
use strata_engine::{Database, EventLog, KVStore, SearchSubsystem, StrataError};

fn open_cache() -> Arc<Database> {
    Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap()
}

fn payload_int(value: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(value))]))
}

#[test]
fn concurrent_kv_writes_leave_a_valid_committed_value() {
    let db = open_cache();
    let branch_id = BranchId::new();
    let kv = KVStore::new(db.clone());

    let workers = 6;
    let writes_per_worker = 40;
    let barrier = Arc::new(Barrier::new(workers));

    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let kv = kv.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for seq in 0..writes_per_worker {
                    kv.put(
                        &branch_id,
                        "default",
                        "shared",
                        Value::Int((worker * 1_000 + seq) as i64),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let final_value = kv.get(&branch_id, "default", "shared").unwrap().unwrap();
    match final_value {
        Value::Int(value) => {
            assert!((0..(workers * 1_000 + writes_per_worker)).contains(&(value as usize)));
        }
        other => panic!("expected integer value, got {other:?}"),
    }
}

#[test]
fn concurrent_event_appends_keep_sequences_dense_and_hashes_linked() {
    let db = open_cache();
    let branch_id = BranchId::new();
    let log = EventLog::new(db);

    let workers = 4;
    let events_per_worker = 12;
    let barrier = Arc::new(Barrier::new(workers));

    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let log = log.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for seq in 0..events_per_worker {
                    let mut attempts = 0;
                    loop {
                        match log.append(
                            &branch_id,
                            "default",
                            "task",
                            payload_int((worker * 100 + seq) as i64),
                        ) {
                            Ok(_) => break,
                            Err(StrataError::TransactionAborted { .. }) if attempts < 64 => {
                                attempts += 1;
                                thread::yield_now();
                            }
                            Err(err) => panic!("append failed after {attempts} retries: {err}"),
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let total = (workers * events_per_worker) as u64;
    assert_eq!(log.len(&branch_id, "default").unwrap(), total);
    let verification = log.verify_chain(&branch_id, "default").unwrap();
    assert_eq!(verification, strata_engine::ChainVerification::valid(total));

    let first = log.get(&branch_id, "default", 0).unwrap().unwrap();
    assert_eq!(first.value.prev_hash, [0u8; 32]);

    for seq in 1..total {
        let previous = log.get(&branch_id, "default", seq - 1).unwrap().unwrap();
        let current = log.get(&branch_id, "default", seq).unwrap().unwrap();
        assert_eq!(current.value.prev_hash, previous.value.hash);
    }
}
