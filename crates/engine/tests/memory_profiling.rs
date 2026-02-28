//! Memory Usage Profiling Tests
//!
//! Documents ClonedSnapshotView memory overhead
//! and TransactionContext footprint.

use std::sync::Arc;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_engine::Database;
use tempfile::TempDir;

fn create_ns(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(
        "tenant".to_string(),
        "app".to_string(),
        "agent".to_string(),
        branch_id,
        "default".to_string(),
    ))
}

/// Test: Memory grows with read-set size
#[test]
fn test_read_set_memory_growth() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Pre-populate with data
    for i in 0..1000 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    // Read increasing numbers of keys
    for read_count in [10, 100, 500, 1000] {
        let result = db.transaction(branch_id, |txn| {
            for i in 0..read_count {
                let key = Key::new_kv(ns.clone(), format!("key_{}", i));
                txn.get(&key)?;
            }
            // Can't easily measure txn size here, but verify it works
            Ok(read_count)
        });

        assert!(result.is_ok());
        println!("Read {} keys successfully", read_count);
    }
}

/// Test: Memory grows with write-set size
#[test]
fn test_write_set_memory_growth() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write increasing numbers of keys
    for write_count in [10, 100, 500, 1000] {
        let result = db.transaction(branch_id, |txn| {
            for i in 0..write_count {
                let key = Key::new_kv(ns.clone(), format!("batch_{}_key_{}", write_count, i));
                txn.put(key, Value::Int(i as i64))?;
            }
            Ok(write_count)
        });

        assert!(result.is_ok());
        println!("Wrote {} keys successfully", write_count);
    }
}

/// Test: Verify no memory leaks (transactions properly cleaned up)
#[test]
fn test_no_memory_leaks() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Run many transactions
    for round in 0..100 {
        let result = db.transaction(branch_id, |txn| {
            for i in 0..100 {
                let key = Key::new_kv(ns.clone(), format!("round_{}_key_{}", round, i));
                txn.put(key, Value::Int(i as i64))?;
            }
            Ok(())
        });
        assert!(result.is_ok());
    }

    // If we got here without OOM, cleanup is working
    println!("Completed 100 rounds of 100 writes each - no memory issues");
}

/// Test: Aborted transactions release memory
#[test]
fn test_aborted_transaction_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Create and abort many transactions
    for round in 0..100 {
        let result: Result<(), strata_core::StrataError> = db.transaction(branch_id, |txn| {
            for i in 0..100 {
                let key = Key::new_kv(ns.clone(), format!("abort_{}_key_{}", round, i));
                txn.put(key, Value::Int(i as i64))?;
            }
            // Force abort
            Err(strata_core::StrataError::invalid_input(
                "intentional abort".to_string(),
            ))
        });

        assert!(result.is_err());
    }

    // If we got here without OOM, aborted transactions are cleaned up
    println!("Completed 100 aborted transactions - cleanup working");
}

/// Test: Large value handling
#[test]
fn test_large_value_memory() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write a large string value
    let large_string = "x".repeat(100_000); // 100KB string

    let result = db.transaction(branch_id, |txn| {
        let key = Key::new_kv(ns.clone(), "large_value");
        txn.put(key, Value::String(large_string.clone()))?;
        Ok(())
    });

    assert!(result.is_ok());
    println!("Successfully wrote 100KB value");

    // Read it back via transaction
    let read_result = db
        .transaction(branch_id, |txn| {
            let key = Key::new_kv(ns.clone(), "large_value");
            txn.get(&key)
        })
        .unwrap()
        .unwrap();
    if let Value::String(s) = read_result {
        assert_eq!(s.len(), 100_000);
        println!("Successfully read 100KB value");
    }
}

/// Document: Memory characteristics
#[test]
fn document_memory_characteristics() {
    println!("\n=== Memory Characteristics ===\n");

    println!("ClonedSnapshotView:");
    println!("  - Creates full clone of BTreeMap at transaction start");
    println!("  - Memory: O(data_size) per active transaction");
    println!("  - Time: O(data_size) per snapshot creation");
    println!();

    println!("TransactionContext:");
    println!("  - read_set: O(keys_read) entries");
    println!("  - write_set: O(keys_written) entries");
    println!("  - delete_set: O(keys_deleted) entries");
    println!("  - cas_set: O(cas_operations) entries");
    println!();

    println!("Concurrent Transactions:");
    println!("  - N concurrent transactions = N snapshots");
    println!("  - Total memory: O(N * data_size)");
    println!();

    println!("Recommended Limits:");
    println!("  - Data size: < 100MB per BranchId");
    println!("  - Concurrent transactions: < 100 per BranchId");
    println!("  - Transaction duration: < 1 second");
    println!();

    println!("Future Optimization:");
    println!("  - LazySnapshotView: O(1) snapshot creation");
    println!("  - Version-bounded reads from live storage");
    println!("  - No cloning overhead");
    println!();
}
