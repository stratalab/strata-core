//! Recovery infrastructure for transaction-aware database recovery
//!
//! Per spec Section 5 (Replay Semantics):
//! - Replays do NOT re-run conflict detection
//! - Replays apply commit decisions, not re-execute logic
//! - Replays are single-threaded
//! - Versions are preserved exactly
//!
//! ## Recovery Procedure
//!
//! 1. Load snapshot (if exists) - not implemented in M2
//! 2. Scan segmented WAL directory for records
//! 3. Each WalRecord = one committed transaction (TransactionPayload)
//! 4. Apply all records in order
//! 5. Initialize TransactionManager with final version

use crate::payload::TransactionPayload;
use crate::TransactionManager;
use std::path::PathBuf;
use strata_core::StrataResult;
use strata_durability::wal::WalReader;
use strata_storage::SegmentedStore;

/// Coordinates database recovery after crash or restart
///
/// Per spec Section 5.4:
/// 1. Loads checkpoint (if exists) - not implemented in M2
/// 2. Reads all WAL records from the segmented WAL directory
/// 3. Each record is a committed transaction (one WalRecord per txn)
/// 4. Applies all writes/deletes with version preservation
/// 5. Initializes TransactionManager with final version
pub struct RecoveryCoordinator {
    /// Path to WAL directory (contains wal-NNNNNN.seg files)
    wal_dir: PathBuf,
    /// Path to snapshot directory (optional, not used in M2)
    #[allow(dead_code)]
    snapshot_path: Option<PathBuf>,
    /// Path to segments directory for on-disk segment storage (optional)
    segments_dir: Option<PathBuf>,
    /// Write buffer size in bytes for SegmentedStore (used when segments_dir is set)
    write_buffer_size: usize,
}

impl RecoveryCoordinator {
    /// Create a new recovery coordinator
    ///
    /// # Arguments
    /// * `wal_dir` - Path to the segmented WAL directory
    pub fn new(wal_dir: PathBuf) -> Self {
        RecoveryCoordinator {
            wal_dir,
            snapshot_path: None,
            segments_dir: None,
            write_buffer_size: 0,
        }
    }

    /// Set snapshot path for checkpoint-based recovery (M3+ feature)
    ///
    /// Note: Snapshot-based recovery is not implemented in M2.
    /// This method is provided for future extensibility.
    #[allow(dead_code)]
    pub(crate) fn with_snapshot_path(mut self, path: PathBuf) -> Self {
        self.snapshot_path = Some(path);
        self
    }

    /// Set the segments directory and write buffer size for on-disk segment storage.
    ///
    /// When set, recovery will create a `SegmentedStore::with_dir()` instead of
    /// an ephemeral `SegmentedStore::new()`, enabling flush/compaction to persist
    /// frozen memtables as on-disk SST segments.
    pub fn with_segments(mut self, segments_dir: PathBuf, write_buffer_size: usize) -> Self {
        self.segments_dir = Some(segments_dir);
        self.write_buffer_size = write_buffer_size;
        self
    }

    /// Perform recovery and return initialized components
    ///
    /// Each WalRecord in the segmented WAL represents a single committed
    /// transaction. The writeset field contains a serialized TransactionPayload
    /// with the version, puts, and deletes.
    ///
    /// # Returns
    /// - `RecoveryResult` containing storage, transaction manager, and stats
    ///
    /// # Errors
    /// - If WAL directory cannot be read
    /// - If record deserialization fails
    pub fn recover(&self) -> StrataResult<RecoveryResult> {
        let storage = match &self.segments_dir {
            Some(dir) => SegmentedStore::with_dir(dir.clone(), self.write_buffer_size),
            None => SegmentedStore::new(),
        };
        let mut max_version = 0u64;
        let mut max_txn_id = 0u64;
        let mut stats = RecoveryStats::default();

        // If WAL dir doesn't exist, return empty result
        if !self.wal_dir.exists() {
            return Ok(RecoveryResult {
                storage,
                txn_manager: TransactionManager::new(0),
                stats,
            });
        }

        // Stream records from segmented WAL one segment at a time.
        // This bounds memory to O(largest_segment) instead of O(total_wal_size),
        // preventing OOM on large databases.
        let reader = WalReader::new();
        let records_iter = reader
            .iter_all(&self.wal_dir)
            .map_err(|e| strata_core::StrataError::storage(format!("WAL read failed: {}", e)))?;

        for record_result in records_iter {
            let record = record_result.map_err(|e| {
                strata_core::StrataError::storage(format!("WAL segment read failed: {}", e))
            })?;
            max_txn_id = max_txn_id.max(record.txn_id);

            let payload = TransactionPayload::from_bytes(&record.writeset).map_err(|e| {
                strata_core::StrataError::storage(format!(
                    "Failed to decode transaction payload for txn {}: {}",
                    record.txn_id, e
                ))
            })?;

            max_version = max_version.max(payload.version);

            // Apply puts — use recovery-specific method to preserve original
            // commit timestamp instead of generating a new Timestamp::now().
            for (key, value) in &payload.puts {
                storage.put_recovery_entry(
                    key.clone(),
                    value.clone(),
                    payload.version,
                    record.timestamp,
                )?;
                stats.writes_applied += 1;
            }

            // Apply deletes with original timestamp
            for key in &payload.deletes {
                storage.delete_recovery_entry(key, payload.version, record.timestamp)?;
                stats.deletes_applied += 1;
            }

            stats.txns_replayed += 1;
        }

        stats.final_version = max_version;
        stats.max_txn_id = max_txn_id;

        let txn_manager = TransactionManager::with_txn_id(max_version, max_txn_id);

        Ok(RecoveryResult {
            storage,
            txn_manager,
            stats,
        })
    }
}

/// Result of recovery operation
pub struct RecoveryResult {
    /// Recovered storage with all committed transactions applied
    pub storage: SegmentedStore,
    /// Transaction manager initialized with recovered version
    ///
    /// Per spec Section 6.1: The global version counter is set to the
    /// highest version seen in the WAL, ensuring new transactions get
    /// monotonically increasing versions.
    pub txn_manager: TransactionManager,
    /// Statistics about the recovery process
    pub stats: RecoveryStats,
}

impl RecoveryResult {
    /// Create an empty recovery result with fresh storage and zero stats.
    ///
    /// Used as a fallback when recovery fails (e.g., corrupted snapshot or
    /// unreadable WAL), allowing the database to start with a clean state
    /// rather than refusing to open.
    pub fn empty() -> Self {
        RecoveryResult {
            storage: SegmentedStore::new(),
            txn_manager: TransactionManager::new(0),
            stats: RecoveryStats::default(),
        }
    }
}

/// Statistics from recovery
///
/// Provides detailed information about what happened during recovery,
/// useful for debugging, monitoring, and verification.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Number of committed transactions replayed
    ///
    /// Each WalRecord = one committed transaction.
    pub txns_replayed: usize,

    /// Number of incomplete transactions discarded
    ///
    /// With the segmented WAL, incomplete transactions never produce a
    /// WalRecord (the record is only written after commit), so this is
    /// always 0. Partial records are silently skipped by the reader.
    pub incomplete_txns: usize,

    /// Number of aborted transactions discarded
    ///
    /// With the segmented WAL, aborted transactions never produce a
    /// WalRecord, so this is always 0.
    pub aborted_txns: usize,

    /// Number of write operations applied
    pub writes_applied: usize,

    /// Number of delete operations applied
    pub deletes_applied: usize,

    /// Final version after recovery
    ///
    /// This is the highest version seen in the WAL, used to initialize
    /// the TransactionManager's version counter.
    pub final_version: u64,

    /// Maximum transaction ID seen in WAL
    ///
    /// This is used to initialize the TransactionManager's next_txn_id counter
    /// to ensure new transactions get unique IDs that don't conflict with
    /// transactions already in the WAL.
    pub max_txn_id: u64,

    /// Whether recovery was from checkpoint
    ///
    /// In M2, this is always false as checkpoint-based recovery is not implemented.
    pub from_checkpoint: bool,
}

impl RecoveryStats {
    /// Total operations applied (writes + deletes)
    pub fn total_operations(&self) -> usize {
        self.writes_applied + self.deletes_applied
    }

    /// Total transactions found (replayed + incomplete + aborted)
    pub fn total_transactions(&self) -> usize {
        self.txns_replayed + self.incomplete_txns + self.aborted_txns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::TransactionPayload;
    use std::sync::Arc;
    use strata_core::types::{BranchId, Key, Namespace};
    use strata_core::value::Value;
    use strata_durability::codec::IdentityCodec;
    use strata_durability::format::WalRecord;
    use strata_durability::now_micros;
    use strata_durability::wal::{DurabilityMode, WalConfig, WalWriter};
    use tempfile::TempDir;

    fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn create_test_wal(dir: &std::path::Path) -> WalWriter {
        WalWriter::new(
            dir.to_path_buf(),
            [0u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap()
    }

    /// Helper: write a committed transaction to the WAL
    fn write_txn(
        wal: &mut WalWriter,
        txn_id: u64,
        branch_id: BranchId,
        puts: Vec<(Key, Value)>,
        deletes: Vec<Key>,
        version: u64,
    ) {
        let payload = TransactionPayload {
            version,
            puts,
            deletes,
        };
        let record = WalRecord::new(
            txn_id,
            *branch_id.as_bytes(),
            now_micros(),
            payload.to_bytes(),
        );
        wal.append(&record).unwrap();
        wal.flush().unwrap();
    }

    #[test]
    fn test_recovery_empty_wal() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        // Create empty WAL directory with an empty segment
        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, 0);
        assert_eq!(result.txn_manager.current_version(), 0);
    }

    #[test]
    fn test_recovery_nonexistent_dir() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("nonexistent");

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, 0);
    }

    #[test]
    fn test_recovery_committed_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "test_key");

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(key.clone(), Value::Int(42))],
                vec![],
                100,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.writes_applied, 1);
        assert_eq!(result.stats.final_version, 100);
        assert_eq!(result.txn_manager.current_version(), 100);

        let stored = result
            .storage
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::Int(42));
        assert_eq!(stored.version.as_u64(), 100);
    }

    #[test]
    fn test_recovery_version_preservation() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);

            // Transaction 1: version 100
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![
                    (Key::new_kv(ns.clone(), "key1"), Value::Int(1)),
                    (Key::new_kv(ns.clone(), "key2"), Value::Int(2)),
                ],
                vec![],
                100,
            );

            // Transaction 2: version 200
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key3"), Value::Int(3))],
                vec![],
                200,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.final_version, 200);
        assert_eq!(result.txn_manager.current_version(), 200);

        let key1 = Key::new_kv(ns.clone(), "key1");
        assert_eq!(
            result
                .storage
                .get_versioned(&key1, u64::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            100
        );

        let key2 = Key::new_kv(ns.clone(), "key2");
        assert_eq!(
            result
                .storage
                .get_versioned(&key2, u64::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            100
        );

        let key3 = Key::new_kv(ns.clone(), "key3");
        assert_eq!(
            result
                .storage
                .get_versioned(&key3, u64::MAX)
                .unwrap()
                .unwrap()
                .version
                .as_u64(),
            200
        );
    }

    #[test]
    fn test_recovery_determinism() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=5u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i * 100,
                );
            }
        }

        let coordinator = RecoveryCoordinator::new(wal_dir.clone());
        let result1 = coordinator.recover().unwrap();

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result2 = coordinator.recover().unwrap();

        assert_eq!(result1.stats.final_version, result2.stats.final_version);
        assert_eq!(result1.stats.txns_replayed, result2.stats.txns_replayed);
        assert_eq!(result1.stats.writes_applied, result2.stats.writes_applied);

        for i in 1..=5u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let v1 = result1
                .storage
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .unwrap();
            let v2 = result2
                .storage
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(v1.value, v2.value);
            assert_eq!(v1.version, v2.version);
        }
    }

    #[test]
    fn test_recovery_with_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "deleted_key");

        {
            let mut wal = create_test_wal(&wal_dir);

            // Write then delete in separate transactions
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(key.clone(), Value::String("exists".to_string()))],
                vec![],
                100,
            );
            write_txn(&mut wal, 2, branch_id, vec![], vec![key.clone()], 101);
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.writes_applied, 1);
        assert_eq!(result.stats.deletes_applied, 1);

        // Key should be deleted
        assert!(result
            .storage
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_recovery_stats_helpers() {
        let stats = RecoveryStats {
            txns_replayed: 5,
            incomplete_txns: 0,
            aborted_txns: 0,
            writes_applied: 10,
            deletes_applied: 3,
            final_version: 100,
            max_txn_id: 8,
            from_checkpoint: false,
        };

        assert_eq!(stats.total_operations(), 13);
        assert_eq!(stats.total_transactions(), 5);
    }

    #[test]
    fn test_recovery_coordinator_builder() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let snapshot_path = temp_dir.path().join("snapshots");

        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let coordinator = RecoveryCoordinator::new(wal_dir).with_snapshot_path(snapshot_path);

        let result = coordinator.recover().unwrap();
        assert!(!result.stats.from_checkpoint);
    }

    // ========================================
    // Crash Scenario Tests
    // ========================================
    //
    // With the segmented WAL, crash scenarios are simpler:
    // - A WalRecord is only written for committed transactions
    // - Partial records at end of segment are silently skipped
    // - No BeginTxn/CommitTxn framing means no "incomplete" transactions

    #[test]
    fn test_crash_before_any_activity() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        std::fs::create_dir_all(&wal_dir).unwrap();
        let _wal = create_test_wal(&wal_dir);

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 0);
        assert_eq!(result.stats.final_version, 0);
        assert_eq!(result.stats.incomplete_txns, 0);
        assert_eq!(result.txn_manager.current_version(), 0);
    }

    #[test]
    fn test_crash_after_commit_written() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "durable_key"),
                    Value::String("must_exist".to_string()),
                )],
                vec![],
                100,
            );
            // CRASH after commit marker written - record is durable
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 1);
        assert_eq!(result.stats.incomplete_txns, 0);

        let stored = result
            .storage
            .get_versioned(&Key::new_kv(ns, "durable_key"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::String("must_exist".to_string()));
        assert_eq!(stored.version.as_u64(), 100);
    }

    #[test]
    fn test_partial_record_at_end() {
        // Simulate crash mid-write by appending garbage after valid records
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "valid"), Value::Int(42))],
                vec![],
                100,
            );
        }

        // Append garbage to simulate crash mid-write of a second record
        let segment_path = strata_durability::format::WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 20]).unwrap();

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        // Valid record should be recovered, garbage skipped
        assert_eq!(result.stats.txns_replayed, 1);
        let stored = result
            .storage
            .get_versioned(&Key::new_kv(ns, "valid"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_crash_recovery_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key"), Value::Int(42))],
                vec![],
                100,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir.clone());
        let result1 = coordinator.recover().unwrap();

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result2 = coordinator.recover().unwrap();

        assert_eq!(result1.stats.txns_replayed, result2.stats.txns_replayed);
        assert_eq!(result1.stats.final_version, result2.stats.final_version);
        assert_eq!(result1.stats.writes_applied, result2.stats.writes_applied);

        let v1 = result1
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key"), u64::MAX)
            .unwrap()
            .unwrap();
        let v2 = result2
            .storage
            .get_versioned(&Key::new_kv(ns, "key"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(v1.value, v2.value);
        assert_eq!(v1.version, v2.version);
    }

    #[test]
    fn test_recovery_version_counter() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns, "key"), Value::Int(1))],
                vec![],
                999,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.txn_manager.current_version(), 999);
        assert_eq!(result.stats.final_version, 999);
    }

    #[test]
    fn test_full_database_lifecycle_with_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=10u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 10);
        assert_eq!(result.stats.final_version, 10);
        assert_eq!(result.txn_manager.current_version(), 10);

        for i in 1..=10u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let stored = result
                .storage
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64 * 10));
            assert_eq!(stored.version.as_u64(), i);
        }
    }

    #[test]
    fn test_recovery_mixed_operations_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);

            // Txn 1: Write key1
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key1"),
                    Value::String("initial".to_string()),
                )],
                vec![],
                1,
            );

            // Txn 2: Update key1
            write_txn(
                &mut wal,
                2,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key1"),
                    Value::String("updated".to_string()),
                )],
                vec![],
                2,
            );

            // Txn 3: Write key2 and delete it
            write_txn(
                &mut wal,
                3,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key2"),
                    Value::String("temp".to_string()),
                )],
                vec![Key::new_kv(ns.clone(), "key2")],
                3,
            );

            // Txn 4: Write key3
            write_txn(
                &mut wal,
                4,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "key3"), Value::Int(42))],
                vec![],
                5,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 4);
        assert_eq!(result.stats.writes_applied, 4);
        assert_eq!(result.stats.deletes_applied, 1);
        assert_eq!(result.stats.final_version, 5);

        // key1 should be "updated" at version 2
        let key1 = result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key1"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(key1.value, Value::String("updated".to_string()));
        assert_eq!(key1.version.as_u64(), 2);

        // key2 should be deleted
        assert!(result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key2"), u64::MAX)
            .unwrap()
            .is_none());

        // key3 should exist
        let key3 = result
            .storage
            .get_versioned(&Key::new_kv(ns.clone(), "key3"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(key3.value, Value::Int(42));
        assert_eq!(key3.version.as_u64(), 5);
    }

    #[test]
    fn test_recovery_maintains_transaction_order() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for v in [100u64, 200, 300] {
                write_txn(
                    &mut wal,
                    v,
                    branch_id,
                    vec![(Key::new_kv(ns.clone(), "counter"), Value::Int(v as i64))],
                    vec![],
                    v,
                );
            }
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        let counter = result
            .storage
            .get_versioned(&Key::new_kv(ns, "counter"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(counter.value, Value::Int(300));
        assert_eq!(counter.version.as_u64(), 300);
    }

    #[test]
    fn test_new_transactions_after_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            write_txn(
                &mut wal,
                1,
                branch_id,
                vec![(Key::new_kv(ns, "existing"), Value::Int(100))],
                vec![],
                100,
            );
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.txn_manager.current_version(), 100);
        let new_txn_id = result.txn_manager.next_txn_id().unwrap();
        assert!(new_txn_id > 0);
    }

    #[test]
    fn test_recovery_many_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let num_txns = 100u64;

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=num_txns {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key_{}", i)),
                        Value::Int(i as i64),
                    )],
                    vec![],
                    i,
                );
            }
        }

        let coordinator = RecoveryCoordinator::new(wal_dir);
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, num_txns as usize);
        assert_eq!(result.stats.final_version, num_txns);

        for i in [1, 50, 100] {
            let key = Key::new_kv(ns.clone(), format!("key_{}", i));
            let stored = result
                .storage
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64));
        }
    }

    /// Verify that streaming recovery (iter_all) produces the same results
    /// as the prior bulk approach (read_all). This test uses iter_all
    /// indirectly via RecoveryCoordinator::recover(), which now uses iter_all.
    #[test]
    fn test_streaming_recovery_matches_read_all() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        {
            let mut wal = create_test_wal(&wal_dir);
            for i in 1..=20u64 {
                write_txn(
                    &mut wal,
                    i,
                    branch_id,
                    vec![(
                        Key::new_kv(ns.clone(), format!("key{}", i)),
                        Value::Int(i as i64 * 10),
                    )],
                    vec![],
                    i,
                );
            }
        }

        // Verify recovery (which uses iter_all) returns correct results
        let coordinator = RecoveryCoordinator::new(wal_dir.clone());
        let result = coordinator.recover().unwrap();

        assert_eq!(result.stats.txns_replayed, 20);
        assert_eq!(result.stats.final_version, 20);
        assert_eq!(result.stats.writes_applied, 20);
        assert_eq!(result.txn_manager.current_version(), 20);

        // Cross-check: read_all should yield the same records
        let reader = WalReader::new();
        let read_all_result = reader.read_all(&wal_dir).unwrap();
        assert_eq!(read_all_result.records.len(), 20);

        // Verify each key was written correctly
        for i in 1..=20u64 {
            let key = Key::new_kv(ns.clone(), format!("key{}", i));
            let stored = result
                .storage
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(stored.value, Value::Int(i as i64 * 10));
            assert_eq!(stored.version.as_u64(), i);
        }
    }
}
