//! Unit tests for the `database` module.
//!
//! Split into themed submodules to keep each file scannable. Shared helpers
//! (test-only `Database` constructors, WAL write harness, namespace / value
//! helpers, the `TestRuntimeSubsystem` double) live in this file so every
//! submodule picks them up via `use super::*;`.

// Re-export parent-module items commonly used in tests. Submodules reach
// them through `use super::*;` so the test bodies that were written when
// everything lived in a single `tests.rs` keep compiling unchanged.
pub use super::*;
pub use crate::recovery::Subsystem;
pub use serial_test::serial;
pub use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
pub use std::sync::Arc;
pub use std::time::Duration;
pub use strata_concurrency::TransactionPayload;
pub use strata_concurrency::__internal as concurrency_test_hooks;
pub use strata_core::id::{CommitVersion, TxnId};
pub use strata_core::types::{Key, Namespace, TypeTag};
pub use strata_core::value::Value;
pub use strata_core::{Storage, StrataError, Timestamp, WriteMode};
pub use strata_durability::__internal::WalWriterEngineExt;
pub use strata_durability::codec::IdentityCodec;
pub use strata_durability::format::WalRecord;
pub use strata_durability::now_micros;
pub use strata_durability::wal::WalConfig;
pub use strata_durability::KvSnapshotEntry;
pub use tempfile::TempDir;

// `super::test_hooks` is a private sibling module of `database::tests`;
// a non-`pub` `use` still makes it reachable from submodules via
// `super::test_hooks::...` because child modules can see private items of
// their parent.
#[allow(unused_imports)]
use super::test_hooks;

mod checkpoint;
mod contention;
mod open;
mod regressions;
mod shutdown;
mod transactions;

// ========================================================================
// Shared test helpers
// ========================================================================

impl Database {
    /// Test-only helper: open with a specific durability mode.
    pub fn open_with_durability<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
    ) -> StrataResult<Arc<Self>> {
        let dur_str = match durability_mode {
            DurabilityMode::Always => "always",
            DurabilityMode::Cache => "cache",
            _ => "standard",
        };
        let cfg = StrataConfig {
            durability: dur_str.to_string(),
            ..StrataConfig::default()
        };
        let spec = super::spec::OpenSpec::primary(path)
            .with_config(cfg)
            .with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }
}

/// Helper: write a committed transaction to the segmented WAL.
pub fn write_wal_txn(
    wal_dir: &std::path::Path,
    _txn_id: u64,
    branch_id: BranchId,
    puts: Vec<(Key, Value)>,
    deletes: Vec<Key>,
    version: u64,
) {
    let mut wal = WalWriter::new(
        wal_dir.to_path_buf(),
        [0u8; 16],
        DurabilityMode::Always,
        WalConfig::for_testing(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let payload = TransactionPayload {
        version,
        puts,
        deletes,
        put_ttls: vec![],
    };
    // Use version (commit_version) as WAL record ordering key (#1696)
    let record = WalRecord::new(
        TxnId(version),
        *branch_id.as_bytes(),
        now_micros(),
        payload.to_bytes(),
    );
    wal.append(&record).unwrap();
    wal.flush().unwrap();
}

pub fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition not satisfied within {:?}", timeout);
}

pub fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch_id, "default".to_string()))
}

/// Helper: blind-write a single key via transaction_with_version.
pub fn blind_write(db: &Database, key: Key, value: Value) -> u64 {
    let branch_id = key.namespace.branch_id;
    let ((), version) = db
        .transaction_with_version(branch_id, |txn| txn.put(key, value))
        .expect("blind write failed");
    version
}

pub struct TestRuntimeSubsystem {
    pub name: &'static str,
    pub events: Option<Arc<parking_lot::Mutex<Vec<&'static str>>>>,
    pub fail_initialize_once: Option<Arc<AtomicBool>>,
}

impl TestRuntimeSubsystem {
    pub fn named(name: &'static str) -> Self {
        Self {
            name,
            events: None,
            fail_initialize_once: None,
        }
    }

    pub fn recording(
        name: &'static str,
        events: Arc<parking_lot::Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            name,
            events: Some(events),
            fail_initialize_once: None,
        }
    }

    pub fn fail_initialize_once(name: &'static str, fail_flag: Arc<AtomicBool>) -> Self {
        Self {
            name,
            events: None,
            fail_initialize_once: Some(fail_flag),
        }
    }

    fn record(&self, event: &'static str) {
        if let Some(events) = &self.events {
            events.lock().push(event);
        }
    }
}

impl Subsystem for TestRuntimeSubsystem {
    fn name(&self) -> &'static str {
        self.name
    }

    fn recover(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("recover");
        Ok(())
    }

    fn initialize(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("initialize");
        if self
            .fail_initialize_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            return Err(StrataError::internal("initialize failed"));
        }
        Ok(())
    }

    fn bootstrap(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("bootstrap");
        Ok(())
    }
}
