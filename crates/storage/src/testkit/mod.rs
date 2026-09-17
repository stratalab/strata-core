//! Feature-gated storage conformance testkit.

#![doc(hidden)]

use std::fmt;

mod api;
mod branch_lsm;
mod commit_runtime;
mod commit_runtime_allocator;
mod commit_runtime_branch_guards;
mod commit_runtime_cache;
mod commit_runtime_conflicts;
mod commit_runtime_durable;
mod commit_runtime_model;
mod commit_runtime_outcome;
mod commit_runtime_runner;
mod commit_runtime_script;
mod commit_runtime_timeline;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod compound_faults;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod config_differential;
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
mod corpus_harvest;
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
mod dual_mutation;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod fault_sweep;
mod format_fuzz;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod fs_models;
mod integration_harness;
mod layout_fuzz;
mod leak;
mod lifecycle;
#[cfg(all(
    any(test, feature = "fault-injection"),
    test,
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod process_crash;
mod progress_watchdog;
mod quarantine_fuzz;
mod recovery_oracle;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod recovery_read_faults;
mod reopen_retry;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod reordering_backend;
pub(crate) mod rng;
mod service_fuzz;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod simulation;
mod table_runtime;
mod wal_segment_loss;
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
mod write_ordering_watchdog;

pub use api::{
    check_storage_api_branch_model_contract, check_storage_api_commit_fault_contract,
    check_storage_api_commit_model_contract, check_storage_api_diagnostics_model_contract,
    check_storage_api_maintenance_fault_contract, check_storage_api_maintenance_model_contract,
    check_storage_api_read_model_contract, StorageApiBranchModelOutcome,
    StorageApiCommitFaultOutcome, StorageApiCommitModelOutcome, StorageApiDiagnosticsModelOutcome,
    StorageApiMaintenanceFaultOutcome, StorageApiMaintenanceModelOutcome,
    StorageApiReadModelOutcome,
};
pub use branch_lsm::{
    check_branch_lsm_fault_window_contract, check_branch_lsm_inheritance_contract,
    check_branch_lsm_install_contract, check_branch_lsm_reads_contract,
    check_branch_lsm_reference_model_contract, check_branch_lsm_scaffold_contract,
    BranchLsmScaffoldOutcome,
};
pub use commit_runtime::{check_commit_runtime_scaffold_contract, CommitRuntimeScaffoldOutcome};
pub use commit_runtime_runner::{
    check_commit_runtime_batch_contract, check_commit_runtime_conflict_contract,
    check_commit_runtime_conflict_script_contract, check_commit_runtime_durable_contract,
    check_commit_runtime_durable_script_contract, check_commit_runtime_fault_contract,
    check_commit_runtime_generated_input_contract, check_commit_runtime_script_contract,
    check_commit_runtime_timeline_contract, check_commit_runtime_timeline_script_contract,
    CommitRuntimeAssuranceOutcome,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use compound_faults::{
    run_compound_fault_maintenance_cases, run_compound_fault_recovery_sweep,
    CompoundFaultMaintenanceOutcome, CompoundFaultRecoveryOutcome,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use config_differential::{
    run_config_differential, run_low_memory_pressure_equivalence, storage_config_matrix,
    ConfigDifferentialOutcome, StorageConfigCase,
};
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use corpus_harvest::{harvest_format_corpus, HarvestSeed};
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use dual_mutation::{check_dual_mutation_contract, DualMutationOutcome};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use fault_sweep::{run_fault_sweep_harness, FaultSweepOutcome};
pub use format_fuzz::{
    check_table_format_model_script, decode_format_bytes, FormatDecodeOutcome, FormatDecoder,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use fs_models::{run_fs_model_harness, FsModelSweepOutcome};
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use integration_harness::run_localfs_crash_recovery_harness;
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use integration_harness::run_localfs_lifecycle_retention_runner_harness;
#[cfg(any(test, feature = "fault-injection"))]
pub use integration_harness::run_service_fault_window_harness;
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use integration_harness::LifecycleRetentionRunnerHarnessOutcome;
pub use integration_harness::{
    run_storage_stress_harness, CrashRecoveryHarnessOutcome, ServiceFaultWindowHarnessOutcome,
    StorageStressHarnessOutcome,
};
pub use layout_fuzz::{assert_u64_id_roundtrips, classify_object_text, LayoutClassifyOutcome};
pub use leak::{forget_registered, leak_static};
pub use lifecycle::{
    check_lifecycle_bootstrap_contract, check_lifecycle_branch_lifecycle_contract,
    check_lifecycle_branch_lifecycle_recovery_contract, check_lifecycle_budget_contract,
    check_lifecycle_checkpoint_contract, check_lifecycle_close_contract,
    check_lifecycle_crash_contract, check_lifecycle_fault_contract, check_lifecycle_flush_contract,
    check_lifecycle_generated_script_contract, check_lifecycle_maintenance_contract,
    check_lifecycle_maintenance_fuzz_contract, check_lifecycle_quarantine_contract,
    check_lifecycle_recovery_contract, check_lifecycle_recovery_fuzz_contract,
    check_lifecycle_retention_contract, check_lifecycle_retention_fuzz_contract,
    check_lifecycle_row_pruning_contract, check_lifecycle_scaffold_contract,
    check_lifecycle_table_rewrite_contract, LifecycleBootstrapContractOutcome,
    LifecycleBranchLifecycleContractOutcome, LifecycleBranchLifecycleRecoveryContractOutcome,
    LifecycleBudgetContractOutcome, LifecycleCheckpointContractOutcome,
    LifecycleCloseContractOutcome, LifecycleCrashContractOutcome, LifecycleFaultContractOutcome,
    LifecycleFlushContractOutcome, LifecycleGeneratedScriptOutcome,
    LifecycleMaintenanceContractOutcome, LifecycleQuarantineContractOutcome,
    LifecycleRecoveryContractOutcome, LifecycleRetentionContractOutcome,
    LifecycleRowPruningContractOutcome, LifecycleScaffoldOutcome,
    LifecycleTableRewriteContractOutcome,
};
pub use progress_watchdog::{ProgressTicker, ProgressWatchdog};
pub use quarantine_fuzz::{run_quarantine_service_script, QuarantineServiceFuzzOutcome};
#[cfg(all(feature = "localfs", not(target_arch = "wasm32")))]
pub use recovery_oracle::{run_recovery_oracle_harness, RecoveryOracleOutcome};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use recovery_read_faults::{
    run_recovery_read_fault_harness, run_recovery_read_fault_soak, RecoveryReadFaultOutcome,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use reordering_backend::{FsModel, ReorderingBackend};
pub use service_fuzz::{
    run_snapshot_service_script, ServiceFuzzViolation, SnapshotServiceFuzzOutcome,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use simulation::{
    replay_whole_db_seed, run_fault_simulation_harness, run_lineage_history_harness,
    run_simulation_harness, run_whole_db_harness, LineageHistoryOutcome, SimulationFaultOutcome,
    SimulationOutcome, WholeDbOutcome,
};
#[cfg(all(
    any(test, feature = "fault-injection"),
    feature = "localfs",
    not(target_arch = "wasm32")
))]
pub use write_ordering_watchdog::{
    CheckedPublishFamily, WriteOrderingReport, WriteOrderingViolation, WriteOrderingWatchdog,
};

pub use table_runtime::{
    check_table_runtime_compaction_contract, check_table_runtime_cursor_contract,
    check_table_runtime_reader_contract, check_table_runtime_scaffold_contract,
    TableRuntimeScaffoldOutcome,
};

/// Test-only backend selector used by external conformance tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestBackendKind {
    Memory,
    #[cfg(feature = "localfs")]
    LocalFilesystem,
}

impl TestBackendKind {
    /// Parses the backend name used by storage conformance test runners.
    pub fn parse(name: &str) -> Result<Self, TestkitError> {
        match name {
            "memory" => Ok(Self::Memory),
            #[cfg(feature = "localfs")]
            "localfs" => Ok(Self::LocalFilesystem),
            #[cfg(not(feature = "localfs"))]
            "localfs" => Err(TestkitError::new(
                "test backend \"localfs\" requires the localfs feature",
            )),
            _ => Err(TestkitError::new(format!(
                "unsupported test backend {name:?}"
            ))),
        }
    }

    /// Returns the stable name used in test logs and environment values.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            #[cfg(feature = "localfs")]
            Self::LocalFilesystem => "localfs",
        }
    }
}

/// Error returned by testkit selection and setup code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestkitError {
    message: String,
}

impl TestkitError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TestkitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TestkitError {}

#[cfg(any(test, feature = "fault-injection"))]
mod fault {
    use crate::backend::{
        Backend, BackendCapabilities, BackendError, BackendErrorKind, BackendFence,
        BackendMetadata, BackendRange, BackendResult, PublishError, PublishFailureKind,
        PublishMode, PublishOutcome, PublishResult,
    };
    use crate::object::{ObjectName, ObjectPrefix};
    use std::collections::HashMap;
    use std::fmt;
    use std::num::NonZeroU64;
    use std::sync::{Arc, Mutex};

    /// Backend operation that can be targeted by deterministic test faults.
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub enum BackendOperation {
        ReadObject,
        ReadRange,
        WriteObject,
        DeleteObject,
        ListPrefix,
        ObjectMetadata,
        AppendObject,
        SyncObject,
        ConditionalCreate,
        ConditionalUpdate,
        PublishObject,
    }

    impl BackendOperation {
        pub const fn name(self) -> &'static str {
            match self {
                Self::ReadObject => "read_object",
                Self::ReadRange => "read_range",
                Self::WriteObject => "write_object",
                Self::DeleteObject => "delete_object",
                Self::ListPrefix => "list_prefix",
                Self::ObjectMetadata => "object_metadata",
                Self::AppendObject => "append_object",
                Self::SyncObject => "sync_object",
                Self::ConditionalCreate => "conditional_create",
                Self::ConditionalUpdate => "conditional_update",
                Self::PublishObject => "publish_object",
            }
        }
    }

    impl fmt::Display for BackendOperation {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.name())
        }
    }

    /// Error kind that the testkit can inject into backend operations.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum FaultKind {
        NotFound,
        AlreadyExists,
        PreconditionFailed,
        PermissionDenied,
        InvalidObjectName,
        InvalidRange,
        UnsupportedOperation,
        CapabilityMismatch,
        Unavailable,
        Interrupted,
        MetadataMismatch,
        Corruption,
        NoSpace,
        Unknown,
    }

    impl FaultKind {
        const fn backend_kind(self) -> BackendErrorKind {
            match self {
                Self::NotFound => BackendErrorKind::NotFound,
                Self::AlreadyExists => BackendErrorKind::AlreadyExists,
                Self::PreconditionFailed => BackendErrorKind::PreconditionFailed,
                Self::PermissionDenied => BackendErrorKind::PermissionDenied,
                Self::InvalidObjectName => BackendErrorKind::InvalidObjectName,
                Self::InvalidRange => BackendErrorKind::InvalidRange,
                Self::UnsupportedOperation => BackendErrorKind::UnsupportedOperation,
                Self::CapabilityMismatch => BackendErrorKind::CapabilityMismatch,
                Self::Unavailable => BackendErrorKind::Unavailable,
                Self::Interrupted => BackendErrorKind::Interrupted,
                Self::MetadataMismatch => BackendErrorKind::MetadataMismatch,
                Self::Corruption => BackendErrorKind::Corruption,
                Self::NoSpace => BackendErrorKind::NoSpace,
                Self::Unknown => BackendErrorKind::Unknown,
            }
        }
    }

    /// Whether a fault rule fires only on its target call, or on that call and
    /// every subsequent matching call (the fail-once vs fail-continuously sweep).
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum FaultMode {
        #[default]
        Once,
        Continuously,
    }

    /// One deterministic backend fault.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FaultRule {
        operation: BackendOperation,
        call_number: NonZeroU64,
        kind: FaultKind,
        mode: FaultMode,
    }

    impl FaultRule {
        /// A fail-once rule: fires on exactly the `call_number`-th matching call.
        pub const fn new(
            operation: BackendOperation,
            call_number: NonZeroU64,
            kind: FaultKind,
        ) -> Self {
            Self::with_mode(operation, call_number, kind, FaultMode::Once)
        }

        /// A rule with an explicit fail-once / fail-continuously mode.
        pub const fn with_mode(
            operation: BackendOperation,
            call_number: NonZeroU64,
            kind: FaultKind,
            mode: FaultMode,
        ) -> Self {
            Self {
                operation,
                call_number,
                kind,
                mode,
            }
        }

        pub const fn operation(&self) -> BackendOperation {
            self.operation
        }

        pub const fn call_number(&self) -> NonZeroU64 {
            self.call_number
        }

        pub const fn kind(&self) -> FaultKind {
            self.kind
        }

        pub const fn mode(&self) -> FaultMode {
            self.mode
        }
    }

    /// Deterministic fault script for a backend wrapper.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct FaultScript {
        rules: Vec<FaultRule>,
    }

    impl FaultScript {
        pub fn empty() -> Self {
            Self { rules: Vec::new() }
        }

        pub fn new(rules: impl IntoIterator<Item = FaultRule>) -> Self {
            Self {
                rules: rules.into_iter().collect(),
            }
        }

        fn fault_for(&self, operation: BackendOperation, call_number: u64) -> Option<FaultKind> {
            self.rules
                .iter()
                .find(|rule| {
                    rule.operation == operation
                        && match rule.mode {
                            FaultMode::Once => rule.call_number.get() == call_number,
                            FaultMode::Continuously => call_number >= rule.call_number.get(),
                        }
                })
                .map(FaultRule::kind)
        }
    }

    /// Observed backend operation call.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct BackendCall {
        operation: BackendOperation,
        call_number: u64,
    }

    impl BackendCall {
        pub const fn operation(self) -> BackendOperation {
            self.operation
        }

        pub const fn call_number(self) -> u64 {
            self.call_number
        }
    }

    #[derive(Debug)]
    struct FaultState {
        script: FaultScript,
        calls: Vec<BackendCall>,
        operation_counts: HashMap<BackendOperation, u64>,
        byte_quota: Option<u64>,
        bytes_consumed: u64,
    }

    impl FaultState {
        fn new(script: FaultScript) -> Self {
            Self {
                script,
                calls: Vec::new(),
                operation_counts: HashMap::new(),
                byte_quota: None,
                bytes_consumed: 0,
            }
        }

        fn observe(&mut self, operation: BackendOperation) -> Option<FaultKind> {
            let count = self.operation_counts.entry(operation).or_insert(0);
            *count += 1;
            let call_number = *count;
            self.calls.push(BackendCall {
                operation,
                call_number,
            });
            self.script.fault_for(operation, call_number)
        }

        /// Charge `len` write bytes against the optional disk-space quota,
        /// returning `NoSpace` once the cumulative total exceeds it.
        fn account_bytes(&mut self, len: usize) -> Option<FaultKind> {
            let quota = self.byte_quota?;
            self.bytes_consumed = self
                .bytes_consumed
                .saturating_add(u64::try_from(len).unwrap_or(u64::MAX));
            (self.bytes_consumed > quota).then_some(FaultKind::NoSpace)
        }
    }

    /// Test-only backend wrapper that injects deterministic operation failures.
    ///
    /// BS4.4h: `state` is `Arc`-shared so the backend is `Clone` and can produce an owned
    /// handle (`to_owned_backend_handle`). A clone shares the same fault state, so the
    /// runtime's owned clone and the test's retained handle observe identical faults.
    #[derive(Clone, Debug)]
    pub struct FaultingBackend<B> {
        inner: B,
        state: Arc<Mutex<FaultState>>,
    }

    impl<B> FaultingBackend<B> {
        /// Creates a test-only faulting wrapper around an arbitrary backend handle.
        pub fn new(inner: B, script: FaultScript) -> Self {
            Self {
                inner,
                state: Arc::new(Mutex::new(FaultState::new(script))),
            }
        }

        /// Bound cumulative write bytes: once exceeded, every write op returns
        /// `NoSpace` — a deterministic disk-full (ENOSPC) simulation.
        #[must_use]
        pub fn with_byte_quota(self, quota: u64) -> Self {
            if let Ok(mut state) = self.state.lock() {
                state.byte_quota = Some(quota);
            }
            self
        }

        /// Returns the wrapped backend handle.
        pub fn inner(&self) -> &B {
            &self.inner
        }

        /// Returns the backend operations observed by this wrapper.
        pub fn calls(&self) -> Vec<BackendCall> {
            self.state.lock().map_or_else(
                |poisoned| poisoned.into_inner().calls.clone(),
                |state| state.calls.clone(),
            )
        }

        /// Records one operation and reports whether the script injects a fault.
        ///
        /// External conformance tests can call this before invoking the matching
        /// operation on `inner()`. The storage crate also uses it to implement
        /// the internal backend trait when `B` is a storage backend.
        pub fn before_operation(&self, operation: BackendOperation) -> Result<(), FaultKind> {
            let fault = self
                .state
                .lock()
                .map_or(Some(FaultKind::Unknown), |mut state| {
                    state.observe(operation)
                });

            fault.map_or(Ok(()), Err)
        }

        fn observe(&self, operation: BackendOperation) -> BackendResult<()> {
            self.before_operation(operation).map_err(|kind| {
                BackendError::new(
                    kind.backend_kind(),
                    format!("test fault injected for {operation}"),
                )
            })
        }

        /// Charge `len` write bytes against the disk-space quota (if any).
        fn charge_bytes(&self, len: usize) -> Result<(), FaultKind> {
            let fault = self
                .state
                .lock()
                .map_or(Some(FaultKind::Unknown), |mut state| {
                    state.account_bytes(len)
                });
            fault.map_or(Ok(()), Err)
        }

        /// Observe a write operation and charge its bytes against the quota.
        fn observe_write(&self, operation: BackendOperation, len: usize) -> BackendResult<()> {
            self.observe(operation)?;
            self.charge_bytes(len).map_err(|kind| {
                BackendError::new(
                    kind.backend_kind(),
                    format!("test quota exhausted for {operation}"),
                )
            })
        }
    }

    impl<B: Backend> Backend for FaultingBackend<B> {
        fn capabilities(&self) -> BackendCapabilities {
            self.inner.capabilities()
        }

        fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
            self.observe(BackendOperation::ReadObject)?;
            self.inner.read_object(name)
        }

        fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
            self.observe(BackendOperation::ReadRange)?;
            self.inner.read_range(name, range)
        }

        fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
            self.observe_write(BackendOperation::WriteObject, bytes.len())?;
            self.inner.write_object(name, bytes)
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            self.observe(BackendOperation::DeleteObject)
                .map_err(|error| crate::backend::DeleteError::failed_before_removal(name, error))?;
            self.inner.delete_object(name)
        }

        fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
            self.observe(BackendOperation::ListPrefix)?;
            self.inner.list_prefix(prefix)
        }

        fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
            self.observe(BackendOperation::ObjectMetadata)?;
            self.inner.object_metadata(name)
        }

        // Forwarded but not fault-targeted: the single-writer lock is acquired at
        // open and is not one of the swept I/O operations.
        fn acquire_writer_lock(
            &self,
            name: &ObjectName,
        ) -> BackendResult<crate::backend::BackendWriterGuard> {
            self.inner.acquire_writer_lock(name)
        }

        fn append_object(
            &self,
            name: &ObjectName,
            bytes: &[u8],
        ) -> BackendResult<crate::backend::BackendAppend> {
            self.observe_write(BackendOperation::AppendObject, bytes.len())?;
            self.inner.append_object(name, bytes)
        }

        fn sync_object(&self, name: &ObjectName) -> crate::backend::BackendResult<()> {
            self.observe(BackendOperation::SyncObject)?;
            self.inner.sync_object(name)
        }

        fn conditional_create(
            &self,
            name: &ObjectName,
            bytes: &[u8],
        ) -> BackendResult<BackendMetadata> {
            self.observe_write(BackendOperation::ConditionalCreate, bytes.len())?;
            self.inner.conditional_create(name, bytes)
        }

        fn conditional_update(
            &self,
            name: &ObjectName,
            expected: &BackendFence,
            bytes: &[u8],
        ) -> BackendResult<BackendMetadata> {
            self.observe_write(BackendOperation::ConditionalUpdate, bytes.len())?;
            self.inner.conditional_update(name, expected, bytes)
        }

        fn publish_object(
            &self,
            name: &ObjectName,
            bytes: &[u8],
            mode: PublishMode,
        ) -> PublishResult<PublishOutcome> {
            let publish_fault = |kind: FaultKind, reason: &str| {
                PublishError::new(
                    name.clone(),
                    PublishFailureKind::FailedBeforeVisibility,
                    BackendError::new(
                        kind.backend_kind(),
                        format!("{reason} for {}", BackendOperation::PublishObject),
                    ),
                )
            };
            self.before_operation(BackendOperation::PublishObject)
                .map_err(|kind| publish_fault(kind, "test fault injected"))?;
            self.charge_bytes(bytes.len())
                .map_err(|kind| publish_fault(kind, "test quota exhausted"))?;
            self.inner.publish_object(name, bytes, mode)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{BackendOperation, FaultKind, FaultRule, FaultScript, FaultingBackend};
        use crate::backend::memory::MemoryBackend;
        use crate::backend::{Backend, BackendErrorKind, PublishFailureKind, PublishMode};
        use crate::test_support::{assert_backend_error_kind, object_name};
        use std::num::NonZeroU64;

        #[test]
        fn faulting_backend_delegates_when_script_is_empty() {
            let backend = FaultingBackend::new(MemoryBackend::new(), FaultScript::empty());
            let name = object_name("fault/delegate");

            backend.write_object(&name, b"abc").expect("write");
            backend
                .publish_object(&name, b"publish", PublishMode::NonDurableReplace)
                .expect("publish");

            assert_eq!(backend.read_object(&name).expect("read"), b"publish");
            assert_eq!(
                backend
                    .calls()
                    .iter()
                    .map(|call| (call.operation(), call.call_number()))
                    .collect::<Vec<_>>(),
                vec![
                    (BackendOperation::WriteObject, 1),
                    (BackendOperation::PublishObject, 1),
                    (BackendOperation::ReadObject, 1),
                ]
            );
        }

        #[test]
        fn faulting_backend_injects_configured_operation_failure() {
            let script = FaultScript::new([FaultRule::new(
                BackendOperation::ReadObject,
                NonZeroU64::new(1).expect("non-zero"),
                FaultKind::Unavailable,
            )]);
            let backend = FaultingBackend::new(MemoryBackend::new(), script);
            let name = object_name("fault/read");

            backend.write_object(&name, b"abc").expect("write");
            assert_backend_error_kind(backend.read_object(&name), BackendErrorKind::Unavailable);
            assert_eq!(backend.read_object(&name).expect("second read"), b"abc");
        }

        #[test]
        fn byte_quota_returns_no_space_once_exceeded() {
            let backend =
                FaultingBackend::new(MemoryBackend::new(), FaultScript::empty()).with_byte_quota(8);
            let name = object_name("fault/quota");
            // The quota is the maximum allowed: 5 + 3 = 8 bytes fit; the next exceeds it.
            backend
                .write_object(&name, b"01234")
                .expect("first write within quota");
            backend
                .write_object(&name, b"567")
                .expect("second write reaches the quota");
            assert_backend_error_kind(backend.write_object(&name, b"8"), BackendErrorKind::NoSpace);
        }

        #[test]
        fn faulting_backend_can_fail_conditional_operations_before_delegate() {
            let script = FaultScript::new([FaultRule::new(
                BackendOperation::ConditionalCreate,
                NonZeroU64::new(1).expect("non-zero"),
                FaultKind::PermissionDenied,
            )]);
            let backend = FaultingBackend::new(MemoryBackend::new(), script);
            let name = object_name("fault/conditional");

            assert_backend_error_kind(
                backend.conditional_create(&name, b"bytes"),
                BackendErrorKind::PermissionDenied,
            );
        }

        #[test]
        fn faulting_backend_can_fail_publish_before_delegate() {
            let script = FaultScript::new([FaultRule::new(
                BackendOperation::PublishObject,
                NonZeroU64::new(1).expect("non-zero"),
                FaultKind::Interrupted,
            )]);
            let backend = FaultingBackend::new(MemoryBackend::new(), script);
            let name = object_name("fault/publish");

            let error = backend
                .publish_object(&name, b"bytes", PublishMode::NonDurableReplace)
                .expect_err("publish fault");

            assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
            assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
            assert_eq!(
                backend
                    .read_object(&name)
                    .expect_err("not delegated")
                    .kind(),
                BackendErrorKind::NotFound
            );
        }

        #[test]
        fn faulting_backend_can_gate_non_backend_handles() {
            #[derive(Debug)]
            struct ExternalHandle {
                value: &'static str,
            }

            let script = FaultScript::new([FaultRule::new(
                BackendOperation::WriteObject,
                NonZeroU64::new(1).expect("non-zero"),
                FaultKind::Interrupted,
            )]);
            let backend = FaultingBackend::new(ExternalHandle { value: "ready" }, script);

            assert_eq!(
                backend.before_operation(BackendOperation::WriteObject),
                Err(FaultKind::Interrupted)
            );
            assert_eq!(backend.inner().value, "ready");
            assert_eq!(
                backend.before_operation(BackendOperation::WriteObject),
                Ok(())
            );
            assert_eq!(
                backend
                    .calls()
                    .iter()
                    .map(|call| (call.operation(), call.call_number()))
                    .collect::<Vec<_>>(),
                vec![
                    (BackendOperation::WriteObject, 1),
                    (BackendOperation::WriteObject, 2),
                ]
            );
        }
    }
}

#[cfg(any(test, feature = "fault-injection"))]
pub use fault::{
    BackendCall, BackendOperation, FaultKind, FaultMode, FaultRule, FaultScript, FaultingBackend,
};

#[cfg(test)]
mod tests {
    use super::{TestBackendKind, TestkitError};

    #[test]
    fn test_backend_kind_parses_memory() {
        let backend = TestBackendKind::parse("memory").expect("memory backend");

        assert_eq!(backend.name(), "memory");
    }

    #[cfg(feature = "localfs")]
    #[test]
    fn test_backend_kind_parses_localfs_when_feature_is_enabled() {
        let backend = TestBackendKind::parse("localfs").expect("localfs backend");

        assert_eq!(backend.name(), "localfs");
    }

    #[cfg(not(feature = "localfs"))]
    #[test]
    fn test_backend_kind_rejects_localfs_without_feature() {
        assert_eq!(
            TestBackendKind::parse("localfs"),
            Err(TestkitError::new(
                "test backend \"localfs\" requires the localfs feature"
            ))
        );
    }

    #[test]
    fn test_backend_kind_rejects_unknown_backend() {
        assert_eq!(
            TestBackendKind::parse("remote"),
            Err(TestkitError::new("unsupported test backend \"remote\""))
        );
    }
}
