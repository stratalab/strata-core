//! API diagnostics request and snapshot shells.

use strata_core::{BranchId, CommitVersion, Timestamp};

use super::{
    MaintenanceQueueSummary, MaintenanceWalGrowthSummary, RecoveryHealthSummary, StorageMode,
    StorageRuntimeState,
};

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsScope {
    Global,
    Branch(BranchId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsRequest {
    scope: DiagnosticsScope,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsFactState {
    Known,
    Unknown,
    Unsupported,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsRecoveryClass {
    Corruption,
    Io,
    Policy,
    Telemetry,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsRecoveryFaultKind {
    CorruptManifest,
    CorruptSnapshot,
    CorruptWal,
    MissingManifestObject,
    MissingSnapshotObject,
    MissingTableObject,
    InheritedLayerLoss,
    NoManifestFallback,
    IoFailure,
    QuarantineInventoryMismatch,
    TimelineMismatch,
    WalTailRepairFailed,
    /// Committed WAL data attested by the durable watermark is unrecoverable.
    WalCommittedSuffixMissing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticsRecoveryFault {
    kind: DiagnosticsRecoveryFaultKind,
    reason: &'static str,
    affected_branch: Option<BranchId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticsRecoveryReport {
    state: DiagnosticsFactState,
    health: Option<RecoveryHealthSummary>,
    class: Option<DiagnosticsRecoveryClass>,
    faults: Vec<DiagnosticsRecoveryFault>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsBudgetPool {
    BlockCache,
    TableReader,
    ActiveMutable,
    FrozenMutable,
    MaintenanceQueue,
    GeneratedArtifact,
    ManifestCatalog,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsBudgetPressure {
    Normal,
    Evicting,
    DeferOptionalMaintenance,
    RejectOptionalWork,
    RejectMutatingAdmission,
}

/// Whether a reported budget usage value is a live, tracked figure or an admission-only
/// estimate. This realizes the diagnostics contract's "exact or approximate" distinction with
/// names that match how each pool is accounted.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsBudgetAccuracy {
    /// A live figure that reflects current usage. Memtable byte sizes are size-estimates, but
    /// the value is tracked continuously and is non-zero while objects are resident.
    Tracked,
    /// An admission-only figure: the pool is checked per allocation and the charge is not
    /// retained, so the reported value does not reflect live usage (it reads ~0 while objects
    /// are live).
    AdmissionOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsBudgetUsage {
    pool: DiagnosticsBudgetPool,
    used_bytes: u64,
    limit_bytes: u64,
    used_count: u64,
    limit_count: Option<u64>,
    pressure: DiagnosticsBudgetPressure,
    accuracy: DiagnosticsBudgetAccuracy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticsBudgetReport {
    state: DiagnosticsFactState,
    total_limit_bytes: Option<u64>,
    total_used_bytes: Option<u64>,
    total_used_accuracy: Option<DiagnosticsBudgetAccuracy>,
    global_pressure: DiagnosticsBudgetPressure,
    usages: Vec<DiagnosticsBudgetUsage>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsStoragePressureSeverity {
    None,
    Background,
    Urgent,
    BlockMutatingAdmission,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticsStoragePressureReason {
    None,
    ActiveMutableBytes,
    FrozenBacklog,
    LevelZeroTableBacklog,
    NonZeroLevelTableBacklog,
    InheritedLayerBacklog,
    MaintenanceQueueBacklog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsStoragePressureReport {
    state: DiagnosticsFactState,
    branch_id: Option<BranchId>,
    severity: DiagnosticsStoragePressureSeverity,
    reason: DiagnosticsStoragePressureReason,
    active_rows: usize,
    active_bytes: u64,
    frozen_tables: usize,
    frozen_bytes: u64,
    level_zero_tables: usize,
    owned_tables: usize,
    inherited_layers: usize,
    pending_maintenance: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsSourceLevelTableCount {
    level: u8,
    table_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticsSourceLayoutReport {
    state: DiagnosticsFactState,
    active_rows: usize,
    frozen_table_count: usize,
    frozen_rows: usize,
    owned_l0_tables: usize,
    owned_nonzero_level_table_counts: Vec<DiagnosticsSourceLevelTableCount>,
    owned_total_tables: usize,
    inherited_layers: usize,
    inherited_l0_tables: usize,
    inherited_nonzero_level_table_counts: Vec<DiagnosticsSourceLevelTableCount>,
    inherited_total_tables: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsReadActivityReport {
    state: DiagnosticsFactState,
    block_hits: Option<u64>,
    block_misses: Option<u64>,
    opened_readers: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsTableReachabilityReport {
    state: DiagnosticsFactState,
    table_count: usize,
    object_count: usize,
    next_manifest_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsRetentionReport {
    state: DiagnosticsFactState,
    protected_objects: Option<usize>,
    pending_releases: Option<usize>,
    reclaimed_objects: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsQuarantineReport {
    state: DiagnosticsFactState,
    quarantined_objects: Option<usize>,
    quarantined_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsCheckpointReport {
    state: DiagnosticsFactState,
    snapshot_id: Option<u64>,
    checkpoint_watermark: Option<CommitVersion>,
    flush_watermark: Option<CommitVersion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsWalGrowthReport {
    state: DiagnosticsFactState,
    policy_enabled: bool,
    max_retained_wal_bytes: Option<u64>,
    max_retained_wal_segments: Option<usize>,
    retained_wal_bytes: Option<u64>,
    retained_wal_segments: Option<usize>,
    max_commits_since_checkpoint: Option<u64>,
    last_status: Option<MaintenanceWalGrowthSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsBranchCatalogReport {
    state: DiagnosticsFactState,
    active_branches: usize,
    deleted_branches: usize,
    min_generation: Option<super::BranchGeneration>,
    max_generation: Option<super::BranchGeneration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsTimelineReport {
    state: DiagnosticsFactState,
    min_version: Option<CommitVersion>,
    max_version: Option<CommitVersion>,
    min_timestamp: Option<Timestamp>,
    max_timestamp: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticsOutcome {
    scope: DiagnosticsScope,
    runtime_state: StorageRuntimeState,
    mode: Option<StorageMode>,
    visible_version: Option<CommitVersion>,
    recovery: DiagnosticsRecoveryReport,
    maintenance_state: DiagnosticsFactState,
    maintenance: Option<MaintenanceQueueSummary>,
    budget: DiagnosticsBudgetReport,
    pressure: DiagnosticsStoragePressureReport,
    source_layout: DiagnosticsSourceLayoutReport,
    read_activity: DiagnosticsReadActivityReport,
    table_manifest: DiagnosticsTableReachabilityReport,
    retention: DiagnosticsRetentionReport,
    quarantine: DiagnosticsQuarantineReport,
    checkpoint: DiagnosticsCheckpointReport,
    wal_growth: DiagnosticsWalGrowthReport,
    branch_catalog: DiagnosticsBranchCatalogReport,
    timeline: DiagnosticsTimelineReport,
}

impl DiagnosticsRequest {
    #[must_use]
    pub const fn new(scope: DiagnosticsScope) -> Self {
        Self { scope }
    }

    #[must_use]
    pub const fn scope(self) -> DiagnosticsScope {
        self.scope
    }
}

impl DiagnosticsRecoveryFault {
    #[must_use]
    pub const fn new(
        kind: DiagnosticsRecoveryFaultKind,
        reason: &'static str,
        affected_branch: Option<BranchId>,
    ) -> Self {
        Self {
            kind,
            reason,
            affected_branch,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> DiagnosticsRecoveryFaultKind {
        self.kind
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }

    #[must_use]
    pub const fn affected_branch(&self) -> Option<BranchId> {
        self.affected_branch
    }
}

impl DiagnosticsRecoveryReport {
    #[must_use]
    pub(crate) fn new(
        health: RecoveryHealthSummary,
        class: Option<DiagnosticsRecoveryClass>,
        faults: Vec<DiagnosticsRecoveryFault>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            health: Some(health),
            class,
            faults,
        }
    }

    #[must_use]
    pub fn healthy() -> Self {
        Self::new(RecoveryHealthSummary::Healthy, None, Vec::new())
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            health: None,
            class: None,
            faults: Vec::new(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn health(&self) -> Option<RecoveryHealthSummary> {
        self.health
    }

    #[must_use]
    pub const fn class(&self) -> Option<DiagnosticsRecoveryClass> {
        self.class
    }

    #[must_use]
    pub fn faults(&self) -> &[DiagnosticsRecoveryFault] {
        &self.faults
    }
}

impl DiagnosticsBudgetUsage {
    #[must_use]
    pub(crate) const fn new(
        pool: DiagnosticsBudgetPool,
        used_bytes: u64,
        limit_bytes: u64,
        used_count: u64,
        limit_count: Option<u64>,
        pressure: DiagnosticsBudgetPressure,
        accuracy: DiagnosticsBudgetAccuracy,
    ) -> Self {
        Self {
            pool,
            used_bytes,
            limit_bytes,
            used_count,
            limit_count,
            pressure,
            accuracy,
        }
    }

    #[must_use]
    pub const fn pool(self) -> DiagnosticsBudgetPool {
        self.pool
    }

    #[must_use]
    pub const fn used_bytes(self) -> u64 {
        self.used_bytes
    }

    #[must_use]
    pub const fn limit_bytes(self) -> u64 {
        self.limit_bytes
    }

    #[must_use]
    pub const fn used_count(self) -> u64 {
        self.used_count
    }

    #[must_use]
    pub const fn limit_count(self) -> Option<u64> {
        self.limit_count
    }

    #[must_use]
    pub const fn pressure(self) -> DiagnosticsBudgetPressure {
        self.pressure
    }

    /// Whether `used_bytes` is a tracked live figure or an admission-only estimate.
    #[must_use]
    pub const fn accuracy(self) -> DiagnosticsBudgetAccuracy {
        self.accuracy
    }
}

impl DiagnosticsBudgetReport {
    #[must_use]
    pub(crate) fn known(
        total_limit_bytes: u64,
        total_used_bytes: u64,
        global_pressure: DiagnosticsBudgetPressure,
        usages: Vec<DiagnosticsBudgetUsage>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            total_limit_bytes: Some(total_limit_bytes),
            total_used_bytes: Some(total_used_bytes),
            total_used_accuracy: Some(DiagnosticsBudgetAccuracy::Tracked),
            global_pressure,
            usages,
        }
    }

    #[must_use]
    pub fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            total_limit_bytes: None,
            total_used_bytes: None,
            total_used_accuracy: None,
            global_pressure: DiagnosticsBudgetPressure::Normal,
            usages: Vec::new(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn total_limit_bytes(&self) -> Option<u64> {
        self.total_limit_bytes
    }

    /// Live database-wide Strata-owned bytes: memtables, resident owned-table readers, the block
    /// cache, and in-flight operation reservations summed across branches. `None` when unknown.
    #[must_use]
    pub const fn total_used_bytes(&self) -> Option<u64> {
        self.total_used_bytes
    }

    /// Whether `total_used_bytes` is a tracked live figure. The total sums the tracked-live
    /// resident pools and excludes the admission-only transient pools by design, so it is
    /// `Tracked` when known and `None` when unknown.
    #[must_use]
    pub const fn total_used_accuracy(&self) -> Option<DiagnosticsBudgetAccuracy> {
        self.total_used_accuracy
    }

    /// Database-wide memory pressure derived from the live total against the configured budget.
    #[must_use]
    pub const fn global_pressure(&self) -> DiagnosticsBudgetPressure {
        self.global_pressure
    }

    #[must_use]
    pub fn usages(&self) -> &[DiagnosticsBudgetUsage] {
        &self.usages
    }
}

impl DiagnosticsStoragePressureReport {
    #[expect(
        clippy::too_many_arguments,
        reason = "pressure reports are flat storage counters"
    )]
    #[must_use]
    pub(crate) const fn known(
        branch_id: BranchId,
        severity: DiagnosticsStoragePressureSeverity,
        reason: DiagnosticsStoragePressureReason,
        active_rows: usize,
        active_bytes: u64,
        frozen_tables: usize,
        frozen_bytes: u64,
        level_zero_tables: usize,
        owned_tables: usize,
        inherited_layers: usize,
        pending_maintenance: usize,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            branch_id: Some(branch_id),
            severity,
            reason,
            active_rows,
            active_bytes,
            frozen_tables,
            frozen_bytes,
            level_zero_tables,
            owned_tables,
            inherited_layers,
            pending_maintenance,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            branch_id: None,
            severity: DiagnosticsStoragePressureSeverity::None,
            reason: DiagnosticsStoragePressureReason::None,
            active_rows: 0,
            active_bytes: 0,
            frozen_tables: 0,
            frozen_bytes: 0,
            level_zero_tables: 0,
            owned_tables: 0,
            inherited_layers: 0,
            pending_maintenance: 0,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn branch_id(self) -> Option<BranchId> {
        self.branch_id
    }

    #[must_use]
    pub const fn severity(self) -> DiagnosticsStoragePressureSeverity {
        self.severity
    }

    #[must_use]
    pub const fn reason(self) -> DiagnosticsStoragePressureReason {
        self.reason
    }

    #[must_use]
    pub const fn active_rows(self) -> usize {
        self.active_rows
    }

    #[must_use]
    pub const fn active_bytes(self) -> u64 {
        self.active_bytes
    }

    #[must_use]
    pub const fn frozen_tables(self) -> usize {
        self.frozen_tables
    }

    #[must_use]
    pub const fn frozen_bytes(self) -> u64 {
        self.frozen_bytes
    }

    #[must_use]
    pub const fn level_zero_tables(self) -> usize {
        self.level_zero_tables
    }

    #[must_use]
    pub const fn owned_tables(self) -> usize {
        self.owned_tables
    }

    #[must_use]
    pub const fn inherited_layers(self) -> usize {
        self.inherited_layers
    }

    #[must_use]
    pub const fn pending_maintenance(self) -> usize {
        self.pending_maintenance
    }
}

impl DiagnosticsSourceLevelTableCount {
    #[must_use]
    pub(crate) const fn new(level: u8, table_count: usize) -> Self {
        Self { level, table_count }
    }

    #[must_use]
    pub const fn level(self) -> u8 {
        self.level
    }

    #[must_use]
    pub const fn table_count(self) -> usize {
        self.table_count
    }
}

impl DiagnosticsSourceLayoutReport {
    #[expect(
        clippy::too_many_arguments,
        reason = "source layout reports are flat serving-source counters"
    )]
    #[must_use]
    pub(crate) fn known(
        active_rows: usize,
        frozen_table_count: usize,
        frozen_rows: usize,
        owned_l0_tables: usize,
        owned_nonzero_level_table_counts: Vec<DiagnosticsSourceLevelTableCount>,
        owned_total_tables: usize,
        inherited_layers: usize,
        inherited_l0_tables: usize,
        inherited_nonzero_level_table_counts: Vec<DiagnosticsSourceLevelTableCount>,
        inherited_total_tables: usize,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            active_rows,
            frozen_table_count,
            frozen_rows,
            owned_l0_tables,
            owned_nonzero_level_table_counts,
            owned_total_tables,
            inherited_layers,
            inherited_l0_tables,
            inherited_nonzero_level_table_counts,
            inherited_total_tables,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            active_rows: 0,
            frozen_table_count: 0,
            frozen_rows: 0,
            owned_l0_tables: 0,
            owned_nonzero_level_table_counts: Vec::new(),
            owned_total_tables: 0,
            inherited_layers: 0,
            inherited_l0_tables: 0,
            inherited_nonzero_level_table_counts: Vec::new(),
            inherited_total_tables: 0,
        }
    }

    #[must_use]
    pub const fn state(&self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn active_rows(&self) -> usize {
        self.active_rows
    }

    #[must_use]
    pub const fn frozen_table_count(&self) -> usize {
        self.frozen_table_count
    }

    #[must_use]
    pub const fn frozen_rows(&self) -> usize {
        self.frozen_rows
    }

    #[must_use]
    pub const fn owned_l0_tables(&self) -> usize {
        self.owned_l0_tables
    }

    #[must_use]
    pub fn owned_nonzero_level_table_counts(&self) -> &[DiagnosticsSourceLevelTableCount] {
        &self.owned_nonzero_level_table_counts
    }

    #[must_use]
    pub const fn owned_total_tables(&self) -> usize {
        self.owned_total_tables
    }

    #[must_use]
    pub const fn inherited_layers(&self) -> usize {
        self.inherited_layers
    }

    #[must_use]
    pub const fn inherited_l0_tables(&self) -> usize {
        self.inherited_l0_tables
    }

    #[must_use]
    pub fn inherited_nonzero_level_table_counts(&self) -> &[DiagnosticsSourceLevelTableCount] {
        &self.inherited_nonzero_level_table_counts
    }

    #[must_use]
    pub const fn inherited_total_tables(&self) -> usize {
        self.inherited_total_tables
    }
}

impl DiagnosticsReadActivityReport {
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            block_hits: None,
            block_misses: None,
            opened_readers: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn block_hits(self) -> Option<u64> {
        self.block_hits
    }

    #[must_use]
    pub const fn block_misses(self) -> Option<u64> {
        self.block_misses
    }

    #[must_use]
    pub const fn opened_readers(self) -> Option<u64> {
        self.opened_readers
    }
}

impl DiagnosticsTableReachabilityReport {
    #[must_use]
    pub const fn known(
        table_count: usize,
        object_count: usize,
        next_manifest_sequence: Option<u64>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            table_count,
            object_count,
            next_manifest_sequence,
        }
    }

    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            state: DiagnosticsFactState::Unsupported,
            table_count: 0,
            object_count: 0,
            next_manifest_sequence: None,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            table_count: 0,
            object_count: 0,
            next_manifest_sequence: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn table_count(self) -> usize {
        self.table_count
    }

    #[must_use]
    pub const fn object_count(self) -> usize {
        self.object_count
    }

    #[must_use]
    pub const fn next_manifest_sequence(self) -> Option<u64> {
        self.next_manifest_sequence
    }
}

impl DiagnosticsRetentionReport {
    #[must_use]
    pub const fn known(
        protected_objects: Option<usize>,
        pending_releases: Option<usize>,
        reclaimed_objects: Option<usize>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            protected_objects,
            pending_releases,
            reclaimed_objects,
        }
    }

    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            state: DiagnosticsFactState::Unsupported,
            protected_objects: None,
            pending_releases: None,
            reclaimed_objects: None,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            protected_objects: None,
            pending_releases: None,
            reclaimed_objects: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn protected_objects(self) -> Option<usize> {
        self.protected_objects
    }

    #[must_use]
    pub const fn pending_releases(self) -> Option<usize> {
        self.pending_releases
    }

    #[must_use]
    pub const fn reclaimed_objects(self) -> Option<usize> {
        self.reclaimed_objects
    }
}

impl DiagnosticsQuarantineReport {
    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            state: DiagnosticsFactState::Unsupported,
            quarantined_objects: None,
            quarantined_bytes: None,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            quarantined_objects: None,
            quarantined_bytes: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn quarantined_objects(self) -> Option<usize> {
        self.quarantined_objects
    }

    #[must_use]
    pub const fn quarantined_bytes(self) -> Option<u64> {
        self.quarantined_bytes
    }
}

impl DiagnosticsCheckpointReport {
    #[must_use]
    pub const fn known(
        snapshot_id: Option<u64>,
        checkpoint_watermark: Option<CommitVersion>,
        flush_watermark: Option<CommitVersion>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            snapshot_id,
            checkpoint_watermark,
            flush_watermark,
        }
    }

    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            state: DiagnosticsFactState::Unsupported,
            snapshot_id: None,
            checkpoint_watermark: None,
            flush_watermark: None,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            snapshot_id: None,
            checkpoint_watermark: None,
            flush_watermark: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn snapshot_id(self) -> Option<u64> {
        self.snapshot_id
    }

    #[must_use]
    pub const fn checkpoint_watermark(self) -> Option<CommitVersion> {
        self.checkpoint_watermark
    }

    #[must_use]
    pub const fn flush_watermark(self) -> Option<CommitVersion> {
        self.flush_watermark
    }
}

impl DiagnosticsWalGrowthReport {
    #[must_use]
    pub const fn known(
        policy_enabled: bool,
        max_retained_wal_bytes: Option<u64>,
        max_retained_wal_segments: Option<usize>,
        max_commits_since_checkpoint: Option<u64>,
        last_status: Option<MaintenanceWalGrowthSummary>,
    ) -> Self {
        Self::known_with_current_retention(
            policy_enabled,
            max_retained_wal_bytes,
            max_retained_wal_segments,
            None,
            None,
            max_commits_since_checkpoint,
            last_status,
        )
    }

    #[must_use]
    pub const fn known_with_current_retention(
        policy_enabled: bool,
        max_retained_wal_bytes: Option<u64>,
        max_retained_wal_segments: Option<usize>,
        retained_wal_bytes: Option<u64>,
        retained_wal_segments: Option<usize>,
        max_commits_since_checkpoint: Option<u64>,
        last_status: Option<MaintenanceWalGrowthSummary>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            policy_enabled,
            max_retained_wal_bytes,
            max_retained_wal_segments,
            retained_wal_bytes,
            retained_wal_segments,
            max_commits_since_checkpoint,
            last_status,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            policy_enabled: false,
            max_retained_wal_bytes: None,
            max_retained_wal_segments: None,
            retained_wal_bytes: None,
            retained_wal_segments: None,
            max_commits_since_checkpoint: None,
            last_status: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn policy_enabled(self) -> bool {
        self.policy_enabled
    }

    #[must_use]
    pub const fn max_retained_wal_bytes(self) -> Option<u64> {
        self.max_retained_wal_bytes
    }

    #[must_use]
    pub const fn max_retained_wal_segments(self) -> Option<usize> {
        self.max_retained_wal_segments
    }

    #[must_use]
    pub const fn retained_wal_bytes(self) -> Option<u64> {
        self.retained_wal_bytes
    }

    #[must_use]
    pub const fn retained_wal_segments(self) -> Option<usize> {
        self.retained_wal_segments
    }

    #[must_use]
    pub const fn max_commits_since_checkpoint(self) -> Option<u64> {
        self.max_commits_since_checkpoint
    }

    #[must_use]
    pub const fn last_status(self) -> Option<MaintenanceWalGrowthSummary> {
        self.last_status
    }
}

impl DiagnosticsBranchCatalogReport {
    #[must_use]
    pub const fn known(
        active_branches: usize,
        deleted_branches: usize,
        min_generation: Option<super::BranchGeneration>,
        max_generation: Option<super::BranchGeneration>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            active_branches,
            deleted_branches,
            min_generation,
            max_generation,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            active_branches: 0,
            deleted_branches: 0,
            min_generation: None,
            max_generation: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn active_branches(self) -> usize {
        self.active_branches
    }

    #[must_use]
    pub const fn deleted_branches(self) -> usize {
        self.deleted_branches
    }

    #[must_use]
    pub const fn min_generation(self) -> Option<super::BranchGeneration> {
        self.min_generation
    }

    #[must_use]
    pub const fn max_generation(self) -> Option<super::BranchGeneration> {
        self.max_generation
    }
}

impl DiagnosticsTimelineReport {
    #[must_use]
    pub const fn known(
        min_version: Option<CommitVersion>,
        max_version: Option<CommitVersion>,
        min_timestamp: Option<Timestamp>,
        max_timestamp: Option<Timestamp>,
    ) -> Self {
        Self {
            state: DiagnosticsFactState::Known,
            min_version,
            max_version,
            min_timestamp,
            max_timestamp,
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: DiagnosticsFactState::Unknown,
            min_version: None,
            max_version: None,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    #[must_use]
    pub const fn state(self) -> DiagnosticsFactState {
        self.state
    }

    #[must_use]
    pub const fn min_version(self) -> Option<CommitVersion> {
        self.min_version
    }

    #[must_use]
    pub const fn max_version(self) -> Option<CommitVersion> {
        self.max_version
    }

    #[must_use]
    pub const fn min_timestamp(self) -> Option<Timestamp> {
        self.min_timestamp
    }

    #[must_use]
    pub const fn max_timestamp(self) -> Option<Timestamp> {
        self.max_timestamp
    }
}

impl DiagnosticsOutcome {
    #[expect(
        clippy::too_many_arguments,
        reason = "diagnostics outcome is a flat API snapshot"
    )]
    // `allow`, not `expect`: the type is over the by-value threshold on a
    // 64-bit host and under it on wasm32, so an `expect` is unfulfilled on
    // one of the two targets whichever way it is written.
    #[allow(
        clippy::large_types_passed_by_value,
        reason = "constructor stores the maintenance summary; callers move it in"
    )]
    #[must_use]
    pub(crate) fn new(
        scope: DiagnosticsScope,
        runtime_state: StorageRuntimeState,
        mode: Option<StorageMode>,
        visible_version: Option<CommitVersion>,
        recovery: DiagnosticsRecoveryReport,
        maintenance: Option<MaintenanceQueueSummary>,
        budget: DiagnosticsBudgetReport,
        pressure: DiagnosticsStoragePressureReport,
        source_layout: DiagnosticsSourceLayoutReport,
        read_activity: DiagnosticsReadActivityReport,
        table_manifest: DiagnosticsTableReachabilityReport,
        retention: DiagnosticsRetentionReport,
        quarantine: DiagnosticsQuarantineReport,
        checkpoint: DiagnosticsCheckpointReport,
        wal_growth: DiagnosticsWalGrowthReport,
        branch_catalog: DiagnosticsBranchCatalogReport,
        timeline: DiagnosticsTimelineReport,
    ) -> Self {
        Self {
            scope,
            runtime_state,
            mode,
            visible_version,
            recovery,
            maintenance_state: if maintenance.is_some() {
                DiagnosticsFactState::Known
            } else {
                DiagnosticsFactState::Unknown
            },
            maintenance,
            budget,
            pressure,
            source_layout,
            read_activity,
            table_manifest,
            retention,
            quarantine,
            checkpoint,
            wal_growth,
            branch_catalog,
            timeline,
        }
    }

    #[must_use]
    pub const fn scope(&self) -> DiagnosticsScope {
        self.scope
    }

    #[must_use]
    pub const fn runtime_state(&self) -> StorageRuntimeState {
        self.runtime_state
    }

    #[must_use]
    pub const fn mode(&self) -> Option<StorageMode> {
        self.mode
    }

    #[must_use]
    pub const fn visible_version(&self) -> Option<CommitVersion> {
        self.visible_version
    }

    #[must_use]
    pub const fn recovery(&self) -> &DiagnosticsRecoveryReport {
        &self.recovery
    }

    #[must_use]
    pub const fn maintenance_state(&self) -> DiagnosticsFactState {
        self.maintenance_state
    }

    #[must_use]
    pub const fn maintenance(&self) -> Option<MaintenanceQueueSummary> {
        self.maintenance
    }

    #[must_use]
    pub const fn budget(&self) -> &DiagnosticsBudgetReport {
        &self.budget
    }

    #[must_use]
    pub const fn pressure(&self) -> DiagnosticsStoragePressureReport {
        self.pressure
    }

    #[must_use]
    pub const fn source_layout(&self) -> &DiagnosticsSourceLayoutReport {
        &self.source_layout
    }

    #[must_use]
    pub const fn read_activity(&self) -> DiagnosticsReadActivityReport {
        self.read_activity
    }

    #[must_use]
    pub const fn table_manifest(&self) -> DiagnosticsTableReachabilityReport {
        self.table_manifest
    }

    #[must_use]
    pub const fn retention(&self) -> DiagnosticsRetentionReport {
        self.retention
    }

    #[must_use]
    pub const fn quarantine(&self) -> DiagnosticsQuarantineReport {
        self.quarantine
    }

    #[must_use]
    pub const fn checkpoint(&self) -> DiagnosticsCheckpointReport {
        self.checkpoint
    }

    #[must_use]
    pub const fn wal_growth(&self) -> DiagnosticsWalGrowthReport {
        self.wal_growth
    }

    #[must_use]
    pub const fn branch_catalog(&self) -> DiagnosticsBranchCatalogReport {
        self.branch_catalog
    }

    #[must_use]
    pub const fn timeline(&self) -> DiagnosticsTimelineReport {
        self.timeline
    }
}
