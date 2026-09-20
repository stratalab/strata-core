//! Branch table rewrite scheduling.

use super::{
    telemetry_health_debt, LifecycleCompactionIoPolicy, LifecycleError, LifecycleLowerLayer,
    LifecycleResult, LifecycleStats, MaintenanceExecutorStatus, MaintenanceOutcome,
    MaintenanceOutcomeStatus, MaintenanceTask, MaintenanceTaskKind, MaintenanceTaskRequest,
    MaintenanceTaskScope, RecoveryHealth, StorageBudgetPool, StorageBudgetPressureSeverity,
    StorageRuntimeBudget,
};
use crate::branch::error::BranchRuntimeError;
use crate::branch::facts::{
    BranchLevel, BranchReachabilitySnapshot, BranchSourceLayout, BranchTableRef,
};
use crate::branch::pruning::BranchCompactionPruningProof;
use crate::branch::read::{BranchInheritedLayer, BranchOwnedTable};
use crate::branch::state::compaction::{
    BranchCompactionCandidate, BranchCompactionKind, BranchCompactionOperation,
    BranchCompactionOutcome, BranchCompactionPlan, BranchCompactionRequest,
    BranchCompactionRetentionPolicy,
};
use crate::branch::state::materialization::{
    BranchMaterializationHandle, BranchMaterializationOutcome, BranchMaterializationPreparedOutput,
    BranchMaterializationRecovery, BranchMaterializationRequest,
};
use crate::branch::state::BranchLocalState;
use crate::object::ObjectName;
use crate::table::{
    TableBuilderConfig, TableCompactionConfig, TableCompactionReport, TableIdentity,
    TableInternalKeyBytes, TablePhysicalKeyBytes,
};
use strata_core::BranchId;

/// L0 write-admission grades, aligned to `RocksDB`'s slowdown/stop triggers
/// (`durable-write-pipeline-scaling.md:184`, `db/column_family.cc:992`): compaction is scheduled at
/// 4, writes enter the slowdown (delay) band at 20, and mutating admission hard-blocks at 36 — matching
/// `RocksDB`'s L0 defaults. Strata's earlier 8/16 blocked ~2x sooner, collapsing the update-heavy crawl
/// into a stall with little compaction runway (G10, BS3.4a). The block ceiling is 36 x 64 MiB ~=
/// 2.3 GiB of L0 *on disk* worst-case; write-path memory stays bounded by the active/frozen byte pools
/// independently of this count grade (see the profile-tier matrix test). BS3.4b replaces the quadratic
/// delay band between 20 and 36 with a debt-adaptive rate ramp.
const LEVEL_ZERO_COMPACTION_THRESHOLD: usize = 4;
pub(crate) const LEVEL_ZERO_URGENT_COMPACTION_THRESHOLD: usize = 20;
pub(crate) const LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD: usize = 36;

/// L0 slowdown (delay-band onset) grade, for the BS3.4b graded-admission rate ramp.
pub(super) const fn level_zero_urgent_threshold() -> usize {
    LEVEL_ZERO_URGENT_COMPACTION_THRESHOLD
}

/// L0 hard-stop grade, for the BS3.4b near-stop braking + delay-band ceiling.
pub(super) const fn level_zero_blocking_threshold() -> usize {
    LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD
}
/// W1.1: byte bound for one L0→L1 pass (input + L1 overlap). Bounds the
/// unit of L0-blocking relief: pre-W1.1 a single pass took ALL of L0 plus all
/// overlap — GBs at 10M scale, ~50s per pass, which made the L0 admission
/// wall's relief a ~50s lottery (billion-scale-ledger.md § 10M three-way,
/// roadmap-v2 T1). 256MiB ≈ seconds-scale passes; L0 count drops
/// incrementally pass by pass.
const L0_PASS_MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;

/// W1.3a: cut every table-rewrite pass's output tables so each overlaps at most
/// this many bytes of the level BELOW the output level ("grandparent" tables —
/// `RocksDB`'s `max_compaction_bytes` / `CompactionOutputs::ShouldStopBefore`).
/// This bounds the input size of every FUTURE pass that compacts an output
/// table down a level: without it, full-keyspan L(n) tables force multi-GB
/// L(n)→L(n+1) rewrites, and the dispatch conflict rule (same branch,
/// level ±1) makes the L0 admission wall wait out whichever monster is in
/// flight — the stack-sampled 40-70s stall (ledger § stall root cause).
/// 256MiB matches `L0_PASS_MAX_INPUT_BYTES`: a future one-table pass reads
/// ~table + bound ≈ seconds-scale.
const OUTPUT_GRANDPARENT_OVERLAP_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// W2.2: bloom filter density for lifecycle-built tables (`RocksDB` default).
/// BS4.3 built and gated the machinery (`filter_bits_per_key: None` = the
/// pre-W2.2 unfiltered table); this flips it on for every lifecycle-driven
/// build. The read-path profile measured 2.6 of 3.63 table seeks per point
/// read probing tables that do not contain the key — exactly the seeks a
/// filter's negative probe skips. ~10 bits/key ≈ 82KB per 64MB table, charged
/// to the reader budget via `resident_metadata_bytes`.
const TABLE_FILTER_BITS_PER_KEY: usize = 10;

/// The builder config for every lifecycle-driven table build (flush and
/// compaction): defaults plus the persisted bloom filter. Tables built
/// elsewhere (materialization, snapshot install — W2.2 follow-up) stay
/// unfiltered; the reader treats a missing filter as `Unavailable` and probes
/// normally, so mixed tables are fine.
pub(super) fn lifecycle_table_builder_config(
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
) -> LifecycleResult<TableBuilderConfig> {
    // B2: the per-database data-block byte target (configured at open,
    // carried by the lifecycle config) overrides the built-in default; the
    // open-time validation bounds it, so a failure here is an internal
    // invariant break. #3499: the compression codec is carried the same way
    // (Zstd by default), applied uniformly to flush and compaction outputs.
    let bytes =
        data_block_bytes.unwrap_or_else(|| TableBuilderConfig::default().target_data_block_size());
    let base = TableBuilderConfig::new(
        bytes,
        TableBuilderConfig::default().rows_per_block(),
        compression,
    )
    .map_err(|_| LifecycleError::InvalidConfig {
        field: "data_block_bytes",
        reason: "table builder rejected the configured block byte target",
    })?;
    Ok(base.with_filter_bits_per_key(Some(TABLE_FILTER_BITS_PER_KEY)))
}

const NONZERO_LEVEL_COMPACTION_THRESHOLD: usize = 4;
const NONZERO_LEVEL_URGENT_COMPACTION_THRESHOLD: usize = 8;
const NONZERO_LEVEL_MIN_BASE_TARGET_BYTES: u64 = 1024 * 1024;
const NONZERO_LEVEL_MAX_BASE_TARGET_BYTES: u64 = 256 * 1024 * 1024;
const NONZERO_LEVEL_TARGET_GROWTH_FACTOR: u64 = 10;
const NONZERO_LEVEL_URGENT_TARGET_MULTIPLIER: u64 = 2;
// Urgency-score normalizer for inherited-layer backlog (`count_pressure_score(layer_count, ..)`): a layer
// counts as `layer_count * 1000` pressure, so materialization ranks linearly by backlog depth against
// compaction. This is the *rank* input and is deliberately independent of the onset gate below.
const INHERITED_LAYER_MATERIALIZATION_THRESHOLD: usize = 1;
// Onset gate: proactive inherited-layer flattening starts at 2 layers, not 1. A lone Active inherited layer
// is a healthy copy-on-write fork (every historical COW fork and every `fork_current`), and flattening it
// would re-materialize the parent's `<= fork_version` snapshot into O(dataset) child-owned tables — the
// exact copy COW eliminates. A lone layer therefore stays lazy indefinitely; its pinned snapshot is retained
// correctly (the child needs it, and reachability-gated reclaim never deletes a still-referenced object).
// Deeper fork-of-fork chains still flatten: Background (2-3), Urgent (>=4), Blocking write-admission (>=16).
const INHERITED_LAYER_PROACTIVE_MATERIALIZATION_MIN: usize = 2;
const INHERITED_LAYER_URGENT_MATERIALIZATION_THRESHOLD: usize = 4;
const INHERITED_LAYER_BLOCKING_MATERIALIZATION_THRESHOLD: usize = 16;
pub(crate) const FROZEN_BLOCKING_FLUSH_THRESHOLD: usize = 4;
const PENDING_MAINTENANCE_BLOCKING_THRESHOLD: usize = 16;
const DEFAULT_COMPACTION_DRAIN_PASS_LIMIT: usize = 16;
const BOTTOMMOST_COMPACTION_INPUT_LIMIT: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleTableRewriteDurability {
    VolatileOnly,
    CheckpointRequiredAfterRewrite,
    DurableTableManifestBacked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleCompactionRequest {
    branch_id: BranchId,
    kind: BranchCompactionKind,
    output_identity_seed: String,
    durability: LifecycleTableRewriteDurability,
    retention_policy: BranchCompactionRetentionPolicy,
    pruning_proof: Option<BranchCompactionPruningProof>,
    /// W1.1: per-pass input byte bound applied to L0→L1 requests (see
    /// `L0_PASS_MAX_INPUT_BYTES`). A field rather than the bare constant so
    /// tests and the W1.4 pacing calibration can vary it per request; every
    /// constructor seeds the default.
    l0_pass_max_input_bytes: u64,
    data_block_bytes: Option<u32>,
    /// #3499: compression codec for the rewritten tables, stamped from
    /// `LifecycleConfig::table_compression` at dispatch (Zstd by default).
    table_compression: crate::format::TableCompression,
    /// W1.3a: per-output grandparent-overlap bound applied to every
    /// table-rewrite request (see `OUTPUT_GRANDPARENT_OVERLAP_MAX_BYTES`);
    /// same field-over-constant rationale as the L0 pass bound.
    output_grandparent_overlap_max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleCompactionOutcome {
    status: LifecycleCompactionStatus,
    branch_id: BranchId,
    plan: BranchCompactionPlan,
    branch_outcome: BranchCompactionOutcome,
    io_facts: LifecycleCompactionIoFacts,
    elapsed: std::time::Duration,
    checkpoint_required: bool,
    recovery_health: Option<RecoveryHealth>,
    durable_output_objects: Vec<ObjectName>,
    retained_input_objects: Vec<String>,
    failure: Option<LifecycleError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleCompactionDrainRequest {
    branch_id: BranchId,
    output_identity_prefix: String,
    max_passes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LifecycleCompactionIoFacts {
    input_bytes: u64,
    estimated_output_bytes: u64,
    metadata_bytes_avoided: u64,
    input_rows: u64,
}

pub(crate) const COMPACTION_IO_BUDGET_DEFERRED_REASON: &str =
    "compaction IO byte budget deferred table rewrite";
pub(crate) const FLUSH_PRESSURE_PREEMPTED_COMPACTION_REASON: &str =
    "flush pressure preempted compaction";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleCompactionDrainOutcome {
    branch_id: BranchId,
    operations_attempted: usize,
    operations_installed: usize,
    table_rewrites: usize,
    metadata_promotions: usize,
    levels_touched: Vec<BranchLevel>,
    input_tables_removed: usize,
    output_tables_installed: usize,
    final_source_layout: BranchSourceLayout,
    affected_object_names: Vec<String>,
    checkpoint_required: bool,
    deferred_reason: Option<&'static str>,
    retryable: bool,
    recovery_health: Option<RecoveryHealth>,
    failure: Option<LifecycleError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleCompactionStatus {
    Completed,
    CompletedCheckpointRequired,
    CompletedDurable,
    CompletedManifestDebt,
    DeferredNoCandidate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleMaterializationRequest {
    child_branch_id: BranchId,
    layer_index: usize,
    handle: Option<BranchMaterializationHandle>,
    output_identity_prefix: String,
    durability: LifecycleTableRewriteDurability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleMaterializationOutcome {
    status: LifecycleMaterializationStatus,
    child_branch_id: BranchId,
    layer_index: usize,
    materialization_handle: Option<BranchMaterializationHandle>,
    reachability_snapshot: Option<BranchReachabilitySnapshot>,
    branch_outcome: Option<BranchMaterializationOutcome>,
    checkpoint_required: bool,
    recovery_health: Option<RecoveryHealth>,
    durable_output_objects: Vec<ObjectName>,
    failure: Option<LifecycleError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleMaterializationStatus {
    Completed,
    CompletedCheckpointRequired,
    CompletedDurable,
    CompletedManifestDebt,
    AlreadyMaterialized,
    DeferredNoLayer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleStoragePressure {
    branch_id: BranchId,
    severity: LifecycleStoragePressureSeverity,
    reason: LifecycleStoragePressureReason,
    suggested_task: Option<MaintenanceTaskRequest>,
    active_rows: usize,
    active_bytes: u64,
    frozen_tables: usize,
    frozen_bytes: u64,
    level_zero_tables: usize,
    owned_tables: usize,
    table_rewrite_bytes: u64,
    inherited_layers: usize,
    pending_maintenance: usize,
    /// Continuous write-throttle fullness in permille (0..=1000): the max of the
    /// active-pool, frozen-pool, and L0 fullness ratios. Drives the proportional
    /// pre-budget write throttle (fix #2). 0 for cache mode and below all soft thresholds.
    throttle_ratio_permille: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleCompactionScore {
    level: u8,
    table_index: Option<usize>,
    score: u64,
    severity: LifecycleStoragePressureSeverity,
    reason: LifecycleStoragePressureReason,
    table_count: usize,
    byte_count: u64,
    target_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleTableRewriteScore {
    work: LifecycleTableRewriteWork,
    score: u64,
    severity: LifecycleStoragePressureSeverity,
    reason: LifecycleStoragePressureReason,
    item_count: usize,
    byte_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleTableRewriteWork {
    Compaction { level: u8 },
    Materialization { layer_index: usize },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LifecycleTableRewriteScoreKey {
    severity_rank: u8,
    score: u64,
    item_count: usize,
    byte_count: u64,
    work_tiebreaker: u8,
    low_level_tiebreaker: std::cmp::Reverse<u8>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LifecycleCompactionScoreKey {
    severity_rank: u8,
    score: u64,
    table_count: usize,
    byte_count: u64,
    low_level_tiebreaker: std::cmp::Reverse<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleStoragePressureSeverity {
    None,
    Background,
    Urgent,
    BlockMutatingAdmission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleStoragePressureReason {
    None,
    ActiveMutableBytes,
    FrozenBacklog,
    LevelZeroTableBacklog,
    NonZeroLevelTableBacklog,
    InheritedLayerBacklog,
    MaintenanceQueueBacklog,
}

impl LifecycleCompactionRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        kind: BranchCompactionKind,
        output_identity_seed: impl Into<String>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            branch_id,
            kind,
            output_identity_seed: output_identity_seed.into(),
            durability: LifecycleTableRewriteDurability::VolatileOnly,
            retention_policy: BranchCompactionRetentionPolicy::KeepAll,
            pruning_proof: None,
            l0_pass_max_input_bytes: L0_PASS_MAX_INPUT_BYTES,
            output_grandparent_overlap_max_bytes: OUTPUT_GRANDPARENT_OVERLAP_MAX_BYTES,
            data_block_bytes: None,
            // Uncompressed default keeps request-construction byte-stable; the
            // production codec (Zstd) is stamped from the lifecycle config at
            // dispatch via `with_table_compression` (#3499).
            table_compression: crate::format::TableCompression::Uncompressed,
        };
        request.branch_request()?;
        Ok(request)
    }

    /// B2: stamp the per-database data-block byte target onto the request
    /// (None = built-in default). Set at runtime dispatch from
    /// `LifecycleConfig::data_block_bytes`.
    pub(crate) const fn with_data_block_bytes(mut self, data_block_bytes: Option<u32>) -> Self {
        self.data_block_bytes = data_block_bytes;
        self
    }

    /// #3499: stamp the compression codec onto the request, from
    /// `LifecycleConfig::table_compression` at dispatch (Zstd by default).
    pub(crate) const fn with_table_compression(
        mut self,
        compression: crate::format::TableCompression,
    ) -> Self {
        self.table_compression = compression;
        self
    }

    /// W1.1b: override the per-pass L0→L1 input byte bound (tests and pacing
    /// calibration; production requests keep `L0_PASS_MAX_INPUT_BYTES`).
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "pass-bound override is consumed by W1 tests and tuning"
        )
    )]
    pub(crate) const fn with_l0_pass_max_input_bytes(mut self, bound: u64) -> Self {
        self.l0_pass_max_input_bytes = bound;
        self
    }

    /// W1.3a: override the per-output grandparent-overlap bound (tests and
    /// pacing calibration; production requests keep
    /// `OUTPUT_GRANDPARENT_OVERLAP_MAX_BYTES`).
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "overlap-bound override is consumed by W1 tests and tuning"
        )
    )]
    pub(crate) const fn with_output_grandparent_overlap_max_bytes(mut self, bound: u64) -> Self {
        self.output_grandparent_overlap_max_bytes = bound;
        self
    }

    pub(crate) const fn with_durability(
        mut self,
        durability: LifecycleTableRewriteDurability,
    ) -> Self {
        self.durability = durability;
        self
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn kind(&self) -> BranchCompactionKind {
        self.kind
    }

    pub(crate) fn output_identity_seed(&self) -> &str {
        &self.output_identity_seed
    }

    pub(crate) const fn durability(&self) -> LifecycleTableRewriteDurability {
        self.durability
    }

    #[allow(
        dead_code,
        reason = "row-pruning lifecycle request accessors are consumed by generated coverage"
    )]
    pub(crate) const fn retention_policy(&self) -> BranchCompactionRetentionPolicy {
        self.retention_policy
    }

    #[allow(
        dead_code,
        reason = "row-pruning lifecycle request accessors are consumed by generated coverage"
    )]
    pub(crate) const fn pruning_proof(&self) -> Option<BranchCompactionPruningProof> {
        self.pruning_proof
    }

    #[allow(
        dead_code,
        reason = "proof-backed lifecycle compaction is exercised by row-pruning callers"
    )]
    pub(crate) fn with_retention_policy(
        mut self,
        retention_policy: BranchCompactionRetentionPolicy,
    ) -> Self {
        self.retention_policy = retention_policy;
        self
    }

    #[allow(
        dead_code,
        reason = "proof-backed lifecycle compaction is exercised by row-pruning callers"
    )]
    pub(crate) fn with_pruning_proof(mut self, proof: BranchCompactionPruningProof) -> Self {
        self.pruning_proof = Some(proof);
        self
    }

    pub(super) fn branch_request(&self) -> LifecycleResult<BranchCompactionRequest> {
        let mut request = BranchCompactionRequest::new(
            self.branch_id,
            self.kind,
            self.output_identity_seed.clone(),
        )
        .map_err(branch_error)?
        .with_table_compaction_config(table_compaction_config_for_kind(self.kind)?)
        .with_retention_policy(self.retention_policy);
        if let Some(proof) = self.pruning_proof {
            request = request.with_pruning_proof(proof);
        }
        // W1.1: every lifecycle-driven L0→L1 pass is byte-bounded (all entry
        // points route through here — background, inline, explicit drains).
        if matches!(self.kind, BranchCompactionKind::CompactL0ToLevelOne) {
            request = request.with_max_pass_input_bytes(self.l0_pass_max_input_bytes);
        }
        // W2.2: every lifecycle-built table persists a bloom filter.
        request = request
            .with_table_builder_config(lifecycle_table_builder_config(
                self.data_block_bytes,
                self.table_compression,
            )?);
        // W1.3a: every pass cuts its outputs by grandparent overlap. Kinds
        // whose output level is bottommost get no hints downstream (the
        // grandparent level is empty), so applying the bound universally is
        // exact, not approximate.
        request = request
            .with_output_grandparent_overlap_max_bytes(self.output_grandparent_overlap_max_bytes);
        Ok(request)
    }
}

fn table_compaction_config_for_kind(
    kind: BranchCompactionKind,
) -> LifecycleResult<TableCompactionConfig> {
    validate_compaction_output_level_for_kind(kind)?;
    let default = TableCompactionConfig::default();
    TableCompactionConfig::new(default.target_output_bytes(), default.max_output_tables()).map_err(
        |source| {
            LifecycleError::lower_layer_with(
                LifecycleLowerLayer::TableRuntime,
                "table runtime failed",
                source,
            )
        },
    )
}

fn validate_compaction_output_level_for_kind(kind: BranchCompactionKind) -> LifecycleResult<()> {
    match kind {
        BranchCompactionKind::CompactLevel { level, .. } => {
            level
                .raw()
                .checked_add(1)
                .ok_or(LifecycleError::MaintenanceTaskFailed {
                    reason: "compaction output level overflow",
                })?;
        }
        BranchCompactionKind::CompactL0
        | BranchCompactionKind::CompactL0ToLevelOne
        | BranchCompactionKind::CompactBottommostLevel { .. } => {}
    }
    Ok(())
}

pub(crate) fn table_compaction_config_with_storage_budget(
    config: TableCompactionConfig,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<TableCompactionConfig> {
    let Some(budget) = budget else {
        return Ok(config);
    };
    let limit = budget.pool_limit_bytes(StorageBudgetPool::GeneratedArtifact);
    if limit == 0 {
        return Ok(config);
    }
    let capped_target = config.target_output_bytes().min((limit / 2).max(1));
    if capped_target == config.target_output_bytes() {
        return Ok(config);
    }
    TableCompactionConfig::new(capped_target, config.max_output_tables()).map_err(|source| {
        LifecycleError::lower_layer_with(
            LifecycleLowerLayer::TableRuntime,
            "table runtime failed",
            source,
        )
    })
}

impl LifecycleCompactionDrainRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        output_identity_prefix: impl Into<String>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            branch_id,
            output_identity_prefix: output_identity_prefix.into(),
            max_passes: DEFAULT_COMPACTION_DRAIN_PASS_LIMIT,
        };
        TableIdentity::new(request.output_identity_prefix.clone()).map_err(|source| {
            LifecycleError::lower_layer_with(
                LifecycleLowerLayer::TableRuntime,
                "table runtime failed",
                source,
            )
        })?;
        request.compaction_request_for(0, 0, 0)?;
        Ok(request)
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn output_identity_prefix(&self) -> &str {
        &self.output_identity_prefix
    }

    pub(crate) const fn max_passes(&self) -> usize {
        self.max_passes
    }

    #[allow(
        dead_code,
        reason = "pass-limit behavior is exercised by targeted drain tests"
    )]
    pub(crate) const fn with_max_passes(mut self, max_passes: usize) -> Self {
        self.max_passes = max_passes;
        self
    }

    fn compaction_request_for(
        &self,
        pass_index: usize,
        level_index: usize,
        operation_index: usize,
    ) -> LifecycleResult<LifecycleCompactionRequest> {
        let kind = if level_index == 0 {
            BranchCompactionKind::CompactL0ToLevelOne
        } else {
            BranchCompactionKind::CompactLevel {
                level: BranchLevel::new(u8::try_from(level_index).map_err(|_| {
                    LifecycleError::MaintenanceTaskFailed {
                        reason: "compaction drain level must fit in BranchLevel",
                    }
                })?),
                table_index: 0,
            }
        };
        LifecycleCompactionRequest::new(
            self.branch_id,
            kind,
            format!(
                "{}-pass-{pass_index}-level-{level_index}-op-{operation_index}",
                self.output_identity_prefix
            ),
        )
    }
}

pub(crate) fn nonzero_level_target_bytes(level: BranchLevel) -> u64 {
    debug_assert!(
        level.raw() >= 1,
        "nonzero level target bytes requires a nonzero level"
    );
    (1..level.raw()).fold(NONZERO_LEVEL_MIN_BASE_TARGET_BYTES, |target, _| {
        target.saturating_mul(NONZERO_LEVEL_TARGET_GROWTH_FACTOR)
    })
}

pub(super) fn nonzero_level_targets_from_level_bytes(level_bytes: &[u64]) -> Vec<u64> {
    let mut max_bytes = vec![0u64; level_bytes.len().max(1)];
    if max_bytes.len() == 1 {
        return max_bytes;
    }
    let max_base = NONZERO_LEVEL_MAX_BASE_TARGET_BYTES.max(NONZERO_LEVEL_MIN_BASE_TARGET_BYTES);
    let mut bottom_level = 0usize;
    let mut bottom_bytes = 0u64;
    for (level, &bytes) in level_bytes.iter().enumerate().skip(1) {
        if bytes > bottom_bytes {
            bottom_level = level;
            bottom_bytes = bytes;
        }
    }

    if bottom_level == 0 {
        let mut target = NONZERO_LEVEL_MIN_BASE_TARGET_BYTES;
        for slot in max_bytes.iter_mut().skip(1) {
            *slot = target;
            target = target.saturating_mul(NONZERO_LEVEL_TARGET_GROWTH_FACTOR);
        }
        return max_bytes;
    }

    let mut unclamped_base = bottom_bytes;
    for _ in 1..bottom_level {
        unclamped_base /= NONZERO_LEVEL_TARGET_GROWTH_FACTOR;
    }

    let (base_level, base) = if unclamped_base >= NONZERO_LEVEL_MIN_BASE_TARGET_BYTES {
        (
            1,
            unclamped_base.clamp(NONZERO_LEVEL_MIN_BASE_TARGET_BYTES, max_base),
        )
    } else {
        let mut base_level = 1;
        let mut base = unclamped_base;
        while base < NONZERO_LEVEL_MIN_BASE_TARGET_BYTES && base_level < bottom_level {
            base = base.saturating_mul(NONZERO_LEVEL_TARGET_GROWTH_FACTOR);
            base_level += 1;
        }
        (
            base_level,
            base.clamp(NONZERO_LEVEL_MIN_BASE_TARGET_BYTES, max_base),
        )
    };

    for slot in max_bytes.iter_mut().take(base_level).skip(1) {
        *slot = max_base;
    }
    let mut target = base;
    for slot in max_bytes.iter_mut().skip(base_level) {
        *slot = target;
        target = target.saturating_mul(NONZERO_LEVEL_TARGET_GROWTH_FACTOR);
    }
    max_bytes
}

fn nonzero_level_targets_for_branch(branch: &BranchLocalState) -> Vec<u64> {
    // The cached per-level bytes are the same fold `level_byte_count` computes for each level
    // (`facts().byte_count()` sum), maintained at structural-mutation cadence, so this is O(1) in
    // the number of tables and byte-identical to the previous per-level fold.
    nonzero_level_targets_from_level_bytes(branch.per_level_bytes())
}

fn nonzero_level_target_bytes_for_branch(
    branch: &BranchLocalState,
    level: BranchLevel,
) -> Option<u64> {
    nonzero_level_targets_for_branch(branch)
        .get(usize::from(level.raw()))
        .copied()
}

fn nonzero_level_urgent_bytes(target_bytes: u64) -> u64 {
    target_bytes.saturating_mul(NONZERO_LEVEL_URGENT_TARGET_MULTIPLIER)
}

impl LifecycleCompactionDrainOutcome {
    fn new(branch_id: BranchId, final_source_layout: BranchSourceLayout) -> Self {
        Self {
            branch_id,
            operations_attempted: 0,
            operations_installed: 0,
            table_rewrites: 0,
            metadata_promotions: 0,
            levels_touched: Vec::new(),
            input_tables_removed: 0,
            output_tables_installed: 0,
            final_source_layout,
            affected_object_names: Vec::new(),
            checkpoint_required: false,
            deferred_reason: None,
            retryable: false,
            recovery_health: None,
            failure: None,
        }
    }

    /// A no-op drain outcome produced when the branch's manifest publish slot is held by an
    /// in-flight off-lock publish. Nothing was attempted; the caller should retry once the slot
    /// frees. `maintenance_outcome` reports this as `Deferred` (zero operations installed).
    pub(crate) fn deferred_for_publish_busy(
        branch_id: BranchId,
        final_source_layout: BranchSourceLayout,
    ) -> Self {
        let mut outcome = Self::new(branch_id, final_source_layout);
        outcome.deferred_reason = Some("table manifest publish slot is busy for this branch");
        outcome
    }

    fn record_attempt(&mut self) {
        self.operations_attempted = self.operations_attempted.saturating_add(1);
    }

    fn record_install(&mut self, outcome: &LifecycleCompactionOutcome) {
        self.operations_installed = self.operations_installed.saturating_add(1);
        if let Some(candidate) = outcome.branch_outcome().candidate() {
            self.record_candidate(candidate);
        }
        self.input_tables_removed = self.input_tables_removed.saturating_add(
            outcome
                .branch_outcome()
                .candidate()
                .map_or(0, |candidate| candidate.input_refs().len()),
        );
        self.output_tables_installed = self
            .output_tables_installed
            .saturating_add(outcome.branch_outcome().output_refs().len());
        let maintenance = outcome.maintenance_outcome();
        for object_name in maintenance.affected_object_names() {
            if !self.affected_object_names.contains(object_name) {
                self.affected_object_names.push(object_name.clone());
            }
        }
        self.checkpoint_required |= maintenance.checkpoint_required();
        self.retryable |= maintenance.retryable();
        if self.recovery_health.is_none() {
            self.recovery_health = maintenance.recovery_health().cloned();
        }
        if self.failure.is_none() {
            self.failure = maintenance.source_error().cloned();
        }
    }

    fn record_deferred(&mut self, outcome: &MaintenanceOutcome) {
        self.deferred_reason = outcome.reason();
        self.retryable |= outcome.retryable();
        if self.recovery_health.is_none() {
            self.recovery_health = outcome.recovery_health().cloned();
        }
        if self.failure.is_none() {
            self.failure = outcome.source_error().cloned();
        }
    }

    fn record_candidate(&mut self, candidate: &BranchCompactionCandidate) {
        match candidate.operation() {
            BranchCompactionOperation::TableRewrite => {
                self.table_rewrites = self.table_rewrites.saturating_add(1);
            }
            BranchCompactionOperation::MetadataPromotion => {
                self.metadata_promotions = self.metadata_promotions.saturating_add(1);
            }
        }
        for table_ref in candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs().iter())
        {
            self.record_touched_level(table_ref.level());
        }
        self.record_touched_level(candidate.output_level());
    }

    fn record_touched_level(&mut self, level: BranchLevel) {
        if !self.levels_touched.contains(&level) {
            self.levels_touched.push(level);
        }
    }

    fn with_final_source_layout(mut self, final_source_layout: BranchSourceLayout) -> Self {
        self.final_source_layout = final_source_layout;
        self
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn operations_attempted(&self) -> usize {
        self.operations_attempted
    }

    pub(crate) const fn operations_installed(&self) -> usize {
        self.operations_installed
    }

    pub(crate) const fn table_rewrites(&self) -> usize {
        self.table_rewrites
    }

    pub(crate) const fn metadata_promotions(&self) -> usize {
        self.metadata_promotions
    }

    pub(crate) fn levels_touched(&self) -> &[BranchLevel] {
        &self.levels_touched
    }

    pub(crate) const fn input_tables_removed(&self) -> usize {
        self.input_tables_removed
    }

    pub(crate) const fn output_tables_installed(&self) -> usize {
        self.output_tables_installed
    }

    pub(crate) const fn final_source_layout(&self) -> &BranchSourceLayout {
        &self.final_source_layout
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = if self.deferred_reason.is_some() || self.operations_installed == 0 {
            MaintenanceOutcomeStatus::Deferred
        } else {
            MaintenanceOutcomeStatus::Completed
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Compaction, status)
            .with_affected_object_names(self.affected_object_names.clone())
            .with_effects(self.affected_object_names.len(), 0, self.retryable)
            .with_state_changes(self.operations_installed)
            .with_checkpoint_required(self.checkpoint_required)
            .with_stats(LifecycleStats::new(0, 0, self.operations_attempted, 0, 0));
        if let Some(reason) = self.deferred_reason {
            outcome = outcome.with_reason(reason);
        } else if self.operations_installed == 0 {
            outcome = outcome.with_reason("compaction drain has no candidate tables");
        }
        if let Some(health) = &self.recovery_health {
            outcome = outcome.with_recovery_health(health.clone());
        }
        if let Some(error) = &self.failure {
            outcome = outcome.with_source_error(error.clone());
        }
        outcome
    }
}

impl LifecycleCompactionOutcome {
    pub(super) fn new(
        request: &LifecycleCompactionRequest,
        plan: BranchCompactionPlan,
        branch_outcome: BranchCompactionOutcome,
        io_facts: LifecycleCompactionIoFacts,
        elapsed: std::time::Duration,
    ) -> Self {
        let no_candidate = branch_outcome.noop_reason().is_some();
        let checkpoint_required = !no_candidate
            && request.durability()
                == LifecycleTableRewriteDurability::CheckpointRequiredAfterRewrite;
        let status = if no_candidate {
            LifecycleCompactionStatus::DeferredNoCandidate
        } else if checkpoint_required {
            LifecycleCompactionStatus::CompletedCheckpointRequired
        } else {
            LifecycleCompactionStatus::Completed
        };
        Self {
            status,
            branch_id: branch_outcome.branch_id(),
            plan,
            branch_outcome,
            io_facts,
            elapsed,
            checkpoint_required,
            recovery_health: None,
            durable_output_objects: Vec::new(),
            retained_input_objects: Vec::new(),
            failure: None,
        }
    }

    pub(super) fn completed_durable(
        plan: BranchCompactionPlan,
        branch_outcome: BranchCompactionOutcome,
        io_facts: LifecycleCompactionIoFacts,
        elapsed: std::time::Duration,
        durable_output_objects: Vec<ObjectName>,
        retained_input_objects: Vec<String>,
    ) -> Self {
        Self {
            status: LifecycleCompactionStatus::CompletedDurable,
            branch_id: branch_outcome.branch_id(),
            plan,
            branch_outcome,
            io_facts,
            elapsed,
            checkpoint_required: false,
            recovery_health: None,
            durable_output_objects,
            retained_input_objects,
            failure: None,
        }
    }

    pub(super) fn manifest_debt(mut self, failure: LifecycleError) -> Self {
        self.status = LifecycleCompactionStatus::CompletedManifestDebt;
        self.checkpoint_required = true;
        self.recovery_health = telemetry_health();
        self.failure = Some(failure);
        self
    }

    pub(crate) const fn status(&self) -> LifecycleCompactionStatus {
        self.status
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn plan(&self) -> &BranchCompactionPlan {
        &self.plan
    }

    pub(crate) const fn branch_outcome(&self) -> &BranchCompactionOutcome {
        &self.branch_outcome
    }

    pub(crate) const fn io_facts(&self) -> LifecycleCompactionIoFacts {
        self.io_facts
    }

    pub(crate) const fn elapsed(&self) -> std::time::Duration {
        self.elapsed
    }

    pub(crate) const fn checkpoint_required(&self) -> bool {
        self.checkpoint_required
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) fn durable_output_objects(&self) -> &[ObjectName] {
        &self.durable_output_objects
    }

    pub(crate) const fn failure(&self) -> Option<&LifecycleError> {
        self.failure.as_ref()
    }

    /// Superseded input objects whose manifest refs this compaction dropped. They are retained
    /// on disk at publish (children's inherited layers may still reference them); the
    /// table-object GC mark decides their reclaim through reachability.
    pub(crate) fn retained_input_objects(&self) -> &[String] {
        &self.retained_input_objects
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = match self.status {
            LifecycleCompactionStatus::Completed
            | LifecycleCompactionStatus::CompletedCheckpointRequired
            | LifecycleCompactionStatus::CompletedDurable
            | LifecycleCompactionStatus::CompletedManifestDebt => {
                MaintenanceOutcomeStatus::Completed
            }
            LifecycleCompactionStatus::DeferredNoCandidate => MaintenanceOutcomeStatus::Deferred,
        };
        // Durable rewrite outcomes carry ObjectNames for outputs and identity
        // strings for retained inputs. Volatile/legacy-durable outcomes only
        // have the branch refs, so fall back to table identities then. Either
        // path lists each logical object once.
        let affected_object_names: Vec<String> =
            if self.durable_output_objects.is_empty() && self.retained_input_objects.is_empty() {
                self.branch_outcome
                    .output_refs()
                    .iter()
                    .chain(self.branch_outcome.removed_refs())
                    .map(|table_ref| table_ref.table_identity().as_str().to_owned())
                    .collect()
            } else {
                self.durable_output_objects
                    .iter()
                    .map(|object| object.as_str().to_owned())
                    .chain(self.retained_input_objects.iter().cloned())
                    .collect()
            };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Compaction, status)
            .with_affected_object_names(affected_object_names)
            .with_state_changes(usize::from(!matches!(
                self.status,
                LifecycleCompactionStatus::DeferredNoCandidate
            )))
            .with_checkpoint_required(self.checkpoint_required)
            .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
        if matches!(self.status, LifecycleCompactionStatus::DeferredNoCandidate) {
            outcome = outcome.with_reason("compaction has no candidate tables");
        }
        if matches!(
            self.status,
            LifecycleCompactionStatus::CompletedManifestDebt
        ) {
            outcome = outcome.with_reason("table rewrite manifest publication needs recovery");
        }
        if let Some(health) = &self.recovery_health {
            outcome = outcome.with_recovery_health(health.clone());
        }
        if let Some(error) = &self.failure {
            outcome = outcome.with_source_error(error.clone());
        }
        outcome
    }
}

impl LifecycleCompactionIoFacts {
    pub(super) fn from_plan(branch: &BranchLocalState, plan: &BranchCompactionPlan) -> Self {
        let Some(candidate) = plan.candidate() else {
            return Self::default();
        };
        let source_bytes = candidate
            .input_refs()
            .iter()
            .chain(candidate.overlap_refs())
            .fold(0u64, |bytes, table_ref| {
                bytes.saturating_add(table_ref_byte_count(branch, table_ref))
            });
        let input_bytes = if candidate.requires_table_rewrite() {
            source_bytes
        } else {
            0
        };
        let estimated_output_bytes = if candidate.requires_table_rewrite() {
            source_bytes
        } else {
            0
        };
        let metadata_bytes_avoided = if candidate.is_metadata_promotion() {
            source_bytes
        } else {
            0
        };
        let input_rows = if candidate.requires_table_rewrite() {
            candidate.input_row_count()
        } else {
            0
        };
        Self {
            input_bytes,
            estimated_output_bytes,
            metadata_bytes_avoided,
            input_rows,
        }
    }

    pub(crate) const fn input_bytes(self) -> u64 {
        self.input_bytes
    }

    pub(crate) const fn estimated_budget_bytes(self) -> u64 {
        self.input_bytes.saturating_add(self.estimated_output_bytes)
    }

    pub(crate) const fn metadata_bytes_avoided(self) -> u64 {
        self.metadata_bytes_avoided
    }

    pub(crate) const fn input_rows(self) -> u64 {
        self.input_rows
    }
}

fn table_ref_byte_count(branch: &BranchLocalState, table_ref: &BranchTableRef) -> u64 {
    if table_ref.table_branch_id() != branch.branch_id() {
        return 0;
    }
    branch
        .owned_levels()
        .get(usize::from(table_ref.level().raw()))
        .and_then(|tables| tables.get(table_ref.table_index()))
        .filter(|table| table.descriptor().identity() == table_ref.table_identity())
        .map_or(0, |table| table.facts().byte_count())
}

impl LifecycleMaterializationRequest {
    pub(crate) fn new(
        child_branch_id: BranchId,
        layer_index: usize,
        output_identity_prefix: impl Into<String>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            child_branch_id,
            layer_index,
            handle: None,
            output_identity_prefix: output_identity_prefix.into(),
            durability: LifecycleTableRewriteDurability::VolatileOnly,
        };
        request.branch_request()?;
        Ok(request)
    }

    pub(crate) fn from_handle(
        handle: BranchMaterializationHandle,
        output_identity_prefix: impl Into<String>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            child_branch_id: handle.child_branch_id(),
            layer_index: handle.layer_index(),
            handle: Some(handle),
            output_identity_prefix: output_identity_prefix.into(),
            durability: LifecycleTableRewriteDurability::VolatileOnly,
        };
        request.branch_request()?;
        Ok(request)
    }

    pub(crate) const fn with_durability(
        mut self,
        durability: LifecycleTableRewriteDurability,
    ) -> Self {
        self.durability = durability;
        self
    }

    pub(crate) const fn child_branch_id(&self) -> BranchId {
        self.child_branch_id
    }

    pub(crate) const fn layer_index(&self) -> usize {
        self.layer_index
    }

    pub(crate) const fn handle(&self) -> Option<BranchMaterializationHandle> {
        self.handle
    }

    pub(crate) fn output_identity_prefix(&self) -> &str {
        &self.output_identity_prefix
    }

    pub(crate) const fn durability(&self) -> LifecycleTableRewriteDurability {
        self.durability
    }

    pub(super) fn branch_request(&self) -> LifecycleResult<BranchMaterializationRequest> {
        match self.handle {
            Some(handle) => BranchMaterializationRequest::from_handle(
                handle,
                self.output_identity_prefix.clone(),
            ),
            None => BranchMaterializationRequest::new(
                self.child_branch_id,
                self.layer_index,
                self.output_identity_prefix.clone(),
            ),
        }
        .map_err(branch_error)
    }
}

impl LifecycleMaterializationOutcome {
    pub(super) fn deferred(request: &LifecycleMaterializationRequest) -> Self {
        Self {
            status: LifecycleMaterializationStatus::DeferredNoLayer,
            child_branch_id: request.child_branch_id(),
            layer_index: request.layer_index(),
            materialization_handle: None,
            reachability_snapshot: None,
            branch_outcome: None,
            checkpoint_required: false,
            recovery_health: None,
            durable_output_objects: Vec::new(),
            failure: None,
        }
    }

    pub(super) fn completed(
        request: &LifecycleMaterializationRequest,
        materialization_handle: BranchMaterializationHandle,
        reachability_snapshot: BranchReachabilitySnapshot,
        branch_outcome: BranchMaterializationOutcome,
    ) -> Self {
        let checkpoint_required = request.durability()
            == LifecycleTableRewriteDurability::CheckpointRequiredAfterRewrite
            && !matches!(
                branch_outcome.recovery(),
                BranchMaterializationRecovery::LayerAlreadyMaterialized
            );
        let status = match branch_outcome.recovery() {
            BranchMaterializationRecovery::LayerAlreadyMaterialized => {
                LifecycleMaterializationStatus::AlreadyMaterialized
            }
            BranchMaterializationRecovery::ReplacementVisibleLayerRemoved
            | BranchMaterializationRecovery::ReplacementAlreadyVisibleLayerRemoved => {
                if checkpoint_required {
                    LifecycleMaterializationStatus::CompletedCheckpointRequired
                } else {
                    LifecycleMaterializationStatus::Completed
                }
            }
        };
        Self {
            status,
            child_branch_id: branch_outcome.child_branch_id(),
            layer_index: branch_outcome.layer_index(),
            materialization_handle: Some(materialization_handle),
            reachability_snapshot: Some(reachability_snapshot),
            branch_outcome: Some(branch_outcome),
            checkpoint_required,
            recovery_health: None,
            durable_output_objects: Vec::new(),
            failure: None,
        }
    }

    pub(super) fn completed_durable(
        materialization_handle: BranchMaterializationHandle,
        reachability_snapshot: BranchReachabilitySnapshot,
        branch_outcome: BranchMaterializationOutcome,
        durable_output_objects: Vec<ObjectName>,
    ) -> Self {
        Self {
            status: LifecycleMaterializationStatus::CompletedDurable,
            child_branch_id: branch_outcome.child_branch_id(),
            layer_index: branch_outcome.layer_index(),
            materialization_handle: Some(materialization_handle),
            reachability_snapshot: Some(reachability_snapshot),
            branch_outcome: Some(branch_outcome),
            checkpoint_required: false,
            recovery_health: None,
            durable_output_objects,
            failure: None,
        }
    }

    pub(super) fn manifest_debt(mut self, failure: LifecycleError) -> Self {
        self.status = LifecycleMaterializationStatus::CompletedManifestDebt;
        self.checkpoint_required = true;
        self.recovery_health = telemetry_health();
        self.failure = Some(failure);
        self
    }

    pub(crate) const fn status(&self) -> LifecycleMaterializationStatus {
        self.status
    }

    pub(crate) const fn child_branch_id(&self) -> BranchId {
        self.child_branch_id
    }

    pub(crate) const fn layer_index(&self) -> usize {
        self.layer_index
    }

    pub(crate) const fn materialization_handle(&self) -> Option<BranchMaterializationHandle> {
        self.materialization_handle
    }

    pub(crate) const fn reachability_snapshot(&self) -> Option<&BranchReachabilitySnapshot> {
        self.reachability_snapshot.as_ref()
    }

    pub(crate) const fn branch_outcome(&self) -> Option<&BranchMaterializationOutcome> {
        self.branch_outcome.as_ref()
    }

    pub(crate) const fn checkpoint_required(&self) -> bool {
        self.checkpoint_required
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = match self.status {
            LifecycleMaterializationStatus::Completed
            | LifecycleMaterializationStatus::CompletedCheckpointRequired
            | LifecycleMaterializationStatus::CompletedDurable
            | LifecycleMaterializationStatus::CompletedManifestDebt
            | LifecycleMaterializationStatus::AlreadyMaterialized => {
                MaintenanceOutcomeStatus::Completed
            }
            LifecycleMaterializationStatus::DeferredNoLayer => MaintenanceOutcomeStatus::Deferred,
        };
        // Durable materialization carries ObjectNames for the published
        // replacements. Volatile materialization only knows the table
        // identities created by the branch runtime. Either path lists each
        // output once.
        let affected_object_names: Vec<String> = if self.durable_output_objects.is_empty() {
            self.branch_outcome
                .as_ref()
                .map_or_else(Vec::new, |outcome| {
                    outcome
                        .created_table_identities()
                        .iter()
                        .map(|identity| identity.as_str().to_owned())
                        .collect()
                })
        } else {
            self.durable_output_objects
                .iter()
                .map(|object| object.as_str().to_owned())
                .collect()
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Materialization, status)
            .with_affected_object_names(affected_object_names)
            .with_state_changes(usize::from(!matches!(
                self.status,
                LifecycleMaterializationStatus::DeferredNoLayer
            )))
            .with_checkpoint_required(self.checkpoint_required)
            .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
        if matches!(self.status, LifecycleMaterializationStatus::DeferredNoLayer) {
            outcome = outcome.with_reason("materialization has no inherited layer");
        }
        if matches!(
            self.status,
            LifecycleMaterializationStatus::CompletedManifestDebt
        ) {
            outcome = outcome.with_reason("table rewrite manifest publication needs recovery");
        }
        if let Some(health) = &self.recovery_health {
            outcome = outcome.with_recovery_health(health.clone());
        }
        if let Some(error) = &self.failure {
            outcome = outcome.with_source_error(error.clone());
        }
        outcome
    }
}

impl LifecycleStoragePressure {
    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    /// Return a copy with all source/table-shape pressure neutralized: the
    /// severity, reason, and suggested maintenance task are cleared while the
    /// descriptive shape counts are preserved for diagnostics. Volatile (cache)
    /// modes use this so writes never slow or block on source-table shape and no
    /// maintenance task is suggested, without forking the shared collection
    /// logic.
    pub(crate) const fn with_source_shape_neutralized(self) -> Self {
        Self {
            severity: LifecycleStoragePressureSeverity::None,
            reason: LifecycleStoragePressureReason::None,
            suggested_task: None,
            // Cache mode never throttles writes on source/table shape.
            throttle_ratio_permille: 0,
            ..self
        }
    }

    /// Hold back optional (background) maintenance when the database-wide memory budget is
    /// under pressure. Optional flush/compaction is the only kind cleared here because, with
    /// whole-object materialized readers, starting one holds its inputs and output resident
    /// at once — a transient spike exactly when the budget is already tight. Required
    /// maintenance (`Urgent` / `BlockMutatingAdmission`, which gates write admission) always
    /// runs, so deferral never deadlocks writes. The descriptive shape counts are preserved
    /// for diagnostics.
    pub(crate) const fn deferred_under_global_memory_pressure(
        self,
        global: StorageBudgetPressureSeverity,
    ) -> Self {
        if matches!(self.severity, LifecycleStoragePressureSeverity::Background)
            && global.defers_optional_maintenance()
        {
            self.with_source_shape_neutralized()
        } else {
            self
        }
    }

    pub(crate) const fn severity(self) -> LifecycleStoragePressureSeverity {
        self.severity
    }

    pub(crate) const fn reason(self) -> LifecycleStoragePressureReason {
        self.reason
    }

    pub(crate) const fn suggested_task(self) -> Option<MaintenanceTaskRequest> {
        self.suggested_task
    }

    pub(crate) const fn active_rows(self) -> usize {
        self.active_rows
    }

    pub(crate) const fn active_bytes(self) -> u64 {
        self.active_bytes
    }

    pub(crate) const fn frozen_tables(self) -> usize {
        self.frozen_tables
    }

    pub(crate) const fn frozen_bytes(self) -> u64 {
        self.frozen_bytes
    }

    pub(crate) const fn level_zero_tables(self) -> usize {
        self.level_zero_tables
    }

    pub(crate) const fn owned_tables(self) -> usize {
        self.owned_tables
    }

    pub(crate) const fn table_rewrite_bytes(self) -> u64 {
        self.table_rewrite_bytes
    }

    pub(crate) const fn inherited_layers(self) -> usize {
        self.inherited_layers
    }

    pub(crate) const fn pending_maintenance(self) -> usize {
        self.pending_maintenance
    }

    pub(crate) const fn throttle_ratio_permille(self) -> u16 {
        self.throttle_ratio_permille
    }
}

pub(crate) fn compact_cache_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionRequest,
) -> LifecycleResult<LifecycleCompactionOutcome> {
    compact_cache_branch_with_budget(branch, request, None)
}

pub(crate) fn compact_cache_branch_with_budget(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionRequest,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<LifecycleCompactionOutcome> {
    compact_branch(branch, request, budget)
}

pub(crate) fn compact_cache_branch_to_fixed_point(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionDrainRequest,
) -> LifecycleResult<LifecycleCompactionDrainOutcome> {
    compact_cache_branch_to_fixed_point_with_policy(
        branch,
        request,
        LifecycleCompactionIoPolicy::Unlimited,
    )
}

pub(crate) fn compact_cache_branch_to_fixed_point_with_policy(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionDrainRequest,
    policy: LifecycleCompactionIoPolicy,
) -> LifecycleResult<LifecycleCompactionDrainOutcome> {
    compact_branch_to_fixed_point_with_resource_policy(
        branch,
        request,
        policy,
        |branch, request| compact_branch(branch, request, None),
    )
}

pub(crate) fn compact_durable_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionRequest,
) -> LifecycleResult<LifecycleCompactionOutcome> {
    let request = request
        .clone()
        .with_durability(LifecycleTableRewriteDurability::CheckpointRequiredAfterRewrite);
    compact_branch(branch, &request, None)
}

pub(crate) fn defer_compaction_for_resource_policy(
    branch: &BranchLocalState,
    request: &LifecycleCompactionRequest,
    policy: LifecycleCompactionIoPolicy,
) -> LifecycleResult<Option<MaintenanceOutcome>> {
    // Yield to flush only when the frozen backlog is at the blocking threshold — i.e.
    // flush is so far behind it is blocking write admission. Below that, compaction runs
    // concurrently with flush (different lanes, owned-table-only inputs) instead of
    // churning on preempt/requeue while a single frozen memtable exists.
    if branch.frozen_table_count() >= FROZEN_BLOCKING_FLUSH_THRESHOLD {
        return Ok(Some(flush_pressure_preempted_compaction_outcome()));
    }

    let Some(limit_bytes) = policy.max_bytes_per_task() else {
        return Ok(None);
    };
    let branch_request = request.branch_request()?;
    let plan = branch
        .plan_branch_compaction(&branch_request)
        .map_err(branch_error)?;
    let io_facts = LifecycleCompactionIoFacts::from_plan(branch, &plan);
    let estimated_bytes = io_facts.estimated_budget_bytes();
    if estimated_bytes > limit_bytes {
        return Ok(Some(compaction_io_budget_deferred_outcome(
            estimated_bytes,
            limit_bytes,
        )));
    }
    Ok(None)
}

pub(crate) fn record_lifecycle_compaction_outcome(outcome: &LifecycleCompactionOutcome) {
    let Some(candidate) = outcome.branch_outcome().candidate() else {
        return;
    };
    let io_facts = outcome.io_facts();
    let output_bytes = outcome
        .branch_outcome()
        .table_report()
        .map_or(0, crate::table::TableCompactionReport::output_bytes);
    let operation_kind = match outcome.plan().kind() {
        BranchCompactionKind::CompactL0 => {
            crate::observability::perf_trace::LifecycleCompactionOperationKind::L0
        }
        BranchCompactionKind::CompactL0ToLevelOne => {
            crate::observability::perf_trace::LifecycleCompactionOperationKind::L0ToLevelOne
        }
        BranchCompactionKind::CompactLevel { .. } => {
            crate::observability::perf_trace::LifecycleCompactionOperationKind::Nonzero
        }
        BranchCompactionKind::CompactBottommostLevel { .. } => {
            crate::observability::perf_trace::LifecycleCompactionOperationKind::Bottommost
        }
    };
    crate::debug_trace::trace(format_args!(
        "compact level={} input={} overlap={} in_bytes={} ms={} promo={}",
        candidate.output_level().raw(),
        candidate.input_refs().len(),
        candidate.overlap_refs().len(),
        io_facts.input_bytes(),
        outcome.elapsed().as_millis(),
        candidate.is_metadata_promotion(),
    ));
    crate::observability::perf_trace::record_lifecycle_compaction_operation(
        candidate.output_level().raw(),
        operation_kind,
        candidate.input_refs().len(),
        candidate.overlap_refs().len(),
        outcome.branch_outcome().output_refs().len(),
        io_facts.input_bytes(),
        output_bytes,
        io_facts.metadata_bytes_avoided(),
        outcome.elapsed(),
        io_facts.input_rows(),
        candidate.is_metadata_promotion(),
    );
}

pub(crate) fn table_rewrite_outcome_allows_chain_resubmit(outcome: &MaintenanceOutcome) -> bool {
    matches!(
        outcome.status(),
        MaintenanceOutcomeStatus::Completed | MaintenanceOutcomeStatus::Deferred
    ) && outcome.recovery_health().is_none()
        && outcome.source_error().is_none()
}

pub(crate) fn table_rewrite_outcome_was_flush_preempted(outcome: &MaintenanceOutcome) -> bool {
    outcome.status() == MaintenanceOutcomeStatus::Deferred
        && outcome.reason() == Some(FLUSH_PRESSURE_PREEMPTED_COMPACTION_REASON)
        && outcome.source_error().is_none()
}

pub(crate) fn materialize_cache_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleMaterializationRequest,
) -> LifecycleResult<LifecycleMaterializationOutcome> {
    materialize_branch(branch, request)
}

pub(crate) fn materialize_durable_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleMaterializationRequest,
) -> LifecycleResult<LifecycleMaterializationOutcome> {
    let request = request
        .clone()
        .with_durability(LifecycleTableRewriteDurability::CheckpointRequiredAfterRewrite);
    materialize_branch(branch, &request)
}

pub(crate) fn compaction_request_from_maintenance_task(
    task: &MaintenanceTask,
) -> LifecycleResult<LifecycleCompactionRequest> {
    let MaintenanceTaskScope::TableLevel { branch_id, level } = task.scope() else {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        });
    };
    if task.kind() != MaintenanceTaskKind::Compaction {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "maintenance task is not a compaction task",
        });
    }
    let kind = if level == 0 {
        BranchCompactionKind::CompactL0ToLevelOne
    } else {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "nonzero compaction task requires current branch state",
        });
    };
    LifecycleCompactionRequest::new(
        branch_id,
        kind,
        format!(
            "maintenance-compaction-{}-{level}",
            branch_component(branch_id)
        ),
    )
}

pub(crate) fn current_compaction_request_from_maintenance_task(
    task: &MaintenanceTask,
    branch: &BranchLocalState,
) -> LifecycleResult<Option<LifecycleCompactionRequest>> {
    current_compaction_request_from_maintenance_task_with_budget(task, branch, None)
}

pub(crate) fn current_compaction_request_from_maintenance_task_with_budget(
    task: &MaintenanceTask,
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<Option<LifecycleCompactionRequest>> {
    let MaintenanceTaskScope::TableLevel { branch_id, level } = task.scope() else {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        });
    };
    if task.kind() != MaintenanceTaskKind::Compaction {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "maintenance task is not a compaction task",
        });
    }
    if branch.branch_id() != branch_id {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task branch must match branch state",
        });
    }
    if let Some(score) = selected_compaction_score_for_task(branch, task, budget) {
        return compaction_request_from_score(branch, score, budget).map(Some);
    }
    let Some(kind) = direct_compaction_kind_for_task(branch, level, budget) else {
        return Ok(None);
    };
    LifecycleCompactionRequest::new(
        branch_id,
        kind,
        format!(
            "maintenance-compaction-{}-level-{level}-direct",
            branch_component(branch_id)
        ),
    )
    .map(Some)
}

fn compaction_request_from_score(
    branch: &BranchLocalState,
    score: LifecycleCompactionScore,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<LifecycleCompactionRequest> {
    let branch_id = branch.branch_id();
    let kind = if score.level == 0 {
        BranchCompactionKind::CompactL0ToLevelOne
    } else {
        let level = BranchLevel::new(score.level);
        if is_final_configured_level(branch, usize::from(score.level)) {
            let target_output_bytes = bottommost_compaction_output_target(level, budget).ok_or(
                LifecycleError::MaintenanceTaskFailed {
                    reason: "scored bottommost compaction requires an output target",
                },
            )?;
            let (start_table_index, table_count) = selected_bottommost_compaction_run_with_target(
                branch.owned_levels().get(usize::from(score.level)).ok_or(
                    LifecycleError::MaintenanceTaskFailed {
                        reason: "scored bottommost compaction requires an existing level",
                    },
                )?,
                target_output_bytes,
            )
            .ok_or(LifecycleError::MaintenanceTaskFailed {
                reason: "scored bottommost compaction requires a mergeable table run",
            })?;
            record_selected_bottommost_input_policy(branch, level, start_table_index, table_count);
            BranchCompactionKind::CompactBottommostLevel {
                level,
                start_table_index,
                table_count,
            }
        } else {
            let table_index = score
                .table_index
                .ok_or(LifecycleError::MaintenanceTaskFailed {
                    reason: "scored nonzero compaction requires a table index",
                })?;
            record_selected_nonzero_input_policy(branch, level, table_index);
            BranchCompactionKind::CompactLevel { level, table_index }
        }
    };
    crate::observability::perf_trace::record_lifecycle_compaction_selected(
        score.level,
        score.score,
        score.table_count,
        score.byte_count,
        score.target_bytes.unwrap_or(0),
    );
    LifecycleCompactionRequest::new(
        branch_id,
        kind,
        format!(
            "maintenance-compaction-{}-level-{}-score-{}",
            branch_component(branch_id),
            score.level,
            score.score
        ),
    )
}

fn direct_compaction_kind_for_task(
    branch: &BranchLocalState,
    level: u8,
    budget: Option<StorageRuntimeBudget>,
) -> Option<BranchCompactionKind> {
    if level == BranchLevel::ZERO.raw() {
        let tables = branch
            .owned_levels()
            .get(usize::from(BranchLevel::ZERO.raw()))?;
        return (!tables.is_empty()).then_some(BranchCompactionKind::CompactL0ToLevelOne);
    }
    let level_index = usize::from(level);
    if level_index >= branch.owned_levels().len() {
        return None;
    }
    let tables = branch.owned_levels().get(level_index)?;
    if is_final_configured_level(branch, level_index) {
        let target_output_bytes =
            bottommost_compaction_output_target(BranchLevel::new(level), budget)?;
        if budget.is_some() && level_byte_count(tables) < target_output_bytes {
            return None;
        }
        let (start_table_index, table_count) =
            selected_bottommost_compaction_run_with_target(tables, target_output_bytes)?;
        record_selected_bottommost_input_policy(
            branch,
            BranchLevel::new(level),
            start_table_index,
            table_count,
        );
        return Some(BranchCompactionKind::CompactBottommostLevel {
            level: BranchLevel::new(level),
            start_table_index,
            table_count,
        });
    }
    let table_index = selected_nonzero_compaction_table_index_for_branch(branch, level_index)?;
    record_selected_nonzero_input_policy(branch, BranchLevel::new(level), table_index);
    Some(BranchCompactionKind::CompactLevel {
        level: BranchLevel::new(level),
        table_index,
    })
}

pub(crate) fn stale_compaction_maintenance_outcome() -> MaintenanceOutcome {
    MaintenanceOutcome::new(
        MaintenanceTaskKind::Compaction,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_reason("compaction task no longer has a pressure candidate")
}

pub(crate) fn compaction_io_budget_deferred_outcome(
    estimated_bytes: u64,
    limit_bytes: u64,
) -> MaintenanceOutcome {
    crate::observability::perf_trace::record_lifecycle_compaction_io_budget_deferred(
        estimated_bytes,
        limit_bytes,
    );
    let mut outcome = MaintenanceOutcome::new(
        MaintenanceTaskKind::Compaction,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_effects(0, 0, true)
    .with_reason(COMPACTION_IO_BUDGET_DEFERRED_REASON);
    if let Ok(health) = telemetry_health_debt(COMPACTION_IO_BUDGET_DEFERRED_REASON) {
        outcome = outcome.with_recovery_health(health);
    }
    outcome
}

pub(crate) fn flush_pressure_preempted_compaction_outcome() -> MaintenanceOutcome {
    crate::observability::perf_trace::record_lifecycle_compaction_flush_preempted();
    let mut outcome = MaintenanceOutcome::new(
        MaintenanceTaskKind::Compaction,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_effects(0, 0, true)
    .with_reason(FLUSH_PRESSURE_PREEMPTED_COMPACTION_REASON);
    if let Ok(health) = telemetry_health_debt(FLUSH_PRESSURE_PREEMPTED_COMPACTION_REASON) {
        outcome = outcome.with_recovery_health(health);
    }
    outcome
}

pub(crate) fn materialization_request_from_maintenance_task(
    task: &MaintenanceTask,
) -> LifecycleResult<LifecycleMaterializationRequest> {
    let MaintenanceTaskScope::InheritedLayer {
        branch_id,
        layer_index,
    } = task.scope()
    else {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task must target an inherited layer",
        });
    };
    if task.kind() != MaintenanceTaskKind::Materialization {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "maintenance task is not a materialization task",
        });
    }
    if let Some(handle) = task.materialization_handle() {
        let prefix = format!(
            "maintenance-materialization-{}-source-{}-fork-{}",
            branch_component(branch_id),
            branch_component(handle.source_branch_id()),
            handle.fork_version().as_u64()
        );
        LifecycleMaterializationRequest::from_handle(handle, prefix)
    } else {
        let prefix = format!(
            "maintenance-materialization-{}-layer-{}",
            branch_component(branch_id),
            layer_index
        );
        LifecycleMaterializationRequest::new(branch_id, layer_index, prefix)
    }
}

pub(crate) fn bind_materialization_task_for_enqueue(
    branch: &mut BranchLocalState,
    request: MaintenanceTaskRequest,
) -> LifecycleResult<MaintenanceTaskRequest> {
    if request.kind() != MaintenanceTaskKind::Materialization
        || request.materialization_handle().is_some()
    {
        return Ok(request);
    }
    let MaintenanceTaskScope::InheritedLayer {
        branch_id,
        layer_index,
    } = request.scope()
    else {
        return Ok(request);
    };
    if branch.branch_id() != branch_id {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task branch must match runtime branch",
        });
    }
    if branch.inherited_layers().get(layer_index).is_none() {
        return Ok(request);
    }
    let (handle, _) = branch
        .mark_inherited_layer_materializing(layer_index)
        .map_err(branch_error)?;
    request.with_materialization_handle(handle)
}

pub(crate) fn collect_storage_pressure(
    branch: &BranchLocalState,
    maintenance: MaintenanceExecutorStatus,
) -> LifecycleStoragePressure {
    collect_storage_pressure_with_budget(branch, maintenance, None)
}

pub(crate) fn collect_storage_pressure_with_budget(
    branch: &BranchLocalState,
    maintenance: MaintenanceExecutorStatus,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleStoragePressure {
    let timer = crate::observability::perf_trace::start_timer();
    let branch_id = branch.branch_id();
    let active_rows = branch.active_row_count();
    let active_bytes = branch.active_byte_count();
    let frozen_tables = branch.frozen_table_count();
    let frozen_bytes = branch.frozen_byte_count();
    let level_zero_tables = branch
        .owned_levels()
        .get(usize::from(BranchLevel::ZERO.raw()))
        .map_or(0, std::vec::Vec::len);
    let table_rewrite_score = selected_table_rewrite_score(branch, budget);
    let table_rewrite_bytes = table_rewrite_score.map_or(0, |score| score.byte_count);
    let owned_tables = branch.owned_table_count();
    let inherited_layers = branch.inherited_layer_count();
    let inherited_tables = branch.inherited_table_count();
    let pending_maintenance = maintenance.pending_tasks();
    let active_byte_pressure = active_mutable_byte_pressure(branch, active_bytes);
    let frozen_byte_pressure =
        frozen_mutable_byte_pressure(frozen_tables, frozen_bytes, active_bytes, budget);
    let (severity, reason, suggested_task) = storage_pressure_decision(
        branch_id,
        frozen_tables,
        table_rewrite_score,
        pending_maintenance,
        active_byte_pressure,
        frozen_byte_pressure,
    );

    record_active_byte_pressure(active_byte_pressure);
    crate::observability::perf_trace::record_lifecycle_pressure_collection(
        1,
        branch.owned_levels().len(),
        frozen_tables
            .saturating_add(owned_tables)
            .saturating_add(inherited_tables),
        timer,
        false,
    );

    let throttle_ratio_permille = storage_pressure_throttle_ratio_permille(
        branch,
        active_bytes,
        frozen_bytes,
        frozen_tables,
        level_zero_tables,
        pending_maintenance,
        budget,
    );

    LifecycleStoragePressure {
        branch_id,
        severity,
        reason,
        suggested_task,
        active_rows,
        active_bytes,
        frozen_tables,
        frozen_bytes,
        level_zero_tables,
        owned_tables,
        table_rewrite_bytes,
        inherited_layers,
        pending_maintenance,
        throttle_ratio_permille,
    }
}

/// The eligible table-rewrite task (compaction/materialization) for the most-backed-up
/// level, decoupled from the flush-first `suggested_task` cascade so a backed-up level is
/// scheduled even while frozen memtables pin the single suggestion to flush. Returns
/// `None` when no level is at its rewrite trigger, or when an optional (Background)
/// rewrite is held back under global memory pressure (mirroring the `suggested_task`
/// deferral, but keyed on the rewrite's own severity rather than the overall pressure).
pub(crate) fn eligible_compaction_task(
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
    global: StorageBudgetPressureSeverity,
) -> Option<MaintenanceTaskRequest> {
    let score = selected_table_rewrite_score(branch, budget)?;
    if score.severity == LifecycleStoragePressureSeverity::Background
        && global.defers_optional_maintenance()
    {
        return None;
    }
    Some(score.task_request(branch.branch_id()))
}

/// Every eligible compaction level for the branch as a separate task request (plus
/// materialization if eligible), rather than only the single top-scored one. Per-(branch,
/// level) coalescing keeps re-enqueues idempotent; concurrent workers then pick disjoint
/// levels. Applies the same background/global-pressure defer gate per level as
/// [`eligible_compaction_task`].
pub(crate) fn eligible_compaction_tasks(
    branch: &BranchLocalState,
    _budget: Option<StorageRuntimeBudget>,
    global: StorageBudgetPressureSeverity,
) -> Vec<MaintenanceTaskRequest> {
    let mut compaction_scores: Vec<LifecycleCompactionScore> =
        level_zero_compaction_score(branch).into_iter().collect();
    let level_targets = nonzero_level_targets_for_branch(branch);
    for (level_index, tables) in branch
        .owned_levels()
        .iter()
        .enumerate()
        .skip(usize::from(BranchLevel::ZERO.raw()) + 1)
    {
        let Some(target_bytes) = level_targets.get(level_index).copied() else {
            continue;
        };
        if is_final_configured_level(branch, level_index) {
            continue;
        }
        if let Some(score) =
            nonzero_compaction_score_for_branch(branch, level_index, tables, target_bytes)
        {
            compaction_scores.push(score);
        }
    }

    let defers = global.defers_optional_maintenance();
    let branch_id = branch.branch_id();
    let mut requests = Vec::new();
    for score in compaction_scores {
        let rewrite = LifecycleTableRewriteScore::from(score);
        if rewrite.severity == LifecycleStoragePressureSeverity::Background && defers {
            continue;
        }
        requests.push(rewrite.task_request(branch_id));
    }
    if let Some(score) = materialization_score(branch) {
        if !(score.severity == LifecycleStoragePressureSeverity::Background && defers) {
            requests.push(score.task_request(branch_id));
        }
    }
    requests
}

fn storage_pressure_decision(
    branch_id: BranchId,
    frozen_tables: usize,
    table_rewrite_score: Option<LifecycleTableRewriteScore>,
    pending_maintenance: usize,
    active_byte_pressure: Option<LifecycleStoragePressureSeverity>,
    frozen_byte_pressure: Option<LifecycleStoragePressureSeverity>,
) -> (
    LifecycleStoragePressureSeverity,
    LifecycleStoragePressureReason,
    Option<MaintenanceTaskRequest>,
) {
    let active_byte_suggested_task =
        (frozen_tables > 0).then_some(MaintenanceTaskRequest::flush(branch_id));
    if frozen_tables >= FROZEN_BLOCKING_FLUSH_THRESHOLD {
        return (
            LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            LifecycleStoragePressureReason::FrozenBacklog,
            Some(MaintenanceTaskRequest::flush(branch_id)),
        );
    }
    if frozen_byte_pressure.is_some_and(|severity| {
        severity == LifecycleStoragePressureSeverity::BlockMutatingAdmission
    }) {
        return (
            LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            LifecycleStoragePressureReason::FrozenBacklog,
            Some(MaintenanceTaskRequest::flush(branch_id)),
        );
    }
    if active_byte_pressure.is_some_and(|severity| {
        severity == LifecycleStoragePressureSeverity::BlockMutatingAdmission
    }) && active_byte_suggested_task.is_some()
    {
        return (
            LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            LifecycleStoragePressureReason::ActiveMutableBytes,
            active_byte_suggested_task,
        );
    }
    if frozen_tables > 0 {
        return (
            LifecycleStoragePressureSeverity::Urgent,
            LifecycleStoragePressureReason::FrozenBacklog,
            Some(MaintenanceTaskRequest::flush(branch_id)),
        );
    }
    if table_rewrite_score.is_some_and(|score| {
        score.severity == LifecycleStoragePressureSeverity::BlockMutatingAdmission
    }) {
        let score = table_rewrite_score.expect("checked table rewrite pressure");
        return (
            score.severity,
            score.reason,
            Some(score.task_request(branch_id)),
        );
    }
    if pending_maintenance >= PENDING_MAINTENANCE_BLOCKING_THRESHOLD {
        return (
            LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            LifecycleStoragePressureReason::MaintenanceQueueBacklog,
            None,
        );
    }
    if active_byte_pressure.is_some_and(|severity| {
        severity == LifecycleStoragePressureSeverity::BlockMutatingAdmission
    }) {
        return (
            LifecycleStoragePressureSeverity::Urgent,
            LifecycleStoragePressureReason::ActiveMutableBytes,
            None,
        );
    }
    storage_pressure_nonblocking_decision(
        branch_id,
        table_rewrite_score,
        pending_maintenance,
        active_byte_pressure,
    )
}

fn storage_pressure_nonblocking_decision(
    branch_id: BranchId,
    table_rewrite_score: Option<LifecycleTableRewriteScore>,
    pending_maintenance: usize,
    active_byte_pressure: Option<LifecycleStoragePressureSeverity>,
) -> (
    LifecycleStoragePressureSeverity,
    LifecycleStoragePressureReason,
    Option<MaintenanceTaskRequest>,
) {
    if table_rewrite_score
        .is_some_and(|score| score.severity == LifecycleStoragePressureSeverity::Urgent)
    {
        let score = table_rewrite_score.expect("checked table rewrite pressure");
        return (
            score.severity,
            score.reason,
            Some(score.task_request(branch_id)),
        );
    }
    if active_byte_pressure
        .is_some_and(|severity| severity == LifecycleStoragePressureSeverity::Urgent)
    {
        return (
            LifecycleStoragePressureSeverity::Urgent,
            LifecycleStoragePressureReason::ActiveMutableBytes,
            None,
        );
    }
    if let Some(score) = table_rewrite_score {
        return (
            score.severity,
            score.reason,
            Some(score.task_request(branch_id)),
        );
    }
    if active_byte_pressure
        .is_some_and(|severity| severity == LifecycleStoragePressureSeverity::Background)
    {
        return (
            LifecycleStoragePressureSeverity::Background,
            LifecycleStoragePressureReason::ActiveMutableBytes,
            None,
        );
    }
    if pending_maintenance > 0 {
        return (
            LifecycleStoragePressureSeverity::Background,
            LifecycleStoragePressureReason::MaintenanceQueueBacklog,
            None,
        );
    }
    (
        LifecycleStoragePressureSeverity::None,
        LifecycleStoragePressureReason::None,
        None,
    )
}

fn active_mutable_byte_pressure(
    branch: &BranchLocalState,
    active_bytes: u64,
) -> Option<LifecycleStoragePressureSeverity> {
    if active_bytes == 0 {
        return None;
    }
    let rotation_bytes = u64::try_from(branch.config().active_rotation_bytes()).unwrap_or(u64::MAX);
    let background_threshold = proportional_threshold(rotation_bytes, 1, 2);
    let urgent_threshold = proportional_threshold(rotation_bytes, 3, 4);
    if active_bytes >= rotation_bytes && branch.frozen_table_count() > 0 {
        Some(LifecycleStoragePressureSeverity::BlockMutatingAdmission)
    } else if active_bytes >= urgent_threshold {
        Some(LifecycleStoragePressureSeverity::Urgent)
    } else if active_bytes >= background_threshold {
        Some(LifecycleStoragePressureSeverity::Background)
    } else {
        None
    }
}

fn frozen_mutable_byte_pressure(
    frozen_tables: usize,
    frozen_bytes: u64,
    active_bytes: u64,
    budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleStoragePressureSeverity> {
    if frozen_tables == 0 {
        return None;
    }
    let budget = budget?;
    let limit = budget.pool_limit_bytes(StorageBudgetPool::FrozenMutable);
    if limit == 0 {
        return None;
    }
    let projected_rotation_bytes = frozen_bytes.saturating_add(active_bytes);
    let background_threshold = proportional_threshold(limit, 1, 2);
    let urgent_threshold = proportional_threshold(limit, 3, 4);
    if frozen_bytes >= limit || projected_rotation_bytes >= limit {
        Some(LifecycleStoragePressureSeverity::BlockMutatingAdmission)
    } else if frozen_bytes >= urgent_threshold || projected_rotation_bytes >= urgent_threshold {
        Some(LifecycleStoragePressureSeverity::Urgent)
    } else if frozen_bytes >= background_threshold
        || projected_rotation_bytes >= background_threshold
    {
        Some(LifecycleStoragePressureSeverity::Background)
    } else {
        None
    }
}

fn proportional_threshold(total: u64, numerator: u64, denominator: u64) -> u64 {
    debug_assert!(denominator != 0);
    total.saturating_mul(numerator).div_ceil(denominator).max(1)
}

/// Fullness of `used` against `limit` in permille; 0 when `limit == 0` (mirrors the
/// byte-pressure guards). May exceed 1000 when over the limit; the caller clamps.
fn fullness_permille(used: u64, limit: u64) -> u64 {
    if limit == 0 {
        return 0;
    }
    used.saturating_mul(1000) / limit
}

/// `fullness_permille` for count-based dimensions (usize used/limit).
fn count_fullness_permille(used: usize, limit: usize) -> u64 {
    fullness_permille(
        u64::try_from(used).unwrap_or(u64::MAX),
        u64::try_from(limit).unwrap_or(u64::MAX),
    )
}

/// Max write-throttle fullness across every hard-block admission trigger — active-pool bytes,
/// frozen-pool bytes, frozen-table count, L0-table count, and pending-maintenance depth — in
/// permille (0..=1000). Pure function of values already loaded by
/// `collect_storage_pressure_with_budget` (no extra branch reads). Taking the max over all
/// triggers makes the proportional throttle ramp before *each* binary block; the frozen-count
/// and L0-count cliffs are the in-budget RC2 collapse drivers, and the frozen-pool projection
/// (frozen+active vs the frozen budget) leads the rotation stall.
fn storage_pressure_throttle_ratio_permille(
    branch: &BranchLocalState,
    active_bytes: u64,
    frozen_bytes: u64,
    frozen_tables: usize,
    level_zero_tables: usize,
    pending_maintenance: usize,
    budget: Option<StorageRuntimeBudget>,
) -> u16 {
    let rotation_bytes = u64::try_from(branch.config().active_rotation_bytes()).unwrap_or(u64::MAX);
    let mut ratio = fullness_permille(active_bytes, rotation_bytes);

    if frozen_tables > 0 {
        if let Some(budget) = budget {
            let frozen_limit = budget.pool_limit_bytes(StorageBudgetPool::FrozenMutable);
            ratio = ratio.max(fullness_permille(
                frozen_bytes.saturating_add(active_bytes),
                frozen_limit,
            ));
        }
    }

    ratio = ratio.max(count_fullness_permille(
        frozen_tables,
        FROZEN_BLOCKING_FLUSH_THRESHOLD,
    ));
    ratio = ratio.max(count_fullness_permille(
        level_zero_tables,
        LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD,
    ));
    ratio = ratio.max(count_fullness_permille(
        pending_maintenance,
        PENDING_MAINTENANCE_BLOCKING_THRESHOLD,
    ));

    u16::try_from(ratio.min(1000)).unwrap_or(1000)
}

fn record_active_byte_pressure(severity: Option<LifecycleStoragePressureSeverity>) {
    let Some(severity) = severity else {
        return;
    };
    crate::observability::perf_trace::record_lifecycle_active_byte_pressure(severity);
}

pub(crate) fn compaction_score_key_for_task(
    branch: &BranchLocalState,
    task: MaintenanceTask,
) -> Option<LifecycleCompactionScoreKey> {
    selected_compaction_score_for_task(branch, &task, None).map(compaction_score_key)
}

pub(crate) fn table_rewrite_score_key_for_task_with_budget(
    branch: &BranchLocalState,
    task: MaintenanceTask,
    budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleTableRewriteScoreKey> {
    Some(table_rewrite_score_key(match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, .. }
            if branch_id == branch.branch_id()
                && task.kind() == MaintenanceTaskKind::Compaction =>
        {
            selected_compaction_score_for_task(branch, &task, budget)?.into()
        }
        MaintenanceTaskScope::InheritedLayer {
            branch_id,
            layer_index,
        } if branch_id == branch.branch_id()
            && task.kind() == MaintenanceTaskKind::Materialization
            && layer_index < branch.inherited_layer_count() =>
        {
            materialization_score_for_layer(branch, layer_index)?
        }
        _ => return None,
    }))
}

fn selected_compaction_score_for_task(
    branch: &BranchLocalState,
    task: &MaintenanceTask,
    budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleCompactionScore> {
    let MaintenanceTaskScope::TableLevel { branch_id, level } = task.scope() else {
        return None;
    };
    if branch_id != branch.branch_id() || task.kind() != MaintenanceTaskKind::Compaction {
        return None;
    }
    if level == BranchLevel::ZERO.raw() {
        return selected_compaction_score(branch, budget);
    }
    let level_index = usize::from(level);
    if level_index >= branch.owned_levels().len() {
        return None;
    }
    if is_final_configured_level(branch, level_index) {
        return None;
    }
    let target_bytes = nonzero_level_target_bytes_for_branch(branch, BranchLevel::new(level))?;
    nonzero_compaction_score_for_branch(
        branch,
        level_index,
        branch.owned_levels().get(level_index)?,
        target_bytes,
    )
}

pub(crate) fn table_rewrite_score_key_for_branch_with_budget(
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleTableRewriteScoreKey> {
    selected_table_rewrite_score(branch, budget).map(table_rewrite_score_key)
}

pub(crate) fn table_rewrite_task_request_for_branch_with_budget(
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
) -> Option<MaintenanceTaskRequest> {
    selected_table_rewrite_score(branch, budget).map(|score| score.task_request(branch.branch_id()))
}

pub(crate) fn record_lifecycle_table_rewrite_post_operation_score_with_budget(
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
) {
    let score = selected_table_rewrite_score(branch, budget);
    let (remaining, pressure_score, item_count, byte_count) = score
        .map_or((false, 0, 0, 0), |score| {
            (true, score.score, score.item_count, score.byte_count)
        });
    crate::observability::perf_trace::record_lifecycle_table_rewrite_post_operation_score(
        remaining,
        pressure_score,
        item_count,
        byte_count,
    );
}

fn selected_table_rewrite_score(
    branch: &BranchLocalState,
    budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleTableRewriteScore> {
    let mut best = selected_compaction_score(branch, budget).map(LifecycleTableRewriteScore::from);
    if let Some(score) = materialization_score(branch) {
        best = Some(match best {
            Some(current) if table_rewrite_score_key(current) >= table_rewrite_score_key(score) => {
                current
            }
            _ => score,
        });
    }
    best
}

fn selected_compaction_score(
    branch: &BranchLocalState,
    _budget: Option<StorageRuntimeBudget>,
) -> Option<LifecycleCompactionScore> {
    let mut best = level_zero_compaction_score(branch);
    let level_targets = nonzero_level_targets_for_branch(branch);
    for (level_index, tables) in branch
        .owned_levels()
        .iter()
        .enumerate()
        .skip(usize::from(BranchLevel::ZERO.raw()) + 1)
    {
        let Some(target_bytes) = level_targets.get(level_index).copied() else {
            continue;
        };
        if is_final_configured_level(branch, level_index) {
            continue;
        }
        if let Some(score) =
            nonzero_compaction_score_for_branch(branch, level_index, tables, target_bytes)
        {
            best = Some(match best {
                Some(current) if compaction_score_key(current) >= compaction_score_key(score) => {
                    current
                }
                _ => score,
            });
        }
    }
    best
}

fn level_zero_compaction_score(branch: &BranchLocalState) -> Option<LifecycleCompactionScore> {
    let tables = branch
        .owned_levels()
        .get(usize::from(BranchLevel::ZERO.raw()))?;
    let table_count = tables.len();
    if table_count < LEVEL_ZERO_COMPACTION_THRESHOLD {
        return None;
    }
    // Cached `per_level_bytes()[0]` — byte-identical to `level_byte_count(tables)`, O(1).
    let byte_count = branch.per_level_bytes().first().copied().unwrap_or(0);
    let severity = if table_count >= LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD {
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    } else if table_count >= LEVEL_ZERO_URGENT_COMPACTION_THRESHOLD {
        LifecycleStoragePressureSeverity::Urgent
    } else {
        LifecycleStoragePressureSeverity::Background
    };
    let score = count_pressure_score(table_count, LEVEL_ZERO_COMPACTION_THRESHOLD);
    crate::observability::perf_trace::record_lifecycle_compaction_score_candidate(
        0,
        score,
        table_count,
        byte_count,
    );
    Some(LifecycleCompactionScore {
        level: 0,
        table_index: None,
        score,
        severity,
        reason: LifecycleStoragePressureReason::LevelZeroTableBacklog,
        table_count,
        byte_count,
        target_bytes: None,
    })
}

fn nonzero_compaction_score(
    level_index: usize,
    tables: &[crate::branch::read::BranchOwnedTable],
    byte_count: u64,
    target_bytes: u64,
) -> Option<LifecycleCompactionScore> {
    let level = u8::try_from(level_index).ok()?;
    crate::observability::perf_trace::record_lifecycle_compaction_level_target(level, target_bytes);
    let table_count = tables.len();
    // `byte_count` is the caller's cached `per_level_bytes()[level_index]` — identical to
    // `level_byte_count(tables)` but O(1). `tables` is retained only for the O(1) `table_count`.
    let (severity, score, target_bytes) =
        nonzero_compaction_pressure_for_target(table_count, byte_count, target_bytes)?;
    crate::observability::perf_trace::record_lifecycle_compaction_score_candidate(
        level,
        score,
        table_count,
        byte_count,
    );
    Some(LifecycleCompactionScore {
        level,
        table_index: None,
        score,
        severity,
        reason: LifecycleStoragePressureReason::NonZeroLevelTableBacklog,
        table_count,
        byte_count,
        target_bytes: Some(target_bytes),
    })
}

fn nonzero_compaction_score_for_branch(
    branch: &BranchLocalState,
    level_index: usize,
    tables: &[crate::branch::read::BranchOwnedTable],
    target_bytes: u64,
) -> Option<LifecycleCompactionScore> {
    if is_final_configured_level(branch, level_index) {
        return None;
    }
    let byte_count = branch
        .per_level_bytes()
        .get(level_index)
        .copied()
        .unwrap_or(0);
    let mut score = nonzero_compaction_score(level_index, tables, byte_count, target_bytes)?;
    score.table_index = selected_nonzero_compaction_table_index_for_branch(branch, level_index);
    Some(score)
}

pub(super) fn nonzero_compaction_pressure(
    level: BranchLevel,
    table_count: usize,
    byte_count: u64,
) -> Option<(LifecycleStoragePressureSeverity, u64, u64)> {
    let target_bytes = nonzero_level_target_bytes(level);
    nonzero_compaction_pressure_for_target(table_count, byte_count, target_bytes)
}

fn nonzero_compaction_pressure_for_target(
    table_count: usize,
    byte_count: u64,
    target_bytes: u64,
) -> Option<(LifecycleStoragePressureSeverity, u64, u64)> {
    if table_count < NONZERO_LEVEL_COMPACTION_THRESHOLD && byte_count < target_bytes {
        return None;
    }
    // Non-zero-level backlog NEVER blocks admission (caps at Urgent). A level's
    // table count and byte overshoot are STRUCTURAL at scale — L1+ legitimately
    // holds `dataset / output-table-size` tables while the level cascade catches
    // up — and relief requires multi-GB level merges whose duration (30-60s at
    // 10M x 1KB) blows past every pacing assumption: measured as a 52.8s
    // single-commit stall in one run and a caller-visible
    // `failed_precondition` load abort (stall-wall watchdog firing during one
    // >30s merge with no other completions) in another — billion-scale-ledger.md
    // § 10M three-way. Ln debt is a PACING signal: it already drives the graded
    // admission ramp (`compaction_debt`), which brakes writers toward the
    // near-stop floor as debt grows. Hard admission walls remain where relief is
    // fast and bounded: the L0 backlog (one L0 merge pass) and the frozen
    // backlog (a flush drain).
    let severity = if table_count >= NONZERO_LEVEL_URGENT_COMPACTION_THRESHOLD
        || byte_count >= nonzero_level_urgent_bytes(target_bytes)
    {
        LifecycleStoragePressureSeverity::Urgent
    } else {
        LifecycleStoragePressureSeverity::Background
    };
    let count_score = count_pressure_score(table_count, NONZERO_LEVEL_COMPACTION_THRESHOLD);
    let byte_score = byte_pressure_score(byte_count, target_bytes);
    Some((severity, count_score.max(byte_score), target_bytes))
}

fn materialization_score(branch: &BranchLocalState) -> Option<LifecycleTableRewriteScore> {
    let (layer_index, _, _) = selected_materialization_layer(branch)?;
    materialization_score_for_layer(branch, layer_index)
}

fn materialization_score_for_layer(
    branch: &BranchLocalState,
    layer_index: usize,
) -> Option<LifecycleTableRewriteScore> {
    let layer_count = branch.inherited_layer_count();
    if layer_count < INHERITED_LAYER_PROACTIVE_MATERIALIZATION_MIN {
        return None;
    }
    let layer = branch.inherited_layers().get(layer_index)?;
    let severity = if layer_count >= INHERITED_LAYER_BLOCKING_MATERIALIZATION_THRESHOLD {
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    } else if layer_count >= INHERITED_LAYER_URGENT_MATERIALIZATION_THRESHOLD {
        LifecycleStoragePressureSeverity::Urgent
    } else {
        LifecycleStoragePressureSeverity::Background
    };
    Some(LifecycleTableRewriteScore {
        work: LifecycleTableRewriteWork::Materialization { layer_index },
        score: count_pressure_score(layer_count, INHERITED_LAYER_MATERIALIZATION_THRESHOLD),
        severity,
        reason: LifecycleStoragePressureReason::InheritedLayerBacklog,
        item_count: layer_count,
        byte_count: inherited_layer_byte_count(layer),
    })
}

fn selected_materialization_layer(branch: &BranchLocalState) -> Option<(usize, usize, u64)> {
    branch
        .inherited_layers()
        .iter()
        .enumerate()
        .map(|(layer_index, layer)| {
            let table_count = layer.table_count();
            let byte_count = inherited_layer_byte_count(layer);
            let score =
                count_pressure_score(table_count, INHERITED_LAYER_MATERIALIZATION_THRESHOLD);
            crate::observability::perf_trace::record_lifecycle_materialization_score_candidate(
                layer_index,
                score,
                table_count,
                byte_count,
            );
            (layer_index, table_count, byte_count)
        })
        .max_by_key(|(layer_index, table_count, byte_count)| {
            (*table_count, *byte_count, std::cmp::Reverse(*layer_index))
        })
}

impl From<LifecycleCompactionScore> for LifecycleTableRewriteScore {
    fn from(score: LifecycleCompactionScore) -> Self {
        Self {
            work: LifecycleTableRewriteWork::Compaction { level: score.level },
            score: score.score,
            severity: score.severity,
            reason: score.reason,
            item_count: score.table_count,
            byte_count: score.byte_count,
        }
    }
}

impl LifecycleTableRewriteScore {
    fn task_request(self, branch_id: BranchId) -> MaintenanceTaskRequest {
        match self.work {
            LifecycleTableRewriteWork::Compaction { level } => {
                MaintenanceTaskRequest::compaction(branch_id, level)
            }
            LifecycleTableRewriteWork::Materialization { layer_index } => {
                MaintenanceTaskRequest::materialization_layer(branch_id, layer_index)
            }
        }
    }
}

fn compaction_score_key(score: LifecycleCompactionScore) -> LifecycleCompactionScoreKey {
    LifecycleCompactionScoreKey {
        severity_rank: score.severity.rank(),
        score: score.score,
        table_count: score.table_count,
        byte_count: score.byte_count,
        low_level_tiebreaker: std::cmp::Reverse(score.level),
    }
}

fn table_rewrite_score_key(score: LifecycleTableRewriteScore) -> LifecycleTableRewriteScoreKey {
    let (work_tiebreaker, level) = match score.work {
        LifecycleTableRewriteWork::Compaction { level } => (1, level),
        LifecycleTableRewriteWork::Materialization { .. } => (0, u8::MAX),
    };
    LifecycleTableRewriteScoreKey {
        severity_rank: score.severity.rank(),
        score: score.score,
        item_count: score.item_count,
        byte_count: score.byte_count,
        work_tiebreaker,
        low_level_tiebreaker: std::cmp::Reverse(level),
    }
}

impl LifecycleStoragePressureSeverity {
    const fn rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Background => 1,
            Self::Urgent => 2,
            Self::BlockMutatingAdmission => 3,
        }
    }
}

fn count_pressure_score(count: usize, threshold: usize) -> u64 {
    if threshold == 0 {
        return u64::MAX;
    }
    u64::try_from(count)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000)
        / u64::try_from(threshold).expect("threshold fits in u64")
}

fn byte_pressure_score(bytes: u64, threshold: u64) -> u64 {
    if threshold == 0 {
        return u64::MAX;
    }
    bytes.saturating_mul(1_000) / threshold
}

fn level_byte_count(tables: &[crate::branch::read::BranchOwnedTable]) -> u64 {
    tables.iter().fold(0u64, |total, table| {
        total.saturating_add(table.facts().byte_count())
    })
}

fn inherited_layer_byte_count(layer: &BranchInheritedLayer) -> u64 {
    layer
        .owned_levels()
        .iter()
        .flat_map(|tables| tables.iter())
        .fold(0u64, |total, table| {
            total.saturating_add(table.facts().byte_count())
        })
}

fn selected_nonzero_compaction_table_index_for_branch(
    branch: &BranchLocalState,
    level_index: usize,
) -> Option<usize> {
    let tables = branch.owned_levels().get(level_index)?;
    let level = BranchLevel::new(u8::try_from(level_index).ok()?);
    selected_nonzero_compaction_table_index_after_pointer(tables, branch.compact_pointer(level))
}

fn selected_nonzero_compaction_table_index_after_pointer(
    tables: &[crate::branch::read::BranchOwnedTable],
    pointer: Option<&TablePhysicalKeyBytes>,
) -> Option<usize> {
    if tables.is_empty() {
        return None;
    }
    let Some(pointer) = pointer else {
        return Some(0);
    };
    tables
        .iter()
        .enumerate()
        .find_map(|(index, table)| {
            let last_key = nonzero_table_physical_last_key(table)?;
            (last_key.as_slice() > pointer.as_slice()).then_some(index)
        })
        .or(Some(0))
}

fn nonzero_table_physical_last_key(
    table: &crate::branch::read::BranchOwnedTable,
) -> Option<TablePhysicalKeyBytes> {
    let last_key = table.facts().key_range().last_key();
    (!last_key.is_empty()).then(|| TablePhysicalKeyBytes::from_encoded_internal_key(last_key))
}

fn selected_bottommost_compaction_run_with_target(
    tables: &[crate::branch::read::BranchOwnedTable],
    target_output_bytes: u64,
) -> Option<(usize, usize)> {
    if tables.len() < 2 || target_output_bytes == 0 {
        return None;
    }
    let max_run_len = tables.len().min(BOTTOMMOST_COMPACTION_INPUT_LIMIT);
    (2..=max_run_len)
        .flat_map(|run_len| {
            tables
                .windows(run_len)
                .enumerate()
                .filter_map(move |(start_index, run)| {
                    let byte_count = run.iter().fold(0u64, |bytes, table| {
                        bytes.saturating_add(table.facts().byte_count())
                    });
                    let estimated_output_tables =
                        estimated_compaction_output_tables(byte_count, target_output_bytes)?;
                    if estimated_output_tables >= run_len {
                        return None;
                    }
                    let row_count = run.iter().fold(0u64, |rows, table| {
                        rows.saturating_add(table.facts().row_count())
                    });
                    Some((
                        start_index,
                        run_len,
                        run_len.saturating_sub(estimated_output_tables),
                        byte_count,
                        row_count,
                    ))
                })
        })
        .max_by_key(
            |(start_index, run_len, table_reduction, byte_count, row_count)| {
                (
                    *table_reduction,
                    *byte_count,
                    *row_count,
                    *run_len,
                    std::cmp::Reverse(*start_index),
                )
            },
        )
        .map(|(start_index, run_len, _, _, _)| (start_index, run_len))
}

fn estimated_compaction_output_tables(byte_count: u64, target_output_bytes: u64) -> Option<usize> {
    if byte_count == 0 || target_output_bytes == 0 {
        return None;
    }
    usize::try_from(byte_count.div_ceil(target_output_bytes).max(1)).ok()
}

fn bottommost_compaction_output_target(
    level: BranchLevel,
    budget: Option<StorageRuntimeBudget>,
) -> Option<u64> {
    let kind = BranchCompactionKind::CompactBottommostLevel {
        level,
        start_table_index: 0,
        table_count: 2,
    };
    let config = table_compaction_config_for_kind(kind).ok()?;
    table_compaction_config_with_storage_budget(config, budget)
        .ok()
        .map(TableCompactionConfig::target_output_bytes)
}

fn is_final_configured_level(branch: &BranchLocalState, level_index: usize) -> bool {
    level_index
        .checked_add(1)
        .is_some_and(|next| next == branch.config().max_level_count())
}

fn record_selected_nonzero_input_policy(
    branch: &BranchLocalState,
    level: BranchLevel,
    table_index: usize,
) {
    let Some(table) = branch
        .owned_levels()
        .get(usize::from(level.raw()))
        .and_then(|level_tables| level_tables.get(table_index))
    else {
        return;
    };
    crate::observability::perf_trace::record_lifecycle_compaction_nonzero_input_selected(
        level.raw(),
        table_index,
        table.facts().byte_count(),
        table.facts().row_count(),
    );
    let deeper_overlap_bytes = selected_nonzero_deeper_overlap_bytes(branch, level, table_index);
    crate::observability::perf_trace::record_lifecycle_compaction_deeper_overlap_considered(
        level.raw(),
        deeper_overlap_bytes,
    );
    if deeper_overlap_bytes > 0 {
        crate::observability::perf_trace::record_lifecycle_compaction_output_split_budget_deferred(
            deeper_overlap_bytes,
        );
    }
}

fn record_selected_bottommost_input_policy(
    branch: &BranchLocalState,
    level: BranchLevel,
    start_table_index: usize,
    table_count: usize,
) {
    let Some(tables) = branch.owned_levels().get(usize::from(level.raw())) else {
        return;
    };
    let byte_count = tables
        .iter()
        .skip(start_table_index)
        .take(table_count)
        .fold(0u64, |bytes, table| {
            bytes.saturating_add(table.facts().byte_count())
        });
    let row_count = tables
        .iter()
        .skip(start_table_index)
        .take(table_count)
        .fold(0u64, |rows, table| {
            rows.saturating_add(table.facts().row_count())
        });
    crate::observability::perf_trace::record_lifecycle_compaction_nonzero_input_selected(
        level.raw(),
        start_table_index,
        byte_count,
        row_count,
    );
    crate::observability::perf_trace::record_lifecycle_compaction_deeper_overlap_considered(
        level.raw(),
        0,
    );
}

fn selected_nonzero_deeper_overlap_bytes(
    branch: &BranchLocalState,
    level: BranchLevel,
    table_index: usize,
) -> u64 {
    let level_index = usize::from(level.raw());
    let Some(input) = branch
        .owned_levels()
        .get(level_index)
        .and_then(|level_tables| level_tables.get(table_index))
    else {
        return 0;
    };
    branch
        .owned_levels()
        .get(level_index.saturating_add(2))
        .map_or(0, |deeper_tables| {
            deeper_tables
                .iter()
                .filter(|deeper| table_key_ranges_overlap(input, deeper))
                .fold(0u64, |bytes, table| {
                    bytes.saturating_add(table.facts().byte_count())
                })
        })
}

fn table_key_ranges_overlap(
    left: &crate::branch::read::BranchOwnedTable,
    right: &crate::branch::read::BranchOwnedTable,
) -> bool {
    let Some((left_first, left_last)) = table_physical_key_bounds(left) else {
        return false;
    };
    let Some((right_first, right_last)) = table_physical_key_bounds(right) else {
        return false;
    };
    left_first <= right_last && right_first <= left_last
}

fn table_physical_key_bounds(
    table: &crate::branch::read::BranchOwnedTable,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let first =
        TableInternalKeyBytes::from_canonical_bytes(table.facts().key_range().first_key().to_vec())
            .ok()?;
    let last =
        TableInternalKeyBytes::from_canonical_bytes(table.facts().key_range().last_key().to_vec())
            .ok()?;
    Some((
        first.physical_key_bytes().to_vec(),
        last.physical_key_bytes().to_vec(),
    ))
}

fn compact_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionRequest,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<LifecycleCompactionOutcome> {
    let started = crate::time_compat::Instant::now();
    let branch_request = cache_compaction_branch_request(request, budget)?;
    let plan = branch
        .plan_branch_compaction(&branch_request)
        .map_err(branch_error)?;
    let io_facts = LifecycleCompactionIoFacts::from_plan(branch, &plan);
    let branch_outcome = branch
        .install_branch_compaction_plan(&branch_request, &plan)
        .map_err(branch_error)?;
    Ok(LifecycleCompactionOutcome::new(
        request,
        plan,
        branch_outcome,
        io_facts,
        started.elapsed(),
    ))
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCacheCompaction {
    request: LifecycleCompactionRequest,
    branch_request: BranchCompactionRequest,
    plan: BranchCompactionPlan,
    io_facts: LifecycleCompactionIoFacts,
    prepared_output: Option<(Vec<BranchOwnedTable>, TableCompactionReport)>,
}

pub(crate) fn prepare_cache_compaction_with_budget(
    branch: &BranchLocalState,
    request: &LifecycleCompactionRequest,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<PreparedCacheCompaction> {
    let branch_request = cache_compaction_branch_request(request, budget)?;
    let plan = branch
        .plan_branch_compaction(&branch_request)
        .map_err(branch_error)?;
    let io_facts = LifecycleCompactionIoFacts::from_plan(branch, &plan);
    let prepared_output = match branch
        .prepare_branch_compaction_plan(&branch_request, &plan)
        .map_err(branch_error)?
    {
        Some((artifacts, report)) => {
            let Some(output_level) = plan.output_level() else {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "prepared compaction output requires a candidate output level",
                });
            };
            let output_tables = branch
                .compaction_output_tables(output_level, artifacts, plan.materialization_source())
                .map_err(branch_error)?;
            Some((output_tables, report))
        }
        None => None,
    };
    Ok(PreparedCacheCompaction {
        request: request.clone(),
        branch_request,
        plan,
        io_facts,
        prepared_output,
    })
}

fn cache_compaction_branch_request(
    request: &LifecycleCompactionRequest,
    budget: Option<StorageRuntimeBudget>,
) -> LifecycleResult<BranchCompactionRequest> {
    let branch_request = request.branch_request()?;
    let config = table_compaction_config_with_storage_budget(
        branch_request.table_compaction_config(),
        budget,
    )?;
    Ok(branch_request.with_table_compaction_config(config))
}

pub(crate) fn install_prepared_cache_compaction(
    branch: &mut BranchLocalState,
    prepared: PreparedCacheCompaction,
    elapsed: std::time::Duration,
) -> LifecycleResult<LifecycleCompactionOutcome> {
    let branch_outcome = match prepared.prepared_output {
        Some((output_tables, report)) => branch
            .install_branch_compaction_prepared_plan(
                &prepared.branch_request,
                &prepared.plan,
                output_tables,
                report,
            )
            .map_err(branch_error)?,
        None => branch
            .install_branch_compaction_plan(&prepared.branch_request, &prepared.plan)
            .map_err(branch_error)?,
    };
    Ok(LifecycleCompactionOutcome::new(
        &prepared.request,
        prepared.plan,
        branch_outcome,
        prepared.io_facts,
        elapsed,
    ))
}

pub(crate) fn compact_branch_to_fixed_point_with_resource_policy<F>(
    branch: &mut BranchLocalState,
    request: &LifecycleCompactionDrainRequest,
    policy: LifecycleCompactionIoPolicy,
    mut compact_once: F,
) -> LifecycleResult<LifecycleCompactionDrainOutcome>
where
    F: FnMut(
        &mut BranchLocalState,
        &LifecycleCompactionRequest,
    ) -> LifecycleResult<LifecycleCompactionOutcome>,
{
    if branch.branch_id() != request.branch_id() {
        return Err(branch_error(BranchRuntimeError::InvalidBranchState {
            reason: "compaction drain request branch id must match branch state",
        }));
    }
    let mut outcome =
        LifecycleCompactionDrainOutcome::new(branch.branch_id(), branch.source_layout());
    if branch_compaction_drain_is_stable(branch) {
        return Ok(outcome);
    }

    for pass_index in 0..request.max_passes() {
        let installed_before_pass = outcome.operations_installed();
        let compactable_level_count = branch.config().max_level_count().saturating_sub(1);
        for level_index in 0..compactable_level_count {
            while branch_owned_level_table_count(branch, level_index) > 0 {
                let input_tables_before = branch_owned_level_table_count(branch, level_index);
                let request_for_level = request.compaction_request_for(
                    pass_index,
                    level_index,
                    outcome.operations_attempted(),
                )?;
                outcome.record_attempt();
                if let Some(deferred) =
                    defer_compaction_for_resource_policy(branch, &request_for_level, policy)?
                {
                    outcome.record_deferred(&deferred);
                    return Ok(outcome.with_final_source_layout(branch.source_layout()));
                }
                let compaction = compact_once(branch, &request_for_level)?;
                record_lifecycle_compaction_outcome(&compaction);
                if compaction.branch_outcome().noop_reason().is_some() {
                    break;
                }
                let input_tables_after = branch_owned_level_table_count(branch, level_index);
                if input_tables_after >= input_tables_before {
                    return Err(LifecycleError::MaintenanceTaskFailed {
                        reason: "compaction drain operation made no input-level progress",
                    });
                }
                outcome.record_install(&compaction);
                if compaction.failure().is_some() {
                    return Ok(outcome.with_final_source_layout(branch.source_layout()));
                }
            }
        }
        if branch_compaction_drain_is_stable(branch) {
            return Ok(outcome.with_final_source_layout(branch.source_layout()));
        }
        if outcome.operations_installed() == installed_before_pass {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "compaction drain found no progress with remaining compactable tables",
            });
        }
    }

    Err(LifecycleError::MaintenanceTaskFailed {
        reason: "compaction drain exceeded pass limit",
    })
}

fn branch_compaction_drain_is_stable(branch: &BranchLocalState) -> bool {
    branch
        .owned_levels()
        .iter()
        .take(branch.config().max_level_count().saturating_sub(1))
        .all(Vec::is_empty)
}

fn branch_owned_level_table_count(branch: &BranchLocalState, level_index: usize) -> usize {
    branch.owned_levels().get(level_index).map_or(0, Vec::len)
}

fn materialize_branch(
    branch: &mut BranchLocalState,
    request: &LifecycleMaterializationRequest,
) -> LifecycleResult<LifecycleMaterializationOutcome> {
    if branch.branch_id() != request.child_branch_id() {
        return Err(branch_error(BranchRuntimeError::InvalidBranchState {
            reason: "materialization request branch id must match branch state",
        }));
    }
    if branch
        .inherited_layers()
        .get(request.layer_index())
        .is_none()
        && request.handle().is_none()
    {
        return Ok(LifecycleMaterializationOutcome::deferred(request));
    }
    let (materialization_handle, reachability_snapshot, branch_request) =
        if let Some(handle) = request.handle() {
            if let Some(layer_index) = materialization_layer_index_for_handle(branch, handle) {
                let (bound_handle, snapshot) = branch
                    .mark_inherited_layer_materializing(layer_index)
                    .map_err(branch_error)?;
                if bound_handle.child_branch_id() != handle.child_branch_id()
                    || bound_handle.source_branch_id() != handle.source_branch_id()
                    || bound_handle.fork_version() != handle.fork_version()
                {
                    return Err(branch_error(BranchRuntimeError::InvalidInheritedLayer {
                        reason: "materialization handle must match target layer",
                    }));
                }
                let branch_request = BranchMaterializationRequest::from_handle(
                    bound_handle,
                    request.output_identity_prefix().to_owned(),
                )
                .map_err(branch_error)?;
                (bound_handle, snapshot, branch_request)
            } else {
                let snapshot = branch.reachability_snapshot().map_err(branch_error)?;
                (handle, snapshot, request.branch_request()?)
            }
        } else {
            let (handle, snapshot) = branch
                .mark_inherited_layer_materializing(request.layer_index())
                .map_err(branch_error)?;
            let branch_request = BranchMaterializationRequest::from_handle(
                handle,
                request.output_identity_prefix().to_owned(),
            )
            .map_err(branch_error)?;
            (handle, snapshot, branch_request)
        };
    let branch_outcome = branch
        .materialize_inherited_layer(&branch_request)
        .map_err(branch_error)?;
    Ok(LifecycleMaterializationOutcome::completed(
        request,
        materialization_handle,
        reachability_snapshot,
        branch_outcome,
    ))
}

#[derive(Clone, Debug)]
pub(crate) enum CacheMaterializationBegin {
    Deferred(Box<LifecycleMaterializationOutcome>),
    Build(Box<CacheMaterializationBuild>),
}

#[derive(Clone, Debug)]
pub(crate) struct CacheMaterializationBuild {
    request: LifecycleMaterializationRequest,
    materialization_handle: BranchMaterializationHandle,
    reachability_snapshot: BranchReachabilitySnapshot,
    branch_request: BranchMaterializationRequest,
    branch_snapshot: BranchLocalState,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCacheMaterialization {
    request: LifecycleMaterializationRequest,
    materialization_handle: BranchMaterializationHandle,
    reachability_snapshot: BranchReachabilitySnapshot,
    branch_request: BranchMaterializationRequest,
    prepared: Option<BranchMaterializationPreparedOutput>,
    replacement_tables: Vec<BranchOwnedTable>,
}

pub(crate) fn begin_cache_materialization_build(
    branch: &mut BranchLocalState,
    request: &LifecycleMaterializationRequest,
) -> LifecycleResult<CacheMaterializationBegin> {
    if branch.branch_id() != request.child_branch_id() {
        return Err(branch_error(BranchRuntimeError::InvalidBranchState {
            reason: "materialization request branch id must match branch state",
        }));
    }
    if branch
        .inherited_layers()
        .get(request.layer_index())
        .is_none()
        && request.handle().is_none()
    {
        return Ok(CacheMaterializationBegin::Deferred(Box::new(
            LifecycleMaterializationOutcome::deferred(request),
        )));
    }
    let (materialization_handle, reachability_snapshot, branch_request) =
        if let Some(handle) = request.handle() {
            if let Some(layer_index) = materialization_layer_index_for_handle(branch, handle) {
                let (bound_handle, snapshot) = branch
                    .mark_inherited_layer_materializing(layer_index)
                    .map_err(branch_error)?;
                if bound_handle.child_branch_id() != handle.child_branch_id()
                    || bound_handle.source_branch_id() != handle.source_branch_id()
                    || bound_handle.fork_version() != handle.fork_version()
                {
                    return Err(branch_error(BranchRuntimeError::InvalidInheritedLayer {
                        reason: "materialization handle must match target layer",
                    }));
                }
                let branch_request = BranchMaterializationRequest::from_handle(
                    bound_handle,
                    request.output_identity_prefix().to_owned(),
                )
                .map_err(branch_error)?;
                (bound_handle, snapshot, branch_request)
            } else {
                let snapshot = branch.reachability_snapshot().map_err(branch_error)?;
                (handle, snapshot, request.branch_request()?)
            }
        } else {
            let (handle, snapshot) = branch
                .mark_inherited_layer_materializing(request.layer_index())
                .map_err(branch_error)?;
            let branch_request = BranchMaterializationRequest::from_handle(
                handle,
                request.output_identity_prefix().to_owned(),
            )
            .map_err(branch_error)?;
            (handle, snapshot, branch_request)
        };
    Ok(CacheMaterializationBegin::Build(Box::new(
        CacheMaterializationBuild {
            request: request.clone(),
            materialization_handle,
            reachability_snapshot,
            branch_request,
            branch_snapshot: branch.clone(),
        },
    )))
}

impl CacheMaterializationBuild {
    pub(crate) fn build(self) -> LifecycleResult<PreparedCacheMaterialization> {
        let prepared = self
            .branch_snapshot
            .prepare_materialization_output(&self.branch_request)
            .map_err(branch_error)?;
        let replacement_tables = match prepared.as_ref() {
            Some(prepared) if !prepared.artifacts().is_empty() => self
                .branch_snapshot
                .materialization_tables_from_artifacts(
                    prepared.layer_index(),
                    prepared.materialization_source(),
                    prepared.artifacts().to_vec(),
                )
                .map_err(branch_error)?,
            _ => Vec::new(),
        };
        Ok(PreparedCacheMaterialization {
            request: self.request,
            materialization_handle: self.materialization_handle,
            reachability_snapshot: self.reachability_snapshot,
            branch_request: self.branch_request,
            prepared,
            replacement_tables,
        })
    }
}

pub(crate) fn install_prepared_cache_materialization(
    branch: &mut BranchLocalState,
    prepared: PreparedCacheMaterialization,
) -> LifecycleResult<LifecycleMaterializationOutcome> {
    let branch_outcome = match prepared.prepared {
        Some(prepared_output) => branch
            .install_materialization_prepared_output(
                &prepared.branch_request,
                &prepared_output,
                prepared.replacement_tables,
            )
            .map_err(branch_error)?,
        None => branch
            .materialize_inherited_layer(&prepared.branch_request)
            .map_err(branch_error)?,
    };
    Ok(LifecycleMaterializationOutcome::completed(
        &prepared.request,
        prepared.materialization_handle,
        prepared.reachability_snapshot,
        branch_outcome,
    ))
}

fn materialization_layer_index_for_handle(
    branch: &BranchLocalState,
    handle: BranchMaterializationHandle,
) -> Option<usize> {
    branch.inherited_layers().iter().position(|layer| {
        layer.source_branch_id() == handle.source_branch_id()
            && layer.fork_version() == handle.fork_version()
    })
}

pub(super) fn branch_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::BranchRuntime,
        "branch runtime failed",
        error,
    )
}

fn telemetry_health() -> Option<RecoveryHealth> {
    telemetry_health_debt("table rewrite publication needs recovery").ok()
}

fn branch_component(branch_id: BranchId) -> String {
    let mut component = String::with_capacity(BranchId::BYTE_LEN * 2);
    for byte in branch_id.as_bytes() {
        component.push(hex_digit(byte >> 4));
        component.push(hex_digit(byte & 0x0f));
    }
    component
}

const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + nibble - 10) as char,
        _ => unreachable!(),
    }
}
