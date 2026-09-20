//! Frozen-state flush orchestration.

use super::{
    require_generated_artifact_budget, require_table_reader_budget, telemetry_health_debt,
    LifecycleError, LifecycleLowerLayer, LifecycleResult, LifecycleStats, MaintenanceOutcome,
    MaintenanceOutcomeStatus, MaintenanceTask, MaintenanceTaskKind, MaintenanceTaskScope,
    RecoveryHealth, StorageBudgetLedger,
};
use crate::backend::{PublishError, PublishFailureKind};
use crate::branch::facts::{BranchLevel, BranchTableDescriptor};
use crate::branch::read::BranchOwnedTable;
use crate::branch::state::{
    frozen_rows_match_tables, BranchImmutableInstallOutcome, BranchLocalState,
};
use crate::object::ObjectName;
use crate::service::{
    TableObjectFacts, TableObjectReadError, TableObjectReaderService, TableObjectService,
    TableObjectServiceError,
};
use crate::table::{
    FrozenTable, ImmutableTableBuilder, ImmutableTableReader, TableIdentity, TableReaderConfig,
    TableRow, TableRuntimeFacts, TableSummaryExtras,
};
use strata_core::BranchId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FlushFrozenRequest {
    branch_id: BranchId,
    frozen_index: Option<usize>,
    table_identity_seed: FlushTableIdentitySeed,
    table_object_id: FlushTableObjectId,
    target_level: BranchLevel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FlushTableIdentitySeed(String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FlushTableObjectId(String);

/// One flushed table in a [`FlushFrozenOutcome`]. A1 (#2524): a flush
/// produces exactly one; A2's zone cuts emit several key-disjoint tables
/// per frozen memtable, in key order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlushOutcomeTable {
    table_identity: TableIdentity,
    table_facts: TableRuntimeFacts,
    table_object: Option<ObjectName>,
    object_facts: Option<TableObjectFacts>,
}

impl FlushOutcomeTable {
    pub(crate) fn table_identity(&self) -> &TableIdentity {
        &self.table_identity
    }

    pub(crate) fn object_facts(&self) -> Option<&TableObjectFacts> {
        self.object_facts.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlushFrozenOutcome {
    branch_id: BranchId,
    frozen_index: Option<usize>,
    rows_flushed: u64,
    /// The flushed tables (empty on deferral or pre-publication failure).
    tables: Vec<FlushOutcomeTable>,
    install_outcome: Option<BranchImmutableInstallOutcome>,
    failure: Option<LifecycleError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FlushDrainRequest {
    branch_id: BranchId,
    table_identity_seed: FlushTableIdentitySeed,
    table_object_id: FlushTableObjectId,
    freeze_during_drain_retry_limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlushDrainOutcome {
    branch_id: BranchId,
    frozen_tables_discovered: usize,
    completed_flushes: usize,
    deferred_flushes: usize,
    failed_flushes: usize,
    skipped_flushes: usize,
    freeze_during_drain_retries: usize,
    post_drain_frozen_tables: usize,
    affected_objects: usize,
    affected_object_names: Vec<String>,
    bytes_reclaimed: u64,
    retryable: bool,
    state_changes: usize,
    source_error: Option<LifecycleError>,
    recovery_health: Option<RecoveryHealth>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCacheFlush {
    request: FlushFrozenRequest,
    frozen_index: usize,
    table_facts: TableRuntimeFacts,
    table: BranchOwnedTable,
}

/// One built, published, row-verified flush output awaiting install.
#[derive(Clone, Debug)]
pub(crate) struct PreparedFlushOutput {
    table_facts: TableRuntimeFacts,
    object_facts: TableObjectFacts,
    table: BranchOwnedTable,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedDurableFlush {
    request: FlushFrozenRequest,
    frozen_index: usize,
    /// `Arc` identity of the sealed memtable this build consumed (BS5.3b):
    /// the install matches by identity in O(1); row equality against this
    /// same sealed input is verified here in the (off-lock) prepare phase.
    frozen_identity: usize,
    /// The frozen memtable's outputs, key-disjoint and in key order — their
    /// concatenation partitions the frozen rows (A1: exactly one; A2's zone
    /// cuts emit several). `Err` carries a published-not-installed outcome.
    outputs: Result<Vec<PreparedFlushOutput>, FlushFrozenOutcome>,
}

impl PreparedDurableFlush {
    /// #2553: the published output object names, for the install-time
    /// sweep-race check (adoption of a staged orphan must defer).
    pub(crate) fn output_object_names(&self) -> Vec<crate::object::ObjectName> {
        match &self.outputs {
            Ok(outputs) => outputs
                .iter()
                .map(|output| output.object_facts.object().clone())
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedDurableFlushDrain {
    request: FlushDrainRequest,
    prepared_flushes: Vec<PreparedDurableFlush>,
    /// #2524: pins this drain's published-not-yet-installed object names in
    /// the in-flight registry. Held (shared across clones) until the drain
    /// is consumed or abandoned — by then the outputs are either installed
    /// (covered by the in-memory pins) or orphaned and sweepable.
    #[allow(dead_code, reason = "held for its Drop; never read")]
    inflight_guard: Option<std::sync::Arc<super::durable::InFlightOutputsGuard>>,
}

const DEFAULT_FLUSH_DRAIN_FREEZE_RETRY_LIMIT: usize = 4;
const MEMORY_RELEASE_REEVALUATION_RETAINED_BYTES: u64 = 512 * 1024 * 1024;

/// A2 (#2524): minimum L1 bytes a key gap must skip to earn a flush cut.
/// The zone-gluing pathology jumps whole keyspace zones — hundreds of MiB of
/// L1 between consecutive memtable keys; single-zone workloads' gaps skip
/// ~0-1 tables and never reach this, so they keep today's one-table flush.
const FLUSH_ZONE_CUT_MIN_SKIP_BYTES: u64 = 32 * 1024 * 1024;

/// A2 (#2524): output-table cap per flushed memtable (largest gaps win).
/// Bounds the L0 count inflation against the count-based severity
/// thresholds; a multi-zone commit shape needs one cut per zone boundary,
/// so 4 outputs cover three zones (e.g. meta / page / hot keys).
const FLUSH_MAX_OUTPUT_TABLES: usize = 4;

/// A2 (#2524): cut keys — cut BEFORE each returned encoded physical key —
/// where the frozen memtable's key sequence jumps over at least
/// `min_skip_bytes` of WHOLE level-1 tables. Cutting there unglues
/// multi-zone flushes: each output's span stops covering the L1 bytes the
/// gap skipped, so L0→L1 passes stop dragging the whole level in as
/// overlap and narrow outputs regain the byte-free metadata-promotion path.
///
/// Both sequences are key-ordered (frozen iteration and the disjoint sorted
/// L1 spans), so one monotone two-pointer walk with prefix sums scores every
/// gap in O(rows + tables). Only tables ENTIRELY inside the open gap count —
/// edge-straddling tables get rewritten by a neighboring pass regardless, so
/// counting them would over-cut. Ties break toward the smaller key; the
/// selected cuts return in ascending key order. Cuts land only at physical
/// key transitions, so one key's versions can never split.
pub(crate) fn flush_zone_cut_keys(
    level_one_spans: &[(Vec<u8>, Vec<u8>, u64)],
    frozen: &FrozenTable,
    min_skip_bytes: u64,
    max_outputs: usize,
) -> Vec<Vec<u8>> {
    if level_one_spans.is_empty() || max_outputs <= 1 {
        return Vec::new();
    }
    let mut prefix_bytes = Vec::with_capacity(level_one_spans.len() + 1);
    prefix_bytes.push(0_u64);
    for (_, _, bytes) in level_one_spans {
        let last = *prefix_bytes.last().unwrap_or(&0);
        prefix_bytes.push(last.saturating_add(*bytes));
    }
    // (skip_bytes, cut_key): every distinct-key transition whose gap fully
    // contains >= min_skip_bytes of L1.
    let mut candidates: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut previous_key: Option<Vec<u8>> = None;
    // Monotone bounds over the sorted spans: `low` = first span whose FIRST
    // key sorts above the gap's lower edge; `high` = first span whose LAST
    // key sorts at/above the gap's upper edge. Both only move forward.
    let mut low = 0_usize;
    let mut high = 0_usize;
    for row in frozen.iter() {
        let key = row.key().physical_key_bytes();
        let Some(previous) = previous_key.as_deref() else {
            previous_key = Some(key.to_vec());
            continue;
        };
        if key == previous {
            continue;
        }
        while low < level_one_spans.len() && level_one_spans[low].0.as_slice() <= previous {
            low = low.saturating_add(1);
        }
        high = high.max(low);
        while high < level_one_spans.len() && level_one_spans[high].1.as_slice() < key {
            high = high.saturating_add(1);
        }
        if high > low {
            let skipped = prefix_bytes[high].saturating_sub(prefix_bytes[low]);
            if skipped >= min_skip_bytes {
                candidates.push((skipped, key.to_vec()));
            }
        }
        previous_key = Some(key.to_vec());
    }
    // Largest gaps win the (max_outputs - 1) cut slots; ties toward the
    // smaller key keep the selection deterministic across retries.
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    candidates.truncate(max_outputs.saturating_sub(1));
    let mut cut_keys: Vec<Vec<u8>> = candidates.into_iter().map(|(_, key)| key).collect();
    cut_keys.sort();
    cut_keys
}

impl FlushFrozenRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        frozen_index: Option<usize>,
        table_identity_seed: FlushTableIdentitySeed,
        table_object_id: FlushTableObjectId,
    ) -> LifecycleResult<Self> {
        Self::new_for_level(
            branch_id,
            frozen_index,
            table_identity_seed,
            table_object_id,
            BranchLevel::ZERO,
        )
    }

    pub(crate) fn new_for_level(
        branch_id: BranchId,
        frozen_index: Option<usize>,
        table_identity_seed: FlushTableIdentitySeed,
        table_object_id: FlushTableObjectId,
        target_level: BranchLevel,
    ) -> LifecycleResult<Self> {
        if target_level != BranchLevel::ZERO {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush target level must be zero",
            });
        }
        Ok(Self {
            branch_id,
            frozen_index,
            table_identity_seed,
            table_object_id,
            target_level,
        })
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn frozen_index(&self) -> Option<usize> {
        self.frozen_index
    }

    pub(crate) fn table_identity_seed(&self) -> &FlushTableIdentitySeed {
        &self.table_identity_seed
    }

    pub(crate) fn table_object_id(&self) -> &FlushTableObjectId {
        &self.table_object_id
    }

    pub(crate) const fn target_level(&self) -> BranchLevel {
        self.target_level
    }
}

impl FlushDrainRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        table_identity_seed: FlushTableIdentitySeed,
        table_object_id: FlushTableObjectId,
    ) -> Self {
        Self {
            branch_id,
            table_identity_seed,
            table_object_id,
            freeze_during_drain_retry_limit: DEFAULT_FLUSH_DRAIN_FREEZE_RETRY_LIMIT,
        }
    }

    #[cfg(test)]
    pub(crate) const fn with_freeze_during_drain_retry_limit(mut self, limit: usize) -> Self {
        self.freeze_during_drain_retry_limit = limit;
        self
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    const fn freeze_during_drain_retry_limit(&self) -> usize {
        self.freeze_during_drain_retry_limit
    }

    pub(crate) fn flush_request(
        &self,
        operation_index: usize,
    ) -> LifecycleResult<FlushFrozenRequest> {
        FlushFrozenRequest::new(
            self.branch_id,
            None,
            FlushTableIdentitySeed::new(format!(
                "{}-drain-{operation_index}",
                self.table_identity_seed.as_str()
            ))?,
            FlushTableObjectId::new(format!(
                "{}-drain-{operation_index}",
                self.table_object_id.as_str()
            ))?,
        )
    }
}

impl FlushTableIdentitySeed {
    pub(crate) fn new(value: impl Into<String>) -> LifecycleResult<Self> {
        let value = value.into();
        validate_single_component("table identity seed", &value)?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FlushTableObjectId {
    pub(crate) fn new(value: impl Into<String>) -> LifecycleResult<Self> {
        let value = value.into();
        validate_single_component("table object id", &value)?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FlushFrozenOutcome {
    pub(crate) fn deferred(request: &FlushFrozenRequest) -> Self {
        Self {
            branch_id: request.branch_id(),
            frozen_index: None,
            rows_flushed: 0,
            tables: Vec::new(),
            install_outcome: None,
            failure: None,
        }
    }

    fn completed_outcome(
        request: &FlushFrozenRequest,
        frozen_index: usize,
        tables: Vec<FlushOutcomeTable>,
        install_outcome: BranchImmutableInstallOutcome,
    ) -> Self {
        let rows_flushed = tables
            .iter()
            .map(|table| table.table_facts.row_count())
            .sum();
        Self {
            branch_id: request.branch_id(),
            frozen_index: Some(frozen_index),
            rows_flushed,
            tables,
            install_outcome: Some(install_outcome),
            failure: None,
        }
    }

    fn published_not_installed_outcome(
        request: &FlushFrozenRequest,
        frozen_index: usize,
        published: Vec<(TableRuntimeFacts, TableObjectFacts)>,
        failure: LifecycleError,
    ) -> Self {
        let tables: Vec<FlushOutcomeTable> = published
            .into_iter()
            .map(|(table_facts, object_facts)| FlushOutcomeTable {
                table_identity: table_facts.identity().clone(),
                table_facts,
                table_object: Some(object_facts.object().clone()),
                object_facts: Some(object_facts),
            })
            .collect();
        let rows_flushed = tables
            .iter()
            .map(|table| table.table_facts.row_count())
            .sum();
        // The typed orphan error names the first published object; the
        // maintenance outcome's affected names carry the full set.
        let failure = LifecycleError::flush_publication_orphaned_with(
            tables
                .first()
                .and_then(|table| table.table_object.as_ref())
                .map(|object| object.as_str().to_owned()),
            "flush published table object before install failed",
            failure,
        );
        Self {
            branch_id: request.branch_id(),
            frozen_index: Some(frozen_index),
            rows_flushed,
            tables,
            install_outcome: None,
            failure: Some(failure),
        }
    }

    fn failed(
        request: &FlushFrozenRequest,
        frozen_index: Option<usize>,
        failure: LifecycleError,
    ) -> Self {
        Self {
            branch_id: request.branch_id(),
            frozen_index,
            rows_flushed: 0,
            tables: Vec::new(),
            install_outcome: None,
            failure: Some(failure),
        }
    }

    pub(crate) const fn completed(&self) -> bool {
        self.install_outcome.is_some()
    }

    pub(crate) fn deferred_no_frozen_state(&self) -> bool {
        self.install_outcome.is_none() && self.failure.is_none() && self.tables.is_empty()
    }

    pub(crate) fn failed_before_publication(&self) -> bool {
        self.failure.is_some() && !self.tables.iter().any(|table| table.table_object.is_some())
    }

    pub(crate) fn published_not_installed(&self) -> bool {
        self.failure.is_some()
            && self.tables.iter().any(|table| table.table_object.is_some())
            && self.install_outcome.is_none()
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn frozen_index(&self) -> Option<usize> {
        self.frozen_index
    }

    pub(crate) const fn rows_flushed(&self) -> u64 {
        self.rows_flushed
    }

    /// The flushed tables, in key order (empty on deferral or
    /// pre-publication failure).
    pub(crate) fn tables(&self) -> &[FlushOutcomeTable] {
        &self.tables
    }

    /// The first flushed table's identity (the only one until A2 cuts).
    pub(crate) fn table_identity(&self) -> Option<&TableIdentity> {
        self.tables.first().map(|table| &table.table_identity)
    }

    pub(crate) fn table_facts(&self) -> Option<&TableRuntimeFacts> {
        self.tables.first().map(|table| &table.table_facts)
    }

    pub(crate) fn table_object(&self) -> Option<&ObjectName> {
        self.tables
            .first()
            .and_then(|table| table.table_object.as_ref())
    }

    pub(crate) fn object_facts(&self) -> Option<&TableObjectFacts> {
        self.tables
            .first()
            .and_then(|table| table.object_facts.as_ref())
    }

    pub(crate) const fn install_outcome(&self) -> Option<BranchImmutableInstallOutcome> {
        self.install_outcome
    }

    pub(crate) fn failure(&self) -> Option<&LifecycleError> {
        self.failure.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = if self.completed() {
            MaintenanceOutcomeStatus::Completed
        } else if self.deferred_no_frozen_state() {
            MaintenanceOutcomeStatus::Deferred
        } else {
            MaintenanceOutcomeStatus::Failed
        };
        let affected_objects = self
            .tables
            .iter()
            .filter(|table| table.table_object.is_some())
            .count();
        let retryable = self.published_not_installed()
            && self
                .failure
                .as_ref()
                .is_some_and(published_not_installed_retryable);
        let bytes_reclaimed = if self.completed() {
            self.tables
                .iter()
                .map(|table| table.table_facts.byte_count())
                .sum()
        } else {
            0
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Flush, status)
            .with_effects(affected_objects, bytes_reclaimed, retryable)
            .with_state_changes(usize::from(self.install_outcome.is_some()))
            .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
        let affected_names: Vec<String> = self
            .tables
            .iter()
            .filter_map(|table| table.table_object.as_ref())
            .map(|object| object.as_str().to_owned())
            .collect();
        if !affected_names.is_empty() {
            outcome = outcome.with_affected_object_names(affected_names);
        }
        if self.deferred_no_frozen_state() {
            outcome = outcome.with_reason("flush has no frozen state to publish");
        }
        if self.published_not_installed() {
            outcome = outcome.with_reason("flush published table object before install failed");
        }
        if self.failed_before_publication() {
            outcome = outcome.with_reason("flush failed before table object publication");
        }
        if let Some(error) = &self.failure {
            outcome = outcome.with_source_error(error.clone());
        }
        outcome
    }
}

impl FlushDrainOutcome {
    fn new(branch_id: BranchId, frozen_tables_discovered: usize) -> Self {
        Self {
            branch_id,
            frozen_tables_discovered,
            completed_flushes: 0,
            deferred_flushes: 0,
            failed_flushes: 0,
            skipped_flushes: 0,
            freeze_during_drain_retries: 0,
            post_drain_frozen_tables: 0,
            affected_objects: 0,
            affected_object_names: Vec::new(),
            bytes_reclaimed: 0,
            retryable: false,
            state_changes: 0,
            source_error: None,
            recovery_health: None,
        }
    }

    fn skipped(mut self, post_drain_frozen_tables: usize) -> Self {
        self.skipped_flushes = 1;
        self.post_drain_frozen_tables = post_drain_frozen_tables;
        self
    }

    fn with_post_drain_frozen_tables(mut self, post_drain_frozen_tables: usize) -> Self {
        self.post_drain_frozen_tables = post_drain_frozen_tables;
        if post_drain_frozen_tables > 0 && self.failed_flushes == 0 {
            self.deferred_flushes = self.deferred_flushes.saturating_add(1);
        }
        self
    }

    fn with_freeze_during_drain_retries(mut self, retries: usize) -> Self {
        self.freeze_during_drain_retries = retries;
        self
    }

    fn record_maintenance_outcome(&mut self, outcome: &MaintenanceOutcome) -> bool {
        match outcome.status() {
            MaintenanceOutcomeStatus::Completed => {
                self.completed_flushes = self.completed_flushes.saturating_add(1);
            }
            MaintenanceOutcomeStatus::Deferred | MaintenanceOutcomeStatus::Canceled => {
                self.deferred_flushes = self.deferred_flushes.saturating_add(1);
            }
            MaintenanceOutcomeStatus::Failed => {
                self.failed_flushes = self.failed_flushes.saturating_add(1);
            }
        }
        self.affected_objects = self
            .affected_objects
            .saturating_add(outcome.affected_objects());
        self.affected_object_names
            .extend(outcome.affected_object_names().iter().cloned());
        self.bytes_reclaimed = self
            .bytes_reclaimed
            .saturating_add(outcome.bytes_reclaimed());
        self.retryable |= outcome.retryable();
        self.state_changes = self.state_changes.saturating_add(outcome.state_changes());
        if self.source_error.is_none() {
            self.source_error = outcome.source_error().cloned();
        }
        if self.recovery_health.is_none() {
            self.recovery_health = outcome.recovery_health().cloned();
        }
        matches!(outcome.status(), MaintenanceOutcomeStatus::Completed)
            && outcome.source_error().is_none()
            && outcome.recovery_health().is_none()
    }

    fn record_error(&mut self, error: LifecycleError) {
        // #2553's third site (#2778): a flush output racing the table-object
        // sweep is a benign scheduling race — the frozen input stays queued
        // and the retried drain publishes fresh bytes. Count it as a
        // deferral, exactly like the locked-publish and compaction-install
        // arms, never as a task failure. The error stays attached for
        // diagnostics.
        if error.is_rewrite_output_sweep_race() {
            self.deferred_flushes = self.deferred_flushes.saturating_add(1);
        } else {
            self.failed_flushes = self.failed_flushes.saturating_add(1);
        }
        self.retryable = true;
        self.source_error.get_or_insert(error);
    }

    fn status(&self) -> MaintenanceOutcomeStatus {
        if self.failed_flushes > 0 {
            MaintenanceOutcomeStatus::Failed
        } else if self.completed_flushes > 0 && self.post_drain_frozen_tables == 0 {
            MaintenanceOutcomeStatus::Completed
        } else {
            MaintenanceOutcomeStatus::Deferred
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn frozen_tables_discovered(&self) -> usize {
        self.frozen_tables_discovered
    }

    pub(crate) const fn completed_flushes(&self) -> usize {
        self.completed_flushes
    }

    pub(crate) const fn deferred_flushes(&self) -> usize {
        self.deferred_flushes
    }

    pub(crate) const fn failed_flushes(&self) -> usize {
        self.failed_flushes
    }

    pub(crate) const fn skipped_flushes(&self) -> usize {
        self.skipped_flushes
    }

    pub(crate) const fn freeze_during_drain_retries(&self) -> usize {
        self.freeze_during_drain_retries
    }

    pub(crate) const fn post_drain_frozen_tables(&self) -> usize {
        self.post_drain_frozen_tables
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = self.status();
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Flush, status)
            .with_effects(self.affected_objects, self.bytes_reclaimed, self.retryable)
            .with_state_changes(self.state_changes)
            .with_stats(LifecycleStats::new(
                0,
                0,
                self.completed_flushes
                    .saturating_add(self.deferred_flushes)
                    .saturating_add(self.failed_flushes)
                    .saturating_add(self.skipped_flushes),
                0,
                0,
            ));
        if !self.affected_object_names.is_empty() {
            outcome = outcome.with_affected_object_names(self.affected_object_names.clone());
        }
        if self.skipped_flushes > 0 {
            outcome = outcome.with_reason("flush drain has no frozen state to publish");
        } else if self.post_drain_frozen_tables > 0 {
            outcome = outcome.with_reason("flush drain left deferred frozen state");
        } else if self.failed_flushes > 0 {
            outcome = outcome.with_reason("flush drain failed before all frozen state was drained");
        }
        if let Some(error) = &self.source_error {
            outcome = outcome.with_source_error(error.clone());
        }
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        } else if (self.completed_flushes > 0 && status != MaintenanceOutcomeStatus::Completed)
            || self.post_drain_frozen_tables > 0
        {
            if let Ok(health) = telemetry_health_debt("flush drain made partial progress") {
                outcome = outcome.with_recovery_health(health);
            }
        }
        outcome
    }
}

pub(crate) fn flush_cache_branch(
    branch: &mut BranchLocalState,
    request: &FlushFrozenRequest,
) -> LifecycleResult<FlushFrozenOutcome> {
    flush_cache_branch_with_budget(branch, request, None, None)
}

pub(crate) fn flush_cache_branch_with_budget(
    branch: &mut BranchLocalState,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
) -> LifecycleResult<FlushFrozenOutcome> {
    let Some(frozen_index) = select_frozen_index(branch, request)? else {
        return Ok(FlushFrozenOutcome::deferred(request));
    };
    let artifact = build_frozen_artifact(
        branch,
        request,
        frozen_index,
        data_block_bytes,
        crate::format::TableCompression::Uncompressed,
    )?;
    require_optional_generated_artifact_budget(
        budget,
        artifact.byte_count(),
        "flush artifact exceeds generated artifact budget",
    )?;
    require_optional_table_reader_budget(
        budget,
        artifact.byte_count(),
        "flush table reader exceeds storage budget",
    )?;
    let identity = artifact.facts().identity().clone();
    let table_facts = artifact.facts().clone();
    let extras = artifact.extras().clone();
    let reader = ImmutableTableReader::open_bytes(
        identity.clone(),
        artifact.into_bytes(),
        TableReaderConfig::default().with_eager_filter_unavailable(),
    )
    .map_err(table_error)?;
    let table = branch_owned_table(branch.branch_id(), identity, reader, extras)?;
    let install_outcome = match branch.replace_frozen_with_level_zero_table(frozen_index, table) {
        Ok(outcome) => outcome,
        Err(error) => {
            return Ok(FlushFrozenOutcome::failed(
                request,
                Some(frozen_index),
                branch_error(error),
            ));
        }
    };
    Ok(FlushFrozenOutcome::completed_outcome(
        request,
        frozen_index,
        vec![FlushOutcomeTable {
            table_identity: table_facts.identity().clone(),
            table_facts,
            table_object: None,
            object_facts: None,
        }],
        install_outcome,
    ))
}

pub(crate) fn prepare_cache_flush_with_budget(
    branch: &BranchLocalState,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
) -> LifecycleResult<Option<PreparedCacheFlush>> {
    let Some(frozen_index) = select_frozen_index(branch, request)? else {
        return Ok(None);
    };
    let artifact = build_frozen_artifact(
        branch,
        request,
        frozen_index,
        data_block_bytes,
        crate::format::TableCompression::Uncompressed,
    )?;
    require_optional_generated_artifact_budget(
        budget,
        artifact.byte_count(),
        "flush artifact exceeds generated artifact budget",
    )?;
    require_optional_table_reader_budget(
        budget,
        artifact.byte_count(),
        "flush table reader exceeds storage budget",
    )?;
    let identity = artifact.facts().identity().clone();
    let table_facts = artifact.facts().clone();
    let extras = artifact.extras().clone();
    let reader = ImmutableTableReader::open_bytes(
        identity.clone(),
        artifact.into_bytes(),
        TableReaderConfig::default().with_eager_filter_unavailable(),
    )
    .map_err(table_error)?;
    let table = branch_owned_table(branch.branch_id(), identity, reader, extras)?;
    Ok(Some(PreparedCacheFlush {
        request: request.clone(),
        frozen_index,
        table_facts,
        table,
    }))
}

pub(crate) fn install_prepared_cache_flush(
    branch: &mut BranchLocalState,
    prepared: PreparedCacheFlush,
) -> FlushFrozenOutcome {
    let PreparedCacheFlush {
        request,
        frozen_index,
        table_facts,
        table,
    } = prepared;
    let install_outcome = match branch.replace_frozen_with_level_zero_table(frozen_index, table) {
        Ok(outcome) => outcome,
        Err(error) => {
            return FlushFrozenOutcome::failed(&request, Some(frozen_index), branch_error(error));
        }
    };
    FlushFrozenOutcome::completed_outcome(
        &request,
        frozen_index,
        vec![FlushOutcomeTable {
            table_identity: table_facts.identity().clone(),
            table_facts,
            table_object: None,
            object_facts: None,
        }],
        install_outcome,
    )
}

pub(crate) fn flush_durable_branch(
    branch: &mut BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
) -> LifecycleResult<FlushFrozenOutcome> {
    flush_durable_branch_with_budget(
        branch,
        table_service,
        reader_service,
        request,
        None,
        None,
        crate::format::TableCompression::Uncompressed,
    )
}

pub(crate) fn flush_durable_branch_with_budget(
    branch: &mut BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
) -> LifecycleResult<FlushFrozenOutcome> {
    let Some(prepared) = prepare_durable_flush_with_budget(
        branch,
        table_service,
        reader_service,
        request,
        budget,
        data_block_bytes,
        compression,
        // Foreground path: publish and install share one runtime-lock hold,
        // so the mark can never interleave — no in-flight pin needed.
        None,
    )?
    else {
        return Ok(FlushFrozenOutcome::deferred(request));
    };
    Ok(install_prepared_durable_flush(branch, prepared))
}

#[allow(clippy::too_many_arguments, reason = "explicit build-input plumbing")]
pub(crate) fn prepare_durable_flush_with_budget(
    branch: &BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    inflight: Option<&super::durable::InFlightOutputsGuard>,
) -> LifecycleResult<Option<PreparedDurableFlush>> {
    let Some(frozen_index) = select_frozen_index(branch, request)? else {
        return Ok(None);
    };
    // A2 (#2524): cut the memtable at zone jumps. Zero cuts — every
    // single-zone workload — takes today's single-output path unchanged
    // (byte-identical artifact and identity).
    let cut_keys = planned_flush_zone_cut_keys(branch, frozen_index)?;
    if !cut_keys.is_empty() {
        return prepare_segmented_flush(
            branch,
            table_service,
            reader_service,
            request,
            budget,
            data_block_bytes,
            compression,
            inflight,
            frozen_index,
            &cut_keys,
        );
    }
    prepare_single_output_flush(
        branch,
        table_service,
        reader_service,
        request,
        budget,
        data_block_bytes,
        compression,
        inflight,
        frozen_index,
    )
}

#[allow(clippy::too_many_arguments, reason = "explicit build-input plumbing")]
fn prepare_single_output_flush(
    branch: &BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    inflight: Option<&super::durable::InFlightOutputsGuard>,
    frozen_index: usize,
) -> LifecycleResult<Option<PreparedDurableFlush>> {
    let artifact =
        build_frozen_artifact(branch, request, frozen_index, data_block_bytes, compression)?;
    require_optional_generated_artifact_budget(
        budget,
        artifact.byte_count(),
        "flush artifact exceeds generated artifact budget",
    )?;
    // BS4.5a: the installed durable table holds a lazy, disk-resident reader — charge only its
    // metadata-resident footprint (index + properties + filter frame), not the full encoded object.
    // The generated-artifact pool above still accounts the transient in-memory artifact at full size.
    // (Cache-mode flush keeps charging full bytes: those tables install eager, genuinely-resident
    // readers — constraint C2.)
    require_optional_table_reader_budget(
        budget,
        artifact.resident_metadata_bytes(),
        "flush table reader exceeds storage budget",
    )?;
    let identity = artifact.facts().identity().clone();
    let table_facts = artifact.facts().clone();
    let branch_component = request.branch_id().to_string();
    let object_id = derived_object_id(request, &table_facts);
    reserve_inflight_flush_output(inflight, request, &branch_component, &object_id)?;
    let (object_facts, adopted) = publish_or_load_existing(
        table_service,
        &branch_component,
        request.target_level().raw().into(),
        &object_id,
        artifact.bytes(),
        &table_facts,
    )?;
    let frozen =
        branch
            .frozen()
            .get(frozen_index)
            .ok_or(LifecycleError::MaintenanceTaskFailed {
                reason: "flush frozen index must exist",
            })?;
    let frozen_identity = frozen.memory_state_identity();
    let reader = match reader_service.open_reader(
        identity.clone(),
        &object_facts,
        TableReaderConfig::default()
            .with_eager_filter_unavailable()
            .deny_runtime_materialization(),
    ) {
        Ok(reader) => reader,
        Err(error) => {
            if let Some(race) = adopted_output_swept(table_service, adopted, &object_facts) {
                return Err(race);
            }
            return Ok(Some(published_not_installed_flush(
                request,
                frozen_index,
                frozen_identity,
                table_facts,
                object_facts,
                table_read_error(error),
            )));
        }
    };
    // W2.4: warm the block cache from the just-encoded bytes (no-evict
    // inserts only) — a fresh L0 table serves the hottest recent keys and
    // should not start cold. Best-effort is wrong here for the same reason as
    // the rewrite path: bounds are index-derived over just-published bytes,
    // so a failure means a corrupt index and fails closed. (No sweep-race
    // arm: warming reads no backend bytes — the encoded input is in hand and
    // the bounds were read at open.)
    if let Err(error) = reader.warm_data_blocks_from_encoded(artifact.bytes()) {
        return Ok(Some(published_not_installed_flush(
            request,
            frozen_index,
            frozen_identity,
            table_facts,
            object_facts,
            table_error(error),
        )));
    }
    let extras = artifact.extras().clone();
    let outputs = match branch_owned_table(branch.branch_id(), identity, reader, extras) {
        // BS5.3b: the row-equality verification moved OFF-lock from the
        // install (where it walked every row under the runtime lock, ~7.5 ms
        // per flush): verify the built, published tables' rows end to end
        // through the readers against the sealed memtable the build consumed
        // (the outputs' concatenation must partition the frozen rows). The
        // install then matches that memtable by O(1) identity.
        Ok(table) => {
            if frozen_rows_match_tables(&[&table], frozen) {
                Ok(vec![PreparedFlushOutput {
                    table_facts,
                    object_facts,
                    table,
                }])
            } else if let Some(race) = adopted_output_swept(table_service, adopted, &object_facts) {
                // The row walk reads through the reader; an adopted object
                // deleted mid-verify reads as a mismatch.
                return Err(race);
            } else {
                Err(FlushFrozenOutcome::published_not_installed_outcome(
                    request,
                    frozen_index,
                    vec![(table_facts, object_facts)],
                    LifecycleError::MaintenanceTaskFailed {
                        reason: "flush artifact rows do not match the frozen table",
                    },
                ))
            }
        }
        Err(error) => Err(FlushFrozenOutcome::published_not_installed_outcome(
            request,
            frozen_index,
            vec![(table_facts, object_facts)],
            error,
        )),
    };
    Ok(Some(PreparedDurableFlush {
        request: request.clone(),
        frozen_index,
        frozen_identity,
        outputs,
    }))
}

/// A2 (#2524): the planned zone cuts for one frozen memtable, with the
/// outputs/cuts counters recorded at plan time.
fn planned_flush_zone_cut_keys(
    branch: &BranchLocalState,
    frozen_index: usize,
) -> LifecycleResult<Vec<Vec<u8>>> {
    let frozen =
        branch
            .frozen()
            .get(frozen_index)
            .ok_or(LifecycleError::MaintenanceTaskFailed {
                reason: "flush frozen index must exist",
            })?;
    let cut_keys = flush_zone_cut_keys(
        &branch.level_one_physical_spans(),
        frozen,
        FLUSH_ZONE_CUT_MIN_SKIP_BYTES,
        FLUSH_MAX_OUTPUT_TABLES,
    );
    crate::observability::perf_trace::record_lifecycle_flush_zone_outputs(
        cut_keys.len() as u64 + 1,
        cut_keys.len() as u64,
    );
    Ok(cut_keys)
}

/// A2 test seam: the segmented path with explicit cut keys, so tests can
/// exercise multi-output flushes without constructing >=32MiB of L1.
#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "test seam mirrors the production signature"
)]
pub(crate) fn prepare_durable_flush_with_cuts_for_test(
    branch: &BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    inflight: Option<&super::durable::InFlightOutputsGuard>,
    cut_keys: &[Vec<u8>],
) -> LifecycleResult<Option<PreparedDurableFlush>> {
    let Some(frozen_index) = select_frozen_index(branch, request)? else {
        return Ok(None);
    };
    prepare_segmented_flush(
        branch,
        table_service,
        reader_service,
        request,
        budget,
        data_block_bytes,
        compression,
        inflight,
        frozen_index,
        cut_keys,
    )
}

/// A2 (#2524): the multi-output flush — one segment per zone, built,
/// published, and verified per segment so transient artifact memory stays
/// bounded to one segment's encoding. Any post-publication failure reports
/// EVERY name published so far (the outputs are unreachable orphans the
/// sweep reclaims). The outputs' ordered concatenation must partition the
/// frozen rows — the same off-lock verify contract as the single path.
#[allow(clippy::too_many_arguments, reason = "explicit build-input plumbing")]
#[allow(
    clippy::too_many_lines,
    reason = "one publish pipeline per segment, kept linear"
)]
fn prepare_segmented_flush(
    branch: &BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushFrozenRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    inflight: Option<&super::durable::InFlightOutputsGuard>,
    frozen_index: usize,
    cut_keys: &[Vec<u8>],
) -> LifecycleResult<Option<PreparedDurableFlush>> {
    let frozen =
        branch
            .frozen()
            .get(frozen_index)
            .ok_or(LifecycleError::MaintenanceTaskFailed {
                reason: "flush frozen index must exist",
            })?;
    let frozen_identity = frozen.memory_state_identity();
    let rows: Vec<TableRow> = frozen.iter().map(|row| row.as_ref().clone()).collect();
    // Segment bounds: cut BEFORE each cut key. Cuts derive from observed
    // physical-key transitions, so every segment is non-empty and one key's
    // versions never split.
    let mut segments: Vec<&[TableRow]> = Vec::with_capacity(cut_keys.len() + 1);
    let mut start = 0_usize;
    for cut in cut_keys {
        let end = start
            + rows[start..].partition_point(|row| row.key().physical_key_bytes() < cut.as_slice());
        if end > start {
            segments.push(&rows[start..end]);
            start = end;
        }
    }
    segments.push(&rows[start..]);
    let branch_component = request.branch_id().to_string();
    let mut outputs: Vec<PreparedFlushOutput> = Vec::with_capacity(segments.len());
    let mut published: Vec<(TableRuntimeFacts, TableObjectFacts)> = Vec::new();
    let mut adopted_outputs: Vec<TableObjectFacts> = Vec::new();
    for segment in segments {
        if segment.is_empty() {
            continue;
        }
        let identity = derived_segment_identity(request, segment)?;
        let artifact = ImmutableTableBuilder::new(
            super::compaction::lifecycle_table_builder_config(data_block_bytes, compression)?,
        )
        .map_err(table_error)?
        .build_from_rows(identity.clone(), segment)
        .map_err(table_error)?;
        require_optional_generated_artifact_budget(
            budget,
            artifact.byte_count(),
            "flush artifact exceeds generated artifact budget",
        )?;
        require_optional_table_reader_budget(
            budget,
            artifact.resident_metadata_bytes(),
            "flush table reader exceeds storage budget",
        )?;
        let table_facts = artifact.facts().clone();
        let object_id = derived_object_id(request, &table_facts);
        reserve_inflight_flush_output(inflight, request, &branch_component, &object_id)?;
        let publish = publish_or_load_existing(
            table_service,
            &branch_component,
            request.target_level().raw().into(),
            &object_id,
            artifact.bytes(),
            &table_facts,
        );
        let (object_facts, adopted) = match publish {
            Ok(published_output) => published_output,
            // Nothing published yet: propagate like the single path.
            Err(error) if published.is_empty() => return Err(error),
            Err(error) => {
                return Ok(Some(published_not_installed_flush_outputs(
                    request,
                    frozen_index,
                    frozen_identity,
                    published,
                    error,
                )));
            }
        };
        let reader = match reader_service.open_reader(
            identity.clone(),
            &object_facts,
            TableReaderConfig::default()
                .with_eager_filter_unavailable()
                .deny_runtime_materialization(),
        ) {
            Ok(reader) => reader,
            Err(error) => {
                if let Some(race) = adopted_output_swept(table_service, adopted, &object_facts) {
                    return Err(race);
                }
                published.push((table_facts, object_facts));
                return Ok(Some(published_not_installed_flush_outputs(
                    request,
                    frozen_index,
                    frozen_identity,
                    published,
                    table_read_error(error),
                )));
            }
        };
        // No sweep-race arm: warming reads no backend bytes (see the single
        // path).
        if let Err(error) = reader.warm_data_blocks_from_encoded(artifact.bytes()) {
            published.push((table_facts, object_facts));
            return Ok(Some(published_not_installed_flush_outputs(
                request,
                frozen_index,
                frozen_identity,
                published,
                table_error(error),
            )));
        }
        if adopted {
            adopted_outputs.push(object_facts.clone());
        }
        let extras = artifact.extras().clone();
        match branch_owned_table(branch.branch_id(), identity, reader, extras) {
            Ok(table) => {
                published.push((table_facts.clone(), object_facts.clone()));
                outputs.push(PreparedFlushOutput {
                    table_facts,
                    object_facts,
                    table,
                });
            }
            Err(error) => {
                published.push((table_facts, object_facts));
                return Ok(Some(published_not_installed_flush_outputs(
                    request,
                    frozen_index,
                    frozen_identity,
                    published,
                    error,
                )));
            }
        }
    }
    let refs: Vec<&BranchOwnedTable> = outputs.iter().map(|output| &output.table).collect();
    let outputs = if frozen_rows_match_tables(&refs, frozen) {
        Ok(outputs)
    } else {
        // The row walk reads through the readers; an adopted segment deleted
        // mid-verify reads as a mismatch.
        for object_facts in &adopted_outputs {
            if let Some(race) = adopted_output_swept(table_service, true, object_facts) {
                return Err(race);
            }
        }
        Err(FlushFrozenOutcome::published_not_installed_outcome(
            request,
            frozen_index,
            published,
            LifecycleError::MaintenanceTaskFailed {
                reason: "flush artifact rows do not match the frozen table",
            },
        ))
    };
    Ok(Some(PreparedDurableFlush {
        request: request.clone(),
        frozen_index,
        frozen_identity,
        outputs,
    }))
}

pub(crate) fn install_prepared_durable_flush(
    branch: &mut BranchLocalState,
    prepared: PreparedDurableFlush,
) -> FlushFrozenOutcome {
    let PreparedDurableFlush {
        request,
        frozen_index,
        frozen_identity,
        outputs,
    } = prepared;
    let outputs = match outputs {
        Ok(outputs) => outputs,
        Err(outcome) => return outcome,
    };
    // Keep the facts for the orphan outcome: the install consumes the tables.
    let published: Vec<(TableRuntimeFacts, TableObjectFacts)> = outputs
        .iter()
        .map(|output| (output.table_facts.clone(), output.object_facts.clone()))
        .collect();
    let tables: Vec<BranchOwnedTable> = outputs.into_iter().map(|output| output.table).collect();
    let install_outcome =
        match branch.replace_frozen_with_level_zero_tables_by_identity(frozen_identity, tables) {
            Ok(outcome) => outcome,
            Err(error) => {
                return FlushFrozenOutcome::published_not_installed_outcome(
                    &request,
                    frozen_index,
                    published,
                    branch_error(error),
                );
            }
        };
    let tables = published
        .into_iter()
        .map(|(table_facts, object_facts)| FlushOutcomeTable {
            table_identity: table_facts.identity().clone(),
            table_facts,
            table_object: Some(object_facts.object().clone()),
            object_facts: Some(object_facts),
        })
        .collect();
    FlushFrozenOutcome::completed_outcome(&request, frozen_index, tables, install_outcome)
}

#[allow(clippy::too_many_arguments, reason = "explicit build-input plumbing")]
pub(crate) fn prepare_durable_flush_drain_with_budget(
    branch: &BranchLocalState,
    table_service: &TableObjectService<'_>,
    reader_service: &TableObjectReaderService<'static>,
    request: &FlushDrainRequest,
    budget: Option<&StorageBudgetLedger>,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    inflight: Option<&super::durable::InFlightTableOutputs>,
) -> LifecycleResult<PreparedDurableFlushDrain> {
    if branch.branch_id() != request.branch_id() {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush drain branch id must match branch state",
        });
    }
    let inflight_guard = inflight.map(|registry| std::sync::Arc::new(registry.guard()));
    let mut branch_snapshot = branch.clone();
    let frozen_tables_discovered = branch_snapshot.frozen_table_count();
    let operation_limit =
        frozen_tables_discovered.saturating_add(request.freeze_during_drain_retry_limit());
    let mut prepared_flushes = Vec::new();
    let mut simulated_outcome =
        FlushDrainOutcome::new(request.branch_id(), frozen_tables_discovered);
    let mut operation_index = 0usize;
    while branch_snapshot.frozen_table_count() > 0 && operation_index < operation_limit {
        let flush_request = request.flush_request(operation_index)?;
        let Some(prepared) = prepare_durable_flush_with_budget(
            &branch_snapshot,
            table_service,
            reader_service,
            &flush_request,
            budget,
            data_block_bytes,
            compression,
            inflight_guard.as_deref(),
        )?
        else {
            let maintenance = FlushFrozenOutcome::deferred(&flush_request).maintenance_outcome();
            simulated_outcome.record_maintenance_outcome(&maintenance);
            break;
        };
        let maintenance = install_prepared_durable_flush(&mut branch_snapshot, prepared.clone())
            .maintenance_outcome();
        let can_continue = simulated_outcome.record_maintenance_outcome(&maintenance);
        prepared_flushes.push(prepared);
        operation_index = operation_index.saturating_add(1);
        if !can_continue {
            break;
        }
    }
    Ok(PreparedDurableFlushDrain {
        request: request.clone(),
        prepared_flushes,
        inflight_guard,
    })
}

pub(crate) fn install_prepared_durable_flush_drain_with(
    branch: &mut BranchLocalState,
    prepared: PreparedDurableFlushDrain,
    mut install_one: impl FnMut(
        &mut BranchLocalState,
        PreparedDurableFlush,
    ) -> LifecycleResult<MaintenanceOutcome>,
) -> LifecycleResult<MaintenanceOutcome> {
    if branch.branch_id() != prepared.request.branch_id() {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush drain branch id must match branch state",
        });
    }
    let active_bytes_before = branch.active_byte_count();
    let frozen_bytes_before = branch.frozen_byte_count();
    let frozen_tables_discovered = branch.frozen_table_count();
    crate::observability::perf_trace::record_lifecycle_flush_drain_frozen_tables_discovered(
        frozen_tables_discovered,
    );
    if prepared.prepared_flushes.is_empty() {
        let outcome =
            FlushDrainOutcome::new(prepared.request.branch_id(), frozen_tables_discovered)
                .skipped(0);
        record_flush_drain_outcome_counters(&outcome);
        record_flush_memory_retention(
            active_bytes_before,
            frozen_bytes_before,
            branch.active_byte_count(),
            branch.frozen_byte_count(),
        );
        return Ok(outcome.maintenance_outcome());
    }

    let mut outcome =
        FlushDrainOutcome::new(prepared.request.branch_id(), frozen_tables_discovered);
    for prepared_flush in prepared.prepared_flushes {
        match install_one(branch, prepared_flush) {
            Ok(maintenance) => {
                if !outcome.record_maintenance_outcome(&maintenance) {
                    break;
                }
            }
            Err(error) => {
                outcome.record_error(error);
                break;
            }
        }
    }

    let freeze_during_drain_retries = outcome
        .completed_flushes()
        .saturating_sub(frozen_tables_discovered);
    outcome = outcome
        .with_freeze_during_drain_retries(freeze_during_drain_retries)
        .with_post_drain_frozen_tables(branch.frozen_table_count());
    record_flush_drain_outcome_counters(&outcome);
    record_flush_memory_retention(
        active_bytes_before,
        frozen_bytes_before,
        branch.active_byte_count(),
        branch.frozen_byte_count(),
    );
    Ok(outcome.maintenance_outcome())
}

pub(crate) fn flush_branch_drain_with(
    branch: &mut BranchLocalState,
    request: &FlushDrainRequest,
    mut flush_one: impl FnMut(
        &mut BranchLocalState,
        &FlushFrozenRequest,
    ) -> LifecycleResult<MaintenanceOutcome>,
) -> LifecycleResult<FlushDrainOutcome> {
    if branch.branch_id() != request.branch_id() {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush drain branch id must match branch state",
        });
    }
    let active_bytes_before = branch.active_byte_count();
    let frozen_bytes_before = branch.frozen_byte_count();
    let frozen_tables_discovered = branch.frozen_table_count();
    crate::observability::perf_trace::record_lifecycle_flush_drain_frozen_tables_discovered(
        frozen_tables_discovered,
    );
    if frozen_tables_discovered == 0 {
        let outcome = FlushDrainOutcome::new(request.branch_id(), 0).skipped(0);
        record_flush_drain_outcome_counters(&outcome);
        record_flush_memory_retention(
            active_bytes_before,
            frozen_bytes_before,
            branch.active_byte_count(),
            branch.frozen_byte_count(),
        );
        return Ok(outcome);
    }

    let operation_limit =
        frozen_tables_discovered.saturating_add(request.freeze_during_drain_retry_limit());
    let mut outcome = FlushDrainOutcome::new(request.branch_id(), frozen_tables_discovered);
    let mut operation_index = 0usize;
    while branch.frozen_table_count() > 0 {
        if operation_index >= operation_limit {
            break;
        }
        let flush_request = request.flush_request(operation_index)?;
        match flush_one(branch, &flush_request) {
            Ok(maintenance) => {
                let can_continue = outcome.record_maintenance_outcome(&maintenance);
                operation_index = operation_index.saturating_add(1);
                if !can_continue {
                    break;
                }
            }
            Err(error) => {
                outcome.record_error(error);
                break;
            }
        }
    }

    let freeze_during_drain_retries = outcome
        .completed_flushes()
        .saturating_sub(frozen_tables_discovered);
    outcome = outcome
        .with_freeze_during_drain_retries(freeze_during_drain_retries)
        .with_post_drain_frozen_tables(branch.frozen_table_count());
    record_flush_drain_outcome_counters(&outcome);
    record_flush_memory_retention(
        active_bytes_before,
        frozen_bytes_before,
        branch.active_byte_count(),
        branch.frozen_byte_count(),
    );
    Ok(outcome)
}

pub(crate) fn flush_drain_request_from_maintenance_task(
    task: &MaintenanceTask,
) -> LifecycleResult<FlushDrainRequest> {
    let MaintenanceTaskScope::Branch(branch_id) = task.scope() else {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush drain task must target a branch",
        });
    };
    flush_drain_request_for_branch_from_maintenance_task(task, branch_id)
}

pub(crate) fn flush_drain_request_for_branch_from_maintenance_task(
    task: &MaintenanceTask,
    branch_id: BranchId,
) -> LifecycleResult<FlushDrainRequest> {
    if task.kind() != MaintenanceTaskKind::Flush {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "maintenance task kind is not flush",
        });
    }
    match task.scope() {
        MaintenanceTaskScope::Branch(task_branch_id) if task_branch_id == branch_id => {}
        MaintenanceTaskScope::Branch(_) => {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush drain branch id must match task scope",
            });
        }
        MaintenanceTaskScope::Global => {}
        _ => {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush drain task must target a branch or global scope",
            });
        }
    }
    flush_drain_request_for_branch(branch_id)
}

pub(crate) fn flush_drain_request_for_branch(
    branch_id: BranchId,
) -> LifecycleResult<FlushDrainRequest> {
    Ok(FlushDrainRequest::new(
        branch_id,
        FlushTableIdentitySeed::new(format!("flush-seed-{branch_id}"))?,
        FlushTableObjectId::new(format!("flush-object-{branch_id}"))?,
    ))
}

pub(crate) fn flush_drain_maintenance_outcome_for_scope(
    outcomes: &[FlushDrainOutcome],
) -> MaintenanceOutcome {
    let mut completed_flushes = 0usize;
    let mut deferred_flushes = 0usize;
    let mut failed_flushes = 0usize;
    let mut skipped_flushes = 0usize;
    let mut post_drain_frozen_tables = 0usize;
    let mut affected_objects = 0usize;
    let mut affected_object_names = Vec::new();
    let mut bytes_reclaimed = 0u64;
    let mut retryable = false;
    let mut state_changes = 0usize;
    let mut source_error = None;
    let mut recovery_health = None;

    for outcome in outcomes {
        completed_flushes = completed_flushes.saturating_add(outcome.completed_flushes);
        deferred_flushes = deferred_flushes.saturating_add(outcome.deferred_flushes);
        failed_flushes = failed_flushes.saturating_add(outcome.failed_flushes);
        skipped_flushes = skipped_flushes.saturating_add(outcome.skipped_flushes);
        post_drain_frozen_tables =
            post_drain_frozen_tables.saturating_add(outcome.post_drain_frozen_tables);
        affected_objects = affected_objects.saturating_add(outcome.affected_objects);
        affected_object_names.extend(outcome.affected_object_names.iter().cloned());
        bytes_reclaimed = bytes_reclaimed.saturating_add(outcome.bytes_reclaimed);
        retryable |= outcome.retryable;
        state_changes = state_changes.saturating_add(outcome.state_changes);
        if source_error.is_none() {
            source_error.clone_from(&outcome.source_error);
        }
        if recovery_health.is_none() {
            recovery_health.clone_from(&outcome.recovery_health);
        }
    }

    let status = if failed_flushes > 0 {
        MaintenanceOutcomeStatus::Failed
    } else if completed_flushes > 0 && post_drain_frozen_tables == 0 {
        MaintenanceOutcomeStatus::Completed
    } else {
        MaintenanceOutcomeStatus::Deferred
    };
    let mut maintenance = MaintenanceOutcome::new(MaintenanceTaskKind::Flush, status)
        .with_effects(affected_objects, bytes_reclaimed, retryable)
        .with_state_changes(state_changes)
        .with_stats(LifecycleStats::new(
            0,
            0,
            completed_flushes
                .saturating_add(deferred_flushes)
                .saturating_add(failed_flushes)
                .saturating_add(skipped_flushes),
            0,
            0,
        ));
    if !affected_object_names.is_empty() {
        maintenance = maintenance.with_affected_object_names(affected_object_names);
    }
    if skipped_flushes > 0 && completed_flushes == 0 && failed_flushes == 0 {
        maintenance = maintenance.with_reason("flush drain has no frozen state to publish");
    } else if post_drain_frozen_tables > 0 {
        maintenance = maintenance.with_reason("flush drain left deferred frozen state");
    } else if failed_flushes > 0 {
        maintenance =
            maintenance.with_reason("flush drain failed before all frozen state was drained");
    }
    if let Some(error) = source_error {
        maintenance = maintenance.with_source_error(error);
    }
    if let Some(health) = recovery_health {
        maintenance = maintenance.with_recovery_health(health);
    } else if (completed_flushes > 0 && status != MaintenanceOutcomeStatus::Completed)
        || post_drain_frozen_tables > 0
    {
        if let Ok(health) = telemetry_health_debt("flush drain made partial progress") {
            maintenance = maintenance.with_recovery_health(health);
        }
    }
    maintenance
}

fn record_flush_drain_outcome_counters(outcome: &FlushDrainOutcome) {
    crate::observability::perf_trace::record_lifecycle_flush_drain_operations_completed(
        outcome.completed_flushes(),
    );
    crate::observability::perf_trace::record_lifecycle_flush_drain_freeze_retries(
        outcome.freeze_during_drain_retries(),
    );
    crate::observability::perf_trace::record_lifecycle_flush_drain_failures(
        outcome.failed_flushes(),
    );
    crate::observability::perf_trace::record_lifecycle_flush_drain_post_drain_frozen_tables(
        outcome.post_drain_frozen_tables(),
    );
}

fn record_flush_memory_retention(
    active_bytes_before: u64,
    frozen_bytes_before: u64,
    active_bytes_after: u64,
    frozen_bytes_after: u64,
) {
    crate::observability::perf_trace::record_lifecycle_flush_memory_retention(
        active_bytes_before,
        frozen_bytes_before,
        active_bytes_after,
        frozen_bytes_after,
        MEMORY_RELEASE_REEVALUATION_RETAINED_BYTES,
    );
}

fn select_frozen_index(
    branch: &BranchLocalState,
    request: &FlushFrozenRequest,
) -> LifecycleResult<Option<usize>> {
    if branch.branch_id() != request.branch_id() {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush branch id must match branch state",
        });
    }
    let frozen_count = branch.frozen_table_count();
    match request.frozen_index() {
        Some(index) if index < frozen_count => Ok(Some(index)),
        Some(_) => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush frozen index must exist",
        }),
        None if frozen_count == 0 => Ok(None),
        None => Ok(Some(frozen_count - 1)),
    }
}

fn build_frozen_artifact(
    branch: &BranchLocalState,
    request: &FlushFrozenRequest,
    frozen_index: usize,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
) -> LifecycleResult<crate::table::BuiltTableArtifact> {
    let frozen =
        branch
            .frozen()
            .get(frozen_index)
            .ok_or(LifecycleError::MaintenanceTaskFailed {
                reason: "flush frozen index must exist",
            })?;
    let identity = derived_table_identity(request, frozen)?;
    ImmutableTableBuilder::new(super::compaction::lifecycle_table_builder_config(
        data_block_bytes,
        compression,
    )?)
    .map_err(table_error)?
    .build_from_frozen(identity, frozen)
    .map_err(table_error)
}

fn derived_table_identity(
    request: &FlushFrozenRequest,
    frozen: &FrozenTable,
) -> LifecycleResult<TableIdentity> {
    let facts = frozen.facts();
    TableIdentity::new(format!(
        "{}-{}-frozen-{}-{}-{}-{:016x}",
        request.table_identity_seed().as_str(),
        request.branch_id(),
        facts.row_count(),
        facts
            .min_commit()
            .map_or(0, strata_core::CommitVersion::as_u64),
        facts
            .max_commit()
            .map_or(0, strata_core::CommitVersion::as_u64),
        frozen_key_span_digest(frozen),
    ))
    .map_err(table_error)
}

/// A2 (#2524): per-segment identity — the whole-memtable recipe above,
/// derived from the segment's own rows (count, commit range, key-span
/// digest). Content-derived, never index-salted: a retry with the same
/// content and cuts idempotently loads the existing object, while layout
/// drift after recovery changes the cuts and orphans the old objects for
/// the sweep.
fn derived_segment_identity(
    request: &FlushFrozenRequest,
    rows: &[TableRow],
) -> LifecycleResult<TableIdentity> {
    let mut min_commit: Option<u64> = None;
    let mut max_commit: Option<u64> = None;
    for row in rows {
        let version = row.row().commit_version().as_u64();
        min_commit = Some(min_commit.map_or(version, |current| current.min(version)));
        max_commit = Some(max_commit.map_or(version, |current| current.max(version)));
    }
    let first_key = rows.first().map(|row| row.key().as_slice());
    let last_key = rows.last().map(|row| row.key().as_slice());
    TableIdentity::new(format!(
        "{}-{}-frozen-{}-{}-{}-{:016x}",
        request.table_identity_seed().as_str(),
        request.branch_id(),
        rows.len(),
        min_commit.unwrap_or(0),
        max_commit.unwrap_or(0),
        key_span_digest(first_key, last_key),
    ))
    .map_err(table_error)
}

/// FNV-1a-64 over the frozen table's physical key span (first key, a separator, then last key).
///
/// The flush object id derives from the table identity (`derived_object_id`), and
/// `publish_or_load_existing` treats an id that already exists as the same content (idempotent
/// retry). So two frozen tables that share a row count and commit range but cover different keys
/// MUST NOT share an identity — otherwise the second flush would load the first's stale object,
/// whose rows would not match this table's summary (extras). Production keeps flush identities
/// distinct through monotonic commit versions; this span digest keeps them distinct even when the
/// row count and commit range collide (e.g. raw same-version rows). It is idempotent — the same
/// frozen span yields the same digest — so retry-load stays sound.
fn frozen_key_span_digest(frozen: &FrozenTable) -> u64 {
    let first = frozen.first_key();
    let last = frozen.last_key();
    key_span_digest(
        first
            .as_ref()
            .map(super::super::table::TableInternalKeyBytes::as_slice),
        last.as_ref()
            .map(super::super::table::TableInternalKeyBytes::as_slice),
    )
}

fn key_span_digest(first_key: Option<&[u8]>, last_key: Option<&[u8]>) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    fn mix(mut hash: u64, bytes: &[u8]) -> u64 {
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
    let mut hash = FNV_OFFSET;
    if let Some(first) = first_key {
        hash = mix(hash, first);
    }
    // Unit separator so an empty last key cannot alias a first key that ends where last begins.
    hash = mix(hash, b"\x1f");
    if let Some(last) = last_key {
        hash = mix(hash, last);
    }
    hash
}

/// #2524: pin the output name BEFORE the bytes land — the table-object mark
/// runs concurrently with this off-lock build, and an unreachable, unpinned
/// object would be swept out from under the publish.
fn reserve_inflight_flush_output(
    inflight: Option<&super::durable::InFlightOutputsGuard>,
    request: &FlushFrozenRequest,
    branch_component: &str,
    object_id: &str,
) -> LifecycleResult<()> {
    let Some(inflight) = inflight else {
        return Ok(());
    };
    let object_name = crate::layout::ObjectLayout::table_object(
        branch_component,
        request.target_level().raw().into(),
        object_id,
    )
    .map_err(|source| {
        LifecycleError::lower_layer_with(
            crate::lifecycle::LifecycleLowerLayer::Layout,
            "layout failed",
            source,
        )
    })?;
    inflight.reserve(object_name);
    Ok(())
}

fn derived_object_id(request: &FlushFrozenRequest, table_facts: &TableRuntimeFacts) -> String {
    format!(
        "{}-{}",
        request.table_object_id().as_str(),
        table_facts.identity().as_str(),
    )
}

/// Publishes the table object, or — when a content-identical id already
/// exists (idempotent retry) — adopts it. The second tuple field reports the
/// adoption: an adopted object may be an orphan of an abandoned attempt that
/// a concurrent table-object sweep is entitled to delete, so the caller must
/// classify read failures on it as the #2553 sweep race, not as its own
/// publication failure.
pub(crate) fn publish_or_load_existing(
    table_service: &TableObjectService<'_>,
    branch_component: &str,
    level: u32,
    object_id: &str,
    bytes: &[u8],
    table_facts: &TableRuntimeFacts,
) -> LifecycleResult<(TableObjectFacts, bool)> {
    match table_service.publish_create(branch_component, level, object_id, bytes) {
        Ok(facts) => Ok((facts, false)),
        Err(TableObjectServiceError::Publish { source, .. })
            if source.kind() == PublishFailureKind::PreconditionFailed =>
        {
            TableObjectService::facts_for_table(branch_component, level, object_id, table_facts)
                .map(|facts| (facts, true))
                .map_err(table_service_error)
        }
        Err(error) => Err(table_service_error(error)),
    }
}

/// #3382: the build-phase leg of the #2553 sweep race. A read failure on an
/// ADOPTED object whose object is now gone means a concurrent table-object
/// sweep deleted the orphan the retry adopted — the same benign scheduling
/// race the install-time `verify_output_objects_not_swept` check catches, one
/// phase earlier. Returns the typed race for the caller to propagate; `None`
/// (fresh publish, object still present, or probe failure) keeps the caller's
/// fail-closed classification.
fn adopted_output_swept(
    table_service: &TableObjectService<'_>,
    adopted: bool,
    object_facts: &TableObjectFacts,
) -> Option<LifecycleError> {
    if !adopted {
        return None;
    }
    match table_service.object_exists(object_facts.object()) {
        Ok(false) => Some(LifecycleError::RewriteOutputRacedSweep {
            object: object_facts.object().clone(),
        }),
        // A still-present object means the read failure is the object's own
        // (corruption/conflict — fail closed); a probe failure must not mask
        // the original error either.
        Ok(true) | Err(_) => None,
    }
}

pub(crate) fn branch_owned_table(
    branch_id: BranchId,
    identity: TableIdentity,
    reader: ImmutableTableReader<'static>,
    extras: TableSummaryExtras,
) -> LifecycleResult<BranchOwnedTable> {
    let descriptor =
        BranchTableDescriptor::new(identity, reader.facts().clone(), BranchLevel::ZERO)
            .map_err(branch_error)?;
    BranchOwnedTable::new(branch_id, descriptor, reader, extras).map_err(branch_error)
}

fn validate_single_component(field: &'static str, value: &str) -> LifecycleResult<()> {
    if value.is_empty() {
        return Err(LifecycleError::InvalidConfig {
            field,
            reason: "flush component must not be empty",
        });
    }
    if value.as_bytes().contains(&0) || value.contains('/') {
        return Err(LifecycleError::InvalidConfig {
            field,
            reason: "flush component must be a single object component",
        });
    }
    ObjectName::new(value).map_err(|_| LifecycleError::InvalidConfig {
        field,
        reason: "flush component must be a valid object name",
    })?;
    Ok(())
}

fn require_optional_generated_artifact_budget(
    budget: Option<&StorageBudgetLedger>,
    bytes: u64,
    reason: &'static str,
) -> LifecycleResult<()> {
    if let Some(budget) = budget {
        require_generated_artifact_budget(budget, bytes, reason)?;
    }
    Ok(())
}

fn require_optional_table_reader_budget(
    budget: Option<&StorageBudgetLedger>,
    bytes: u64,
    reason: &'static str,
) -> LifecycleResult<()> {
    if let Some(budget) = budget {
        require_table_reader_budget(budget, bytes, reason)?;
    }
    Ok(())
}

/// A flush whose object published but whose table cannot install — the
/// prepared outcome carries the error while the published object stays
/// reachable for reconciliation.
fn published_not_installed_flush(
    request: &FlushFrozenRequest,
    frozen_index: usize,
    frozen_identity: usize,
    table_facts: crate::table::TableRuntimeFacts,
    object_facts: TableObjectFacts,
    error: LifecycleError,
) -> PreparedDurableFlush {
    published_not_installed_flush_outputs(
        request,
        frozen_index,
        frozen_identity,
        vec![(table_facts, object_facts)],
        error,
    )
}

/// A2 (#2524): the multi-output orphan arm — the outcome names EVERY object
/// published before the failure so reclaim accounting sees the full set.
fn published_not_installed_flush_outputs(
    request: &FlushFrozenRequest,
    frozen_index: usize,
    frozen_identity: usize,
    published: Vec<(crate::table::TableRuntimeFacts, TableObjectFacts)>,
    error: LifecycleError,
) -> PreparedDurableFlush {
    let outcome = FlushFrozenOutcome::published_not_installed_outcome(
        request,
        frozen_index,
        published,
        error,
    );
    PreparedDurableFlush {
        request: request.clone(),
        frozen_index,
        frozen_identity,
        outputs: Err(outcome),
    }
}

pub(crate) fn table_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::TableRuntime,
        "table runtime failed",
        error,
    )
}

fn table_service_error(error: TableObjectServiceError) -> LifecycleError {
    if let TableObjectServiceError::Publish { source, .. } = &error {
        if matches!(
            source.kind(),
            PublishFailureKind::VisibilityUnknown
                | PublishFailureKind::VisibleDurabilityUnconfirmed
        ) {
            return LifecycleError::flush_publication_uncertain_with(
                table_publish_reason(source),
                error,
            );
        }
    }
    let reason = match &error {
        TableObjectServiceError::Layout { .. } => "table object layout failed",
        TableObjectServiceError::List { .. } => "table object list failed",
        TableObjectServiceError::Metadata { .. } => "table object metadata failed",
        TableObjectServiceError::Decode { .. } => "table object decode failed",
        TableObjectServiceError::Publish { source, .. } => table_publish_reason(source),
        TableObjectServiceError::InvalidPublishMetadata { .. } => {
            "table object publish metadata invalid"
        }
    };
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, reason, error)
}

fn table_publish_reason(error: &PublishError) -> &'static str {
    match error.kind() {
        PublishFailureKind::Unsupported => "table object publish unsupported",
        PublishFailureKind::PreconditionFailed => "table object already exists",
        PublishFailureKind::FailedBeforeVisibility => {
            "table object publish failed before visibility"
        }
        PublishFailureKind::VisibilityUnknown => "table object publish visibility unknown",
        PublishFailureKind::VisibleDurabilityUnconfirmed => {
            "table object publish durability unconfirmed"
        }
    }
}

fn published_not_installed_retryable(error: &LifecycleError) -> bool {
    match error {
        LifecycleError::FlushPublicationUncertain { .. } => true,
        LifecycleError::FlushPublicationOrphaned {
            source: Some(source),
            ..
        } => source
            .downcast_ref::<LifecycleError>()
            .is_some_and(published_not_installed_retryable),
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service | LifecycleLowerLayer::Backend,
            reason,
            ..
        } => !matches!(
            *reason,
            "table object already exists" | "table object publish metadata invalid"
        ),
        _ => false,
    }
}

fn table_read_error(error: TableObjectReadError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "table object read failed",
        error,
    )
}

fn branch_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::BranchRuntime,
        "branch runtime failed",
        error,
    )
}
