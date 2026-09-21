//! Branch-local state and descriptor shells.

use std::sync::Arc;

use super::config::BranchRuntimeConfig;
use super::error::{BranchRuntimeError, BranchRuntimeResult};
use super::facts::{BranchLevel, BranchReachabilitySnapshot, BranchTableRef, InheritedLayerStatus};
use super::read::{
    for_each_reader_row, require_table_physical_first_key, table_physical_ranges_overlap,
    try_for_each_reader_row, BranchInheritedLayer, BranchLayout, BranchOwnedTable,
    BranchTimestampCoverage,
};
use crate::observability::perf_trace;
use crate::row::StorageRow;
use crate::table::{
    FrozenTable, MutableTable, TableCursor, TableIdentity, TableInternalKeyBytes,
    TablePhysicalKeyBytes, TableRuntimeError,
};
use strata_core::{BranchId, CommitVersion, Timestamp};

pub(crate) mod append;
pub(crate) mod compaction;
pub(crate) mod fork;
pub(crate) mod manifest_recovery;
pub(crate) mod materialization;
pub(crate) mod read_hooks;
pub(crate) mod rotation;
pub(crate) mod snapshot;

pub(crate) use rotation::BranchRotationOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchImmutableInstallOutcome {
    branch_id: BranchId,
    level: BranchLevel,
    table_index: usize,
    level_table_count: usize,
    owned_table_count: usize,
    replaced_frozen_index: Option<usize>,
}

impl BranchImmutableInstallOutcome {
    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn level(self) -> BranchLevel {
        self.level
    }

    pub(crate) const fn table_index(self) -> usize {
        self.table_index
    }

    pub(crate) const fn level_table_count(self) -> usize {
        self.level_table_count
    }

    pub(crate) const fn owned_table_count(self) -> usize {
        self.owned_table_count
    }

    pub(crate) const fn replaced_frozen_index(self) -> Option<usize> {
        self.replaced_frozen_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchLocalState {
    branch_id: BranchId,
    config: BranchRuntimeConfig,
    active: MutableTable,
    frozen: Vec<FrozenTable>,
    layout: Arc<BranchLayout>,
    compact_pointers: Vec<Option<TablePhysicalKeyBytes>>,
    inherited_layers: Vec<BranchInheritedLayer>,
    max_commit_version: Option<CommitVersion>,
    timestamp_min: Option<Timestamp>,
    timestamp_max: Option<Timestamp>,
    timestamp_coverage: BranchTimestampCoverage,
    put_rows: u64,
    tombstone_rows: u64,
    shape: BranchShapeAggregates,
    /// W3.1a: the retained commit-timeline index — observed at row apply,
    /// shared into read views. Starts unseeded (lookups fall back to the
    /// timeline-space scan, which seeds it).
    retained_timeline: Arc<crate::timeline_index::RetainedCommitTimeline>,
    /// #3502 Slice A: the retained-history version floor published by MVCC
    /// version pruning. `None` = unbounded (nothing pruned yet). Shared into
    /// read views so an `as_of` below `max(timeline.min_version, this)` raises
    /// `RetainedHistoryUnavailable` rather than returning a below-floor
    /// survivor.
    retained_history_floor: Option<CommitVersion>,
}

impl BranchLocalState {
    pub(crate) fn new(
        branch_id: BranchId,
        config: BranchRuntimeConfig,
    ) -> BranchRuntimeResult<Self> {
        config.validate()?;
        Ok(Self {
            branch_id,
            config,
            active: MutableTable::new(),
            frozen: Vec::new(),
            layout: Arc::new(BranchLayout::with_level_count(config.max_level_count())),
            compact_pointers: vec![None; config.max_level_count()],
            inherited_layers: Vec::new(),
            max_commit_version: None,
            timestamp_min: None,
            timestamp_max: None,
            timestamp_coverage: BranchTimestampCoverage::unknown(),
            put_rows: 0,
            tombstone_rows: 0,
            shape: BranchShapeAggregates::empty(config.max_level_count()),
            retained_timeline: crate::timeline_index::RetainedCommitTimeline::new(),
            retained_history_floor: None,
        })
    }

    pub(crate) fn empty(branch_id: BranchId) -> Self {
        Self::new(branch_id, BranchRuntimeConfig::default())
            .expect("default branch-local state configuration is valid")
    }

    /// Cheap O(1) clone of the branch's immutable durable layout snapshot. The
    /// flush-watermark coverage scan captures this under a brief runtime lock, then
    /// runs the O(rows) scan on the owned `Arc` off-lock, so the lock hold stops
    /// scaling with dataset size (D.2b). The snapshot is immutable: a
    /// concurrent install under the lock reassigns the branch's `Arc` (via
    /// `Arc::make_mut`), leaving this clone untouched.
    /// W3.1a: the branch's shared retained-timeline index handle.
    pub(crate) fn retained_timeline(&self) -> &Arc<crate::timeline_index::RetainedCommitTimeline> {
        &self.retained_timeline
    }

    pub(crate) fn layout_snapshot(&self) -> Arc<BranchLayout> {
        Arc::clone(&self.layout)
    }

    pub(crate) fn compact_pointer(&self, level: BranchLevel) -> Option<&TablePhysicalKeyBytes> {
        self.compact_pointers
            .get(usize::from(level.raw()))
            .and_then(Option::as_ref)
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn set_compact_pointer_for_test(
        &mut self,
        level: BranchLevel,
        pointer: Option<TablePhysicalKeyBytes>,
    ) {
        if let Some(slot) = self.compact_pointers.get_mut(usize::from(level.raw())) {
            *slot = pointer;
        }
    }

    pub(crate) fn reachability_snapshot(&self) -> BranchRuntimeResult<BranchReachabilitySnapshot> {
        let mut table_refs = Vec::new();
        for tables in self.owned_levels() {
            for (table_index, table) in tables.iter().enumerate() {
                let table_ref = if let Some(materialization_source) = table.materialization_source()
                {
                    BranchTableRef::replacement(
                        self.branch_id,
                        materialization_source.source_branch_id(),
                        materialization_source.fork_version(),
                        table.level(),
                        table_index,
                        table.descriptor().identity().clone(),
                    )?
                } else {
                    BranchTableRef::owned(
                        self.branch_id,
                        table.level(),
                        table_index,
                        table.descriptor().identity().clone(),
                    )?
                };
                table_refs.push(table_ref);
            }
        }
        for (layer_index, layer) in self.inherited_layers.iter().enumerate() {
            let reference_kind = match layer.status() {
                InheritedLayerStatus::Active => InheritedReferenceKind::Active,
                InheritedLayerStatus::Materializing => InheritedReferenceKind::Materializing,
                InheritedLayerStatus::Materialized => continue,
                InheritedLayerStatus::Unavailable => {
                    return Err(BranchRuntimeError::InvalidInheritedLayer {
                        reason: "unavailable inherited layers cannot emit reachability",
                    });
                }
            };
            for tables in layer.owned_levels() {
                for (table_index, table) in tables.iter().enumerate() {
                    let table_ref = match reference_kind {
                        InheritedReferenceKind::Active => BranchTableRef::inherited(
                            self.branch_id,
                            layer.source_branch_id(),
                            layer.fork_version(),
                            layer_index,
                            table.level(),
                            table_index,
                            table.descriptor().identity().clone(),
                        )?,
                        InheritedReferenceKind::Materializing => {
                            BranchTableRef::materializing_source(
                                self.branch_id,
                                layer.source_branch_id(),
                                layer.fork_version(),
                                layer_index,
                                table.level(),
                                table_index,
                                table.descriptor().identity().clone(),
                            )?
                        }
                    };
                    table_refs.push(table_ref);
                }
            }
        }
        BranchReachabilitySnapshot::new(self.branch_id, table_refs)
    }

    pub(crate) fn install_l0_table(
        &mut self,
        table: BranchOwnedTable,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        self.install_owned_table_at_level(BranchLevel::ZERO, table)
    }

    pub(crate) fn install_owned_table_at_level(
        &mut self,
        level: BranchLevel,
        table: BranchOwnedTable,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        let level_index = self.validate_install(level, &table)?;
        let table_index = if level == BranchLevel::ZERO {
            Arc::make_mut(&mut self.layout).levels_mut()[level_index].insert(0, table);
            0
        } else {
            insert_sorted_by_range(
                &mut Arc::make_mut(&mut self.layout).levels_mut()[level_index],
                table,
            )?
        };
        self.refresh_observed_row_facts();
        Ok(self.install_outcome(level, table_index, None))
    }

    pub(crate) fn replace_frozen_with_l0_table(
        &mut self,
        frozen_index: usize,
        table: BranchOwnedTable,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        self.replace_frozen_with_level_zero_table(frozen_index, table)
    }

    pub(crate) fn replace_frozen_with_level_zero_table(
        &mut self,
        frozen_index: usize,
        table: BranchOwnedTable,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        let Some(replacement_index) = self.matching_frozen_replacement_index(frozen_index, &table)
        else {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "frozen replacement table rows must match a frozen table",
            });
        };
        self.install_level_zero_replacement(replacement_index, table)
    }

    /// O(1)-identity flush install (BS5.3b): the prepared durable flush captured the `Arc`
    /// identity of the sealed memtable its build consumed, so matching by identity proves
    /// "the same frozen table" strictly more precisely than the row-by-row walk in
    /// [`replace_frozen_with_level_zero_table`] — which ran under the runtime lock at
    /// ~7.5 ms per install (measured), the dominant remaining flush publish-lock cost
    /// after BS5.3a. Row equality of the BUILT tables against that same sealed input is
    /// verified off-lock in the prepare phase; a cheap row-count cross-check stays here.
    ///
    /// A1 (#2524): installs the frozen memtable's N key-disjoint output tables
    /// atomically — every table is validated BEFORE any mutation, so an
    /// identity collision or range violation on table k leaves the branch
    /// untouched. L0 legality: key-disjoint same-flush siblings cannot shadow
    /// each other, and L0 permits overlapping tables regardless.
    pub(crate) fn replace_frozen_with_level_zero_tables_by_identity(
        &mut self,
        frozen_identity: usize,
        tables: Vec<BranchOwnedTable>,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        if tables.is_empty() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "frozen replacement requires at least one table",
            });
        }
        let Some(replacement_index) = self
            .frozen
            .iter()
            .position(|frozen| frozen.memory_state_identity() == frozen_identity)
        else {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "frozen replacement identity must match a frozen table",
            });
        };
        let total_rows: u64 = tables.iter().map(|table| table.facts().row_count()).sum();
        if self.frozen[replacement_index].len() as u64 != total_rows {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "frozen replacement table rows must match a frozen table",
            });
        }
        // Validate ALL tables (identity uniqueness against the branch AND
        // within this batch) before mutating anything.
        let mut level_index = 0;
        for (offset, table) in tables.iter().enumerate() {
            level_index = self.validate_install_identity_and_range(BranchLevel::ZERO, table)?;
            if tables[..offset]
                .iter()
                .any(|earlier| earlier.descriptor().identity() == table.descriptor().identity())
            {
                return Err(BranchRuntimeError::InvalidBranchState {
                    reason: "frozen replacement tables must have distinct identities",
                });
            }
        }
        let new_l0_resident: u64 = tables
            .iter()
            .map(BranchOwnedTable::approximate_size_bytes)
            .sum();
        let new_l0_logical: u64 = tables.iter().map(|table| table.facts().byte_count()).sum();
        let installed_tables = tables.len();
        let removed_frozen_resident =
            u64::try_from(self.frozen[replacement_index].approximate_size_bytes())
                .unwrap_or(u64::MAX);
        // Insert in reverse so the outputs land at L0 indices 0..N in key
        // order (L0 is newest-first; same-flush siblings are key-disjoint,
        // so their relative order cannot affect shadowing).
        let levels = Arc::make_mut(&mut self.layout).levels_mut();
        for table in tables.into_iter().rev() {
            levels[level_index].insert(0, table);
        }
        self.frozen.remove(replacement_index);
        self.apply_flush_install_shape_delta(
            level_index,
            new_l0_resident,
            new_l0_logical,
            installed_tables,
            removed_frozen_resident,
        );
        Ok(self.install_outcome(BranchLevel::ZERO, 0, Some(replacement_index)))
    }

    fn install_level_zero_replacement(
        &mut self,
        replacement_index: usize,
        table: BranchOwnedTable,
    ) -> BranchRuntimeResult<BranchImmutableInstallOutcome> {
        let level_index = self.validate_install_identity_and_range(BranchLevel::ZERO, &table)?;

        // A flush moves the same rows from a frozen table into a new L0 table, so the
        // observed-row facts are unchanged and need no full rescan. This is the hot
        // flush-install path; the rescan here was the dominant publish-lock cost (a
        // full resident-dataset scan) before per-table summaries. Only the table-shape
        // aggregates shift (frozen -1 table, L0 +1 table), applied as an incremental
        // delta below. Capture the sizes before `table` is moved into the layout and
        // the frozen table is removed.
        let new_l0_resident = table.approximate_size_bytes();
        let new_l0_logical = table.facts().byte_count();
        let removed_frozen_resident =
            u64::try_from(self.frozen[replacement_index].approximate_size_bytes())
                .unwrap_or(u64::MAX);

        Arc::make_mut(&mut self.layout).levels_mut()[level_index].insert(0, table);
        self.frozen.remove(replacement_index);
        self.apply_flush_install_shape_delta(
            level_index,
            new_l0_resident,
            new_l0_logical,
            1,
            removed_frozen_resident,
        );
        Ok(self.install_outcome(BranchLevel::ZERO, 0, Some(replacement_index)))
    }

    fn matching_frozen_replacement_index(
        &self,
        preferred_index: usize,
        table: &BranchOwnedTable,
    ) -> Option<usize> {
        let _ = self.frozen.get(preferred_index)?;
        if self
            .frozen
            .get(preferred_index)
            .is_some_and(|frozen| frozen_rows_match_table(table, frozen))
        {
            return Some(preferred_index);
        }
        self.frozen
            .iter()
            .enumerate()
            .find_map(|(index, frozen)| frozen_rows_match_table(table, frozen).then_some(index))
    }

    fn require_absent_internal_key(&self, key: &TableInternalKeyBytes) -> BranchRuntimeResult<()> {
        if self.contains_internal_key(key)? {
            return Err(BranchRuntimeError::TableRuntime {
                source: TableRuntimeError::DuplicateInternalKey {
                    key: key.as_slice().to_vec(),
                },
            });
        }
        Ok(())
    }

    /// Whether `key` is present anywhere in this branch's local stack — active
    /// memtable, frozen memtables, or owned tables. Inherited layers are NOT
    /// probed: installs into branch-local state may legitimately carry rows
    /// that also exist in a layer (inheritance materialization copies them).
    pub(crate) fn contains_internal_key(
        &self,
        key: &TableInternalKeyBytes,
    ) -> BranchRuntimeResult<bool> {
        perf_trace::record_append_absent_internal_key_check();
        if self.active.get(key).is_some()
            || self.frozen.iter().any(|table| table.get(key).is_some())
        {
            return Ok(true);
        }
        // BS4.4d: the owned-side probe is fallible (a lazy reader can fail its targeted read), so thread
        // the error out of the `.any()` via an explicit loop.
        for table in self.owned_levels().iter().flatten() {
            if table
                .reader()
                .try_get_exact(key)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn validate_install(
        &self,
        level: BranchLevel,
        table: &BranchOwnedTable,
    ) -> BranchRuntimeResult<usize> {
        let level_index = self.validate_install_identity_and_range(level, table)?;
        try_for_each_reader_row(table.reader(), |row| {
            self.require_absent_internal_key(row.key())
        })?;
        Ok(level_index)
    }

    fn validate_install_identity_and_range(
        &self,
        level: BranchLevel,
        table: &BranchOwnedTable,
    ) -> BranchRuntimeResult<usize> {
        if table.branch_id() != self.branch_id {
            return Err(BranchRuntimeError::InvalidBranchRow {
                reason: "branch-owned table branch id must match branch state",
            });
        }
        if table.level() != level {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table level must match install level",
            });
        }
        let level_index = usize::from(level.raw());
        if level_index >= self.owned_levels().len() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table level is outside configured level count",
            });
        }
        if branch_reachable_table_identity_exists(
            table.descriptor().identity(),
            self.owned_levels(),
            &self.inherited_layers,
        ) {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table identity must not collide with reachable table",
            });
        }
        if level != BranchLevel::ZERO {
            self.require_non_overlapping_level(level_index, table)?;
        }
        Ok(level_index)
    }

    fn require_non_overlapping_level(
        &self,
        level_index: usize,
        table: &BranchOwnedTable,
    ) -> BranchRuntimeResult<()> {
        if self.owned_levels()[level_index]
            .iter()
            .any(|existing| table_physical_ranges_overlap(existing, table))
        {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned nonzero level tables must not overlap by physical key range",
            });
        }
        Ok(())
    }

    fn install_outcome(
        &self,
        level: BranchLevel,
        table_index: usize,
        replaced_frozen_index: Option<usize>,
    ) -> BranchImmutableInstallOutcome {
        let level_index = usize::from(level.raw());
        BranchImmutableInstallOutcome {
            branch_id: self.branch_id,
            level,
            table_index,
            level_table_count: self.owned_levels()[level_index].len(),
            owned_table_count: self.owned_table_count(),
            replaced_frozen_index,
        }
    }

    fn track_committed_row(
        &mut self,
        commit_version: CommitVersion,
        commit_timestamp: Timestamp,
        is_tombstone: bool,
    ) {
        self.max_commit_version = Some(
            self.max_commit_version
                .map_or(commit_version, |max| max.max(commit_version)),
        );
        self.timestamp_min = Some(
            self.timestamp_min
                .map_or(commit_timestamp, |min| min.min(commit_timestamp)),
        );
        self.timestamp_max = Some(
            self.timestamp_max
                .map_or(commit_timestamp, |max| max.max(commit_timestamp)),
        );
        if is_tombstone {
            self.tombstone_rows = self.tombstone_rows.saturating_add(1);
        } else {
            self.put_rows = self.put_rows.saturating_add(1);
        }
    }

    fn refresh_observed_row_facts(&mut self) {
        let observed = self.observe_rows_from_summaries();
        // The cached fold must stay bit-exactly equal to a full row scan: these
        // facts are asserted equal to a rescan during read-view recovery
        // validation and feed read bounds. The scan oracle runs in debug builds.
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            observed,
            self.observe_rows(),
            "cached observed-row facts diverged from a full row scan"
        );
        self.max_commit_version = observed.max_commit_version;
        self.timestamp_min = observed.timestamp_min;
        self.timestamp_max = observed.timestamp_max;
        self.put_rows = observed.put_rows;
        self.tombstone_rows = observed.tombstone_rows;
        // Structural mutations route through this hook, so recomputing the table-shape
        // aggregates here (O(tables), at event cadence) keeps them correct without a
        // per-commit fold. The two hot-path mutators that skip this hook (rotation,
        // flush install) apply incremental deltas instead.
        self.shape = self.recompute_shape_aggregates();
    }

    /// Aggregate observed-row facts from cached per-table summaries instead of a
    /// full row scan. Active and frozen rows (bounded by the rotation threshold)
    /// are still scanned, but owned-level and inherited tables fold their
    /// sealed-time `TableSummaryExtras` — O(tables), not O(resident rows). Equal
    /// to `observe_rows` by construction.
    fn observe_rows_from_summaries(&self) -> ObservedBranchRows {
        let mut observed = ObservedBranchRows::default();
        for row in self.active.iter() {
            observed.record(row.row());
        }
        for table in &self.frozen {
            for row in table.iter() {
                observed.record(row.row());
            }
        }
        for table in self.owned_levels().iter().flatten() {
            observed.record_owned_table(table);
        }
        for layer in &self.inherited_layers {
            observed.record_inherited_layer_summary(layer);
        }
        observed
    }

    fn observe_own_rows(&self) -> ObservedBranchRows {
        let mut observed = ObservedBranchRows::default();
        for row in self.active.iter() {
            observed.record(row.row());
        }
        for table in &self.frozen {
            for row in table.iter() {
                observed.record(row.row());
            }
        }
        for table in self.owned_levels().iter().flatten() {
            for_each_reader_row(table.reader(), |row| observed.record(row.row()));
        }
        observed
    }

    fn observe_rows(&self) -> ObservedBranchRows {
        let mut observed = self.observe_own_rows();
        for layer in &self.inherited_layers {
            observed.record_inherited_layer(layer);
        }
        observed
    }

    /// Historical-fork COW gate: does any not-yet-sealed row (in the active memtable or a frozen table)
    /// carry a commit version at or below `fork_version`? When `false`, every `<= fork_version` row is
    /// already in a sealed owned table, so a copy-on-write inherited layer over the owned tables alone
    /// represents the as-of-`fork_version` view; when `true`, the fork must fall back to materialization
    /// (an inherited layer cannot reference unsealed rows). The scan is bounded — active + frozen are
    /// capped by the rotation threshold, not O(dataset).
    pub(crate) fn has_in_fork_unsealed_rows(&self, fork_version: CommitVersion) -> bool {
        let in_fork = |version: CommitVersion| version.as_u64() <= fork_version.as_u64();
        self.active
            .iter()
            .any(|row| in_fork(row.row().commit_version()))
            || self
                .frozen
                .iter()
                .any(|table| table.iter().any(|row| in_fork(row.row().commit_version())))
    }

    /// Recompute the cached table-shape aggregates from the branch's tables. Called
    /// at the `refresh_observed_row_facts` hook (every structural mutation) and by the
    /// debug oracle. Reads raw fields directly — never the cached accessors — so it is
    /// the independent reference the oracle checks against.
    fn recompute_shape_aggregates(&self) -> BranchShapeAggregates {
        let levels = self.owned_levels();
        let mut per_level_bytes = vec![0u64; levels.len()];
        let mut owned_bytes = 0u64;
        let mut owned_tables = 0usize;
        for (level_index, tables) in levels.iter().enumerate() {
            let mut level_logical = 0u64;
            for table in tables {
                level_logical = level_logical.saturating_add(table.facts().byte_count());
                owned_bytes = owned_bytes.saturating_add(table.approximate_size_bytes());
                owned_tables += 1;
            }
            per_level_bytes[level_index] = level_logical;
        }
        let frozen_bytes = self.frozen.iter().fold(0u64, |total, table| {
            total.saturating_add(u64::try_from(table.approximate_size_bytes()).unwrap_or(u64::MAX))
        });
        let inherited_tables = self
            .inherited_layers
            .iter()
            .map(BranchInheritedLayer::table_count)
            .sum();
        BranchShapeAggregates {
            per_level_bytes,
            owned_bytes,
            owned_tables,
            frozen_bytes,
            inherited_tables,
        }
    }

    /// Incremental shape update for the hot flush-install path (frozen table -> new L0
    /// table). Uses resident bytes for `owned_bytes`/`frozen_bytes` and logical bytes
    /// for `per_level_bytes`, matching `recompute_shape_aggregates` field-for-field.
    fn apply_flush_install_shape_delta(
        &mut self,
        level_index: usize,
        new_l0_resident: u64,
        new_l0_logical: u64,
        installed_tables: usize,
        removed_frozen_resident: u64,
    ) {
        self.shape.owned_bytes = self.shape.owned_bytes.saturating_add(new_l0_resident);
        if let Some(level) = self.shape.per_level_bytes.get_mut(level_index) {
            *level = level.saturating_add(new_l0_logical);
        }
        self.shape.owned_tables = self.shape.owned_tables.saturating_add(installed_tables);
        self.shape.frozen_bytes = self
            .shape
            .frozen_bytes
            .saturating_sub(removed_frozen_resident);
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
    }

    /// Debug oracle: the cached shape aggregates must equal a fresh fold. Called from
    /// every cached-shape accessor and after each incremental delta, so the whole test
    /// suite (which exercises every structural mutation) detects a stale cache. Compiled
    /// out of release builds.
    #[cfg(debug_assertions)]
    fn debug_assert_shape_consistent(&self) {
        debug_assert_eq!(
            self.shape,
            self.recompute_shape_aggregates(),
            "branch shape aggregate cache diverged from a fresh fold"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InheritedReferenceKind {
    Active,
    Materializing,
}

/// Cached table-shape aggregates for `BranchLocalState`, maintained at structural
/// mutation cadence (the `refresh_observed_row_facts` hook plus the rotation and
/// flush-install deltas) so per-commit pressure / budget / scoring reads are O(1)
/// instead of folding over every owned table. A pure function of the branch's tables;
/// the debug oracle `debug_assert_shape_consistent` proves it against a fresh fold.
///
/// Two distinct byte sources are cached and must not be crossed: `owned_bytes` and
/// `frozen_bytes` are *resident* bytes (`approximate_size_bytes`, the in-RAM footprint
/// for the memory budget), while `per_level_bytes` is *logical* bytes
/// (`facts().byte_count()`, what compaction scoring sums). `sum(per_level_bytes)` does
/// not equal `owned_bytes` in general.
#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchShapeAggregates {
    per_level_bytes: Vec<u64>,
    owned_bytes: u64,
    owned_tables: usize,
    frozen_bytes: u64,
    inherited_tables: usize,
}

impl BranchShapeAggregates {
    fn empty(level_count: usize) -> Self {
        Self {
            per_level_bytes: vec![0u64; level_count],
            owned_bytes: 0,
            owned_tables: 0,
            frozen_bytes: 0,
            inherited_tables: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ObservedBranchRows {
    max_commit_version: Option<CommitVersion>,
    timestamp_min: Option<Timestamp>,
    timestamp_max: Option<Timestamp>,
    put_rows: u64,
    tombstone_rows: u64,
}

impl ObservedBranchRows {
    fn record(&mut self, row: &StorageRow) {
        self.record_commit_version(row.commit_version());
        self.record_timestamp(row.commit_timestamp());
        if row.is_tombstone() {
            self.tombstone_rows = self.tombstone_rows.saturating_add(1);
        } else {
            self.put_rows = self.put_rows.saturating_add(1);
        }
    }

    fn record_inherited_layer(&mut self, layer: &BranchInheritedLayer) {
        if layer.status() == InheritedLayerStatus::Materialized {
            return;
        }
        let fork_version = layer.fork_version();
        for table in layer.owned_levels().iter().flatten() {
            for_each_reader_row(table.reader(), |row| {
                if row.commit_version().as_u64() <= fork_version.as_u64() {
                    self.record_commit_version(row.commit_version());
                    self.record_timestamp(row.commit_timestamp());
                }
            });
        }
    }

    fn record_commit_version(&mut self, commit_version: CommitVersion) {
        self.max_commit_version = Some(
            self.max_commit_version
                .map_or(commit_version, |max| max.max(commit_version)),
        );
    }

    fn record_timestamp(&mut self, commit_timestamp: Timestamp) {
        self.timestamp_min = Some(
            self.timestamp_min
                .map_or(commit_timestamp, |min| min.min(commit_timestamp)),
        );
        self.timestamp_max = Some(
            self.timestamp_max
                .map_or(commit_timestamp, |max| max.max(commit_timestamp)),
        );
    }

    /// Fold a sealed owned table's cached summary into the observed facts without
    /// a per-row scan. Equivalent to recording every row of the table:
    /// `commit_range().max()` is the table's max commit version, and folding the
    /// cached timestamp min and max reproduces a scan of every row's timestamp.
    fn record_owned_table(&mut self, table: &BranchOwnedTable) {
        self.record_commit_version(table.facts().commit_range().max());
        let extras = table.extras();
        if let Some(timestamp) = extras.timestamp_min() {
            self.record_timestamp(timestamp);
        }
        if let Some(timestamp) = extras.timestamp_max() {
            self.record_timestamp(timestamp);
        }
        self.put_rows = self.put_rows.saturating_add(extras.put_rows());
        self.tombstone_rows = self.tombstone_rows.saturating_add(extras.tombstone_rows());
    }

    /// Fold a non-materialized inherited layer's contribution from cached
    /// per-table summaries. Inherited layers contribute version and timestamp
    /// extremes only (never put/tombstone counts), and only for rows at or
    /// before the fork version. `BranchInheritedLayer::new` forbids rows past the
    /// fork version, so a table whose commit range is within the cutoff folds
    /// directly from its summary; a table straddling the cutoff (only reachable
    /// via unchecked test construction) falls back to a per-row scan to stay
    /// exactly equal to `record_inherited_layer`.
    fn record_inherited_layer_summary(&mut self, layer: &BranchInheritedLayer) {
        if layer.status() == InheritedLayerStatus::Materialized {
            return;
        }
        // Historical-fork COW: a straddle layer folds its cached `<= fork_version` extremes in O(1)
        // instead of re-scanning the straddle tables on every recompute. The cached values equal
        // `record_inherited_layer`'s full scan, which the observed-facts debug oracle re-checks on
        // every structural mutation.
        if let Some(summary) = layer.straddle_read_view_summary() {
            if let Some(version) = summary.max_commit_version() {
                self.record_commit_version(version);
            }
            if let Some(timestamp) = summary.timestamp_min() {
                self.record_timestamp(timestamp);
            }
            if let Some(timestamp) = summary.timestamp_max() {
                self.record_timestamp(timestamp);
            }
            return;
        }
        // Non-straddle layer: every table sits entirely at/below the fork, fold from cached facts.
        for table in layer.owned_levels().iter().flatten() {
            self.record_commit_version(table.facts().commit_range().max());
            let extras = table.extras();
            if let Some(timestamp) = extras.timestamp_min() {
                self.record_timestamp(timestamp);
            }
            if let Some(timestamp) = extras.timestamp_max() {
                self.record_timestamp(timestamp);
            }
        }
    }
}

fn branch_reachable_table_identity_exists(
    identity: &TableIdentity,
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> bool {
    owned_levels
        .iter()
        .flatten()
        .any(|table| table.descriptor().identity() == identity)
        || inherited_layers
            .iter()
            .flat_map(BranchInheritedLayer::owned_levels)
            .flatten()
            .any(|table| table.descriptor().identity() == identity)
}

fn insert_sorted_by_range(
    tables: &mut Vec<BranchOwnedTable>,
    table: BranchOwnedTable,
) -> BranchRuntimeResult<usize> {
    let first_key = require_table_physical_first_key(&table)?;
    let mut index = 0;
    while index < tables.len() && require_table_physical_first_key(&tables[index])? < first_key {
        index += 1;
    }
    tables.insert(index, table);
    Ok(index)
}

/// Row-for-row equality of a built table (read back through its reader) against a sealed
/// memtable. BS5.3b moved the hot durable-flush call OFF the runtime lock into the prepare
/// phase; the install matches by memtable identity instead.
impl BranchLocalState {
    /// A2 (#2524): the encoded-physical-key spans and logical byte counts of
    /// the branch's level-1 tables, in key order — the flush zone-cut
    /// policy's skip map. Tables whose facts carry no key range (empty) are
    /// skipped. Encoded physical keys compare in logical key order (the
    /// escape encoding is order-preserving), so callers can compare these
    /// bounds directly against frozen rows' encoded physical keys.
    pub(crate) fn level_one_physical_spans(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        let Some(level_one) = self.owned_levels().get(1) else {
            return Vec::new();
        };
        level_one
            .iter()
            .filter_map(|table| {
                let range = table.facts().key_range();
                let (first, last) = (range.first_key(), range.last_key());
                if first.is_empty() || last.is_empty() {
                    return None;
                }
                Some((
                    TablePhysicalKeyBytes::from_encoded_internal_key(first)
                        .as_slice()
                        .to_vec(),
                    TablePhysicalKeyBytes::from_encoded_internal_key(last)
                        .as_slice()
                        .to_vec(),
                    table.facts().byte_count(),
                ))
            })
            .collect()
    }
}

/// A1 (#2524): do the tables' rows — concatenated in the given order —
/// partition the frozen memtable exactly? The key-disjoint flush outputs
/// arrive in key order, and frozen iteration is key-ordered, so sequential
/// lockstep over each table's cursor against the shared frozen iterator
/// checks disjointness, totality, and per-row equality in one pass. Any
/// shortfall, surplus, misorder, or cursor error fails closed.
pub(crate) fn frozen_rows_match_tables(tables: &[&BranchOwnedTable], frozen: &FrozenTable) -> bool {
    let total_rows: u64 = tables.iter().map(|table| table.facts().row_count()).sum();
    if total_rows != frozen.len() as u64 {
        return false;
    }
    let mut frozen_rows = frozen.iter();
    for table in tables {
        let mut cursor = table.reader().cursor();
        if cursor.seek_to_first().is_err() {
            return false;
        }
        while let Some(left) = cursor.current() {
            match frozen_rows.next() {
                Some(right) if left.row() == right.row() => {}
                _ => return false,
            }
            if cursor.advance().is_err() {
                return false;
            }
        }
    }
    frozen_rows.next().is_none()
}

pub(crate) fn frozen_rows_match_table(table: &BranchOwnedTable, frozen: &FrozenTable) -> bool {
    if table.facts().row_count() != frozen.len() as u64 {
        return false;
    }
    // BS4.4a-ii-a: row counts match, so walk the owned table's cursor (ascending internal-key order,
    // same as the frozen rows) in lockstep rather than materializing the whole table. A cursor error
    // (unreachable on today's eager readers) is treated as a mismatch.
    let mut cursor = table.reader().cursor();
    if cursor.seek_to_first().is_err() {
        return false;
    }
    for right in frozen.iter() {
        match cursor.current() {
            Some(left) if left.row() == right.row() => {}
            _ => return false,
        }
        if cursor.advance().is_err() {
            return false;
        }
    }
    cursor.current().is_none()
}
