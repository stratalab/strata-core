//! Read-facing facts, descriptors, and view capture for branch-local state.

use super::BranchLocalState;
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::error::{BranchRuntimeError, BranchRuntimeResult};
use crate::branch::facts::{BranchSourceLayout, BranchStateFacts};
use crate::branch::read::{
    inherited_table_count, source_layout_from_sources, try_for_each_reader_row,
    BranchInheritedLayer, BranchOwnedTable, BranchReadView, BranchTimestampCoverage,
};
use crate::observability::perf_trace;
use crate::table::{FrozenTable, MutableTable};
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchStateDescriptor {
    branch_id: BranchId,
    facts: BranchStateFacts,
}

impl BranchStateDescriptor {
    pub(crate) fn new(branch_id: BranchId, facts: BranchStateFacts) -> BranchRuntimeResult<Self> {
        if branch_id != facts.branch_id() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "state descriptor branch id must match branch facts",
            });
        }
        Ok(Self { branch_id, facts })
    }

    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn facts(self) -> BranchStateFacts {
        self.facts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchViewDescriptor {
    branch_id: BranchId,
    facts: BranchStateFacts,
}

impl BranchViewDescriptor {
    pub(crate) fn new(branch_id: BranchId, facts: BranchStateFacts) -> BranchRuntimeResult<Self> {
        if branch_id != facts.branch_id() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "view descriptor branch id must match branch facts",
            });
        }
        Ok(Self { branch_id, facts })
    }

    pub(crate) const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn facts(self) -> BranchStateFacts {
        self.facts
    }
}

impl BranchLocalState {
    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn config(&self) -> BranchRuntimeConfig {
        self.config
    }

    pub(crate) const fn active(&self) -> &MutableTable {
        &self.active
    }

    pub(crate) fn frozen(&self) -> &[FrozenTable] {
        &self.frozen
    }

    pub(crate) fn owned_levels(&self) -> &[Vec<BranchOwnedTable>] {
        self.layout.levels()
    }

    pub(crate) fn inherited_layers(&self) -> &[BranchInheritedLayer] {
        &self.inherited_layers
    }

    pub(crate) fn active_row_count(&self) -> usize {
        self.active.len()
    }

    pub(crate) fn active_byte_count(&self) -> u64 {
        u64::try_from(self.active.approximate_size_bytes()).unwrap_or(u64::MAX)
    }

    pub(crate) fn frozen_table_count(&self) -> usize {
        self.frozen.len()
    }

    pub(crate) fn frozen_byte_count(&self) -> u64 {
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
        self.shape.frozen_bytes
    }

    /// Approximate resident bytes held by this branch's durable owned tables. Whole-object readers
    /// materialize each table in memory, so this is the dominant steady-state read footprint and
    /// must be counted toward the database memory total. O(1): served from the cached shape
    /// aggregates, maintained at structural-mutation cadence.
    pub(crate) fn owned_table_byte_count(&self) -> u64 {
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
        self.shape.owned_bytes
    }

    /// Per-level logical byte totals (`facts().byte_count()`), index-aligned with
    /// `owned_levels()`. O(1): served from the cached shape aggregates. Consumed by
    /// compaction level-target scoring.
    pub(crate) fn per_level_bytes(&self) -> &[u64] {
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
        &self.shape.per_level_bytes
    }

    pub(crate) fn owned_table_count(&self) -> usize {
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
        self.shape.owned_tables
    }

    pub(crate) fn inherited_layer_count(&self) -> usize {
        self.inherited_layers.len()
    }

    pub(crate) fn inherited_table_count(&self) -> usize {
        #[cfg(debug_assertions)]
        self.debug_assert_shape_consistent();
        self.shape.inherited_tables
    }

    pub(crate) fn source_layout(&self) -> BranchSourceLayout {
        source_layout_from_sources(
            &self.active,
            &self.frozen,
            self.owned_levels(),
            &self.inherited_layers,
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
            && self.frozen.is_empty()
            && self.owned_table_count() == 0
            && self.inherited_layers.is_empty()
    }

    pub(crate) const fn max_commit_version(&self) -> Option<CommitVersion> {
        self.max_commit_version
    }

    /// Map a wall-clock timestamp to the highest commit version whose row was
    /// stamped at or before `timestamp`. Walks active, frozen, owned, and
    /// inherited rows (inherited rows are bounded by their layer's fork
    /// version). Returns `None` when no row qualifies.
    ///
    /// Tiebreaker: among rows with identical timestamps, the largest commit
    /// version wins - consistent with the rest of the storage timeline
    /// rule.
    pub(crate) fn resolve_timestamp_to_commit_version(
        &self,
        timestamp: Timestamp,
    ) -> BranchRuntimeResult<Option<CommitVersion>> {
        perf_trace::record_branch_timestamp_rows(source_row_counts(
            &self.active,
            &self.frozen,
            self.owned_levels(),
            &self.inherited_layers,
            |_| true,
        ));
        let mut best: Option<CommitVersion> = None;
        let mut consider = |version: CommitVersion| {
            best = match best {
                Some(current) if current.as_u64() >= version.as_u64() => Some(current),
                _ => Some(version),
            };
        };
        for row in self.active.iter() {
            if row.row().commit_timestamp() <= timestamp {
                consider(row.row().commit_version());
            }
        }
        for table in &self.frozen {
            for row in table.iter() {
                if row.row().commit_timestamp() <= timestamp {
                    consider(row.row().commit_version());
                }
            }
        }
        for table in self.owned_levels().iter().flatten() {
            let version = owned_timestamp_commit_version(table, timestamp)?;
            #[cfg(debug_assertions)]
            {
                let scan = owned_timestamp_commit_version_by_scan(table, timestamp)?;
                debug_assert_eq!(
                    version, scan,
                    "owned timestamp resolution diverged from a full row scan",
                );
            }
            if let Some(version) = version {
                consider(version);
            }
        }
        for layer in &self.inherited_layers {
            let fork_version = layer.fork_version();
            for table in layer.owned_levels().iter().flatten() {
                let version = inherited_timestamp_commit_version(table, timestamp, fork_version)?;
                #[cfg(debug_assertions)]
                {
                    let scan =
                        inherited_timestamp_commit_version_by_scan(table, timestamp, fork_version)?;
                    debug_assert_eq!(
                        version, scan,
                        "inherited timestamp resolution diverged from a full row scan",
                    );
                }
                if let Some(version) = version {
                    consider(version);
                }
            }
        }
        Ok(best)
    }

    pub(crate) const fn timestamp_min(&self) -> Option<Timestamp> {
        self.timestamp_min
    }

    pub(crate) const fn timestamp_max(&self) -> Option<Timestamp> {
        self.timestamp_max
    }

    pub(crate) const fn timestamp_coverage(&self) -> BranchTimestampCoverage {
        self.timestamp_coverage
    }

    pub(crate) fn set_timestamp_coverage(&mut self, coverage: BranchTimestampCoverage) {
        self.timestamp_coverage = coverage;
    }

    /// #3502 Slice A: set the retained-history version floor (shared into read
    /// views; `None` = unbounded). The capture paths read the field directly.
    pub(crate) fn set_retained_history_floor(&mut self, floor: Option<CommitVersion>) {
        self.retained_history_floor = floor;
    }

    pub(crate) const fn put_rows(&self) -> u64 {
        self.put_rows
    }

    pub(crate) const fn tombstone_rows(&self) -> u64 {
        self.tombstone_rows
    }

    pub(crate) fn facts(&self) -> BranchRuntimeResult<BranchStateFacts> {
        perf_trace::record_branch_facts_observed(0);
        perf_trace::record_branch_fact_source_rows(perf_trace::BranchSourceRowCounts::default());
        BranchStateFacts::new(
            self.branch_id,
            u64::try_from(self.active.len()).expect("active row count fits in u64"),
            self.frozen.len(),
            self.owned_table_count(),
            self.inherited_layers.len(),
            self.max_commit_version,
            self.timestamp_min,
            self.timestamp_max,
        )
    }

    pub(crate) fn capture_read_view(&self) -> BranchRuntimeResult<BranchReadView> {
        perf_trace::record_read_view_capture(read_view_source_handle_count(self), 0, 0);
        BranchReadView::new_from_validated_state(
            self.branch_id,
            self.active.clone_for_read_view(),
            self.frozen.clone(),
            self.owned_levels().to_vec(),
            self.inherited_layers.clone(),
            self.facts()?,
        )
        .map(|view| {
            view.with_timestamp_coverage(self.timestamp_coverage)
                .with_retained_timeline(Arc::clone(self.retained_timeline()), false)
                .with_retained_history_floor(self.retained_history_floor)
        })
    }

    /// Capture a snapshot for off-lock publication (BS2.4 Model 2): identical to
    /// [`capture_read_view`](Self::capture_read_view) but holds the **live** (unpinned) active
    /// handle, so the published snapshot sees commits appended to the shared memtable without a
    /// per-commit republish. Each off-lock read pins the active at read time. Called only under the
    /// runtime lock, where `active.len()` and `self.facts()` observe the same `inner`, so the
    /// construction validation (`facts.active_rows == active.len()`) holds at capture.
    pub(crate) fn capture_snapshot(&self) -> BranchRuntimeResult<BranchReadView> {
        perf_trace::record_read_view_capture(read_view_source_handle_count(self), 0, 0);
        BranchReadView::new_from_validated_state(
            self.branch_id,
            self.active.clone(),
            self.frozen.clone(),
            self.owned_levels().to_vec(),
            self.inherited_layers.clone(),
            self.facts()?,
        )
        .map(|view| {
            view.with_timestamp_coverage(self.timestamp_coverage)
                .with_retained_timeline(Arc::clone(self.retained_timeline()), true)
                .with_retained_history_floor(self.retained_history_floor)
        })
    }

    pub(crate) fn validate_read_view_sources(&self) -> BranchRuntimeResult<()> {
        BranchReadView::new_with_inherited(
            self.branch_id,
            self.active.clone_for_read_view(),
            self.frozen.clone(),
            self.owned_levels().to_vec(),
            self.inherited_layers.clone(),
            self.facts()?,
        )
        .map(|_| ())
    }
}

/// BS4.4c: the max commit version among an owned table's rows stamped at or before `timestamp`. The
/// table's timestamp extras skip the row scan when the whole table is on one side of the query; only a
/// table straddling the query timestamp is scanned (`by_scan`; BS4.4d cursor-izes that fallback).
fn owned_timestamp_commit_version(
    table: &BranchOwnedTable,
    timestamp: Timestamp,
) -> BranchRuntimeResult<Option<CommitVersion>> {
    let extras = table.extras();
    if extras.timestamp_max().is_some_and(|max| max <= timestamp) {
        Ok(Some(table.facts().commit_range().max()))
    } else if extras.timestamp_min().is_some_and(|min| min > timestamp) {
        Ok(None)
    } else {
        owned_timestamp_commit_version_by_scan(table, timestamp)
    }
}

fn owned_timestamp_commit_version_by_scan(
    table: &BranchOwnedTable,
    timestamp: Timestamp,
) -> BranchRuntimeResult<Option<CommitVersion>> {
    let mut best: Option<CommitVersion> = None;
    try_for_each_reader_row(table.reader(), |row| {
        if row.row().commit_timestamp() <= timestamp {
            let version = row.row().commit_version();
            best = Some(best.map_or(version, |current| current.max(version)));
        }
        Ok(())
    })?;
    Ok(best)
}

/// BS4.4c: like [`owned_timestamp_commit_version`] but for an inherited table, excluding rows past the
/// layer's fork. A validly-constructed layer has no rows past the fork, so the fork check is a no-op and
/// the owned fast path applies; the fork-straddle branch is reachable only via unchecked test construction.
fn inherited_timestamp_commit_version(
    table: &BranchOwnedTable,
    timestamp: Timestamp,
    fork_version: CommitVersion,
) -> BranchRuntimeResult<Option<CommitVersion>> {
    if table.facts().commit_range().max().as_u64() <= fork_version.as_u64() {
        owned_timestamp_commit_version(table, timestamp)
    } else {
        inherited_timestamp_commit_version_by_scan(table, timestamp, fork_version)
    }
}

fn inherited_timestamp_commit_version_by_scan(
    table: &BranchOwnedTable,
    timestamp: Timestamp,
    fork_version: CommitVersion,
) -> BranchRuntimeResult<Option<CommitVersion>> {
    let mut best: Option<CommitVersion> = None;
    try_for_each_reader_row(table.reader(), |row| {
        if row.row().commit_timestamp() <= timestamp
            && row.row().commit_version().as_u64() <= fork_version.as_u64()
        {
            let version = row.row().commit_version();
            best = Some(best.map_or(version, |current| current.max(version)));
        }
        Ok(())
    })?;
    Ok(best)
}

fn read_view_source_handle_count(state: &BranchLocalState) -> usize {
    1usize
        .saturating_add(state.frozen.len())
        .saturating_add(state.owned_table_count())
        .saturating_add(state.inherited_layers.len())
        .saturating_add(inherited_table_count(&state.inherited_layers))
}

fn source_row_counts(
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    include_inherited_layer: impl Fn(&BranchInheritedLayer) -> bool,
) -> perf_trace::BranchSourceRowCounts {
    let mut counts = perf_trace::BranchSourceRowCounts {
        active: active.len(),
        frozen: frozen.iter().map(FrozenTable::len).sum(),
        ..Default::default()
    };
    for (level_index, tables) in owned_levels.iter().enumerate() {
        for table in tables {
            if level_index == 0 {
                counts.owned_l0 = counts.owned_l0.saturating_add(table.row_count_usize());
            } else {
                counts.owned_nonzero = counts.owned_nonzero.saturating_add(table.row_count_usize());
            }
        }
    }
    for layer in inherited_layers {
        if !include_inherited_layer(layer) {
            continue;
        }
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            for table in tables {
                if level_index == 0 {
                    counts.inherited_l0 =
                        counts.inherited_l0.saturating_add(table.row_count_usize());
                } else {
                    counts.inherited_nonzero = counts
                        .inherited_nonzero
                        .saturating_add(table.row_count_usize());
                }
            }
        }
    }
    counts
}
