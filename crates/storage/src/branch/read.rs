//! Branch read-bound, selected-row, and own-branch read-view vocabulary.

use super::error::{BranchRuntimeError, BranchRuntimeResult, BranchTimestampHistorySource};
use super::facts::{
    BranchLevel, BranchLevelTableCount, BranchSourceLayout, BranchStateFacts,
    BranchTableDescriptor, InheritedLayerDescriptor, InheritedLayerStatus,
};
use super::identity::{rewrite_physical_key_branch, rewrite_row_branch};
use super::state::BranchLocalState;
use crate::observability::perf_trace;
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::table::{
    BoundedTableCursor, FrozenTable, ImmutableTableReader, MutableTable, TableCursor,
    TableInternalKeyBytes, TableKeyBounds, TablePhysicalKeyBound, TablePhysicalKeyBytes,
    TablePointLookupRow, TablePreparedPointLookup, TableRow, TableRuntimeFacts, TableSummaryExtras,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchReadBound {
    Latest,
    AtVersion(CommitVersion),
    AtTimestamp(Timestamp),
}

impl BranchReadBound {
    pub(crate) const fn latest() -> Self {
        Self::Latest
    }

    pub(crate) const fn at_version(version: CommitVersion) -> Self {
        Self::AtVersion(version)
    }

    pub(crate) const fn at_timestamp(timestamp: Timestamp) -> Self {
        Self::AtTimestamp(timestamp)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchEffectiveReadBound {
    max_commit_version: Option<CommitVersion>,
    max_commit_timestamp: Option<Timestamp>,
    min_commit_version: Option<CommitVersion>,
}

impl BranchEffectiveReadBound {
    pub(crate) const fn new(
        max_commit_version: Option<CommitVersion>,
        max_commit_timestamp: Option<Timestamp>,
    ) -> Self {
        Self {
            max_commit_version,
            max_commit_timestamp,
            min_commit_version: None,
        }
    }

    /// Add a generic lower bound: only rows whose commit version is strictly greater than
    /// `min_commit_version` are visible. Applied uniformly at selection, so a key whose newest
    /// visible version is at or below the watermark is dropped (it is covered elsewhere, e.g. by a
    /// sealed index artifact), while post-watermark writes — including tombstones — are kept.
    pub(crate) const fn with_min_commit_version(
        mut self,
        min_commit_version: Option<CommitVersion>,
    ) -> Self {
        self.min_commit_version = min_commit_version;
        self
    }

    pub(crate) const fn for_own_branch(bound: BranchReadBound) -> Self {
        match bound {
            BranchReadBound::Latest => Self::new(None, None),
            BranchReadBound::AtVersion(version) => Self::new(Some(version), None),
            BranchReadBound::AtTimestamp(timestamp) => Self::new(None, Some(timestamp)),
        }
    }

    /// Compute the effective read bound for a forked child reading rows
    /// from a parent's inherited layer. The fork inheritance contract is:
    ///
    /// - `Latest` and `AtVersion(version)` reads cap the version at
    ///   `fork_version`. The child sees parent rows up to and including
    ///   the fork point; parent post-fork commits are invisible.
    /// - `AtTimestamp(timestamp)` reads apply BOTH the `fork_version` cap
    ///   AND the timestamp filter. Per-row `commit_timestamp` drives
    ///   timestamp matching against parent's physical rows in the
    ///   inherited layer; no timeline transcription happens at fork
    ///   time and no parent-timeline lookup happens at read time.
    /// - For `T < fork_timestamp`, reads resolve through the parent's
    ///   inherited rows.
    /// - For `T >= fork_timestamp`, the version cap still bounds the
    ///   view at `fork_version`; the child's own (post-fork) commits
    ///   are read via `for_own_branch`, not this helper.
    ///
    /// The catalog does not transcribe parent timeline rows under the
    /// child's `branch_id` at fork, and the read path does not consult
    /// the parent's timeline metadata. Three pinning tests in
    /// `lifecycle/tests/branch_lifecycle/fork.rs` verify the contract:
    /// `forked_branch_at_timestamp_before_fork_returns_parent_row`,
    /// `forked_branch_at_timestamp_after_fork_returns_child_row`, and
    /// `forked_branch_isolated_from_parent_post_fork_commits`.
    pub(crate) const fn for_inherited_layer(
        bound: BranchReadBound,
        fork_version: CommitVersion,
    ) -> Self {
        match bound {
            BranchReadBound::Latest => Self::new(Some(fork_version), None),
            BranchReadBound::AtVersion(version) => {
                let effective_version = if version.as_u64() <= fork_version.as_u64() {
                    version
                } else {
                    fork_version
                };
                Self::new(Some(effective_version), None)
            }
            BranchReadBound::AtTimestamp(timestamp) => {
                Self::new(Some(fork_version), Some(timestamp))
            }
        }
    }

    pub(crate) const fn max_commit_version(self) -> Option<CommitVersion> {
        self.max_commit_version
    }

    pub(crate) const fn max_commit_timestamp(self) -> Option<Timestamp> {
        self.max_commit_timestamp
    }

    pub(crate) const fn row_version_in_bound(self, row: &StorageRow) -> bool {
        if let Some(version) = self.max_commit_version {
            if row.commit_version().as_u64() > version.as_u64() {
                return false;
            }
        }
        if let Some(min) = self.min_commit_version {
            if row.commit_version().as_u64() <= min.as_u64() {
                return false;
            }
        }
        true
    }

    pub(crate) const fn row_timestamp_in_bound(self, row: &StorageRow) -> bool {
        match self.max_commit_timestamp {
            Some(timestamp) => row.commit_timestamp().as_micros() <= timestamp.as_micros(),
            None => true,
        }
    }

    pub(crate) const fn matches_row(self, row: &StorageRow) -> bool {
        self.row_version_in_bound(row) && self.row_timestamp_in_bound(row)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchTimestampCoverage {
    Unknown,
    Complete,
    CompleteSince { earliest_timestamp: Timestamp },
}

impl BranchTimestampCoverage {
    pub(crate) const fn unknown() -> Self {
        Self::Unknown
    }

    pub(crate) const fn complete() -> Self {
        Self::Complete
    }

    pub(crate) const fn complete_since(earliest_timestamp: Timestamp) -> Self {
        Self::CompleteSince { earliest_timestamp }
    }

    fn require_timestamp(
        self,
        branch_id: BranchId,
        requested_timestamp: Timestamp,
        source: BranchTimestampHistorySource,
    ) -> BranchRuntimeResult<()> {
        if let Self::CompleteSince { earliest_timestamp } = self {
            if requested_timestamp < earliest_timestamp {
                Err(BranchRuntimeError::InsufficientTimestampHistory {
                    branch_id,
                    requested_timestamp,
                    earliest_available_timestamp: Some(earliest_timestamp),
                    source,
                })
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchRowSource {
    Active,
    Frozen {
        index: usize,
    },
    OwnedTable {
        level: BranchLevel,
        table_index: usize,
    },
    Inherited {
        source_branch_id: BranchId,
        layer_index: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchVisibleRow {
    row: StorageRow,
    source: BranchRowSource,
}

impl BranchVisibleRow {
    pub(crate) fn new(row: StorageRow, source: BranchRowSource) -> Self {
        Self { row, source }
    }

    pub(crate) const fn row(&self) -> &StorageRow {
        &self.row
    }

    /// B4: the owned exit — point reads move the row out instead of copying
    /// key and value at the api boundary.
    pub(crate) fn into_storage_row(self) -> StorageRow {
        self.row
    }

    pub(crate) const fn source(&self) -> BranchRowSource {
        self.source
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchHistoryRow {
    row: StorageRow,
    source: BranchRowSource,
}

impl BranchHistoryRow {
    pub(crate) fn new(row: StorageRow, source: BranchRowSource) -> Self {
        Self { row, source }
    }

    pub(crate) const fn row(&self) -> &StorageRow {
        &self.row
    }

    /// B4: the owned exit for point reads (see [`BranchVisibleRow`]).
    pub(crate) fn into_storage_row(self) -> StorageRow {
        self.row
    }

    pub(crate) const fn source(&self) -> BranchRowSource {
        self.source
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchImmutableRowSource {
    source_id: String,
    source_branch_id: BranchId,
    source_generation: CommitVersion,
    fork_version_cap: Option<CommitVersion>,
    rows: Vec<BranchHistoryRow>,
}

impl BranchImmutableRowSource {
    fn new(
        source_id: String,
        source_branch_id: BranchId,
        source_generation: CommitVersion,
        fork_version_cap: Option<CommitVersion>,
        rows: Vec<BranchHistoryRow>,
    ) -> Self {
        Self {
            source_id,
            source_branch_id,
            source_generation,
            fork_version_cap,
            rows,
        }
    }

    pub(crate) fn source_id(&self) -> &str {
        &self.source_id
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn source_generation(&self) -> CommitVersion {
        self.source_generation
    }

    pub(crate) const fn fork_version_cap(&self) -> Option<CommitVersion> {
        self.fork_version_cap
    }

    pub(crate) fn rows(&self) -> &[BranchHistoryRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BranchUserKeyBound {
    Unbounded,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

impl BranchUserKeyBound {
    pub(crate) fn included(user_key: impl Into<Vec<u8>>) -> Self {
        Self::Included(user_key.into())
    }

    pub(crate) fn excluded(user_key: impl Into<Vec<u8>>) -> Self {
        Self::Excluded(user_key.into())
    }

    fn finite(&self) -> Option<&[u8]> {
        match self {
            Self::Unbounded => None,
            Self::Included(user_key) | Self::Excluded(user_key) => Some(user_key),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchScanBounds {
    branch_id: BranchId,
    space: String,
    storage_space_id: StorageSpaceId,
    user_key_prefix: Option<Vec<u8>>,
    lower_user_key: BranchUserKeyBound,
    upper_user_key: BranchUserKeyBound,
}

impl BranchScanBounds {
    pub(crate) fn prefix(prefix: &PhysicalKey) -> Self {
        Self {
            branch_id: prefix.branch_id(),
            space: prefix.space().to_owned(),
            storage_space_id: prefix.storage_space_id(),
            user_key_prefix: Some(prefix.user_key().to_vec()),
            lower_user_key: BranchUserKeyBound::Unbounded,
            upper_user_key: BranchUserKeyBound::Unbounded,
        }
    }

    pub(crate) fn unbounded(
        branch_id: BranchId,
        space: impl Into<String>,
        storage_space_id: StorageSpaceId,
    ) -> BranchRuntimeResult<Self> {
        Self::range(
            branch_id,
            space,
            storage_space_id,
            BranchUserKeyBound::Unbounded,
            BranchUserKeyBound::Unbounded,
        )
    }

    pub(crate) fn range(
        branch_id: BranchId,
        space: impl Into<String>,
        storage_space_id: StorageSpaceId,
        lower_user_key: BranchUserKeyBound,
        upper_user_key: BranchUserKeyBound,
    ) -> BranchRuntimeResult<Self> {
        let space = space.into();
        validate_scan_space(branch_id, &space, storage_space_id)?;
        validate_user_key_bound_order(&lower_user_key, &upper_user_key)?;
        Ok(Self {
            branch_id,
            space,
            storage_space_id,
            user_key_prefix: None,
            lower_user_key,
            upper_user_key,
        })
    }

    pub(crate) fn closed(lower: &PhysicalKey, upper: &PhysicalKey) -> BranchRuntimeResult<Self> {
        Self::from_physical_bounds(
            lower,
            upper,
            BranchUserKeyBound::included(lower.user_key()),
            BranchUserKeyBound::included(upper.user_key()),
        )
    }

    pub(crate) fn open(lower: &PhysicalKey, upper: &PhysicalKey) -> BranchRuntimeResult<Self> {
        Self::from_physical_bounds(
            lower,
            upper,
            BranchUserKeyBound::excluded(lower.user_key()),
            BranchUserKeyBound::excluded(upper.user_key()),
        )
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    fn table_key_bounds(&self) -> BranchRuntimeResult<TableKeyBounds> {
        self.table_key_bounds_for_branch(self.branch_id)
    }

    fn table_key_bounds_for_branch(
        &self,
        branch_id: BranchId,
    ) -> BranchRuntimeResult<TableKeyBounds> {
        if let Some(prefix) = &self.user_key_prefix {
            let prefix_key = scan_physical_key(
                branch_id,
                &self.space,
                self.storage_space_id,
                prefix.clone(),
            )?;
            return Ok(TableKeyBounds::prefix(
                TablePhysicalKeyBytes::from_physical_key_prefix(&prefix_key)
                    .as_slice()
                    .to_vec(),
            ));
        }

        let namespace_key =
            scan_physical_key(branch_id, &self.space, self.storage_space_id, Vec::new())?;
        let namespace_prefix = TablePhysicalKeyBytes::from_physical_key_prefix(&namespace_key);
        let lower = table_physical_bound_for_user_key(
            branch_id,
            &self.space,
            self.storage_space_id,
            &self.lower_user_key,
        )?;
        let upper = table_physical_bound_for_user_key(
            branch_id,
            &self.space,
            self.storage_space_id,
            &self.upper_user_key,
        )?;
        TableKeyBounds::physical_range(&namespace_prefix, lower, upper).map_err(|_| {
            BranchRuntimeError::InvalidReadBound {
                reason: "scan table key bounds are invalid",
            }
        })
    }

    fn from_physical_bounds(
        lower: &PhysicalKey,
        upper: &PhysicalKey,
        lower_user_key: BranchUserKeyBound,
        upper_user_key: BranchUserKeyBound,
    ) -> BranchRuntimeResult<Self> {
        if lower.branch_id() != upper.branch_id()
            || lower.space() != upper.space()
            || lower.storage_space_id() != upper.storage_space_id()
        {
            return Err(BranchRuntimeError::InvalidReadBound {
                reason: "range bounds must target the same branch, space, and storage space",
            });
        }
        Self::range(
            lower.branch_id(),
            lower.space().to_owned(),
            lower.storage_space_id(),
            lower_user_key,
            upper_user_key,
        )
    }
}

/// Outcome of [`BranchReadView::classify_own_internal_row`]: whether the
/// branch's own sources hold a byte-equal copy of the probed internal row, a
/// same-version row with DIFFERENT content, or nothing at that version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BranchOwnRowMatch {
    Equal,
    SameVersionDiffers,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchHistoryOptions {
    before_version: Option<CommitVersion>,
    limit: Option<usize>,
    include_tombstones: bool,
}

impl BranchHistoryOptions {
    pub(crate) const fn all() -> Self {
        Self {
            before_version: None,
            limit: None,
            include_tombstones: true,
        }
    }

    pub(crate) const fn before_version(mut self, before_version: CommitVersion) -> Self {
        self.before_version = Some(before_version);
        self
    }

    pub(crate) const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) const fn include_tombstones(mut self, include_tombstones: bool) -> Self {
        self.include_tombstones = include_tombstones;
        self
    }

    pub(crate) const fn before_version_bound(self) -> Option<CommitVersion> {
        self.before_version
    }

    pub(crate) const fn limit_bound(self) -> Option<usize> {
        self.limit
    }

    pub(crate) const fn includes_tombstones(self) -> bool {
        self.include_tombstones
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchOwnedTable {
    branch_id: BranchId,
    descriptor: BranchTableDescriptor,
    reader: ImmutableTableReader<'static>,
    materialization_source: Option<BranchMaterializationSource>,
    extras: TableSummaryExtras,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchMaterializationSource {
    source_branch_id: BranchId,
    fork_version: CommitVersion,
}

impl BranchMaterializationSource {
    pub(crate) const fn new(source_branch_id: BranchId, fork_version: CommitVersion) -> Self {
        Self {
            source_branch_id,
            fork_version,
        }
    }

    pub(crate) const fn source_branch_id(self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn fork_version(self) -> CommitVersion {
        self.fork_version
    }
}

impl BranchOwnedTable {
    pub(crate) fn new(
        branch_id: BranchId,
        descriptor: BranchTableDescriptor,
        reader: ImmutableTableReader<'static>,
        extras: TableSummaryExtras,
    ) -> BranchRuntimeResult<Self> {
        Self::new_with_materialization_layer(branch_id, descriptor, reader, extras, None)
    }

    pub(crate) fn new_materialization_replacement(
        branch_id: BranchId,
        descriptor: BranchTableDescriptor,
        reader: ImmutableTableReader<'static>,
        extras: TableSummaryExtras,
        materialization_source: BranchMaterializationSource,
    ) -> BranchRuntimeResult<Self> {
        Self::new_with_materialization_layer(
            branch_id,
            descriptor,
            reader,
            extras,
            Some(materialization_source),
        )
    }

    fn new_with_materialization_layer(
        branch_id: BranchId,
        descriptor: BranchTableDescriptor,
        reader: ImmutableTableReader<'static>,
        extras: TableSummaryExtras,
        materialization_source: Option<BranchMaterializationSource>,
    ) -> BranchRuntimeResult<Self> {
        if descriptor.facts() != reader.facts() {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table descriptor facts must match reader facts",
            });
        }
        // BS4.4j: the reader is installed as-is. Durable flush/recovery readers are now lazy (disk-backed)
        // and held without decoding rows — the `into_materialized()` eager bridge is gone. The O(1) facts
        // checks below (and the debug oracle) validate the table without a full scan.
        if reader.facts().row_count() == 0 {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table must not be empty",
            });
        }
        // BS4.4e: branch id is the fixed leading 16 bytes of every internal key and rows are
        // internal-key-sorted, so both endpoints of the key range sharing the branch prefix proves every
        // row does — an O(1) facts check instead of an O(rows) scan (which would trip the guard when lazy).
        if !key_range_matches_branch(
            reader.facts().key_range().first_key(),
            reader.facts().key_range().last_key(),
            branch_id,
        ) {
            return Err(BranchRuntimeError::InvalidBranchRow {
                reason: "branch-owned table rows must match the target branch",
            });
        }
        // BS4.4f: `extras` is supplied by the caller — the build artifact's precomputed summary
        // (`BuiltTableArtifact::extras()`), a promoted table's cloned summary, or a recovery scan — instead
        // of the constructor re-materializing every row. BS4.4g: this debug cross-check reads via the
        // oracle hatch, which bypasses the BS4.4d guard, so it keeps validating the supplied summary once
        // durable readers go lazy (where a plain `rows()` would trip the guard).
        #[cfg(debug_assertions)]
        {
            let scanned_rows = reader
                .materialize_rows_for_oracle()
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            let scanned = TableSummaryExtras::from_rows(&scanned_rows)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            debug_assert_eq!(
                extras, scanned,
                "branch-owned table extras must match a full row scan"
            );
        }
        Ok(Self {
            branch_id,
            descriptor,
            reader,
            materialization_source,
            extras,
        })
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn descriptor(&self) -> &BranchTableDescriptor {
        &self.descriptor
    }

    pub(crate) const fn facts(&self) -> &TableRuntimeFacts {
        self.descriptor.facts()
    }

    /// BS4.4a-i: the table's row count as `usize`, without materializing rows. Saturating — a table's
    /// row count always fits `usize` (it equals a `usize` row length), so this never truncates.
    pub(crate) fn row_count_usize(&self) -> usize {
        usize::try_from(self.facts().row_count()).unwrap_or(usize::MAX)
    }

    pub(crate) const fn level(&self) -> BranchLevel {
        self.descriptor.level()
    }

    pub(crate) const fn materialization_source(&self) -> Option<BranchMaterializationSource> {
        self.materialization_source
    }

    pub(crate) fn rows(&self) -> &[TableRow] {
        self.reader.rows()
    }

    /// Materialize all rows for a **debug oracle** via the reader's oracle hatch, bypassing the
    /// BS4.4d guard. Panics on a materialization error (a corrupt-table bug), matching the old
    /// `rows()` — the callers are debug-only oracles cross-checking a facts-based fast path.
    pub(crate) fn materialize_rows_for_oracle(&self) -> Vec<TableRow> {
        self.reader
            .materialize_rows_for_oracle()
            .expect("branch-owned table oracle materialization failed")
    }

    pub(crate) const fn extras(&self) -> &TableSummaryExtras {
        &self.extras
    }

    pub(crate) fn reader(&self) -> &ImmutableTableReader<'static> {
        &self.reader
    }

    /// Approximate **resident** (in-RAM) bytes for this owned table — the value the branch's shape
    /// aggregates and memory accounting want. BS4.4g routes this through the reader's resident/object
    /// seam (`resident_size_bytes`); object/level size is sourced separately from `facts().byte_count()`.
    /// Today an eager reader holds the whole object, so resident == object; BS4.4h makes it drop for a
    /// lazy reader.
    pub(crate) fn approximate_size_bytes(&self) -> u64 {
        self.reader.resident_size_bytes()
    }
}

/// Immutable snapshot of a branch's own on-disk table levels (L0..Ln) — the unit that
/// flush / compaction / materialization install replaces. Introduced for Group D:
/// it becomes the `ArcSwap`-published layout so reads / compaction scoring / flush-watermark
/// coverage take no runtime lock. Per-table facts (commit/key ranges) live in each
/// `BranchOwnedTable`, so a reader holding a layout snapshot is self-consistent; branch-total
/// aggregate facts (max commit version, row counts, timestamp span) stay on `BranchLocalState`
/// because they also count the active/frozen memtable and are not layout-derived. In D.1 this
/// was a plain field with in-place mutation; D.2a makes the `BranchLocalState` field
/// `Arc<BranchLayout>` (plain `Arc`, not `ArcSwap`: installs are serialized under the runtime
/// mutex, so an install builds a new value via `Arc::make_mut` and off-lock readers hold a
/// cheap immutable snapshot). `ArcSwap` for off-lock install composes later with Group C.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchLayout {
    owned_levels: Vec<Vec<BranchOwnedTable>>,
}

impl BranchLayout {
    pub(crate) fn with_level_count(level_count: usize) -> Self {
        Self {
            owned_levels: vec![Vec::new(); level_count],
        }
    }

    pub(crate) fn from_levels(owned_levels: Vec<Vec<BranchOwnedTable>>) -> Self {
        Self { owned_levels }
    }

    pub(crate) fn levels(&self) -> &[Vec<BranchOwnedTable>] {
        &self.owned_levels
    }

    pub(crate) fn levels_mut(&mut self) -> &mut Vec<Vec<BranchOwnedTable>> {
        &mut self.owned_levels
    }

    #[allow(dead_code)]
    pub(crate) fn into_levels(self) -> Vec<Vec<BranchOwnedTable>> {
        self.owned_levels
    }
}

/// Historical-fork COW (Option A): the `<= fork_version` version/timestamp extremes of an inherited
/// layer that references at least one "straddle" table (one holding rows both at/below and above the
/// fork). Computed once at construction so the per-mutation observed-row and read-view facts folds
/// contribute the layer's in-fork extremes in O(1) instead of re-scanning the straddle tables on every
/// recompute. A non-straddle layer (e.g. `fork_current`, every table `<= V`) is `None` and folds from
/// cached facts, exactly as before.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InheritedLayerReadViewSummary {
    max_commit_version: Option<CommitVersion>,
    timestamp_min: Option<Timestamp>,
    timestamp_max: Option<Timestamp>,
}

impl InheritedLayerReadViewSummary {
    pub(crate) const fn max_commit_version(self) -> Option<CommitVersion> {
        self.max_commit_version
    }

    pub(crate) const fn timestamp_min(self) -> Option<Timestamp> {
        self.timestamp_min
    }

    pub(crate) const fn timestamp_max(self) -> Option<Timestamp> {
        self.timestamp_max
    }
}

/// Fold the `<= fork_version` version/timestamp extremes of `owned_levels`, returning `Some` only when
/// at least one table straddles the fork (otherwise the cheap facts-based folds already suffice and
/// there is no scan to amortize). Straddle tables are scanned once here; non-straddle tables fold from
/// their sealed facts — so the result equals `ObservedBranchRows::record_inherited_layer`'s full scan,
/// which the observed-facts debug oracle re-checks on every mutation.
fn compute_straddle_read_view_summary(
    owned_levels: &[Vec<BranchOwnedTable>],
    fork_version: CommitVersion,
) -> BranchRuntimeResult<Option<InheritedLayerReadViewSummary>> {
    let has_straddle = owned_levels
        .iter()
        .flatten()
        .any(|table| table.facts().commit_range().max().as_u64() > fork_version.as_u64());
    if !has_straddle {
        return Ok(None);
    }
    let mut max_commit_version: Option<CommitVersion> = None;
    let mut timestamp_min: Option<Timestamp> = None;
    let mut timestamp_max: Option<Timestamp> = None;
    for table in owned_levels.iter().flatten() {
        if table.facts().commit_range().max().as_u64() <= fork_version.as_u64() {
            record_commit_version(&mut max_commit_version, table.facts().commit_range().max());
            let extras = table.extras();
            if let Some(timestamp) = extras.timestamp_min() {
                record_timestamp(&mut timestamp_min, &mut timestamp_max, timestamp);
            }
            if let Some(timestamp) = extras.timestamp_max() {
                record_timestamp(&mut timestamp_min, &mut timestamp_max, timestamp);
            }
        } else {
            try_for_each_reader_row(table.reader(), |row| {
                if row.commit_version().as_u64() <= fork_version.as_u64() {
                    record_commit_version(&mut max_commit_version, row.commit_version());
                    record_timestamp(
                        &mut timestamp_min,
                        &mut timestamp_max,
                        row.commit_timestamp(),
                    );
                }
                Ok(())
            })?;
        }
    }
    Ok(Some(InheritedLayerReadViewSummary {
        max_commit_version,
        timestamp_min,
        timestamp_max,
    }))
}

/// #3519: fold one data row's commit stamp into the version→timestamp map,
/// keeping the first timestamp seen for a version (all rows of one commit carry
/// the same stamp; `CommitVersion::ZERO` is reserved and never a real commit).
/// Rows carrying a foreign `branch_id` are skipped — the reconstruction is the
/// branch's OWN timeline, and a materialized owned table holds only this
/// branch's rows, but the guard mirrors the elided `from_rows` scan it replaces
/// and keeps a stray COW row from forging a spurious version.
fn record_own_commit_stamp(
    stamps: &mut BTreeMap<CommitVersion, Timestamp>,
    branch_id: BranchId,
    row: &TableRow,
) {
    if row.row().physical_key().branch_id() != branch_id {
        return;
    }
    let version = row.commit_version();
    if version != CommitVersion::ZERO {
        stamps
            .entry(version)
            .or_insert_with(|| row.commit_timestamp());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchInheritedLayer {
    descriptor: InheritedLayerDescriptor,
    owned_levels: Vec<Vec<BranchOwnedTable>>,
    straddle_read_view_summary: Option<InheritedLayerReadViewSummary>,
}

impl BranchInheritedLayer {
    pub(crate) fn new(
        descriptor: InheritedLayerDescriptor,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
    ) -> BranchRuntimeResult<Self> {
        if descriptor.table_count() != owned_table_count(&owned_levels) {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer table count must match descriptor",
            });
        }
        validate_owned_levels(&owned_levels)?;
        validate_inherited_layer_unique_table_identities(&owned_levels)?;
        validate_inherited_layer_unique_keys(&owned_levels)?;
        for table in owned_levels.iter().flatten() {
            if table.branch_id() != descriptor.source_branch_id() {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited table branch id must match source branch",
                });
            }
            // BS4.4e: same fixed-16-byte-prefix bounds argument as the owned path — verify the source
            // branch by the sealed table's key range instead of scanning every row.
            if !key_range_matches_branch(
                table.facts().key_range().first_key(),
                table.facts().key_range().last_key(),
                descriptor.source_branch_id(),
            ) {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited table rows must match source branch",
                });
            }
            // Historical-fork COW (Option A): admit "straddle" tables — the parent's current boundary
            // tables that hold rows both at/below and above the fork version, which a fork at V < the
            // parent's current version must reference. Every inherited read/observe/materialize path
            // caps per-row to `<= fork_version` (`for_inherited_layer`, the summary folds, the
            // materialization cursor), so the `> V` rows are never visible. Reject only a table with no
            // in-fork rows at all (`min > V`) — referencing it would be meaningless.
            if table.facts().commit_range().min().as_u64() > descriptor.fork_version().as_u64() {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited table has no rows at or before the fork version",
                });
            }
        }
        let straddle_read_view_summary =
            compute_straddle_read_view_summary(&owned_levels, descriptor.fork_version())?;
        Ok(Self {
            descriptor,
            owned_levels,
            straddle_read_view_summary,
        })
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn new_unchecked_for_test(
        descriptor: InheritedLayerDescriptor,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
    ) -> Self {
        let straddle_read_view_summary =
            compute_straddle_read_view_summary(&owned_levels, descriptor.fork_version())
                .expect("straddle read-view summary for unchecked test inherited layer");
        Self {
            descriptor,
            owned_levels,
            straddle_read_view_summary,
        }
    }

    pub(crate) const fn descriptor(&self) -> InheritedLayerDescriptor {
        self.descriptor
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.descriptor.source_branch_id()
    }

    pub(crate) const fn fork_version(&self) -> CommitVersion {
        self.descriptor.fork_version()
    }

    pub(crate) const fn status(&self) -> InheritedLayerStatus {
        self.descriptor.status()
    }

    pub(crate) fn owned_levels(&self) -> &[Vec<BranchOwnedTable>] {
        &self.owned_levels
    }

    /// The cached `<= fork_version` extremes for a straddle layer, or `None` when every table is
    /// entirely at/below the fork (the folds then use the per-table facts fast path).
    pub(crate) const fn straddle_read_view_summary(&self) -> Option<InheritedLayerReadViewSummary> {
        self.straddle_read_view_summary
    }

    pub(crate) fn with_status(&self, status: InheritedLayerStatus) -> BranchRuntimeResult<Self> {
        Self::new(
            InheritedLayerDescriptor::new(
                self.source_branch_id(),
                self.fork_version(),
                status,
                self.table_count(),
            ),
            self.owned_levels.clone(),
        )
    }

    pub(crate) fn table_count(&self) -> usize {
        owned_table_count(&self.owned_levels)
    }

    pub(crate) fn clone_active_for_fork(&self) -> BranchRuntimeResult<Option<Self>> {
        let status = self.status();
        match status {
            InheritedLayerStatus::Active | InheritedLayerStatus::Materializing => {
                Ok(Some(Self::new(
                    InheritedLayerDescriptor::new(
                        self.source_branch_id(),
                        self.fork_version(),
                        status,
                        self.table_count(),
                    ),
                    self.owned_levels.clone(),
                )?))
            }
            InheritedLayerStatus::Materialized => Ok(None),
            InheritedLayerStatus::Unavailable => Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "unavailable inherited layers cannot be forked",
            }),
        }
    }

    fn is_readable(&self) -> bool {
        matches!(
            self.status(),
            InheritedLayerStatus::Active | InheritedLayerStatus::Materializing
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchReadView {
    branch_id: BranchId,
    active: MutableTable,
    frozen: Vec<FrozenTable>,
    owned_levels: Vec<Vec<BranchOwnedTable>>,
    inherited_layers: Vec<BranchInheritedLayer>,
    facts: BranchStateFacts,
    timestamp_coverage: BranchTimestampCoverage,
    /// W3.1a: the branch's retained-timeline index and whether this view
    /// holds the LIVE active memtable (`capture_snapshot`) — live views may
    /// serve the index tip; pinned views clamp to their captured facts.
    /// `None` (constructors that never attach one, e.g. testkit) = always
    /// fall back to the timeline-space scan.
    retained_timeline: Option<(Arc<crate::timeline_index::RetainedCommitTimeline>, bool)>,
    /// #3502 Slice A: the retained-history version floor published by MVCC
    /// version pruning (`None` = unbounded). A read `as_of` below
    /// `max(timeline.min_version, this)` raises `RetainedHistoryUnavailable`
    /// rather than returning a below-floor survivor.
    retained_history_floor: Option<CommitVersion>,
}

impl BranchReadView {
    pub(crate) fn new(
        branch_id: BranchId,
        active: MutableTable,
        frozen: Vec<FrozenTable>,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
        facts: BranchStateFacts,
    ) -> BranchRuntimeResult<Self> {
        Self::new_with_inherited(branch_id, active, frozen, owned_levels, Vec::new(), facts)
    }

    pub(crate) fn new_with_inherited(
        branch_id: BranchId,
        active: MutableTable,
        frozen: Vec<FrozenTable>,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
        inherited_layers: Vec<BranchInheritedLayer>,
        facts: BranchStateFacts,
    ) -> BranchRuntimeResult<Self> {
        validate_read_view_inputs(
            branch_id,
            &active,
            &frozen,
            &owned_levels,
            &inherited_layers,
            facts,
        )?;
        Ok(Self {
            branch_id,
            active,
            frozen,
            owned_levels,
            inherited_layers,
            facts,
            timestamp_coverage: BranchTimestampCoverage::unknown(),
            retained_timeline: None,
            retained_history_floor: None,
        })
    }

    pub(in crate::branch) fn new_from_validated_state(
        branch_id: BranchId,
        active: MutableTable,
        frozen: Vec<FrozenTable>,
        owned_levels: Vec<Vec<BranchOwnedTable>>,
        inherited_layers: Vec<BranchInheritedLayer>,
        facts: BranchStateFacts,
    ) -> BranchRuntimeResult<Self> {
        validate_read_view_source_counts(
            branch_id,
            &active,
            &frozen,
            &owned_levels,
            &inherited_layers,
            facts,
        )?;
        Ok(Self {
            branch_id,
            active,
            frozen,
            owned_levels,
            inherited_layers,
            facts,
            timestamp_coverage: BranchTimestampCoverage::unknown(),
            retained_timeline: None,
            retained_history_floor: None,
        })
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn facts(&self) -> BranchStateFacts {
        self.facts
    }

    pub(crate) const fn timestamp_coverage(&self) -> BranchTimestampCoverage {
        self.timestamp_coverage
    }

    pub(crate) fn with_timestamp_coverage(
        mut self,
        timestamp_coverage: BranchTimestampCoverage,
    ) -> Self {
        self.timestamp_coverage = timestamp_coverage;
        self
    }

    /// W3.1a: attach the branch's retained-timeline index. `live_active` is
    /// true for `capture_snapshot` views (live memtable — lookups may serve
    /// the index tip) and false for pinned captures (lookups clamp to the
    /// view's captured max commit version).
    pub(crate) fn with_retained_timeline(
        mut self,
        index: Arc<crate::timeline_index::RetainedCommitTimeline>,
        live_active: bool,
    ) -> Self {
        self.retained_timeline = Some((index, live_active));
        self
    }

    /// W3.1a: the attached retained-timeline index, if any, with the
    /// live-active flag.
    pub(crate) fn retained_timeline(
        &self,
    ) -> Option<(&Arc<crate::timeline_index::RetainedCommitTimeline>, bool)> {
        self.retained_timeline
            .as_ref()
            .map(|(index, live)| (index, *live))
    }

    /// #3502 Slice A: attach the branch's retained-history version floor.
    pub(crate) fn with_retained_history_floor(mut self, floor: Option<CommitVersion>) -> Self {
        self.retained_history_floor = floor;
        self
    }

    /// #3502 Slice A: the retained-history version floor for this view, if the
    /// branch has pruned below one (`None` = unbounded).
    pub(crate) const fn retained_history_floor(&self) -> Option<CommitVersion> {
        self.retained_history_floor
    }

    pub(crate) fn active_row_count(&self) -> usize {
        self.active.len()
    }

    pub(crate) fn frozen_table_count(&self) -> usize {
        self.frozen.len()
    }

    /// Strong-count of a frozen table's backing `Arc`, for the BS2.4b snapshot-lifetime probe.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn frozen_table_strong_count(&self, index: usize) -> Option<usize> {
        Some(self.frozen.get(index)?.memory_state_strong_count())
    }

    pub(crate) fn owned_table_count(&self) -> usize {
        owned_table_count(&self.owned_levels)
    }

    pub(crate) fn owned_levels(&self) -> &[Vec<BranchOwnedTable>] {
        &self.owned_levels
    }

    pub(crate) fn inherited_layer_count(&self) -> usize {
        self.inherited_layers.len()
    }

    pub(crate) fn inherited_layers(&self) -> &[BranchInheritedLayer] {
        &self.inherited_layers
    }

    pub(crate) fn source_layout(&self) -> BranchSourceLayout {
        source_layout_from_sources(
            &self.active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
        )
    }

    pub(crate) fn latest(
        &self,
        key: &PhysicalKey,
    ) -> BranchRuntimeResult<Option<BranchVisibleRow>> {
        self.read_point(key, BranchReadBound::latest())
    }

    pub(crate) fn at_version(
        &self,
        key: &PhysicalKey,
        version: CommitVersion,
    ) -> BranchRuntimeResult<Option<BranchVisibleRow>> {
        self.read_point(key, BranchReadBound::at_version(version))
    }

    /// Pin the (in Model 2, live) active memtable for the duration of one read: a consistent
    /// sequence cut plus fresh facts, so the multi-source read observes a stable active while
    /// commits may be appending off-lock. Idempotent on an already-pinned active (the under-lock
    /// conflict-validation and diagnostics paths), so it is O(1) there. Structural sources
    /// (`frozen`/`owned_levels`/`inherited_layers`) are immutable once published and read by
    /// reference — the pin never clones them.
    fn pinned_active(&self) -> MutableTable {
        self.active.clone_for_read_view()
    }

    /// #3519: the version→timestamp map derivable from this branch's OWN rows
    /// (active + frozen + owned tables). W3.1c elided the `COMMIT_TIMELINE`
    /// rows, so a commit's `(version, timestamp)` fact now survives durably
    /// only on the data rows it wrote and in a checkpoint's timeline group. A
    /// branch that was flushed but never checkpointed loses the group (close
    /// always defers the checkpoint while a non-seeded branch exists), and the
    /// old timeline-space scan then finds nothing — so this reconstructs the
    /// facts from the data rows, which is exact for the branch's own retained
    /// history: every commit writes at least one row carrying its stamp, and
    /// the `BTreeMap` collapses the per-commit row fan-out by version. Inherited
    /// (pre-fork parent) rows are deliberately excluded: recovery seeds a fork's
    /// pre-fork coverage from the parent chain, and a straddle layer would
    /// otherwise fold in parent commits above the fork version.
    pub(crate) fn own_commit_timestamps(
        &self,
    ) -> BranchRuntimeResult<BTreeMap<CommitVersion, Timestamp>> {
        let mut stamps: BTreeMap<CommitVersion, Timestamp> = BTreeMap::new();
        let branch_id = self.branch_id;
        let active = self.pinned_active();
        for row in active.iter() {
            record_own_commit_stamp(&mut stamps, branch_id, &row);
        }
        for table in &self.frozen {
            for row in table.iter() {
                record_own_commit_stamp(&mut stamps, branch_id, &row);
            }
        }
        for table in self.owned_levels.iter().flatten() {
            try_for_each_reader_row(table.reader(), |row| {
                record_own_commit_stamp(&mut stamps, branch_id, row);
                Ok(())
            })?;
        }
        Ok(stamps)
    }

    pub(crate) fn read_point(
        &self,
        key: &PhysicalKey,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Option<BranchVisibleRow>> {
        self.require_matching_branch(key.branch_id())?;
        let effective_bound = effective_own_read_bound(bound);
        self.require_timestamp_coverage(bound)?;
        // BS2.5: point reads do NOT pin the (live) active. Gate B (`commit_version <= V`, carried in
        // `effective_bound`) already filters everything the sequence pin (gate A) would, because
        // appends are monotonic in BOTH sequence and commit_version and each happens-before the
        // `visible` bump the reader's Acquire load synchronizes with — so no `<= V` row is ever
        // back-dated into the active after V is observed, and any row appended after has
        // `commit_version > V` and is dropped by gate B regardless of gate A. The conflict/
        // diagnostics callers pass an already-pinned view (`Some` bound), so `&self.active` still
        // applies gate A there. Saves a per-read RwLock acquire + a reader-reader-contended
        // `Arc<TableMemoryState>` refcount RMW + 2 heap allocs.
        let selected = select_ordered_visible_point_candidate(
            self.branch_id,
            &self.active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            key,
            bound,
            effective_bound,
        )?;
        Ok(selected.and_then(|candidate| {
            candidate_into_visible_row(candidate, effective_bound.max_commit_timestamp())
        }))
    }

    /// Latest-style point read that returns the newest row **including tombstones** (a delete is a
    /// tombstone row, not `None`), for the off-lock `read_point` Latest verb. Mirrors
    /// [`BranchLocalState::read_point_or_tombstone_borrowed`] but on a published snapshot, pinning
    /// the active at read time.
    pub(crate) fn read_point_or_tombstone(
        &self,
        key: &PhysicalKey,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Option<BranchHistoryRow>> {
        self.require_matching_branch(key.branch_id())?;
        self.require_timestamp_coverage(bound)?;
        let effective_bound = effective_own_read_bound(bound);
        // BS2.5: unpinned — gate B (the visible bound) suffices; see `read_point`.
        let selected = select_ordered_visible_point_candidate(
            self.branch_id,
            &self.active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            key,
            bound,
            effective_bound,
        )?;
        Ok(selected.and_then(|candidate| {
            if row_is_expired_at(
                candidate_row_ref(&candidate),
                effective_bound.max_commit_timestamp(),
            ) {
                None
            } else {
                Some(candidate_into_history_row(candidate))
            }
        }))
    }

    pub(crate) fn history(
        &self,
        key: &PhysicalKey,
        options: BranchHistoryOptions,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.collect_history(key, options, BranchReadBound::latest(), None)
    }

    /// Off-lock history capped at the visible version `V`: identical to [`history`](Self::history)
    /// but the candidate collection is bounded so rows newer than the visibility bound (e.g. an
    /// `applied_not_visible` commit, or a batch mid-apply on another thread) are hidden.
    pub(crate) fn history_visible(
        &self,
        key: &PhysicalKey,
        options: BranchHistoryOptions,
        visible: CommitVersion,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.collect_history(key, options, BranchReadBound::at_version(visible), None)
    }

    /// Replay idempotence probe: locate this exact internal row (physical
    /// key + commit version) among the branch's OWN sources — active, frozen,
    /// then owned levels. Inherited layers never hold the branch's own WAL
    /// rows (replay reconciles only this branch's appends), so they are
    /// skipped. Early-exits on the first byte-equal row. This replaces the
    /// full `history(all())` walk in replay classification, which made
    /// recovery replay O(rows × sources × key versions) — measured ~365µs
    /// and 7 source probes per replayed row at 1M scale.
    pub(crate) fn classify_own_internal_row(
        &self,
        target: &StorageRow,
    ) -> BranchRuntimeResult<(BranchOwnRowMatch, usize)> {
        self.require_matching_branch(target.physical_key().branch_id())?;
        let key = target.physical_key();
        let version = target.commit_version();
        // A point lookup BOUNDED at the target version: each source returns
        // its newest row ≤ version, which is the exact row iff the source
        // holds one at that version. On lazy tables this is an in-block point
        // seek — no decode of the block's other rows (the initial probe used
        // `physical_key_rows`, whose decode-all dominated replay at ~118µs
        // per row in the gdb profile).
        let lookup = TablePreparedPointLookup::new(key, Some(version), None);
        let mut probes = 0usize;
        let mut same_version_differs = false;
        // Byte-equal → done; same version with different content is
        // remembered (a corruption signal) but the scan continues, because a
        // byte-equal copy in a later source still classifies as Exact —
        // identical to the history-walk semantics this replaces.
        let fold = |row: Option<&TableRow>, same_version_differs: &mut bool| -> bool {
            let Some(row) = row else {
                return false;
            };
            if row.row().commit_version() != version {
                return false;
            }
            if row.row() == target {
                return true;
            }
            *same_version_differs = true;
            false
        };

        let active = self.pinned_active();
        probes = probes.saturating_add(1);
        let (row, _visited) = active.seek_prepared_point(&lookup);
        if fold(row.as_deref(), &mut same_version_differs) {
            return Ok((BranchOwnRowMatch::Equal, probes));
        }
        for table in &self.frozen {
            probes = probes.saturating_add(1);
            let (row, _visited) = table.seek_prepared_point(&lookup);
            if fold(row.as_deref(), &mut same_version_differs) {
                return Ok((BranchOwnRowMatch::Equal, probes));
            }
        }
        let key_bytes = TablePhysicalKeyBytes::from_physical_key(key);
        for (level_index, tables) in self.owned_levels.iter().enumerate() {
            if level_index == 0 {
                for table in tables {
                    probes = probes.saturating_add(1);
                    let (row, _visited) = table
                        .reader()
                        .try_seek_prepared_point(&lookup)
                        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
                    if fold(row.as_ref(), &mut same_version_differs) {
                        return Ok((BranchOwnRowMatch::Equal, probes));
                    }
                }
                continue;
            }
            if tables.is_empty() {
                continue;
            }
            probes = probes.saturating_add(1);
            let Some(table_index) = select_nonzero_level_point_table(tables, key_bytes.as_slice())?
            else {
                continue;
            };
            let (row, _visited) = tables[table_index]
                .reader()
                .try_seek_prepared_point(&lookup)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            if fold(row.as_ref(), &mut same_version_differs) {
                return Ok((BranchOwnRowMatch::Equal, probes));
            }
        }
        if same_version_differs {
            Ok((BranchOwnRowMatch::SameVersionDiffers, probes))
        } else {
            Ok((BranchOwnRowMatch::Absent, probes))
        }
    }

    fn collect_history(
        &self,
        key: &PhysicalKey,
        options: BranchHistoryOptions,
        candidate_bound: BranchReadBound,
        source_probes: Option<&mut usize>,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.require_matching_branch(key.branch_id())?;
        if options.limit_bound() == Some(0) {
            return Ok(Vec::new());
        }

        let active = self.pinned_active();
        let mut rows = history_candidates(
            self.branch_id,
            &active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            key,
            candidate_bound,
            effective_own_read_bound(candidate_bound),
            source_probes,
        )?;
        sort_candidates_newest_first(&mut rows);

        let before_version = options.before_version_bound();
        let mut history = Vec::new();
        for candidate in rows {
            let row = candidate_row_ref(&candidate);
            if before_version
                .is_some_and(|version| row.commit_version().as_u64() >= version.as_u64())
            {
                continue;
            }
            if !options.includes_tombstones() && row.is_tombstone() {
                continue;
            }
            history.push(candidate_into_history_row(candidate));
            if options
                .limit_bound()
                .is_some_and(|limit| history.len() >= limit)
            {
                break;
            }
        }
        Ok(history)
    }

    pub(crate) fn scan_prefix(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Vec<BranchVisibleRow>> {
        self.scan(bounds, bound)
    }

    pub(crate) fn scan_range(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Vec<BranchVisibleRow>> {
        self.scan(bounds, bound)
    }

    pub(crate) fn scan_prefix_including_tombstones(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
        after_version: Option<CommitVersion>,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.scan_including_tombstones(bounds, bound, after_version)
    }

    pub(crate) fn scan_range_including_tombstones(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.scan_including_tombstones(bounds, bound, None)
    }

    pub(crate) fn scan_immutable_sources(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Vec<BranchImmutableRowSource>> {
        self.require_matching_branch(bounds.branch_id())?;
        self.require_timestamp_coverage(bound)?;
        let own_bounds = bounds.table_key_bounds()?;
        let own_bound = effective_own_read_bound(bound);
        let mut sources = Vec::new();
        collect_owned_immutable_sources(
            &self.owned_levels,
            &own_bounds,
            own_bound,
            None,
            &mut sources,
        )?;
        collect_inherited_immutable_sources(
            self.branch_id,
            &self.inherited_layers,
            bounds,
            bound,
            &mut sources,
        )?;
        Ok(sources)
    }

    fn scan(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Vec<BranchVisibleRow>> {
        self.require_matching_branch(bounds.branch_id())?;
        let effective_bound = effective_own_read_bound(bound);
        self.require_timestamp_coverage(bound)?;
        let active = self.pinned_active();
        let rows = scan_including_tombstones_from_sources(
            self.branch_id,
            &active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            bounds,
            bound,
            None,
            None,
            effective_bound.max_commit_timestamp(),
            false,
        )?;
        let rows = rows
            .into_iter()
            .filter_map(|row| history_row_into_visible(row, effective_bound.max_commit_timestamp()))
            .collect::<Vec<_>>();
        perf_trace::record_branch_scan_rows_returned(rows.len());
        Ok(rows)
    }

    fn scan_including_tombstones(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
        after_version: Option<CommitVersion>,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.require_matching_branch(bounds.branch_id())?;
        self.require_timestamp_coverage(bound)?;
        let active = self.pinned_active();
        scan_including_tombstones_from_sources(
            self.branch_id,
            &active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            bounds,
            bound,
            after_version,
            None,
            effective_own_read_bound(bound).max_commit_timestamp(),
            true,
        )
    }

    /// Latest-style scan including tombstones with a visible-row limit, for the off-lock
    /// `scan_prefix`/`scan_range` Latest verbs. Mirrors
    /// [`BranchLocalState::scan_including_tombstones_borrowed`] on a published snapshot (the
    /// deleted Latest bridge passed `visible_limit_timestamp = None`), pinning the active at read
    /// time.
    pub(crate) fn scan_including_tombstones_visible(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
        visible_limit: Option<usize>,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        self.require_matching_branch(bounds.branch_id())?;
        self.require_timestamp_coverage(bound)?;
        let active = self.pinned_active();
        scan_including_tombstones_from_sources(
            self.branch_id,
            &active,
            &self.frozen,
            &self.owned_levels,
            &self.inherited_layers,
            bounds,
            bound,
            None,
            visible_limit,
            None,
            true,
        )
    }

    fn require_matching_branch(&self, branch_id: BranchId) -> BranchRuntimeResult<()> {
        if branch_id == self.branch_id {
            Ok(())
        } else {
            Err(BranchRuntimeError::InvalidBranchRow {
                reason: "read target branch id does not match read view branch",
            })
        }
    }

    fn require_timestamp_coverage(&self, bound: BranchReadBound) -> BranchRuntimeResult<()> {
        match bound {
            BranchReadBound::Latest | BranchReadBound::AtVersion(_) => Ok(()),
            BranchReadBound::AtTimestamp(timestamp) => self.timestamp_coverage.require_timestamp(
                self.branch_id,
                timestamp,
                BranchTimestampHistorySource::Combined,
            ),
        }
    }
}

impl BranchLocalState {
    pub(crate) fn read_point_or_tombstone_borrowed(
        &self,
        key: &PhysicalKey,
        bound: BranchReadBound,
    ) -> BranchRuntimeResult<Option<BranchHistoryRow>> {
        require_state_matching_branch(self, key.branch_id())?;
        require_state_timestamp_coverage(self, bound)?;
        let effective_bound = effective_own_read_bound(bound);
        let selected = select_ordered_visible_point_candidate(
            self.branch_id(),
            self.active(),
            self.frozen(),
            self.owned_levels(),
            self.inherited_layers(),
            key,
            bound,
            effective_bound,
        )?;
        Ok(selected.and_then(|candidate| {
            if row_is_expired_at(
                candidate_row_ref(&candidate),
                effective_bound.max_commit_timestamp(),
            ) {
                None
            } else {
                Some(candidate_into_history_row(candidate))
            }
        }))
    }

    pub(crate) fn scan_including_tombstones_borrowed(
        &self,
        bounds: &BranchScanBounds,
        bound: BranchReadBound,
        visible_limit: Option<usize>,
        visible_limit_timestamp: Option<Timestamp>,
    ) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
        require_state_matching_branch(self, bounds.branch_id())?;
        require_state_timestamp_coverage(self, bound)?;
        scan_including_tombstones_from_sources(
            self.branch_id(),
            self.active(),
            self.frozen(),
            self.owned_levels(),
            self.inherited_layers(),
            bounds,
            bound,
            None,
            visible_limit,
            visible_limit_timestamp,
            true,
        )
    }
}

type CandidateRow = (StorageRow, BranchRowSource);

fn candidate_row(row: StorageRow, source: BranchRowSource) -> CandidateRow {
    (row, source)
}

fn candidate_row_ref(candidate: &CandidateRow) -> &StorageRow {
    &candidate.0
}

fn candidate_source(candidate: &CandidateRow) -> BranchRowSource {
    candidate.1
}

fn clone_point_candidate_row(row: &TableRow) -> StorageRow {
    perf_trace::record_branch_point_candidate_row_clone(row.approximate_size_bytes());
    row.row().clone()
}

fn candidate_into_visible_row(
    (row, source): CandidateRow,
    read_timestamp: Option<Timestamp>,
) -> Option<BranchVisibleRow> {
    if row.is_tombstone() || row_is_expired_at(&row, read_timestamp) {
        None
    } else {
        Some(BranchVisibleRow::new(row, source))
    }
}

fn candidate_into_history_row((row, source): CandidateRow) -> BranchHistoryRow {
    BranchHistoryRow::new(row, source)
}

fn history_row_into_visible(
    row: BranchHistoryRow,
    read_timestamp: Option<Timestamp>,
) -> Option<BranchVisibleRow> {
    let BranchHistoryRow { row, source } = row;
    candidate_into_visible_row((row, source), read_timestamp)
}

fn effective_own_read_bound(bound: BranchReadBound) -> BranchEffectiveReadBound {
    BranchEffectiveReadBound::for_own_branch(bound)
}

fn history_candidates(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    key: &PhysicalKey,
    bound: BranchReadBound,
    effective_bound: BranchEffectiveReadBound,
    mut source_probes: Option<&mut usize>,
) -> BranchRuntimeResult<Vec<CandidateRow>> {
    let mut rows = Vec::new();
    let mut rows_visited = 0usize;
    let mut history_counts = perf_trace::BranchSourceRowCounts::default();

    add_history_source_probes(&mut source_probes, 1);
    let (active_rows, visited) = active.physical_key_rows(key);
    rows_visited = rows_visited.saturating_add(visited);
    history_counts.active = history_counts.active.saturating_add(visited);
    push_history_rows(
        active_rows,
        BranchHistoryCandidateSource::Own(BranchRowSource::Active),
        effective_bound,
        &mut rows,
    )?;

    for (index, table) in frozen.iter().enumerate() {
        add_history_source_probes(&mut source_probes, 1);
        let (table_rows, visited) = table.physical_key_rows(key);
        rows_visited = rows_visited.saturating_add(visited);
        history_counts.frozen = history_counts.frozen.saturating_add(visited);
        push_history_rows(
            table_rows,
            BranchHistoryCandidateSource::Own(BranchRowSource::Frozen { index }),
            effective_bound,
            &mut rows,
        )?;
    }

    let key_bytes = TablePhysicalKeyBytes::from_physical_key(key);
    for (level_index, tables) in owned_levels.iter().enumerate() {
        collect_owned_level_history_candidates(
            level_index,
            tables,
            key,
            &key_bytes,
            effective_bound,
            &mut rows,
            &mut rows_visited,
            &mut history_counts,
            &mut source_probes,
        )?;
    }

    collect_inherited_history_candidates(
        branch_id,
        inherited_layers,
        key,
        bound,
        &mut rows,
        &mut rows_visited,
        &mut history_counts,
        &mut source_probes,
    )?;

    perf_trace::record_point_candidate_collection(rows_visited, rows.len());
    perf_trace::record_branch_history_rows(history_counts, rows.len());
    Ok(rows)
}

fn collect_owned_level_history_candidates(
    level_index: usize,
    tables: &[BranchOwnedTable],
    key: &PhysicalKey,
    key_bytes: &TablePhysicalKeyBytes,
    effective_bound: BranchEffectiveReadBound,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    history_counts: &mut perf_trace::BranchSourceRowCounts,
    source_probes: &mut Option<&mut usize>,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for (table_index, table) in tables.iter().enumerate() {
            add_history_source_probes(source_probes, 1);
            let (table_rows, visited) = table
                .reader()
                .try_physical_key_rows(key)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            *rows_visited = (*rows_visited).saturating_add(visited);
            history_counts.owned_l0 = history_counts.owned_l0.saturating_add(visited);
            push_history_rows(
                table_rows,
                BranchHistoryCandidateSource::Own(BranchRowSource::OwnedTable {
                    level: table.level(),
                    table_index,
                }),
                effective_bound,
                rows,
            )?;
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }
    add_history_source_probes(source_probes, 1);
    let Some(table_index) = select_nonzero_level_point_table(tables, key_bytes.as_slice())? else {
        return Ok(());
    };
    let table = &tables[table_index];
    add_history_source_probes(source_probes, 1);
    let (table_rows, visited) = table
        .reader()
        .try_physical_key_rows(key)
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    *rows_visited = (*rows_visited).saturating_add(visited);
    history_counts.owned_nonzero = history_counts.owned_nonzero.saturating_add(visited);
    push_history_rows(
        table_rows,
        BranchHistoryCandidateSource::Own(BranchRowSource::OwnedTable {
            level: table.level(),
            table_index,
        }),
        effective_bound,
        rows,
    )
}

fn collect_inherited_history_candidates(
    child_branch_id: BranchId,
    inherited_layers: &[BranchInheritedLayer],
    key: &PhysicalKey,
    bound: BranchReadBound,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    history_counts: &mut perf_trace::BranchSourceRowCounts,
    source_probes: &mut Option<&mut usize>,
) -> BranchRuntimeResult<()> {
    for (layer_index, layer) in inherited_layers.iter().enumerate() {
        if !layer.is_readable() {
            continue;
        }
        add_history_source_probes(source_probes, 1);
        let source_key =
            rewrite_physical_key_branch(key, layer.source_branch_id()).map_err(|_| {
                BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited point key rewrite failed",
                }
            })?;
        let source_key_bytes = TablePhysicalKeyBytes::from_physical_key(&source_key);
        let inherited_bound =
            BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            collect_inherited_level_history_candidates(
                child_branch_id,
                layer,
                layer_index,
                level_index,
                tables,
                &source_key,
                &source_key_bytes,
                inherited_bound,
                rows,
                rows_visited,
                history_counts,
                source_probes,
            )?;
        }
    }
    Ok(())
}

fn collect_inherited_level_history_candidates(
    child_branch_id: BranchId,
    layer: &BranchInheritedLayer,
    layer_index: usize,
    level_index: usize,
    tables: &[BranchOwnedTable],
    source_key: &PhysicalKey,
    source_key_bytes: &TablePhysicalKeyBytes,
    inherited_bound: BranchEffectiveReadBound,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    history_counts: &mut perf_trace::BranchSourceRowCounts,
    source_probes: &mut Option<&mut usize>,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for table in tables {
            add_history_source_probes(source_probes, 1);
            let (table_rows, visited) = table
                .reader()
                .try_physical_key_rows(source_key)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            *rows_visited = (*rows_visited).saturating_add(visited);
            history_counts.inherited_l0 = history_counts.inherited_l0.saturating_add(visited);
            push_history_rows(
                table_rows,
                BranchHistoryCandidateSource::Inherited {
                    source_branch_id: layer.source_branch_id(),
                    layer_index,
                    child_branch_id,
                },
                inherited_bound,
                rows,
            )?;
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }
    add_history_source_probes(source_probes, 1);
    let Some(table_index) = select_nonzero_level_point_table(tables, source_key_bytes.as_slice())?
    else {
        return Ok(());
    };
    add_history_source_probes(source_probes, 1);
    let (table_rows, visited) = tables[table_index]
        .reader()
        .try_physical_key_rows(source_key)
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    *rows_visited = (*rows_visited).saturating_add(visited);
    history_counts.inherited_nonzero = history_counts.inherited_nonzero.saturating_add(visited);
    push_history_rows(
        table_rows,
        BranchHistoryCandidateSource::Inherited {
            source_branch_id: layer.source_branch_id(),
            layer_index,
            child_branch_id,
        },
        inherited_bound,
        rows,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BranchHistoryCandidateSource {
    Own(BranchRowSource),
    Inherited {
        source_branch_id: BranchId,
        layer_index: usize,
        child_branch_id: BranchId,
    },
}

fn add_history_source_probes(source_probes: &mut Option<&mut usize>, probes: usize) {
    if let Some(source_probes) = source_probes.as_deref_mut() {
        *source_probes = source_probes.saturating_add(probes);
    }
}

fn push_history_rows(
    table_rows: Vec<TableRow>,
    source: BranchHistoryCandidateSource,
    effective_bound: BranchEffectiveReadBound,
    rows: &mut Vec<CandidateRow>,
) -> BranchRuntimeResult<()> {
    for row in table_rows {
        if !effective_bound.matches_row(row.row()) {
            continue;
        }
        match source {
            BranchHistoryCandidateSource::Own(source) => {
                rows.push(candidate_row(row.row().clone(), source));
            }
            BranchHistoryCandidateSource::Inherited {
                source_branch_id,
                layer_index,
                child_branch_id,
            } => {
                rows.push(candidate_row(
                    rewrite_row_branch(row.row(), source_branch_id, child_branch_id).map_err(
                        |_| BranchRuntimeError::InvalidInheritedLayer {
                            reason: "inherited row branch rewrite failed",
                        },
                    )?,
                    BranchRowSource::Inherited {
                        source_branch_id,
                        layer_index,
                    },
                ));
            }
        }
    }
    Ok(())
}

enum PointCandidate<'a> {
    LocalShared {
        row: Arc<TableRow>,
        source: BranchRowSource,
    },
    LocalLookup {
        row: TablePointLookupRow<'a>,
        source: BranchRowSource,
    },
    InheritedLookup {
        row: TablePointLookupRow<'a>,
        source_branch_id: BranchId,
        child_branch_id: BranchId,
        layer_index: usize,
    },
}

impl PointCandidate<'_> {
    fn row(&self) -> &StorageRow {
        match self {
            Self::LocalShared { row, .. } => row.row(),
            Self::LocalLookup { row, .. } | Self::InheritedLookup { row, .. } => row.storage_row(),
        }
    }

    fn source(&self) -> BranchRowSource {
        match self {
            Self::LocalShared { source, .. } | Self::LocalLookup { source, .. } => *source,
            Self::InheritedLookup {
                source_branch_id,
                layer_index,
                ..
            } => BranchRowSource::Inherited {
                source_branch_id: *source_branch_id,
                layer_index: *layer_index,
            },
        }
    }

    fn into_candidate_row(self) -> BranchRuntimeResult<CandidateRow> {
        match self {
            Self::LocalShared { row, source } => Ok(candidate_row(
                clone_point_candidate_row(row.as_ref()),
                source,
            )),
            Self::LocalLookup { row, source } => Ok(candidate_row(
                // B4: lazy block lookups return an already-owned row — move
                // it instead of cloning (the clone counter stays honest: it
                // records only actual clones).
                match row {
                    TablePointLookupRow::Borrowed(row) => clone_point_candidate_row(row),
                    TablePointLookupRow::Owned(row) => row,
                },
                source,
            )),
            Self::InheritedLookup {
                row,
                source_branch_id,
                child_branch_id,
                layer_index,
            } => {
                perf_trace::record_branch_point_candidate_row_clone(row.approximate_size_bytes());
                Ok(candidate_row(
                    rewrite_row_branch(row.storage_row(), source_branch_id, child_branch_id)
                        .map_err(|_| BranchRuntimeError::InvalidInheritedLayer {
                            reason: "inherited row branch rewrite failed",
                        })?,
                    BranchRowSource::Inherited {
                        source_branch_id,
                        layer_index,
                    },
                ))
            }
        }
    }
}

struct PointSelection<'a> {
    selected: Option<PointCandidate<'a>>,
    rows_visited: usize,
    candidates_materialized: usize,
    source_counts: perf_trace::BranchPointSourceCounts,
}

impl<'a> PointSelection<'a> {
    fn new() -> Self {
        Self {
            selected: None,
            rows_visited: 0,
            candidates_materialized: 0,
            source_counts: perf_trace::BranchPointSourceCounts::default(),
        }
    }

    fn add_rows_visited(&mut self, visited: usize) {
        self.rows_visited = self.rows_visited.saturating_add(visited);
    }

    fn consider_shared_table_row(&mut self, row: Arc<TableRow>, source: BranchRowSource) {
        self.consider_candidate(PointCandidate::LocalShared { row, source });
    }

    fn consider_lookup_table_row(&mut self, row: TablePointLookupRow<'a>, source: BranchRowSource) {
        self.consider_candidate(PointCandidate::LocalLookup { row, source });
    }

    fn consider_inherited_table_row(
        &mut self,
        row: TablePointLookupRow<'a>,
        source_branch_id: BranchId,
        child_branch_id: BranchId,
        layer_index: usize,
    ) {
        self.consider_candidate(PointCandidate::InheritedLookup {
            row,
            source_branch_id,
            child_branch_id,
            layer_index,
        });
    }

    fn consider_candidate(&mut self, candidate: PointCandidate<'a>) {
        self.candidates_materialized = self.candidates_materialized.saturating_add(1);
        let should_replace = self
            .selected
            .as_ref()
            .is_none_or(|selected| candidate_is_newer_or_better_tie(&candidate, selected));
        if should_replace {
            self.selected = Some(candidate);
        }
    }

    fn selected_commit_version(&self) -> Option<CommitVersion> {
        self.selected
            .as_ref()
            .map(|candidate| candidate.row().commit_version())
    }

    fn finish(self) -> BranchRuntimeResult<Option<CandidateRow>> {
        perf_trace::record_branch_point_sources(self.source_counts);
        perf_trace::record_point_candidate_collection(
            self.rows_visited,
            self.candidates_materialized,
        );
        if let Some(candidate) = &self.selected {
            record_selected_point_source(candidate.source());
        }
        self.selected
            .map(PointCandidate::into_candidate_row)
            .transpose()
    }
}

fn candidate_is_newer_or_better_tie(
    candidate: &PointCandidate<'_>,
    selected: &PointCandidate<'_>,
) -> bool {
    let candidate_version = candidate.row().commit_version();
    let selected_version = selected.row().commit_version();
    candidate_version > selected_version
        || (candidate_version == selected_version
            && source_order_cmp(candidate.source(), selected.source()).is_lt())
}

#[expect(
    clippy::too_many_lines,
    reason = "ordered point selection intentionally keeps the active, frozen, owned, and inherited early-exit flow together"
)]
fn select_ordered_visible_point_candidate<'a>(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &'a [Vec<BranchOwnedTable>],
    inherited_layers: &'a [BranchInheritedLayer],
    key: &PhysicalKey,
    bound: BranchReadBound,
    effective_bound: BranchEffectiveReadBound,
) -> BranchRuntimeResult<Option<CandidateRow>> {
    let mut selection = PointSelection::new();
    let inherited_source_count = remaining_source_count_after_inherited_layer(inherited_layers, 0);
    let mut skip_remaining_local_sources = false;
    let lookup = TablePreparedPointLookup::new(
        key,
        effective_bound.max_commit_version(),
        effective_bound.max_commit_timestamp(),
    );

    selection.source_counts.active_probes = selection.source_counts.active_probes.saturating_add(1);
    selection.source_counts.table_seeks = selection.source_counts.table_seeks.saturating_add(1);
    let (row, visited) = active.seek_prepared_point(&lookup);
    selection.add_rows_visited(visited);
    if let Some(row) = row {
        selection.consider_shared_table_row(row, BranchRowSource::Active);
    }
    if ordered_point_can_stop(
        &selection,
        perf_trace::BranchPointSourceKind::Active,
        remaining_local_max_commit_after_active(frozen, owned_levels, bound),
        remaining_local_source_count_after_active(frozen, owned_levels),
    ) {
        if inherited_source_count == 0 {
            return selection.finish();
        }
        skip_remaining_local_sources = true;
    }

    if !skip_remaining_local_sources {
        for (index, table) in frozen.iter().enumerate() {
            selection.source_counts.frozen_probes =
                selection.source_counts.frozen_probes.saturating_add(1);
            selection.source_counts.table_seeks =
                selection.source_counts.table_seeks.saturating_add(1);
            let (row, visited) = table.seek_prepared_point(&lookup);
            selection.add_rows_visited(visited);
            if let Some(row) = row {
                selection.consider_shared_table_row(row, BranchRowSource::Frozen { index });
            }
        }
        if ordered_point_can_stop(
            &selection,
            perf_trace::BranchPointSourceKind::Frozen,
            remaining_local_max_commit_after_frozen(owned_levels, bound),
            remaining_local_source_count_after_frozen(owned_levels),
        ) {
            if inherited_source_count == 0 {
                return selection.finish();
            }
            skip_remaining_local_sources = true;
        }
    }

    if !skip_remaining_local_sources {
        for (level_index, tables) in owned_levels.iter().enumerate() {
            select_owned_level_point_candidate(level_index, tables, &lookup, &mut selection)?;
            let source = if level_index == 0 {
                perf_trace::BranchPointSourceKind::OwnedL0
            } else {
                perf_trace::BranchPointSourceKind::OwnedNonzero
            };
            if ordered_point_can_stop(
                &selection,
                source,
                remaining_local_max_commit_after_owned_level(
                    owned_levels,
                    level_index.saturating_add(1),
                    bound,
                ),
                remaining_local_source_count_after_owned_level(
                    owned_levels,
                    level_index.saturating_add(1),
                ),
            ) {
                if inherited_source_count == 0 {
                    return selection.finish();
                }
                break;
            }
        }
    }

    if ordered_point_can_stop(
        &selection,
        perf_trace::BranchPointSourceKind::Inherited,
        remaining_max_commit_after_inherited_layer(inherited_layers, 0, bound),
        inherited_source_count,
    ) {
        return selection.finish();
    }

    for (layer_index, layer) in inherited_layers.iter().enumerate() {
        if !layer.is_readable() {
            continue;
        }
        selection.source_counts.inherited_layer_searches = selection
            .source_counts
            .inherited_layer_searches
            .saturating_add(1);
        let source_key =
            rewrite_physical_key_branch(key, layer.source_branch_id()).map_err(|_| {
                BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited point key rewrite failed",
                }
            })?;
        perf_trace::record_branch_point_inherited_key_rewrite();
        let inherited_bound =
            BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
        let inherited_lookup = TablePreparedPointLookup::new(
            &source_key,
            inherited_bound.max_commit_version(),
            inherited_bound.max_commit_timestamp(),
        );
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            select_inherited_level_point_candidate(
                branch_id,
                layer,
                layer_index,
                level_index,
                tables,
                &inherited_lookup,
                &mut selection,
            )?;
        }
        if ordered_point_can_stop(
            &selection,
            perf_trace::BranchPointSourceKind::Inherited,
            remaining_max_commit_after_inherited_layer(
                inherited_layers,
                layer_index.saturating_add(1),
                bound,
            ),
            remaining_source_count_after_inherited_layer(
                inherited_layers,
                layer_index.saturating_add(1),
            ),
        ) {
            return selection.finish();
        }
    }

    selection.finish()
}

fn select_owned_level_point_candidate<'a>(
    level_index: usize,
    tables: &'a [BranchOwnedTable],
    lookup: &TablePreparedPointLookup,
    selection: &mut PointSelection<'a>,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for (table_index, table) in tables.iter().enumerate() {
            selection.source_counts.owned_l0_table_probes = selection
                .source_counts
                .owned_l0_table_probes
                .saturating_add(1);
            selection.source_counts.table_seeks =
                selection.source_counts.table_seeks.saturating_add(1);
            let (row, visited) = table
                .reader()
                .try_seek_prepared_point_candidate(lookup)
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            selection.add_rows_visited(visited);
            if let Some(row) = row {
                selection.consider_lookup_table_row(
                    row,
                    BranchRowSource::OwnedTable {
                        level: table.level(),
                        table_index,
                    },
                );
            }
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }

    selection.source_counts.owned_nonzero_level_searches = selection
        .source_counts
        .owned_nonzero_level_searches
        .saturating_add(1);
    let Some(table_index) = select_nonzero_level_point_table(tables, lookup.physical_key())? else {
        return Ok(());
    };
    let table = &tables[table_index];
    selection.source_counts.owned_nonzero_table_probes = selection
        .source_counts
        .owned_nonzero_table_probes
        .saturating_add(1);
    selection.source_counts.table_seeks = selection.source_counts.table_seeks.saturating_add(1);
    let (row, visited) = table
        .reader()
        .try_seek_prepared_point_candidate(lookup)
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    selection.add_rows_visited(visited);
    if let Some(row) = row {
        selection.consider_lookup_table_row(
            row,
            BranchRowSource::OwnedTable {
                level: table.level(),
                table_index,
            },
        );
    }
    Ok(())
}

fn select_inherited_level_point_candidate<'a>(
    child_branch_id: BranchId,
    layer: &'a BranchInheritedLayer,
    layer_index: usize,
    level_index: usize,
    tables: &'a [BranchOwnedTable],
    lookup: &TablePreparedPointLookup,
    selection: &mut PointSelection<'a>,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for table in tables {
            selection.source_counts.inherited_l0_table_probes = selection
                .source_counts
                .inherited_l0_table_probes
                .saturating_add(1);
            append_ordered_inherited_point_table_candidate(
                child_branch_id,
                layer,
                layer_index,
                table,
                lookup,
                selection,
            )?;
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }

    selection.source_counts.inherited_nonzero_level_searches = selection
        .source_counts
        .inherited_nonzero_level_searches
        .saturating_add(1);
    let Some(table_index) = select_nonzero_level_point_table(tables, lookup.physical_key())? else {
        return Ok(());
    };
    selection.source_counts.inherited_nonzero_table_probes = selection
        .source_counts
        .inherited_nonzero_table_probes
        .saturating_add(1);
    append_ordered_inherited_point_table_candidate(
        child_branch_id,
        layer,
        layer_index,
        &tables[table_index],
        lookup,
        selection,
    )?;
    Ok(())
}

fn append_ordered_inherited_point_table_candidate<'a>(
    child_branch_id: BranchId,
    layer: &'a BranchInheritedLayer,
    layer_index: usize,
    table: &'a BranchOwnedTable,
    lookup: &TablePreparedPointLookup,
    selection: &mut PointSelection<'a>,
) -> BranchRuntimeResult<()> {
    selection.source_counts.table_seeks = selection.source_counts.table_seeks.saturating_add(1);
    let (row, visited) = table
        .reader()
        .try_seek_prepared_point_candidate(lookup)
        .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    selection.add_rows_visited(visited);
    if let Some(row) = row {
        selection.consider_inherited_table_row(
            row,
            layer.source_branch_id(),
            child_branch_id,
            layer_index,
        );
    }
    Ok(())
}

fn ordered_point_can_stop(
    selection: &PointSelection<'_>,
    source: perf_trace::BranchPointSourceKind,
    remaining_max_commit: Option<CommitVersion>,
    remaining_source_count: usize,
) -> bool {
    if remaining_source_count == 0 {
        return false;
    }
    let Some(selected_commit) = selection.selected_commit_version() else {
        return false;
    };
    if remaining_max_commit.is_none_or(|max_commit| max_commit <= selected_commit) {
        perf_trace::record_branch_point_early_exit(source);
        perf_trace::record_branch_point_remaining_source_skips(remaining_source_count);
        true
    } else {
        false
    }
}

fn remaining_local_max_commit_after_active(
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    bound: BranchReadBound,
) -> Option<CommitVersion> {
    let effective_bound = effective_own_read_bound(bound);
    max_commit_for_frozen_tables(frozen, 0, effective_bound)
        .into_iter()
        .chain(max_commit_for_owned_levels(
            owned_levels,
            0,
            effective_bound,
        ))
        .max()
}

fn remaining_local_max_commit_after_frozen(
    owned_levels: &[Vec<BranchOwnedTable>],
    bound: BranchReadBound,
) -> Option<CommitVersion> {
    let effective_bound = effective_own_read_bound(bound);
    max_commit_for_owned_levels(owned_levels, 0, effective_bound)
}

fn remaining_local_max_commit_after_owned_level(
    owned_levels: &[Vec<BranchOwnedTable>],
    next_level_index: usize,
    bound: BranchReadBound,
) -> Option<CommitVersion> {
    let effective_bound = effective_own_read_bound(bound);
    max_commit_for_owned_levels(owned_levels, next_level_index, effective_bound)
}

fn remaining_max_commit_after_inherited_layer(
    inherited_layers: &[BranchInheritedLayer],
    next_layer_index: usize,
    bound: BranchReadBound,
) -> Option<CommitVersion> {
    max_commit_for_inherited_layers(inherited_layers, next_layer_index, bound)
}

fn max_commit_for_frozen_tables(
    frozen: &[FrozenTable],
    start_index: usize,
    effective_bound: BranchEffectiveReadBound,
) -> Option<CommitVersion> {
    frozen
        .iter()
        .skip(start_index)
        .filter_map(|table| {
            let facts = table.facts();
            cap_commit_range_max(
                facts.min_commit()?,
                facts.max_commit()?,
                effective_bound.max_commit_version(),
            )
        })
        .max()
}

fn max_commit_for_owned_levels(
    owned_levels: &[Vec<BranchOwnedTable>],
    start_level_index: usize,
    effective_bound: BranchEffectiveReadBound,
) -> Option<CommitVersion> {
    owned_levels
        .iter()
        .skip(start_level_index)
        .flat_map(|tables| tables.iter())
        .filter_map(|table| {
            cap_commit_range_max(
                table.facts().commit_range().min(),
                table.facts().commit_range().max(),
                effective_bound.max_commit_version(),
            )
        })
        .max()
}

fn max_commit_for_inherited_layers(
    inherited_layers: &[BranchInheritedLayer],
    start_layer_index: usize,
    bound: BranchReadBound,
) -> Option<CommitVersion> {
    inherited_layers
        .iter()
        .skip(start_layer_index)
        .filter(|layer| layer.is_readable())
        .flat_map(|layer| {
            let inherited_bound =
                BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
            layer
                .owned_levels()
                .iter()
                .flat_map(|tables| tables.iter())
                .filter_map(move |table| {
                    cap_commit_range_max(
                        table.facts().commit_range().min(),
                        table.facts().commit_range().max(),
                        inherited_bound.max_commit_version(),
                    )
                })
        })
        .max()
}

fn cap_commit_range_max(
    min_commit: CommitVersion,
    commit: CommitVersion,
    max_commit_version: Option<CommitVersion>,
) -> Option<CommitVersion> {
    if max_commit_version.is_some_and(|max| min_commit > max) {
        None
    } else {
        Some(max_commit_version.map_or(commit, |max| commit.min(max)))
    }
}

fn remaining_local_source_count_after_active(
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
) -> usize {
    frozen
        .len()
        .saturating_add(owned_point_source_count(owned_levels, 0))
}

fn remaining_local_source_count_after_frozen(owned_levels: &[Vec<BranchOwnedTable>]) -> usize {
    owned_point_source_count(owned_levels, 0)
}

fn remaining_local_source_count_after_owned_level(
    owned_levels: &[Vec<BranchOwnedTable>],
    next_level_index: usize,
) -> usize {
    owned_point_source_count(owned_levels, next_level_index)
}

fn remaining_source_count_after_inherited_layer(
    inherited_layers: &[BranchInheritedLayer],
    next_layer_index: usize,
) -> usize {
    inherited_point_source_count(inherited_layers, next_layer_index)
}

fn owned_point_source_count(
    owned_levels: &[Vec<BranchOwnedTable>],
    start_level_index: usize,
) -> usize {
    owned_levels
        .iter()
        .enumerate()
        .skip(start_level_index)
        .map(|(level_index, tables)| {
            if level_index == 0 {
                tables.len()
            } else {
                usize::from(!tables.is_empty())
            }
        })
        .sum()
}

fn inherited_point_source_count(
    inherited_layers: &[BranchInheritedLayer],
    start_layer_index: usize,
) -> usize {
    inherited_layers
        .iter()
        .skip(start_layer_index)
        .filter(|layer| layer.is_readable())
        .map(|layer| 1usize.saturating_add(owned_point_source_count(layer.owned_levels(), 0)))
        .sum()
}

#[cfg(test)]
#[allow(dead_code)]
fn visible_point_candidates(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    key: &PhysicalKey,
    bound: BranchReadBound,
    effective_bound: BranchEffectiveReadBound,
) -> BranchRuntimeResult<Vec<CandidateRow>> {
    let mut rows = Vec::new();
    let mut rows_visited = 0usize;
    let mut source_counts = perf_trace::BranchPointSourceCounts::default();
    let lookup = TablePreparedPointLookup::new(
        key,
        effective_bound.max_commit_version(),
        effective_bound.max_commit_timestamp(),
    );

    source_counts.active_probes = source_counts.active_probes.saturating_add(1);
    source_counts.table_seeks = source_counts.table_seeks.saturating_add(1);
    let (row, visited) = active.seek_prepared_point(&lookup);
    rows_visited = rows_visited.saturating_add(visited);
    if let Some(row) = row {
        rows.push(candidate_row(
            clone_point_candidate_row(row.as_ref()),
            BranchRowSource::Active,
        ));
    }

    for (index, table) in frozen.iter().enumerate() {
        source_counts.frozen_probes = source_counts.frozen_probes.saturating_add(1);
        source_counts.table_seeks = source_counts.table_seeks.saturating_add(1);
        let (row, visited) = table.seek_prepared_point(&lookup);
        rows_visited = rows_visited.saturating_add(visited);
        if let Some(row) = row {
            rows.push(candidate_row(
                clone_point_candidate_row(row.as_ref()),
                BranchRowSource::Frozen { index },
            ));
        }
    }

    for (level_index, tables) in owned_levels.iter().enumerate() {
        collect_owned_level_point_candidates(
            level_index,
            tables,
            &lookup,
            &mut rows,
            &mut rows_visited,
            &mut source_counts,
        )?;
    }

    collect_visible_inherited_point_candidates(
        branch_id,
        inherited_layers,
        key,
        bound,
        &mut rows,
        &mut rows_visited,
        &mut source_counts,
    )?;
    perf_trace::record_branch_point_sources(source_counts);
    perf_trace::record_point_candidate_collection(rows_visited, rows.len());
    Ok(rows)
}

#[cfg(test)]
#[allow(dead_code)]
fn collect_owned_level_point_candidates(
    level_index: usize,
    tables: &[BranchOwnedTable],
    lookup: &TablePreparedPointLookup,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    source_counts: &mut perf_trace::BranchPointSourceCounts,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for (table_index, table) in tables.iter().enumerate() {
            source_counts.owned_l0_table_probes =
                source_counts.owned_l0_table_probes.saturating_add(1);
            source_counts.table_seeks = source_counts.table_seeks.saturating_add(1);
            let (row, visited) = table.reader().seek_prepared_point(lookup);
            *rows_visited = (*rows_visited).saturating_add(visited);
            if let Some(row) = row {
                rows.push(candidate_row(
                    clone_point_candidate_row(&row),
                    BranchRowSource::OwnedTable {
                        level: table.level(),
                        table_index,
                    },
                ));
            }
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }

    source_counts.owned_nonzero_level_searches =
        source_counts.owned_nonzero_level_searches.saturating_add(1);
    let Some(table_index) = select_nonzero_level_point_table(tables, lookup.physical_key())? else {
        return Ok(());
    };
    let table = &tables[table_index];
    source_counts.owned_nonzero_table_probes =
        source_counts.owned_nonzero_table_probes.saturating_add(1);
    source_counts.table_seeks = source_counts.table_seeks.saturating_add(1);
    let (row, visited) = table.reader().seek_prepared_point(lookup);
    *rows_visited = (*rows_visited).saturating_add(visited);
    if let Some(row) = row {
        rows.push(candidate_row(
            clone_point_candidate_row(&row),
            BranchRowSource::OwnedTable {
                level: table.level(),
                table_index,
            },
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn collect_inherited_level_point_candidates(
    child_branch_id: BranchId,
    layer: &BranchInheritedLayer,
    layer_index: usize,
    level_index: usize,
    tables: &[BranchOwnedTable],
    lookup: &TablePreparedPointLookup,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    source_counts: &mut perf_trace::BranchPointSourceCounts,
) -> BranchRuntimeResult<()> {
    if level_index == 0 {
        for table in tables {
            source_counts.inherited_l0_table_probes =
                source_counts.inherited_l0_table_probes.saturating_add(1);
            append_inherited_point_table_candidate(
                child_branch_id,
                layer,
                layer_index,
                table,
                lookup,
                rows,
                rows_visited,
                source_counts,
            )?;
        }
        return Ok(());
    }

    if tables.is_empty() {
        return Ok(());
    }

    source_counts.inherited_nonzero_level_searches = source_counts
        .inherited_nonzero_level_searches
        .saturating_add(1);
    let Some(table_index) = select_nonzero_level_point_table(tables, lookup.physical_key())? else {
        return Ok(());
    };
    source_counts.inherited_nonzero_table_probes = source_counts
        .inherited_nonzero_table_probes
        .saturating_add(1);
    append_inherited_point_table_candidate(
        child_branch_id,
        layer,
        layer_index,
        &tables[table_index],
        lookup,
        rows,
        rows_visited,
        source_counts,
    )
}

#[cfg(test)]
#[allow(dead_code)]
fn append_inherited_point_table_candidate(
    child_branch_id: BranchId,
    layer: &BranchInheritedLayer,
    layer_index: usize,
    table: &BranchOwnedTable,
    lookup: &TablePreparedPointLookup,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    source_counts: &mut perf_trace::BranchPointSourceCounts,
) -> BranchRuntimeResult<()> {
    source_counts.table_seeks = source_counts.table_seeks.saturating_add(1);
    let (row, visited) = table.reader().seek_prepared_point(lookup);
    *rows_visited = (*rows_visited).saturating_add(visited);
    if let Some(row) = row {
        perf_trace::record_branch_point_candidate_row_clone(row.approximate_size_bytes());
        rows.push(candidate_row(
            rewrite_row_branch(row.row(), layer.source_branch_id(), child_branch_id).map_err(
                |_| BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited row branch rewrite failed",
                },
            )?,
            BranchRowSource::Inherited {
                source_branch_id: layer.source_branch_id(),
                layer_index,
            },
        ));
    }
    Ok(())
}

fn select_nonzero_level_point_table(
    tables: &[BranchOwnedTable],
    target: &[u8],
) -> BranchRuntimeResult<Option<usize>> {
    let mut low = 0usize;
    let mut high = tables.len();
    while low < high {
        let mid = low + (high - low) / 2;
        match compare_physical_key_to_table_range(target, &tables[mid])? {
            Ordering::Less => high = mid,
            Ordering::Greater => low = mid.saturating_add(1),
            Ordering::Equal => return Ok(Some(mid)),
        }
    }
    Ok(None)
}

fn compare_physical_key_to_table_range(
    key: &[u8],
    table: &BranchOwnedTable,
) -> BranchRuntimeResult<Ordering> {
    let (first, last) = table_fact_physical_key_bounds(table)?;
    if key < first.as_slice() {
        Ok(Ordering::Less)
    } else if key > last.as_slice() {
        Ok(Ordering::Greater)
    } else {
        Ok(Ordering::Equal)
    }
}

fn table_fact_physical_key_bounds(
    table: &BranchOwnedTable,
) -> BranchRuntimeResult<(Vec<u8>, Vec<u8>)> {
    let first =
        TableInternalKeyBytes::from_canonical_bytes(table.facts().key_range().first_key().to_vec())
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    let last =
        TableInternalKeyBytes::from_canonical_bytes(table.facts().key_range().last_key().to_vec())
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
    Ok((
        first.physical_key_bytes().to_vec(),
        last.physical_key_bytes().to_vec(),
    ))
}

fn first_nonzero_level_scan_table_index(
    tables: &[BranchOwnedTable],
    bounds: &TableKeyBounds,
) -> BranchRuntimeResult<Option<usize>> {
    if tables.is_empty() {
        return Ok(None);
    }

    let mut low = 0usize;
    let mut high = tables.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if nonzero_table_is_before_scan_bounds(&tables[mid], bounds)? {
            low = mid.saturating_add(1);
        } else {
            high = mid;
        }
    }
    if low >= tables.len() || nonzero_table_is_after_scan_bounds(&tables[low], bounds)? {
        return Ok(None);
    }
    Ok(Some(low))
}

/// Whether every row in `table` is at or below the `after_version` watermark and would therefore be
/// dropped by the selection-time lower bound. Such a table cannot contribute a kept row, so the scan
/// can skip opening it entirely instead of reading and discarding its rows. Returns `false` when no
/// watermark is set.
fn nonzero_table_sealed_at_or_below(
    table: &BranchOwnedTable,
    after_version: Option<CommitVersion>,
) -> bool {
    after_version
        .is_some_and(|watermark| table.facts().commit_range().max().as_u64() <= watermark.as_u64())
}

fn nonzero_table_overlaps_scan_bounds(
    table: &BranchOwnedTable,
    bounds: &TableKeyBounds,
) -> BranchRuntimeResult<bool> {
    Ok(!nonzero_table_is_before_scan_bounds(table, bounds)?
        && !nonzero_table_is_after_scan_bounds(table, bounds)?)
}

fn nonzero_table_is_before_scan_bounds(
    table: &BranchOwnedTable,
    bounds: &TableKeyBounds,
) -> BranchRuntimeResult<bool> {
    let (_, last) = table_fact_physical_key_bounds(table)?;
    Ok(match bounds {
        TableKeyBounds::Range { .. } => false,
        TableKeyBounds::Prefix(prefix) => last.as_slice() < prefix.as_slice(),
        TableKeyBounds::PhysicalRange {
            physical_prefix,
            lower,
            ..
        } => {
            physical_key_is_below_prefix_lower(&last, physical_prefix)
                || physical_key_is_below_lower_bound(&last, lower)
        }
    })
}

fn nonzero_table_is_after_scan_bounds(
    table: &BranchOwnedTable,
    bounds: &TableKeyBounds,
) -> BranchRuntimeResult<bool> {
    let (first, _) = table_fact_physical_key_bounds(table)?;
    Ok(match bounds {
        TableKeyBounds::Range { .. } => false,
        TableKeyBounds::Prefix(prefix) => {
            prefix_upper_bound(prefix).is_some_and(|upper| first.as_slice() >= upper.as_slice())
        }
        TableKeyBounds::PhysicalRange {
            physical_prefix,
            upper,
            ..
        } => {
            physical_key_is_above_prefix_upper(&first, physical_prefix)
                || physical_key_is_above_upper_bound(&first, upper)
        }
    })
}

fn physical_key_is_below_prefix_lower(key: &[u8], prefix: &[u8]) -> bool {
    !prefix.is_empty() && key < prefix
}

fn physical_key_is_above_prefix_upper(key: &[u8], prefix: &[u8]) -> bool {
    prefix_upper_bound(prefix).is_some_and(|upper| key >= upper.as_slice())
}

fn physical_key_is_below_lower_bound(key: &[u8], lower: &TablePhysicalKeyBound) -> bool {
    match lower {
        TablePhysicalKeyBound::Unbounded => false,
        TablePhysicalKeyBound::Included(lower) => key < lower.as_slice(),
        TablePhysicalKeyBound::Excluded(lower) => key <= lower.as_slice(),
    }
}

fn physical_key_is_above_upper_bound(key: &[u8], upper: &TablePhysicalKeyBound) -> bool {
    match upper {
        TablePhysicalKeyBound::Unbounded => false,
        TablePhysicalKeyBound::Included(upper) => key > upper.as_slice(),
        TablePhysicalKeyBound::Excluded(upper) => key >= upper.as_slice(),
    }
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.last_mut() {
        if *last == u8::MAX {
            upper.pop();
            continue;
        }
        *last = last.saturating_add(1);
        return Some(upper);
    }
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BranchScanCursorSource {
    Active,
    Frozen {
        index: usize,
    },
    OwnedTable {
        level: BranchLevel,
        table_index: usize,
    },
    Inherited {
        source_branch_id: BranchId,
        layer_index: usize,
        child_branch_id: BranchId,
    },
}

struct BranchScanCursor<'a> {
    kind: BranchScanCursorKind<'a>,
    effective_bound: BranchEffectiveReadBound,
    current_logical_key: Option<TablePhysicalKeyBytes>,
}

enum BranchScanCursorKind<'a> {
    SingleTable {
        cursor: Box<dyn TableCursor + 'a>,
        source: BranchScanCursorSource,
    },
    NonzeroLevel(Box<BranchNonzeroLevelScanCursor<'a>>),
}

impl<'a> BranchScanCursor<'a> {
    fn new(
        cursor: Box<dyn TableCursor + 'a>,
        source: BranchScanCursorSource,
        effective_bound: BranchEffectiveReadBound,
    ) -> Self {
        Self {
            kind: BranchScanCursorKind::SingleTable { cursor, source },
            effective_bound,
            current_logical_key: None,
        }
    }

    fn nonzero_level(
        tables: &'a [BranchOwnedTable],
        bounds: TableKeyBounds,
        source: BranchNonzeroLevelScanSource,
        first_table_index: usize,
        effective_bound: BranchEffectiveReadBound,
        after_version: Option<CommitVersion>,
    ) -> Self {
        Self {
            kind: BranchScanCursorKind::NonzeroLevel(Box::new(BranchNonzeroLevelScanCursor::new(
                tables,
                bounds,
                source,
                first_table_index,
                after_version,
            ))),
            effective_bound,
            current_logical_key: None,
        }
    }

    fn seek_to_first(&mut self) -> BranchRuntimeResult<()> {
        match &mut self.kind {
            BranchScanCursorKind::SingleTable { cursor, .. } => {
                cursor
                    .seek_to_first()
                    .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            }
            BranchScanCursorKind::NonzeroLevel(cursor) => cursor.seek_to_first()?,
        }
        self.refresh_current_logical_key()
    }

    fn advance(&mut self) -> BranchRuntimeResult<()> {
        match &mut self.kind {
            BranchScanCursorKind::SingleTable { cursor, .. } => {
                cursor
                    .advance()
                    .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            }
            BranchScanCursorKind::NonzeroLevel(cursor) => cursor.advance()?,
        }
        self.refresh_current_logical_key()
    }

    fn current_logical_physical_key(&self) -> Option<&TablePhysicalKeyBytes> {
        self.current_logical_key.as_ref()
    }

    fn refresh_current_logical_key(&mut self) -> BranchRuntimeResult<()> {
        self.current_logical_key = self.compute_current_logical_key()?;
        Ok(())
    }

    fn compute_current_logical_key(&self) -> BranchRuntimeResult<Option<TablePhysicalKeyBytes>> {
        let Some(row) = self.current_row() else {
            return Ok(None);
        };
        let Some(source) = self.current_source() else {
            return Ok(None);
        };
        match source {
            BranchScanCursorSource::Inherited {
                source_branch_id,
                child_branch_id,
                ..
            } => {
                perf_trace::record_scan_logical_key_encode();
                let key = rewrite_physical_key_branch(row.physical_key(), child_branch_id)
                    .map_err(|_| BranchRuntimeError::InvalidInheritedLayer {
                        reason: "inherited scan key branch rewrite failed",
                    })?;
                if key.branch_id() == source_branch_id {
                    return Err(BranchRuntimeError::InvalidInheritedLayer {
                        reason: "inherited scan key rewrite did not change branch",
                    });
                }
                Ok(Some(TablePhysicalKeyBytes::from_physical_key(&key)))
            }
            BranchScanCursorSource::Active
            | BranchScanCursorSource::Frozen { .. }
            | BranchScanCursorSource::OwnedTable { .. } => {
                perf_trace::record_scan_logical_key_encode();
                Ok(Some(TablePhysicalKeyBytes::from_row(row.row())))
            }
        }
    }

    fn current_candidate(&self) -> BranchRuntimeResult<Option<CandidateRow>> {
        let Some(row) = self.current_row() else {
            return Ok(None);
        };
        let Some(source) = self.current_source() else {
            return Ok(None);
        };
        if !self.effective_bound.matches_row(row.row()) {
            return Ok(None);
        }
        match source {
            BranchScanCursorSource::Active => {
                record_scan_candidate_clone(row.row());
                Ok(Some(candidate_row(
                    row.row().clone(),
                    BranchRowSource::Active,
                )))
            }
            BranchScanCursorSource::Frozen { index } => {
                record_scan_candidate_clone(row.row());
                Ok(Some(candidate_row(
                    row.row().clone(),
                    BranchRowSource::Frozen { index },
                )))
            }
            BranchScanCursorSource::OwnedTable { level, table_index } => {
                record_scan_candidate_clone(row.row());
                Ok(Some(candidate_row(
                    row.row().clone(),
                    BranchRowSource::OwnedTable { level, table_index },
                )))
            }
            BranchScanCursorSource::Inherited {
                source_branch_id,
                layer_index,
                child_branch_id,
            } => {
                record_scan_candidate_clone(row.row());
                Ok(Some(candidate_row(
                    rewrite_row_branch(row.row(), source_branch_id, child_branch_id).map_err(
                        |_| BranchRuntimeError::InvalidInheritedLayer {
                            reason: "inherited scan row branch rewrite failed",
                        },
                    )?,
                    BranchRowSource::Inherited {
                        source_branch_id,
                        layer_index,
                    },
                )))
            }
        }
    }

    fn current_row(&self) -> Option<&TableRow> {
        match &self.kind {
            BranchScanCursorKind::SingleTable { cursor, .. } => cursor.current(),
            BranchScanCursorKind::NonzeroLevel(cursor) => cursor.current_row(),
        }
    }

    fn current_source(&self) -> Option<BranchScanCursorSource> {
        match &self.kind {
            BranchScanCursorKind::SingleTable { source, .. } => Some(*source),
            BranchScanCursorKind::NonzeroLevel(cursor) => cursor.current_source(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BranchNonzeroLevelScanSource {
    Owned {
        level: BranchLevel,
    },
    Inherited {
        source_branch_id: BranchId,
        layer_index: usize,
        child_branch_id: BranchId,
    },
}

struct BranchNonzeroLevelScanCursor<'a> {
    tables: &'a [BranchOwnedTable],
    bounds: TableKeyBounds,
    source: BranchNonzeroLevelScanSource,
    first_table_index: usize,
    next_table_index: usize,
    current_table_index: Option<usize>,
    current_cursor: Option<BoundedTableCursor<'a>>,
    after_version: Option<CommitVersion>,
}

impl<'a> BranchNonzeroLevelScanCursor<'a> {
    fn new(
        tables: &'a [BranchOwnedTable],
        bounds: TableKeyBounds,
        source: BranchNonzeroLevelScanSource,
        first_table_index: usize,
        after_version: Option<CommitVersion>,
    ) -> Self {
        Self {
            tables,
            bounds,
            source,
            first_table_index,
            next_table_index: first_table_index,
            current_table_index: None,
            current_cursor: None,
            after_version,
        }
    }

    fn seek_to_first(&mut self) -> BranchRuntimeResult<()> {
        self.next_table_index = self.first_table_index;
        self.current_table_index = None;
        self.current_cursor = None;
        self.open_next_table()
    }

    fn advance(&mut self) -> BranchRuntimeResult<()> {
        let Some(cursor) = &mut self.current_cursor else {
            return Ok(());
        };
        cursor
            .advance()
            .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
        if cursor.current().is_some() {
            return Ok(());
        }
        self.open_next_table()
    }

    fn current_row(&self) -> Option<&TableRow> {
        self.current_cursor.as_ref()?.current()
    }

    fn current_source(&self) -> Option<BranchScanCursorSource> {
        let table_index = self.current_table_index?;
        match self.source {
            BranchNonzeroLevelScanSource::Owned { level } => {
                Some(BranchScanCursorSource::OwnedTable { level, table_index })
            }
            BranchNonzeroLevelScanSource::Inherited {
                source_branch_id,
                layer_index,
                child_branch_id,
            } => Some(BranchScanCursorSource::Inherited {
                source_branch_id,
                layer_index,
                child_branch_id,
            }),
        }
    }

    fn open_next_table(&mut self) -> BranchRuntimeResult<()> {
        self.current_table_index = None;
        self.current_cursor = None;
        while self.next_table_index < self.tables.len() {
            let table_index = self.next_table_index;
            let table = &self.tables[table_index];
            if nonzero_table_is_after_scan_bounds(table, &self.bounds)? {
                self.next_table_index = self.tables.len();
                return Ok(());
            }
            self.next_table_index = self.next_table_index.saturating_add(1);
            if !nonzero_table_overlaps_scan_bounds(table, &self.bounds)? {
                continue;
            }
            if nonzero_table_sealed_at_or_below(table, self.after_version) {
                continue;
            }

            self.record_table_cursor_opened();
            let mut cursor = table.reader().bounded_cursor(self.bounds.clone());
            cursor
                .seek_to_first()
                .map_err(|source| BranchRuntimeError::TableRuntime { source })?;
            if cursor.current().is_some() {
                self.current_table_index = Some(table_index);
                self.current_cursor = Some(cursor);
                return Ok(());
            }
        }
        Ok(())
    }

    fn record_table_cursor_opened(&self) {
        let mut counts = perf_trace::BranchScanSourceCounts::default();
        match self.source {
            BranchNonzeroLevelScanSource::Owned { .. } => {
                counts.owned_nonzero_table_cursors_opened = 1;
            }
            BranchNonzeroLevelScanSource::Inherited { .. } => {
                counts.inherited_nonzero_table_cursors_opened = 1;
            }
        }
        perf_trace::record_branch_scan_sources(counts);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchScanHeapItem {
    key: TablePhysicalKeyBytes,
    source_index: usize,
}

impl Ord for BranchScanHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source_index.cmp(&self.source_index))
    }
}

impl PartialOrd for BranchScanHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "scan source assembly mirrors branch read-view source families"
)]
fn scan_including_tombstones_from_sources(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    bounds: &BranchScanBounds,
    bound: BranchReadBound,
    after_version: Option<CommitVersion>,
    visible_limit: Option<usize>,
    visible_limit_timestamp: Option<Timestamp>,
    record_rows_returned: bool,
) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
    if visible_limit == Some(0) {
        if record_rows_returned {
            perf_trace::record_branch_scan_rows_returned(0);
        }
        return Ok(Vec::new());
    }

    let effective_bound = effective_own_read_bound(bound).with_min_commit_version(after_version);
    let source_setup_timer = perf_trace::start_timer();
    let mut cursors = scan_cursors_for_sources(
        branch_id,
        active,
        frozen,
        owned_levels,
        inherited_layers,
        bounds,
        bound,
        after_version,
    )?;
    for cursor in &mut cursors {
        cursor.seek_to_first()?;
    }
    perf_trace::record_branch_scan_source_cursor_seeks(cursors.len());
    perf_trace::record_branch_scan_source_setup_elapsed(source_setup_timer);

    let merge_timer = perf_trace::start_timer();
    if let [cursor] = cursors.as_mut_slice() {
        let rows = scan_single_source_including_tombstones(
            cursor,
            effective_bound,
            visible_limit,
            visible_limit_timestamp,
        )?;
        if record_rows_returned {
            perf_trace::record_branch_scan_rows_returned(rows.len());
        }
        perf_trace::record_branch_scan_merge_elapsed(merge_timer);
        return Ok(rows);
    }

    let rows = scan_heap_sources_including_tombstones(
        &mut cursors,
        effective_bound,
        visible_limit,
        visible_limit_timestamp,
    )?;
    if record_rows_returned {
        perf_trace::record_branch_scan_rows_returned(rows.len());
    }
    perf_trace::record_branch_scan_merge_elapsed(merge_timer);
    Ok(rows)
}

fn scan_heap_sources_including_tombstones(
    cursors: &mut [BranchScanCursor<'_>],
    effective_bound: BranchEffectiveReadBound,
    visible_limit: Option<usize>,
    visible_limit_timestamp: Option<Timestamp>,
) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
    let mut heap = BinaryHeap::new();
    for (source_index, cursor) in cursors.iter().enumerate() {
        if let Some(key) = cursor.current_logical_physical_key() {
            heap.push(BranchScanHeapItem {
                key: key.clone(),
                source_index,
            });
        }
    }

    let mut rows = Vec::new();
    let mut visible_rows = 0usize;
    let mut candidate_rows = 0usize;
    loop {
        let min_key_timer = perf_trace::start_timer();
        let selected = heap.pop();
        if selected.is_some() {
            perf_trace::record_branch_scan_min_key_elapsed(min_key_timer);
        }
        let Some(selected) = selected else {
            break;
        };
        let selected_key = selected.key;

        let mut candidates = Vec::new();
        candidate_rows = candidate_rows.saturating_add(collect_scan_group_from_cursor(
            &selected_key,
            selected.source_index,
            cursors,
            &mut heap,
            &mut candidates,
        )?);
        while heap.peek().is_some_and(|item| item.key == selected_key) {
            let matching = heap.pop().expect("matching heap item");
            candidate_rows = candidate_rows.saturating_add(collect_scan_group_from_cursor(
                &selected_key,
                matching.source_index,
                cursors,
                &mut heap,
                &mut candidates,
            )?);
        }

        let select_timer = perf_trace::start_timer();
        let selected = select_visible_row_or_tombstone(candidates, effective_bound);
        perf_trace::record_branch_scan_select_elapsed(select_timer);
        if let Some(row) = selected {
            let emit_timer = perf_trace::start_timer();
            let counts_for_limit =
                !row.row().is_tombstone() && !row_is_expired_at(row.row(), visible_limit_timestamp);
            rows.push(row);
            if counts_for_limit {
                visible_rows = visible_rows.saturating_add(1);
                if visible_limit.is_some_and(|limit| visible_rows >= limit) {
                    perf_trace::record_branch_scan_emit_elapsed(emit_timer);
                    break;
                }
            }
            perf_trace::record_branch_scan_emit_elapsed(emit_timer);
        }
    }
    perf_trace::record_scan_candidate_collection(candidate_rows, candidate_rows);
    Ok(rows)
}

fn scan_single_source_including_tombstones(
    cursor: &mut BranchScanCursor<'_>,
    effective_bound: BranchEffectiveReadBound,
    visible_limit: Option<usize>,
    visible_limit_timestamp: Option<Timestamp>,
) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
    let mut rows = Vec::new();
    let mut visible_rows = 0usize;
    let mut candidate_rows = 0usize;
    loop {
        let Some(selected_key) = cursor.current_logical_physical_key().cloned() else {
            break;
        };

        let mut candidates = Vec::new();
        loop {
            let group_key_timer = perf_trace::start_timer();
            let cursor_key = cursor.current_logical_physical_key();
            perf_trace::record_branch_scan_group_key_elapsed(group_key_timer);
            if cursor_key != Some(&selected_key) {
                break;
            }

            let candidate_timer = perf_trace::start_timer();
            let candidate = cursor.current_candidate()?;
            perf_trace::record_branch_scan_candidate_elapsed(candidate_timer);
            if let Some(candidate) = candidate {
                candidate_rows = candidate_rows.saturating_add(1);
                candidates.push(candidate);
            }

            let advance_timer = perf_trace::start_timer();
            cursor.advance()?;
            perf_trace::record_branch_scan_advance_elapsed(advance_timer);
        }

        let select_timer = perf_trace::start_timer();
        let selected = select_visible_row_or_tombstone(candidates, effective_bound);
        perf_trace::record_branch_scan_select_elapsed(select_timer);
        if let Some(row) = selected {
            let emit_timer = perf_trace::start_timer();
            let counts_for_limit =
                !row.row().is_tombstone() && !row_is_expired_at(row.row(), visible_limit_timestamp);
            rows.push(row);
            if counts_for_limit {
                visible_rows = visible_rows.saturating_add(1);
                if visible_limit.is_some_and(|limit| visible_rows >= limit) {
                    perf_trace::record_branch_scan_emit_elapsed(emit_timer);
                    break;
                }
            }
            perf_trace::record_branch_scan_emit_elapsed(emit_timer);
        }
    }
    perf_trace::record_scan_candidate_collection(candidate_rows, candidate_rows);
    Ok(rows)
}

fn collect_scan_group_from_cursor(
    selected_key: &TablePhysicalKeyBytes,
    source_index: usize,
    cursors: &mut [BranchScanCursor<'_>],
    heap: &mut BinaryHeap<BranchScanHeapItem>,
    candidates: &mut Vec<CandidateRow>,
) -> BranchRuntimeResult<usize> {
    let cursor = &mut cursors[source_index];
    let mut candidate_rows = 0usize;
    loop {
        let group_key_timer = perf_trace::start_timer();
        let cursor_key = cursor.current_logical_physical_key();
        perf_trace::record_branch_scan_group_key_elapsed(group_key_timer);
        if cursor_key != Some(selected_key) {
            break;
        }

        let candidate_timer = perf_trace::start_timer();
        let candidate = cursor.current_candidate()?;
        perf_trace::record_branch_scan_candidate_elapsed(candidate_timer);
        if let Some(candidate) = candidate {
            candidate_rows = candidate_rows.saturating_add(1);
            candidates.push(candidate);
        }

        let advance_timer = perf_trace::start_timer();
        cursor.advance()?;
        perf_trace::record_branch_scan_advance_elapsed(advance_timer);
    }
    if let Some(key) = cursor.current_logical_physical_key() {
        heap.push(BranchScanHeapItem {
            key: key.clone(),
            source_index,
        });
    }
    Ok(candidate_rows)
}

fn record_scan_candidate_clone(row: &StorageRow) {
    let bytes = row
        .physical_key()
        .space()
        .len()
        .saturating_add(row.physical_key().user_key().len())
        .saturating_add(row.value().len());
    perf_trace::record_scan_candidate_row_clone(bytes);
}

fn collect_owned_immutable_sources(
    owned_levels: &[Vec<BranchOwnedTable>],
    bounds: &TableKeyBounds,
    effective_bound: BranchEffectiveReadBound,
    fork_version_cap: Option<CommitVersion>,
    sources: &mut Vec<BranchImmutableRowSource>,
) -> BranchRuntimeResult<()> {
    for (level_index, tables) in owned_levels.iter().enumerate() {
        let level = branch_level_from_index(level_index)?;
        for (table_index, table) in tables.iter().enumerate() {
            let rows = collect_immutable_table_rows(
                table,
                bounds,
                effective_bound,
                ImmutableTableSourceKind::Owned { level, table_index },
            )?;
            if rows.is_empty() {
                continue;
            }
            sources.push(BranchImmutableRowSource::new(
                table.descriptor().identity().as_str().to_owned(),
                table.branch_id(),
                table.facts().commit_range().max(),
                fork_version_cap,
                rows,
            ));
        }
    }
    Ok(())
}

fn collect_inherited_immutable_sources(
    child_branch_id: BranchId,
    inherited_layers: &[BranchInheritedLayer],
    bounds: &BranchScanBounds,
    bound: BranchReadBound,
    sources: &mut Vec<BranchImmutableRowSource>,
) -> BranchRuntimeResult<()> {
    for (layer_index, layer) in inherited_layers.iter().enumerate() {
        if !layer.is_readable() {
            continue;
        }
        let source_bounds = bounds.table_key_bounds_for_branch(layer.source_branch_id())?;
        let inherited_bound =
            BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            let level = branch_level_from_index(level_index)?;
            for (table_index, table) in tables.iter().enumerate() {
                let rows = collect_immutable_table_rows(
                    table,
                    &source_bounds,
                    inherited_bound,
                    ImmutableTableSourceKind::Inherited {
                        source_branch_id: layer.source_branch_id(),
                        layer_index,
                        child_branch_id,
                    },
                )?;
                if rows.is_empty() {
                    continue;
                }
                let source_id = format!(
                    "{}@{}:{}:{}",
                    table.descriptor().identity().as_str(),
                    layer.source_branch_id(),
                    level.raw(),
                    table_index
                );
                sources.push(BranchImmutableRowSource::new(
                    source_id,
                    layer.source_branch_id(),
                    table.facts().commit_range().max(),
                    Some(layer.fork_version()),
                    rows,
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ImmutableTableSourceKind {
    Owned {
        level: BranchLevel,
        table_index: usize,
    },
    Inherited {
        source_branch_id: BranchId,
        layer_index: usize,
        child_branch_id: BranchId,
    },
}

fn collect_immutable_table_rows(
    table: &BranchOwnedTable,
    bounds: &TableKeyBounds,
    effective_bound: BranchEffectiveReadBound,
    source: ImmutableTableSourceKind,
) -> BranchRuntimeResult<Vec<BranchHistoryRow>> {
    let mut rows = Vec::new();
    let mut cursor = table.reader().bounded_cursor(bounds.clone());
    cursor
        .seek_to_first()
        .map_err(|_| BranchRuntimeError::InvalidBranchState {
            reason: "immutable source cursor seek failed",
        })?;
    while let Some(row) = cursor.current() {
        if effective_bound.matches_row(row.row()) {
            rows.push(immutable_source_history_row(row.row(), source)?);
        }
        cursor
            .advance()
            .map_err(|_| BranchRuntimeError::InvalidBranchState {
                reason: "immutable source cursor advance failed",
            })?;
    }
    Ok(rows)
}

/// BS4.4a-ii-a: stream a reader's rows through its cursor — in ascending internal-key order, the same
/// order `rows()` yields — applying `f` to each row without materializing the whole table. `f` returning
/// `Err` stops the walk early (so `.any()`-style validations map to an error-returning closure). Cursor
/// seek/advance failures map to `InvalidBranchState`, matching `collect_immutable_table_rows`.
/// C2: no-fill cursor (BS4.4g) — every caller is a full-table streaming walk
/// (observation, validation, snapshot materialization); filling the cache in
/// scan order would overwrite demand recency, and deliberate fill is the
/// preheat's job.
pub(crate) fn try_for_each_reader_row(
    reader: &ImmutableTableReader<'_>,
    mut f: impl FnMut(&TableRow) -> BranchRuntimeResult<()>,
) -> BranchRuntimeResult<()> {
    let mut cursor = reader.cursor_without_cache_fill();
    cursor
        .seek_to_first()
        .map_err(|_| BranchRuntimeError::InvalidBranchState {
            reason: "reader cursor seek failed",
        })?;
    while let Some(row) = cursor.current() {
        f(row)?;
        cursor
            .advance()
            .map_err(|_| BranchRuntimeError::InvalidBranchState {
                reason: "reader cursor advance failed",
            })?;
    }
    Ok(())
}

/// BS4.4g: infallible fold over a reader's rows via its cursor — for the internal shape/summary
/// scans that were `for row in table.rows()` folds. Streams block-by-block, so it never trips the
/// BS4.4d full-materialization guard on a lazy durable reader; panics on a cursor failure, matching
/// the `rows()` it replaces (which `.expect()`d materialization).
pub(crate) fn for_each_reader_row(reader: &ImmutableTableReader<'_>, mut f: impl FnMut(&TableRow)) {
    try_for_each_reader_row(reader, |row| {
        f(row);
        Ok(())
    })
    .expect("reader cursor walk failed");
}

fn immutable_source_history_row(
    row: &StorageRow,
    source: ImmutableTableSourceKind,
) -> BranchRuntimeResult<BranchHistoryRow> {
    match source {
        ImmutableTableSourceKind::Owned { level, table_index } => Ok(BranchHistoryRow::new(
            row.clone(),
            BranchRowSource::OwnedTable { level, table_index },
        )),
        ImmutableTableSourceKind::Inherited {
            source_branch_id,
            layer_index,
            child_branch_id,
        } => Ok(BranchHistoryRow::new(
            rewrite_row_branch(row, source_branch_id, child_branch_id).map_err(|_| {
                BranchRuntimeError::InvalidInheritedLayer {
                    reason: "immutable source row branch rewrite failed",
                }
            })?,
            BranchRowSource::Inherited {
                source_branch_id,
                layer_index,
            },
        )),
    }
}

fn branch_level_from_index(level_index: usize) -> BranchRuntimeResult<BranchLevel> {
    Ok(BranchLevel::new(u8::try_from(level_index).map_err(
        |_| BranchRuntimeError::InvalidBranchState {
            reason: "branch level index exceeds branch level range",
        },
    )?))
}

fn scan_cursors_for_sources<'a>(
    branch_id: BranchId,
    active: &'a MutableTable,
    frozen: &'a [FrozenTable],
    owned_levels: &'a [Vec<BranchOwnedTable>],
    inherited_layers: &'a [BranchInheritedLayer],
    bounds: &BranchScanBounds,
    bound: BranchReadBound,
    after_version: Option<CommitVersion>,
) -> BranchRuntimeResult<Vec<BranchScanCursor<'a>>> {
    let own_bounds = bounds.table_key_bounds()?;
    let own_bound = BranchEffectiveReadBound::for_own_branch(bound);
    let mut cursors = Vec::new();
    let mut source_counts = perf_trace::BranchScanSourceCounts::default();
    push_active_scan_cursor(
        active,
        &own_bounds,
        own_bound,
        &mut cursors,
        &mut source_counts,
    );
    push_frozen_scan_cursors(
        frozen,
        &own_bounds,
        own_bound,
        &mut cursors,
        &mut source_counts,
    );
    push_owned_level_scan_cursors(
        owned_levels,
        &own_bounds,
        own_bound,
        after_version,
        &mut cursors,
        &mut source_counts,
    )?;
    push_inherited_level_scan_cursors(
        branch_id,
        inherited_layers,
        bounds,
        bound,
        after_version,
        &mut cursors,
        &mut source_counts,
    )?;
    perf_trace::record_branch_scan_sources(source_counts);
    Ok(cursors)
}

fn push_active_scan_cursor<'a>(
    active: &'a MutableTable,
    bounds: &TableKeyBounds,
    effective_bound: BranchEffectiveReadBound,
    cursors: &mut Vec<BranchScanCursor<'a>>,
    source_counts: &mut perf_trace::BranchScanSourceCounts,
) {
    cursors.push(BranchScanCursor::new(
        Box::new(BoundedTableCursor::new(
            Box::new(active.cursor()),
            bounds.clone(),
        )),
        BranchScanCursorSource::Active,
        effective_bound,
    ));
    source_counts.active_cursors = source_counts.active_cursors.saturating_add(1);
}

fn push_frozen_scan_cursors<'a>(
    frozen: &'a [FrozenTable],
    bounds: &TableKeyBounds,
    effective_bound: BranchEffectiveReadBound,
    cursors: &mut Vec<BranchScanCursor<'a>>,
    source_counts: &mut perf_trace::BranchScanSourceCounts,
) {
    for (index, table) in frozen.iter().enumerate() {
        cursors.push(BranchScanCursor::new(
            Box::new(BoundedTableCursor::new(
                Box::new(table.cursor()),
                bounds.clone(),
            )),
            BranchScanCursorSource::Frozen { index },
            effective_bound,
        ));
        source_counts.frozen_cursors = source_counts.frozen_cursors.saturating_add(1);
    }
}

fn push_owned_level_scan_cursors<'a>(
    owned_levels: &'a [Vec<BranchOwnedTable>],
    bounds: &TableKeyBounds,
    effective_bound: BranchEffectiveReadBound,
    after_version: Option<CommitVersion>,
    cursors: &mut Vec<BranchScanCursor<'a>>,
    source_counts: &mut perf_trace::BranchScanSourceCounts,
) -> BranchRuntimeResult<()> {
    for (level_index, tables) in owned_levels.iter().enumerate() {
        if level_index == 0 {
            for (table_index, table) in tables.iter().enumerate() {
                if nonzero_table_sealed_at_or_below(table, after_version) {
                    continue;
                }
                cursors.push(BranchScanCursor::new(
                    Box::new(table.reader().bounded_cursor(bounds.clone())),
                    BranchScanCursorSource::OwnedTable {
                        level: table.level(),
                        table_index,
                    },
                    effective_bound,
                ));
                source_counts.owned_l0_cursors = source_counts.owned_l0_cursors.saturating_add(1);
            }
            continue;
        }

        let Some(first_table_index) = first_nonzero_level_scan_table_index(tables, bounds)? else {
            continue;
        };
        let level = BranchLevel::new(u8::try_from(level_index).map_err(|_| {
            BranchRuntimeError::InvalidBranchState {
                reason: "branch scan level index exceeds branch level range",
            }
        })?);
        cursors.push(BranchScanCursor::nonzero_level(
            tables,
            bounds.clone(),
            BranchNonzeroLevelScanSource::Owned { level },
            first_table_index,
            effective_bound,
            after_version,
        ));
        source_counts.owned_nonzero_level_cursors =
            source_counts.owned_nonzero_level_cursors.saturating_add(1);
    }
    Ok(())
}

fn push_inherited_level_scan_cursors<'a>(
    branch_id: BranchId,
    inherited_layers: &'a [BranchInheritedLayer],
    bounds: &BranchScanBounds,
    bound: BranchReadBound,
    after_version: Option<CommitVersion>,
    cursors: &mut Vec<BranchScanCursor<'a>>,
    source_counts: &mut perf_trace::BranchScanSourceCounts,
) -> BranchRuntimeResult<()> {
    for (layer_index, layer) in inherited_layers.iter().enumerate() {
        if !layer.is_readable() {
            continue;
        }
        let inherited_bound =
            BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
        let source_bounds = bounds.table_key_bounds_for_branch(layer.source_branch_id())?;
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            if level_index == 0 {
                for table in tables {
                    if nonzero_table_sealed_at_or_below(table, after_version) {
                        continue;
                    }
                    cursors.push(BranchScanCursor::new(
                        Box::new(table.reader().bounded_cursor(source_bounds.clone())),
                        BranchScanCursorSource::Inherited {
                            source_branch_id: layer.source_branch_id(),
                            layer_index,
                            child_branch_id: branch_id,
                        },
                        inherited_bound,
                    ));
                    source_counts.inherited_l0_cursors =
                        source_counts.inherited_l0_cursors.saturating_add(1);
                }
                continue;
            }

            let Some(first_table_index) =
                first_nonzero_level_scan_table_index(tables, &source_bounds)?
            else {
                continue;
            };
            cursors.push(BranchScanCursor::nonzero_level(
                tables,
                source_bounds.clone(),
                BranchNonzeroLevelScanSource::Inherited {
                    source_branch_id: layer.source_branch_id(),
                    layer_index,
                    child_branch_id: branch_id,
                },
                first_table_index,
                inherited_bound,
                after_version,
            ));
            source_counts.inherited_nonzero_level_cursors = source_counts
                .inherited_nonzero_level_cursors
                .saturating_add(1);
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn collect_visible_inherited_point_candidates(
    branch_id: BranchId,
    inherited_layers: &[BranchInheritedLayer],
    key: &PhysicalKey,
    bound: BranchReadBound,
    rows: &mut Vec<CandidateRow>,
    rows_visited: &mut usize,
    source_counts: &mut perf_trace::BranchPointSourceCounts,
) -> BranchRuntimeResult<()> {
    for (layer_index, layer) in inherited_layers.iter().enumerate() {
        if !layer.is_readable() {
            continue;
        }
        source_counts.inherited_layer_searches =
            source_counts.inherited_layer_searches.saturating_add(1);
        let source_key =
            rewrite_physical_key_branch(key, layer.source_branch_id()).map_err(|_| {
                BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited point key rewrite failed",
                }
            })?;
        perf_trace::record_branch_point_inherited_key_rewrite();
        let inherited_bound =
            BranchEffectiveReadBound::for_inherited_layer(bound, layer.fork_version());
        let lookup = TablePreparedPointLookup::new(
            &source_key,
            inherited_bound.max_commit_version(),
            inherited_bound.max_commit_timestamp(),
        );
        for (level_index, tables) in layer.owned_levels().iter().enumerate() {
            collect_inherited_level_point_candidates(
                branch_id,
                layer,
                layer_index,
                level_index,
                tables,
                &lookup,
                rows,
                rows_visited,
                source_counts,
            )?;
        }
    }
    Ok(())
}

fn require_state_matching_branch(
    state: &BranchLocalState,
    branch_id: BranchId,
) -> BranchRuntimeResult<()> {
    if branch_id == state.branch_id() {
        Ok(())
    } else {
        Err(BranchRuntimeError::InvalidBranchRow {
            reason: "read target branch id does not match read view branch",
        })
    }
}

fn require_state_timestamp_coverage(
    state: &BranchLocalState,
    bound: BranchReadBound,
) -> BranchRuntimeResult<()> {
    match bound {
        BranchReadBound::Latest | BranchReadBound::AtVersion(_) => Ok(()),
        BranchReadBound::AtTimestamp(timestamp) => state.timestamp_coverage().require_timestamp(
            state.branch_id(),
            timestamp,
            BranchTimestampHistorySource::Combined,
        ),
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn select_visible_row(
    mut candidates: Vec<CandidateRow>,
    effective_bound: BranchEffectiveReadBound,
) -> Option<BranchVisibleRow> {
    sort_candidates_newest_first(&mut candidates);
    let candidate = candidates
        .into_iter()
        .find(|candidate| effective_bound.matches_row(candidate_row_ref(candidate)))?;
    record_selected_point_source(candidate_source(&candidate));
    candidate_into_visible_row(candidate, effective_bound.max_commit_timestamp())
}

fn select_visible_row_or_tombstone(
    mut candidates: Vec<CandidateRow>,
    effective_bound: BranchEffectiveReadBound,
) -> Option<BranchHistoryRow> {
    sort_candidates_newest_first(&mut candidates);
    let candidate = candidates
        .into_iter()
        .find(|candidate| effective_bound.matches_row(candidate_row_ref(candidate)))?;
    record_selected_point_source(candidate_source(&candidate));
    if row_is_expired_at(
        candidate_row_ref(&candidate),
        effective_bound.max_commit_timestamp(),
    ) {
        None
    } else {
        Some(candidate_into_history_row(candidate))
    }
}

fn row_is_expired_at(row: &StorageRow, read_timestamp: Option<Timestamp>) -> bool {
    // This layer only applies TTL when the caller supplies an as-of timestamp.
    // Latest and version reads have no wall-clock input at this layer.
    read_timestamp.is_some_and(|timestamp| {
        !row.is_tombstone() && row.expires_at() != Timestamp::EPOCH && row.expires_at() <= timestamp
    })
}

fn record_selected_point_source(source: BranchRowSource) {
    let source = match source {
        BranchRowSource::Active => perf_trace::BranchPointSourceKind::Active,
        BranchRowSource::Frozen { .. } => perf_trace::BranchPointSourceKind::Frozen,
        BranchRowSource::OwnedTable { level, .. } if level == BranchLevel::ZERO => {
            perf_trace::BranchPointSourceKind::OwnedL0
        }
        BranchRowSource::OwnedTable { .. } => perf_trace::BranchPointSourceKind::OwnedNonzero,
        BranchRowSource::Inherited { .. } => perf_trace::BranchPointSourceKind::Inherited,
    };
    perf_trace::record_branch_point_selected(source);
}

fn sort_candidates_newest_first(candidates: &mut [CandidateRow]) {
    candidates.sort_by(|left, right| {
        right
            .0
            .commit_version()
            .as_u64()
            .cmp(&left.0.commit_version().as_u64())
            .then_with(|| source_order_cmp(candidate_source(left), candidate_source(right)))
    });
}

fn source_order_cmp(left: BranchRowSource, right: BranchRowSource) -> Ordering {
    match (left, right) {
        (BranchRowSource::Active, BranchRowSource::Active) => Ordering::Equal,
        (BranchRowSource::Active, _) => Ordering::Less,
        (_, BranchRowSource::Active) => Ordering::Greater,
        (BranchRowSource::Frozen { index: left }, BranchRowSource::Frozen { index: right }) => {
            left.cmp(&right)
        }
        (BranchRowSource::Frozen { .. }, _) => Ordering::Less,
        (_, BranchRowSource::Frozen { .. }) => Ordering::Greater,
        (
            BranchRowSource::OwnedTable {
                level: left_level,
                table_index: left_index,
            },
            BranchRowSource::OwnedTable {
                level: right_level,
                table_index: right_index,
            },
        ) => left_level
            .raw()
            .cmp(&right_level.raw())
            .then_with(|| left_index.cmp(&right_index)),
        (BranchRowSource::OwnedTable { .. }, _) => Ordering::Less,
        (_, BranchRowSource::OwnedTable { .. }) => Ordering::Greater,
        (
            BranchRowSource::Inherited {
                source_branch_id: left_source,
                layer_index: left_index,
            },
            BranchRowSource::Inherited {
                source_branch_id: right_source,
                layer_index: right_index,
            },
        ) => left_index
            .cmp(&right_index)
            .then_with(|| left_source.as_bytes().cmp(right_source.as_bytes())),
    }
}

fn validate_user_key_bound_order(
    lower: &BranchUserKeyBound,
    upper: &BranchUserKeyBound,
) -> BranchRuntimeResult<()> {
    if let (Some(lower), Some(upper)) = (lower.finite(), upper.finite()) {
        if lower > upper {
            return Err(BranchRuntimeError::InvalidReadBound {
                reason: "lower user-key bound must not sort after upper user-key bound",
            });
        }
    }
    Ok(())
}

fn validate_scan_space(
    branch_id: BranchId,
    space: &str,
    storage_space_id: StorageSpaceId,
) -> BranchRuntimeResult<()> {
    PhysicalKey::new(branch_id, space.to_owned(), storage_space_id, Vec::new())
        .map(|_| ())
        .map_err(|_| BranchRuntimeError::InvalidReadBound {
            reason: "scan bounds must use a valid physical key space",
        })
}

fn scan_physical_key(
    branch_id: BranchId,
    space: &str,
    storage_space_id: StorageSpaceId,
    user_key: Vec<u8>,
) -> BranchRuntimeResult<PhysicalKey> {
    PhysicalKey::new(branch_id, space.to_owned(), storage_space_id, user_key).map_err(|_| {
        BranchRuntimeError::InvalidReadBound {
            reason: "scan physical key bound is invalid",
        }
    })
}

fn table_physical_bound_for_user_key(
    branch_id: BranchId,
    space: &str,
    storage_space_id: StorageSpaceId,
    bound: &BranchUserKeyBound,
) -> BranchRuntimeResult<TablePhysicalKeyBound> {
    match bound {
        BranchUserKeyBound::Unbounded => Ok(TablePhysicalKeyBound::Unbounded),
        BranchUserKeyBound::Included(user_key) => {
            let key = scan_physical_key(branch_id, space, storage_space_id, user_key.clone())?;
            Ok(TablePhysicalKeyBound::included(
                TablePhysicalKeyBytes::from_physical_key(&key),
            ))
        }
        BranchUserKeyBound::Excluded(user_key) => {
            let key = scan_physical_key(branch_id, space, storage_space_id, user_key.clone())?;
            Ok(TablePhysicalKeyBound::excluded(
                TablePhysicalKeyBytes::from_physical_key(&key),
            ))
        }
    }
}

fn validate_read_view_inputs(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    facts: BranchStateFacts,
) -> BranchRuntimeResult<()> {
    validate_read_view_source_counts(
        branch_id,
        active,
        frozen,
        owned_levels,
        inherited_layers,
        facts,
    )?;
    validate_owned_levels(owned_levels)?;
    validate_inherited_layers(branch_id, inherited_layers)?;

    perf_trace::record_read_view_validation_scan(read_view_validation_row_count(
        active,
        frozen,
        owned_levels,
        inherited_layers,
    ));

    let mut max_commit_version = None;
    let mut timestamp_min = None;
    let mut timestamp_max = None;
    for row in active.iter() {
        record_read_view_row_facts(
            branch_id,
            row.row(),
            &mut max_commit_version,
            &mut timestamp_min,
            &mut timestamp_max,
        )?;
    }
    for table in frozen {
        for row in table.iter() {
            record_read_view_row_facts(
                branch_id,
                row.row(),
                &mut max_commit_version,
                &mut timestamp_min,
                &mut timestamp_max,
            )?;
        }
    }
    for table in owned_levels.iter().flatten() {
        // BS4.4c: owned tables are branch-validated at construction, so fold their sealed-time
        // aggregates (facts + extras) instead of scanning every row.
        let commit_max = table.facts().commit_range().max();
        let extras = table.extras();
        record_commit_version(&mut max_commit_version, commit_max);
        if let Some(timestamp) = extras.timestamp_min() {
            record_timestamp(&mut timestamp_min, &mut timestamp_max, timestamp);
        }
        if let Some(timestamp) = extras.timestamp_max() {
            record_timestamp(&mut timestamp_min, &mut timestamp_max, timestamp);
        }
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            (
                Some(commit_max),
                extras.timestamp_min(),
                extras.timestamp_max()
            ),
            owned_read_view_facts_by_scan(table),
            "read-view owned facts diverged from a full row scan",
        );
    }
    for layer in inherited_layers {
        record_inherited_layer_read_view_facts(
            layer,
            &mut max_commit_version,
            &mut timestamp_min,
            &mut timestamp_max,
        );
    }
    if max_commit_version != facts.max_commit_version()
        || timestamp_min != facts.timestamp_min()
        || timestamp_max != facts.timestamp_max()
    {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view source facts must match branch facts",
        });
    }
    Ok(())
}

fn validate_read_view_source_counts(
    branch_id: BranchId,
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
    facts: BranchStateFacts,
) -> BranchRuntimeResult<()> {
    if branch_id != facts.branch_id() {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view branch id must match branch facts",
        });
    }
    if facts.active_rows()
        != u64::try_from(active.len()).expect("active read-view row count fits in u64")
    {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view active row count must match branch facts",
        });
    }
    if facts.frozen_table_count() != frozen.len() {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view frozen table count must match branch facts",
        });
    }
    if facts.owned_table_count() != owned_table_count(owned_levels) {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view owned table count must match branch facts",
        });
    }
    if facts.inherited_layer_count() != inherited_layers.len() {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view inherited layer count must match branch facts",
        });
    }
    Ok(())
}

fn read_view_validation_row_count(
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> usize {
    active
        .len()
        .saturating_add(frozen.iter().map(FrozenTable::len).sum::<usize>())
        .saturating_add(
            owned_levels
                .iter()
                .flatten()
                .map(BranchOwnedTable::row_count_usize)
                .sum::<usize>(),
        )
        .saturating_add(
            inherited_layers
                .iter()
                .filter(|layer| layer.status() != InheritedLayerStatus::Materialized)
                .flat_map(|layer| layer.owned_levels().iter().flatten())
                .map(BranchOwnedTable::row_count_usize)
                .sum::<usize>(),
        )
}

fn owned_table_count(owned_levels: &[Vec<BranchOwnedTable>]) -> usize {
    owned_levels.iter().map(Vec::len).sum()
}

pub(super) fn inherited_table_count(layers: &[BranchInheritedLayer]) -> usize {
    layers.iter().map(BranchInheritedLayer::table_count).sum()
}

pub(super) fn source_layout_from_sources(
    active: &MutableTable,
    frozen: &[FrozenTable],
    owned_levels: &[Vec<BranchOwnedTable>],
    inherited_layers: &[BranchInheritedLayer],
) -> BranchSourceLayout {
    let active_rows = active.len();
    let frozen_table_count = frozen.len();
    let frozen_rows = frozen.iter().map(FrozenTable::len).sum();
    let owned_l0_tables = owned_levels.first().map_or(0, Vec::len);
    let owned_nonzero_level_table_counts = nonzero_level_table_counts(owned_levels);
    let owned_total_tables = owned_table_count(owned_levels);

    let mut inherited_active_layers = 0usize;
    let mut inherited_materializing_layers = 0usize;
    let mut inherited_materialized_layers = 0usize;
    let mut inherited_unavailable_layers = 0usize;
    let mut inherited_l0_tables = 0usize;
    let mut inherited_total_tables = 0usize;
    let mut inherited_nonzero_level_totals = vec![0usize; max_level_count(owned_levels)];

    for layer in inherited_layers {
        match layer.status() {
            InheritedLayerStatus::Active => {
                inherited_active_layers = inherited_active_layers.saturating_add(1);
            }
            InheritedLayerStatus::Materializing => {
                inherited_materializing_layers = inherited_materializing_layers.saturating_add(1);
            }
            InheritedLayerStatus::Materialized => {
                inherited_materialized_layers = inherited_materialized_layers.saturating_add(1);
            }
            InheritedLayerStatus::Unavailable => {
                inherited_unavailable_layers = inherited_unavailable_layers.saturating_add(1);
            }
        }
        inherited_l0_tables =
            inherited_l0_tables.saturating_add(layer.owned_levels().first().map_or(0, Vec::len));
        inherited_total_tables = inherited_total_tables.saturating_add(layer.table_count());
        if inherited_nonzero_level_totals.len() < layer.owned_levels().len() {
            inherited_nonzero_level_totals.resize(layer.owned_levels().len(), 0);
        }
        for (level_index, tables) in layer.owned_levels().iter().enumerate().skip(1) {
            inherited_nonzero_level_totals[level_index] =
                inherited_nonzero_level_totals[level_index].saturating_add(tables.len());
        }
    }

    let inherited_nonzero_level_table_counts = inherited_nonzero_level_totals
        .into_iter()
        .enumerate()
        .skip(1)
        .filter(|&(_, table_count)| table_count > 0)
        .map(|(level_index, table_count)| {
            BranchLevelTableCount::new(
                BranchLevel::new(u8::try_from(level_index).expect("branch level fits in u8")),
                table_count,
            )
        })
        .collect::<Vec<_>>();

    BranchSourceLayout::new(
        active_rows,
        frozen_table_count,
        frozen_rows,
        owned_l0_tables,
        owned_nonzero_level_table_counts,
        owned_total_tables,
        inherited_layers.len(),
        inherited_active_layers.saturating_add(inherited_materializing_layers),
        inherited_active_layers,
        inherited_materializing_layers,
        inherited_materialized_layers,
        inherited_unavailable_layers,
        inherited_l0_tables,
        inherited_nonzero_level_table_counts,
        inherited_total_tables,
    )
}

fn nonzero_level_table_counts(levels: &[Vec<BranchOwnedTable>]) -> Vec<BranchLevelTableCount> {
    levels
        .iter()
        .enumerate()
        .skip(1)
        .filter(|(_, tables)| !tables.is_empty())
        .map(|(level_index, tables)| {
            BranchLevelTableCount::new(
                BranchLevel::new(u8::try_from(level_index).expect("branch level fits in u8")),
                tables.len(),
            )
        })
        .collect()
}

fn max_level_count(owned_levels: &[Vec<BranchOwnedTable>]) -> usize {
    owned_levels.len()
}

fn validate_owned_levels(owned_levels: &[Vec<BranchOwnedTable>]) -> BranchRuntimeResult<()> {
    for (level_index, tables) in owned_levels.iter().enumerate() {
        let level = BranchLevel::new(u8::try_from(level_index).map_err(|_| {
            BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned table level index must fit in BranchLevel",
            }
        })?);
        for table in tables {
            if table.level() != level {
                return Err(BranchRuntimeError::InvalidBranchState {
                    reason: "branch-owned table descriptor level must match level index",
                });
            }
        }
        if level != BranchLevel::ZERO {
            validate_non_overlapping_tables(tables)?;
        }
    }
    Ok(())
}

/// BS4.4e: cross-table internal-key uniqueness is a defensive invariant on sealed inherited tables,
/// which compaction already dedups. A full scan per fork/reopen would defeat O(1) fork at billion scale,
/// so release trusts the invariant; debug still verifies it by streaming (guard-safe cursors).
fn validate_inherited_layer_unique_keys(
    owned_levels: &[Vec<BranchOwnedTable>],
) -> BranchRuntimeResult<()> {
    #[cfg(debug_assertions)]
    debug_validate_inherited_layer_unique_keys(owned_levels)?;
    #[cfg(not(debug_assertions))]
    let _ = owned_levels;
    Ok(())
}

#[cfg(debug_assertions)]
fn debug_validate_inherited_layer_unique_keys(
    owned_levels: &[Vec<BranchOwnedTable>],
) -> BranchRuntimeResult<()> {
    let mut keys = BTreeMap::<TableInternalKeyBytes, crate::row::StorageRow>::new();
    for table in owned_levels.iter().flatten() {
        try_for_each_reader_row(table.reader(), |row| {
            match keys.get(row.key()) {
                // #2831 / ACID-005: idempotent recovery replay legitimately
                // leaves the SAME row byte-identical in more than one sealed
                // table (rotated-out source + replayed memtable → re-flush).
                // Identical redundancy is legal — the read path shadows it —
                // and only a DIVERGENT duplicate is a corruption signal
                // (same boundary as the fork-snapshot builder, #2825).
                Some(existing) if existing == row.row() => Ok(()),
                Some(_) => Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited layer tables must not contain divergent duplicate \
                             internal keys",
                }),
                None => {
                    keys.insert(row.key().clone(), row.row().clone());
                    Ok(())
                }
            }
        })?;
    }
    Ok(())
}

/// BS4.4e: does the sealed table's internal-key range prove every row carries `branch_id`? Branch id is
/// the fixed leading 16 bytes and rows are internal-key-sorted, so both endpoints matching ⟹ all rows do.
fn key_range_matches_branch(first_key: &[u8], last_key: &[u8], branch_id: BranchId) -> bool {
    let prefix: &[u8] = branch_id.as_bytes();
    first_key.get(..BranchId::BYTE_LEN) == Some(prefix)
        && last_key.get(..BranchId::BYTE_LEN) == Some(prefix)
}

fn validate_inherited_layer_unique_table_identities(
    owned_levels: &[Vec<BranchOwnedTable>],
) -> BranchRuntimeResult<()> {
    let mut identities = BTreeSet::<&str>::new();
    for table in owned_levels.iter().flatten() {
        if !identities.insert(table.descriptor().identity().as_str()) {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer tables must not contain duplicate table identities",
            });
        }
    }
    Ok(())
}

fn validate_inherited_layers(
    branch_id: BranchId,
    layers: &[BranchInheritedLayer],
) -> BranchRuntimeResult<()> {
    for layer in layers {
        if layer.source_branch_id() == branch_id {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer source branch must differ from child branch",
            });
        }
        if layer.status() == InheritedLayerStatus::Unavailable {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "unavailable inherited layers cannot be read",
            });
        }
        if layer.descriptor().table_count() != layer.table_count() {
            return Err(BranchRuntimeError::InvalidInheritedLayer {
                reason: "inherited layer table count must match descriptor",
            });
        }
        validate_inherited_layer_unique_table_identities(layer.owned_levels())?;
        for table in layer.owned_levels().iter().flatten() {
            if table.branch_id() != layer.source_branch_id() {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited table source branch must match layer",
                });
            }
            // BS4.4e-style O(1) proof (as in `BranchOwnedTable` construction): rows are
            // internal-key-sorted and the branch id is the fixed leading key bytes, so both
            // CRC-protected key-range endpoints carrying the source prefix proves every row does.
            // The per-row rescan below is the debug oracle — as an always-on check it made every
            // read-view validation (including recovery of each fork child's manifest) O(inherited
            // rows) instead of O(tables).
            if !key_range_matches_branch(
                table.facts().key_range().first_key(),
                table.facts().key_range().last_key(),
                layer.source_branch_id(),
            ) {
                return Err(BranchRuntimeError::InvalidInheritedLayer {
                    reason: "inherited table rows must match layer source branch",
                });
            }
            #[cfg(debug_assertions)]
            try_for_each_reader_row(table.reader(), |row| {
                if row.physical_key().branch_id() == layer.source_branch_id() {
                    Ok(())
                } else {
                    Err(BranchRuntimeError::InvalidInheritedLayer {
                        reason: "inherited table rows must match layer source branch",
                    })
                }
            })?;
        }
    }
    Ok(())
}

fn validate_non_overlapping_tables(tables: &[BranchOwnedTable]) -> BranchRuntimeResult<()> {
    for pair in tables.windows(2) {
        let left_first = require_table_physical_first_key(&pair[0])?;
        let right_first = require_table_physical_first_key(&pair[1])?;
        if left_first > right_first || table_physical_ranges_overlap(&pair[0], &pair[1]) {
            return Err(BranchRuntimeError::InvalidBranchState {
                reason: "branch-owned nonzero levels must be sorted and non-overlapping by physical key range",
            });
        }
    }
    Ok(())
}

pub(super) fn table_physical_ranges_overlap(
    left: &BranchOwnedTable,
    right: &BranchOwnedTable,
) -> bool {
    let Some((left_first, left_last)) = table_physical_key_bounds(left) else {
        return false;
    };
    let Some((right_first, right_last)) = table_physical_key_bounds(right) else {
        return false;
    };
    left_first <= right_last && right_first <= left_last
}

pub(super) fn table_physical_first_key(table: &BranchOwnedTable) -> Option<TablePhysicalKeyBytes> {
    table_physical_key_bounds(table).map(|(first, _)| first)
}

pub(super) fn require_table_physical_first_key(
    table: &BranchOwnedTable,
) -> BranchRuntimeResult<TablePhysicalKeyBytes> {
    table_physical_first_key(table).ok_or(BranchRuntimeError::InvalidBranchState {
        reason: "branch-owned table must contain at least one physical key",
    })
}

fn table_physical_key_bounds(
    table: &BranchOwnedTable,
) -> Option<(TablePhysicalKeyBytes, TablePhysicalKeyBytes)> {
    let first_key = table.facts().key_range().first_key();
    let last_key = table.facts().key_range().last_key();
    if first_key.is_empty() || last_key.is_empty() {
        return None;
    }
    Some((
        TablePhysicalKeyBytes::from_encoded_internal_key(first_key),
        TablePhysicalKeyBytes::from_encoded_internal_key(last_key),
    ))
}

fn record_read_view_row_facts(
    branch_id: BranchId,
    row: &StorageRow,
    max_commit_version: &mut Option<CommitVersion>,
    timestamp_min: &mut Option<Timestamp>,
    timestamp_max: &mut Option<Timestamp>,
) -> BranchRuntimeResult<()> {
    if row.physical_key().branch_id() != branch_id {
        return Err(BranchRuntimeError::InvalidBranchState {
            reason: "read view source rows must match the view branch",
        });
    }
    record_commit_version(max_commit_version, row.commit_version());
    record_timestamp(timestamp_min, timestamp_max, row.commit_timestamp());
    Ok(())
}

fn record_inherited_layer_read_view_facts(
    layer: &BranchInheritedLayer,
    max_commit_version: &mut Option<CommitVersion>,
    timestamp_min: &mut Option<Timestamp>,
    timestamp_max: &mut Option<Timestamp>,
) {
    if layer.status() == InheritedLayerStatus::Materialized {
        return;
    }
    // Historical-fork COW: a straddle layer folds its cached `<= fork_version` extremes in O(1) rather
    // than re-scanning the straddle tables on every recompute.
    if let Some(summary) = layer.straddle_read_view_summary() {
        if let Some(version) = summary.max_commit_version() {
            record_commit_version(max_commit_version, version);
        }
        if let Some(timestamp) = summary.timestamp_min() {
            record_timestamp(timestamp_min, timestamp_max, timestamp);
        }
        if let Some(timestamp) = summary.timestamp_max() {
            record_timestamp(timestamp_min, timestamp_max, timestamp);
        }
        return;
    }
    // Non-straddle layer: every table sits entirely at/below the fork, so fold its sealed facts + extras.
    for table in layer.owned_levels().iter().flatten() {
        let commit_max = table.facts().commit_range().max();
        let extras = table.extras();
        record_commit_version(max_commit_version, commit_max);
        if let Some(timestamp) = extras.timestamp_min() {
            record_timestamp(timestamp_min, timestamp_max, timestamp);
        }
        if let Some(timestamp) = extras.timestamp_max() {
            record_timestamp(timestamp_min, timestamp_max, timestamp);
        }
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            (
                Some(commit_max),
                extras.timestamp_min(),
                extras.timestamp_max()
            ),
            owned_read_view_facts_by_scan(table),
            "read-view inherited facts diverged from a full row scan",
        );
    }
}

/// BS4.4c oracle: fold a durable table's read-view facts (max commit version + timestamp bounds) by
/// scanning every row — the reference the facts/extras fast path is `debug_assert`'d against.
#[cfg(debug_assertions)]
fn owned_read_view_facts_by_scan(
    table: &BranchOwnedTable,
) -> (Option<CommitVersion>, Option<Timestamp>, Option<Timestamp>) {
    let mut max_commit_version = None;
    let mut timestamp_min = None;
    let mut timestamp_max = None;
    for row in &table.materialize_rows_for_oracle() {
        record_commit_version(&mut max_commit_version, row.commit_version());
        record_timestamp(
            &mut timestamp_min,
            &mut timestamp_max,
            row.commit_timestamp(),
        );
    }
    (max_commit_version, timestamp_min, timestamp_max)
}

fn record_commit_version(
    max_commit_version: &mut Option<CommitVersion>,
    commit_version: CommitVersion,
) {
    *max_commit_version =
        Some((*max_commit_version).map_or(commit_version, |current| current.max(commit_version)));
}

fn record_timestamp(
    timestamp_min: &mut Option<Timestamp>,
    timestamp_max: &mut Option<Timestamp>,
    commit_timestamp: Timestamp,
) {
    *timestamp_min =
        Some((*timestamp_min).map_or(commit_timestamp, |current| current.min(commit_timestamp)));
    *timestamp_max =
        Some((*timestamp_max).map_or(commit_timestamp, |current| current.max(commit_timestamp)));
}
