//! API read request shells.

use strata_core::{BranchId, CommitVersion, Timestamp};

use super::{ReadLimit, ScanRange, StorageKey, StorageSpaceId, StorageValue};

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadBound {
    Latest,
    AtVersion(CommitVersion),
    AtTimestamp(Timestamp),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointReadRequest {
    branch_id: BranchId,
    storage_space: StorageSpaceId,
    key: StorageKey,
    bound: ReadBound,
}

impl PointReadRequest {
    #[must_use]
    pub const fn new(
        branch_id: BranchId,
        storage_space: StorageSpaceId,
        key: StorageKey,
        bound: ReadBound,
    ) -> Self {
        Self {
            branch_id,
            storage_space,
            key,
            bound,
        }
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn key(&self) -> &StorageKey {
        &self.key
    }

    #[must_use]
    pub const fn bound(&self) -> ReadBound {
        self.bound
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryReadRequest {
    branch_id: BranchId,
    storage_space: StorageSpaceId,
    key: StorageKey,
    before_version: Option<CommitVersion>,
    limit: Option<ReadLimit>,
    include_tombstones: bool,
}

impl HistoryReadRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId, storage_space: StorageSpaceId, key: StorageKey) -> Self {
        Self {
            branch_id,
            storage_space,
            key,
            before_version: None,
            limit: None,
            include_tombstones: true,
        }
    }

    #[must_use]
    pub const fn before_version(mut self, version: CommitVersion) -> Self {
        self.before_version = Some(version);
        self
    }

    #[must_use]
    pub const fn limit(mut self, limit: ReadLimit) -> Self {
        self.limit = Some(limit);
        self
    }

    #[must_use]
    pub const fn include_tombstones(mut self, include_tombstones: bool) -> Self {
        self.include_tombstones = include_tombstones;
        self
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn key(&self) -> &StorageKey {
        &self.key
    }

    #[must_use]
    pub const fn before_version_bound(&self) -> Option<CommitVersion> {
        self.before_version
    }

    #[must_use]
    pub const fn limit_bound(&self) -> Option<ReadLimit> {
        self.limit
    }

    #[must_use]
    pub const fn includes_tombstones(&self) -> bool {
        self.include_tombstones
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefixScanReadRequest {
    branch_id: BranchId,
    storage_space: StorageSpaceId,
    prefix: StorageKey,
    bound: ReadBound,
    limit: Option<ReadLimit>,
    after_version: Option<CommitVersion>,
}

impl PrefixScanReadRequest {
    #[must_use]
    pub const fn new(
        branch_id: BranchId,
        storage_space: StorageSpaceId,
        prefix: StorageKey,
        bound: ReadBound,
        limit: Option<ReadLimit>,
    ) -> Self {
        Self {
            branch_id,
            storage_space,
            prefix,
            bound,
            limit,
            after_version: None,
        }
    }

    /// Restrict the scan to rows whose selected commit version is strictly greater than
    /// `after_version`. This is a generic MVCC lower bound (it carries no product semantics): it
    /// lets a caller read only rows committed after a watermark, skipping immutable sources that
    /// cannot contain a newer version.
    #[must_use]
    pub const fn with_after_version(mut self, after_version: CommitVersion) -> Self {
        self.after_version = Some(after_version);
        self
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn prefix(&self) -> &StorageKey {
        &self.prefix
    }

    #[must_use]
    pub const fn bound(&self) -> ReadBound {
        self.bound
    }

    #[must_use]
    pub const fn limit(&self) -> Option<ReadLimit> {
        self.limit
    }

    #[must_use]
    pub const fn after_version(&self) -> Option<CommitVersion> {
        self.after_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanReadRequest {
    branch_id: BranchId,
    storage_space: StorageSpaceId,
    range: ScanRange,
    bound: ReadBound,
    limit: Option<ReadLimit>,
}

impl ScanReadRequest {
    #[must_use]
    pub const fn new(
        branch_id: BranchId,
        storage_space: StorageSpaceId,
        range: ScanRange,
        bound: ReadBound,
        limit: Option<ReadLimit>,
    ) -> Self {
        Self {
            branch_id,
            storage_space,
            range,
            bound,
            limit,
        }
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn range(&self) -> &ScanRange {
        &self.range
    }

    #[must_use]
    pub const fn bound(&self) -> ReadBound {
        self.bound
    }

    #[must_use]
    pub const fn limit(&self) -> Option<ReadLimit> {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableSourceScanReadRequest {
    branch_id: BranchId,
    storage_space: StorageSpaceId,
    range: ScanRange,
    bound: ReadBound,
}

impl ImmutableSourceScanReadRequest {
    #[must_use]
    pub const fn new(
        branch_id: BranchId,
        storage_space: StorageSpaceId,
        range: ScanRange,
        bound: ReadBound,
    ) -> Self {
        Self {
            branch_id,
            storage_space,
            range,
            bound,
        }
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn range(&self) -> &ScanRange {
        &self.range
    }

    #[must_use]
    pub const fn bound(&self) -> ReadBound {
        self.bound
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampLookupRequest {
    branch_id: BranchId,
    timestamp: Timestamp,
}

impl TimestampLookupRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId, timestamp: Timestamp) -> Self {
        Self {
            branch_id,
            timestamp,
        }
    }

    #[must_use]
    pub const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionLookupRequest {
    branch_id: BranchId,
    version: CommitVersion,
}

impl VersionLookupRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId, version: CommitVersion) -> Self {
        Self { branch_id, version }
    }

    #[must_use]
    pub const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub const fn version(self) -> CommitVersion {
        self.version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimelineBoundsRequest {
    branch_id: BranchId,
}

impl TimelineBoundsRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId) -> Self {
        Self { branch_id }
    }

    #[must_use]
    pub const fn branch_id(self) -> BranchId {
        self.branch_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageReadRow {
    storage_space: StorageSpaceId,
    key: StorageKey,
    value: Option<StorageValue>,
    commit_version: CommitVersion,
    commit_timestamp: Timestamp,
    expires_at: Option<Timestamp>,
    tombstone: bool,
}

impl StorageReadRow {
    #[must_use]
    pub const fn new(
        storage_space: StorageSpaceId,
        key: StorageKey,
        value: Option<StorageValue>,
        commit_version: CommitVersion,
        commit_timestamp: Timestamp,
        expires_at: Option<Timestamp>,
        tombstone: bool,
    ) -> Self {
        Self {
            storage_space,
            key,
            value,
            commit_version,
            commit_timestamp,
            expires_at,
            tombstone,
        }
    }

    #[must_use]
    pub const fn storage_space(&self) -> &StorageSpaceId {
        &self.storage_space
    }

    #[must_use]
    pub const fn key(&self) -> &StorageKey {
        &self.key
    }

    #[must_use]
    /// B4: destructure the row for the engine adapter's move-based point
    /// path — (moved key, moved value, version, timestamp, tombstone).
    /// Rule-32 note: additive public item, cross-crate consumer.
    pub fn into_read_parts(
        self,
    ) -> (
        StorageKey,
        Option<StorageValue>,
        CommitVersion,
        Timestamp,
        bool,
    ) {
        (
            self.key,
            self.value,
            self.commit_version,
            self.commit_timestamp,
            self.tombstone,
        )
    }

    pub const fn value(&self) -> Option<&StorageValue> {
        self.value.as_ref()
    }

    #[must_use]
    pub const fn commit_version(&self) -> CommitVersion {
        self.commit_version
    }

    #[must_use]
    pub const fn commit_timestamp(&self) -> Timestamp {
        self.commit_timestamp
    }

    #[must_use]
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointReadOutcome {
    row: Option<StorageReadRow>,
}

impl PointReadOutcome {
    #[must_use]
    pub const fn new(row: Option<StorageReadRow>) -> Self {
        Self { row }
    }

    /// B4: consume the outcome, moving the row out (rule-32 note: additive
    /// public item for the engine adapter's move-based point path).
    #[must_use]
    pub fn into_row(self) -> Option<StorageReadRow> {
        self.row
    }

    #[must_use]
    pub const fn row(&self) -> Option<&StorageReadRow> {
        self.row.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryReadOutcome {
    rows: Vec<StorageReadRow>,
}

impl HistoryReadOutcome {
    #[must_use]
    pub const fn new(rows: Vec<StorageReadRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    pub fn rows(&self) -> &[StorageReadRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanReadOutcome {
    rows: Vec<StorageReadRow>,
}

impl ScanReadOutcome {
    #[must_use]
    pub const fn new(rows: Vec<StorageReadRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    pub fn rows(&self) -> &[StorageReadRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageImmutableSource {
    source_id: String,
    source_branch_id: BranchId,
    source_generation: CommitVersion,
    fork_version_cap: Option<CommitVersion>,
    rows: Vec<StorageReadRow>,
}

impl StorageImmutableSource {
    #[must_use]
    pub fn new(
        source_id: impl Into<String>,
        source_branch_id: BranchId,
        source_generation: CommitVersion,
        fork_version_cap: Option<CommitVersion>,
        rows: Vec<StorageReadRow>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            source_branch_id,
            source_generation,
            fork_version_cap,
            rows,
        }
    }

    #[must_use]
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    #[must_use]
    pub const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    #[must_use]
    pub const fn source_generation(&self) -> CommitVersion {
        self.source_generation
    }

    #[must_use]
    pub const fn fork_version_cap(&self) -> Option<CommitVersion> {
        self.fork_version_cap
    }

    #[must_use]
    pub fn rows(&self) -> &[StorageReadRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableSourceScanReadOutcome {
    sources: Vec<StorageImmutableSource>,
}

impl ImmutableSourceScanReadOutcome {
    #[must_use]
    pub const fn new(sources: Vec<StorageImmutableSource>) -> Self {
        Self { sources }
    }

    #[must_use]
    pub fn sources(&self) -> &[StorageImmutableSource] {
        &self.sources
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampLookupMiss {
    AfterLatestRetained,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampLookupOutcome {
    query_timestamp: Timestamp,
    matched_version: CommitVersion,
    matched_timestamp: Timestamp,
    miss: Option<TimestampLookupMiss>,
}

impl TimestampLookupOutcome {
    #[must_use]
    pub const fn new(
        query_timestamp: Timestamp,
        matched_version: CommitVersion,
        matched_timestamp: Timestamp,
        miss: Option<TimestampLookupMiss>,
    ) -> Self {
        Self {
            query_timestamp,
            matched_version,
            matched_timestamp,
            miss,
        }
    }

    #[must_use]
    pub const fn query_timestamp(self) -> Timestamp {
        self.query_timestamp
    }

    #[must_use]
    pub const fn matched_version(self) -> CommitVersion {
        self.matched_version
    }

    #[must_use]
    pub const fn matched_timestamp(self) -> Timestamp {
        self.matched_timestamp
    }

    #[must_use]
    pub const fn miss(self) -> Option<TimestampLookupMiss> {
        self.miss
    }
}

/// #3112 S4: the wall-clock instants recorded for a batch of commit versions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitInstantsRequest {
    branch_id: BranchId,
    versions: Vec<CommitVersion>,
}

impl CommitInstantsRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId, versions: Vec<CommitVersion>) -> Self {
        Self {
            branch_id,
            versions,
        }
    }

    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    #[must_use]
    pub fn versions(&self) -> &[CommitVersion] {
        &self.versions
    }
}

/// #3112 S3a: resolve a wall-clock instant to a commit boundary on a branch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WallClockLookupRequest {
    branch_id: BranchId,
    instant: Timestamp,
}

impl WallClockLookupRequest {
    #[must_use]
    pub const fn new(branch_id: BranchId, instant: Timestamp) -> Self {
        Self { branch_id, instant }
    }

    #[must_use]
    pub const fn branch_id(self) -> BranchId {
        self.branch_id
    }

    /// The wall-clock instant being resolved (UTC epoch micros).
    #[must_use]
    pub const fn instant(self) -> Timestamp {
        self.instant
    }
}

/// #3112 S3a: the commit boundary a wall-clock instant resolved to.
///
/// `timestamp` is the LOGICAL commit timestamp — the whole point of resolving
/// up front is that the read then runs as an ordinary `as_of` at this value, so
/// wall-clock and logical time travel cannot drift apart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WallClockLookupOutcome {
    version: CommitVersion,
    timestamp: Timestamp,
    committed_at: Timestamp,
}

impl WallClockLookupOutcome {
    #[must_use]
    pub const fn new(
        version: CommitVersion,
        timestamp: Timestamp,
        committed_at: Timestamp,
    ) -> Self {
        Self {
            version,
            timestamp,
            committed_at,
        }
    }

    #[must_use]
    pub const fn version(self) -> CommitVersion {
        self.version
    }

    /// The logical `as_of` the resolved read runs at.
    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }

    /// The resolved commit's own wall-clock instant — at or before the
    /// requested one, never after.
    #[must_use]
    pub const fn committed_at(self) -> Timestamp {
        self.committed_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionLookupOutcome {
    version: CommitVersion,
    timestamp: Timestamp,
}

impl VersionLookupOutcome {
    #[must_use]
    pub const fn new(version: CommitVersion, timestamp: Timestamp) -> Self {
        Self { version, timestamp }
    }

    #[must_use]
    pub const fn version(self) -> CommitVersion {
        self.version
    }

    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimelineBoundsOutcome {
    min_timestamp: Option<Timestamp>,
    max_timestamp: Option<Timestamp>,
    min_version: Option<CommitVersion>,
    max_version: Option<CommitVersion>,
}

impl TimelineBoundsOutcome {
    #[must_use]
    pub const fn new(
        min_timestamp: Option<Timestamp>,
        max_timestamp: Option<Timestamp>,
        min_version: Option<CommitVersion>,
        max_version: Option<CommitVersion>,
    ) -> Self {
        Self {
            min_timestamp,
            max_timestamp,
            min_version,
            max_version,
        }
    }

    #[must_use]
    pub const fn min_timestamp(self) -> Option<Timestamp> {
        self.min_timestamp
    }

    #[must_use]
    pub const fn max_timestamp(self) -> Option<Timestamp> {
        self.max_timestamp
    }

    #[must_use]
    pub const fn min_version(self) -> Option<CommitVersion> {
        self.min_version
    }

    #[must_use]
    pub const fn max_version(self) -> Option<CommitVersion> {
        self.max_version
    }
}
