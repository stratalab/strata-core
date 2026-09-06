use super::{Bytes, Deserialize, Serialize};

/// Stored value with commit metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VersionedValue {
    value: Bytes,
    version: u64,
    /// Logical commit-timeline position of the commit that wrote this value: a
    /// monotonic per-commit counter (a fresh database starts small, near 1),
    /// never a calendar date. Pass it to `--as-of` and match it against
    /// `history`; do not format it as a Unix/epoch timestamp.
    timestamp: u64,
}

/// Branch status exposed through the command boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    /// Branch accepts reads and writes.
    Active,
    /// Branch was deleted and is hidden from normal listing.
    Deleted,
}

/// Fork parent facts exposed through the command boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BranchParentItem {
    name: String,
    branch_id: String,
    generation: u64,
    fork_version: u64,
    fork_timestamp: Option<u64>,
}

impl BranchParentItem {
    /// Creates branch parent facts.
    pub fn new(
        name: String,
        branch_id: String,
        generation: u64,
        fork_version: u64,
        fork_timestamp: Option<u64>,
    ) -> Self {
        Self {
            name,
            branch_id,
            generation,
            fork_version,
            fork_timestamp,
        }
    }

    /// Returns the parent branch name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the parent branch id.
    pub fn branch_id(&self) -> &str {
        &self.branch_id
    }

    /// Returns the parent branch generation at fork time.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the fork version.
    pub const fn fork_version(&self) -> u64 {
        self.fork_version
    }

    /// Returns the timestamp used to resolve the fork point.
    pub const fn fork_timestamp(&self) -> Option<u64> {
        self.fork_timestamp
    }
}

/// Promotion (merge) lineage exposed through the command boundary: the source
/// branch most recently promoted into this branch and the target commit that
/// incorporated it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BranchMergeItem {
    source_name: String,
    source_branch_id: String,
    source_generation: u64,
    merged_at: u64,
    merged_timestamp: Option<u64>,
}

impl BranchMergeItem {
    /// Creates branch merge-lineage facts.
    pub fn new(
        source_name: String,
        source_branch_id: String,
        source_generation: u64,
        merged_at: u64,
        merged_timestamp: Option<u64>,
    ) -> Self {
        Self {
            source_name,
            source_branch_id,
            source_generation,
            merged_at,
            merged_timestamp,
        }
    }

    /// Returns the promoted source branch name.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the promoted source branch id.
    pub fn source_branch_id(&self) -> &str {
        &self.source_branch_id
    }

    /// Returns the source branch generation at promotion time.
    pub const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    /// Returns the target commit version that incorporated the source.
    pub const fn merged_at(&self) -> u64 {
        self.merged_at
    }

    /// Returns the target commit timestamp, when storage reported it.
    pub const fn merged_timestamp(&self) -> Option<u64> {
        self.merged_timestamp
    }
}

/// Branch summary exposed through the command boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BranchItem {
    name: String,
    branch_id: String,
    generation: u64,
    status: BranchStatus,
    parent: Option<BranchParentItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merge_parent: Option<BranchMergeItem>,
    created_at: Option<u64>,
    deleted_at: Option<u64>,
    state_revision: u64,
}

impl BranchItem {
    /// Creates a branch item.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        branch_id: String,
        generation: u64,
        status: BranchStatus,
        parent: Option<BranchParentItem>,
        merge_parent: Option<BranchMergeItem>,
        created_at: Option<u64>,
        deleted_at: Option<u64>,
        state_revision: u64,
    ) -> Self {
        Self {
            name,
            branch_id,
            generation,
            status,
            parent,
            merge_parent,
            created_at,
            deleted_at,
            state_revision,
        }
    }

    /// Returns the branch name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the branch id.
    pub fn branch_id(&self) -> &str {
        &self.branch_id
    }

    /// Returns the branch generation.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the branch status.
    pub const fn status(&self) -> BranchStatus {
        self.status
    }

    /// Returns fork parent facts, when any.
    pub const fn parent(&self) -> Option<&BranchParentItem> {
        self.parent.as_ref()
    }

    /// Returns the promotion (merge) lineage recorded on this branch, when any.
    pub const fn merge_parent(&self) -> Option<&BranchMergeItem> {
        self.merge_parent.as_ref()
    }

    /// Returns the storage creation version, when known.
    pub const fn created_at(&self) -> Option<u64> {
        self.created_at
    }

    /// Returns the storage deletion version, when known.
    pub const fn deleted_at(&self) -> Option<u64> {
        self.deleted_at
    }

    /// Returns the storage state revision.
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }
}

/// Cleanup facts for branch deletion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BranchCleanupItem {
    removed_refs: u64,
    releasable_tables: u64,
    protected_tables: u64,
}

impl BranchCleanupItem {
    /// Creates branch cleanup facts.
    pub const fn new(removed_refs: u64, releasable_tables: u64, protected_tables: u64) -> Self {
        Self {
            removed_refs,
            releasable_tables,
            protected_tables,
        }
    }

    /// Returns the number of removed references.
    pub const fn removed_refs(self) -> u64 {
        self.removed_refs
    }

    /// Returns the number of releasable tables.
    pub const fn releasable_tables(self) -> u64 {
        self.releasable_tables
    }

    /// Returns the number of protected tables.
    pub const fn protected_tables(self) -> u64 {
        self.protected_tables
    }
}

impl VersionedValue {
    /// Creates a versioned value.
    pub fn new(value: Bytes, version: u64, timestamp: u64) -> Self {
        Self {
            value,
            version,
            timestamp,
        }
    }

    /// Returns the stored value.
    pub const fn value(&self) -> &Bytes {
        &self.value
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Positional batch write result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status, mutation
/// effect, commit receipt, and error; this payload carries only the KV-specific
/// echoed key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchItemResult {
    key: Bytes,
}

impl BatchItemResult {
    /// Creates a batch item result payload.
    pub const fn new(key: Bytes) -> Self {
        Self { key }
    }

    /// Returns the input key.
    pub const fn key(&self) -> &Bytes {
        &self.key
    }
}

/// Positional batch read result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status and error;
/// this payload carries the echoed key and the read facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchGetItemResult {
    key: Bytes,
    found: bool,
    value: Option<Bytes>,
    version: Option<u64>,
    timestamp: Option<u64>,
}

impl BatchGetItemResult {
    /// Creates a batch read result.
    pub fn new(
        key: Bytes,
        value: Option<Bytes>,
        version: Option<u64>,
        timestamp: Option<u64>,
    ) -> Self {
        Self {
            key,
            found: value.is_some(),
            value,
            version,
            timestamp,
        }
    }

    /// Creates a batch read payload for an item that failed validation.
    ///
    /// The failure carries no read facts; the [`BatchItem`](crate::BatchItem)
    /// wrapper carries the error.
    pub const fn not_found(key: Bytes) -> Self {
        Self {
            key,
            found: false,
            value: None,
            version: None,
            timestamp: None,
        }
    }

    /// Returns the input key.
    pub const fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns true when the key exists.
    pub const fn found(&self) -> bool {
        self.found
    }

    /// Returns the stored value, when present.
    pub const fn value(&self) -> Option<&Bytes> {
        if self.found {
            self.value.as_ref()
        } else {
            None
        }
    }

    /// Returns the commit version, when present.
    pub const fn version(&self) -> Option<u64> {
        self.version
    }

    /// Returns the commit timestamp, when present.
    pub const fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }
}

/// Positional batch existence result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status and error;
/// this payload carries the echoed key and the definitive existence answer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchExistsItemResult {
    key: Bytes,
    exists: bool,
}

impl BatchExistsItemResult {
    /// Creates a batch existence result. `exists` is a definitive answer,
    /// so both true and false are `ok` items (never a miss).
    pub const fn new(key: Bytes, exists: bool) -> Self {
        Self { key, exists }
    }

    /// Returns the input key.
    pub const fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns whether the key exists.
    pub const fn exists(&self) -> bool {
        self.exists
    }
}

/// KV scan item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct ScanItem {
    key: Bytes,
    value: Bytes,
    version: u64,
    /// Logical commit-timeline position of the commit that wrote this value: a
    /// monotonic per-commit counter (a fresh database starts small, near 1),
    /// never a calendar date. Pass it to `--as-of` and match it against
    /// `history`; do not format it as a Unix/epoch timestamp.
    timestamp: u64,
}

impl ScanItem {
    /// Creates a scan item.
    pub fn new(key: Bytes, value: Bytes, version: u64, timestamp: u64) -> Self {
        Self {
            key,
            value,
            version,
            timestamp,
        }
    }

    /// Returns the item key.
    pub const fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns the item value.
    pub const fn value(&self) -> &Bytes {
        &self.value
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Version-history item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HistoryItem {
    value: Option<Bytes>,
    tombstone: bool,
    /// The commit version this change was written at — the commit's identity,
    /// distinct from its position in time.
    version: u64,
    /// This change's position on the logical commit timeline: a monotonic
    /// counter assigned per commit, so a fresh database starts small (near 1)
    /// and it is never a calendar date. Pass it to `as_of` to read this exact
    /// point; do not format it as a Unix/epoch timestamp. For when the change
    /// actually happened, use `committed_at`.
    timestamp: u64,
    /// The commit's wall-clock instant in microseconds since the Unix epoch
    /// (UTC), or absent when unknown — a commit written before the database
    /// recorded instants, or one whose date the branch cannot vouch for.
    /// Distinct from `timestamp`, which is a commit-timeline position, not a
    /// date.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_at: Option<u64>,
}

impl HistoryItem {
    /// Creates a history item.
    pub fn new(value: Option<Bytes>, tombstone: bool, version: u64, timestamp: u64) -> Self {
        Self {
            value,
            tombstone,
            version,
            timestamp,
            committed_at: None,
        }
    }

    /// Returns the commit's wall-clock instant (UTC epoch micros), when known.
    pub const fn committed_at(&self) -> Option<u64> {
        self.committed_at
    }

    /// Attaches the commit's wall-clock instant. A builder so `new`'s call
    /// sites stay put (#3112 S4).
    #[must_use]
    pub fn with_committed_at(mut self, committed_at: Option<u64>) -> Self {
        self.committed_at = committed_at;
        self
    }

    /// Returns the item value, when this is not a tombstone.
    pub const fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Returns true when this item represents a delete.
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Version-history result for one key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HistoryResult {
    items: Vec<HistoryItem>,
}

impl HistoryResult {
    /// Creates a version-history result.
    pub const fn new(items: Vec<HistoryItem>) -> Self {
        Self { items }
    }

    /// Returns the number of history items.
    pub const fn count(&self) -> usize {
        self.items.len()
    }

    /// Returns version-history items from newest to oldest.
    pub const fn items(&self) -> &[HistoryItem] {
        self.items.as_slice()
    }

    /// Consumes the result and returns its items.
    pub fn into_items(self) -> Vec<HistoryItem> {
        self.items
    }
}

/// Sampled KV item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct SampleItem {
    key: Bytes,
    value: Bytes,
    version: u64,
    timestamp: u64,
}

impl SampleItem {
    /// Creates a sample item.
    pub fn new(key: Bytes, value: Bytes, version: u64, timestamp: u64) -> Self {
        Self {
            key,
            value,
            version,
            timestamp,
        }
    }

    /// Returns the item key.
    pub const fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns the item value.
    pub const fn value(&self) -> &Bytes {
        &self.value
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// The data capability a branch comparison entry belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ComparedCapability {
    /// The key-value capability.
    KeyValue,
    /// The JSON document capability.
    Json,
    /// The vector capability.
    Vector,
    /// The vector collection configuration capability (comparison only).
    VectorCollection,
    /// The event capability (comparison only).
    Event,
    /// The graph metadata capability (comparison only).
    GraphMetadata,
    /// The graph node capability (comparison only).
    GraphNode,
    /// The graph edge capability (comparison only).
    GraphEdge,
    /// The graph ontology capability (comparison only).
    GraphOntology,
}

/// One entity that differs between two branches, exposed through the command
/// boundary. `identity` is the capability's space-relative logical key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ComparedEntityItem {
    identity: Bytes,
    version: u64,
}

impl ComparedEntityItem {
    /// Creates a compared entity item.
    pub const fn new(identity: Bytes, version: u64) -> Self {
        Self { identity, version }
    }

    /// Returns the entity's space-relative logical key.
    pub const fn identity(&self) -> &Bytes {
        &self.identity
    }

    /// Returns the commit version observed on the reported side.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// The differing entities for one capability within one space. `added` are
/// present on branch B but not A, `removed` on A but not B, `modified` on both
/// with differing values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SpaceComparisonItem {
    space: String,
    capability: ComparedCapability,
    added: Vec<ComparedEntityItem>,
    removed: Vec<ComparedEntityItem>,
    modified: Vec<ComparedEntityItem>,
}

impl SpaceComparisonItem {
    /// Creates a space comparison item.
    pub fn new(
        space: String,
        capability: ComparedCapability,
        added: Vec<ComparedEntityItem>,
        removed: Vec<ComparedEntityItem>,
        modified: Vec<ComparedEntityItem>,
    ) -> Self {
        Self {
            space,
            capability,
            added,
            removed,
            modified,
        }
    }

    /// Returns the space this comparison covers.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Returns the capability this comparison covers.
    pub const fn capability(&self) -> ComparedCapability {
        self.capability
    }

    /// Entities present on branch B but not branch A.
    pub fn added(&self) -> &[ComparedEntityItem] {
        &self.added
    }

    /// Entities present on branch A but not branch B.
    pub fn removed(&self) -> &[ComparedEntityItem] {
        &self.removed
    }

    /// Entities present on both branches with differing values.
    pub fn modified(&self) -> &[ComparedEntityItem] {
        &self.modified
    }
}

/// The result of comparing two branches, exposed through the command boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BranchComparisonItem {
    branch_a: String,
    branch_b: String,
    spaces: Vec<SpaceComparisonItem>,
}

impl BranchComparisonItem {
    /// Creates a branch comparison item.
    pub fn new(branch_a: String, branch_b: String, spaces: Vec<SpaceComparisonItem>) -> Self {
        Self {
            branch_a,
            branch_b,
            spaces,
        }
    }

    /// Returns the first branch of the comparison (the `A` side).
    pub fn branch_a(&self) -> &str {
        &self.branch_a
    }

    /// Returns the second branch of the comparison (the `B` side).
    pub fn branch_b(&self) -> &str {
        &self.branch_b
    }

    /// Returns the per-capability, per-space comparisons.
    pub fn spaces(&self) -> &[SpaceComparisonItem] {
        &self.spaces
    }
}

/// The conflict-resolution strategy for a promotion, exposed through the command
/// boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PromotionStrategy {
    /// Refuse the promotion when any conflict exists.
    #[default]
    Strict,
    /// Apply the source side's value or tombstone for each conflict.
    SourceWins,
}

/// How two branches diverged on one entity since their branch point.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ConflictKind {
    /// Both sides changed the entity to different present values.
    ValueDivergence,
    /// One side changed the value while the other deleted the entity.
    ModifyDeleteDivergence,
    /// The two sides hold structurally incompatible schema for the same entity
    /// (e.g. a vector collection created on both with a different dimension or
    /// metric); no strategy can merge it, so promotion refuses under both.
    IncompatibleCollection,
}

/// What the selected strategy did with a conflict.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ConflictStrategyResult {
    /// The conflict blocked the promotion (`strict`).
    Refused,
    /// The source value or tombstone overwrote the target (`source_wins`).
    SourceWins,
}

/// One entity a promotion applied to the target branch, exposed through the
/// command boundary. `value` is absent for a propagated deletion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PromotedEntityItem {
    capability: ComparedCapability,
    space: String,
    identity: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Bytes>,
}

impl PromotedEntityItem {
    /// Creates a promoted entity item.
    pub const fn new(
        capability: ComparedCapability,
        space: String,
        identity: Bytes,
        value: Option<Bytes>,
    ) -> Self {
        Self {
            capability,
            space,
            identity,
            value,
        }
    }

    /// Returns the capability the promoted entity belongs to.
    pub const fn capability(&self) -> ComparedCapability {
        self.capability
    }

    /// Returns the space the promoted entity belongs to.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Returns the entity's space-relative logical key.
    pub const fn identity(&self) -> &Bytes {
        &self.identity
    }

    /// Returns the value written to the target, or `None` for a deletion.
    pub const fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }
}

/// One conflicting entity a promotion encountered, exposed through the command
/// boundary. `source_value`/`target_value` are absent for a deletion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PreviewConflictItem {
    capability: ComparedCapability,
    space: String,
    identity: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_value: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_value: Option<Bytes>,
    kind: ConflictKind,
    strategy_result: ConflictStrategyResult,
}

impl PreviewConflictItem {
    /// Creates a preview conflict item.
    pub const fn new(
        capability: ComparedCapability,
        space: String,
        identity: Bytes,
        source_value: Option<Bytes>,
        target_value: Option<Bytes>,
        kind: ConflictKind,
        strategy_result: ConflictStrategyResult,
    ) -> Self {
        Self {
            capability,
            space,
            identity,
            source_value,
            target_value,
            kind,
            strategy_result,
        }
    }

    /// Returns the capability the conflicting entity belongs to.
    pub const fn capability(&self) -> ComparedCapability {
        self.capability
    }

    /// Returns the space the conflicting entity belongs to.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Returns the entity's space-relative logical key.
    pub const fn identity(&self) -> &Bytes {
        &self.identity
    }

    /// Returns the source side's value, or `None` if deleted.
    pub const fn source_value(&self) -> Option<&Bytes> {
        self.source_value.as_ref()
    }

    /// Returns the target side's value, or `None` if deleted.
    pub const fn target_value(&self) -> Option<&Bytes> {
        self.target_value.as_ref()
    }

    /// Returns how the two sides diverged.
    pub const fn kind(&self) -> ConflictKind {
        self.kind
    }

    /// Returns what the strategy did with this conflict.
    pub const fn strategy_result(&self) -> ConflictStrategyResult {
        self.strategy_result
    }
}

/// Whether a capability's derived-state rows remain correct after a promotion or
/// need rebuilding, exposed through the command boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DerivedStateDisposition {
    /// The derived rows remain correct and need no work.
    Current,
    /// The derived rows are stale and must be rebuilt before the derived path is
    /// trusted again; the authoritative rows still serve correct results.
    RebuildRequired,
}

/// One capability's derived-state disposition after a promotion or preview.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DerivedStateReportItem {
    capability: ComparedCapability,
    disposition: DerivedStateDisposition,
}

impl DerivedStateReportItem {
    /// Creates a derived-state report item.
    pub const fn new(capability: ComparedCapability, disposition: DerivedStateDisposition) -> Self {
        Self {
            capability,
            disposition,
        }
    }

    /// Returns the capability whose derived rows this report describes.
    pub const fn capability(&self) -> ComparedCapability {
        self.capability
    }

    /// Returns the disposition of the capability's derived rows.
    pub const fn disposition(&self) -> DerivedStateDisposition {
        self.disposition
    }
}

/// The result of promoting one branch into another, exposed through the command
/// boundary. `target_version` is absent when the promotion applied nothing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PromotionOutcomeItem {
    source: String,
    target: String,
    branch_point: u64,
    strategy: PromotionStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_timestamp: Option<u64>,
    applied: Vec<PromotedEntityItem>,
    deleted: Vec<PromotedEntityItem>,
    conflicts: Vec<PreviewConflictItem>,
    #[serde(default)]
    spaces_covered: Vec<String>,
    #[serde(default)]
    capabilities_covered: Vec<ComparedCapability>,
    #[serde(default)]
    capabilities_unsupported: Vec<ComparedCapability>,
    #[serde(default)]
    derived_state: Vec<DerivedStateReportItem>,
}

impl PromotionOutcomeItem {
    /// Creates a promotion outcome item.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: String,
        target: String,
        branch_point: u64,
        strategy: PromotionStrategy,
        target_version: Option<u64>,
        target_timestamp: Option<u64>,
        applied: Vec<PromotedEntityItem>,
        deleted: Vec<PromotedEntityItem>,
        conflicts: Vec<PreviewConflictItem>,
        spaces_covered: Vec<String>,
        capabilities_covered: Vec<ComparedCapability>,
        capabilities_unsupported: Vec<ComparedCapability>,
        derived_state: Vec<DerivedStateReportItem>,
    ) -> Self {
        Self {
            source,
            target,
            branch_point,
            strategy,
            target_version,
            target_timestamp,
            applied,
            deleted,
            conflicts,
            spaces_covered,
            capabilities_covered,
            capabilities_unsupported,
            derived_state,
        }
    }

    /// Returns the branch whose changes were promoted.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the branch that received the promotion.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the derived branch point the promotion merged against.
    pub const fn branch_point(&self) -> u64 {
        self.branch_point
    }

    /// Returns the strategy the promotion was applied under.
    pub const fn strategy(&self) -> PromotionStrategy {
        self.strategy
    }

    /// Returns the target commit version written, or `None` for a no-op.
    pub const fn target_version(&self) -> Option<u64> {
        self.target_version
    }

    /// Returns the target commit timestamp (µs) written, or `None` for a no-op.
    pub const fn target_timestamp(&self) -> Option<u64> {
        self.target_timestamp
    }

    /// Returns the source entities written onto the target.
    pub fn applied(&self) -> &[PromotedEntityItem] {
        &self.applied
    }

    /// Returns the target entities deleted by propagated source deletions.
    pub fn deleted(&self) -> &[PromotedEntityItem] {
        &self.deleted
    }

    /// Returns the entities that diverged on both sides.
    pub fn conflicts(&self) -> &[PreviewConflictItem] {
        &self.conflicts
    }

    /// Returns the spaces the promotion spanned.
    pub fn spaces_covered(&self) -> &[String] {
        &self.spaces_covered
    }

    /// Returns the capabilities the promotion could carry (promotable).
    pub fn capabilities_covered(&self) -> &[ComparedCapability] {
        &self.capabilities_covered
    }

    /// Returns the compare-only capabilities promotion does not carry in V1.
    pub fn capabilities_unsupported(&self) -> &[ComparedCapability] {
        &self.capabilities_unsupported
    }

    /// Returns the derived-state disposition the promotion produced.
    pub fn derived_state(&self) -> &[DerivedStateReportItem] {
        &self.derived_state
    }
}

/// The result of previewing a promotion of `source` into `target`, exposed
/// through the command boundary. Preview is read-only: it reports the conflicts
/// a promotion would hit without mutating either branch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BranchPreviewItem {
    source: String,
    target: String,
    branch_point: u64,
    strategy: PromotionStrategy,
    conflicts: Vec<PreviewConflictItem>,
    #[serde(default)]
    spaces_covered: Vec<String>,
    #[serde(default)]
    capabilities_covered: Vec<ComparedCapability>,
    #[serde(default)]
    capabilities_unsupported: Vec<ComparedCapability>,
    #[serde(default)]
    derived_state: Vec<DerivedStateReportItem>,
}

impl BranchPreviewItem {
    /// Creates a branch preview item.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: String,
        target: String,
        branch_point: u64,
        strategy: PromotionStrategy,
        conflicts: Vec<PreviewConflictItem>,
        spaces_covered: Vec<String>,
        capabilities_covered: Vec<ComparedCapability>,
        capabilities_unsupported: Vec<ComparedCapability>,
        derived_state: Vec<DerivedStateReportItem>,
    ) -> Self {
        Self {
            source,
            target,
            branch_point,
            strategy,
            conflicts,
            spaces_covered,
            capabilities_covered,
            capabilities_unsupported,
            derived_state,
        }
    }

    /// Returns the branch whose changes would be promoted.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the branch that would receive the promotion.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the derived branch point the preview compared against.
    pub const fn branch_point(&self) -> u64 {
        self.branch_point
    }

    /// Returns the strategy the preview was evaluated under.
    pub const fn strategy(&self) -> PromotionStrategy {
        self.strategy
    }

    /// Returns the conflicts a promotion would encounter.
    pub fn conflicts(&self) -> &[PreviewConflictItem] {
        &self.conflicts
    }

    /// Returns whether the promotion is conflict-free.
    pub fn is_clean(&self) -> bool {
        self.conflicts.is_empty()
    }

    /// Returns the spaces the promotion would span.
    pub fn spaces_covered(&self) -> &[String] {
        &self.spaces_covered
    }

    /// Returns the capabilities a promotion could carry (promotable).
    pub fn capabilities_covered(&self) -> &[ComparedCapability] {
        &self.capabilities_covered
    }

    /// Returns the compare-only capabilities promotion does not carry in V1.
    pub fn capabilities_unsupported(&self) -> &[ComparedCapability] {
        &self.capabilities_unsupported
    }

    /// Returns the derived-state disposition a promotion would trigger.
    pub fn derived_state(&self) -> &[DerivedStateReportItem] {
        &self.derived_state
    }
}

#[cfg(test)]
mod branch_comparison_tests {
    use super::{
        BranchComparisonItem, BranchPreviewItem, Bytes, ComparedCapability, ComparedEntityItem,
        ConflictKind, ConflictStrategyResult, DerivedStateDisposition, DerivedStateReportItem,
        PreviewConflictItem, PromotedEntityItem, PromotionOutcomeItem, PromotionStrategy,
        SpaceComparisonItem,
    };

    #[test]
    fn branch_comparison_item_exposes_every_part() {
        let entity = ComparedEntityItem::new(Bytes::from(&b"alpha"[..]), 7);
        assert_eq!(entity.identity(), &Bytes::from(&b"alpha"[..]));
        assert_eq!(entity.version(), 7);

        let space = SpaceComparisonItem::new(
            "default".to_owned(),
            ComparedCapability::Json,
            vec![ComparedEntityItem::new(Bytes::from(&b"add"[..]), 1)],
            vec![ComparedEntityItem::new(Bytes::from(&b"rem"[..]), 2)],
            vec![ComparedEntityItem::new(Bytes::from(&b"mod"[..]), 3)],
        );
        assert_eq!(space.space(), "default");
        assert_eq!(space.capability(), ComparedCapability::Json);
        assert_eq!(space.added().len(), 1);
        assert_eq!(space.added()[0].identity(), &Bytes::from(&b"add"[..]));
        assert_eq!(space.removed()[0].identity(), &Bytes::from(&b"rem"[..]));
        assert_eq!(space.modified()[0].identity(), &Bytes::from(&b"mod"[..]));

        let comparison =
            BranchComparisonItem::new("default".to_owned(), "feature".to_owned(), vec![space]);
        assert_eq!(comparison.branch_a(), "default");
        assert_eq!(comparison.branch_b(), "feature");
        assert_eq!(comparison.spaces().len(), 1);
        assert_eq!(comparison.spaces()[0].space(), "default");
    }

    #[test]
    fn promotion_outcome_item_exposes_every_part() {
        let applied = PromotedEntityItem::new(
            ComparedCapability::KeyValue,
            "default".to_owned(),
            Bytes::from(&b"shared"[..]),
            Some(Bytes::from(&b"src"[..])),
        );
        assert_eq!(applied.capability(), ComparedCapability::KeyValue);
        assert_eq!(applied.space(), "default");
        assert_eq!(applied.identity(), &Bytes::from(&b"shared"[..]));
        assert_eq!(applied.value(), Some(&Bytes::from(&b"src"[..])));

        let deleted = PromotedEntityItem::new(
            ComparedCapability::Json,
            "docs".to_owned(),
            Bytes::from(&b"md"[..]),
            None,
        );
        assert_eq!(deleted.capability(), ComparedCapability::Json);
        assert_eq!(deleted.value(), None);

        let conflict = PreviewConflictItem::new(
            ComparedCapability::KeyValue,
            "default".to_owned(),
            Bytes::from(&b"shared"[..]),
            Some(Bytes::from(&b"src"[..])),
            Some(Bytes::from(&b"tgt"[..])),
            ConflictKind::ValueDivergence,
            ConflictStrategyResult::SourceWins,
        );
        assert_eq!(conflict.capability(), ComparedCapability::KeyValue);
        assert_eq!(conflict.space(), "default");
        assert_eq!(conflict.identity(), &Bytes::from(&b"shared"[..]));
        assert_eq!(conflict.source_value(), Some(&Bytes::from(&b"src"[..])));
        assert_eq!(conflict.target_value(), Some(&Bytes::from(&b"tgt"[..])));
        assert_eq!(conflict.kind(), ConflictKind::ValueDivergence);
        assert_eq!(
            conflict.strategy_result(),
            ConflictStrategyResult::SourceWins
        );

        let outcome = PromotionOutcomeItem::new(
            "feature".to_owned(),
            "default".to_owned(),
            3,
            PromotionStrategy::SourceWins,
            Some(9),
            Some(11),
            vec![applied],
            vec![deleted],
            vec![conflict],
            vec!["default".to_owned()],
            vec![ComparedCapability::KeyValue],
            vec![ComparedCapability::Event],
            vec![DerivedStateReportItem::new(
                ComparedCapability::Json,
                DerivedStateDisposition::RebuildRequired,
            )],
        );
        assert_eq!(outcome.source(), "feature");
        assert_eq!(outcome.target(), "default");
        assert_eq!(outcome.branch_point(), 3);
        assert_eq!(outcome.strategy(), PromotionStrategy::SourceWins);
        assert_eq!(outcome.target_version(), Some(9));
        assert_eq!(outcome.target_timestamp(), Some(11));
        assert_eq!(outcome.applied().len(), 1);
        assert_eq!(
            outcome.applied()[0].identity(),
            &Bytes::from(&b"shared"[..])
        );
        assert_eq!(outcome.deleted().len(), 1);
        assert_eq!(outcome.deleted()[0].identity(), &Bytes::from(&b"md"[..]));
        assert_eq!(outcome.conflicts().len(), 1);
        assert_eq!(outcome.conflicts()[0].kind(), ConflictKind::ValueDivergence);
        assert_eq!(outcome.spaces_covered(), ["default".to_owned()]);
        assert_eq!(
            outcome.capabilities_covered(),
            [ComparedCapability::KeyValue]
        );
        assert_eq!(
            outcome.capabilities_unsupported(),
            [ComparedCapability::Event]
        );
        assert_eq!(outcome.derived_state().len(), 1);
        assert_eq!(
            outcome.derived_state()[0].capability(),
            ComparedCapability::Json
        );
        assert_eq!(
            outcome.derived_state()[0].disposition(),
            DerivedStateDisposition::RebuildRequired
        );
    }

    #[test]
    fn branch_preview_item_exposes_every_part() {
        let conflict = PreviewConflictItem::new(
            ComparedCapability::KeyValue,
            "default".to_owned(),
            Bytes::from(&b"shared"[..]),
            Some(Bytes::from(&b"src"[..])),
            Some(Bytes::from(&b"tgt"[..])),
            ConflictKind::ValueDivergence,
            ConflictStrategyResult::Refused,
        );
        let preview = BranchPreviewItem::new(
            "feature".to_owned(),
            "default".to_owned(),
            3,
            PromotionStrategy::Strict,
            vec![conflict],
            vec!["default".to_owned()],
            vec![ComparedCapability::KeyValue],
            vec![ComparedCapability::GraphNode],
            vec![DerivedStateReportItem::new(
                ComparedCapability::Json,
                DerivedStateDisposition::RebuildRequired,
            )],
        );
        assert_eq!(preview.source(), "feature");
        assert_eq!(preview.target(), "default");
        assert_eq!(preview.branch_point(), 3);
        assert_eq!(preview.strategy(), PromotionStrategy::Strict);
        assert_eq!(preview.conflicts().len(), 1);
        assert!(!preview.is_clean());
        assert_eq!(preview.spaces_covered(), ["default".to_owned()]);
        assert_eq!(
            preview.capabilities_covered(),
            [ComparedCapability::KeyValue]
        );
        assert_eq!(
            preview.capabilities_unsupported(),
            [ComparedCapability::GraphNode]
        );
        assert_eq!(preview.derived_state().len(), 1);
        assert_eq!(
            preview.derived_state()[0].capability(),
            ComparedCapability::Json
        );
        assert_eq!(
            preview.derived_state()[0].disposition(),
            DerivedStateDisposition::RebuildRequired
        );

        let clean = BranchPreviewItem::new(
            "feature".to_owned(),
            "default".to_owned(),
            3,
            PromotionStrategy::Strict,
            vec![],
            vec!["default".to_owned()],
            vec![ComparedCapability::KeyValue],
            vec![ComparedCapability::GraphNode],
            vec![],
        );
        assert!(clean.is_clean());
    }
}
