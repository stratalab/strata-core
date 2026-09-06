use super::{Deserialize, Serialize, Value, VectorDistanceMetric};

/// Vector collection facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorCollectionInfo {
    name: String,
    dimension: u64,
    metric: VectorDistanceMetric,
    count: u64,
}

impl VectorCollectionInfo {
    /// Creates vector collection facts.
    pub fn new(name: String, dimension: u64, metric: VectorDistanceMetric, count: u64) -> Self {
        Self {
            name,
            dimension,
            metric,
            count,
        }
    }

    /// Returns the collection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the embedding dimension.
    pub const fn dimension(&self) -> u64 {
        self.dimension
    }

    /// Returns the distance metric.
    pub const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Returns the visible vector count.
    pub const fn count(&self) -> u64 {
        self.count
    }
}

/// Vector value payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorData {
    embedding: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
}

impl VectorData {
    /// Creates a vector value payload.
    pub fn new(embedding: Vec<f32>, metadata: Option<Value>) -> Self {
        Self {
            embedding,
            metadata,
        }
    }

    /// Returns the embedding.
    pub fn embedding(&self) -> &[f32] {
        &self.embedding
    }

    /// Returns optional metadata.
    pub const fn metadata(&self) -> Option<&Value> {
        self.metadata.as_ref()
    }
}

/// Vector value with commit metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorVersionedData {
    key: String,
    data: VectorData,
    version: u64,
    /// Logical commit-timeline position of the commit that wrote this value: a
    /// monotonic per-commit counter (a fresh database starts small, near 1),
    /// never a calendar date. Pass it to `--as-of` and match it against
    /// `history`; do not format it as a Unix/epoch timestamp.
    timestamp: u64,
    vector_revision: u64,
}

impl VectorVersionedData {
    /// Creates a versioned vector value.
    pub fn new(
        key: String,
        data: VectorData,
        version: u64,
        timestamp: u64,
        vector_revision: u64,
    ) -> Self {
        Self {
            key,
            data,
            version,
            timestamp,
            vector_revision,
        }
    }

    /// Returns the vector key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the vector payload.
    pub const fn data(&self) -> &VectorData {
        &self.data
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the vector revision.
    pub const fn vector_revision(&self) -> u64 {
        self.vector_revision
    }
}

/// Vector history item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorHistoryItem {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<VectorData>,
    /// The commit version this change was written at — the commit's identity,
    /// distinct from its position in time.
    version: u64,
    /// This change's position on the logical commit timeline: a monotonic
    /// counter assigned per commit, so a fresh database starts small (near 1)
    /// and it is never a calendar date. Pass it to `as_of` to read this exact
    /// point; do not format it as a Unix/epoch timestamp. For when the change
    /// actually happened, use `committed_at`.
    timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vector_revision: Option<u64>,
    tombstone: bool,
    /// The commit's wall-clock instant in microseconds since the Unix epoch
    /// (UTC), or absent when unknown — a commit written before the database
    /// recorded instants, or one whose date the branch cannot vouch for.
    /// Distinct from `timestamp`, which is a commit-timeline position, not a
    /// date.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_at: Option<u64>,
}

impl VectorHistoryItem {
    /// Creates a vector history item.
    pub fn new(
        key: String,
        data: Option<VectorData>,
        version: u64,
        timestamp: u64,
        vector_revision: Option<u64>,
        tombstone: bool,
    ) -> Self {
        Self {
            key,
            data,
            version,
            timestamp,
            vector_revision,
            tombstone,
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

    /// Returns the vector key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns vector data when this item is not a tombstone.
    pub const fn data(&self) -> Option<&VectorData> {
        self.data.as_ref()
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the commit timestamp.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the vector revision when present.
    pub const fn vector_revision(&self) -> Option<u64> {
        self.vector_revision
    }

    /// Returns true for delete tombstones.
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

/// Vector history result for one key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorHistoryResult {
    items: Vec<VectorHistoryItem>,
}

impl VectorHistoryResult {
    /// Creates a vector history result.
    pub const fn new(items: Vec<VectorHistoryItem>) -> Self {
        Self { items }
    }

    /// Returns the number of history items.
    pub const fn count(&self) -> usize {
        self.items.len()
    }

    /// Returns vector history items from newest to oldest.
    pub const fn items(&self) -> &[VectorHistoryItem] {
        self.items.as_slice()
    }

    /// Consumes the result and returns its items.
    pub fn into_items(self) -> Vec<VectorHistoryItem> {
        self.items
    }
}

/// One vector search match.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorMatch {
    key: String,
    score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
}

impl VectorMatch {
    /// Creates a vector search match.
    pub fn new(key: String, score: f32, metadata: Option<Value>) -> Self {
        Self {
            key,
            score,
            metadata,
        }
    }

    /// Returns the vector key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the score.
    pub const fn score(&self) -> f32 {
        self.score
    }

    /// Returns optional metadata.
    pub const fn metadata(&self) -> Option<&Value> {
        self.metadata.as_ref()
    }
}

/// One vector index artifact diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorIndexArtifactSource {
    artifact_id: String,
    status: String,
    searched: bool,
}

impl VectorIndexArtifactSource {
    /// Creates one vector index artifact diagnostic.
    pub const fn new(artifact_id: String, status: String, searched: bool) -> Self {
        Self {
            artifact_id,
            status,
            searched,
        }
    }

    /// Returns the artifact identity recorded in the engine manifest.
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Returns the artifact load status.
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns true when this artifact was searched.
    pub const fn searched(&self) -> bool {
        self.searched
    }
}

/// Vector index planner diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorIndexDiagnostics {
    collection: String,
    manifest_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manifest_generation: Option<u64>,
    manifest_ref_count: u64,
    manifest_inherited_ref_count: u64,
    manifest_owned_ref_count: u64,
    active_delta_count: u64,
    policy_mode: String,
    collection_exact_threshold: u64,
    source_flat_threshold: u64,
    source_hnsw_threshold: u64,
    overfetch_factor: u64,
    filtered_underfill_fallback: bool,
    active_delta_seal_threshold: u64,
    hnsw_memory_budget_bytes: u64,
    source_candidate_limit: u64,
    resolved_index_kind_summary: String,
    exact_fallback_count: u64,
    hnsw_graph_builds: u64,
    indexed_source_count: u64,
    exact_source_count: u64,
    flat_source_count: u64,
    hnsw_source_count: u64,
    active_delta_source_count: u64,
    indexed_vector_count: u64,
    derived_bytes: u64,
    last_query_used_index: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_query_fallback_reason: Option<String>,
    artifact_sources: Vec<VectorIndexArtifactSource>,
}

impl VectorIndexDiagnostics {
    /// Creates vector index planner diagnostics.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection: String,
        manifest_status: String,
        manifest_generation: Option<u64>,
        manifest_ref_count: u64,
        manifest_inherited_ref_count: u64,
        manifest_owned_ref_count: u64,
        active_delta_count: u64,
        policy_mode: String,
        collection_exact_threshold: u64,
        source_flat_threshold: u64,
        source_hnsw_threshold: u64,
        overfetch_factor: u64,
        filtered_underfill_fallback: bool,
        active_delta_seal_threshold: u64,
        hnsw_memory_budget_bytes: u64,
        source_candidate_limit: u64,
        resolved_index_kind_summary: String,
        exact_fallback_count: u64,
        hnsw_graph_builds: u64,
        indexed_source_count: u64,
        exact_source_count: u64,
        flat_source_count: u64,
        hnsw_source_count: u64,
        active_delta_source_count: u64,
        indexed_vector_count: u64,
        derived_bytes: u64,
        last_query_used_index: bool,
        last_query_fallback_reason: Option<String>,
        artifact_sources: Vec<VectorIndexArtifactSource>,
    ) -> Self {
        Self {
            collection,
            manifest_status,
            manifest_generation,
            manifest_ref_count,
            manifest_inherited_ref_count,
            manifest_owned_ref_count,
            active_delta_count,
            policy_mode,
            collection_exact_threshold,
            source_flat_threshold,
            source_hnsw_threshold,
            overfetch_factor,
            filtered_underfill_fallback,
            active_delta_seal_threshold,
            hnsw_memory_budget_bytes,
            source_candidate_limit,
            resolved_index_kind_summary,
            exact_fallback_count,
            hnsw_graph_builds,
            indexed_source_count,
            exact_source_count,
            flat_source_count,
            hnsw_source_count,
            active_delta_source_count,
            indexed_vector_count,
            derived_bytes,
            last_query_used_index,
            last_query_fallback_reason,
            artifact_sources,
        }
    }

    /// Returns the searched collection.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Returns the branch-local index manifest status.
    pub fn manifest_status(&self) -> &str {
        &self.manifest_status
    }

    /// Returns the loaded index manifest generation, when available.
    pub const fn manifest_generation(&self) -> Option<u64> {
        self.manifest_generation
    }

    /// Returns the total artifact ref count in the loaded manifest.
    pub const fn manifest_ref_count(&self) -> u64 {
        self.manifest_ref_count
    }

    /// Returns the inherited artifact ref count in the loaded manifest.
    pub const fn manifest_inherited_ref_count(&self) -> u64 {
        self.manifest_inherited_ref_count
    }

    /// Returns the branch-owned artifact ref count in the loaded manifest.
    pub const fn manifest_owned_ref_count(&self) -> u64 {
        self.manifest_owned_ref_count
    }

    /// Returns the active delta count advertised by the loaded manifest.
    pub const fn active_delta_count(&self) -> u64 {
        self.active_delta_count
    }

    /// Returns the resolved planner policy mode.
    pub fn policy_mode(&self) -> &str {
        &self.policy_mode
    }

    /// Returns the collection-level exact-search threshold.
    pub const fn collection_exact_threshold(&self) -> u64 {
        self.collection_exact_threshold
    }

    /// Returns the flat-source threshold.
    pub const fn source_flat_threshold(&self) -> u64 {
        self.source_flat_threshold
    }

    /// Returns the approximate-source threshold.
    pub const fn source_hnsw_threshold(&self) -> u64 {
        self.source_hnsw_threshold
    }

    /// Returns the candidate overfetch factor.
    pub const fn overfetch_factor(&self) -> u64 {
        self.overfetch_factor
    }

    /// Returns true when selective filters can fall back to exact search on underfill.
    pub const fn filtered_underfill_fallback(&self) -> bool {
        self.filtered_underfill_fallback
    }

    /// Returns the active-delta sealing threshold.
    pub const fn active_delta_seal_threshold(&self) -> u64 {
        self.active_delta_seal_threshold
    }

    /// Returns the approximate index memory budget.
    pub const fn hnsw_memory_budget_bytes(&self) -> u64 {
        self.hnsw_memory_budget_bytes
    }

    /// Returns the resolved per-source candidate limit.
    pub const fn source_candidate_limit(&self) -> u64 {
        self.source_candidate_limit
    }

    /// Returns the resolved source kind summary.
    pub fn resolved_index_kind_summary(&self) -> &str {
        &self.resolved_index_kind_summary
    }

    /// Returns true when the query used at least one indexed source.
    pub const fn last_query_used_index(&self) -> bool {
        self.last_query_used_index
    }

    /// Returns the planner fallback reason, if one was recorded.
    pub fn last_query_fallback_reason(&self) -> Option<&str> {
        self.last_query_fallback_reason.as_deref()
    }

    /// Returns the number of exact fallbacks taken by this query.
    pub const fn exact_fallback_count(&self) -> u64 {
        self.exact_fallback_count
    }

    /// Returns the number of approximate index graphs built while answering this query.
    pub const fn hnsw_graph_builds(&self) -> u64 {
        self.hnsw_graph_builds
    }

    /// Returns the number of indexed sources searched by this query.
    pub const fn indexed_source_count(&self) -> u64 {
        self.indexed_source_count
    }

    /// Returns the number of exact sources searched by this query.
    pub const fn exact_source_count(&self) -> u64 {
        self.exact_source_count
    }

    /// Returns the number of flat sources searched by this query.
    pub const fn flat_source_count(&self) -> u64 {
        self.flat_source_count
    }

    /// Returns the number of approximate sources searched by this query.
    pub const fn hnsw_source_count(&self) -> u64 {
        self.hnsw_source_count
    }

    /// Returns the number of active delta sources searched by this query.
    pub const fn active_delta_source_count(&self) -> u64 {
        self.active_delta_source_count
    }

    /// Returns the number of vectors covered by indexed sources.
    pub const fn indexed_vector_count(&self) -> u64 {
        self.indexed_vector_count
    }

    /// Returns the estimated derived bytes touched by indexed sources.
    pub const fn derived_bytes(&self) -> u64 {
        self.derived_bytes
    }

    /// Returns artifact load and search facts.
    pub fn artifact_sources(&self) -> &[VectorIndexArtifactSource] {
        &self.artifact_sources
    }
}

/// Vector index search output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorIndexQueryResult {
    matches: Vec<VectorMatch>,
    diagnostics: VectorIndexDiagnostics,
}

impl VectorIndexQueryResult {
    /// Creates vector index search output.
    pub const fn new(matches: Vec<VectorMatch>, diagnostics: VectorIndexDiagnostics) -> Self {
        Self {
            matches,
            diagnostics,
        }
    }

    /// Returns search matches.
    pub fn matches(&self) -> &[VectorMatch] {
        &self.matches
    }

    /// Returns index planner diagnostics.
    pub const fn diagnostics(&self) -> &VectorIndexDiagnostics {
        &self.diagnostics
    }
}

/// Positional vector batch write/delete result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status, mutation
/// effect, commit receipt, and error; this payload carries only the
/// vector-specific revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorBatchItemResult {
    vector_revision: Option<u64>,
}

impl VectorBatchItemResult {
    /// Creates a vector batch result payload.
    pub const fn new(vector_revision: Option<u64>) -> Self {
        Self { vector_revision }
    }

    /// Returns the vector revision when present.
    pub const fn vector_revision(&self) -> Option<u64> {
        self.vector_revision
    }
}

/// Positional vector batch read result payload.
///
/// The shared [`BatchItem`](crate::BatchItem) wrapper owns the status and error;
/// this payload carries the read facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct VectorBatchGetItemResult {
    found: bool,
    value: Option<VectorVersionedData>,
}

impl VectorBatchGetItemResult {
    /// Creates a vector batch read result.
    pub fn new(value: Option<VectorVersionedData>) -> Self {
        Self {
            found: value.is_some(),
            value,
        }
    }

    /// Creates a vector batch read payload for an item that failed validation.
    ///
    /// The failure carries no read facts; the [`BatchItem`](crate::BatchItem)
    /// wrapper carries the error.
    pub const fn not_found() -> Self {
        Self {
            found: false,
            value: None,
        }
    }

    /// Returns true when the vector entry exists.
    pub const fn found(&self) -> bool {
        self.found
    }

    /// Returns the value when present.
    pub const fn value(&self) -> Option<&VectorVersionedData> {
        if self.found {
            self.value.as_ref()
        } else {
            None
        }
    }
}
