//! Serializable command vocabulary.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{
    ArrowExportPrimitive, ArrowFileFormat, ArrowImportTarget, BatchEventEntry,
    BatchJsonDeleteEntry, BatchJsonEntry, BatchJsonGetEntry, BatchKvEntry, BatchVectorEntry, Bytes,
    EventRangeDirection, GraphAnalyticsBudget, GraphBatchOperation, GraphBindingTarget,
    GraphBulkEdge, GraphBulkNode, GraphDeletePolicy, GraphDirection, GraphEntityBinding,
    GraphPropertyDef, HubDatasetSort, JsonIndexType, PromotionStrategy, VectorDistanceMetric,
    VectorMetadataFilter,
};

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

/// Serializable executor command.
// `Command` is the serialized wire enum; its variants are inherently large
// (chat request bodies, graph node payloads) and are constructed one-at-a-time,
// not held in bulk. Boxing a variant would change the generated JSON Schema
// (schemars does not treat `Box<T>` transparently), so we accept the size spread.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// Lightweight admin liveness check.
    Ping {},
    /// Returns database identity and catalog summary.
    Info {
        /// Branch whose space catalog should be summarized. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    /// Returns control-plane health facts.
    Health {
        /// Branch whose space catalog should be checked. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    /// Returns lightweight database metrics.
    Metrics {
        /// Branch whose space catalog should be counted. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    /// Returns a compact database description.
    Describe {
        /// Branch whose primitive data should be described. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    /// Returns sanitized configuration facts.
    ConfigGet {},
    /// Reports this process's multi-process IPC state (whether it hosts a
    /// broker socket, the socket path, owner pid, and live client count).
    IpcStatus {},
    /// Stops hosting the multi-process broker socket (a client forwards this to
    /// the owner). Idempotent; a non-host reports nothing was stopped.
    IpcStop {},
    /// Reads where this database was cloned from (its remote origin),
    /// when clone recorded one.
    RemoteGet {},
    /// Clones a dataset from a hub into a new local database directory.
    ///
    /// Orchestration (resolution, download, verification, reconstitution,
    /// origin recording) runs once behind this command; every frontend
    /// reaches it here. The session database is not touched.
    HubClone {
        /// Dataset to clone.
        dataset: String,
        /// Branch to fetch. Defaults to the dataset's default branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Destination directory (must not exist, or be empty).
        dest: String,
        /// Explicit hub URL; when absent the 5-layer resolver runs
        /// (flag, `STRATA_HUB_URL`, project config, global config).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
    },
    /// Reads the hub's V1 capability advertisement (`GET /v1/info`).
    HubInfo {
        /// Explicit hub URL; when absent the 5-layer resolver runs
        /// (flag, `STRATA_HUB_URL`, project config, global config).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
    },
    /// Lists hub datasets (`GET /v1/datasets`).
    HubListDatasets {
        /// Explicit hub URL; when absent the 5-layer resolver runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
        /// Task filters.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tasks: Vec<String>,
        /// Tag filters.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
        /// Primitive filters.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        primitives: Vec<String>,
        /// License filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        license: Option<String>,
        /// Minimum dataset size in bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size_min_bytes: Option<u64>,
        /// Maximum dataset size in bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size_max_bytes: Option<u64>,
        /// Sort key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sort: Option<HubDatasetSort>,
        /// Page size.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        /// Zero-based page offset.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<u32>,
    },
    /// Reads one hub dataset card (`GET /v1/datasets/{name}`).
    HubGetDataset {
        /// Dataset slug.
        name: String,
        /// Explicit hub URL; when absent the 5-layer resolver runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
    },
    /// Lists refs for one hub dataset (`GET /v1/datasets/{name}/refs`).
    HubListRefs {
        /// Dataset slug.
        dataset: String,
        /// Explicit hub URL; when absent the 5-layer resolver runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
    },
    /// Lists yanked hub refs (`GET /v1/yanked`).
    HubListYanked {
        /// RFC 3339 lower-bound timestamp.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<String>,
        /// Explicit hub URL; when absent the 5-layer resolver runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_url: Option<String>,
    },
    /// Returns one sanitized configuration value by key.
    ConfigureGetKey {
        /// Config key.
        key: String,
    },
    /// Lists product spaces for a branch.
    SpaceList {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    /// Creates a product space for a branch.
    ///
    /// # Guaranteed semantics
    ///
    /// - **Idempotent success.** Creating a space that already exists is not
    ///   an error: the command succeeds with `created: false` and no mutation
    ///   effect. `created: true` is reported only by the call that first
    ///   materialized the space.
    /// - **Immediate visibility.** Once the command returns success, the
    ///   space is visible to every subsequent command on any handle of the
    ///   same database — `SpaceExists` reports `true` and data commands
    ///   targeting the space are accepted.
    SpaceCreate {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Product space name.
        space: String,
    },
    /// Checks whether a product space exists for a branch.
    SpaceExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Product space name.
        space: String,
    },
    /// Deletes a product space from a branch.
    SpaceDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Product space name.
        space: String,
        /// Delete visible data in the space before dropping the catalog entry.
        #[serde(default, skip_serializing_if = "is_false")]
        force: bool,
    },
    /// Lists active branches.
    BranchList {},
    /// Reads one branch summary.
    BranchGet {
        /// Branch name.
        branch: String,
    },
    /// Compares two branches.
    BranchDiff {
        /// The first branch (the `A` side).
        branch_a: String,
        /// The second branch (the `B` side).
        branch_b: String,
        /// Optional read-as-of commit timestamp: compare each branch as of the
        /// `timestamp` from `history` output (a commit-timeline position, not
        /// the `version`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at_timestamp: Option<u64>,
    },
    /// Creates an empty root branch.
    BranchCreate {
        /// Branch name.
        branch: String,
    },
    /// Forks a branch from the current source head.
    BranchForkCurrent {
        /// Source branch name.
        source: String,
        /// Destination branch name.
        branch: String,
    },
    /// Forks a branch from a retained source version.
    BranchForkAtVersion {
        /// Source branch name.
        source: String,
        /// Destination branch name.
        branch: String,
        /// Source version.
        version: u64,
    },
    /// Forks a branch from a retained source timestamp.
    BranchForkAtTimestamp {
        /// Source branch name.
        source: String,
        /// Destination branch name.
        branch: String,
        /// Source timestamp in microseconds.
        timestamp: u64,
    },
    /// Deletes an active branch.
    BranchDelete {
        /// Branch name.
        branch: String,
    },
    /// Promotes one branch's changes into another as a single atomic commit.
    BranchMerge {
        /// The branch whose changes are promoted.
        source: String,
        /// The branch that receives the promotion.
        target: String,
        /// Conflict-resolution strategy (`strict` refuses on conflict).
        #[serde(default)]
        strategy: PromotionStrategy,
    },
    /// Previews promoting one branch into another, reporting conflicts without
    /// mutating either branch.
    BranchPreview {
        /// The branch whose changes would be promoted.
        source: String,
        /// The branch that would receive the promotion.
        target: String,
        /// Conflict-resolution strategy to evaluate the preview under.
        #[serde(default)]
        strategy: PromotionStrategy,
    },
    /// Writes one KV entry.
    ///
    /// # Guaranteed semantics
    ///
    /// - **Atomic per key.** The write is a single engine commit: it either
    ///   applies fully (value plus a new commit version) or not at all. No
    ///   reader ever observes a partial or torn value.
    /// - **Read-after-write visibility.** Once the command returns success,
    ///   every subsequent read through any handle of the same database
    ///   observes this write (or a newer one) — including immediately, from
    ///   the handle that issued it. Acknowledged writes are never
    ///   transiently invisible.
    KvPut {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key bytes.
        key: Bytes,
        /// Value bytes.
        value: Bytes,
    },
    /// Reads one KV entry.
    KvGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key bytes.
        key: Bytes,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Deletes one KV entry.
    KvDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key bytes.
        key: Bytes,
    },
    /// Lists KV keys.
    KvList {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<Bytes>,
        /// Optional key cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<Bytes>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Scans KV rows.
    KvScan {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional inclusive start key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<Bytes>,
        /// Optional row limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Writes multiple KV entries in one engine commit.
    KvBatchPut {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entries to write.
        entries: Vec<BatchKvEntry>,
    },
    /// Reads multiple KV entries.
    KvBatchGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Keys to read.
        keys: Vec<Bytes>,
    },
    /// Deletes multiple KV entries in one engine commit.
    KvBatchDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Keys to delete.
        keys: Vec<Bytes>,
    },
    /// Checks multiple keys for existence.
    KvBatchExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Keys to check.
        keys: Vec<Bytes>,
    },
    /// Checks one key for existence.
    KvExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to check.
        key: Bytes,
    },
    /// Reads full version history for one key.
    KvHistory {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to read.
        key: Bytes,
    },
    /// Counts keys.
    KvCount {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<Bytes>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Samples keys and values.
    KvSample {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<Bytes>,
        /// Optional sample count. Defaults to 10.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u64>,
    },
    /// Sets a JSON value at a document path, creating the document when missing.
    JsonSet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
        /// JSON value.
        value: Value,
    },
    /// Reads a JSON value at a document path.
    JsonGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Deletes a whole JSON document or one JSON path.
    JsonDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path.
        path: String,
    },
    /// Reads full JSON document version history.
    JsonHistory {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
    },
    /// Checks whether a JSON document exists.
    JsonExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
    },
    /// Batch-checks JSON document existence.
    JsonBatchExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document keys to check.
        keys: Vec<String>,
    },
    /// Sets multiple JSON values in one engine commit.
    JsonBatchSet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entries to set.
        entries: Vec<BatchJsonEntry>,
    },
    /// Reads multiple JSON values.
    JsonBatchGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entries to read.
        entries: Vec<BatchJsonGetEntry>,
    },
    /// Deletes multiple JSON documents or paths.
    JsonBatchDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entries to delete.
        entries: Vec<BatchJsonDeleteEntry>,
    },
    /// Lists JSON document keys.
    ///
    /// # Guaranteed semantics
    ///
    /// - **Ordering.** Keys are returned in ascending byte-lexicographic
    ///   order, stable across calls for unchanged data.
    /// - **Cursor.** The returned cursor is the last key of a non-terminal
    ///   page; resuming lists strictly *after* that key. Cursors are plain
    ///   positions: they stay valid indefinitely, across interleaved writes,
    ///   and even if the cursor document itself is deleted. A key is never
    ///   returned twice for the same cursor chain.
    /// - **Interleaved writes.** Each page reads the latest committed state
    ///   unless `as_of` is set: documents created behind the cursor position
    ///   do not appear; documents created or deleted ahead of it are
    ///   reflected in later pages. For a snapshot-stable enumeration across
    ///   pages, pass the same `as_of` timestamp on every page.
    /// - **Termination.** The terminal page reports `has_more: false` and
    ///   `cursor: null`. A `limit` of zero returns an empty terminal page.
    JsonList {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional document key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional document key cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Scans JSON documents.
    JsonScan {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional inclusive start document key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        /// Optional row limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Counts JSON documents.
    JsonCount {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional document key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Samples JSON documents.
    JsonSample {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional document key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional sample count. Defaults to 10.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u64>,
    },
    /// Creates a JSON secondary index.
    JsonCreateIndex {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Index name.
        name: String,
        /// Indexed field path.
        field_path: String,
        /// Index kind.
        index_type: JsonIndexType,
    },
    /// Drops a JSON secondary index.
    JsonDropIndex {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Index name.
        name: String,
    },
    /// Lists JSON secondary indexes.
    JsonListIndexes {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
    },
    /// Creates a vector collection.
    VectorCreateCollection {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Embedding dimension.
        dimension: u64,
        /// Distance metric.
        metric: VectorDistanceMetric,
    },
    /// Deletes a vector collection.
    VectorDeleteCollection {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
    },
    /// Lists vector collections.
    VectorListCollections {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
    },
    /// Reads vector collection facts.
    VectorCollectionStats {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
    },
    /// Counts visible vectors in one collection.
    VectorCount {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Samples vectors.
    VectorSample {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Optional sample count. Defaults to 10.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u64>,
    },
    /// Upserts one vector.
    VectorUpsert {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Dense embedding. Accepted at wire (f64) precision and narrowed to the
        /// stored f32; a value that underflows or overflows f32 is rejected.
        vector: Vec<f64>,
        /// Optional metadata.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    /// Reads one vector.
    VectorGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Reads full vector history.
    VectorHistory {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// Checks whether one vector exists.
    VectorExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// Batch-checks vector key existence.
    VectorBatchExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector keys to check.
        keys: Vec<String>,
    },
    /// Lists vector keys.
    VectorListKeys {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional key cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Scans vectors.
    VectorScan {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Optional inclusive start key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<String>,
        /// Optional row limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Updates vector metadata.
    VectorUpdateMetadata {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Top-level metadata patch.
        patch: Value,
    },
    /// Deletes one vector.
    VectorDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },
    /// Deletes vectors matching a metadata filter.
    VectorDeleteByFilter {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Metadata filter.
        filter: VectorMetadataFilter,
    },
    /// Deletes all visible vectors in one collection.
    VectorDeleteAll {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
    },
    /// Runs vector search with the default engine planner.
    VectorQuery {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Query embedding. Accepted at wire (f64) precision and narrowed to the
        /// searched f32; a value that underflows or overflows f32 is rejected.
        query: Vec<f64>,
        /// Maximum number of matches.
        k: u64,
        /// Optional metadata filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<VectorMetadataFilter>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Runs vector search and returns index planner diagnostics.
    VectorIndexQuery {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Query embedding. Accepted at wire (f64) precision and narrowed to the
        /// searched f32; a value that underflows or overflows f32 is rejected.
        query: Vec<f64>,
        /// Maximum number of matches.
        k: u64,
        /// Optional metadata filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<VectorMetadataFilter>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Upserts multiple vectors.
    VectorBatchUpsert {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Entries to write.
        entries: Vec<BatchVectorEntry>,
    },
    /// Reads multiple vectors.
    VectorBatchGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Keys to read.
        keys: Vec<String>,
    },
    /// Deletes multiple vectors.
    VectorBatchDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Keys to delete.
        keys: Vec<String>,
    },
    /// Appends multiple events in one engine commit.
    EventBatchAppend {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Events to append.
        entries: Vec<BatchEventEntry>,
    },
    /// Appends one event.
    EventAppend {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event type.
        event_type: String,
        /// Event payload.
        payload: Value,
    },
    /// Reads one event by sequence.
    EventGet {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event sequence.
        sequence: u64,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Checks whether one event sequence exists.
    EventExists {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event sequence.
        sequence: u64,
    },
    /// Counts visible events.
    EventCount {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Reads an event sequence range.
    EventRange {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Inclusive lower bound of the sequence window (same in both directions).
        start_seq: u64,
        /// Optional exclusive upper bound of the sequence window (same in both directions).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_seq: Option<u64>,
        /// Optional item limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Result ordering.
        direction: EventRangeDirection,
        /// Optional event type filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_type: Option<String>,
    },
    /// Reads an event timestamp range.
    EventRangeByTime {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Inclusive start timestamp in microseconds.
        start_ts: u64,
        /// Optional exclusive end timestamp in microseconds (half-open window,
        /// matching the sequence-addressed range's exclusive end).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_ts: Option<u64>,
        /// Optional item limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Result ordering.
        direction: EventRangeDirection,
        /// Optional event type filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_type: Option<String>,
    },
    /// Lists event types.
    EventListTypes {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Lists events.
    EventList {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional event type filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_type: Option<String>,
        /// Optional item limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Optional exclusive sequence cursor.
        #[serde(default, alias = "cursor", skip_serializing_if = "Option::is_none")]
        after_sequence: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Verifies visible event density and hash linkage.
    EventVerifyChain {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
    },
    /// Creates a graph.
    GraphCreate {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
    },
    /// Deletes a graph and its visible graph rows.
    GraphDelete {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
    },
    /// Lists graphs.
    GraphList {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional exclusive graph cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Reads graph metadata.
    GraphGetMeta {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Adds or replaces a graph node.
    GraphAddNode {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Optional node properties.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<Value>,
        /// Optional entity binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<GraphEntityBinding>,
        /// Optional declared object type (validated once the ontology is frozen).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_type: Option<String>,
    },
    /// Reads a graph node.
    GraphGetNode {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Deletes a graph node and incident edges.
    GraphRemoveNode {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
    },
    /// Lists graph nodes.
    GraphListNodes {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional node id prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional exclusive node id cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Samples graph nodes.
    GraphSample {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional sample count. Defaults to 10.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u64>,
    },
    /// Adds or replaces a graph edge.
    GraphAddEdge {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Optional edge weight. Defaults to 1.0.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weight: Option<f64>,
        /// Optional edge properties.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<Value>,
    },
    /// Reads a graph edge.
    GraphGetEdge {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Deletes a graph edge.
    GraphRemoveEdge {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
    },
    /// Lists neighboring graph nodes.
    GraphNeighbors {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Traversal direction.
        direction: GraphDirection,
        /// Optional edge type filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_type: Option<String>,
        /// Optional exclusive cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Lists graph nodes bound to one entity target.
    GraphBindingsForEntity {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entity target to search for.
        target: GraphBindingTarget,
        /// Optional exclusive cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Applies graph mutations in one engine commit.
    GraphBatchWrite {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Batch operations.
        operations: Vec<GraphBatchOperation>,
    },
    /// Defines (or, while the ontology is draft, redefines) an object type.
    GraphDefineObjectType {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Object type name.
        name: String,
        /// Declared properties by name.
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    },
    /// Defines (or, while the ontology is draft, redefines) a link type.
    GraphDefineLinkType {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Link type name.
        name: String,
        /// Declared source object type.
        source: String,
        /// Declared target object type.
        target: String,
        /// Optional cardinality: one of `one-to-one`, `one-to-many`,
        /// `many-to-one`, or `many-to-many`. Unknown values are rejected.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cardinality: Option<String>,
        /// Declared properties by name.
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        properties: std::collections::BTreeMap<String, GraphPropertyDef>,
    },
    /// Deletes a draft object type.
    GraphDeleteObjectType {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Object type name.
        name: String,
    },
    /// Deletes a draft link type.
    GraphDeleteLinkType {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Link type name.
        name: String,
    },
    /// Freezes the ontology after validating it; writes then enforce it.
    GraphFreezeOntology {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
    },
    /// Reads the graph's ontology (status plus every declared type).
    GraphGetOntology {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Reads the ontology with per-type node and edge usage counts.
    GraphOntologySummary {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Lists nodes declaring an object type (node-id ordered).
    GraphNodesByType {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Object type name.
        object_type: String,
        /// Optional exclusive node id cursor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Optional item limit. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Computes weakly connected components over a graph snapshot.
    GraphWcc {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Computes local clustering coefficients over a graph snapshot.
    GraphLcc {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Computes shortest-path distances from a source node.
    GraphSssp {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Source node id.
        source: String,
        /// Optional traversal direction. Defaults to outgoing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<GraphDirection>,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Computes `PageRank` scores, optionally personalized by seed weights.
    GraphPagerank {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional damping factor. Defaults to 0.85.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        damping: Option<f64>,
        /// Optional iteration bound. Defaults to 20.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_iterations: Option<u64>,
        /// Optional convergence tolerance. Defaults to 1e-6.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tolerance: Option<f64>,
        /// Optional seed weights (node id to weight). When present, both
        /// teleport and dangling mass follow the seeds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        personalization: Option<std::collections::BTreeMap<String, f64>>,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Detects communities via label propagation.
    GraphCdlp {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Optional iteration bound. Defaults to 10.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_iterations: Option<u64>,
        /// Optional propagation direction. Defaults to both.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<GraphDirection>,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Runs a bounded breadth-first traversal from a start node.
    GraphBfs {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Start node id.
        start: String,
        /// Optional depth bound. Defaults to 100.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<u64>,
        /// Optional visited-node bound. Defaults to 10000.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_nodes: Option<u64>,
        /// Optional edge-type restriction applied at every hop.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_types: Option<Vec<String>>,
        /// Optional traversal direction. Defaults to outgoing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<GraphDirection>,
        /// Optional snapshot size bounds. Defaults to the engine limits.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        budget: Option<GraphAnalyticsBudget>,
        /// Read as of a position on the logical commit timeline — the
        /// `timestamp` from `history` output, not the `version`, and never a
        /// calendar date. Reads the graph state visible at that timeline
        /// position. To read as of a real time, use `as_of_time` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        /// Read as of a real time: a wall-clock instant in microseconds since
        /// the Unix epoch (UTC), as reported by `committed_at` on a write ack
        /// or on any `history` row. Resolves to the commit at or before that
        /// instant, and fails rather than guessing if the instant falls
        /// outside the branch's recorded history. Mutually exclusive with
        /// `as_of`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of_time: Option<u64>,
    },
    /// Ingests nodes and edges in chunked commits.
    GraphBulkInsert {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Graph name.
        graph: String,
        /// Nodes to upsert (committed before edges).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        nodes: Vec<GraphBulkNode>,
        /// Edges to upsert; endpoints must exist or arrive in `nodes`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        edges: Vec<GraphBulkEdge>,
        /// Optional items-per-commit chunk size. Defaults to 512;
        /// values above 800 clamp so one chunk fits one storage commit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chunk_size: Option<u64>,
    },
    /// Applies an explicit delete policy to graph facts bound to an entity.
    GraphApplyDeletePolicy {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// The bound entity target.
        target: GraphBindingTarget,
        /// Policy to apply: `cascade`, `detach`, or `keep_dangling`.
        policy: GraphDeletePolicy,
    },
    /// Imports an Arrow-compatible file into a product primitive.
    ArrowImport {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Input file path.
        file_path: String,
        /// Input file format. Defaults to extension detection.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<ArrowFileFormat>,
        /// Product primitive to import into.
        target: ArrowImportTarget,
        /// Optional key column override.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_column: Option<String>,
        /// Optional value, document, or embedding column override.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
        /// Target vector collection for vector imports.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collection: Option<String>,
        /// Target graph for graph imports.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        graph: Option<String>,
    },
    /// Exports a product primitive to an Arrow-compatible file.
    ArrowExport {
        /// Target branch. Defaults to the executor handle branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Target product space. Defaults to `"default"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Product primitive to export.
        primitive: ArrowExportPrimitive,
        /// Output file format.
        format: ArrowFileFormat,
        /// Output file path. Graph exports treat this as a stem and return concrete node and edge paths.
        path: String,
        /// Optional key, document, vector-key, or node-id prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional row limit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Target vector collection for vector exports.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collection: Option<String>,
        /// Target graph for graph exports.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        graph: Option<String>,
        /// Optional event type filter for event exports.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_type: Option<String>,
    },
    /// Lists catalog models known to the inference runtime.
    #[cfg(feature = "inference")]
    InferenceModelsList {},
    /// Lists locally available inference models.
    #[cfg(feature = "inference")]
    InferenceModelsLocal {},
    /// Pulls an inference model into the local model directory.
    #[cfg(feature = "inference")]
    InferenceModelsPull {
        /// Model spec or catalog name.
        model: String,
    },
    /// Returns capability facts for one inference model spec.
    #[cfg(feature = "inference")]
    InferenceModelCapability {
        /// Model spec.
        model: String,
    },
    /// Generates text with an inference model.
    #[cfg(feature = "inference")]
    InferenceGenerate {
        /// Model spec.
        model: String,
        /// Generation request.
        request: strata_inference::ChatRequest,
    },
    /// Tokenizes text with a local inference model.
    #[cfg(feature = "inference")]
    InferenceTokenize {
        /// Model spec.
        model: String,
        /// Text to tokenize.
        text: String,
        /// Whether to add special tokens.
        #[serde(default)]
        add_special: bool,
    },
    /// Detokenizes token ids with a local inference model.
    #[cfg(feature = "inference")]
    InferenceDetokenize {
        /// Model spec.
        model: String,
        /// Token ids.
        ids: Vec<u32>,
    },
    /// Embeds one or more texts with an inference model.
    #[cfg(feature = "inference")]
    InferenceEmbed {
        /// Model spec.
        model: String,
        /// Embedding request.
        request: strata_inference::EmbeddingsRequest,
    },
    /// Ranks passages against a query with an inference model.
    #[cfg(feature = "inference")]
    InferenceRank {
        /// Model spec.
        model: String,
        /// Ranking request.
        request: strata_inference::RankRequest,
    },
    /// Unloads one cached inference model, or all cached models when omitted.
    #[cfg(feature = "inference")]
    InferenceUnload {
        /// Optional model spec.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    /// Returns inference runtime cache diagnostics.
    #[cfg(feature = "inference")]
    InferenceCacheStatus {},
}

impl Command {
    /// Returns the stable command name.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Ping {} => "ping",
            Self::Info { .. } => "info",
            Self::Health { .. } => "health",
            Self::Metrics { .. } => "metrics",
            Self::Describe { .. } => "describe",
            Self::ConfigGet {} => "config_get",
            Self::IpcStatus {} => "ipc_status",
            Self::IpcStop {} => "ipc_stop",
            Self::RemoteGet {} => "remote_get",
            Self::HubClone { .. } => "hub_clone",
            Self::HubInfo { .. } => "hub_info",
            Self::HubListDatasets { .. } => "hub_list_datasets",
            Self::HubGetDataset { .. } => "hub_get_dataset",
            Self::HubListRefs { .. } => "hub_list_refs",
            Self::HubListYanked { .. } => "hub_list_yanked",
            Self::ConfigureGetKey { .. } => "configure_get_key",
            Self::SpaceList { .. } => "space_list",
            Self::SpaceCreate { .. } => "space_create",
            Self::SpaceExists { .. } => "space_exists",
            Self::SpaceDelete { .. } => "space_delete",
            Self::BranchList {} => "branch_list",
            Self::BranchGet { .. } => "branch_get",
            Self::BranchDiff { .. } => "branch_diff",
            Self::BranchCreate { .. } => "branch_create",
            Self::BranchForkCurrent { .. } => "branch_fork_current",
            Self::BranchForkAtVersion { .. } => "branch_fork_at_version",
            Self::BranchForkAtTimestamp { .. } => "branch_fork_at_timestamp",
            Self::BranchDelete { .. } => "branch_delete",
            Self::BranchMerge { .. } => "branch_merge",
            Self::BranchPreview { .. } => "branch_preview",
            Self::KvPut { .. } => "kv_put",
            Self::KvGet { .. } => "kv_get",
            Self::KvDelete { .. } => "kv_delete",
            Self::KvList { .. } => "kv_list",
            Self::KvScan { .. } => "kv_scan",
            Self::KvBatchPut { .. } => "kv_batch_put",
            Self::KvBatchGet { .. } => "kv_batch_get",
            Self::KvBatchDelete { .. } => "kv_batch_delete",
            Self::KvBatchExists { .. } => "kv_batch_exists",
            Self::KvExists { .. } => "kv_exists",
            Self::KvHistory { .. } => "kv_history",
            Self::KvCount { .. } => "kv_count",
            Self::KvSample { .. } => "kv_sample",
            Self::JsonSet { .. } => "json_set",
            Self::JsonGet { .. } => "json_get",
            Self::JsonDelete { .. } => "json_delete",
            Self::JsonHistory { .. } => "json_history",
            Self::JsonExists { .. } => "json_exists",
            Self::JsonBatchExists { .. } => "json_batch_exists",
            Self::JsonBatchSet { .. } => "json_batch_set",
            Self::JsonBatchGet { .. } => "json_batch_get",
            Self::JsonBatchDelete { .. } => "json_batch_delete",
            Self::JsonList { .. } => "json_list",
            Self::JsonScan { .. } => "json_scan",
            Self::JsonCount { .. } => "json_count",
            Self::JsonSample { .. } => "json_sample",
            Self::JsonCreateIndex { .. } => "json_create_index",
            Self::JsonDropIndex { .. } => "json_drop_index",
            Self::JsonListIndexes { .. } => "json_list_indexes",
            Self::VectorCreateCollection { .. } => "vector_create_collection",
            Self::VectorDeleteCollection { .. } => "vector_delete_collection",
            Self::VectorListCollections { .. } => "vector_list_collections",
            Self::VectorCollectionStats { .. } => "vector_collection_stats",
            Self::VectorCount { .. } => "vector_count",
            Self::VectorSample { .. } => "vector_sample",
            Self::VectorUpsert { .. } => "vector_upsert",
            Self::VectorGet { .. } => "vector_get",
            Self::VectorHistory { .. } => "vector_history",
            Self::VectorExists { .. } => "vector_exists",
            Self::VectorBatchExists { .. } => "vector_batch_exists",
            Self::VectorListKeys { .. } => "vector_list_keys",
            Self::VectorScan { .. } => "vector_scan",
            Self::VectorUpdateMetadata { .. } => "vector_update_metadata",
            Self::VectorDelete { .. } => "vector_delete",
            Self::VectorDeleteByFilter { .. } => "vector_delete_by_filter",
            Self::VectorDeleteAll { .. } => "vector_delete_all",
            Self::VectorQuery { .. } => "vector_query",
            Self::VectorIndexQuery { .. } => "vector_index_query",
            Self::VectorBatchUpsert { .. } => "vector_batch_upsert",
            Self::VectorBatchGet { .. } => "vector_batch_get",
            Self::VectorBatchDelete { .. } => "vector_batch_delete",
            Self::EventBatchAppend { .. } => "event_batch_append",
            Self::EventAppend { .. } => "event_append",
            Self::EventGet { .. } => "event_get",
            Self::EventExists { .. } => "event_exists",
            Self::EventCount { .. } => "event_count",
            Self::EventRange { .. } => "event_range",
            Self::EventRangeByTime { .. } => "event_range_by_time",
            Self::EventListTypes { .. } => "event_list_types",
            Self::EventList { .. } => "event_list",
            Self::EventVerifyChain { .. } => "event_verify_chain",
            Self::GraphCreate { .. } => "graph_create",
            Self::GraphDelete { .. } => "graph_delete",
            Self::GraphList { .. } => "graph_list",
            Self::GraphGetMeta { .. } => "graph_get_meta",
            Self::GraphAddNode { .. } => "graph_add_node",
            Self::GraphGetNode { .. } => "graph_get_node",
            Self::GraphRemoveNode { .. } => "graph_remove_node",
            Self::GraphListNodes { .. } => "graph_list_nodes",
            Self::GraphSample { .. } => "graph_sample",
            Self::GraphAddEdge { .. } => "graph_add_edge",
            Self::GraphGetEdge { .. } => "graph_get_edge",
            Self::GraphRemoveEdge { .. } => "graph_remove_edge",
            Self::GraphNeighbors { .. } => "graph_neighbors",
            Self::GraphBindingsForEntity { .. } => "graph_bindings_for_entity",
            Self::GraphBatchWrite { .. } => "graph_batch_write",
            Self::GraphDefineObjectType { .. } => "graph_define_object_type",
            Self::GraphDefineLinkType { .. } => "graph_define_link_type",
            Self::GraphDeleteObjectType { .. } => "graph_delete_object_type",
            Self::GraphDeleteLinkType { .. } => "graph_delete_link_type",
            Self::GraphFreezeOntology { .. } => "graph_freeze_ontology",
            Self::GraphGetOntology { .. } => "graph_get_ontology",
            Self::GraphOntologySummary { .. } => "graph_ontology_summary",
            Self::GraphNodesByType { .. } => "graph_nodes_by_type",
            Self::GraphWcc { .. } => "graph_wcc",
            Self::GraphLcc { .. } => "graph_lcc",
            Self::GraphSssp { .. } => "graph_sssp",
            Self::GraphPagerank { .. } => "graph_pagerank",
            Self::GraphCdlp { .. } => "graph_cdlp",
            Self::GraphBfs { .. } => "graph_bfs",
            Self::GraphBulkInsert { .. } => "graph_bulk_insert",
            Self::GraphApplyDeletePolicy { .. } => "graph_apply_delete_policy",
            Self::ArrowImport { .. } => "arrow_import",
            Self::ArrowExport { .. } => "arrow_export",
            #[cfg(feature = "inference")]
            Self::InferenceModelsList {} => "inference_models_list",
            #[cfg(feature = "inference")]
            Self::InferenceModelsLocal {} => "inference_models_local",
            #[cfg(feature = "inference")]
            Self::InferenceModelsPull { .. } => "inference_models_pull",
            #[cfg(feature = "inference")]
            Self::InferenceModelCapability { .. } => "inference_model_capability",
            #[cfg(feature = "inference")]
            Self::InferenceGenerate { .. } => "inference_generate",
            #[cfg(feature = "inference")]
            Self::InferenceTokenize { .. } => "inference_tokenize",
            #[cfg(feature = "inference")]
            Self::InferenceDetokenize { .. } => "inference_detokenize",
            #[cfg(feature = "inference")]
            Self::InferenceEmbed { .. } => "inference_embed",
            #[cfg(feature = "inference")]
            Self::InferenceRank { .. } => "inference_rank",
            #[cfg(feature = "inference")]
            Self::InferenceUnload { .. } => "inference_unload",
            #[cfg(feature = "inference")]
            Self::InferenceCacheStatus {} => "inference_cache_status",
        }
    }

    /// Returns true when the command may mutate database state or disrupt the
    /// database's transport (`ipc_stop`).
    ///
    /// This is the runtime authority the read-only session gate dispatches on;
    /// the authored truth is the IDL `access` facet, and the two are pinned
    /// together for every cataloged command by
    /// `tests/command_write_classification.rs` — change them in lockstep.
    pub const fn is_write(&self) -> bool {
        matches!(
            self,
            Self::SpaceCreate { .. }
                | Self::SpaceDelete { .. }
                | Self::BranchCreate { .. }
                | Self::BranchForkCurrent { .. }
                | Self::BranchForkAtVersion { .. }
                | Self::BranchForkAtTimestamp { .. }
                | Self::BranchDelete { .. }
                | Self::BranchMerge { .. }
                | Self::KvPut { .. }
                | Self::KvDelete { .. }
                | Self::KvBatchPut { .. }
                | Self::KvBatchDelete { .. }
                | Self::JsonSet { .. }
                | Self::JsonDelete { .. }
                | Self::JsonBatchSet { .. }
                | Self::JsonBatchDelete { .. }
                | Self::JsonCreateIndex { .. }
                | Self::JsonDropIndex { .. }
                | Self::VectorCreateCollection { .. }
                | Self::VectorDeleteCollection { .. }
                | Self::VectorUpsert { .. }
                | Self::VectorUpdateMetadata { .. }
                | Self::VectorDelete { .. }
                | Self::VectorDeleteByFilter { .. }
                | Self::VectorDeleteAll { .. }
                | Self::VectorBatchUpsert { .. }
                | Self::VectorBatchDelete { .. }
                | Self::EventBatchAppend { .. }
                | Self::EventAppend { .. }
                | Self::GraphCreate { .. }
                | Self::GraphDelete { .. }
                | Self::GraphAddNode { .. }
                | Self::GraphRemoveNode { .. }
                | Self::GraphAddEdge { .. }
                | Self::GraphRemoveEdge { .. }
                | Self::GraphBatchWrite { .. }
                | Self::GraphBulkInsert { .. }
                | Self::GraphApplyDeletePolicy { .. }
                | Self::GraphDefineObjectType { .. }
                | Self::GraphDefineLinkType { .. }
                | Self::GraphDeleteObjectType { .. }
                | Self::GraphDeleteLinkType { .. }
                | Self::GraphFreezeOntology { .. }
                | Self::ArrowImport { .. }
                | Self::HubClone { .. }
                | Self::IpcStop {}
        )
    }
}
