# Branch Lifecycle: Technical Implementation Document

**Issue:** #1462
**Date:** 2026-03-15
**Companion:** `docs/design/branch-lifecycle.md` (PRD)

---

## Epic 1: `_system_` Branch Infrastructure

**Objective:** Reserve the `_system_` namespace, auto-create the branch and `_branch_dag` graph on database init, and hide everything from user-facing APIs.

### Epic 1.1: Reserve `_system_` branch name

**File:** `crates/executor/src/handlers/branch.rs`

Modify `validate_branch_name()` (line 44) to reject system branch names:

```rust
fn validate_branch_name(name: &str) -> Result<()> {
    // ... existing checks (empty, whitespace, NUL, control, length) ...

    // NEW: reject reserved system prefix
    if name.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Branch names starting with '_system' are reserved for internal use"
                .to_string(),
        });
    }
    Ok(())
}
```

`branch_create` already calls `validate_branch_name` at line 102, so this is sufficient for create.

**Additional guards needed** for operations that take raw strings and don't call `validate_branch_name`:

`branch_fork` (line 194) — add validation for both source and destination:
```rust
pub fn branch_fork(p: &Arc<Primitives>, source: String, destination: String) -> Result<Output> {
    // NEW: reject system branches
    validate_branch_name(&destination)?;
    if source.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot fork from a system branch".to_string(),
        });
    }
    // ... existing code ...
}
```

`branch_merge` (line 234) — add validation:
```rust
pub fn branch_merge(
    p: &Arc<Primitives>,
    source: String,
    target: String,
    strategy: strata_engine::MergeStrategy,
) -> Result<Output> {
    // NEW: reject system branches
    if source.starts_with("_system") || target.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot merge system branches".to_string(),
        });
    }
    // ... existing code ...
}
```

`branch_delete` (line 163) — add guard:
```rust
pub fn branch_delete(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    reject_default_branch(&branch, "delete")?;
    // NEW: reject system branches
    if branch.as_str().starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot delete system branches".to_string(),
        });
    }
    // ... existing code ...
}
```

### Epic 1.2: Hide `_system_` from user-facing APIs

**File:** `crates/executor/src/handlers/mod.rs`

Add a new helper function after `require_branch_exists` (after line 59):

```rust
/// Guard: reject direct access to system branches.
///
/// System branches (names starting with `_system`) are reserved for internal
/// engine use. All user-facing read/write operations must reject them.
pub(crate) fn reject_system_branch(branch: &BranchId) -> Result<()> {
    if branch.as_str().starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "System branches are not accessible via user APIs".to_string(),
        });
    }
    Ok(())
}
```

**File:** `crates/executor/src/handlers/branch.rs`

Modify `branch_list` (line 127) to filter out system branches:
```rust
pub fn branch_list(
    p: &Arc<Primitives>,
    _state: Option<crate::types::BranchStatus>,
    limit: Option<u64>,
    _offset: Option<u64>,
) -> Result<Output> {
    let ids = convert_result(p.branch.list_branches())?;

    let mut all = Vec::new();
    for id in ids {
        // NEW: hide system branches
        if id.starts_with("_system") {
            continue;
        }
        if let Some(versioned) = convert_result(p.branch.get_branch(&id))? {
            all.push(versioned_to_branch_info(versioned));
        }
    }
    // ... rest unchanged ...
}
```

Modify `branch_get` (line 118):
```rust
pub fn branch_get(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    // NEW: hide system branches
    if branch.as_str().starts_with("_system") {
        return Ok(Output::MaybeBranchInfo(None));
    }
    // ... existing code ...
}
```

Modify `branch_exists` (line 153):
```rust
pub fn branch_exists(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    // NEW: hide system branches
    if branch.as_str().starts_with("_system") {
        return Ok(Output::Bool(false));
    }
    // ... existing code ...
}
```

**File:** `crates/executor/src/executor.rs`

Add a centralized guard for data commands targeting system branches. Insert after `cmd.resolve_defaults()` at line 171, before the `match cmd` dispatch:

```rust
cmd.resolve_defaults();

// NEW: reject data operations on system branches
if let Some(branch) = cmd.resolved_branch() {
    if branch.as_str().starts_with("_system") {
        // Allow branch lifecycle commands (they have their own guards)
        // Reject all data commands
        if !matches!(
            cmd,
            Command::BranchCreate { .. }
                | Command::BranchGet { .. }
                | Command::BranchList { .. }
                | Command::BranchExists { .. }
                | Command::BranchDelete { .. }
                | Command::BranchFork { .. }
                | Command::BranchDiff { .. }
                | Command::BranchMerge { .. }
        ) {
            return Err(Error::ConstraintViolation {
                reason: "System branches are not accessible via user APIs".to_string(),
            });
        }
    }
}
```

This requires adding a `resolved_branch()` accessor to `Command`.

**File:** `crates/executor/src/command.rs`

Add after `is_write()` (after line 1649):

```rust
/// Returns the resolved branch for data commands, if any.
///
/// Returns `None` for branch lifecycle commands and database commands
/// that don't operate on a specific branch.
pub fn resolved_branch(&self) -> Option<&BranchId> {
    match self {
        // Data commands with branch field
        Command::KvPut { branch, .. }
        | Command::KvBatchPut { branch, .. }
        | Command::KvGet { branch, .. }
        | Command::KvDelete { branch, .. }
        | Command::KvList { branch, .. }
        | Command::KvGetv { branch, .. }
        | Command::JsonSet { branch, .. }
        | Command::JsonBatchSet { branch, .. }
        | Command::JsonBatchGet { branch, .. }
        | Command::JsonBatchDelete { branch, .. }
        | Command::JsonGet { branch, .. }
        | Command::JsonGetv { branch, .. }
        | Command::JsonDelete { branch, .. }
        | Command::JsonList { branch, .. }
        | Command::EventAppend { branch, .. }
        | Command::EventBatchAppend { branch, .. }
        | Command::EventGet { branch, .. }
        | Command::EventGetByType { branch, .. }
        | Command::EventLen { branch, .. }
        | Command::StateSet { branch, .. }
        | Command::StateBatchSet { branch, .. }
        | Command::StateGet { branch, .. }
        | Command::StateGetv { branch, .. }
        | Command::StateCas { branch, .. }
        | Command::StateInit { branch, .. }
        | Command::StateDelete { branch, .. }
        | Command::StateList { branch, .. }
        | Command::VectorUpsert { branch, .. }
        | Command::VectorGet { branch, .. }
        | Command::VectorDelete { branch, .. }
        | Command::VectorSearch { branch, .. }
        | Command::VectorCreateCollection { branch, .. }
        | Command::VectorDeleteCollection { branch, .. }
        | Command::VectorListCollections { branch, .. }
        | Command::VectorCollectionStats { branch, .. }
        | Command::VectorBatchUpsert { branch, .. }
        | Command::Search { branch, .. }
        | Command::KvCount { branch, .. }
        | Command::JsonCount { branch, .. }
        | Command::KvSample { branch, .. }
        | Command::JsonSample { branch, .. }
        | Command::VectorSample { branch, .. }
        | Command::DbExport { branch, .. }
        | Command::GraphCreate { branch, .. }
        | Command::GraphDelete { branch, .. }
        | Command::GraphList { branch, .. }
        | Command::GraphGetMeta { branch, .. }
        | Command::GraphAddNode { branch, .. }
        | Command::GraphGetNode { branch, .. }
        | Command::GraphRemoveNode { branch, .. }
        | Command::GraphListNodes { branch, .. }
        | Command::GraphListNodesPaginated { branch, .. }
        | Command::GraphAddEdge { branch, .. }
        | Command::GraphRemoveEdge { branch, .. }
        | Command::GraphNeighbors { branch, .. }
        | Command::GraphBulkInsert { branch, .. }
        | Command::GraphBfs { branch, .. }
        | Command::GraphDefineObjectType { branch, .. }
        | Command::GraphGetObjectType { branch, .. }
        | Command::GraphListObjectTypes { branch, .. }
        | Command::GraphListOntologyTypes { branch, .. }
        | Command::GraphDeleteObjectType { branch, .. }
        | Command::GraphDefineLinkType { branch, .. }
        | Command::GraphGetLinkType { branch, .. }
        | Command::GraphListLinkTypes { branch, .. }
        | Command::GraphDeleteLinkType { branch, .. }
        | Command::GraphFreezeOntology { branch, .. }
        | Command::GraphOntologyStatus { branch, .. }
        | Command::GraphOntologySummary { branch, .. }
        | Command::GraphNodesByType { branch, .. }
        | Command::GraphWcc { branch, .. }
        | Command::GraphCdlp { branch, .. }
        | Command::GraphPagerank { branch, .. }
        | Command::GraphLcc { branch, .. }
        | Command::GraphSssp { branch, .. }
        | Command::RetentionApply { branch, .. }
        | Command::RetentionStats { branch, .. }
        | Command::RetentionPreview { branch, .. }
        | Command::TxnBegin { branch, .. }
        | Command::TimeRange { branch, .. }
        | Command::Describe { branch, .. }
        | Command::SpaceList { branch, .. }
        | Command::SpaceCreate { branch, .. }
        | Command::SpaceDelete { branch, .. }
        | Command::SpaceExists { branch, .. } => branch.as_ref(),

        // Branch lifecycle and database commands — no data branch
        _ => None,
    }
}
```

### Epic 1.3: Auto-create `_system_` branch and `_branch_dag` graph on init

**New file:** `crates/engine/src/branch_dag.rs`

```rust
//! Branch DAG: internal graph tracking branch lineage and lifecycle.
//!
//! The `_branch_dag` graph on the `_system_` branch records branch creation,
//! fork events, and merge events as nodes in a directed acyclic graph.
//! This module provides internal helpers for DAG manipulation — these are
//! NOT exposed via the executor.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use strata_core::contract::Timestamp;
use strata_core::StrataResult;
use tracing::{info, warn};

use crate::database::Database;
use crate::graph::GraphStore;
use crate::primitives::branch::resolve_branch_name;

/// Reserved branch name for internal system data.
pub const SYSTEM_BRANCH: &str = "_system_";

/// Graph name for the branch DAG.
pub const BRANCH_DAG_GRAPH: &str = "_branch_dag";

/// Ensure the `_system_` branch exists.
///
/// Creates the branch via `BranchIndex` if it doesn't already exist.
/// Idempotent — safe to call on every database open.
pub fn ensure_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    let branch_index = crate::primitives::branch::BranchIndex::new(db.clone());
    match branch_index.exists(SYSTEM_BRANCH)? {
        true => Ok(()),
        false => {
            branch_index.create_branch(SYSTEM_BRANCH)?;
            info!(target: "strata::branch_dag", "Created _system_ branch");
            Ok(())
        }
    }
}

/// Ensure the `_branch_dag` graph exists on the `_system_` branch.
///
/// Creates the graph if it doesn't exist. Idempotent.
///
/// NOTE: `GraphStore::create_graph()` does NOT error on duplicates — it
/// silently overwrites metadata. We use `get_graph_meta()` to check first
/// so we don't reset metadata on every database open.
pub fn ensure_branch_dag(db: &Arc<Database>) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Check if graph already exists — don't recreate (would overwrite metadata)
    if graph.get_graph_meta(branch_id, BRANCH_DAG_GRAPH)?.is_some() {
        return Ok(());
    }

    graph.create_graph(branch_id, BRANCH_DAG_GRAPH, None)?;
    info!(target: "strata::branch_dag", "Created _branch_dag graph");
    Ok(())
}

/// Initialize the full system branch infrastructure.
///
/// Called during `Database::open()` and `Database::cache()` after
/// recovery participants have run. Order:
/// 1. Create `_system_` branch (if needed)
/// 2. Create `_branch_dag` graph on `_system_` (if needed)
/// 3. Seed default branch node in DAG (if needed)
pub fn init_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    ensure_system_branch(db)?;
    ensure_branch_dag(db)?;
    seed_default_branch(db)?;
    Ok(())
}

/// Add a "default" branch node to the DAG if not already present.
fn seed_default_branch(db: &Arc<Database>) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Check if default node already exists
    if graph.get_node(branch_id, BRANCH_DAG_GRAPH, "default")?.is_some() {
        return Ok(());
    }

    let props = serde_json::json!({
        "status": "active",
        "created_at": Timestamp::now().as_micros(),
        "updated_at": Timestamp::now().as_micros(),
    });

    let node_data = crate::graph::types::NodeData {
        entity_ref: None,
        properties: props,
        object_type: Some("branch".to_string()),
    };

    graph.add_node(branch_id, BRANCH_DAG_GRAPH, "default", node_data)?;
    info!(target: "strata::branch_dag", "Seeded default branch node in DAG");
    Ok(())
}
```

**File:** `crates/engine/src/lib.rs`

Add module declaration (after line 51, after `pub mod bundle;`):

```rust
pub mod branch_dag;
```

**File:** `crates/engine/src/database/mod.rs`

Call `init_system_branch()` in all three init paths:

**1. `open_internal` path (line 374-386)** — after `recover_all_participants(&db)?`:
```rust
crate::recovery::recover_all_participants(&db)?;
let index = db.extension::<crate::search::InvertedIndex>()?;
if !index.is_enabled() {
    index.enable();
}

// NEW: initialize system branch infrastructure
if let Err(e) = crate::branch_dag::init_system_branch(&db) {
    warn!(target: "strata::db", error = %e, "Failed to initialize system branch — DAG tracking unavailable");
}

Ok(db)
```

**2. `cache()` path (line 688-691)** — after InvertedIndex setup:
```rust
let index = db.extension::<crate::search::InvertedIndex>()?;
index.enable();

// NEW: initialize system branch infrastructure
if let Err(e) = crate::branch_dag::init_system_branch(&db) {
    warn!(target: "strata::db", error = %e, "Failed to initialize system branch — DAG tracking unavailable");
}

Ok(db)
```

**3. `open_follower_internal` path (line 476-484)** — after InvertedIndex setup:

**NOTE:** Followers are read-only (`wal_writer: None`). Calling `init_system_branch()`
would attempt writes (create branch, graph, seed node) which will fail. Instead,
only populate the status cache from existing DAG data if the primary has already
initialized the system branch.

```rust
let index = db.extension::<crate::search::InvertedIndex>()?;
if !index.is_enabled() {
    index.enable();
}

// NEW: read-only — populate status cache from existing DAG (if primary initialized it)
// Do NOT call init_system_branch() — followers cannot write.
if let Err(e) = crate::branch_dag::load_status_cache_readonly(&db) {
    warn!(target: "strata::db", error = %e, "Failed to load branch status cache from DAG");
}

Ok(db)
```

Add a read-only loader to `branch_dag.rs`:

```rust
/// Load the status cache from existing DAG data without creating anything.
///
/// Used by follower databases that cannot write. If the `_system_` branch
/// doesn't exist yet (primary hasn't initialized), this is a no-op.
pub fn load_status_cache_readonly(db: &Arc<Database>) -> StrataResult<()> {
    let branch_index = crate::primitives::branch::BranchIndex::new(db.clone());
    if !branch_index.exists(SYSTEM_BRANCH)? {
        return Ok(()); // Primary hasn't initialized yet — nothing to read
    }

    populate_status_cache(db)
}
```

**Design note:** Failures are logged but do NOT prevent database startup. The system branch is a metadata overlay — core data operations must never be blocked by DAG initialization failures.

### Epic 1 Tests

**New file:** `crates/engine/src/branch_dag_tests.rs` (or `#[cfg(test)] mod tests` inside `branch_dag.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_db() -> (TempDir, Arc<Database>) {
        let tmp = TempDir::new().unwrap();
        let db = Database::open(tmp.path()).unwrap();
        (tmp, db)
    }

    #[test]
    fn system_branch_auto_created() {
        let (_tmp, db) = open_db();
        let idx = crate::primitives::branch::BranchIndex::new(db.clone());
        assert!(idx.exists("_system_").unwrap());
    }

    #[test]
    fn branch_dag_graph_exists() {
        let (_tmp, db) = open_db();
        let graph = GraphStore::new(db.clone());
        let bid = resolve_branch_name(SYSTEM_BRANCH);
        assert!(graph.get_graph_meta(bid, BRANCH_DAG_GRAPH).unwrap().is_some());
    }

    #[test]
    fn default_branch_seeded() {
        let (_tmp, db) = open_db();
        let graph = GraphStore::new(db.clone());
        let bid = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph.get_node(bid, BRANCH_DAG_GRAPH, "default").unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().object_type, Some("branch".to_string()));
    }

    #[test]
    fn init_is_idempotent() {
        let (_tmp, db) = open_db();
        // Called once during open, call again
        init_system_branch(&db).unwrap();
        init_system_branch(&db).unwrap();
        // No error, no duplicates
    }

    #[test]
    fn cache_db_has_system_branch() {
        let db = Database::cache().unwrap();
        let idx = crate::primitives::branch::BranchIndex::new(db.clone());
        assert!(idx.exists("_system_").unwrap());
    }
}
```

**Executor-level tests** (in `crates/executor/src/handlers/branch.rs` tests):

```rust
#[test]
fn reject_create_system_branch() {
    // Setup executor with test database
    let result = branch_create(p, Some("_system_foo".to_string()), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("reserved"));
}

#[test]
fn system_branch_hidden_from_list() {
    // branch_list should not include _system_
    let result = branch_list(p, None, None, None).unwrap();
    match result {
        Output::BranchInfoList(branches) => {
            for b in &branches {
                assert!(!b.info.id.as_str().starts_with("_system"));
            }
        }
        _ => panic!("unexpected output"),
    }
}

#[test]
fn system_branch_hidden_from_exists() {
    let result = branch_exists(p, BranchId::from("_system_")).unwrap();
    assert_eq!(result, Output::Bool(false));
}

#[test]
fn system_branch_hidden_from_get() {
    let result = branch_get(p, BranchId::from("_system_")).unwrap();
    assert_eq!(result, Output::MaybeBranchInfo(None));
}

#[test]
fn reject_delete_system_branch() {
    let result = branch_delete(p, BranchId::from("_system_"));
    assert!(result.is_err());
}

#[test]
fn reject_kv_put_to_system_branch() {
    // executor.execute(Command::KvPut { branch: Some("_system_".into()), ... })
    // should fail with ConstraintViolation
}
```

---

## Epic 2: Branch DAG Tracking

**Objective:** Record branch creation, forks, and merges as nodes/edges in the DAG graph. Provide internal helper functions for reading DAG state.

### Epic 2.1: DAG types

**File:** `crates/engine/src/branch_dag.rs` — add types after the existing code from Epic 1:

```rust
// =========================================================================
// DAG Types
// =========================================================================

/// Unique identifier for a fork or merge event in the DAG.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DagEventId(pub String);

impl DagEventId {
    /// Create a new fork event ID.
    pub fn new_fork() -> Self {
        DagEventId(format!("fork:{}", uuid::Uuid::new_v4()))
    }

    /// Create a new merge event ID.
    pub fn new_merge() -> Self {
        DagEventId(format!("merge:{}", uuid::Uuid::new_v4()))
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Check if this is a fork event.
    pub fn is_fork(&self) -> bool {
        self.0.starts_with("fork:")
    }

    /// Check if this is a merge event.
    pub fn is_merge(&self) -> bool {
        self.0.starts_with("merge:")
    }
}

impl std::fmt::Display for DagEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Branch status in the DAG.
///
/// This is distinct from `engine::BranchStatus` (which only has `Active`).
/// The DAG tracks the full lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagBranchStatus {
    /// Branch is active and accepting writes.
    Active,
    /// Branch has been merged into another branch. Writes are rejected.
    Merged,
    /// Branch has been archived by user. Writes are rejected.
    Archived,
    /// Branch data has been deleted. DAG node preserved for lineage.
    Deleted,
}

impl DagBranchStatus {
    /// Returns true if the branch is writable.
    pub fn is_writable(&self) -> bool {
        matches!(self, DagBranchStatus::Active)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DagBranchStatus::Active => "active",
            DagBranchStatus::Merged => "merged",
            DagBranchStatus::Archived => "archived",
            DagBranchStatus::Deleted => "deleted",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(DagBranchStatus::Active),
            "merged" => Some(DagBranchStatus::Merged),
            "archived" => Some(DagBranchStatus::Archived),
            "deleted" => Some(DagBranchStatus::Deleted),
            _ => None,
        }
    }
}

impl std::fmt::Display for DagBranchStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Record of a merge event, read from the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRecord {
    pub event_id: String,
    pub source: String,
    pub target: String,
    pub timestamp: u64,
    pub strategy: Option<String>,
    pub keys_applied: Option<u64>,
    pub spaces_merged: Option<u64>,
    pub conflicts: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
}

/// Record of a fork event, read from the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkRecord {
    pub event_id: String,
    pub parent: String,
    pub child: String,
    pub timestamp: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
}

/// Full branch info assembled from DAG queries.
///
/// Uses `DagBranchStatus` directly (not a string) for type safety.
/// The executor layer converts to its own `BranchStatus` via bridge functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagBranchInfo {
    pub name: String,
    pub status: DagBranchStatus,
    pub created_at: Option<u64>,
    pub updated_at: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
    pub forked_from: Option<ForkRecord>,
    pub merges: Vec<MergeRecord>,
    pub children: Vec<String>,
}
```

### Epic 2.2: DAG write helpers

**File:** `crates/engine/src/branch_dag.rs` — add after the types:

```rust
// =========================================================================
// DAG Write Operations
// =========================================================================

/// Record a new branch in the DAG.
///
/// Called by the executor after a successful `branch_create`.
/// Creates a node with `object_type: "branch"` and status `active`.
pub fn dag_add_branch(
    db: &Arc<Database>,
    name: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "status": "active",
        "created_at": now,
        "updated_at": now,
    });

    if let Some(msg) = message {
        props["message"] = serde_json::Value::String(msg.to_string());
    }
    if let Some(c) = creator {
        props["creator"] = serde_json::Value::String(c.to_string());
    }

    let node_data = crate::graph::types::NodeData {
        entity_ref: None,
        properties: props,
        object_type: Some("branch".to_string()),
    };

    graph.add_node(branch_id, BRANCH_DAG_GRAPH, name, node_data)?;
    Ok(())
}

/// Ensure a branch has a node in the DAG, creating one if missing.
///
/// This handles branches created before the DAG existed, or branches
/// whose DAG recording failed. Called before creating edges to satisfy
/// the graph engine's requirement that both nodes exist for an edge.
fn ensure_branch_node_exists(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    if graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)?.is_none() {
        dag_add_branch(db, name, None, None)?;
    }
    Ok(())
}

/// Record a fork event in the DAG.
///
/// Creates a fork event node and edges: parent --[parent]--> fork --[child]--> child.
/// Called by the executor after a successful `branch_fork`.
///
/// NOTE: Both parent and child nodes MUST exist before edges can be created
/// (`GraphStore::add_edge` validates this). We call `ensure_branch_node_exists`
/// to handle branches that predate the DAG.
pub fn dag_record_fork(
    db: &Arc<Database>,
    parent: &str,
    child: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // Ensure both branch nodes exist (may be missing if branch predates DAG)
    ensure_branch_node_exists(db, parent)?;
    ensure_branch_node_exists(db, child)?;

    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let event_id = DagEventId::new_fork();
    let now = Timestamp::now().as_micros();

    // Create fork event node
    let mut props = serde_json::json!({
        "timestamp": now,
    });
    if let Some(msg) = message {
        props["message"] = serde_json::Value::String(msg.to_string());
    }
    if let Some(c) = creator {
        props["creator"] = serde_json::Value::String(c.to_string());
    }

    let node_data = crate::graph::types::NodeData {
        entity_ref: None,
        properties: props,
        object_type: Some("fork".to_string()),
    };
    graph.add_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str(), node_data)?;

    // Edges: parent --[parent]--> fork_event --[child]--> child
    let edge_data = crate::graph::types::EdgeData::default();
    graph.add_edge(
        branch_id, BRANCH_DAG_GRAPH,
        parent, event_id.as_str(), "parent", edge_data.clone(),
    )?;
    graph.add_edge(
        branch_id, BRANCH_DAG_GRAPH,
        event_id.as_str(), child, "child", edge_data,
    )?;

    Ok(event_id)
}

/// Record a merge event in the DAG.
///
/// Creates a merge event node and edges: source --[source]--> merge --[target]--> target.
/// Called by the executor after a successful `branch_merge`.
///
/// NOTE: Both source and target nodes MUST exist before edges can be created.
/// We call `ensure_branch_node_exists` to handle branches that predate the DAG.
pub fn dag_record_merge(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    merge_info: &crate::branch_ops::MergeInfo,
    strategy: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // Ensure both branch nodes exist (may be missing if branch predates DAG)
    ensure_branch_node_exists(db, source)?;
    ensure_branch_node_exists(db, target)?;

    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let event_id = DagEventId::new_merge();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "strategy": strategy,
        "keys_applied": merge_info.keys_applied,
        "spaces_merged": merge_info.spaces_merged,
        "conflicts": merge_info.conflicts.len() as u64,
    });
    if let Some(msg) = message {
        props["message"] = serde_json::Value::String(msg.to_string());
    }
    if let Some(c) = creator {
        props["creator"] = serde_json::Value::String(c.to_string());
    }

    let node_data = crate::graph::types::NodeData {
        entity_ref: None,
        properties: props,
        object_type: Some("merge".to_string()),
    };
    graph.add_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str(), node_data)?;

    // Edges: source --[source]--> merge_event --[target]--> target
    let edge_data = crate::graph::types::EdgeData::default();
    graph.add_edge(
        branch_id, BRANCH_DAG_GRAPH,
        source, event_id.as_str(), "source", edge_data.clone(),
    )?;
    graph.add_edge(
        branch_id, BRANCH_DAG_GRAPH,
        event_id.as_str(), target, "target", edge_data,
    )?;

    Ok(event_id)
}

/// Update the status property on a branch's DAG node.
pub fn dag_set_status(
    db: &Arc<Database>,
    name: &str,
    status: DagBranchStatus,
) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let existing = graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)?
        .ok_or_else(|| strata_core::StrataError::invalid_input(
            format!("Branch '{}' not found in DAG", name),
        ))?;

    let mut props = existing.properties;
    props["status"] = serde_json::Value::String(status.as_str().to_string());
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let node_data = crate::graph::types::NodeData {
        entity_ref: existing.entity_ref,
        properties: props,
        object_type: existing.object_type,
    };
    graph.add_node(branch_id, BRANCH_DAG_GRAPH, name, node_data)?;
    Ok(())
}

/// Mark a branch as deleted in the DAG (preserves lineage).
pub fn dag_mark_deleted(
    db: &Arc<Database>,
    name: &str,
) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    if let Some(existing) = graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        let mut props = existing.properties;
        props["status"] = serde_json::Value::String("deleted".to_string());
        props["deleted_at"] = serde_json::json!(Timestamp::now().as_micros());

        let node_data = crate::graph::types::NodeData {
            entity_ref: existing.entity_ref,
            properties: props,
            object_type: existing.object_type,
        };
        graph.add_node(branch_id, BRANCH_DAG_GRAPH, name, node_data)?;
    }
    Ok(())
}

/// Update the message property on a branch's DAG node.
pub fn dag_set_message(
    db: &Arc<Database>,
    name: &str,
    message: &str,
) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let existing = graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)?
        .ok_or_else(|| strata_core::StrataError::invalid_input(
            format!("Branch '{}' not found in DAG", name),
        ))?;

    let mut props = existing.properties;
    props["message"] = serde_json::Value::String(message.to_string());
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let node_data = crate::graph::types::NodeData {
        entity_ref: existing.entity_ref,
        properties: props,
        object_type: existing.object_type,
    };
    graph.add_node(branch_id, BRANCH_DAG_GRAPH, name, node_data)?;
    Ok(())
}
```

### Epic 2.3: DAG read helpers

**File:** `crates/engine/src/branch_dag.rs` — add after write helpers:

```rust
// =========================================================================
// DAG Read Operations
// =========================================================================

/// Get the status of a branch from its DAG node.
///
/// Returns `Active` if the branch has no DAG node (backwards compat with
/// branches created before the DAG existed).
pub fn dag_get_status(
    db: &Arc<Database>,
    name: &str,
) -> StrataResult<DagBranchStatus> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    match graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        Some(node) => {
            let status_str = node.properties.get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("active");
            Ok(DagBranchStatus::from_str(status_str).unwrap_or(DagBranchStatus::Active))
        }
        None => Ok(DagBranchStatus::Active), // Not in DAG → assume active
    }
}

/// Get full branch info assembled from DAG queries.
///
/// Returns branch metadata, fork origin, merge history, and children.
pub fn dag_get_branch_info(
    db: &Arc<Database>,
    name: &str,
) -> StrataResult<Option<DagBranchInfo>> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let node = match graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        Some(n) => n,
        None => return Ok(None),
    };

    let props = &node.properties;

    // Find fork origin: look for incoming "child" edges from fork nodes
    let forked_from = find_fork_origin(db, name)?;

    // Find merge history: look for outgoing "source" edges to merge nodes
    let merges = find_merge_history(db, name)?;

    // Find children: look for outgoing "parent" edges to fork nodes,
    // then follow "child" edges from those fork nodes
    let children = find_children(db, name)?;

    let status_str = props.get("status").and_then(|v| v.as_str()).unwrap_or("active");
    let status = DagBranchStatus::from_str(status_str).unwrap_or(DagBranchStatus::Active);

    Ok(Some(DagBranchInfo {
        name: name.to_string(),
        status,
        created_at: props.get("created_at").and_then(|v| v.as_u64()),
        updated_at: props.get("updated_at").and_then(|v| v.as_u64()),
        message: props.get("message").and_then(|v| v.as_str()).map(|s| s.to_string()),
        creator: props.get("creator").and_then(|v| v.as_str()).map(|s| s.to_string()),
        forked_from,
        merges,
        children,
    }))
}

/// Find the fork event that created a branch (if any).
fn find_fork_origin(
    db: &Arc<Database>,
    branch_name: &str,
) -> StrataResult<Option<ForkRecord>> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Look for incoming "child" edges — these come from fork event nodes
    let neighbors = graph.neighbors(
        branch_id, BRANCH_DAG_GRAPH, branch_name,
        crate::graph::types::Direction::Incoming,
        Some("child"),
    )?;

    for neighbor in neighbors {
        // The neighbor is a fork event node — get its properties
        if let Some(fork_node) = graph.get_node(branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id)? {
            if fork_node.object_type.as_deref() != Some("fork") {
                continue;
            }
            // Find the parent: look for incoming "parent" edges to this fork node
            let parents = graph.neighbors(
                branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id,
                crate::graph::types::Direction::Incoming,
                Some("parent"),
            )?;
            let parent_name = parents.first().map(|n| n.node_id.clone()).unwrap_or_default();

            let props = &fork_node.properties;
            return Ok(Some(ForkRecord {
                event_id: neighbor.node_id.clone(),
                parent: parent_name,
                child: branch_name.to_string(),
                timestamp: props.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
                message: props.get("message").and_then(|v| v.as_str()).map(|s| s.to_string()),
                creator: props.get("creator").and_then(|v| v.as_str()).map(|s| s.to_string()),
            }));
        }
    }
    Ok(None)
}

/// Find all merge events involving this branch as source.
fn find_merge_history(
    db: &Arc<Database>,
    branch_name: &str,
) -> StrataResult<Vec<MergeRecord>> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let mut records = Vec::new();

    // Look for outgoing "source" edges — these go to merge event nodes
    let neighbors = graph.neighbors(
        branch_id, BRANCH_DAG_GRAPH, branch_name,
        crate::graph::types::Direction::Outgoing,
        Some("source"),
    )?;

    for neighbor in neighbors {
        if let Some(merge_node) = graph.get_node(branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id)? {
            if merge_node.object_type.as_deref() != Some("merge") {
                continue;
            }
            // Find the target: look for outgoing "target" edges from this merge node
            let targets = graph.neighbors(
                branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id,
                crate::graph::types::Direction::Outgoing,
                Some("target"),
            )?;
            let target_name = targets.first().map(|n| n.node_id.clone()).unwrap_or_default();

            let props = &merge_node.properties;
            records.push(MergeRecord {
                event_id: neighbor.node_id.clone(),
                source: branch_name.to_string(),
                target: target_name,
                timestamp: props.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
                strategy: props.get("strategy").and_then(|v| v.as_str()).map(|s| s.to_string()),
                keys_applied: props.get("keys_applied").and_then(|v| v.as_u64()),
                spaces_merged: props.get("spaces_merged").and_then(|v| v.as_u64()),
                conflicts: props.get("conflicts").and_then(|v| v.as_u64()),
                message: props.get("message").and_then(|v| v.as_str()).map(|s| s.to_string()),
                creator: props.get("creator").and_then(|v| v.as_str()).map(|s| s.to_string()),
            });
        }
    }

    // Sort by timestamp
    records.sort_by_key(|r| r.timestamp);
    Ok(records)
}

/// Find all branches forked from this branch.
///
/// Public because it's called from executor handlers (filtered branch listing).
pub fn find_children(
    db: &Arc<Database>,
    branch_name: &str,
) -> StrataResult<Vec<String>> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let mut children = Vec::new();

    // Look for outgoing "parent" edges — these go to fork event nodes
    let neighbors = graph.neighbors(
        branch_id, BRANCH_DAG_GRAPH, branch_name,
        crate::graph::types::Direction::Outgoing,
        Some("parent"),
    )?;

    for neighbor in neighbors {
        // Follow "child" edges from the fork node to find the child branch
        let child_neighbors = graph.neighbors(
            branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id,
            crate::graph::types::Direction::Outgoing,
            Some("child"),
        )?;
        for child in child_neighbors {
            children.push(child.node_id);
        }
    }

    Ok(children)
}
```

### Epic 2.4: Wire branch operations to DAG

**File:** `crates/executor/src/handlers/branch.rs`

Add import at top:
```rust
use strata_engine::branch_dag;
```

Modify `branch_create` (line 93) — add DAG recording after successful creation:
```rust
pub fn branch_create(
    p: &Arc<Primitives>,
    branch_id: Option<String>,
    _metadata: Option<strata_core::Value>,
) -> Result<Output> {
    let branch_str = match &branch_id {
        Some(s) => {
            validate_branch_name(s)?;
            s.clone()
        }
        None => uuid::Uuid::new_v4().to_string(),
    };

    let versioned = convert_result(p.branch.create_branch(&branch_str))?;

    // NEW: record in DAG (best-effort)
    if let Err(e) = branch_dag::dag_add_branch(&p.db, &branch_str, None, None) {
        tracing::warn!(target: "strata::branch_dag", error = %e, branch = %branch_str, "Failed to record branch creation in DAG");
    }

    Ok(Output::BranchWithVersion {
        info: metadata_to_branch_info(&versioned.value),
        version: extract_version(&versioned.version),
    })
}
```

Modify `branch_fork` (line 194):
```rust
pub fn branch_fork(
    p: &Arc<Primitives>,
    source: String,
    destination: String,
    message: Option<String>,   // NEW parameter
    creator: Option<String>,   // NEW parameter
) -> Result<Output> {
    // Validation (from Epic 1.1)
    validate_branch_name(&destination)?;
    if source.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot fork from a system branch".to_string(),
        });
    }

    let info =
        strata_engine::branch_ops::fork_branch(&p.db, &source, &destination).map_err(|e| {
            Error::Internal { reason: e.to_string() }
        })?;

    // NEW: record in DAG (best-effort)
    if let Err(e) = branch_dag::dag_add_branch(&p.db, &destination, message.as_deref(), creator.as_deref()) {
        tracing::warn!(target: "strata::branch_dag", error = %e, "Failed to record forked branch in DAG");
    }
    if let Err(e) = branch_dag::dag_record_fork(&p.db, &source, &destination, message.as_deref(), creator.as_deref()) {
        tracing::warn!(target: "strata::branch_dag", error = %e, "Failed to record fork event in DAG");
    }

    Ok(Output::BranchForked(info))
}
```

Modify `branch_merge` (line 234):
```rust
pub fn branch_merge(
    p: &Arc<Primitives>,
    source: String,
    target: String,
    strategy: strata_engine::MergeStrategy,
    message: Option<String>,   // NEW parameter
    creator: Option<String>,   // NEW parameter
) -> Result<Output> {
    // Validation (from Epic 1.1)
    if source.starts_with("_system") || target.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot merge system branches".to_string(),
        });
    }

    let strategy_str = match strategy {
        strata_engine::MergeStrategy::LastWriterWins => "last_writer_wins",
        strata_engine::MergeStrategy::Strict => "strict",
    };

    let info = strata_engine::branch_ops::merge_branches(&p.db, &source, &target, strategy)
        .map_err(|e| Error::Internal { reason: e.to_string() })?;

    // NEW: record merge event (best-effort)
    if let Err(e) = branch_dag::dag_record_merge(
        &p.db, &source, &target, &info, strategy_str,
        message.as_deref(), creator.as_deref(),
    ) {
        tracing::warn!(target: "strata::branch_dag", error = %e, "Failed to record merge event in DAG");
    }

    // NOTE: We do NOT auto-set source to Merged here. Auto-merge blocks
    // iterative workflows (write → merge → write more → merge again) and
    // Merged status is irreversible. Instead, users should explicitly call
    // `branch_archive` after merge if they want to lock the source branch.
    // The merge event in the DAG provides the audit trail regardless.

    Ok(Output::BranchMerged(info))
}
```

Modify `branch_delete` (line 163) — add DAG recording:
```rust
pub fn branch_delete(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    reject_default_branch(&branch, "delete")?;
    if branch.as_str().starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot delete system branches".to_string(),
        });
    }

    convert_result(p.branch.delete_branch(branch.as_str()))?;

    // ... existing cleanup (lock, vector collections) ...

    // NEW: mark deleted in DAG (best-effort — preserves lineage)
    if let Err(e) = branch_dag::dag_mark_deleted(&p.db, branch.as_str()) {
        tracing::warn!(target: "strata::branch_dag", error = %e, "Failed to mark branch deleted in DAG");
    }

    Ok(Output::Unit)
}
```

Modify `branch_import` (line 270) — add DAG recording for imported branches:
```rust
pub fn branch_import(p: &Arc<Primitives>, path: String) -> Result<Output> {
    let import_path = std::path::Path::new(&path);
    let info = strata_engine::bundle::import_branch(&p.db, import_path).map_err(|e| Error::Io {
        reason: format!("Import failed: {}", e),
    })?;

    // NEW: record imported branch in DAG (best-effort)
    if let Err(e) = branch_dag::dag_add_branch(&p.db, &info.branch_id, None, None) {
        tracing::warn!(target: "strata::branch_dag", error = %e,
            branch = %info.branch_id, "Failed to record imported branch in DAG");
    }

    Ok(Output::BranchImported(crate::types::BranchImportResult {
        branch_id: info.branch_id,
        transactions_applied: info.transactions_applied,
        keys_written: info.keys_written,
    }))
}
```

### Epic 2.5: Update Command enum for new parameters

**File:** `crates/executor/src/command.rs`

Modify `BranchFork` (line 684):
```rust
BranchFork {
    source: String,
    destination: String,
    /// Optional message describing why this fork was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    /// Optional creator identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    creator: Option<String>,
},
```

Modify `BranchMerge` (line 711):
```rust
BranchMerge {
    source: String,
    target: String,
    strategy: MergeStrategy,
    /// Optional message describing why this merge was performed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    /// Optional creator identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    creator: Option<String>,
},
```

**File:** `crates/executor/src/executor.rs`

Update dispatch (line 920-942) to pass new fields:
```rust
Command::BranchFork {
    source,
    destination,
    message,       // NEW
    creator,       // NEW
} => crate::handlers::branch::branch_fork(
    &self.primitives, source, destination, message, creator,
),

Command::BranchMerge {
    source,
    target,
    strategy,
    message,       // NEW
    creator,       // NEW
} => crate::handlers::branch::branch_merge(
    &self.primitives, source, target, strategy, message, creator,
),
```

**File:** `crates/executor/src/api/branches.rs`

Update public API methods:
```rust
/// Fork a branch, creating a copy with all its data.
///
/// NOTE: The existing `fork()` signature is preserved for backward compat.
/// New `message`/`creator` fields default to `None`.
pub fn fork(&self, source: &str, destination: &str) -> Result<ForkInfo> {
    self.fork_with_options(source, destination, None, None)
}

/// Fork with optional message and creator.
pub fn fork_with_options(
    &self,
    source: &str,
    destination: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> Result<ForkInfo> {
    match self.executor.execute(Command::BranchFork {
        source: source.to_string(),
        destination: destination.to_string(),
        message: message.map(|s| s.to_string()),
        creator: creator.map(|s| s.to_string()),
    })? {
        Output::BranchForked(info) => Ok(info),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchFork".into(),
        }),
    }
}

/// Merge data from source branch into target branch.
///
/// NOTE: The existing `merge()` signature is preserved for backward compat.
/// New `message`/`creator` fields default to `None`.
pub fn merge(&self, source: &str, target: &str, strategy: MergeStrategy) -> Result<MergeInfo> {
    self.merge_with_options(source, target, strategy, None, None)
}

/// Merge with optional message and creator.
pub fn merge_with_options(
    &self,
    source: &str,
    target: &str,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
) -> Result<MergeInfo> {
    match self.executor.execute(Command::BranchMerge {
        source: source.to_string(),
        target: target.to_string(),
        strategy,
        message: message.map(|s| s.to_string()),
        creator: creator.map(|s| s.to_string()),
    })? {
        Output::BranchMerged(info) => Ok(info),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchMerge".into(),
        }),
    }
}
```

---

## Epic 3: Status-Based Write Rejection

**Objective:** Prevent writes to merged/archived branches using an in-memory cache for performance, and expose archive/unarchive APIs.

### Epic 3.0: Extend executor `BranchStatus` and add bridge conversions

The engine's `BranchStatus` (only `Active`) is separate from `DagBranchStatus`
(which tracks the full lifecycle). The executor's `BranchStatus` must be extended
to expose Merged/Archived to callers, and bridge functions must convert between them.

**File:** `crates/executor/src/types.rs`

Extend `BranchStatus` (line 59):
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    Active,
    Merged,    // NEW
    Archived,  // NEW
}
```

**File:** `crates/executor/src/bridge.rs`

Add new conversion functions (after `from_engine_branch_status` at line 411):
```rust
/// Convert DAG branch status to executor branch status.
pub fn from_dag_branch_status(
    status: strata_engine::branch_dag::DagBranchStatus,
) -> crate::types::BranchStatus {
    match status {
        strata_engine::branch_dag::DagBranchStatus::Active => crate::types::BranchStatus::Active,
        strata_engine::branch_dag::DagBranchStatus::Merged => crate::types::BranchStatus::Merged,
        strata_engine::branch_dag::DagBranchStatus::Archived => crate::types::BranchStatus::Archived,
        strata_engine::branch_dag::DagBranchStatus::Deleted => crate::types::BranchStatus::Archived,
    }
}

/// Convert executor branch status to DAG branch status (for filtering).
pub fn to_dag_branch_status(
    status: crate::types::BranchStatus,
) -> strata_engine::branch_dag::DagBranchStatus {
    match status {
        crate::types::BranchStatus::Active => strata_engine::branch_dag::DagBranchStatus::Active,
        crate::types::BranchStatus::Merged => strata_engine::branch_dag::DagBranchStatus::Merged,
        crate::types::BranchStatus::Archived => strata_engine::branch_dag::DagBranchStatus::Archived,
    }
}
```

**File:** `crates/executor/src/handlers/branch.rs`

Update `metadata_to_branch_info` to accept an optional DAG status override:
```rust
fn metadata_to_branch_info(
    m: &BranchMetadata,
    dag_status: Option<strata_engine::branch_dag::DagBranchStatus>,
) -> BranchInfo {
    BranchInfo {
        id: BranchId::from(m.name.clone()),
        status: dag_status
            .map(crate::bridge::from_dag_branch_status)
            .unwrap_or_else(|| from_engine_branch_status(m.status)),
        created_at: m.created_at.as_micros(),
        updated_at: m.updated_at.as_micros(),
        parent_id: None,
    }
}
```

Update callers of `metadata_to_branch_info` to look up DAG status from the cache:
```rust
// In branch_list, branch_get, branch_create:
let dag_status = p.db
    .extension::<strata_engine::branch_status_cache::BranchStatusCache>()
    .ok()
    .and_then(|cache| cache.get(&id));
all.push(metadata_to_branch_info(&versioned.value, dag_status));
```

### Epic 3.1: Branch status cache

**New file:** `crates/engine/src/branch_status_cache.rs`

```rust
//! In-memory cache for branch lifecycle status.
//!
//! Provides O(1) status lookups for write guards. Populated from the branch
//! DAG on database open. Updated atomically on status transitions.

use dashmap::DashMap;
use crate::branch_dag::DagBranchStatus;

/// Cache of branch name → lifecycle status.
///
/// Registered as a database extension via `db.extension::<BranchStatusCache>()`.
/// Thread-safe for concurrent read/write access.
#[derive(Default)]
pub struct BranchStatusCache {
    statuses: DashMap<String, DagBranchStatus>,
}

impl BranchStatusCache {
    /// Get the cached status for a branch.
    ///
    /// Returns `None` if the branch is not in the cache (treat as `Active`).
    pub fn get(&self, name: &str) -> Option<DagBranchStatus> {
        self.statuses.get(name).map(|v| *v)
    }

    /// Set the cached status for a branch.
    pub fn set(&self, name: &str, status: DagBranchStatus) {
        self.statuses.insert(name.to_string(), status);
    }

    /// Remove a branch from the cache.
    pub fn remove(&self, name: &str) {
        self.statuses.remove(name);
    }

    /// Check if a branch is writable (active status or not in cache).
    pub fn is_writable(&self, name: &str) -> bool {
        match self.get(name) {
            Some(status) => status.is_writable(),
            None => true, // Not cached → assume writable
        }
    }
}
```

**File:** `crates/engine/src/lib.rs` — add module:
```rust
pub mod branch_status_cache;
```

**File:** `crates/engine/src/branch_dag.rs`

Add cache population to `init_system_branch()`:
```rust
pub fn init_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    ensure_system_branch(db)?;
    ensure_branch_dag(db)?;
    seed_default_branch(db)?;
    populate_status_cache(db)?;  // NEW
    Ok(())
}

/// Populate the status cache from existing DAG branch nodes.
fn populate_status_cache(db: &Arc<Database>) -> StrataResult<()> {
    let cache = db.extension::<crate::branch_status_cache::BranchStatusCache>()?;
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let nodes = graph.list_nodes(branch_id, BRANCH_DAG_GRAPH)?;
    for node_id in nodes {
        if let Some(node) = graph.get_node(branch_id, BRANCH_DAG_GRAPH, &node_id)? {
            if node.object_type.as_deref() == Some("branch") {
                let status_str = node.properties.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("active");
                if let Some(status) = DagBranchStatus::from_str(status_str) {
                    cache.set(&node_id, status);
                }
            }
        }
    }
    Ok(())
}
```

Also update `dag_set_status` and `dag_mark_deleted` to update the cache:
```rust
pub fn dag_set_status(db: &Arc<Database>, name: &str, status: DagBranchStatus) -> StrataResult<()> {
    // ... existing graph update code ...

    // Update cache
    if let Ok(cache) = db.extension::<crate::branch_status_cache::BranchStatusCache>() {
        cache.set(name, status);
    }
    Ok(())
}

pub fn dag_mark_deleted(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    // ... existing graph update code ...

    // Update cache
    if let Ok(cache) = db.extension::<crate::branch_status_cache::BranchStatusCache>() {
        cache.remove(name);
    }
    Ok(())
}
```

### Epic 3.2: Write guard in executor

**File:** `crates/executor/src/handlers/mod.rs`

Add after `reject_system_branch`:
```rust
/// Guard: reject writes to non-active branches (merged or archived).
///
/// Uses the in-memory BranchStatusCache for O(1) lookups. The cache is
/// populated from the DAG on database open and updated on status transitions.
pub(crate) fn require_branch_writable(p: &Arc<Primitives>, branch: &BranchId) -> Result<()> {
    if branch.is_default() {
        return Ok(()); // Default branch always writable
    }

    if let Ok(cache) = p.db.extension::<strata_engine::branch_status_cache::BranchStatusCache>() {
        if !cache.is_writable(branch.as_str()) {
            let status = cache.get(branch.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(Error::ConstraintViolation {
                reason: format!(
                    "Branch '{}' is {} and cannot be written to. Use branch_unarchive to reactivate.",
                    branch.as_str(),
                    status,
                ),
            });
        }
    }
    Ok(())
}
```

**File:** `crates/executor/src/executor.rs`

Add the write guard in `execute()` after the read-only check and after `resolve_defaults()`, before dispatch:

```rust
cmd.resolve_defaults();

// NEW: reject writes to non-active branches
if cmd.is_write() {
    if let Some(branch) = cmd.resolved_branch() {
        crate::handlers::require_branch_writable(&self.primitives, branch)?;
    }
}

// NEW: reject data operations on system branches (from Epic 1.2)
// ...
```

This is a **single insertion point** that covers all write operations. No need to modify individual handlers.

### Epic 3.3: Archive/unarchive commands

**File:** `crates/executor/src/command.rs`

Add new command variants after `BranchMerge` (after line 718):
```rust
/// Archive a branch (makes it read-only).
/// Returns: `Output::Unit`
BranchArchive {
    /// Branch to archive.
    branch: BranchId,
    /// Optional message explaining why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
},

/// Unarchive a branch (makes it writable again).
/// Returns: `Output::Unit`
BranchUnarchive {
    /// Branch to unarchive.
    branch: BranchId,
},
```

Add to `is_write()`:
```rust
| Command::BranchArchive { .. }
| Command::BranchUnarchive { .. }
```

Add to `name()`:
```rust
Command::BranchArchive { .. } => "BranchArchive",
Command::BranchUnarchive { .. } => "BranchUnarchive",
```

Add to `resolve_defaults()` (in the branch lifecycle group):
```rust
| Command::BranchArchive { .. }
| Command::BranchUnarchive { .. }
```

**File:** `crates/executor/src/handlers/branch.rs`

Add handlers:
```rust
/// Handle BranchArchive command.
pub fn branch_archive(
    p: &Arc<Primitives>,
    branch: BranchId,
    message: Option<String>,
) -> Result<Output> {
    reject_default_branch(&branch, "archive")?;
    require_branch_exists(p, &branch)?;

    let status = convert_result(branch_dag::dag_get_status(&p.db, branch.as_str()))?;
    match status {
        branch_dag::DagBranchStatus::Archived => return Ok(Output::Unit), // idempotent
        branch_dag::DagBranchStatus::Merged => {
            return Err(Error::ConstraintViolation {
                reason: "Cannot archive a merged branch".to_string(),
            });
        }
        branch_dag::DagBranchStatus::Deleted => {
            return Err(Error::ConstraintViolation {
                reason: "Cannot archive a deleted branch".to_string(),
            });
        }
        branch_dag::DagBranchStatus::Active => {} // proceed
    }

    convert_result(branch_dag::dag_set_status(
        &p.db, branch.as_str(), branch_dag::DagBranchStatus::Archived,
    ))?;

    if let Some(msg) = message {
        let _ = branch_dag::dag_set_message(&p.db, branch.as_str(), &msg);
    }

    Ok(Output::Unit)
}

/// Handle BranchUnarchive command.
pub fn branch_unarchive(
    p: &Arc<Primitives>,
    branch: BranchId,
) -> Result<Output> {
    reject_default_branch(&branch, "unarchive")?;
    require_branch_exists(p, &branch)?;

    let status = convert_result(branch_dag::dag_get_status(&p.db, branch.as_str()))?;
    if status != branch_dag::DagBranchStatus::Archived {
        return Err(Error::ConstraintViolation {
            reason: format!(
                "Cannot unarchive a branch with status '{}'. Only archived branches can be unarchived.",
                status,
            ),
        });
    }

    convert_result(branch_dag::dag_set_status(
        &p.db, branch.as_str(), branch_dag::DagBranchStatus::Active,
    ))?;

    Ok(Output::Unit)
}
```

**File:** `crates/executor/src/executor.rs`

Add dispatch:
```rust
Command::BranchArchive { branch, message } => {
    crate::handlers::branch::branch_archive(&self.primitives, branch, message)
}
Command::BranchUnarchive { branch } => {
    crate::handlers::branch::branch_unarchive(&self.primitives, branch)
}
```

**File:** `crates/executor/src/api/branches.rs`

Add public API:
```rust
/// Archive a branch (makes it read-only).
pub fn archive(&self, name: &str) -> Result<()> {
    self.archive_with_message(name, None)
}

/// Archive a branch with an optional message.
pub fn archive_with_message(&self, name: &str, message: Option<&str>) -> Result<()> {
    match self.executor.execute(Command::BranchArchive {
        branch: BranchId::from(name),
        message: message.map(|s| s.to_string()),
    })? {
        Output::Unit => Ok(()),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchArchive".into(),
        }),
    }
}

/// Unarchive a branch (makes it writable again).
pub fn unarchive(&self, name: &str) -> Result<()> {
    match self.executor.execute(Command::BranchUnarchive {
        branch: BranchId::from(name),
    })? {
        Output::Unit => Ok(()),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchUnarchive".into(),
        }),
    }
}
```

---

## Epic 4: Branch Info and Event Lookup

**Objective:** Expose DAG queries through new commands for branch info, event lookup, and filtered listing.

### Epic 4.1: New commands and output types

**File:** `crates/executor/src/command.rs`

Add:
```rust
/// Get detailed branch info including lineage and merge history.
/// Returns: `Output::BranchDagInfo`
BranchInfo {
    /// Branch name to query.
    branch: String,
},

/// Look up a specific fork or merge event by ID.
/// Returns: `Output::BranchEventInfo`
BranchEvent {
    /// Event ID (e.g., "fork:..." or "merge:...").
    event_id: String,
},

/// Set a message/comment on a branch.
/// Returns: `Output::Unit`
BranchSetMessage {
    /// Target branch.
    branch: BranchId,
    /// Message to set.
    message: String,
},
```

**File:** `crates/executor/src/types.rs`

Add typed structs for branch DAG info (consistent with other typed outputs):
```rust
/// Detailed branch info from the DAG (lineage, merges, children).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDagInfoOutput {
    pub name: String,
    pub status: BranchStatus,
    pub created_at: Option<u64>,
    pub updated_at: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
    pub forked_from: Option<BranchForkEventOutput>,
    pub merges: Vec<BranchMergeEventOutput>,
    pub children: Vec<String>,
}

/// Fork event summary (returned inside BranchDagInfoOutput).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchForkEventOutput {
    pub event_id: String,
    pub parent: String,
    pub timestamp: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
}

/// Merge event summary (returned inside BranchDagInfoOutput).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchMergeEventOutput {
    pub event_id: String,
    pub target: String,
    pub timestamp: u64,
    pub strategy: Option<String>,
    pub keys_applied: Option<u64>,
    pub conflicts: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
}

/// Full event details (fork or merge) from the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchEventOutput {
    pub event_id: String,
    pub event_type: String,  // "fork" or "merge"
    pub timestamp: Option<u64>,
    pub properties: Value,
}
```

**File:** `crates/executor/src/output.rs`

Add:
```rust
/// Detailed branch info from the DAG (lineage, merges, children).
BranchDagInfo(Option<BranchDagInfoOutput>),

/// Fork or merge event details from the DAG.
BranchEventInfo(Option<BranchEventOutput>),
```

### Epic 4.2: Handlers

**File:** `crates/executor/src/handlers/branch.rs`

```rust
/// Handle BranchInfo command.
pub fn branch_info(p: &Arc<Primitives>, branch: String) -> Result<Output> {
    if branch.starts_with("_system") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot query system branch info".to_string(),
        });
    }

    let info = convert_result(branch_dag::dag_get_branch_info(&p.db, &branch))?;
    match info {
        Some(dag_info) => {
            // Convert engine DagBranchInfo → executor typed output
            let output = crate::types::BranchDagInfoOutput {
                name: dag_info.name,
                status: crate::bridge::from_dag_branch_status(dag_info.status),
                created_at: dag_info.created_at,
                updated_at: dag_info.updated_at,
                message: dag_info.message,
                creator: dag_info.creator,
                forked_from: dag_info.forked_from.map(|f| crate::types::BranchForkEventOutput {
                    event_id: f.event_id,
                    parent: f.parent,
                    timestamp: f.timestamp,
                    message: f.message,
                    creator: f.creator,
                }),
                merges: dag_info.merges.into_iter().map(|m| crate::types::BranchMergeEventOutput {
                    event_id: m.event_id,
                    target: m.target,
                    timestamp: m.timestamp,
                    strategy: m.strategy,
                    keys_applied: m.keys_applied,
                    conflicts: m.conflicts,
                    message: m.message,
                    creator: m.creator,
                }).collect(),
                children: dag_info.children,
            };
            Ok(Output::BranchDagInfo(Some(output)))
        }
        None => Ok(Output::BranchDagInfo(None)),
    }
}

/// Handle BranchEvent command.
pub fn branch_event(p: &Arc<Primitives>, event_id: String) -> Result<Output> {
    let graph = strata_engine::GraphStore::new(p.db.clone());
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("_system_");

    let node = convert_result(
        graph.get_node(branch_id, branch_dag::BRANCH_DAG_GRAPH, &event_id),
    )?;

    match node {
        Some(node_data) => {
            let event_type = node_data.object_type.clone().unwrap_or_default();
            let timestamp = node_data.properties.get("timestamp").and_then(|v| v.as_u64());
            Ok(Output::BranchEventInfo(Some(crate::types::BranchEventOutput {
                event_id: event_id.clone(),
                event_type,
                timestamp,
                properties: strata_core::Value::from(node_data.properties),
            })))
        }
        None => Ok(Output::BranchEventInfo(None)),
    }
}

/// Handle BranchSetMessage command.
pub fn branch_set_message(
    p: &Arc<Primitives>,
    branch: BranchId,
    message: String,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    convert_result(branch_dag::dag_set_message(&p.db, branch.as_str(), &message))?;
    Ok(Output::Unit)
}
```

### Epic 4.3: Filtered branch listing

**File:** `crates/executor/src/command.rs`

Extend `BranchList`:
```rust
BranchList {
    state: Option<BranchStatus>,
    limit: Option<u64>,
    offset: Option<u64>,
    /// Filter by parent branch name (immediate children only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    /// Filter: only branches created after this timestamp (micros).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_after: Option<u64>,
    /// Filter: only branches created before this timestamp (micros).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_before: Option<u64>,
},
```

**File:** `crates/executor/src/handlers/branch.rs`

Update `branch_list` to use DAG for filtering when filter parameters are present:
```rust
pub fn branch_list(
    p: &Arc<Primitives>,
    _state: Option<crate::types::BranchStatus>,
    limit: Option<u64>,
    _offset: Option<u64>,
    parent: Option<String>,
    created_after: Option<u64>,
    created_before: Option<u64>,
) -> Result<Output> {
    let ids = convert_result(p.branch.list_branches())?;

    let mut all = Vec::new();
    for id in ids {
        if id.starts_with("_system") {
            continue;
        }

        // Apply parent filter via DAG
        if let Some(ref parent_name) = parent {
            let children = branch_dag::find_children(&p.db, parent_name).unwrap_or_default();
            if !children.contains(&id) {
                continue;
            }
        }

        if let Some(versioned) = convert_result(p.branch.get_branch(&id))? {
            // Apply time filters
            let created = versioned.value.created_at.as_micros();
            if let Some(after) = created_after {
                if created < after { continue; }
            }
            if let Some(before) = created_before {
                if created > before { continue; }
            }

            // Apply status filter via DAG cache
            if let Some(ref status_filter) = _state {
                let dag_filter = crate::bridge::to_dag_branch_status(*status_filter);
                let dag_status = p.db
                    .extension::<strata_engine::branch_status_cache::BranchStatusCache>()
                    .ok()
                    .and_then(|cache| cache.get(&id))
                    .unwrap_or(strata_engine::branch_dag::DagBranchStatus::Active);
                if dag_status != dag_filter {
                    continue;
                }
            }

            // Get DAG status for accurate BranchInfo
            let dag_status = p.db
                .extension::<strata_engine::branch_status_cache::BranchStatusCache>()
                .ok()
                .and_then(|cache| cache.get(&id));
            all.push(versioned_to_branch_info_with_status(versioned, dag_status));
        }
    }

    let limited: Vec<VersionedBranchInfo> = match limit {
        Some(l) => all.into_iter().take(l as usize).collect(),
        None => all,
    };

    Ok(Output::BranchInfoList(limited))
}
```

**Callers that must be updated for the new `branch_list` signature:**

**File:** `crates/executor/src/executor.rs` (line 909-913) — update dispatch:
```rust
Command::BranchList {
    state,
    limit,
    offset,
    parent,          // NEW
    created_after,   // NEW
    created_before,  // NEW
} => crate::handlers::branch::branch_list(
    &self.primitives, state, limit, offset, parent, created_after, created_before,
),
```

**File:** `crates/executor/src/api/branch.rs` (line 59-74) — pass `None` for new params:
```rust
pub fn branch_list(
    &self,
    state: Option<BranchStatus>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Vec<VersionedBranchInfo>> {
    match self.executor.execute(Command::BranchList {
        state,
        limit,
        offset,
        parent: None,          // NEW: default
        created_after: None,   // NEW: default
        created_before: None,  // NEW: default
    })? {
        Output::BranchInfoList(branches) => Ok(branches),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchList".into(),
        }),
    }
}
```

**File:** `crates/executor/src/api/branches.rs` (line 50-63) — same:
```rust
pub fn list(&self) -> Result<Vec<String>> {
    match self.executor.execute(Command::BranchList {
        state: None,
        limit: None,
        offset: None,
        parent: None,          // NEW
        created_after: None,   // NEW
        created_before: None,  // NEW
    })? {
        // ...
    }
}
```

---

## Epic 5: Background Maintenance (DAG Consistency)

**Objective:** Add a periodic DAG consistency check via the existing BackgroundScheduler.

### Epic 5.1: Consistency check

**File:** `crates/engine/src/branch_dag.rs`

```rust
/// Check DAG consistency against BranchIndex and auto-repair missing entries.
///
/// Returns (branches_checked, repairs_made).
pub fn dag_consistency_check(db: &Arc<Database>) -> StrataResult<(usize, usize)> {
    let branch_index = crate::primitives::branch::BranchIndex::new(db.clone());
    let graph = GraphStore::new(db.clone());
    let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let branches = branch_index.list_branches()?;
    let mut repairs = 0;

    for name in &branches {
        if name.starts_with("_system") {
            continue;
        }
        // Check if branch has a DAG node
        if graph.get_node(system_branch_id, BRANCH_DAG_GRAPH, name)?.is_none() {
            // Auto-repair: create missing DAG node
            if let Err(e) = dag_add_branch(db, name, None, None) {
                warn!(target: "strata::branch_dag", error = %e, branch = %name,
                    "Failed to repair missing DAG node");
            } else {
                info!(target: "strata::branch_dag", branch = %name,
                    "Repaired missing DAG node");
                repairs += 1;
            }
        }
    }

    Ok((branches.len(), repairs))
}
```

### Epic 5.2: Schedule on init

**File:** `crates/engine/src/branch_dag.rs`

Add to `init_system_branch()`:
```rust
pub fn init_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    ensure_system_branch(db)?;
    ensure_branch_dag(db)?;
    seed_default_branch(db)?;
    populate_status_cache(db)?;

    // Run initial consistency check (repairs branches created before DAG existed)
    if let Err(e) = dag_consistency_check(db) {
        warn!(target: "strata::branch_dag", error = %e, "DAG consistency check failed");
    }

    Ok(())
}
```

No periodic scheduling in this epic — that belongs in the broader maintenance system (#1408).

---

## File Change Summary

### New Files
| File | Lines (est.) | Purpose |
|------|-------------|---------|
| `crates/engine/src/branch_dag.rs` | ~500 | DAG types, read/write helpers, init, consistency check |
| `crates/engine/src/branch_status_cache.rs` | ~50 | In-memory status cache (DashMap) |

### Modified Files
| File | Changes |
|------|---------|
| `crates/engine/src/lib.rs` | Add `pub mod branch_dag; pub mod branch_status_cache;` |
| `crates/engine/src/database/mod.rs` | Call `init_system_branch()` in 3 init paths |
| `crates/executor/src/command.rs` | Add `BranchArchive`, `BranchUnarchive`, `BranchInfo`, `BranchEvent`, `BranchSetMessage`; extend `BranchFork`/`BranchMerge` with `message`/`creator`; add `resolved_branch()` method; extend `BranchList` with filter params; update `is_write()`, `name()`, `resolve_defaults()` |
| `crates/executor/src/output.rs` | Add `BranchDagInfo(Option<BranchDagInfoOutput>)`, `BranchEventInfo(Option<BranchEventOutput>)` |
| `crates/executor/src/executor.rs` | Add centralized write guard + system branch guard; dispatch new commands |
| `crates/executor/src/handlers/branch.rs` | Guard `_system_`; wire DAG calls into create/fork/merge/delete; add archive/unarchive/info/event/set_message handlers; update fork/merge signatures |
| `crates/executor/src/handlers/mod.rs` | Add `reject_system_branch()`, `require_branch_writable()` |
| `crates/executor/src/api/branch.rs` | Add `branch_archive`, `branch_unarchive`, `branch_info` methods |
| `crates/executor/src/api/branches.rs` | Add `archive()`, `unarchive()`, `info()`, `fork_with_options()`, `merge_with_options()` |
| `crates/executor/src/types.rs` | Add `Merged`, `Archived` to `BranchStatus`; add `BranchDagInfoOutput`, `BranchForkEventOutput`, `BranchMergeEventOutput`, `BranchEventOutput` structs |
| `crates/executor/src/bridge.rs` | Add `from_dag_branch_status()`, `to_dag_branch_status()` conversion functions |
| `crates/executor/src/error.rs` | No changes needed — `ConstraintViolation` already exists |

---

## Implementation Order

```
Epic 1 (system branch infra)
  1.1  Reserve _system_ name          ← start here
  1.2  Hide from APIs
  1.3  Auto-create on init
  Tests

Epic 2 (DAG tracking)                 ← depends on Epic 1
  2.1  Types
  2.2  Write helpers
  2.3  Read helpers
  2.4  Wire into handlers
  2.5  Update Command enum
  Tests

Epic 3 (write rejection)             ← depends on Epic 2
  3.1  Status cache
  3.2  Write guard in executor
  3.3  Archive/unarchive commands
  Tests

Epic 4 (info + listing)              ← depends on Epic 2, parallel with Epic 3
  4.1  New commands + output types
  4.2  Handlers
  4.3  Filtered listing
  Tests

Epic 5 (consistency check)           ← depends on Epic 2, parallel with Epic 3/4
  5.1  Consistency check function
  5.2  Run on init
  Tests
```

Epics 3, 4, and 5 can be developed in parallel after Epic 2 merges.

---

## Review Errata: Issues Found During Self-Review

> **All 10 issues below have been fixed in the main epic text above.**
> This section is preserved for audit trail — it documents what was caught
> and why each fix was necessary.

The following issues were identified through a thorough review of the implementation plan against the actual codebase. Each issue includes the fix needed.

### Issue 1: `create_graph()` Does NOT Error on Duplicates — Idempotency Logic is Wrong

**Problem:** `ensure_branch_dag()` catches errors containing `"already exists"` as a string to handle the idempotent case. But `GraphStore::create_graph()` (`graph/mod.rs:47-77`) **never returns an error for duplicate graph names** — it silently overwrites the metadata and deduplicates the catalog entry. The error-catching code is dead logic.

**Impact:** On every database open, `ensure_branch_dag()` would re-create the graph metadata (overwriting with defaults). This is harmless for default `GraphMeta` but is misleading code.

**Fix:** Replace the error-catching approach with a `get_graph_meta()` check:

```rust
pub fn ensure_branch_dag(db: &Arc<Database>) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Check if graph already exists — don't recreate
    if graph.get_graph_meta(branch_id, BRANCH_DAG_GRAPH)?.is_some() {
        return Ok(());
    }

    graph.create_graph(branch_id, BRANCH_DAG_GRAPH, None)?;
    info!(target: "strata::branch_dag", "Created _branch_dag graph");
    Ok(())
}
```

### Issue 2: Edges Require Both Nodes to Exist — Fork/Merge Recording Can Fail

**Problem:** `GraphStore::add_edge()` (`graph/mod.rs:586-597`) validates that **both source and destination nodes exist** inside the transaction, returning `StrataError::invalid_input` if either is missing. This means:

- `dag_record_fork(parent, child)` creates edges `parent → fork_event → child`. If `parent` has no DAG node (e.g., it was created before the DAG existed), the edge creation fails.
- `dag_record_merge(source, target)` creates edges `source → merge_event → target`. Same problem if either branch lacks a DAG node.

The consistency check (Epic 5) repairs missing nodes on init, but fork/merge happens at runtime — a branch created before the DAG was deployed won't have a node until the next restart.

**Fix:** Add "ensure node exists" logic at the top of `dag_record_fork` and `dag_record_merge`:

```rust
pub fn dag_record_fork(
    db: &Arc<Database>,
    parent: &str,
    child: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // Ensure parent node exists in DAG (may be missing if branch predates DAG)
    ensure_branch_node_exists(db, parent)?;
    // child node should already exist from dag_add_branch() call,
    // but ensure it as a safety net
    ensure_branch_node_exists(db, child)?;

    // ... rest of existing implementation ...
}

pub fn dag_record_merge(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    merge_info: &crate::branch_ops::MergeInfo,
    strategy: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    // Ensure both branch nodes exist in DAG
    ensure_branch_node_exists(db, source)?;
    ensure_branch_node_exists(db, target)?;

    // ... rest of existing implementation ...
}

/// Ensure a branch has a node in the DAG, creating one if missing.
fn ensure_branch_node_exists(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    if graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)?.is_none() {
        dag_add_branch(db, name, None, None)?;
    }
    Ok(())
}
```

### Issue 3: `branch_import` is a Missed Branch Creation Path

**Problem:** `import_branch` (`crates/engine/src/bundle.rs:215-277`) creates a branch via `BranchIndex::create_branch()` at line 232. The implementation plan does not add DAG recording to the `branch_import` handler (`handlers/branch.rs:270`). Imported branches will be invisible to the DAG.

**Fix:** Modify `branch_import` handler to record the imported branch in the DAG:

```rust
pub fn branch_import(p: &Arc<Primitives>, path: String) -> Result<Output> {
    let import_path = std::path::Path::new(&path);
    let info = strata_engine::bundle::import_branch(&p.db, import_path).map_err(|e| Error::Io {
        reason: format!("Import failed: {}", e),
    })?;

    // NEW: record imported branch in DAG (best-effort)
    if let Err(e) = branch_dag::dag_add_branch(&p.db, &info.branch_id, None, None) {
        tracing::warn!(target: "strata::branch_dag", error = %e, "Failed to record imported branch in DAG");
    }

    Ok(Output::BranchImported(crate::types::BranchImportResult {
        branch_id: info.branch_id,
        transactions_applied: info.transactions_applied,
        keys_written: info.keys_written,
    }))
}
```

Also add `branch_import` to the write guard exclusion list — imported branches should not be subject to write rejection since the import itself is a creation operation, not a write to an existing branch.

### Issue 4: Follower Mode Cannot Write — `init_system_branch()` Will Fail

**Problem:** Followers are read-only (no WAL writer: `wal_writer: None`). The plan calls `init_system_branch()` in the follower init path (`open_follower_internal`, line 476), but this function creates a branch, creates a graph, and seeds a node — all write operations. These will fail on a follower because `Database::transaction()` calls `check_accepting()` and then tries to write to the WAL.

**Fix:** Skip system branch initialization on followers. They don't need it — they can't write anyway, so write guards are irrelevant. If branch info queries are needed on followers, they can read the DAG created by the primary:

```rust
// In open_follower_internal — REMOVE the init_system_branch call:
// DON'T DO THIS:
// if let Err(e) = crate::branch_dag::init_system_branch(&db) { ... }

// Instead, only populate the status cache from existing DAG data (read-only):
if let Err(e) = crate::branch_dag::populate_status_cache_readonly(&db) {
    warn!(target: "strata::db", error = %e, "Failed to load branch status cache");
}
```

Add a read-only variant:
```rust
/// Read-only cache population for follower databases.
/// Does NOT create any branches/graphs — only reads existing DAG state.
pub fn populate_status_cache_readonly(db: &Arc<Database>) -> StrataResult<()> {
    let branch_index = crate::primitives::branch::BranchIndex::new(db.clone());
    if !branch_index.exists(SYSTEM_BRANCH)? {
        return Ok(()); // No system branch yet — primary hasn't initialized
    }

    populate_status_cache(db)
}
```

### Issue 5: Type Confusion — Three Separate `BranchStatus` Types

**Problem:** The implementation introduces `DagBranchStatus` in the engine, but doesn't reconcile it with:
- Engine's existing `BranchStatus` (`primitives/branch/index.rs:75`) — only `Active`
- Executor's `BranchStatus` (`types.rs:59`) — only `Active`
- `from_engine_branch_status()` bridge function (`bridge.rs:405`) — maps `Active` → `Active`

The `BranchList` filter parameter uses executor's `BranchStatus`, but filtering by `Merged`/`Archived` won't work unless the executor type is extended and the bridge knows about `DagBranchStatus`.

**Fix:** Extend the executor's `BranchStatus` and add bridge conversion:

```rust
// crates/executor/src/types.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    Active,
    Merged,    // NEW
    Archived,  // NEW
}

// crates/executor/src/bridge.rs — add new conversion function
pub fn from_dag_branch_status(
    status: strata_engine::branch_dag::DagBranchStatus,
) -> crate::types::BranchStatus {
    match status {
        strata_engine::branch_dag::DagBranchStatus::Active => crate::types::BranchStatus::Active,
        strata_engine::branch_dag::DagBranchStatus::Merged => crate::types::BranchStatus::Merged,
        strata_engine::branch_dag::DagBranchStatus::Archived => crate::types::BranchStatus::Archived,
        strata_engine::branch_dag::DagBranchStatus::Deleted => crate::types::BranchStatus::Archived, // Map deleted to archived for user-facing API
    }
}

pub fn to_dag_branch_status(
    status: crate::types::BranchStatus,
) -> strata_engine::branch_dag::DagBranchStatus {
    match status {
        crate::types::BranchStatus::Active => strata_engine::branch_dag::DagBranchStatus::Active,
        crate::types::BranchStatus::Merged => strata_engine::branch_dag::DagBranchStatus::Merged,
        crate::types::BranchStatus::Archived => strata_engine::branch_dag::DagBranchStatus::Archived,
    }
}
```

Also update `metadata_to_branch_info()` in `handlers/branch.rs` to pull status from the DAG cache instead of from `BranchMetadata::status` (which is always `Active`):

```rust
fn metadata_to_branch_info(m: &BranchMetadata, dag_status: Option<DagBranchStatus>) -> BranchInfo {
    BranchInfo {
        id: BranchId::from(m.name.clone()),
        status: dag_status
            .map(from_dag_branch_status)
            .unwrap_or(from_engine_branch_status(m.status)),
        created_at: m.created_at.as_micros(),
        updated_at: m.updated_at.as_micros(),
        parent_id: None,
    }
}
```

### Issue 6: `find_children` Visibility — Called From Handler But Defined as Private

**Problem:** Epic 4.3 calls `branch_dag::find_children(&p.db, parent_name)` from the executor handler, but `find_children` is defined as `fn find_children(...)` (private) in `branch_dag.rs`.

**Fix:** Change visibility to `pub`:
```rust
pub fn find_children(db: &Arc<Database>, branch_name: &str) -> StrataResult<Vec<String>> {
```

### Issue 7: `branch_list` Handler Signature Change Without Updating All Callers

**Problem:** Epic 4.3 adds `parent`, `created_after`, `created_before` parameters to `branch_list`, but the handler is called from multiple places:
- `executor.rs` dispatch (line 909-913)
- `api/branch.rs:branch_list` (line 59-74)
- `api/branches.rs:list` (line 50-63)

All callers must be updated to pass the new parameters, and the `Command::BranchList` variants in `resolve_defaults()` and `name()` must also be updated.

**Fix:** Ensure all dispatch sites pass the new fields. The `Command::BranchList` enum already has the new fields (Epic 4.3 specifies this), so the executor dispatch just needs to destructure them:

```rust
Command::BranchList {
    state,
    limit,
    offset,
    parent,          // NEW
    created_after,   // NEW
    created_before,  // NEW
} => crate::handlers::branch::branch_list(
    &self.primitives, state, limit, offset, parent, created_after, created_before,
),
```

And `api/branch.rs:branch_list` needs the new params:
```rust
pub fn branch_list(
    &self,
    state: Option<BranchStatus>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Vec<VersionedBranchInfo>> {
    match self.executor.execute(Command::BranchList {
        state,
        limit,
        offset,
        parent: None,          // NEW: default
        created_after: None,   // NEW: default
        created_before: None,  // NEW: default
    })? {
        // ...
    }
}
```

### Issue 8: `Branches::merge()` Must Pass `None` for New Optional Fields

**Problem:** The existing `Branches::merge()` API constructs `Command::BranchMerge` without `message`/`creator` fields. After Epic 2.5, these become required fields in the enum (even if `Option`).

**Fix:** Already handled correctly — `#[serde(default, skip_serializing_if = "Option::is_none")]` on the new fields means deserialization from JSON won't break. But the Rust API constructor in `Branches::merge()` must explicitly pass `None`:

```rust
pub fn merge(&self, source: &str, target: &str, strategy: MergeStrategy) -> Result<MergeInfo> {
    match self.executor.execute(Command::BranchMerge {
        source: source.to_string(),
        target: target.to_string(),
        strategy,
        message: None,   // NEW: explicit None
        creator: None,   // NEW: explicit None
    })? {
        Output::BranchMerged(info) => Ok(info),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchMerge".into(),
        }),
    }
}
```

Same for `Branches::fork()`:
```rust
pub fn fork(&self, source: &str, destination: &str) -> Result<ForkInfo> {
    match self.executor.execute(Command::BranchFork {
        source: source.to_string(),
        destination: destination.to_string(),
        message: None,   // NEW: explicit None
        creator: None,   // NEW: explicit None
    })? {
        Output::BranchForked(info) => Ok(info),
        _ => Err(Error::Internal {
            reason: "Unexpected output for BranchFork".into(),
        }),
    }
}
```

### Issue 9: Auto-Setting Source to `Merged` After Merge May Surprise Users

**Problem:** The plan auto-sets the source branch status to `Merged` after every successful `branch_merge`. This is a significant behavioral change — currently merges leave the source writable, allowing iterative merge workflows (make changes, merge, make more changes, merge again).

**Impact:** Any existing workflow that does:
```
fork main → feature
write to feature
merge feature → main
write more to feature   ← THIS NOW FAILS
merge feature → main    ← AND THIS
```

**Options:**
1. **Keep auto-merge** (as designed in #1462) — this is the git-like model where merged branches are "done"
2. **Don't auto-merge** — leave status as `Active` after merge, let users explicitly archive
3. **Make it configurable** — add `auto_archive_source: Option<bool>` parameter to merge command

**Recommendation:** Given the issue explicitly states "Sets source branch status to `merged`", keep the auto-merge behavior but document it prominently. Users who want iterative merges should use `branch_unarchive` — oh wait, `Merged` status **cannot be reverted** per the design. This means iterative merge workflows are impossible.

**Fix:** Either:
- (a) Allow `Merged` → `Active` transition via a dedicated `branch_reactivate` command (different from `unarchive`), or
- (b) Don't auto-set `Merged` — only set it if the user explicitly requests it via a parameter, or
- (c) Document this as a deliberate constraint and require users to fork again for iterative workflows

This is a **design decision that needs user input**, not just a code fix. Flag it in the implementation doc.

### Issue 10: `BranchDagInfo` Output Uses Generic `Value` — Inconsistent With Other Outputs

**Problem:** The plan defines `Output::BranchDagInfo(Value)` — wrapping DAG info as a generic `Value`. This is inconsistent with all other branch outputs which use typed structs (`BranchInfo`, `VersionedBranchInfo`, `ForkInfo`, `MergeInfo`). It also means the Python SDK can't provide typed access.

**Fix:** Define a proper typed struct in `crates/executor/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDagInfo {
    pub name: String,
    pub status: BranchStatus,
    pub created_at: Option<u64>,
    pub updated_at: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
    pub forked_from: Option<BranchForkEvent>,
    pub merges: Vec<BranchMergeEvent>,
    pub children: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchForkEvent {
    pub event_id: String,
    pub parent: String,
    pub timestamp: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchMergeEvent {
    pub event_id: String,
    pub target: String,
    pub timestamp: u64,
    pub strategy: Option<String>,
    pub keys_applied: Option<u64>,
    pub conflicts: Option<u64>,
    pub message: Option<String>,
    pub creator: Option<String>,
}
```

And use `Output::BranchDagInfo(BranchDagInfo)` instead of `Output::BranchDagInfo(Value)`.

### Summary of Required Changes

| # | Severity | Issue | Fix Location |
|---|----------|-------|--------------|
| 1 | **Bug** | `create_graph()` idempotency check uses wrong pattern | `branch_dag.rs:ensure_branch_dag()` |
| 2 | **Bug** | Edges fail if branch nodes don't exist in DAG | `branch_dag.rs:dag_record_fork/merge()` |
| 3 | **Gap** | `branch_import` not wired to DAG | `handlers/branch.rs:branch_import()` |
| 4 | **Bug** | Follower init tries to write (will fail) | `database/mod.rs` follower init path |
| 5 | **Gap** | Three `BranchStatus` types not reconciled | `types.rs`, `bridge.rs`, `handlers/branch.rs` |
| 6 | **Bug** | `find_children` is private, called from handler | `branch_dag.rs` visibility |
| 7 | **Gap** | `branch_list` callers not updated for new params | `executor.rs`, `api/branch.rs`, `api/branches.rs` |
| 8 | **Bug** | Existing `merge()`/`fork()` API won't compile | `api/branches.rs` |
| 9 | **Design** | Auto-merge blocks iterative workflows | Needs design decision |
| 10 | **Quality** | Generic `Value` output instead of typed struct | `output.rs`, `types.rs` |
