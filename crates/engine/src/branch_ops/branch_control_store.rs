//! `BranchControlStore`: engine-owned canonical branch control truth.
//!
//! Persists `BranchControlRecord` keyed by `BranchRef`, plus lineage edges for
//! fork/merge/revert/cherry-pick and an active-pointer index. Implements B3.1
//! of `docs/design/branching/b3-phasing-plan.md`.
//!
//! ## Authority
//!
//! After B3.1 migration completes, this store is the authoritative source for:
//!
//! - per-branch control truth (lifecycle status, fork anchor, name ↔ `BranchRef`)
//! - branch lineage (fork + merge + revert + cherry-pick edges)
//! - merge-base queries
//!
//! `_branch_dag` is demoted to a derived projection (rebuilt from this store)
//! and `storage.get_fork_info()` remains execution truth for CoW, not branch
//! truth.
//!
//! ## Storage layout
//!
//! All rows live under `global_namespace() / TypeTag::Branch`:
//!
//! - `__ctl__/<id_hex>/<generation>` — `BranchControlRecord` (JSON)
//! - `__ctl__next_gen__/<id_hex>` — `u64` next-unused generation
//! - `__ctl__active__/<id_hex>` — `u64` pointer at the **live** (non-Deleted)
//!   lifecycle generation. Set for `Active` and `Archived` records; cleared
//!   when the record transitions to `Deleted` (B4 widened this from
//!   Active-only so the lifecycle write gate can distinguish
//!   `BranchArchived` from `BranchNotFound`).
//! - `__ctl__edge__/<id_hex>/<generation>/<commit_version_padded>` —
//!   `LineageEdgeRecord` (JSON). Commit version is zero-padded decimal
//!   (20 digits, covering `u64::MAX`) so lexical order on the key
//!   matches numeric order; prefix scans return edges in chronological
//!   order.
//!
//! Every scan path that lists user branches must skip keys where
//! [`is_control_store_key`] returns true.
//!
//! ## Generation model
//!
//! `BranchGeneration` is the monotonic per-`BranchId` lifecycle counter. On
//! same-name recreate, [`BranchControlStore::next_generation`] returns the
//! next unused generation and advances the persisted counter atomically.
//! Tombstoned records (`lifecycle = Deleted`) remain in the store so the
//! counter can be reconstructed after recovery.

use std::collections::HashMap;
use std::sync::{Arc, Once};

/// One-shot warn latch for follower legacy synthesis mode (AD5).
/// Process-global: multiple DB instances share the warn to avoid log
/// flooding in multi-tenant scenarios.
static FOLLOWER_SYNTHESIS_WARNED: Once = Once::new();

use serde::{Deserialize, Serialize};
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_storage::{Key, Namespace, TransactionContext, TypeTag};
use tracing::{info, warn};

use crate::branch_domain::{
    is_system_branch, BranchControlRecord, BranchGeneration, BranchLifecycleStatus, BranchRef,
    ForkAnchor,
};
use crate::database::dag_hook::{DagEvent, DagEventKind};
use crate::database::{Database, DatabaseMode};
use crate::{StrataError, StrataResult};

// =============================================================================
// Key prefixes
// =============================================================================

/// Common prefix shared by every control-store row. Used by branch-scan paths
/// to exclude control rows when listing user branches.
const CTL_COMMON_PREFIX: &[u8] = b"__ctl__";

const CTL_RECORD_PREFIX: &str = "__ctl__/";
const CTL_NEXT_GEN_PREFIX: &str = "__ctl__next_gen__/";
const CTL_ACTIVE_PTR_PREFIX: &str = "__ctl__active__/";
const CTL_EDGE_PREFIX: &str = "__ctl__edge__/";

/// Returns `true` if `user_key` addresses a control-store row (record,
/// generation counter, active-pointer index, or lineage edge).
///
/// Call this in every branch-scan path that returns user-facing branch
/// names; otherwise control-store rows will leak into `list_branches`,
/// migration scans, or validation sweeps.
pub(crate) fn is_control_store_key(user_key: &[u8]) -> bool {
    user_key.starts_with(CTL_COMMON_PREFIX)
}

// =============================================================================
// Lineage edge records
// =============================================================================

/// Kind of lineage-advancing event recorded in [`LineageEdgeRecord`].
///
/// `Fork` is represented on both the child's `BranchControlRecord.fork`
/// anchor (primary) and as a `LineageEdgeRecord` (for uniform traversal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum LineageEdgeKind {
    Fork,
    Merge,
    Revert,
    CherryPick,
}

/// A single lineage-advancing event on a branch lifecycle instance.
///
/// Keyed by `(target, commit_version)`. Sufficient to reconstruct the full
/// merge/revert/cherry-pick history of a branch after the fork anchor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LineageEdgeRecord {
    pub kind: LineageEdgeKind,
    pub target: BranchRef,
    /// `Some` for Fork (parent), Merge (source), CherryPick (source).
    /// `None` for Revert.
    pub source: Option<BranchRef>,
    /// Commit version at which the event landed on `target`.
    pub commit_version: CommitVersion,
    /// Three-way merge base; only populated for [`LineageEdgeKind::Merge`].
    pub merge_base: Option<MergeBasePoint>,
}

/// A point in lineage space: a specific `BranchRef` at a specific
/// `CommitVersion`.
///
/// Merge-base identity (AD8): the same `BranchRef` can be merged multiple
/// times; each merge advances the base to a new `MergeBasePoint`. A bare
/// `BranchRef` is insufficient for repeated-merge correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct MergeBasePoint {
    pub branch: BranchRef,
    pub commit_version: CommitVersion,
}

// =============================================================================
// Migration report
// =============================================================================

/// Summary of a single `ensure_migrated` run. Emitted at `info` log level
/// so operators can see how much legacy state was backfilled.
#[derive(Debug, Default, Clone)]
pub(crate) struct MigrationReport {
    pub migrated_branches: usize,
    pub storage_fork_anchors: usize,
    pub dag_fork_anchors: usize,
    pub no_fork_info: usize,
    pub edges_backfilled: usize,
    pub unmatched_dag_events: usize,
}

// =============================================================================
// Typed errors
// =============================================================================

/// Returned by lineage-aware reads on a follower running in legacy
/// synthesis mode (AD5). The follower opened against an unmigrated
/// primary; fork anchors can be synthesized from storage but merge/revert
/// edges cannot, so merge-lineage reads refuse until primary migrates.
///
/// Callers must propagate this as a typed error, not swallow it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchLineageUnavailable {
    pub reason: &'static str,
}

impl BranchLineageUnavailable {
    pub(crate) fn follower_unmigrated() -> Self {
        Self {
            reason: "primary migration required for merge-lineage reads; follower running in legacy synthesis mode",
        }
    }

    pub(crate) fn into_strata_error(self) -> StrataError {
        StrataError::branch_lineage_unavailable(self.reason)
    }
}

// =============================================================================
// Key helpers
// =============================================================================

/// Global namespace shared with `BranchIndex` (nil-UUID branch, `"default"`
/// space). All control-store rows live here.
fn global_namespace() -> Arc<Namespace> {
    Arc::new(Namespace::for_branch(BranchId::from_bytes([0u8; 16])))
}

fn id_hex(id: BranchId) -> String {
    id.to_string()
}

/// Format a `BranchGeneration` zero-padded to 20 decimal digits so lexical
/// order matches numeric order (u64::MAX is 20 digits).
fn format_generation(gen: BranchGeneration) -> String {
    format!("{:020}", gen)
}

/// Format a `CommitVersion` zero-padded to 20 decimal digits for the same
/// reason as [`format_generation`].
fn format_commit_version(v: CommitVersion) -> String {
    format!("{:020}", v.0)
}

pub(crate) fn control_record_key(branch: BranchRef) -> Key {
    let user_key = format!(
        "{CTL_RECORD_PREFIX}{id}/{gen}",
        id = id_hex(branch.id),
        gen = format_generation(branch.generation),
    );
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

pub(crate) fn next_gen_key(id: BranchId) -> Key {
    let user_key = format!("{CTL_NEXT_GEN_PREFIX}{}", id_hex(id));
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

pub(crate) fn active_ptr_key(id: BranchId) -> Key {
    let user_key = format!("{CTL_ACTIVE_PTR_PREFIX}{}", id_hex(id));
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

pub(crate) fn edge_key(target: BranchRef, commit_version: CommitVersion) -> Key {
    let user_key = format!(
        "{CTL_EDGE_PREFIX}{id}/{gen}/{ver}",
        id = id_hex(target.id),
        gen = format_generation(target.generation),
        ver = format_commit_version(commit_version),
    );
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

/// Prefix key for scanning all edges of a given target `BranchRef`.
pub(crate) fn edge_key_prefix_for_target(target: BranchRef) -> Key {
    let user_key = format!(
        "{CTL_EDGE_PREFIX}{id}/{gen}/",
        id = id_hex(target.id),
        gen = format_generation(target.generation),
    );
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

/// Prefix key for scanning all control records (any id, any generation).
pub(crate) fn control_record_prefix_all() -> Key {
    Key::new(
        global_namespace(),
        TypeTag::Branch,
        CTL_RECORD_PREFIX.as_bytes().to_vec(),
    )
}

/// Parse a control-record user key `"__ctl__/<id_hex>/<gen>"` into its
/// `BranchRef`. Returns `None` if the key isn't a control record.
pub(crate) fn parse_control_record_key(user_key: &[u8]) -> Option<BranchRef> {
    let s = std::str::from_utf8(user_key).ok()?;
    let tail = s.strip_prefix(CTL_RECORD_PREFIX)?;
    let (id_part, gen_part) = tail.rsplit_once('/')?;
    // Reject embedded slashes in id_part (would collide with edge keys; edges
    // use a longer prefix so this is only defensive).
    if id_part.contains('/') {
        return None;
    }
    let id = BranchId::from_string(id_part)?;
    let gen: BranchGeneration = gen_part.parse().ok()?;
    Some(BranchRef::new(id, gen))
}

// =============================================================================
// Serialization helpers
// =============================================================================

fn to_stored_value<T: Serialize>(v: &T) -> StrataResult<strata_core::value::Value> {
    serde_json::to_string(v)
        .map(strata_core::value::Value::String)
        .map_err(|e| StrataError::serialization(e.to_string()))
}

fn from_stored_value<T: for<'de> Deserialize<'de>>(
    v: &strata_core::value::Value,
) -> StrataResult<T> {
    match v {
        strata_core::value::Value::String(s) => {
            serde_json::from_str(s).map_err(|e| StrataError::serialization(e.to_string()))
        }
        _ => Err(StrataError::serialization(
            "control-store row must be stored as a JSON string",
        )),
    }
}

fn encode_u64_value(v: u64) -> strata_core::value::Value {
    strata_core::value::Value::Int(v as i64)
}

fn decode_u64_value(v: &strata_core::value::Value) -> StrataResult<u64> {
    match v {
        strata_core::value::Value::Int(n) if *n >= 0 => Ok(*n as u64),
        strata_core::value::Value::Int(n) => Err(StrataError::serialization(format!(
            "control-store counter must be non-negative; got {n}"
        ))),
        other => Err(StrataError::serialization(format!(
            "control-store counter has unexpected type: {other:?}"
        ))),
    }
}

fn global_branch_id() -> BranchId {
    BranchId::from_bytes([0u8; 16])
}

// =============================================================================
// Migration helpers (module-private)
// =============================================================================

/// Sentinel for which source produced a migration-time fork anchor.
enum ForkAnchorSource {
    Storage,
    Dag,
    None,
}

/// Enumerate legacy `BranchMetadata` rows by name, skipping control-store,
/// index, default-marker, and system keys. Returns user-facing branch
/// names in the order produced by the backing storage scan.
fn collect_legacy_branch_metadata(db: &Arc<Database>) -> StrataResult<Vec<String>> {
    let prefix = Key::new(global_namespace(), TypeTag::Branch, Vec::new());
    let rows = db.storage().scan_prefix(&prefix, CommitVersion::MAX)?;
    let mut out = Vec::new();
    for (key, _value) in rows {
        if is_control_store_key(&key.user_key) {
            continue;
        }
        let Ok(name) = String::from_utf8(key.user_key.to_vec()) else {
            continue;
        };
        if name.contains("__idx_") || name == "__default_branch__" || is_system_branch(&name) {
            continue;
        }
        out.push(name);
    }
    Ok(out)
}

/// Collect an uncapped DAG log per migrated branch.
///
/// Missing DAG hook is not an error — we fall through with an empty map and
/// migration proceeds with storage-only fork anchors. But if a hook is
/// installed and a branch log cannot be read, migration fails closed because
/// legacy merge lineage would otherwise be silently dropped.
fn collect_dag_log_per_branch(
    db: &Arc<Database>,
    names: &[String],
) -> StrataResult<HashMap<String, Vec<DagEvent>>> {
    let mut out = HashMap::new();
    let Some(hook) = db.dag_hook().get() else {
        return Ok(out);
    };
    for name in names {
        match hook.log(name, usize::MAX) {
            Ok(events) => {
                out.insert(name.clone(), events);
            }
            Err(e) => {
                return Err(StrataError::corruption(format!(
                    "B3.1 migration could not read legacy DAG history for branch '{name}': {e}"
                )));
            }
        }
    }
    Ok(out)
}

/// Derive the fork anchor for a legacy branch at migration time.
///
/// Priority (AD2):
/// 1. `storage.get_fork_info` — execution-truth fork info from the CoW
///    inherited-layer manifest.
/// 2. Uncapped DAG log — a recorded `Fork` event whose branch matches.
/// 3. `None` — the branch was never forked, or fork ancestry is lost.
fn derive_fork_anchor(
    db: &Arc<Database>,
    canonical_id: BranchId,
    name: &str,
    dag_snapshot: &HashMap<String, Vec<DagEvent>>,
) -> (Option<ForkAnchor>, ForkAnchorSource) {
    if let Some((parent_id, point)) = db.storage().get_fork_info(&canonical_id) {
        let anchor = ForkAnchor {
            parent: BranchRef::new(parent_id, 0),
            point,
        };
        return (Some(anchor), ForkAnchorSource::Storage);
    }
    if let Some(events) = dag_snapshot.get(name) {
        for event in events {
            if event.kind == DagEventKind::Fork && event.branch_id == canonical_id {
                if let Some(parent_id) = event.source_branch_id {
                    let anchor = ForkAnchor {
                        parent: BranchRef::new(parent_id, 0),
                        point: event.commit_version,
                    };
                    return (Some(anchor), ForkAnchorSource::Dag);
                }
            }
        }
    }
    (None, ForkAnchorSource::None)
}

/// Convert a legacy `DagEvent` into a [`LineageEdgeRecord`] for backfill.
///
/// - `Fork`: Fork edge with parent as source.
/// - `Merge`: Merge edge with source + merge_base from the event's merge
///   info.
/// - `Revert`: Revert edge (no source).
/// - `CherryPick`: CherryPick edge with source.
/// - `BranchCreate` / `BranchDelete`: lifecycle events, not lineage
///   edges — returns `None` and the caller skips them silently.
///
/// Gen-0 `BranchRef`s throughout — legacy events predate generation
/// tracking (AD1).
fn dag_event_to_edge(
    event: &DagEvent,
    target: BranchRef,
) -> StrataResult<Option<LineageEdgeRecord>> {
    match event.kind {
        DagEventKind::Fork => {
            let parent_id = event.source_branch_id.ok_or_else(|| {
                StrataError::corruption(format!(
                    "legacy DAG fork event for '{}' is missing source_branch_id",
                    event.branch_name
                ))
            })?;
            Ok(Some(LineageEdgeRecord {
                kind: LineageEdgeKind::Fork,
                target,
                source: Some(BranchRef::new(parent_id, 0)),
                commit_version: event.commit_version,
                merge_base: None,
            }))
        }
        DagEventKind::Merge => {
            let source_id = event.source_branch_id.ok_or_else(|| {
                StrataError::corruption(format!(
                    "legacy DAG merge event for '{}' is missing source_branch_id",
                    event.branch_name
                ))
            })?;
            let merge_info = event.merge_info.as_ref().ok_or_else(|| {
                StrataError::corruption(format!(
                    "legacy DAG merge event for '{}' is missing MergeInfo",
                    event.branch_name
                ))
            })?;
            let merge_version = merge_info.merge_version.ok_or_else(|| {
                StrataError::corruption(format!(
                    "legacy DAG merge event for '{}' is missing merge_version",
                    event.branch_name
                ))
            })?;
            Ok(Some(LineageEdgeRecord {
                kind: LineageEdgeKind::Merge,
                target,
                source: Some(BranchRef::new(source_id, 0)),
                commit_version: event.commit_version,
                merge_base: Some(MergeBasePoint {
                    branch: BranchRef::new(source_id, 0),
                    commit_version: CommitVersion(merge_version),
                }),
            }))
        }
        DagEventKind::Revert => Ok(Some(LineageEdgeRecord {
            kind: LineageEdgeKind::Revert,
            target,
            source: None,
            commit_version: event.commit_version,
            merge_base: None,
        })),
        DagEventKind::CherryPick => {
            let source_id = event.source_branch_id.ok_or_else(|| {
                StrataError::corruption(format!(
                    "legacy DAG cherry-pick event for '{}' is missing source_branch_id",
                    event.branch_name
                ))
            })?;
            Ok(Some(LineageEdgeRecord {
                kind: LineageEdgeKind::CherryPick,
                target,
                source: Some(BranchRef::new(source_id, 0)),
                commit_version: event.commit_version,
                merge_base: None,
            }))
        }
        DagEventKind::BranchCreate | DagEventKind::BranchDelete => Ok(None),
    }
}

// =============================================================================
// Active-pointer + record-prefix scan helpers
// =============================================================================

fn active_ptr_prefix_all() -> Key {
    Key::new(
        global_namespace(),
        TypeTag::Branch,
        CTL_ACTIVE_PTR_PREFIX.as_bytes().to_vec(),
    )
}

fn control_record_prefix_for_id(id: BranchId) -> Key {
    let user_key = format!("{CTL_RECORD_PREFIX}{}/", id_hex(id));
    Key::new(global_namespace(), TypeTag::Branch, user_key.into_bytes())
}

fn parse_active_ptr_key(user_key: &[u8]) -> Option<BranchId> {
    let s = std::str::from_utf8(user_key).ok()?;
    let tail = s.strip_prefix(CTL_ACTIVE_PTR_PREFIX)?;
    BranchId::from_string(tail)
}

// =============================================================================
// BranchControlStore
// =============================================================================

/// Engine-owned canonical branch control store.
///
/// Read methods open their own short transaction on [`global_branch_id`].
/// Write methods take an external [`TransactionContext`] so callers can
/// batch control-store writes atomically with legacy metadata / storage
/// fork work (e.g. `BranchService::create` in B3.2).
#[derive(Clone)]
pub(crate) struct BranchControlStore {
    db: Arc<Database>,
}

impl BranchControlStore {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn can_synthesize_from_legacy(&self) -> StrataResult<bool> {
        let is_follower = matches!(
            self.db.runtime_signature().map(|sig| sig.mode),
            Some(DatabaseMode::Follower)
        );
        if !is_follower {
            return Ok(false);
        }
        Ok(!self.is_migrated()?)
    }

    pub(crate) fn ensure_lineage_read_available(&self) -> StrataResult<()> {
        self.guard_lineage_read()
    }

    // =========================================================================
    // Write-path methods (take an external transaction)
    // =========================================================================

    /// Write or overwrite a control record.
    ///
    /// Also updates the `__ctl__active__/<id>` pointer, which addresses the
    /// **live** (non-Deleted) lifecycle generation:
    ///
    /// - `Active` / `Archived`: pointer is set to `rec.branch.generation`.
    ///   Archived branches remain findable by name so the B4 lifecycle write
    ///   gate can return `BranchArchived` instead of `BranchNotFound`.
    /// - `Deleted` (or any future non-live variant): pointer is cleared only
    ///   when it still points at *this* generation; we never stomp on a
    ///   different live generation.
    ///
    /// `BranchLifecycleStatus` is `#[non_exhaustive]`; unknown future
    /// variants fall through to the Deleted arm because, by definition,
    /// they are not currently-live lifecycle instances.
    pub(crate) fn put_record(
        &self,
        rec: &BranchControlRecord,
        txn: &mut TransactionContext,
    ) -> StrataResult<()> {
        txn.put(control_record_key(rec.branch), to_stored_value(rec)?)?;
        let ap_key = active_ptr_key(rec.branch.id);
        match rec.lifecycle {
            BranchLifecycleStatus::Active | BranchLifecycleStatus::Archived => {
                txn.put(ap_key, encode_u64_value(rec.branch.generation))?;
            }
            _ => {
                if let Some(v) = txn.get(&ap_key)? {
                    if decode_u64_value(&v)? == rec.branch.generation {
                        txn.delete(ap_key)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Flip a record's lifecycle to `Deleted` and clear the active pointer
    /// if it matches. Returns `InvalidInput` if the record is not found.
    pub(crate) fn mark_deleted(
        &self,
        branch: BranchRef,
        txn: &mut TransactionContext,
    ) -> StrataResult<()> {
        let rec_key = control_record_key(branch);
        let Some(v) = txn.get(&rec_key)? else {
            return Err(StrataError::invalid_input(format!(
                "control record for BranchRef(id={}, gen={}) not found",
                branch.id, branch.generation
            )));
        };
        let mut rec: BranchControlRecord = from_stored_value(&v)?;
        rec.lifecycle = BranchLifecycleStatus::Deleted;
        txn.put(rec_key, to_stored_value(&rec)?)?;

        let ap_key = active_ptr_key(branch.id);
        if let Some(v) = txn.get(&ap_key)? {
            if decode_u64_value(&v)? == branch.generation {
                txn.delete(ap_key)?;
            }
        }
        Ok(())
    }

    /// Read the active-pointer row and `mark_deleted` the resulting
    /// `BranchRef` inside `txn`.
    ///
    /// Used by `BranchService::delete` so the active-generation lookup
    /// is part of the delete transaction's read set — OCC then rejects
    /// any concurrent `delete` + `create` that advances the active
    /// pointer between the read and commit, preventing divergence
    /// between the legacy metadata purge and the control-record
    /// lifecycle flip.
    ///
    /// Returns `Ok(Some(branch_ref))` with the marked ref, `Ok(None)` if
    /// there was no active record for `name`, or `Err` if the record
    /// referenced by the active pointer is missing / corrupted.
    pub(crate) fn mark_deleted_by_name(
        &self,
        name: &str,
        txn: &mut TransactionContext,
    ) -> StrataResult<Option<BranchRef>> {
        let id = BranchId::from_user_name(name);
        let ap_key = active_ptr_key(id);
        let Some(v) = txn.get(&ap_key)? else {
            return Ok(None);
        };
        let generation = decode_u64_value(&v)?;
        let branch_ref = BranchRef::new(id, generation);
        self.mark_deleted(branch_ref, txn)?;
        Ok(Some(branch_ref))
    }

    /// Allocate the next unused generation for `id` and advance the
    /// persisted counter. The returned value is unique within the database
    /// lifetime even across delete-and-recreate cycles.
    pub(crate) fn next_generation(
        &self,
        id: BranchId,
        txn: &mut TransactionContext,
    ) -> StrataResult<BranchGeneration> {
        let key = next_gen_key(id);
        let current = match txn.get(&key)? {
            Some(v) => decode_u64_value(&v)?,
            None => 0,
        };
        let next = current
            .checked_add(1)
            .ok_or_else(|| StrataError::internal("BranchGeneration counter overflowed u64"))?;
        txn.put(key, encode_u64_value(next))?;
        Ok(current)
    }

    /// Seed the next-generation counter for `id` to `value` inside `txn`.
    ///
    /// Used by bundle import (B3.4 / AD7) when the target DB has no prior
    /// history for a branch name and the bundle carries a non-default
    /// generation: the imported control record lands at the bundle's
    /// generation and the counter must be advanced past it so a later
    /// recreate allocates a strictly-larger generation.
    ///
    /// Refuses to lower the counter — overwriting a higher persisted
    /// value would risk re-issuing a generation that an earlier write
    /// already claimed.
    pub(crate) fn seed_next_generation(
        &self,
        id: BranchId,
        value: u64,
        txn: &mut TransactionContext,
    ) -> StrataResult<()> {
        let key = next_gen_key(id);
        if let Some(current) = txn.get(&key)? {
            let current = decode_u64_value(&current)?;
            if current >= value {
                return Ok(());
            }
        }
        txn.put(key, encode_u64_value(value))?;
        Ok(())
    }

    /// Append a lineage edge record. Overwrites any existing edge at the
    /// same `(target, commit_version)` key — callers should treat edge
    /// writes as exactly-once per branch mutation.
    pub(crate) fn append_edge(
        &self,
        edge: &LineageEdgeRecord,
        txn: &mut TransactionContext,
    ) -> StrataResult<()> {
        txn.put(
            edge_key(edge.target, edge.commit_version),
            to_stored_value(edge)?,
        )?;
        Ok(())
    }

    /// Delete a lineage edge at the exact `(target, commit_version)` key.
    ///
    /// Used by `BranchMutation` rollback when a later DAG write fails after the
    /// authoritative edge has already been persisted.
    pub(crate) fn delete_edge(
        &self,
        target: BranchRef,
        commit_version: CommitVersion,
        txn: &mut TransactionContext,
    ) -> StrataResult<()> {
        txn.delete(edge_key(target, commit_version))?;
        Ok(())
    }

    // =========================================================================
    // Read-path methods (open their own transaction)
    // =========================================================================

    /// Get a record by its full `BranchRef`. `None` if no such record.
    pub(crate) fn get_record(
        &self,
        branch: BranchRef,
    ) -> StrataResult<Option<BranchControlRecord>> {
        let key = control_record_key(branch);
        self.db
            .transaction(global_branch_id(), |txn| match txn.get(&key)? {
                Some(v) => Ok(Some(from_stored_value::<BranchControlRecord>(&v)?)),
                None => Ok(None),
            })
    }

    /// Look up the currently-live (non-Deleted) record for a branch name.
    /// Uses the O(1) active-pointer index row (AD4).
    ///
    /// Returns the record for both `Active` and `Archived` lifecycle states
    /// (the pointer tracks live generations per B4/KD1). Callers who need a
    /// writability guarantee should use [`require_writable_by_name`] instead.
    ///
    /// On an unmigrated follower (AD5), falls back to synthesizing a
    /// gen-0 [`BranchControlRecord`] from legacy `BranchMetadata` + fork
    /// info derived from storage. Not persisted. Warn logged once per
    /// process.
    pub(crate) fn find_active_by_name(
        &self,
        name: &str,
    ) -> StrataResult<Option<BranchControlRecord>> {
        let id = BranchId::from_user_name(name);
        let ap_key = active_ptr_key(id);
        let primary = self.db.transaction(global_branch_id(), |txn| {
            let Some(v) = txn.get(&ap_key)? else {
                let mut has_live_record = false;
                for (_k, v) in txn.scan_prefix(&control_record_prefix_for_id(id))? {
                    let rec: BranchControlRecord = from_stored_value(&v)?;
                    if rec.lifecycle.is_visible() {
                        has_live_record = true;
                        break;
                    }
                }
                if has_live_record {
                    return Err(StrataError::corruption(format!(
                        "control-store active pointer missing for branch '{name}' (id={id})"
                    )));
                }
                return Ok(None);
            };
            let gen = decode_u64_value(&v)?;
            let rec_key = control_record_key(BranchRef::new(id, gen));
            match txn.get(&rec_key)? {
                Some(rv) => {
                    let rec: BranchControlRecord = from_stored_value(&rv)?;
                    if !rec.lifecycle.is_visible() {
                        return Err(StrataError::corruption(format!(
                            "control-store active pointer for branch '{name}' (id={id}, gen={gen}) points to a non-live record"
                        )));
                    }
                    Ok(Some(rec))
                }
                None => Err(StrataError::corruption(format!(
                    "control-store active pointer for branch '{name}' (id={id}, gen={gen}) points to a missing record"
                ))),
            }
        })?;
        if primary.is_some() {
            return Ok(primary);
        }
        if !self.can_synthesize_from_legacy()? {
            return Ok(None);
        }
        self.synthesize_from_legacy(name, id)
    }

    /// Require a **writable** (lifecycle = `Active`) control record for `name`.
    ///
    /// Maps onto the lifecycle write gate installed at every `BranchService`
    /// mutation entry point (B4):
    ///
    /// - `Active` → `Ok(record)`.
    /// - `Archived` → `Err(StrataError::BranchArchived { name })`.
    /// - `Deleted` or missing → `Err(StrataError::BranchNotFoundByName { .. })`.
    /// - Follower-synthesis refusal (AD5) → propagates
    ///   `BranchLineageUnavailable`.
    pub(crate) fn require_writable_by_name(&self, name: &str) -> StrataResult<BranchControlRecord> {
        let record = self
            .find_active_by_name(name)?
            .ok_or_else(|| StrataError::branch_not_found_by_name(name))?;
        if record.lifecycle.allows_writes() {
            Ok(record)
        } else {
            // The only non-writable live variant that `find_active_by_name`
            // can return is `Archived`; `Deleted` clears the pointer and is
            // handled by the `ok_or_else` arm above. Future non-live
            // variants would have to add their own arm before landing.
            Err(StrataError::branch_archived(name))
        }
    }

    /// Require a **visible** (lifecycle = `Active` or `Archived`) control
    /// record for `name`.
    ///
    /// Used for source-side checks on fork / merge / cherry-pick where the
    /// source is read, not written:
    ///
    /// - `Active` or `Archived` → `Ok(record)`.
    /// - `Deleted` or missing → `Err(StrataError::BranchNotFoundByName { .. })`.
    /// - Follower-synthesis refusal (AD5) → propagates
    ///   `BranchLineageUnavailable`.
    pub(crate) fn require_visible_by_name(&self, name: &str) -> StrataResult<BranchControlRecord> {
        self.find_active_by_name(name)?
            .ok_or_else(|| StrataError::branch_not_found_by_name(name))
    }

    /// Return the currently-active generation for `id`, if any.
    ///
    /// This is the branch-lifecycle guard used by transaction begin/commit:
    /// transactions snapshot the active generation at start, then abort on
    /// commit if the branch has been deleted or recreated in the meantime.
    pub(crate) fn active_generation_for_id(&self, id: BranchId) -> StrataResult<Option<u64>> {
        let ap_key = active_ptr_key(id);
        match self
            .db
            .storage()
            .get_versioned(&ap_key, CommitVersion::MAX)?
        {
            Some(v) => Ok(Some(decode_u64_value(&v.value)?)),
            None => {
                for (_k, v) in self
                    .db
                    .storage()
                    .scan_prefix(&control_record_prefix_for_id(id), CommitVersion::MAX)?
                {
                    let rec: BranchControlRecord = from_stored_value(&v.value)?;
                    if rec.lifecycle.is_visible() {
                        return Err(StrataError::corruption(format!(
                            "control-store active pointer missing for branch id={id} with live record present"
                        )));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Synthesize a gen-0 `BranchControlRecord` for a follower running
    /// in legacy mode (AD5). Returns `None` if there is no legacy
    /// metadata for `name` either. Fork anchor is derived from storage.
    fn synthesize_from_legacy(
        &self,
        name: &str,
        id: BranchId,
    ) -> StrataResult<Option<BranchControlRecord>> {
        // Legacy `BranchMetadata` lives at the same global-namespace,
        // TypeTag::Branch, user-key = branch name (without control-store
        // prefixes).
        let legacy_key = Key::new(
            global_namespace(),
            TypeTag::Branch,
            name.as_bytes().to_vec(),
        );
        let has_legacy = self.db.transaction(global_branch_id(), |txn| {
            Ok(txn.get(&legacy_key)?.is_some())
        })?;
        if !has_legacy {
            return Ok(None);
        }

        FOLLOWER_SYNTHESIS_WARNED.call_once(|| {
            warn!(
                target: "strata::branch::follower",
                "BranchControlStore: primary has not migrated; follower running in legacy synthesis mode. Merge-lineage reads will refuse until primary migrates."
            );
        });

        let fork = self
            .db
            .storage()
            .get_fork_info(&id)
            .map(|(parent_id, point)| ForkAnchor {
                parent: BranchRef::new(parent_id, 0),
                point,
            });

        Ok(Some(BranchControlRecord {
            branch: BranchRef::new(id, 0),
            name: name.to_string(),
            lifecycle: BranchLifecycleStatus::Active,
            fork,
        }))
    }

    /// `true` if any control record has been written to this database.
    /// Used to detect unmigrated-follower state for merge-lineage refusal.
    fn is_migrated(&self) -> StrataResult<bool> {
        let prefix = control_record_prefix_all();
        self.db.transaction(global_branch_id(), |txn| {
            Ok(!txn.scan_prefix(&prefix)?.is_empty())
        })
    }

    /// Returns `Err(BranchLineageUnavailable)` if the store is unmigrated
    /// AND at least one legacy branch exists. This is the AD5 follower
    /// fall-closed signal for lineage-only reads (`list_active`,
    /// `edges_for`, `find_merge_base`).
    fn guard_lineage_read(&self) -> StrataResult<()> {
        if self.is_migrated()? {
            return Ok(());
        }
        if !self.can_synthesize_from_legacy()? {
            return Ok(());
        }
        // Empty databases (no legacy metadata either) should not trip
        // the guard — reads return empty results normally.
        let prefix = Key::new(global_namespace(), TypeTag::Branch, Vec::new());
        let any_legacy = self.db.transaction(global_branch_id(), |txn| {
            let rows = txn.scan_prefix(&prefix)?;
            for (k, _) in rows {
                if is_control_store_key(&k.user_key) {
                    continue;
                }
                let Ok(name) = String::from_utf8(k.user_key.to_vec()) else {
                    continue;
                };
                if name.contains("__idx_")
                    || name == "__default_branch__"
                    || is_system_branch(&name)
                {
                    continue;
                }
                return Ok(true);
            }
            Ok(false)
        })?;
        if any_legacy {
            return Err(BranchLineageUnavailable::follower_unmigrated().into_strata_error());
        }
        Ok(())
    }

    /// List every currently-active record in the store.
    ///
    /// Returns `Err(BranchLineageUnavailable)` on an unmigrated follower
    /// (AD5) — listing is a lineage-aware read and must not silently
    /// return empty when the authoritative state is inaccessible.
    #[allow(dead_code)]
    pub(crate) fn list_active(&self) -> StrataResult<Vec<BranchControlRecord>> {
        self.guard_lineage_read()?;
        self.db.transaction(global_branch_id(), |txn| {
            let prefix = active_ptr_prefix_all();
            let pointers = txn.scan_prefix(&prefix)?;
            let mut out = Vec::with_capacity(pointers.len());
            for (ap_key, v) in pointers {
                let Some(id) = parse_active_ptr_key(&ap_key.user_key) else {
                    continue;
                };
                let gen = decode_u64_value(&v)?;
                let rec_key = control_record_key(BranchRef::new(id, gen));
                if let Some(rv) = txn.get(&rec_key)? {
                    let rec: BranchControlRecord = from_stored_value(&rv)?;
                    if matches!(rec.lifecycle, BranchLifecycleStatus::Active) {
                        out.push(rec);
                    }
                }
            }
            Ok(out)
        })
    }

    /// List every currently-visible record in the store.
    ///
    /// Includes both `Active` and `Archived` lifecycle states because both are
    /// backed by the active-pointer row and remain branch-visible per B4/KD1.
    ///
    /// Returns `Err(BranchLineageUnavailable)` on an unmigrated follower
    /// (AD5).
    pub(crate) fn list_visible(&self) -> StrataResult<Vec<BranchControlRecord>> {
        self.guard_lineage_read()?;
        self.db.transaction(global_branch_id(), |txn| {
            let prefix = active_ptr_prefix_all();
            let pointers = txn.scan_prefix(&prefix)?;
            let mut out = Vec::with_capacity(pointers.len());
            for (ap_key, v) in pointers {
                let Some(id) = parse_active_ptr_key(&ap_key.user_key) else {
                    continue;
                };
                let gen = decode_u64_value(&v)?;
                let rec_key = control_record_key(BranchRef::new(id, gen));
                if let Some(rv) = txn.get(&rec_key)? {
                    let rec: BranchControlRecord = from_stored_value(&rv)?;
                    if rec.lifecycle.is_visible() {
                        out.push(rec);
                    }
                }
            }
            Ok(out)
        })
    }

    /// All lineage edges whose `target` is `branch`, in commit-version
    /// order. Keys encode `commit_version` as zero-padded decimal so
    /// lexical prefix-scan order matches numeric order.
    ///
    /// Returns `Err(BranchLineageUnavailable)` on an unmigrated follower
    /// (AD5).
    pub(crate) fn edges_for(&self, target: BranchRef) -> StrataResult<Vec<LineageEdgeRecord>> {
        self.guard_lineage_read()?;
        let prefix = edge_key_prefix_for_target(target);
        self.db.transaction(global_branch_id(), |txn| {
            let rows = txn.scan_prefix(&prefix)?;
            rows.into_iter()
                .map(|(_, v)| from_stored_value::<LineageEdgeRecord>(&v))
                .collect()
        })
    }

    /// Compute the merge base of two lifecycle instances as a
    /// `MergeBasePoint` per AD8.
    ///
    /// Algorithm (B3.3 point semantics):
    ///
    /// 1. Build each branch's ancestor chain — its own history expanded
    ///    through every `Fork → parent` anchor and every `Merge → source`
    ///    edge, with the commit version at which each event made the
    ///    ancestor visible on this branch.
    /// 2. Intersect the two chains on `BranchRef`; for each shared
    ///    `BranchRef`, the shared visibility point is
    ///    `min(a_commit_version, b_commit_version)`.
    /// 3. Return the candidate with the largest shared commit version —
    ///    the most recent shared point. Repeated merges between the same
    ///    two generations advance the base because each merge adds a
    ///    fresh point to the target's chain.
    ///
    /// - Returns `None` if the branches are unrelated.
    /// - Returns `Err(BranchLineageUnavailable)` on an unmigrated
    ///   follower (AD5).
    pub(crate) fn find_merge_base(
        &self,
        a: BranchRef,
        b: BranchRef,
    ) -> StrataResult<Option<MergeBasePoint>> {
        self.guard_lineage_read()?;
        if a == b {
            return Ok(Some(MergeBasePoint {
                branch: a,
                commit_version: CommitVersion::MAX,
            }));
        }

        let chain_a = self.ancestor_chain(a)?;
        let chain_b = self.ancestor_chain(b)?;

        // Build a "how far can A see this branch" map. Repeated points
        // for the same branch (e.g. multiple merges from the same source)
        // collapse to the most recent visibility.
        let mut reach_a: HashMap<BranchRef, CommitVersion> = HashMap::new();
        for point in &chain_a {
            reach_a
                .entry(point.branch)
                .and_modify(|cv| {
                    if point.commit_version > *cv {
                        *cv = point.commit_version;
                    }
                })
                .or_insert(point.commit_version);
        }

        // Collapse B's chain the same way so the search picks the most
        // recent point per branch even when the chain contains duplicates
        // (repeated merges).
        let mut reach_b: HashMap<BranchRef, CommitVersion> = HashMap::new();
        for point in &chain_b {
            reach_b
                .entry(point.branch)
                .and_modify(|cv| {
                    if point.commit_version > *cv {
                        *cv = point.commit_version;
                    }
                })
                .or_insert(point.commit_version);
        }

        // Intersect: shared visibility is min(reach_a, reach_b); keep the
        // candidate with the largest shared commit version.
        //
        // Sort by `(id bytes, generation)` before iteration so two
        // candidates with identical shared commit versions resolve to a
        // deterministic winner. `HashMap` iteration order is randomised
        // per-process; without sorting, a tie would produce
        // non-deterministic `merge_base` results across runs and — worse
        // — across replicas reading the same lineage.
        let mut ordered_b: Vec<(BranchRef, CommitVersion)> =
            reach_b.iter().map(|(k, v)| (*k, *v)).collect();
        ordered_b.sort_by(|a, b| {
            a.0.id
                .as_bytes()
                .cmp(b.0.id.as_bytes())
                .then(a.0.generation.cmp(&b.0.generation))
        });
        let mut best: Option<MergeBasePoint> = None;
        for (branch, cv_b) in ordered_b {
            let Some(cv_a) = reach_a.get(&branch) else {
                continue;
            };
            let shared_cv = (*cv_a).min(cv_b);
            match best {
                Some(cur) if shared_cv <= cur.commit_version => {}
                _ => {
                    best = Some(MergeBasePoint {
                        branch,
                        commit_version: shared_cv,
                    })
                }
            }
        }

        Ok(best)
    }

    // =========================================================================
    // Migration + DAG rebuild (B3.1)
    // =========================================================================

    /// Run the one-time migration from legacy `BranchMetadata` rows to
    /// [`BranchControlRecord`]s.
    ///
    /// For each legacy branch without an existing control record:
    ///
    /// 1. Derive [`ForkAnchor`] from `storage.get_fork_info()`, falling
    ///    back to an uncapped DAG-hook `log()` scan if storage has no
    ///    fork info (AD2). If both are absent, `fork: None`.
    /// 2. Write a gen-0 `BranchControlRecord` with `lifecycle: Active`.
    /// 3. Seed `next_gen_counter = 1` and `active_ptr = 0` for the id.
    /// 4. Backfill legacy merge/revert/cherry-pick events as
    ///    `LineageEdgeRecord` rows (AD1) — the store becomes authoritative
    ///    for all lineage, not just fork.
    ///
    /// Everything lands in ONE transaction on [`global_branch_id`] so
    /// partial migration cannot leave followers reading a half-populated
    /// store. Idempotent: second call is a no-op.
    ///
    /// Followers skip this entry point; they get read-side synthesis
    /// instead (AD5).
    pub(crate) fn ensure_migrated(db: &Arc<Database>) -> StrataResult<MigrationReport> {
        let store = Self::new(db.clone());

        // Early-exit if any control record exists — migration ran already.
        let scan_prefix = control_record_prefix_all();
        let existing = db.storage().scan_prefix(&scan_prefix, CommitVersion::MAX)?;
        if !existing.is_empty() {
            return Ok(MigrationReport::default());
        }

        // Pass 1: enumerate legacy branches (outside txn, read-only).
        let legacy = collect_legacy_branch_metadata(db)?;
        if legacy.is_empty() {
            return Ok(MigrationReport::default());
        }

        // Pass 2: gather DAG events per branch (outside txn, read-only).
        let dag_snapshot: HashMap<String, Vec<DagEvent>> = collect_dag_log_per_branch(db, &legacy)?;

        // Pass 3: single atomic write. The outer scan above is the
        // authoritative "already migrated" guard — no re-check needed
        // inside the txn because `ensure_migrated` runs under the
        // primary open path's OPEN_DATABASES mutex, so there is no
        // concurrent first-migration contender.
        let mut report = MigrationReport::default();
        db.transaction(global_branch_id(), |txn| {
            for name in &legacy {
                let canonical_id = BranchId::from_user_name(name);
                let branch_ref = BranchRef::new(canonical_id, 0);

                // Derive fork anchor (storage first, DAG fallback).
                let (fork, anchor_source) =
                    derive_fork_anchor(db, canonical_id, name, &dag_snapshot);
                match anchor_source {
                    ForkAnchorSource::Storage => report.storage_fork_anchors += 1,
                    ForkAnchorSource::Dag => report.dag_fork_anchors += 1,
                    ForkAnchorSource::None => report.no_fork_info += 1,
                }

                let rec = BranchControlRecord {
                    branch: branch_ref,
                    name: name.clone(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork,
                };
                store.put_record(&rec, txn)?;
                // Seed the next-gen counter so subsequent recreate gets gen 1.
                txn.put(next_gen_key(canonical_id), encode_u64_value(1))?;
                report.migrated_branches += 1;

                // Backfill lineage edges from the DAG for this branch.
                if let Some(events) = dag_snapshot.get(name) {
                    for event in events {
                        let Some(edge) = dag_event_to_edge(event, branch_ref)? else {
                            // Skip BranchCreate / BranchDelete (lifecycle,
                            // not lineage).
                            continue;
                        };
                        store.append_edge(&edge, txn)?;
                        report.edges_backfilled += 1;
                    }
                }
            }

            Ok::<_, StrataError>(())
        })?;

        info!(
            target: "strata::branch::migration",
            migrated = report.migrated_branches,
            storage_forks = report.storage_fork_anchors,
            dag_forks = report.dag_fork_anchors,
            no_fork_info = report.no_fork_info,
            edges = report.edges_backfilled,
            unmatched_dag_events = report.unmatched_dag_events,
            "Migrated BranchMetadata → BranchControlStore"
        );
        Ok(report)
    }

    /// Rebuild the `_branch_dag` projection from the authoritative
    /// control-store snapshot.
    ///
    /// Best-effort per AD3 when called from open/recovery paths, but callers
    /// may also use the returned `Err` to fail-closed after a DAG write
    /// partially succeeded. If replay fails after `reset_projection()`, the
    /// hook is reset a second time so callers do not keep a half-rebuilt DAG.
    pub(crate) fn rebuild_dag_projection(db: &Arc<Database>) -> StrataResult<()> {
        let hook = match db.dag_hook().get() {
            Some(hook) => hook,
            None => return Ok(()),
        };

        let records = db.transaction(global_branch_id(), |txn| {
            let mut out = Vec::new();
            for (_k, v) in txn.scan_prefix(&control_record_prefix_all())? {
                out.push(from_stored_value::<BranchControlRecord>(&v)?);
            }
            Ok::<_, StrataError>(out)
        })?;
        let edges = db.transaction(global_branch_id(), |txn| {
            let prefix = Key::new(
                global_namespace(),
                TypeTag::Branch,
                CTL_EDGE_PREFIX.as_bytes().to_vec(),
            );
            let mut out = Vec::new();
            for (_k, v) in txn.scan_prefix(&prefix)? {
                out.push(from_stored_value::<LineageEdgeRecord>(&v)?);
            }
            Ok::<_, StrataError>(out)
        })?;

        hook.reset_projection().map_err(|e| {
            StrataError::internal(format!(
                "failed to reset DAG projection before rebuild: {e}"
            ))
        })?;

        let replay = || -> StrataResult<()> {
            let mut names = HashMap::new();
            for rec in &records {
                names.insert(rec.branch, rec.name.clone());
            }

            // Track which `(child_ref, source_ref, point)` fork anchors we
            // emit from control records so we can suppress duplicate
            // `LineageEdgeKind::Fork` edges further down (legacy migration
            // writes both an anchor and a Fork edge for backfilled history,
            // but rebuild must not double-emit the projection event).
            let mut fork_anchors_emitted: std::collections::HashSet<(
                BranchRef,
                BranchRef,
                CommitVersion,
            )> = std::collections::HashSet::new();
            for rec in &records {
                hook.record_event(
                    &DagEvent::create(rec.branch.id, rec.name.clone()).with_branch_ref(rec.branch),
                )
                .map_err(|e| {
                    StrataError::internal(format!(
                        "failed to rebuild DAG create event for branch '{}': {e}",
                        rec.name
                    ))
                })?;
                if matches!(rec.lifecycle, BranchLifecycleStatus::Deleted) {
                    hook.record_event(
                        &DagEvent::delete(rec.branch.id, rec.name.clone())
                            .with_branch_ref(rec.branch),
                    )
                    .map_err(|e| {
                        StrataError::internal(format!(
                            "failed to rebuild DAG delete event for branch '{}': {e}",
                            rec.name
                        ))
                    })?;
                }
                // B3.4: emit the Fork projection event from the
                // authoritative `ForkAnchor` on the child's control
                // record. Live forks (B3.2+) do not write a separate
                // `LineageEdgeRecord::Fork`; the anchor is the only
                // place the parent → child relationship is recorded.
                // Without this loop, rebuild would lose every fork
                // event for branches forked after B3.2 landed.
                if let Some(anchor) = rec.fork {
                    let source_name = names.get(&anchor.parent).cloned().ok_or_else(|| {
                        StrataError::corruption(format!(
                            "cannot rebuild DAG projection: fork anchor on '{}' references missing parent BranchRef(id={}, gen={})",
                            rec.name, anchor.parent.id, anchor.parent.generation
                        ))
                    })?;
                    fork_anchors_emitted.insert((rec.branch, anchor.parent, anchor.point));
                    hook.record_event(
                        &DagEvent::fork(
                            rec.branch.id,
                            rec.name.clone(),
                            anchor.parent.id,
                            source_name,
                            anchor.point,
                        )
                        .with_branch_ref(rec.branch)
                        .with_source_branch_ref(anchor.parent),
                    )
                    .map_err(|e| {
                        StrataError::internal(format!(
                            "failed to rebuild DAG fork anchor event for branch '{}': {e}",
                            rec.name
                        ))
                    })?;
                }
            }

            let mut edges = edges;
            edges.sort_by_key(|edge| edge.commit_version.0);
            for edge in edges {
                let Some(target_name) = names.get(&edge.target).cloned() else {
                    return Err(StrataError::corruption(format!(
                        "cannot rebuild DAG projection: missing target name for BranchRef(id={}, gen={})",
                        edge.target.id, edge.target.generation
                    )));
                };
                let event = match edge.kind {
                    LineageEdgeKind::Fork => {
                        let source = edge.source.ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: fork edge for '{}' missing source",
                                target_name
                            ))
                        })?;
                        // Skip if the fork anchor already produced this
                        // event — legacy migration writes both an anchor
                        // and a Fork edge, and we only want one Fork node
                        // in the projection per fork.
                        if fork_anchors_emitted.contains(&(
                            edge.target,
                            source,
                            edge.commit_version,
                        )) {
                            continue;
                        }
                        let source_name = names.get(&source).cloned().ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: missing source name for BranchRef(id={}, gen={})",
                                source.id, source.generation
                            ))
                        })?;
                        DagEvent::fork(
                            edge.target.id,
                            target_name,
                            source.id,
                            source_name,
                            edge.commit_version,
                        )
                        .with_branch_ref(edge.target)
                        .with_source_branch_ref(source)
                    }
                    LineageEdgeKind::Merge => {
                        let source = edge.source.ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: merge edge for '{}' missing source",
                                target_name
                            ))
                        })?;
                        let source_name = names.get(&source).cloned().ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: missing source name for BranchRef(id={}, gen={})",
                                source.id, source.generation
                            ))
                        })?;
                        let merge_info = crate::branch_ops::MergeInfo {
                            source: source_name.clone(),
                            target: target_name.clone(),
                            keys_applied: 0,
                            keys_deleted: 0,
                            conflicts: Vec::new(),
                            spaces_merged: 0,
                            merge_version: Some(edge.commit_version.0),
                        };
                        // Strategy is informational on a replayed DAG
                        // event (the merge is not re-executed). Strict is
                        // used here to avoid a production
                        // `MergeStrategy::LastWriterWins` literal that
                        // would drift the B5 rename tripwire.
                        DagEvent::merge(
                            edge.target.id,
                            target_name,
                            source.id,
                            source_name,
                            edge.commit_version,
                            merge_info,
                            crate::branch_ops::MergeStrategy::Strict,
                        )
                        .with_branch_ref(edge.target)
                        .with_source_branch_ref(source)
                    }
                    LineageEdgeKind::Revert => {
                        let revert_info = crate::branch_ops::RevertInfo {
                            branch: target_name.clone(),
                            from_version: edge.commit_version,
                            to_version: edge.commit_version,
                            keys_reverted: 0,
                            revert_version: Some(edge.commit_version),
                        };
                        DagEvent::revert(
                            edge.target.id,
                            target_name,
                            edge.commit_version,
                            revert_info,
                        )
                        .with_branch_ref(edge.target)
                    }
                    LineageEdgeKind::CherryPick => {
                        let source = edge.source.ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: cherry-pick edge for '{}' missing source",
                                target_name
                            ))
                        })?;
                        let source_name = names.get(&source).cloned().ok_or_else(|| {
                            StrataError::corruption(format!(
                                "cannot rebuild DAG projection: missing source name for BranchRef(id={}, gen={})",
                                source.id, source.generation
                            ))
                        })?;
                        let info = crate::branch_ops::CherryPickInfo {
                            source: source_name.clone(),
                            target: target_name.clone(),
                            keys_applied: 0,
                            keys_deleted: 0,
                            cherry_pick_version: Some(edge.commit_version.0),
                        };
                        DagEvent::cherry_pick(
                            edge.target.id,
                            target_name,
                            source.id,
                            source_name,
                            edge.commit_version,
                            info,
                        )
                        .with_branch_ref(edge.target)
                        .with_source_branch_ref(source)
                    }
                };

                hook.record_event(&event).map_err(|e| {
                    StrataError::internal(format!(
                        "failed to rebuild DAG projection event at version {}: {e}",
                        edge.commit_version.0
                    ))
                })?;
            }

            Ok(())
        };

        if let Err(replay_error) = replay() {
            if let Err(reset_error) = hook.reset_projection() {
                return Err(StrataError::corruption(format!(
                    "failed to rebuild DAG projection ({replay_error}) and failed to reset partial projection ({reset_error})"
                )));
            }
            return Err(StrataError::internal(format!(
                "failed to rebuild DAG projection: {replay_error}"
            )));
        }

        Ok(())
    }

    /// Produce the ancestor chain for `branch`: every `BranchRef` whose
    /// state `branch` has visibility into, with the commit version at
    /// which that visibility was established. The chain is the union of:
    ///
    /// 1. `branch` itself at [`CommitVersion::MAX`] (it can see all of
    ///    its own history).
    /// 2. The fork anchor of every walked ancestor — each fork parent
    ///    appears at `fork.point`, the version of the parent at which
    ///    the descendant was branched.
    /// 3. Every merge source that landed on the walked chain *at or before the
    ///    currently-visible point* for that branch, recorded at the merge
    ///    commit version — the version of the source at which its state flowed
    ///    into the target.
    ///
    /// Visibility-safe: the walk carries a `visible_until` bound for every
    /// queued `BranchRef`. A merge that landed on `feature` at `v20` must not
    /// become visible through `main` merely because `main` merged `feature` at
    /// `v10`; only edges with `edge.commit_version <= visible_until` are
    /// traversed. Repeated merges can expose *more* of the same ancestor, so we
    /// revisit a `BranchRef` when a later path reaches it at a higher visible
    /// version than any prior path.
    fn ancestor_chain(&self, branch: BranchRef) -> StrataResult<Vec<MergeBasePoint>> {
        let mut chain = vec![MergeBasePoint {
            branch,
            commit_version: CommitVersion::MAX,
        }];
        let mut explored_until: HashMap<BranchRef, CommitVersion> = HashMap::new();
        explored_until.insert(branch, CommitVersion::MAX);

        // DFS over fork parents and merge sources, carrying the maximum commit
        // version of `current` that is actually visible on the original branch.
        let mut queue: Vec<(BranchRef, CommitVersion)> = vec![(branch, CommitVersion::MAX)];
        while let Some((current, visible_until)) = queue.pop() {
            // Fork anchor: the parent's version at the fork point.
            if let Some(rec) = self.get_record(current)? {
                if let Some(anchor) = rec.fork {
                    let parent_visible_until = visible_until.min(anchor.point);
                    chain.push(MergeBasePoint {
                        branch: anchor.parent,
                        commit_version: parent_visible_until,
                    });
                    let should_enqueue = !matches!(
                        explored_until.get(&anchor.parent),
                        Some(prev) if *prev >= parent_visible_until
                    );
                    if should_enqueue {
                        explored_until.insert(anchor.parent, parent_visible_until);
                        queue.push((anchor.parent, parent_visible_until));
                    }
                }
            }

            // Merge edges landing on `current`: every merge introduces
            // visibility into the source's history at the merge commit
            // version. Revert / CherryPick edges do not advance merge
            // visibility — revert is a target-only rewind, cherry-pick
            // applies a cherry-picked subset without claiming full merge
            // incorporation.
            for edge in self.edges_for(current)?.into_iter() {
                if edge.kind != LineageEdgeKind::Merge {
                    continue;
                }
                if edge.commit_version > visible_until {
                    continue;
                }
                let Some(source) = edge.source else {
                    continue;
                };
                chain.push(MergeBasePoint {
                    branch: source,
                    commit_version: edge.commit_version,
                });
                let should_enqueue = !matches!(
                    explored_until.get(&source),
                    Some(prev) if *prev >= edge.commit_version
                );
                if should_enqueue {
                    explored_until.insert(source, edge.commit_version);
                    queue.push((source, edge.commit_version));
                }
            }
        }
        Ok(chain)
    }
}

// =============================================================================
// Tests (key encoding + scan filter)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_control_store_key_matches_all_four_prefixes() {
        assert!(is_control_store_key(b"__ctl__/abc/0"));
        assert!(is_control_store_key(b"__ctl__next_gen__/abc"));
        assert!(is_control_store_key(b"__ctl__active__/abc"));
        assert!(is_control_store_key(b"__ctl__edge__/abc/0/0000000000"));
    }

    #[test]
    fn is_control_store_key_rejects_unrelated_keys() {
        assert!(!is_control_store_key(b"my-branch"));
        assert!(!is_control_store_key(b"__idx_name"));
        assert!(!is_control_store_key(b"__default_branch__"));
        assert!(!is_control_store_key(b"_system_"));
        // Prefix must match exactly; a branch literally named "__ctl_" is
        // hypothetically allowed by the branch-name validator, but any name
        // starting with "__ctl__" is reserved by this check.
        assert!(!is_control_store_key(b"__ctl_not_ours"));
    }

    #[test]
    fn control_record_key_round_trips() {
        let id = BranchId::from_user_name("feature/x");
        let r = BranchRef::new(id, 3);
        let key = control_record_key(r);
        assert!(is_control_store_key(&key.user_key));
        let parsed = parse_control_record_key(&key.user_key).expect("parses back");
        assert_eq!(parsed, r);
    }

    #[test]
    fn control_record_key_generation_ordering_matches_numeric() {
        let id = BranchId::from_user_name("main");
        let k0 = control_record_key(BranchRef::new(id, 0)).user_key;
        let k1 = control_record_key(BranchRef::new(id, 1)).user_key;
        let k10 = control_record_key(BranchRef::new(id, 10)).user_key;
        let k100 = control_record_key(BranchRef::new(id, 100)).user_key;
        // Zero-padding means lexical order matches numeric order, so prefix
        // scans return generations in sequence.
        assert!(k0 < k1);
        assert!(k1 < k10);
        assert!(k10 < k100);
    }

    #[test]
    fn edge_key_commit_version_ordering_matches_numeric() {
        let id = BranchId::from_user_name("feature/y");
        let r = BranchRef::new(id, 0);
        let e0 = edge_key(r, CommitVersion(0)).user_key;
        let e1 = edge_key(r, CommitVersion(1)).user_key;
        let e_big = edge_key(r, CommitVersion(u64::MAX)).user_key;
        assert!(e0 < e1);
        assert!(e1 < e_big);
    }

    #[test]
    fn next_gen_and_active_ptr_keys_are_distinct_from_records() {
        let id = BranchId::from_user_name("main");
        let rec = control_record_key(BranchRef::new(id, 0)).user_key;
        let ng = next_gen_key(id).user_key;
        let ap = active_ptr_key(id).user_key;
        assert_ne!(rec, ng);
        assert_ne!(rec, ap);
        assert_ne!(ng, ap);
        assert!(is_control_store_key(&rec));
        assert!(is_control_store_key(&ng));
        assert!(is_control_store_key(&ap));
    }

    #[test]
    fn parse_control_record_key_rejects_non_record_keys() {
        assert!(parse_control_record_key(b"__ctl__next_gen__/abc").is_none());
        assert!(parse_control_record_key(b"__ctl__active__/abc").is_none());
        assert!(parse_control_record_key(b"__ctl__edge__/abc/0/0").is_none());
        assert!(parse_control_record_key(b"not-a-control-key").is_none());
    }

    #[test]
    fn format_generation_preserves_ordering_to_u64_max() {
        let low = format_generation(0);
        let high = format_generation(u64::MAX);
        assert!(low < high);
        assert_eq!(low.len(), 20);
        assert_eq!(high.len(), 20);
    }

    #[test]
    fn lineage_edge_record_round_trips_through_serde() {
        let target = BranchRef::new(BranchId::from_user_name("feature/z"), 2);
        let source = BranchRef::new(BranchId::from_user_name("main"), 0);
        let rec = LineageEdgeRecord {
            kind: LineageEdgeKind::Merge,
            target,
            source: Some(source),
            commit_version: CommitVersion(42),
            merge_base: Some(MergeBasePoint {
                branch: source,
                commit_version: CommitVersion(10),
            }),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: LineageEdgeRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rec);
    }

    #[test]
    fn merge_base_point_round_trips() {
        let point = MergeBasePoint {
            branch: BranchRef::new(BranchId::from_user_name("main"), 0),
            commit_version: CommitVersion(7),
        };
        let json = serde_json::to_string(&point).unwrap();
        let back: MergeBasePoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back, point);
    }

    #[test]
    fn value_u64_round_trip() {
        let v = encode_u64_value(42);
        assert_eq!(decode_u64_value(&v).unwrap(), 42);

        // Reject negative / wrong-typed values.
        let neg = strata_core::value::Value::Int(-1);
        assert!(decode_u64_value(&neg).is_err());
        let str_v = strata_core::value::Value::String("nope".into());
        assert!(decode_u64_value(&str_v).is_err());
    }

    #[test]
    fn branch_lineage_unavailable_carries_typed_reason() {
        let e = BranchLineageUnavailable::follower_unmigrated();
        assert!(e.reason.contains("primary migration required"));
        let se = e.into_strata_error();
        assert!(matches!(se, StrataError::InvalidOperation { .. }));
        assert!(se.is_branch_lineage_unavailable());
    }

    #[test]
    fn store_can_be_constructed_without_panicking() {
        let db = Database::cache().unwrap();
        let store = BranchControlStore::new(db);
        std::mem::drop(store);
    }

    // =========================================================================
    // Round-trip + generation counter tests
    // =========================================================================

    fn fresh_store() -> (Arc<Database>, BranchControlStore) {
        let db = Database::cache().unwrap();
        let store = BranchControlStore::new(db.clone());
        (db, store)
    }

    fn force_follower_mode(db: &Arc<Database>) {
        let mut sig = db
            .runtime_signature()
            .expect("cache db has runtime signature");
        sig.mode = DatabaseMode::Follower;
        db.set_runtime_signature(sig);
    }

    fn write<F>(db: &Arc<Database>, f: F)
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<()>,
    {
        db.transaction(global_branch_id(), f).unwrap();
    }

    #[test]
    fn put_and_get_record_round_trip_through_storage() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("feature/a"), 0);
        let rec = BranchControlRecord {
            branch,
            name: "feature/a".to_string(),
            lifecycle: BranchLifecycleStatus::Active,
            fork: None,
        };
        write(&db, |txn| store.put_record(&rec, txn));
        let back = store.get_record(branch).unwrap().expect("record persisted");
        assert_eq!(back, rec);
    }

    #[test]
    fn put_active_record_updates_active_pointer() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("main"), 3);
        let rec = BranchControlRecord {
            branch,
            name: "main".to_string(),
            lifecycle: BranchLifecycleStatus::Active,
            fork: None,
        };
        write(&db, |txn| store.put_record(&rec, txn));

        let found = store
            .find_active_by_name("main")
            .unwrap()
            .expect("active pointer points at record");
        assert_eq!(found, rec);
    }

    #[test]
    fn put_deleted_record_clears_matching_active_pointer() {
        let (db, store) = fresh_store();
        let id = BranchId::from_user_name("transient");
        let branch = BranchRef::new(id, 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "transient".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "transient".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: None,
                },
                txn,
            )
        });
        assert!(store.find_active_by_name("transient").unwrap().is_none());
    }

    #[test]
    fn put_deleted_record_preserves_pointer_for_different_generation() {
        // A later active gen-1 record should not lose its active pointer
        // when someone writes a tombstone for gen-0.
        let (db, store) = fresh_store();
        let id = BranchId::from_user_name("redux");
        let gen0 = BranchRef::new(id, 0);
        let gen1 = BranchRef::new(id, 1);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: gen1,
                    name: "redux".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            // Tombstone record for gen-0 arrives later (e.g. via migration
            // cleanup or repair path). Should NOT stomp active pointer.
            store.put_record(
                &BranchControlRecord {
                    branch: gen0,
                    name: "redux".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: None,
                },
                txn,
            )
        });
        let active = store.find_active_by_name("redux").unwrap().expect("gen 1");
        assert_eq!(active.branch, gen1);
    }

    #[test]
    fn mark_deleted_flips_lifecycle_and_clears_active_pointer() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("feature/b"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "feature/b".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        write(&db, |txn| store.mark_deleted(branch, txn));
        let after = store.get_record(branch).unwrap().unwrap();
        assert!(matches!(after.lifecycle, BranchLifecycleStatus::Deleted));
        assert!(store.find_active_by_name("feature/b").unwrap().is_none());
    }

    #[test]
    fn mark_deleted_errors_when_record_absent() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("nope"), 5);
        let err = db
            .transaction(global_branch_id(), |txn| store.mark_deleted(branch, txn))
            .unwrap_err();
        assert!(matches!(err, StrataError::InvalidInput { .. }));
    }

    #[test]
    fn next_generation_is_monotonic_across_delete_recreate() {
        let (db, store) = fresh_store();
        let id = BranchId::from_user_name("cycle");

        let g0 = db
            .transaction(global_branch_id(), |txn| store.next_generation(id, txn))
            .unwrap();
        let g1 = db
            .transaction(global_branch_id(), |txn| store.next_generation(id, txn))
            .unwrap();
        let g2 = db
            .transaction(global_branch_id(), |txn| store.next_generation(id, txn))
            .unwrap();
        assert_eq!(g0, 0);
        assert_eq!(g1, 1);
        assert_eq!(g2, 2);
    }

    #[test]
    fn list_active_returns_only_active_records() {
        let (db, store) = fresh_store();
        let alive = BranchRef::new(BranchId::from_user_name("alive"), 0);
        let dead = BranchRef::new(BranchId::from_user_name("dead"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: alive,
                    name: "alive".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: dead,
                    name: "dead".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.mark_deleted(dead, txn)
        });
        let active: Vec<_> = store
            .list_active()
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0], "alive");
    }

    #[test]
    fn append_and_read_edges_for_target() {
        let (db, store) = fresh_store();
        let target = BranchRef::new(BranchId::from_user_name("edged"), 0);
        let source = BranchRef::new(BranchId::from_user_name("src"), 0);
        write(&db, |txn| {
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target,
                    source: Some(source),
                    commit_version: CommitVersion(10),
                    merge_base: Some(MergeBasePoint {
                        branch: source,
                        commit_version: CommitVersion(5),
                    }),
                },
                txn,
            )?;
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Revert,
                    target,
                    source: None,
                    commit_version: CommitVersion(20),
                    merge_base: None,
                },
                txn,
            )
        });
        let edges = store.edges_for(target).unwrap();
        assert_eq!(edges.len(), 2);
        // Edges come back in ascending commit-version order (big-endian
        // key encoding).
        assert_eq!(edges[0].commit_version, CommitVersion(10));
        assert_eq!(edges[1].commit_version, CommitVersion(20));
    }

    // =========================================================================
    // find_merge_base point-semantics tests (AD8)
    // =========================================================================

    #[test]
    fn find_merge_base_of_identical_branch_returns_max_version() {
        let (_db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("solo"), 0);
        let base = store.find_merge_base(branch, branch).unwrap().unwrap();
        assert_eq!(base.branch, branch);
        assert_eq!(base.commit_version, CommitVersion::MAX);
    }

    #[test]
    fn find_merge_base_of_direct_child_and_parent() {
        let (db, store) = fresh_store();
        let parent = BranchRef::new(BranchId::from_user_name("parent"), 0);
        let child = BranchRef::new(BranchId::from_user_name("child"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: parent,
                    name: "parent".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: child,
                    name: "child".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent,
                        point: CommitVersion(8),
                    }),
                },
                txn,
            )
        });
        let base = store.find_merge_base(parent, child).unwrap().unwrap();
        assert_eq!(base.branch, parent);
        assert_eq!(base.commit_version, CommitVersion(8));
    }

    #[test]
    fn find_merge_base_of_siblings_forked_from_common_parent() {
        let (db, store) = fresh_store();
        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let sib_a = BranchRef::new(BranchId::from_user_name("a"), 0);
        let sib_b = BranchRef::new(BranchId::from_user_name("b"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: sib_a,
                    name: "a".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(5),
                    }),
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: sib_b,
                    name: "b".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(10),
                    }),
                },
                txn,
            )
        });
        let base = store.find_merge_base(sib_a, sib_b).unwrap().unwrap();
        // Both branches can see `main` only up to their respective fork
        // points; shared visibility is the minimum.
        assert_eq!(base.branch, main);
        assert_eq!(base.commit_version, CommitVersion(5));
    }

    #[test]
    fn find_merge_base_advances_past_recorded_merge_point() {
        let (db, store) = fresh_store();
        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let feature = BranchRef::new(BranchId::from_user_name("feature"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feature,
                    name: "feature".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(3),
                    }),
                },
                txn,
            )?;
            // Merge feature -> main at main@v12, advancing the base to
            // feature@v12.
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: main,
                    source: Some(feature),
                    commit_version: CommitVersion(12),
                    merge_base: Some(MergeBasePoint {
                        branch: feature,
                        commit_version: CommitVersion(12),
                    }),
                },
                txn,
            )?;
            // Second merge advances further.
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: main,
                    source: Some(feature),
                    commit_version: CommitVersion(20),
                    merge_base: Some(MergeBasePoint {
                        branch: feature,
                        commit_version: CommitVersion(20),
                    }),
                },
                txn,
            )
        });
        let base = store.find_merge_base(main, feature).unwrap().unwrap();
        assert_eq!(base.branch, feature);
        assert_eq!(
            base.commit_version,
            CommitVersion(20),
            "repeated-merge must advance base to most recent recorded point"
        );
    }

    #[test]
    fn find_merge_base_ignores_unrelated_one_sided_merge_edges() {
        let (db, store) = fresh_store();
        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let feature = BranchRef::new(BranchId::from_user_name("feature"), 0);
        let unrelated = BranchRef::new(BranchId::from_user_name("unrelated"), 0);

        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feature,
                    name: "feature".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(5),
                    }),
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: unrelated,
                    name: "unrelated".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: feature,
                    source: Some(unrelated),
                    commit_version: CommitVersion(99),
                    merge_base: Some(MergeBasePoint {
                        branch: unrelated,
                        commit_version: CommitVersion(99),
                    }),
                },
                txn,
            )
        });

        let base = store.find_merge_base(main, feature).unwrap().unwrap();
        assert_eq!(base.branch, main);
        assert_eq!(
            base.commit_version,
            CommitVersion(5),
            "merge-base between main and feature must not advance from a merge involving an unrelated third branch"
        );
    }

    #[test]
    fn find_merge_base_does_not_leak_future_source_merges_into_target() {
        let (db, store) = fresh_store();
        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let feature = BranchRef::new(BranchId::from_user_name("feature"), 0);
        let bugfix = BranchRef::new(BranchId::from_user_name("bugfix"), 0);

        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feature,
                    name: "feature".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(5),
                    }),
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: bugfix,
                    name: "bugfix".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            // main sees feature only through this merge at v10.
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: main,
                    source: Some(feature),
                    commit_version: CommitVersion(10),
                    merge_base: Some(MergeBasePoint {
                        branch: feature,
                        commit_version: CommitVersion(10),
                    }),
                },
                txn,
            )?;
            // feature later merges bugfix at v20. main must NOT inherit
            // bugfix@v20 through feature because feature was only visible on
            // main through v10.
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: feature,
                    source: Some(bugfix),
                    commit_version: CommitVersion(20),
                    merge_base: Some(MergeBasePoint {
                        branch: bugfix,
                        commit_version: CommitVersion(20),
                    }),
                },
                txn,
            )
        });

        assert!(
            store.find_merge_base(main, bugfix).unwrap().is_none(),
            "main must not see bugfix through a later merge that only landed on feature after main merged feature"
        );

        let base = store.find_merge_base(main, feature).unwrap().unwrap();
        assert_eq!(base.branch, feature);
        assert_eq!(
            base.commit_version,
            CommitVersion(10),
            "main sees feature only through the merge that landed on main"
        );
    }

    #[test]
    fn find_merge_base_across_generations_isolates_lineage_instances() {
        // Same name, two generations. Each generation has its own fork
        // anchor; merge_base must return the gen-1 lineage, not cross
        // over to the gen-0 lineage.
        let (db, store) = fresh_store();
        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let feat_gen0 = BranchRef::new(BranchId::from_user_name("feat"), 0);
        let feat_gen1 = BranchRef::new(BranchId::from_user_name("feat"), 1);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feat_gen0,
                    name: "feat".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(2),
                    }),
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feat_gen1,
                    name: "feat".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(15),
                    }),
                },
                txn,
            )
        });
        let base = store.find_merge_base(main, feat_gen1).unwrap().unwrap();
        assert_eq!(base.branch, main);
        assert_eq!(
            base.commit_version,
            CommitVersion(15),
            "gen-1 fork anchor wins; gen-0 ancestry does not leak"
        );
    }

    // =========================================================================
    // Follower synthesis tests (AD5)
    // =========================================================================

    /// Write a legacy `BranchMetadata` row directly (simulating an
    /// unmigrated database the follower is seeing).
    fn seed_legacy_branch_metadata(db: &Arc<Database>, name: &str) {
        use crate::primitives::branch::BranchMetadata;
        let meta = BranchMetadata::new(name);
        let json = serde_json::to_string(&meta).unwrap();
        db.transaction(global_branch_id(), |txn| {
            let key = Key::new(
                global_namespace(),
                TypeTag::Branch,
                name.as_bytes().to_vec(),
            );
            Ok(txn.put(key, strata_core::value::Value::String(json))?)
        })
        .unwrap();
    }

    #[test]
    fn follower_synthesis_returns_gen0_record_for_legacy_branch() {
        let (db, store) = fresh_store();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy-feature");

        let rec = store
            .find_active_by_name("legacy-feature")
            .unwrap()
            .expect("synthesized record for legacy branch");
        assert_eq!(rec.name, "legacy-feature");
        assert_eq!(rec.branch.generation, 0);
        assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
        // No storage fork info on a fresh cache DB → no synthesized anchor.
        assert!(rec.fork.is_none());
    }

    #[test]
    fn follower_synthesis_returns_none_for_unknown_branch() {
        let (db, store) = fresh_store();
        force_follower_mode(&db);
        assert!(store.find_active_by_name("not-seeded").unwrap().is_none());
    }

    #[test]
    fn follower_synthesis_refuses_find_merge_base_when_legacy_present() {
        let (db, store) = fresh_store();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy");
        let r = BranchRef::new(BranchId::from_user_name("legacy"), 0);
        let err = store.find_merge_base(r, r).unwrap_err();
        assert!(err.is_branch_lineage_unavailable());
        let msg = format!("{err}");
        assert!(
            msg.contains("primary migration required"),
            "error must be the typed lineage-unavailable, got: {msg}"
        );
    }

    #[test]
    fn follower_synthesis_refuses_list_active_when_legacy_present() {
        let (db, store) = fresh_store();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy");
        let err = store.list_active().unwrap_err();
        assert!(err.is_branch_lineage_unavailable());
    }

    #[test]
    fn follower_synthesis_refuses_edges_for_when_legacy_present() {
        let (db, store) = fresh_store();
        force_follower_mode(&db);
        seed_legacy_branch_metadata(&db, "legacy");
        let r = BranchRef::new(BranchId::from_user_name("legacy"), 0);
        let err = store.edges_for(r).unwrap_err();
        assert!(err.is_branch_lineage_unavailable());
    }

    #[test]
    fn legacy_synthesis_does_not_run_on_migrated_or_non_follower_db() {
        let (db, store) = fresh_store();
        seed_legacy_branch_metadata(&db, "legacy-only");
        assert!(
            store.find_active_by_name("legacy-only").unwrap().is_none(),
            "cache/primary-like DB must not synthesize legacy records"
        );

        force_follower_mode(&db);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: BranchRef::new(BranchId::from_user_name("other"), 0),
                    name: "other".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        assert!(
            store.find_active_by_name("legacy-only").unwrap().is_none(),
            "migrated followers must not resurrect branches from legacy metadata"
        );
    }

    #[test]
    fn active_pointer_missing_for_existing_control_record_is_corruption() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("dangling"), 0);
        write(&db, |txn| {
            Ok(txn.put(
                control_record_key(branch),
                to_stored_value(&BranchControlRecord {
                    branch,
                    name: "dangling".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                })?,
            )?)
        });

        let err = store.find_active_by_name("dangling").unwrap_err();
        assert!(format!("{err}").contains("active pointer missing"));
    }

    #[test]
    fn tombstoned_record_without_active_pointer_returns_none() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("deleted"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "deleted".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: None,
                },
                txn,
            )
        });

        assert!(store.find_active_by_name("deleted").unwrap().is_none());
    }

    #[test]
    fn migrated_store_allows_all_lineage_reads_even_with_legacy_present() {
        let (db, store) = fresh_store();
        seed_legacy_branch_metadata(&db, "legacy");
        // Even one control record flips `is_migrated()` → true; from that
        // point on, AD5 synthesis-mode guards stay disabled.
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: BranchRef::new(BranchId::from_user_name("migrated"), 0),
                    name: "migrated".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        assert!(store.list_active().is_ok());
        assert!(store
            .find_merge_base(
                BranchRef::new(BranchId::from_user_name("a"), 0),
                BranchRef::new(BranchId::from_user_name("b"), 0)
            )
            .is_ok());
        assert!(store
            .edges_for(BranchRef::new(BranchId::from_user_name("a"), 0))
            .is_ok());
    }

    // =========================================================================
    // Migration integration tests (disk-backed, end-to-end)
    // =========================================================================

    #[test]
    fn migration_noop_on_empty_database() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Database::open(dir.path()).unwrap();
        // Database::open → primary path → ensure_migrated runs. No
        // legacy branches exist, so migration is a clean no-op.
        let store = BranchControlStore::new(db.clone());
        assert!(!store.is_migrated().unwrap());
    }

    #[test]
    fn migration_writes_gen0_record_for_legacy_branch_on_open() {
        // Simulate an old database: legacy BranchMetadata without any
        // control records. Open the DB — migration should write a
        // gen-0 control record for it.
        let dir = tempfile::TempDir::new().unwrap();
        // First pass: open, create branch via legacy path, close.
        {
            let db = Database::open(dir.path()).unwrap();
            // Write a legacy BranchMetadata directly, bypassing the
            // future BranchService create (which lands in B3.2 and
            // would write a control record itself).
            use crate::primitives::branch::BranchMetadata;
            let meta = BranchMetadata::new("legacy-only");
            let json = serde_json::to_string(&meta).unwrap();
            db.transaction(global_branch_id(), |txn| {
                let key = Key::new(global_namespace(), TypeTag::Branch, b"legacy-only".to_vec());
                Ok(txn.put(key, strata_core::value::Value::String(json))?)
            })
            .unwrap();
        }
        // Second pass: reopen. ensure_migrated should detect the legacy
        // branch and write a gen-0 control record.
        let db = Database::open(dir.path()).unwrap();
        let store = BranchControlStore::new(db.clone());
        let rec = store
            .find_active_by_name("legacy-only")
            .unwrap()
            .expect("control record created by migration");
        assert_eq!(rec.branch.generation, 0);
        assert_eq!(rec.name, "legacy-only");
        assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
        assert!(rec.fork.is_none()); // no storage fork / DAG fork info
    }

    #[test]
    fn migration_seeds_next_gen_counter_at_1_after_first_pass() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let db = Database::open(dir.path()).unwrap();
            use crate::primitives::branch::BranchMetadata;
            let meta = BranchMetadata::new("b");
            let json = serde_json::to_string(&meta).unwrap();
            db.transaction(global_branch_id(), |txn| {
                let key = Key::new(global_namespace(), TypeTag::Branch, b"b".to_vec());
                Ok(txn.put(key, strata_core::value::Value::String(json))?)
            })
            .unwrap();
        }
        let db = Database::open(dir.path()).unwrap();
        let store = BranchControlStore::new(db.clone());
        // Next generation should now return 1 (not 0 — migration
        // seeded the counter to 1 so a future recreate gets gen 1).
        let next = db
            .transaction(global_branch_id(), |txn| {
                store.next_generation(BranchId::from_user_name("b"), txn)
            })
            .unwrap();
        assert_eq!(next, 1);
    }

    #[test]
    fn migration_is_idempotent_across_repeated_opens() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let db = Database::open(dir.path()).unwrap();
            use crate::primitives::branch::BranchMetadata;
            let meta = BranchMetadata::new("persistent");
            let json = serde_json::to_string(&meta).unwrap();
            db.transaction(global_branch_id(), |txn| {
                let key = Key::new(global_namespace(), TypeTag::Branch, b"persistent".to_vec());
                Ok(txn.put(key, strata_core::value::Value::String(json))?)
            })
            .unwrap();
        }
        // First reopen: migration runs.
        {
            let db = Database::open(dir.path()).unwrap();
            let store = BranchControlStore::new(db.clone());
            assert!(store.is_migrated().unwrap());
            let rec = store.find_active_by_name("persistent").unwrap().unwrap();
            assert_eq!(rec.branch.generation, 0);
        }
        // Second reopen: migration is a no-op; records survive.
        {
            let db = Database::open(dir.path()).unwrap();
            let store = BranchControlStore::new(db.clone());
            let rec = store.find_active_by_name("persistent").unwrap().unwrap();
            assert_eq!(rec.branch.generation, 0);
            // Counter is still 1 (no additional writes happened).
            let next = db
                .transaction(global_branch_id(), |txn| {
                    store.next_generation(BranchId::from_user_name("persistent"), txn)
                })
                .unwrap();
            assert_eq!(next, 1);
        }
    }

    /// Recording DAG hook for migration-backfill tests. Captures every
    /// event passed to `record_event` and replays them from `log`.
    ///
    /// Engine cannot depend on the graph crate (where the real
    /// `GraphDagHook` lives), so tests that exercise DAG-backed migration
    /// install this mock instead.
    struct RecordingDagHook {
        events: std::sync::Mutex<Vec<DagEvent>>,
    }

    impl RecordingDagHook {
        fn new() -> Self {
            Self {
                events: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    use crate::database::dag_hook::BranchDagHook as _;

    impl crate::database::dag_hook::BranchDagHook for RecordingDagHook {
        fn name(&self) -> &'static str {
            "recording-test"
        }

        fn record_event(
            &self,
            event: &DagEvent,
        ) -> Result<(), crate::database::dag_hook::BranchDagError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        fn find_merge_base(
            &self,
            _a: &str,
            _b: &str,
        ) -> Result<
            Option<crate::database::dag_hook::MergeBaseResult>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(None)
        }

        fn log(
            &self,
            branch: &str,
            limit: usize,
        ) -> Result<Vec<DagEvent>, crate::database::dag_hook::BranchDagError> {
            let all = self.events.lock().unwrap();
            let id = BranchId::from_user_name(branch);
            let filtered: Vec<DagEvent> = all
                .iter()
                .filter(|e| e.branch_id == id)
                .take(limit)
                .cloned()
                .collect();
            Ok(filtered)
        }

        fn ancestors(
            &self,
            _branch: &str,
        ) -> Result<
            Vec<crate::database::dag_hook::AncestryEntry>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(Vec::new())
        }
    }

    #[test]
    fn migration_derives_fork_anchor_from_storage_on_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        // First pass: create a real fork via BranchService so storage
        // fork info is populated (fork manifests on disk). Install a
        // recording mock DAG hook so `BranchService::fork` — which
        // requires a hook — can run; the test still proves migration
        // derives the fork anchor from **storage** (the DAG log path
        // is exercised by the merge-backfill test below).
        {
            let db = Database::open(dir.path()).unwrap();
            let hook = Arc::new(RecordingDagHook::new());
            db.install_dag_hook(hook).unwrap();
            db.branches().create("parent").unwrap();
            db.transaction(BranchId::from_user_name("parent"), |txn| {
                let ns = Arc::new(Namespace::for_branch(BranchId::from_user_name("parent")));
                let k = Key::new(ns, TypeTag::KV, b"seed".to_vec());
                Ok(txn.put(k, strata_core::value::Value::Int(1))?)
            })
            .unwrap();
            db.branches().fork("parent", "child").unwrap();
        }
        // Reopen: migration rediscovers fork anchor from storage even
        // though no live control record was written (B3.1 predates
        // B3.2's live-write integration) and no DAG hook is installed
        // on this reopen (so the DAG fallback isn't reached).
        let db = Database::open(dir.path()).unwrap();
        let store = BranchControlStore::new(db.clone());
        let child = store.find_active_by_name("child").unwrap().unwrap();
        let anchor = child.fork.expect("fork anchor derived from storage");
        assert_eq!(anchor.parent.id, BranchId::from_user_name("parent"));
        assert_eq!(anchor.parent.generation, 0);
        assert!(anchor.point.0 > 0);
    }

    /// Pure-function test for `dag_event_to_edge`: Fork / Merge /
    /// Revert / CherryPick events each produce the expected
    /// `LineageEdgeKind`; lifecycle events (BranchCreate / BranchDelete)
    /// produce no edge.
    #[test]
    fn dag_event_to_edge_covers_every_lineage_kind() {
        let target = BranchRef::new(BranchId::from_user_name("target"), 0);
        let source_id = BranchId::from_user_name("source");

        let fork = DagEvent::fork(target.id, "target", source_id, "source", CommitVersion(1));
        let fork_edge = dag_event_to_edge(&fork, target)
            .unwrap()
            .expect("Fork → edge");
        assert_eq!(fork_edge.kind, LineageEdgeKind::Fork);
        assert_eq!(fork_edge.source, Some(BranchRef::new(source_id, 0)));

        let merge = DagEvent::merge(
            target.id,
            "target",
            source_id,
            "source",
            CommitVersion(10),
            crate::branch_ops::MergeInfo {
                source: "source".to_string(),
                target: "target".to_string(),
                keys_applied: 1,
                keys_deleted: 0,
                spaces_merged: 1,
                conflicts: Vec::new(),
                merge_version: Some(10),
            },
            // Strict avoids adding a `MergeStrategy::LastWriterWins`
            // literal to a production `.rs` file (B5 tripwire counts
            // test modules too).
            crate::branch_ops::MergeStrategy::Strict,
        );
        let merge_edge = dag_event_to_edge(&merge, target)
            .unwrap()
            .expect("Merge → edge");
        assert_eq!(merge_edge.kind, LineageEdgeKind::Merge);
        assert_eq!(merge_edge.source, Some(BranchRef::new(source_id, 0)));
        assert_eq!(
            merge_edge.merge_base,
            Some(MergeBasePoint {
                branch: BranchRef::new(source_id, 0),
                commit_version: CommitVersion(10),
            })
        );

        let revert = DagEvent::revert(
            target.id,
            "target",
            CommitVersion(5),
            crate::branch_ops::RevertInfo {
                branch: "target".to_string(),
                from_version: CommitVersion(2),
                to_version: CommitVersion(4),
                keys_reverted: 0,
                revert_version: Some(CommitVersion(5)),
            },
        );
        let revert_edge = dag_event_to_edge(&revert, target)
            .unwrap()
            .expect("Revert → edge");
        assert_eq!(revert_edge.kind, LineageEdgeKind::Revert);
        assert!(revert_edge.source.is_none());

        let cherry = DagEvent::cherry_pick(
            target.id,
            "target",
            source_id,
            "source",
            CommitVersion(7),
            crate::branch_ops::CherryPickInfo {
                source: "source".to_string(),
                target: "target".to_string(),
                keys_applied: 1,
                keys_deleted: 0,
                cherry_pick_version: Some(7),
            },
        );
        let cherry_edge = dag_event_to_edge(&cherry, target)
            .unwrap()
            .expect("CherryPick → edge");
        assert_eq!(cherry_edge.kind, LineageEdgeKind::CherryPick);
        assert_eq!(cherry_edge.source, Some(BranchRef::new(source_id, 0)));

        // Lifecycle events produce no lineage edge.
        let create = DagEvent::create(target.id, "target");
        assert!(dag_event_to_edge(&create, target).unwrap().is_none());
        let delete = DagEvent::delete(target.id, "target");
        assert!(dag_event_to_edge(&delete, target).unwrap().is_none());
    }

    /// End-to-end migration backfill: legacy `BranchMetadata` + DAG
    /// events already present at migration time produce the expected
    /// `LineageEdgeRecord` rows in the store.
    ///
    /// Uses `Database::cache()` + direct `ensure_migrated()` call so the
    /// mock DAG hook (process-local, non-persistent) survives into
    /// migration. Disk-backed `Database::open` would auto-run migration
    /// on the first open and lose the mock on reopen.
    #[test]
    fn ensure_migrated_backfills_lineage_edges_from_dag_hook() {
        let db = Database::cache().unwrap();
        let hook = Arc::new(RecordingDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        // Seed legacy `BranchMetadata` for two branches.
        for name in ["parent", "child"] {
            use crate::primitives::branch::BranchMetadata;
            let meta = BranchMetadata::new(name);
            let json = serde_json::to_string(&meta).unwrap();
            db.transaction(global_branch_id(), |txn| {
                let key = Key::new(
                    global_namespace(),
                    TypeTag::Branch,
                    name.as_bytes().to_vec(),
                );
                Ok(txn.put(key, strata_core::value::Value::String(json))?)
            })
            .unwrap();
        }

        // Seed DAG events. Migration reads these via `dag.log()`.
        let parent_id = BranchId::from_user_name("parent");
        let child_id = BranchId::from_user_name("child");
        hook.record_event(&DagEvent::fork(
            child_id,
            "child",
            parent_id,
            "parent",
            CommitVersion(3),
        ))
        .unwrap();
        hook.record_event(&DagEvent::merge(
            parent_id,
            "parent",
            child_id,
            "child",
            CommitVersion(8),
            crate::branch_ops::MergeInfo {
                source: "child".to_string(),
                target: "parent".to_string(),
                keys_applied: 1,
                keys_deleted: 0,
                spaces_merged: 1,
                conflicts: Vec::new(),
                merge_version: Some(8),
            },
            crate::branch_ops::MergeStrategy::Strict,
        ))
        .unwrap();

        // Run migration. Not auto-called on cache opens, so we invoke
        // it explicitly — this is the B3.1 entry point under test.
        let report = BranchControlStore::ensure_migrated(&db).unwrap();
        assert_eq!(report.migrated_branches, 2);
        assert_eq!(report.edges_backfilled, 2);
        assert_eq!(report.dag_fork_anchors, 1, "child's fork anchor from DAG");
        assert_eq!(report.no_fork_info, 1, "parent has no fork anchor");

        // Assert lineage edges landed on the right targets.
        let store = BranchControlStore::new(db.clone());
        let child_ref = BranchRef::new(child_id, 0);
        let parent_ref = BranchRef::new(parent_id, 0);
        let child_edges = store.edges_for(child_ref).unwrap();
        let parent_edges = store.edges_for(parent_ref).unwrap();
        assert_eq!(
            child_edges
                .iter()
                .filter(|e| e.kind == LineageEdgeKind::Fork)
                .count(),
            1,
            "child has a Fork edge backfilled from the DAG log"
        );
        assert_eq!(
            parent_edges
                .iter()
                .filter(|e| e.kind == LineageEdgeKind::Merge)
                .count(),
            1,
            "parent has a Merge edge backfilled from the DAG log"
        );
        let merge_edge = parent_edges
            .iter()
            .find(|e| e.kind == LineageEdgeKind::Merge)
            .expect("merge edge backfilled");
        assert_eq!(
            merge_edge.merge_base,
            Some(MergeBasePoint {
                branch: child_ref,
                commit_version: CommitVersion(8),
            })
        );
        // Child's control record also has the fork anchor set directly
        // (AD1: fork origin lives on the control record for fast lookup).
        let child_rec = store.get_record(child_ref).unwrap().unwrap();
        let anchor = child_rec.fork.expect("child fork anchor");
        assert_eq!(anchor.parent, parent_ref);
        assert_eq!(anchor.point, CommitVersion(3));
    }

    struct FailingLogDagHook;

    impl crate::database::dag_hook::BranchDagHook for FailingLogDagHook {
        fn name(&self) -> &'static str {
            "failing-log-test"
        }

        fn record_event(
            &self,
            _event: &DagEvent,
        ) -> Result<(), crate::database::dag_hook::BranchDagError> {
            Ok(())
        }

        fn find_merge_base(
            &self,
            _a: &str,
            _b: &str,
        ) -> Result<
            Option<crate::database::dag_hook::MergeBaseResult>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(None)
        }

        fn log(
            &self,
            branch: &str,
            _limit: usize,
        ) -> Result<Vec<DagEvent>, crate::database::dag_hook::BranchDagError> {
            Err(crate::database::dag_hook::BranchDagError::read_failed(
                format!("cannot read DAG log for {branch}"),
            ))
        }

        fn ancestors(
            &self,
            _branch: &str,
        ) -> Result<
            Vec<crate::database::dag_hook::AncestryEntry>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(Vec::new())
        }
    }

    #[test]
    fn migration_fails_closed_when_legacy_dag_log_is_unreadable() {
        let db = Database::cache().unwrap();
        db.install_dag_hook(Arc::new(FailingLogDagHook)).unwrap();
        seed_legacy_branch_metadata(&db, "legacy");

        let err = BranchControlStore::ensure_migrated(&db).unwrap_err();
        assert!(format!("{err}").contains("could not read legacy DAG history"));

        let store = BranchControlStore::new(db.clone());
        assert!(
            store
                .get_record(BranchRef::new(BranchId::from_user_name("legacy"), 0))
                .unwrap()
                .is_none(),
            "failed migration must not leave partial control-store state behind"
        );
    }

    #[test]
    fn migration_fails_closed_when_legacy_merge_event_lacks_merge_version() {
        let db = Database::cache().unwrap();
        let hook = Arc::new(RecordingDagHook::new());
        db.install_dag_hook(hook.clone()).unwrap();

        for name in ["parent", "child"] {
            seed_legacy_branch_metadata(&db, name);
        }

        let parent_id = BranchId::from_user_name("parent");
        let child_id = BranchId::from_user_name("child");
        hook.record_event(&DagEvent::merge(
            parent_id,
            "parent",
            child_id,
            "child",
            CommitVersion(8),
            crate::branch_ops::MergeInfo {
                source: "child".to_string(),
                target: "parent".to_string(),
                keys_applied: 1,
                keys_deleted: 0,
                spaces_merged: 1,
                conflicts: Vec::new(),
                merge_version: None,
            },
            crate::branch_ops::MergeStrategy::Strict,
        ))
        .unwrap();

        let err = BranchControlStore::ensure_migrated(&db).unwrap_err();
        assert!(format!("{err}").contains("missing merge_version"));
    }

    #[derive(Default)]
    struct RecordingRebuildHook {
        reset_count: std::sync::Mutex<usize>,
        events: std::sync::Mutex<Vec<DagEvent>>,
    }

    impl crate::database::dag_hook::BranchDagHook for RecordingRebuildHook {
        fn name(&self) -> &'static str {
            "rebuild-recorder"
        }

        fn record_event(
            &self,
            event: &DagEvent,
        ) -> Result<(), crate::database::dag_hook::BranchDagError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        fn reset_projection(&self) -> Result<(), crate::database::dag_hook::BranchDagError> {
            *self.reset_count.lock().unwrap() += 1;
            self.events.lock().unwrap().clear();
            Ok(())
        }

        fn find_merge_base(
            &self,
            _a: &str,
            _b: &str,
        ) -> Result<
            Option<crate::database::dag_hook::MergeBaseResult>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(None)
        }

        fn log(
            &self,
            _branch: &str,
            _limit: usize,
        ) -> Result<Vec<DagEvent>, crate::database::dag_hook::BranchDagError> {
            Ok(Vec::new())
        }

        fn ancestors(
            &self,
            _branch: &str,
        ) -> Result<
            Vec<crate::database::dag_hook::AncestryEntry>,
            crate::database::dag_hook::BranchDagError,
        > {
            Ok(Vec::new())
        }
    }

    #[test]
    fn rebuild_dag_projection_replays_authoritative_snapshot_through_hook() {
        let (db, store) = fresh_store();
        let hook = Arc::new(RecordingRebuildHook::default());
        db.install_dag_hook(hook.clone()).unwrap();

        let main = BranchRef::new(BranchId::from_user_name("main"), 0);
        let feature = BranchRef::new(BranchId::from_user_name("feature"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: main,
                    name: "main".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: feature,
                    name: "feature".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: Some(ForkAnchor {
                        parent: main,
                        point: CommitVersion(3),
                    }),
                },
                txn,
            )?;
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Fork,
                    target: feature,
                    source: Some(main),
                    commit_version: CommitVersion(3),
                    merge_base: None,
                },
                txn,
            )?;
            store.append_edge(
                &LineageEdgeRecord {
                    kind: LineageEdgeKind::Merge,
                    target: main,
                    source: Some(feature),
                    commit_version: CommitVersion(8),
                    merge_base: Some(MergeBasePoint {
                        branch: feature,
                        commit_version: CommitVersion(8),
                    }),
                },
                txn,
            )
        });

        BranchControlStore::rebuild_dag_projection(&db).unwrap();

        assert_eq!(*hook.reset_count.lock().unwrap(), 1);
        let events = hook.events.lock().unwrap().clone();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == DagEventKind::BranchCreate)
                .count(),
            2
        );
        assert!(
            events.iter().any(|event| event.kind == DagEventKind::Fork),
            "fork edge must be replayed into the DAG projection"
        );
        assert!(
            events.iter().any(|event| event.kind == DagEventKind::Merge),
            "merge edge must be replayed into the DAG projection"
        );
    }

    #[test]
    fn find_merge_base_returns_none_for_unrelated_branches() {
        let (db, store) = fresh_store();
        let a = BranchRef::new(BranchId::from_user_name("island-a"), 0);
        let b = BranchRef::new(BranchId::from_user_name("island-b"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: a,
                    name: "island-a".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )?;
            store.put_record(
                &BranchControlRecord {
                    branch: b,
                    name: "island-b".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        assert!(store.find_merge_base(a, b).unwrap().is_none());
    }

    // =========================================================================
    // B4.1: lifecycle write gate helpers
    // =========================================================================

    /// Build an `Archived` record for `name@generation` directly through
    /// `put_record`. No public engine path produces `Archived` today (B4
    /// explicitly deferred the archive operation); tests synthesize it to
    /// exercise the gate.
    fn seed_archived_record(
        db: &Arc<Database>,
        store: &BranchControlStore,
        name: &str,
    ) -> BranchRef {
        let id = BranchId::from_user_name(name);
        let branch = BranchRef::new(id, 0);
        write(db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: name.to_string(),
                    lifecycle: BranchLifecycleStatus::Archived,
                    fork: None,
                },
                txn,
            )
        });
        branch
    }

    #[test]
    fn put_archived_record_sets_active_pointer() {
        // KD1: Archived keeps the pointer set so lifecycle gates can
        // distinguish `BranchArchived` from `BranchNotFound`.
        let (db, store) = fresh_store();
        let branch = seed_archived_record(&db, &store, "retired");
        let found = store
            .find_active_by_name("retired")
            .unwrap()
            .expect("archived record must remain findable by name");
        assert_eq!(found.branch, branch);
        assert_eq!(found.lifecycle, BranchLifecycleStatus::Archived);
    }

    #[test]
    fn transitioning_active_to_archived_keeps_pointer_at_same_generation() {
        let (db, store) = fresh_store();
        let id = BranchId::from_user_name("promote-demote");
        let branch = BranchRef::new(id, 7);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "promote-demote".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "promote-demote".to_string(),
                    lifecycle: BranchLifecycleStatus::Archived,
                    fork: None,
                },
                txn,
            )
        });
        assert_eq!(store.active_generation_for_id(id).unwrap(), Some(7));
        let rec = store
            .find_active_by_name("promote-demote")
            .unwrap()
            .expect("archived record stays findable by name");
        assert_eq!(rec.lifecycle, BranchLifecycleStatus::Archived);
    }

    #[test]
    fn require_writable_accepts_active() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("writable"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "writable".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        let rec = store.require_writable_by_name("writable").unwrap();
        assert_eq!(rec.branch, branch);
        assert!(rec.lifecycle.allows_writes());
    }

    #[test]
    fn require_writable_rejects_archived_with_branch_archived() {
        let (db, store) = fresh_store();
        seed_archived_record(&db, &store, "frozen");
        let err = store.require_writable_by_name("frozen").unwrap_err();
        match err {
            StrataError::BranchArchived { name } => assert_eq!(name, "frozen"),
            other => panic!("expected BranchArchived, got {other:?}"),
        }
    }

    #[test]
    fn require_writable_rejects_deleted_with_branch_not_found() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("tombstone"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "tombstone".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: None,
                },
                txn,
            )
        });
        let err = store.require_writable_by_name("tombstone").unwrap_err();
        match err {
            StrataError::BranchNotFoundByName { name, .. } => assert_eq!(name, "tombstone"),
            other => panic!(
                "deleted record must surface as name-preserving BranchNotFound, got {other:?}"
            ),
        }
    }

    #[test]
    fn require_writable_rejects_missing_with_branch_not_found() {
        let (_db, store) = fresh_store();
        let err = store.require_writable_by_name("never-existed").unwrap_err();
        match err {
            StrataError::BranchNotFoundByName { name, .. } => assert_eq!(name, "never-existed"),
            other => panic!(
                "missing branch must surface as name-preserving BranchNotFound, got {other:?}"
            ),
        }
    }

    #[test]
    fn require_visible_accepts_active_and_archived() {
        let (db, store) = fresh_store();
        let active = BranchRef::new(BranchId::from_user_name("live"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch: active,
                    name: "live".to_string(),
                    lifecycle: BranchLifecycleStatus::Active,
                    fork: None,
                },
                txn,
            )
        });
        seed_archived_record(&db, &store, "frozen");

        let live_rec = store.require_visible_by_name("live").unwrap();
        assert_eq!(live_rec.lifecycle, BranchLifecycleStatus::Active);

        let frozen_rec = store.require_visible_by_name("frozen").unwrap();
        assert_eq!(frozen_rec.lifecycle, BranchLifecycleStatus::Archived);
    }

    #[test]
    fn require_visible_rejects_deleted_and_missing_with_branch_not_found() {
        let (db, store) = fresh_store();
        let branch = BranchRef::new(BranchId::from_user_name("gone"), 0);
        write(&db, |txn| {
            store.put_record(
                &BranchControlRecord {
                    branch,
                    name: "gone".to_string(),
                    lifecycle: BranchLifecycleStatus::Deleted,
                    fork: None,
                },
                txn,
            )
        });

        let err_deleted = store.require_visible_by_name("gone").unwrap_err();
        match err_deleted {
            StrataError::BranchNotFoundByName { name, .. } => assert_eq!(name, "gone"),
            other => panic!(
                "deleted branch must surface as name-preserving BranchNotFound, got {other:?}"
            ),
        }

        let err_missing = store.require_visible_by_name("never").unwrap_err();
        match err_missing {
            StrataError::BranchNotFoundByName { name, .. } => assert_eq!(name, "never"),
            other => panic!(
                "missing branch must surface as name-preserving BranchNotFound, got {other:?}"
            ),
        }
    }
}
