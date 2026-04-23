//! Per-primitive merge dispatch.
//!
//! See `docs/design/branching/primitive-aware-merge.md`.
//!
//! ## Role
//!
//! `merge_branches` would otherwise route every primitive type tag through
//! one identical classification + apply pipeline. That's correct for KV but
//! wrong for primitives with higher-level invariants (Event hash chains,
//! Graph adjacency, JSON document structure, Vector index state). This
//! module gives each primitive its own merge handler with a three-method
//! lifecycle: `precheck` (validate before any writes), `plan` (produce the
//! per-primitive write actions), and `post_commit` (post-apply fix-ups).
//!
//! - `KvMergeHandler` is a no-op pass-through — KV semantics are fully
//!   captured by the generic 14-case classification matrix.
//! - `JsonMergeHandler` runs a per-document path-level three-way merge
//!   in `plan` (combining disjoint path edits on the same JSON document
//!   instead of falling back to whole-doc LWW) and refreshes secondary
//!   indexes (`_idx/{space}/{name}`) plus the BM25 `InvertedIndex` for
//!   affected documents in `post_commit`. See
//!   `crates/engine/src/branch_ops/json_merge.rs` for the recursive
//!   object-walk algorithm.
//! - `EventMergeHandler::precheck` runs the Event hash-chain divergence
//!   check via `super::check_event_merge_divergence`. The check itself
//!   lives in `branch_ops/mod.rs` as a free function for historical
//!   reasons; both `merge_branches` and `cherry_pick_from_diff` reach it
//!   through this handler.
//! - `VectorMergeHandler` tracks affected `(space, collection)` pairs in
//!   `plan` and dispatches to the per-database `MergeHandlerRegistry` for
//!   the dimension/metric mismatch precheck and the per-collection HNSW
//!   rebuild in `post_commit`. If unset (engine-only unit tests that don't
//!   load the vector crate), the handler is a pure pass-through and HNSW
//!   backends only catch up to the merged KV state on the next full recovery.
//! - `GraphMergeHandler::plan` dispatches to the per-database
//!   `MergeHandlerRegistry`, which the graph crate populates via
//!   `GraphSubsystem::initialize()`. It implements the semantic merge
//!   algorithm: decoded edge diffing, additive merging of disjoint edges,
//!   referential integrity validation, additive catalog merging. If unset
//!   (engine-only unit tests that don't load the graph crate), the handler
//!   falls back to `check_graph_merge_divergence` — the tactical "refuse
//!   divergent graph merges" rule.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::Mutex;
use strata_core::primitives::json::JsonValue;
use strata_core::types::{BranchId, TypeTag};
use strata_core::value::Value;
use strata_core::{PrimitiveType, StrataResult};

use super::json_merge::merge_json_values;
use super::{
    build_ancestor_map, build_live_map, check_event_merge_divergence, check_graph_merge_divergence,
    classify_typed_entries_for_tag, format_user_key, ConflictEntry, MergeAction, MergeActionKind,
    MergeBase, MergeStrategy, TypedEntries,
};
use crate::database::Database;
use crate::primitives::json::{JsonDoc, JsonStore};

// =============================================================================
// Lifecycle contexts
// =============================================================================

/// Context passed to `PrimitiveMergeHandler::precheck`.
///
/// Borrows the typed entry slices that the diff already gathered, so a
/// handler that needs to validate the merge shape (e.g. Event divergence)
/// can do so without re-reading from storage.
///
/// `db` is provided so handlers whose validation requires decoding logic
/// that lives in a downstream crate (e.g. `VectorMergeHandler` decoding
/// `CollectionRecord` rows in the vector crate) can dispatch to a
/// callback that re-reads from storage rather than threading internal
/// types through the engine.
#[allow(dead_code)] // contract for future handlers
pub(crate) struct MergePrecheckCtx<'a> {
    pub db: &'a Arc<Database>,
    pub source_id: BranchId,
    pub target_id: BranchId,
    pub merge_base: &'a MergeBase,
    pub strategy: MergeStrategy,
    pub typed_entries: &'a TypedEntries,
}

/// Context passed to `PrimitiveMergeHandler::plan`.
///
/// Plan runs after `precheck` and before the merge apply transaction.
/// Each handler returns its own per-primitive write plan; the engine
/// concatenates them all into the single apply transaction.
///
/// KV / Vector / Event handlers use the default trait implementation,
/// which delegates to `classify_typed_entries_for_tag` (the same 14-case
/// decision matrix the generic merge runs). `GraphMergeHandler::plan`
/// overrides the default to dispatch to the graph crate's semantic merge
/// algorithm. `JsonMergeHandler::plan` overrides to do per-document
/// path-level merge AND emit secondary index `MergeAction`s atomically
/// with the doc updates — see `branch_ops/json_merge.rs` and the
/// `JsonMergeHandler` implementation in this file.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates registering
/// graph plan callbacks can borrow it through the function-pointer
/// signature.
///
/// `db` is provided so handlers whose `plan` needs to read auxiliary
/// state from storage (e.g. `JsonMergeHandler` reading `IndexDef`s from
/// `_idx_meta/{space}` to compute secondary index entry deltas) can
/// open a read-only transaction without the engine threading specific
/// auxiliary types through this struct.
pub struct MergePlanCtx<'a> {
    /// Database handle, for handlers that need to read auxiliary state
    /// (index defs, collection configs, etc.) from storage during plan
    /// construction. JSON uses this to load `IndexDef`s before emitting
    /// secondary index actions.
    pub db: &'a Arc<Database>,
    /// The source branch (changes flowing into target).
    pub source_id: BranchId,
    /// The target branch (the merge destination).
    pub target_id: BranchId,
    /// Common ancestor metadata (branch + version).
    pub merge_base: &'a MergeBase,
    /// Conflict resolution strategy.
    pub strategy: MergeStrategy,
    /// Pre-gathered typed entries for ancestor / source / target.
    pub typed_entries: &'a TypedEntries,
}

/// Result of one handler's `plan` method.
///
/// Contains the per-primitive write actions and any conflicts surfaced
/// during planning. The engine concatenates plans from all handlers into
/// the unified `merge_branches` apply transaction.
pub struct PrimitiveMergePlan {
    /// Write actions (puts and deletes) the engine should apply.
    pub actions: Vec<MergeAction>,
    /// Conflicts surfaced during planning. Cell-level conflicts honor
    /// `MergeStrategy`; structural / referential-integrity conflicts are
    /// always reported regardless of strategy.
    pub conflicts: Vec<ConflictEntry>,
}

/// Context passed to `PrimitiveMergeHandler::post_commit`.
///
/// Runs after the merge transaction has successfully committed. All
/// handlers are no-ops here today; the fields are part of the
/// documented contract for future handlers that may need them.
#[allow(dead_code)] // contract for future handlers
pub(crate) struct MergePostCommitCtx<'a> {
    pub db: &'a Arc<Database>,
    pub source_id: BranchId,
    pub target_id: BranchId,
    pub merge_version: Option<u64>,
}

// =============================================================================
// Trait
// =============================================================================

/// A handler for per-primitive merge semantics.
///
/// One implementation per `TypeTag`. Registered with a `MergeHandlerRegistry`
/// constructed once per `merge_branches` call. The trait has three lifecycle
/// methods: `precheck` (validate before any writes), `plan` (produce the
/// per-primitive write actions), and `post_commit` (post-apply fix-ups).
pub(crate) trait PrimitiveMergeHandler: Send + Sync {
    /// The primitive this handler owns.
    fn type_tag(&self) -> TypeTag;

    /// Validate that the merge is allowed for this primitive.
    ///
    /// Called once per merge, before any classification or write happens.
    /// Returning `Err` aborts the entire merge cleanly with no side
    /// effects. Used by `EventMergeHandler` to refuse merges that would
    /// break the Event hash chain. The graph handler's precheck is a
    /// no-op when its semantic merge is registered (validation happens
    /// inside `plan` instead) and falls back to the divergence refusal
    /// otherwise.
    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()>;

    /// Produce the per-primitive write plan.
    ///
    /// Default implementation delegates to `classify_typed_entries_for_tag`
    /// — the same 14-case decision matrix the generic merge runs.
    /// Handlers that need primitive-aware merge logic (like Graph, which
    /// decodes packed adjacency lists and validates referential
    /// integrity) override this with their own implementation.
    ///
    /// The default impl is what KV / JSON / Vector / Event use today
    /// with zero overhead.
    fn plan(&self, ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
        let (actions, conflicts) =
            classify_typed_entries_for_tag(ctx.typed_entries, ctx.strategy, self.type_tag());
        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    /// Apply post-commit fix-ups to secondary state.
    ///
    /// Called after the merge transaction commits successfully. Must be
    /// idempotent — a retry after crash must converge to the same state.
    /// All handlers are no-ops here today; secondary index refresh
    /// happens via `reload_secondary_backends` in `merge_branches`.
    /// The hook is preserved so individual primitives can take ownership
    /// of their own post-merge refresh incrementally.
    fn post_commit(&self, ctx: &MergePostCommitCtx<'_>) -> StrataResult<()>;
}

// =============================================================================
// Registry
// =============================================================================

/// Maps each `TypeTag` to its `PrimitiveMergeHandler`.
///
/// Backed by `BTreeMap` for deterministic iteration order, matching the
/// existing `DATA_TYPE_TAGS` constant. Constructed once per `merge_branches`
/// call (cheap — five `Arc::new` of unit structs).
pub(crate) struct MergeHandlerRegistry {
    handlers: BTreeMap<TypeTag, Arc<dyn PrimitiveMergeHandler>>,
}

impl MergeHandlerRegistry {
    /// Look up the handler for a given type tag. Panics if a handler is
    /// missing — every `TypeTag` in `DATA_TYPE_TAGS` must be registered.
    pub(crate) fn get(&self, tag: TypeTag) -> &Arc<dyn PrimitiveMergeHandler> {
        self.handlers.get(&tag).unwrap_or_else(|| {
            panic!(
                "MergeHandlerRegistry: missing handler for {:?}. \
                 Every TypeTag in DATA_TYPE_TAGS must have a registered handler.",
                tag
            )
        })
    }
}

/// Build the default registry containing one handler per primitive.
///
/// Each handler is registered under the `TypeTag` it reports from
/// `type_tag()`, which both wires up dispatch and enforces the invariant
/// that "the registry key matches the handler's self-identification" at
/// construction time.
pub(crate) fn build_merge_registry() -> MergeHandlerRegistry {
    let mut handlers: BTreeMap<TypeTag, Arc<dyn PrimitiveMergeHandler>> = BTreeMap::new();
    let entries: Vec<Arc<dyn PrimitiveMergeHandler>> = vec![
        Arc::new(KvMergeHandler),
        Arc::new(JsonMergeHandler::new()),
        Arc::new(EventMergeHandler),
        Arc::new(VectorMergeHandler::new()),
        Arc::new(GraphMergeHandler),
    ];
    for handler in entries {
        let tag = handler.type_tag();
        let prev = handlers.insert(tag, handler);
        debug_assert!(
            prev.is_none(),
            "MergeHandlerRegistry: duplicate handler registered for {:?}",
            tag
        );
    }
    MergeHandlerRegistry { handlers }
}

// =============================================================================
// Handlers
// =============================================================================

/// KV merge handler. Pure no-op pass-through.
///
/// KV is the only primitive whose semantics are fully captured by the raw
/// 14-case classification matrix in `classify_change`, so it has nothing to
/// validate or refresh.
pub(crate) struct KvMergeHandler;

impl PrimitiveMergeHandler for KvMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::KV
    }
    fn precheck(&self, _ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        Ok(())
    }

    /// KV BM25 convergence contract (B5.3):
    ///
    /// - ordinary writes: `ConvergenceClass::StagedPublish` via inline
    ///   `InvertedIndex` update inside `KVStore::put` / `KVStore::delete`
    ///   and the `SearchRefreshHook` staged-publish path on followers.
    /// - merge-time: **explicitly `ConvergenceClass::ReopenHealed`**.
    ///   Merge apply writes target rows via raw `txn.put()`, which
    ///   bypasses the inline indexing path in `KVStore::put`, so the
    ///   in-memory BM25 projection for merged KV rows lags until
    ///   `SearchSubsystem::recover` → `reconcile_index` reruns on the
    ///   next database open. Same-session BM25 queries against the
    ///   merge target may miss merged rows until reopen. See
    ///   `branching_convergence_differential.rs::kv_merge_bm25_reopen_heals_stale_index`.
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
}

/// One per-document merge outcome the JSON handler needs to track for
/// secondary derived state. Populated by `plan` (so it has access to
/// the pre-merge target value) and consumed by `post_commit` for the
/// in-memory BM25 `InvertedIndex` refresh.
///
/// Secondary KV indexes (`_idx/{space}/{name}`) are NOT refreshed via
/// this list — `plan` emits `MergeAction`s for them directly, so they
/// commit atomically inside the merge transaction. This struct only
/// carries the in-memory BM25 update payload, which is non-transactional
/// and lives in the `InvertedIndex` engine extension.
struct JsonAffectedDoc {
    space: String,
    doc_id: String,
    /// Post-merge document value, or `None` if the merge deleted the doc.
    new_value: Option<JsonValue>,
}

/// JSON merge handler.
///
/// Implements per-document path-level merge: when both source and target
/// modified the same JSON document but at disjoint paths, the merged
/// document combines both edits instead of falling through to the
/// 14-case classifier's whole-document LWW. Path-level conflicts (same
/// path edited to different values, or subtree-delete vs. edit) honor
/// `MergeStrategy`.
///
/// `plan` emits two kinds of `MergeAction`s for every affected doc:
/// 1. The doc itself (under the user space).
/// 2. The corresponding secondary index entries (under the internal
///    `_idx/{space}/{name}` namespaces) so doc + index updates commit
///    atomically inside the merge transaction.
///
/// `post_commit` is BM25-only: it refreshes the in-memory `InvertedIndex`
/// extension for the affected docs. BM25 is non-transactional and is
/// already crash-safe via `SearchSubsystem` (the search subsystem
/// rebuilds BM25 from KV state on `Database::open`).
///
/// Each merge gets a fresh handler instance via `build_merge_registry()`,
/// so the `affected` mutex is uncontended in practice — it exists only
/// because the trait methods take `&self`.
pub(crate) struct JsonMergeHandler {
    affected: Mutex<Vec<JsonAffectedDoc>>,
}

impl JsonMergeHandler {
    pub(crate) fn new() -> Self {
        Self {
            affected: Mutex::new(Vec::new()),
        }
    }

    /// Decode an MVCC `Value::Bytes` payload into a `JsonDoc`. Returns
    /// `None` for any decode failure — corrupt rows fall through to the
    /// generic 14-case classifier (which copies bytes verbatim) so a
    /// single bad row doesn't poison the whole merge.
    fn decode_doc(value: &Value) -> Option<JsonDoc> {
        JsonStore::deserialize_doc(value).ok()
    }
}

impl PrimitiveMergeHandler for JsonMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Json
    }

    fn precheck(&self, _ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        Ok(())
    }

    fn plan(&self, ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
        use crate::primitives::json::index;

        let mut actions: Vec<MergeAction> = Vec::new();
        let mut conflicts: Vec<ConflictEntry> = Vec::new();
        // (space, doc_id, old_target_value, new_value) tuples accumulated
        // by the per-doc merge. Used twice below: once to emit secondary
        // index `MergeAction`s, and once to populate `self.affected` for
        // BM25 refresh in `post_commit`.
        let mut per_doc: Vec<(String, String, Option<JsonValue>, Option<JsonValue>)> = Vec::new();

        for ((space, type_tag), cell) in &ctx.typed_entries.cells {
            if *type_tag != TypeTag::Json {
                continue;
            }

            // Build per-cell maps keyed by raw user_key bytes (= doc_id
            // bytes), reusing the same helpers the 14-case classifier
            // uses so the JSON handler observes identical filtering /
            // tombstone semantics.
            let ancestor_map = build_ancestor_map(&cell.ancestor, space);
            let source_map = build_live_map(&cell.source, space);
            let target_map = build_live_map(&cell.target, space);

            let mut all_keys: BTreeSet<&Vec<u8>> = BTreeSet::new();
            all_keys.extend(ancestor_map.keys());
            all_keys.extend(source_map.keys());
            all_keys.extend(target_map.keys());

            for user_key in all_keys {
                let ancestor_val = ancestor_map.get(user_key).and_then(|opt| opt.as_ref());
                let source_val = source_map.get(user_key);
                let target_val = target_map.get(user_key);

                // doc_id is the user_key as UTF-8. JSON keys are always
                // UTF-8 (`Key::new_json` only accepts `&str`); a non-UTF-8
                // user_key can only happen if a row is corrupt — fall
                // through to the opaque fallback inside `merge_one_doc`
                // by passing `None`.
                let doc_id = std::str::from_utf8(user_key).ok();

                merge_one_doc(
                    space,
                    user_key,
                    doc_id,
                    ancestor_val,
                    source_val,
                    target_val,
                    ctx.strategy,
                    &mut actions,
                    &mut conflicts,
                    &mut per_doc,
                );
            }
        }

        // Pass 2: emit `MergeAction`s for secondary index entries so they
        // commit atomically inside the same merge transaction as the doc
        // updates. Loads `IndexDef`s from `_idx_meta/{space}` once per
        // space via a read-only transaction on `ctx.db`. If a space has
        // no indexes, the loop body is a no-op for that space.
        let mut indexes_by_space: BTreeMap<String, Vec<index::IndexDef>> = BTreeMap::new();
        let touched_spaces: BTreeSet<&str> =
            per_doc.iter().map(|(s, _, _, _)| s.as_str()).collect();
        for space in touched_spaces {
            let space_owned = space.to_string();
            let result = ctx
                .db
                .transaction(ctx.target_id, |txn| {
                    JsonStore::load_indexes(txn, &ctx.target_id, &space_owned)
                })
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        target: "strata::branch_ops",
                        space = %space_owned,
                        error = %e,
                        "JSON merge: failed to load index defs for atomic refresh — \
                         skipping secondary index updates for this space"
                    );
                    Vec::new()
                });
            indexes_by_space.insert(space_owned, result);
        }

        for (space, doc_id, old_target, new_value) in &per_doc {
            let Some(indexes) = indexes_by_space.get(space) else {
                continue;
            };
            if indexes.is_empty() {
                continue;
            }
            for idx_def in indexes {
                // Old entry: derive from the pre-merge target doc value
                // and emit a delete. If the old doc didn't have the
                // indexed field, no entry to delete.
                if let Some(old_val) = old_target {
                    if let Some(field_val) =
                        index::extract_field_value(old_val, &idx_def.field_path)
                    {
                        if let Some(encoded) = index::encode_value(&field_val, idx_def.index_type) {
                            actions.push(MergeAction {
                                space: index::index_space_name(space, &idx_def.name),
                                raw_key: index::index_entry_user_key(&encoded, doc_id),
                                type_tag: TypeTag::Json,
                                action: MergeActionKind::Delete,
                                expected_target: None, // OCC skipped for `_idx/`
                            });
                        }
                    }
                }
                // New entry: derive from the merged doc value and emit
                // a put. If the merged doc doesn't have the indexed
                // field (or the doc was deleted), no entry to write.
                if let Some(new_val) = new_value {
                    if let Some(field_val) =
                        index::extract_field_value(new_val, &idx_def.field_path)
                    {
                        if let Some(encoded) = index::encode_value(&field_val, idx_def.index_type) {
                            actions.push(MergeAction {
                                space: index::index_space_name(space, &idx_def.name),
                                raw_key: index::index_entry_user_key(&encoded, doc_id),
                                type_tag: TypeTag::Json,
                                action: MergeActionKind::Put(Value::Bytes(vec![])),
                                expected_target: None, // OCC skipped for `_idx/`
                            });
                        }
                    }
                }
            }
        }

        // Stash affected docs for BM25 refresh in `post_commit`.
        // Drop the (space, old_target_value) fields — BM25 only needs
        // (doc_id, new_value).
        let bm25_affected: Vec<JsonAffectedDoc> = per_doc
            .into_iter()
            .map(|(space, doc_id, _old, new_value)| JsonAffectedDoc {
                space,
                doc_id,
                new_value,
            })
            .collect();
        *self.affected.lock() = bm25_affected;

        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    /// JSON BM25 convergence contract (B5.3):
    ///
    /// - ordinary writes: `ConvergenceClass::StagedPublish` via inline
    ///   `InvertedIndex` update inside `JsonStore::create`/`put`/`delete`.
    /// - merge-time: **immediate refresh** via this `post_commit`.
    ///   The affected-doc list is populated by `plan` and drained
    ///   here to re-index the post-merge doc value. Secondary KV
    ///   indexes (`_idx/{space}/{name}`) ride `StorageCoupled` via
    ///   `MergeAction`s emitted in `plan` (they commit atomically
    ///   inside the merge transaction). The best-effort swallow
    ///   below keeps this hook from aborting an already-committed
    ///   merge; on failure, `SearchSubsystem::recover` →
    ///   `reconcile_index` heals stale entries at the next reopen
    ///   (`ConvergenceClass::ReopenHealed` fallback). See
    ///   `branching_convergence_differential.rs::json_merge_bm25_refreshed_immediately_no_reopen_needed`
    ///   and `::json_bm25_reopens_from_kv_truth_when_on_disk_cache_is_lost`.
    fn post_commit(&self, ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        // No actions applied → nothing to refresh.
        if ctx.merge_version.is_none() {
            return Ok(());
        }

        // BM25 inverted index refresh. Secondary KV indexes are no
        // longer refreshed here — they're committed atomically inside
        // the merge transaction by `plan`. The BM25 `InvertedIndex`
        // engine extension is non-transactional and lives in memory,
        // so it gets refreshed here in a best-effort loop. Failures
        // are logged and swallowed; `SearchSubsystem` rebuilds BM25
        // state from KV on the next `Database::open()`, so a missed
        // refresh self-heals.
        let affected = std::mem::take(&mut *self.affected.lock());
        for doc in &affected {
            let r = match &doc.new_value {
                Some(v) => {
                    JsonStore::index_json_doc(ctx.db, &ctx.target_id, &doc.space, &doc.doc_id, v)
                }
                None => {
                    JsonStore::deindex_json_doc(ctx.db, &ctx.target_id, &doc.space, &doc.doc_id)
                }
            };
            if let Err(e) = r {
                tracing::warn!(
                    target: "strata::branch_ops",
                    doc_id = %doc.doc_id,
                    error = %e,
                    "JSON merge: failed to refresh BM25 inverted-index entry"
                );
            }
        }

        Ok(())
    }
}

/// One per-doc tracked entry: `(space, doc_id, old_target_value, new_value)`.
/// Used by `JsonMergeHandler::plan` to drive both the secondary index
/// `MergeAction`s emission and the BM25 affected-list. Defined as a
/// type alias rather than a named struct to keep `merge_one_doc`'s
/// signature compact and to make explicit that this is a transient
/// in-pass datum, not part of the public API.
type JsonPerDocEntry = (String, String, Option<JsonValue>, Option<JsonValue>);

/// Merge one document's three-way state into actions / conflicts /
/// per-doc tracking list. Splitting this out keeps
/// `JsonMergeHandler::plan` readable.
#[allow(clippy::too_many_arguments)]
fn merge_one_doc(
    space: &str,
    user_key: &[u8],
    doc_id: Option<&str>,
    ancestor_val: Option<&Value>,
    source_val: Option<&Value>,
    target_val: Option<&Value>,
    strategy: MergeStrategy,
    actions: &mut Vec<MergeAction>,
    conflicts: &mut Vec<ConflictEntry>,
    per_doc: &mut Vec<JsonPerDocEntry>,
) {
    // Decode known-decodable sides into JsonDocs. Sides that fail to
    // decode (corrupt bytes) are treated as opaque and routed through
    // the byte-equality fallback below.
    let ancestor_doc = ancestor_val.and_then(JsonMergeHandler::decode_doc);
    let source_doc = source_val.and_then(JsonMergeHandler::decode_doc);
    let target_doc = target_val.and_then(JsonMergeHandler::decode_doc);

    // Helper closures
    let push_put = |actions: &mut Vec<MergeAction>, value: Value| {
        actions.push(MergeAction {
            space: space.to_string(),
            raw_key: user_key.to_vec(),
            type_tag: TypeTag::Json,
            action: MergeActionKind::Put(value),
            expected_target: target_val.cloned(),
        });
    };
    let push_delete = |actions: &mut Vec<MergeAction>| {
        actions.push(MergeAction {
            space: space.to_string(),
            raw_key: user_key.to_vec(),
            type_tag: TypeTag::Json,
            action: MergeActionKind::Delete,
            expected_target: target_val.cloned(),
        });
    };
    let push_conflict = |conflicts: &mut Vec<ConflictEntry>,
                         source_value: Option<&Value>,
                         target_value: Option<&Value>| {
        conflicts.push(ConflictEntry {
            key: format_user_key(user_key),
            primitive: PrimitiveType::Json,
            space: space.to_string(),
            source_value: source_value.cloned(),
            target_value: target_value.cloned(),
        });
    };
    let track_per_doc =
        |per_doc: &mut Vec<JsonPerDocEntry>, old: Option<&JsonDoc>, new: Option<&JsonDoc>| {
            if let Some(id) = doc_id {
                per_doc.push((
                    space.to_string(),
                    id.to_string(),
                    old.map(|d| d.value.clone()),
                    new.map(|d| d.value.clone()),
                ));
            }
        };

    // Doc-level cases.
    match (
        ancestor_val,
        source_val,
        target_val,
        ancestor_doc.as_ref(),
        source_doc.as_ref(),
        target_doc.as_ref(),
    ) {
        // Source absent, target absent — both already in agreement.
        (_, None, None, _, _, _) => {}

        // Source absent (deleted or never had it). Both ancestor and
        // target decoded — handle the SourceDeleted / DeleteModify cases
        // with full doc tracking.
        (Some(_), None, Some(_), Some(_), _, Some(tgt)) => {
            if values_equal(ancestor_val, target_val) {
                // SourceDeleted: target unchanged from ancestor → apply
                // delete to target.
                push_delete(actions);
                track_per_doc(per_doc, Some(tgt), None);
            } else {
                // DeleteModifyConflict: source deleted, target modified.
                push_conflict(conflicts, None, target_val);
                if strategy == MergeStrategy::LastWriterWins {
                    push_delete(actions);
                    track_per_doc(per_doc, Some(tgt), None);
                }
            }
        }
        (None, None, Some(_), _, _, _) => {
            // Source absent, target added (no ancestor) → no action.
        }
        (Some(_), None, Some(_), _, _, _) => {
            // Ancestor or target failed to decode; fall back to opaque
            // byte equality. Index refresh is skipped when the target
            // doc can't be decoded — corrupt rows can't index correctly
            // anyway and the next full recovery cleans them up.
            if values_equal(ancestor_val, target_val) {
                push_delete(actions);
                if let Some(tgt) = target_doc.as_ref() {
                    track_per_doc(per_doc, Some(tgt), None);
                }
            } else {
                push_conflict(conflicts, None, target_val);
                if strategy == MergeStrategy::LastWriterWins {
                    push_delete(actions);
                    if let Some(tgt) = target_doc.as_ref() {
                        track_per_doc(per_doc, Some(tgt), None);
                    }
                }
            }
        }

        // Target absent.
        (_, Some(_), None, _, Some(src), _) => {
            if values_equal(ancestor_val, source_val) {
                // TargetDeleted: source unchanged from ancestor → no action.
            } else if ancestor_val.is_none() {
                // SourceAdded: doc didn't exist on either side, source created it.
                let value = source_val.unwrap().clone();
                push_put(actions, value);
                track_per_doc(per_doc, None, Some(src));
            } else {
                // ModifyDeleteConflict: source modified, target deleted.
                push_conflict(conflicts, source_val, None);
                if strategy == MergeStrategy::LastWriterWins {
                    push_put(actions, source_val.unwrap().clone());
                    track_per_doc(per_doc, None, Some(src));
                }
            }
        }
        (_, Some(_), None, _, None, _) => {
            // Source decode failed; opaque-equality fallback.
            if values_equal(ancestor_val, source_val) {
                // no-op
            } else if ancestor_val.is_none() {
                push_put(actions, source_val.unwrap().clone());
            } else {
                push_conflict(conflicts, source_val, None);
                if strategy == MergeStrategy::LastWriterWins {
                    push_put(actions, source_val.unwrap().clone());
                }
            }
        }

        // Both source and target present.
        (_, Some(_), Some(_), _, Some(src), Some(tgt)) => {
            // Trivial: bytes are identical → no action.
            if values_equal(source_val, target_val) {
                return;
            }
            // Single-sided: source unchanged from ancestor → target's
            // value already wins, no action.
            if values_equal(source_val, ancestor_val) {
                return;
            }
            // Single-sided: target unchanged from ancestor → apply
            // source's value verbatim. Path-level merge would degenerate
            // to the same result; skip the cost.
            if values_equal(target_val, ancestor_val) {
                push_put(actions, source_val.unwrap().clone());
                track_per_doc(per_doc, Some(tgt), Some(src));
                return;
            }

            // True three-way divergence — run the path-level merge.
            let merge = merge_json_values(
                ancestor_doc.as_ref().map(|d| &d.value),
                &src.value,
                &tgt.value,
                strategy,
            );

            // `merge_json_values` always returns `Some` when both source
            // and target are present (which is the precondition for this
            // arm), so unwrapping is safe. The recursive worker only
            // returns `None` when both sides are absent at a sub-path.
            let merged_value = merge
                .merged
                .expect("merge_json_values returns Some when both sides are present");

            // Surface path-level conflicts. Each conflict path is
            // reported as its own ConflictEntry under the doc's user_key
            // so callers see one row per conflicting path within the doc.
            if !merge.conflict_paths.is_empty() {
                for conflict_path in &merge.conflict_paths {
                    conflicts.push(ConflictEntry {
                        key: format!("{}@{}", format_user_key(user_key), conflict_path),
                        primitive: PrimitiveType::Json,
                        space: space.to_string(),
                        source_value: source_val.cloned(),
                        target_value: target_val.cloned(),
                    });
                }
                if strategy != MergeStrategy::LastWriterWins {
                    // Strict: do not write a put. The caller (merge_branches)
                    // sees the conflicts and aborts before applying actions.
                    return;
                }
            }

            // Re-envelope the merged value into a JsonDoc. Identity
            // (`id`, `created_at`) inherits from the target side (the
            // merge writes back into target). `version` bumps to
            // `max(source, target) + 1`; `updated_at` is the latest of
            // the two sides. The 14-case fallback used to produce a
            // version-with-source-bytes envelope, which had the same
            // staleness; this is strictly more correct.
            let merged_doc = JsonDoc {
                id: tgt.id.clone(),
                value: merged_value,
                version: src.version.max(tgt.version) + 1,
                created_at: tgt.created_at.min(src.created_at),
                updated_at: src.updated_at.max(tgt.updated_at),
            };
            let serialized = match JsonStore::serialize_doc(&merged_doc) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::branch_ops",
                        error = %e,
                        space = %space,
                        "JSON merge: failed to serialize merged doc — falling back to source bytes"
                    );
                    push_put(actions, source_val.unwrap().clone());
                    track_per_doc(per_doc, Some(tgt), Some(src));
                    return;
                }
            };
            push_put(actions, serialized);
            // Track the affected doc so `JsonMergeHandler::plan` can
            // emit secondary index actions for the actual merged value
            // (not src.value, which only matches in the LWW degenerate
            // case) and refresh the BM25 index in `post_commit`. Skip
            // when doc_id isn't valid UTF-8 — JSON keys should always
            // be UTF-8 (`Key::new_json` only accepts `&str`), so this
            // branch only fires for corrupt rows.
            if let Some(id) = doc_id {
                per_doc.push((
                    space.to_string(),
                    id.to_string(),
                    Some(tgt.value.clone()),
                    Some(merged_doc.value.clone()),
                ));
            }
        }

        // Both present but at least one fails to decode → opaque
        // 14-case-equivalent fallback.
        (_, Some(_), Some(_), _, _, _) => {
            if values_equal(source_val, target_val) {
                return;
            }
            if values_equal(source_val, ancestor_val) {
                return;
            }
            if values_equal(target_val, ancestor_val) {
                push_put(actions, source_val.unwrap().clone());
                return;
            }
            // True conflict.
            push_conflict(conflicts, source_val, target_val);
            if strategy == MergeStrategy::LastWriterWins {
                push_put(actions, source_val.unwrap().clone());
            }
        }
    }
}

fn values_equal(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

/// Event merge handler. Runs the Event hash-chain divergence check.
///
/// Iterates the per-`(space, TypeTag::Event)` cells in `ctx.typed_entries`
/// and calls `super::check_event_merge_divergence` for each. The check
/// itself lives in `branch_ops/mod.rs` as a free function for historical
/// reasons; both `merge_branches` and `cherry_pick_from_diff` reach it
/// through this handler's `precheck`.
pub(crate) struct EventMergeHandler;

impl PrimitiveMergeHandler for EventMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Event
    }

    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        for ((space, type_tag), cell) in &ctx.typed_entries.cells {
            if *type_tag != TypeTag::Event {
                continue;
            }
            check_event_merge_divergence(space, &cell.ancestor, &cell.source, &cell.target)?;
        }
        Ok(())
    }

    /// Event BM25 convergence contract (B5.3):
    ///
    /// - ordinary writes: `ConvergenceClass::StagedPublish` via inline
    ///   `InvertedIndex` update inside `EventLog::append` and the
    ///   `SearchRefreshHook` staged-publish path on followers.
    /// - merge-time: **explicitly `ConvergenceClass::ReopenHealed`**.
    ///   Merge apply writes event rows via raw `txn.put()`, which
    ///   bypasses the inline indexing path in `EventLog::append`, so
    ///   the in-memory BM25 projection for merged events lags until
    ///   `SearchSubsystem::recover` → `reconcile_index` reruns on the
    ///   next database open. See
    ///   `branching_convergence_differential.rs::event_merge_bm25_reopen_heals_stale_index`.
    ///
    /// Hash-chain divergence is caught earlier by `precheck`; once
    /// the precheck passes, `post_commit` has no event-specific work
    /// beyond the shared reopen-heal contract.
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
}

/// Function pointer type for the vector merge precheck.
///
/// The vector crate provides this implementation and registers it via
/// `db.merge_registry().register_vector()` during `VectorSubsystem::initialize()`.
/// Called from `VectorMergeHandler::precheck` to reject merges that would
/// combine collections with incompatible configurations (dimension or metric
/// mismatch). The check requires decoding `CollectionRecord` rows, which
/// lives in the vector crate, so the engine dispatches to it via this
/// callback rather than rolling its own decoder.
///
/// Returns `Err` to abort the entire merge (no writes happen).
pub type VectorMergePrecheckFn =
    fn(db: &Arc<Database>, source: BranchId, target: BranchId) -> StrataResult<()>;

/// Function pointer type for the vector merge post-commit rebuild.
///
/// The vector crate provides this implementation and registers it via
/// `db.merge_registry().register_vector()` during `VectorSubsystem::initialize()`.
/// Called from `VectorMergeHandler::post_commit` once per merge with the set
/// of `(space, collection)` pairs that the merge actually touched on either
/// side. The vector crate rebuilds only those collections' HNSW backends,
/// leaving untouched collections alone.
///
/// Per-collection failures inside the callback are typically logged and
/// swallowed by the implementation: at this point the merge has already
/// committed to KV, so propagating an error would leave a successful
/// KV merge looking like a failure to the caller. The fallback for any
/// collection that fails to rebuild is the next full recovery on db
/// open (`recover_vector_state`), which scans every collection in the
/// branch from KV.
pub type VectorMergePostCommitFn = fn(
    db: &Arc<Database>,
    source: BranchId,
    target: BranchId,
    affected: &BTreeSet<(String, String)>,
) -> StrataResult<()>;

/// Vector merge handler.
///
/// Tracks the set of `(space, collection)` pairs touched on either side
/// of the merge in `plan`, then dispatches to the registered vector-crate
/// callbacks in `precheck` (dimension/metric mismatch detection) and
/// `post_commit` (per-collection HNSW rebuild). The plan itself uses the
/// trait default (KV-shaped 14-case classification) — vectors are stored
/// as KV entries under `TypeTag::Vector`, so the generic classifier
/// produces correct puts and deletes.
///
/// Each merge gets a fresh handler instance via `build_merge_registry()`,
/// so the `affected` mutex is uncontended in practice — it exists only
/// because the trait methods take `&self`.
pub(crate) struct VectorMergeHandler {
    /// Set of `(space, collection_name)` pairs whose vector cells were
    /// touched on either source or target since the merge base.
    /// Populated by `plan`, drained by `post_commit`.
    affected: Mutex<BTreeSet<(String, String)>>,
}

impl VectorMergeHandler {
    pub(crate) fn new() -> Self {
        Self {
            affected: Mutex::new(BTreeSet::new()),
        }
    }

    /// Extract the collection name from a vector KV user_key.
    ///
    /// Vector keys come in two shapes:
    /// - `__config__/{collection}` for collection config
    /// - `{collection}/{vector_key}` for vector data
    ///
    /// Returns `None` for malformed keys (e.g. user_key with no `/`).
    fn collection_from_user_key(user_key: &[u8]) -> Option<String> {
        let s = std::str::from_utf8(user_key).ok()?;
        if let Some(rest) = s.strip_prefix("__config__/") {
            return Some(rest.to_string());
        }
        let (collection, _) = s.split_once('/')?;
        if collection.is_empty() {
            return None;
        }
        Some(collection.to_string())
    }
}

impl PrimitiveMergeHandler for VectorMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Vector
    }

    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        // Dimension/metric mismatch detection lives in the vector crate
        // (it requires decoding CollectionRecord). If no callback is
        // registered, fall through — the engine has nothing to validate
        // on its own.
        //
        // Per-database registry is the canonical path. No global fallback.
        if let Some(callbacks) = ctx.db.merge_registry().vector_callbacks() {
            (callbacks.precheck)(ctx.db, ctx.source_id, ctx.target_id)?;
        }
        // No callback registered → no validation. Vector data is still
        // KV-correct (the generic classifier writes it), but dimension/metric
        // mismatches won't be caught until the next full recovery.
        Ok(())
    }

    fn plan(&self, ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
        // Run the default 14-case classification first — vectors are
        // KV-shaped at the cell level, so the generic classifier
        // produces the right puts and deletes.
        let (actions, conflicts) =
            classify_typed_entries_for_tag(ctx.typed_entries, ctx.strategy, TypeTag::Vector);

        // Track which (space, collection) pairs were actually mutated by
        // the merge. We walk the produced `actions` (puts and deletes)
        // rather than the raw cell slices because cell.source/target
        // include COW-inherited rows from ancestor — so a collection
        // that no side modified would otherwise be flagged as "affected"
        // and trigger a needless rebuild on every merge.
        //
        // Walking actions also correctly catches deletes (via
        // MergeActionKind::Delete) so wholesale-collection-delete
        // scenarios drop the in-memory backend in post_commit.
        let mut affected = self.affected.lock();
        for action in &actions {
            if action.type_tag != TypeTag::Vector {
                continue;
            }
            if let Some(collection) = Self::collection_from_user_key(&action.raw_key) {
                affected.insert((action.space.clone(), collection));
            }
        }

        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    /// Vector HNSW convergence contract (B5.3):
    ///
    /// - ordinary writes: `ConvergenceClass::StagedPublish` via
    ///   `VectorCommitObserver` (staged backend ops applied
    ///   post-commit) and `VectorLifecycleHook` on followers.
    /// - merge-time: **immediate refresh** via the registered
    ///   per-database callback below. The callback walks only the
    ///   `(space, collection)` pairs actually touched by merge
    ///   actions and re-runs `rebuild_collection_after_merge` for
    ///   each. Untouched collections are left alone.
    /// - unregistered callback (engine-only unit tests): falls back
    ///   to `ConvergenceClass::ReopenHealed` — vectors are still
    ///   KV-correct, but the in-memory HNSW backends rebuild on the
    ///   next `Database::open` via `recover_vector_state`. Production
    ///   subsystem composition always registers the callback; this
    ///   fallback is test-infrastructure-only. See
    ///   `branching_convergence_differential.rs::vector_merge_hnsw_refreshed_immediately_no_reopen_needed`.
    fn post_commit(&self, ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        // If no merge happened (no actions applied), there's nothing to
        // rebuild. The merge_branches caller signals this via merge_version
        // being None.
        if ctx.merge_version.is_none() {
            return Ok(());
        }

        let affected = std::mem::take(&mut *self.affected.lock());
        if affected.is_empty() {
            return Ok(());
        }

        // Per-database registry is the canonical path. No global fallback.
        if let Some(callbacks) = ctx.db.merge_registry().vector_callbacks() {
            (callbacks.post_commit)(ctx.db, ctx.source_id, ctx.target_id, &affected)?;
        }
        // No callback registered → no HNSW rebuild fires here. Vectors are
        // still KV-correct (the generic classifier wrote them) but the
        // in-memory HNSW backends are not refreshed until the next full
        // recovery on db open. Engine-only unit tests are the typical
        // unregistered case.
        Ok(())
    }
}

/// Function pointer type for the graph semantic merge plan.
///
/// The graph crate provides this implementation and registers it via
/// `db.merge_registry().register_graph(graph_plan_fn)` during
/// `GraphSubsystem::initialize()`. The engine's `GraphMergeHandler::plan`
/// method dispatches to it if registered, falling back to divergence
/// refusal + default classify behavior if not.
///
/// Returns the per-handler `PrimitiveMergePlan` shape directly so the
/// graph crate doesn't need access to engine-internal types beyond what's
/// already `pub` (`MergeAction`, `ConflictEntry`).
///
/// The function may return `Err` if decoding the cell's adjacency lists
/// fails (e.g., corrupt packed binary on disk).
pub type GraphMergePlanFn = fn(&MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan>;

/// Graph merge handler.
///
/// Dispatches to the graph crate's semantic merge — decoded edge diffing,
/// additive merging of disjoint edges, referential integrity validation,
/// additive catalog merging — registered via the per-database
/// `MergeHandlerRegistry`.
///
/// If no graph plan function is registered (typical for engine-only unit
/// tests that don't load the graph crate), the handler falls back to:
/// - `precheck`: refuses any merge where both source and target made
///   graph modifications since the merge base
/// - `plan`: the default 14-case classification
///
/// Both `merge_branches` and `cherry_pick_from_diff` go through this
/// handler via the per-handler `plan` dispatch, so cherry-pick inherits
/// the same semantic merge as merge.
pub(crate) struct GraphMergeHandler;

impl PrimitiveMergeHandler for GraphMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Graph
    }

    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        // If a semantic merge is registered in the per-database registry,
        // validation lives inside `plan`. Otherwise, fall back to the
        // divergence refusal so divergent graph merges still get caught.
        //
        // Per-database registry is the canonical path. No global fallback.
        if ctx.db.merge_registry().has_graph() {
            return Ok(());
        }
        for ((space, type_tag), cell) in &ctx.typed_entries.cells {
            if *type_tag != TypeTag::Graph {
                continue;
            }
            check_graph_merge_divergence(space, &cell.ancestor, &cell.source, &cell.target)?;
        }
        Ok(())
    }

    fn plan(&self, ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
        // Per-database registry is the canonical path. No global fallback.
        if let Some(plan_fn) = ctx.db.merge_registry().graph_plan_fn() {
            return plan_fn(ctx);
        }
        // No semantic merge registered → use the default classify path.
        // The divergence refusal already fired in `precheck` above for
        // divergent merges; this branch only runs for safe (single-sided
        // or empty) graph merges, where the default classify is correct.
        let (actions, conflicts) =
            classify_typed_entries_for_tag(ctx.typed_entries, ctx.strategy, TypeTag::Graph);
        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    /// Graph convergence contract (B5.3):
    ///
    /// - packed adjacency rows (fwd/rev lists): `ConvergenceClass::StorageCoupled`.
    ///   The semantic merge in `plan` emits `MergeAction`s that
    ///   commit atomically inside the merge transaction, and
    ///   `AdjacencyIndex` is built on demand from those rows — no
    ///   separate cache to refresh.
    /// - graph-node BM25 search projection: **explicitly
    ///   `ConvergenceClass::ReopenHealed`**. Merge apply writes graph
    ///   rows via raw `txn.put()`, which bypasses the inline
    ///   `InvertedIndex` update path that `GraphStore::put` performs
    ///   for ordinary writes. The in-memory BM25 projection for
    ///   merged graph nodes lags until `SearchSubsystem::recover` →
    ///   `reconcile_index` reruns on the next database open. See
    ///   `branching_convergence_differential.rs::graph_merge_bm25_reopen_heals_stale_index`.
    /// - `BranchStatusCache` is `ConvergenceClass::AdvisoryOnly` and
    ///   is not touched by merge (see `GraphSubsystem::cleanup_deleted_branch`
    ///   for its delete-time cleanup).
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        // Nothing to refresh post-commit. The packed adjacency lists ARE
        // the storage; there's no in-memory cache to rebuild
        // (`AdjacencyIndex` is built on demand). The semantic merge in
        // `plan` already wrote the projected fwd/rev lists with
        // bidirectional consistency.
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::super::DATA_TYPE_TAGS;
    use super::*;

    /// Pin the registry contract: every `TypeTag` in `DATA_TYPE_TAGS` must
    /// have a registered handler whose `type_tag()` matches its key.
    #[test]
    fn registry_contains_one_handler_per_data_type_tag() {
        let registry = build_merge_registry();
        for &tag in &DATA_TYPE_TAGS {
            let handler = registry.get(tag);
            assert_eq!(
                handler.type_tag(),
                tag,
                "registry handler for {:?} reports wrong type_tag()",
                tag
            );
        }
        // Also assert no extras: the registry should hold exactly the five
        // tags from DATA_TYPE_TAGS, no more.
        assert_eq!(
            registry.handlers.len(),
            DATA_TYPE_TAGS.len(),
            "registry should hold exactly {} handlers (one per DATA_TYPE_TAGS), got {}",
            DATA_TYPE_TAGS.len(),
            registry.handlers.len()
        );
    }
}
