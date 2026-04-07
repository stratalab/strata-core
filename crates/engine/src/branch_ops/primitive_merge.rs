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
//! - `JsonMergeHandler` is a no-op today. Per-key inverted-index refresh
//!   in `post_commit` and path-level conflict detection in `precheck`
//!   are TODO; the current generic merge path leaves JSON indexes stale
//!   post-merge.
//! - `EventMergeHandler::precheck` runs the Event hash-chain divergence
//!   check via `super::check_event_merge_divergence`. The check itself
//!   lives in `branch_ops/mod.rs` as a free function for historical
//!   reasons; both `merge_branches` and `cherry_pick_from_diff` reach it
//!   through this handler.
//! - `VectorMergeHandler` tracks affected `(space, collection)` pairs in
//!   `plan` and dispatches to function pointers registered by the vector
//!   crate (`register_vector_merge`) for the dimension/metric mismatch
//!   precheck and the per-collection HNSW rebuild in `post_commit`. If
//!   unset (engine-only unit tests that don't load the vector crate),
//!   the handler is a pure pass-through and HNSW backends only catch up
//!   to the merged KV state on the next full recovery.
//! - `GraphMergeHandler::plan` dispatches to a function pointer registered
//!   by the graph crate (`register_graph_merge_plan`), which implements
//!   the semantic merge algorithm: decoded edge diffing, additive merging
//!   of disjoint edges, referential integrity validation, additive catalog
//!   merging. If unset (engine-only unit tests that don't load the graph
//!   crate), the handler falls back to `check_graph_merge_divergence` —
//!   the tactical "refuse divergent graph merges" rule.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use strata_core::types::{BranchId, TypeTag};
use strata_core::StrataResult;

use super::{
    check_event_merge_divergence, check_graph_merge_divergence, classify_typed_entries_for_tag,
    ConflictEntry, MergeAction, MergeBase, MergeStrategy, TypedEntries,
};
use crate::database::Database;

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
/// KV / JSON / Vector / Event handlers use the default trait
/// implementation, which delegates to `classify_typed_entries_for_tag`
/// (the same 14-case decision matrix the generic merge runs).
/// `GraphMergeHandler::plan` overrides the default to dispatch to the
/// graph crate's semantic merge algorithm.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates registering
/// graph plan callbacks can borrow it through the function-pointer
/// signature.
#[allow(dead_code)] // contract for future handlers
pub struct MergePlanCtx<'a> {
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
        Arc::new(JsonMergeHandler),
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
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
}

/// JSON merge handler. Pure no-op pass-through.
///
/// Per-key inverted-index refresh in `post_commit` and path-level
/// conflict detection in `precheck` are TODO. The current generic merge
/// path leaves JSON indexes stale post-merge — that's a real bug but
/// fixing it requires a `JsonRefreshHook` which doesn't exist yet.
pub(crate) struct JsonMergeHandler;

impl PrimitiveMergeHandler for JsonMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Json
    }
    fn precheck(&self, _ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
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

    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
}

/// Function pointer type for the vector merge precheck.
///
/// The vector crate provides this implementation via
/// `register_vector_merge`. Called from `VectorMergeHandler::precheck` to
/// reject merges that would combine collections with incompatible
/// configurations (dimension or metric mismatch). The check requires
/// decoding `CollectionRecord` rows, which lives in the vector crate, so
/// the engine dispatches to it via this callback rather than rolling its
/// own decoder.
///
/// Returns `Err` to abort the entire merge (no writes happen).
pub type VectorMergePrecheckFn =
    fn(db: &Arc<Database>, source: BranchId, target: BranchId) -> StrataResult<()>;

/// Function pointer type for the vector merge post-commit rebuild.
///
/// The vector crate provides this implementation via
/// `register_vector_merge`. Called from `VectorMergeHandler::post_commit`
/// once per merge with the set of `(space, collection)` pairs that the
/// merge actually touched on either side. The vector crate rebuilds only
/// those collections' HNSW backends, leaving untouched collections alone.
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

/// Bundle of vector-merge callbacks registered by the vector crate.
struct VectorMergeCallbacks {
    precheck: VectorMergePrecheckFn,
    post_commit: VectorMergePostCommitFn,
}

/// Registered vector semantic merge implementation, set by the vector
/// crate at startup via `register_vector_merge`. If unset (engine-only
/// unit tests that don't load the vector crate), `VectorMergeHandler` is
/// a pure pass-through: precheck and post_commit are no-ops, vectors
/// merge through the generic 14-case classifier (which writes correct
/// KV) but no in-memory HNSW rebuild fires. The next full recovery on
/// db open will catch up via `recover_vector_state`.
static VECTOR_MERGE_CALLBACKS: OnceCell<VectorMergeCallbacks> = OnceCell::new();

/// Register the vector crate's semantic merge implementation.
///
/// Should be called once at application/test startup, before any
/// `merge_branches` calls. Subsequent calls are no-ops (the first
/// registration wins).
///
/// The standard test fixtures call this from `ensure_recovery_registered`
/// alongside the existing `register_vector_recovery` /
/// `register_search_recovery` / `register_graph_semantic_merge` hooks.
pub fn register_vector_merge(
    precheck: VectorMergePrecheckFn,
    post_commit: VectorMergePostCommitFn,
) {
    let _ = VECTOR_MERGE_CALLBACKS.set(VectorMergeCallbacks {
        precheck,
        post_commit,
    });
}

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
        if let Some(callbacks) = VECTOR_MERGE_CALLBACKS.get() {
            (callbacks.precheck)(ctx.db, ctx.source_id, ctx.target_id)?;
        }
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

        if let Some(callbacks) = VECTOR_MERGE_CALLBACKS.get() {
            (callbacks.post_commit)(ctx.db, ctx.source_id, ctx.target_id, &affected)?;
        }
        // No callback → no rebuild fires here. Vectors are still
        // KV-correct (the generic classifier wrote them) but the
        // in-memory HNSW backends are not refreshed until the next full
        // recovery on db open. Engine-only unit tests are the typical
        // unregistered case.
        Ok(())
    }
}

/// Function pointer type for the graph semantic merge plan.
///
/// The graph crate provides this implementation via
/// `register_graph_merge_plan`. The engine's `GraphMergeHandler::plan`
/// method dispatches to it if registered, falling back to the divergence
/// refusal + default classify behavior if not.
///
/// Returns the per-handler `PrimitiveMergePlan` shape directly so the
/// graph crate doesn't need access to engine-internal types beyond what's
/// already `pub` (`MergeAction`, `ConflictEntry`).
///
/// The function may return `Err` if decoding the cell's adjacency lists
/// fails (e.g., corrupt packed binary on disk).
pub type GraphMergePlanFn = fn(&MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan>;

/// Registered graph semantic merge implementation, set by the graph
/// crate at startup via `register_graph_merge_plan`. If unset (e.g.
/// in unit tests that don't load the graph crate), `GraphMergeHandler`
/// falls back to the divergence-refusal precheck + default classify
/// behavior.
static GRAPH_MERGE_PLAN_FN: OnceCell<GraphMergePlanFn> = OnceCell::new();

/// Register the graph crate's semantic merge implementation.
///
/// Should be called once at application/test startup, before any
/// `merge_branches` calls. Subsequent calls are no-ops (the first
/// registration wins).
///
/// The standard test fixtures call this from `ensure_recovery_registered`
/// alongside the existing `register_vector_recovery` / `register_search_recovery`
/// hooks.
pub fn register_graph_merge_plan(plan_fn: GraphMergePlanFn) {
    let _ = GRAPH_MERGE_PLAN_FN.set(plan_fn);
}

/// Graph merge handler.
///
/// Dispatches to the graph crate's semantic merge — decoded edge diffing,
/// additive merging of disjoint edges, referential integrity validation,
/// additive catalog merging — registered via `register_graph_merge_plan`.
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
        // If a semantic merge is registered, validation lives inside `plan`.
        // Otherwise, fall back to the divergence refusal so divergent graph
        // merges still get caught.
        if GRAPH_MERGE_PLAN_FN.get().is_some() {
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
        if let Some(plan_fn) = GRAPH_MERGE_PLAN_FN.get() {
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
