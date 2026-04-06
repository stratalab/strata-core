//! Per-primitive merge dispatch (Phase 2 of primitive-aware merge).
//!
//! See `docs/design/branching/primitive-aware-merge.md`.
//!
//! ## Role
//!
//! `merge_branches` previously routed every primitive type tag through one
//! identical classification + apply pipeline. That was correct for KV but
//! wrong for primitives with higher-level invariants (Event hash chains,
//! Graph adjacency, JSON document structure, Vector index state). Phase 1
//! (PR #2330) added a tactical safety check for the most acute case (Event
//! divergence). Phase 2 (this module) introduces the dispatch architecture
//! that Phases 3–6 will fill in with real per-primitive semantic merge.
//!
//! ## Phase 2 scope
//!
//! Phase 2 is **architectural plumbing only** — no new user-visible behavior:
//!
//! - Defines the `PrimitiveMergeHandler` trait (`precheck` + `post_commit`).
//!   The eventual `plan` method is deferred to Phase 3+ where individual
//!   handlers will produce per-primitive write plans (see the "Phased trait
//!   surface" callout in the design doc).
//! - Provides a `MergeHandlerRegistry` with one handler per `TypeTag`.
//! - Migrates the Phase 1 Event divergence check from an inline check inside
//!   `three_way_diff` into `EventMergeHandler::precheck`. The check itself
//!   is still implemented by `super::check_event_merge_divergence`; the
//!   handler is a thin dispatch shim.
//! - All other handlers (KV, JSON, Vector, Graph) are pure no-ops in Phase 2.
//!   Their semantic improvements land in their own dedicated phases.

use std::collections::BTreeMap;
use std::sync::Arc;

use once_cell::sync::OnceCell;
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
/// Several fields are unused in Phase 2 (only `EventMergeHandler` reads
/// `typed_entries`), but they form the documented contract that Phase 3+
/// handlers will rely on for richer per-primitive validation.
#[allow(dead_code)] // see Phase 2 doc note above
pub(crate) struct MergePrecheckCtx<'a> {
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
/// Phase 3b introduces this method on the trait alongside the
/// `GraphMergeHandler::plan` semantic merge implementation. KV / JSON /
/// Vector / Event handlers use the default trait implementation, which
/// delegates to `classify_typed_entries_for_tag` for backward-compat with
/// the Phase 2 single-classify behavior.
///
/// `pub` (re-exported from `strata_engine`) so primitive crates registering
/// graph plan callbacks can borrow it through the function-pointer
/// signature.
#[allow(dead_code)] // forward-looking fields for Phase 4+ handlers
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
/// Runs after the merge transaction has successfully committed. Phase 2
/// handlers do nothing here; Phases 3 and 5 will use this hook to refresh
/// graph adjacency and JSON inverted-index entries respectively, so the
/// fields are part of the documented contract even though no Phase 2
/// handler reads them yet.
#[allow(dead_code)] // see Phase 2 doc note above
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
///
/// Phase 2 introduced the dispatch with `precheck` + `post_commit`. Phase 3b
/// added `plan` with a default impl that preserves Phase 2 behavior for
/// KV/JSON/Vector/Event handlers (delegating to `classify_typed_entries_for_tag`).
/// `GraphMergeHandler::plan` overrides the default with the real semantic
/// graph merge.
pub(crate) trait PrimitiveMergeHandler: Send + Sync {
    /// The primitive this handler owns.
    fn type_tag(&self) -> TypeTag;

    /// Validate that the merge is allowed for this primitive.
    ///
    /// Called once per merge, before any classification or write happens.
    /// Returning `Err` aborts the entire merge cleanly with no side
    /// effects. Phase 1 (Event) and Phase 3 (Graph) used this for tactical
    /// refusal of unsafe merges. Phase 3b's semantic graph merge moves
    /// validation into `plan`, so `GraphMergeHandler::precheck` becomes a
    /// no-op while the Event check still lives here.
    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()>;

    /// Produce the per-primitive write plan.
    ///
    /// Default implementation delegates to `classify_typed_entries_for_tag`
    /// — the same 14-case decision matrix that ran globally before Phase 3b.
    /// Handlers that need primitive-aware merge logic (like Graph, which
    /// needs to decode packed adjacency lists and validate referential
    /// integrity) override this with their own implementation.
    ///
    /// The default impl makes Phase 3b a strict superset of Phase 2 for
    /// non-graph primitives: KV, JSON, Vector, and Event keep their exact
    /// pre-Phase-3b classification behavior with zero overhead.
    fn plan(&self, ctx: &MergePlanCtx<'_>) -> StrataResult<PrimitiveMergePlan> {
        let (actions, conflicts) =
            classify_typed_entries_for_tag(ctx.typed_entries, ctx.strategy, self.type_tag());
        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    /// Apply post-commit fix-ups to secondary state.
    ///
    /// Called after the merge transaction commits successfully. Must be
    /// idempotent — a retry after crash must converge to the same state.
    /// All Phase 2 handlers are no-ops here. Phase 3 (Graph adjacency) and
    /// Phase 5 (JSON per-key index refresh) will fill this in.
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
        Arc::new(VectorMergeHandler),
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

/// KV merge handler. Phase 2: pure no-op pass-through.
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

/// JSON merge handler. Phase 2: pure no-op pass-through.
///
/// Phase 5 will add per-key inverted-index refresh in `post_commit` and
/// path-level conflict detection in `precheck`. The current generic merge
/// path leaves JSON indexes stale post-merge — that's a real bug but
/// fixing it requires a `JsonRefreshHook` which belongs in Phase 5.
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

/// Event merge handler. Phase 2: runs the Phase 1 divergence check.
///
/// This is the only Phase 2 handler with non-trivial behavior. It iterates
/// the per-`(space, TypeTag::Event)` cells in `ctx.typed_entries` and calls
/// `super::check_event_merge_divergence` for each, preserving the Phase 1
/// safety contract. The check itself still lives in `branch_ops/mod.rs` as
/// a free function so `cherry_pick_from_diff` can call it directly without
/// going through the registry.
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

/// Vector merge handler. Phase 2: pure no-op pass-through.
///
/// Vector merges are actually safe today: `VectorRefreshHook::post_merge_reload`
/// (in `crates/engine/src/recovery.rs`) does a full HNSW rebuild from KV
/// state on every merge, so disjoint and conflicting vector merges both
/// produce correct HNSW after the rebuild. There is no silent corruption
/// to refuse. Phase 4 will make the rebuild incremental.
pub(crate) struct VectorMergeHandler;

impl PrimitiveMergeHandler for VectorMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Vector
    }
    fn precheck(&self, _ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        Ok(())
    }
}

/// Function pointer type for the graph semantic merge plan.
///
/// The graph crate provides this implementation via
/// `register_graph_merge_plan`. The engine's `GraphMergeHandler::plan`
/// method dispatches to it if registered, falling back to the Phase 3
/// tactical refusal + default classify behavior if not.
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
/// falls back to Phase 3's tactical refusal + the default classify
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
/// Phase 3b ships a real semantic graph merge in the graph crate that
/// decodes packed adjacency lists, validates referential integrity, and
/// re-encodes the projected state. The graph crate registers its
/// implementation via `register_graph_merge_plan`. This handler dispatches
/// to it when `plan` is called.
///
/// If no graph plan function is registered (typical for engine-only unit
/// tests that don't load the graph crate), the handler falls back to:
/// - `precheck`: Phase 3 tactical refusal of divergent graph merges
/// - `plan`: the default 14-case classification (unchanged from Phase 2)
///
/// `cherry_pick_from_diff` still uses the Phase 3 tactical refusal helper
/// directly. Phase 3b.5 will refactor cherry-pick to use the semantic merge.
pub(crate) struct GraphMergeHandler;

impl PrimitiveMergeHandler for GraphMergeHandler {
    fn type_tag(&self) -> TypeTag {
        TypeTag::Graph
    }

    fn precheck(&self, ctx: &MergePrecheckCtx<'_>) -> StrataResult<()> {
        // If a semantic merge is registered, validation lives inside `plan`.
        // Otherwise, fall back to Phase 3's tactical refusal so divergent
        // graph merges still get caught.
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
        // Phase 3's tactical refusal already fired in precheck above for
        // divergent merges; this branch only runs for safe (single-sided
        // or empty) graph merges, where the default classify is correct.
        let (actions, conflicts) =
            classify_typed_entries_for_tag(ctx.typed_entries, ctx.strategy, TypeTag::Graph);
        Ok(PrimitiveMergePlan { actions, conflicts })
    }

    fn post_commit(&self, _ctx: &MergePostCommitCtx<'_>) -> StrataResult<()> {
        // Phase 3b: nothing to refresh post-commit. The packed adjacency
        // lists ARE the storage; there's no in-memory cache to rebuild
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
