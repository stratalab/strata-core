# Database Lifecycle Unification — Implementation Plan

**Date:** 2026-04-07
**Status:** Implementation plan (epics)
**Tracks design doc:** [`database-lifecycle-unification-plan.md`](./database-lifecycle-unification-plan.md)
**Issue:** [stratalab/strata-core#2354](https://github.com/stratalab/strata-core/issues/2354)

---

## Context

Strata currently has two parallel lifecycle mechanisms:

1. A global `RecoveryParticipant` registry (`crates/engine/src/recovery/participant.rs`) used for open-time recovery.
2. A per-database `Vec<Box<dyn Subsystem>>` used for freeze-on-drop.

In production these lists are sourced from different code paths, so vector state recovers correctly but never freezes on shutdown:

- `Database::open_internal` (`crates/engine/src/database/open.rs:164-239`) hardcodes `db.set_subsystems(vec![Box::new(SearchSubsystem)])` at line 236, with the same hardcoded line at 394 in `open_follower_internal`.
- `Strata::open_with` (`crates/executor/src/api/mod.rs:141-290`) calls `ensure_vector_recovery()` (lines 70-89), which registers `VectorSubsystem`'s recovery via the global participant registry — but never installs `VectorSubsystem` on the database's freeze list.
- `DatabaseBuilder::open` (`crates/engine/src/database/builder.rs:60-64`) only overwrites `subsystems` *after* `Database::open()` already ran with the hardcoded list.

The fix is to make `Subsystem` the single lifecycle contract: the same ordered list drives both recovery and freeze, composed by the executor (above the engine boundary, since `strata-vector → strata-engine` makes the reverse direction a cycle).

---

## Critical files

**Engine (lifecycle owner)**
- `crates/engine/src/database/open.rs` — primary, follower, cache open paths
- `crates/engine/src/database/builder.rs` — current builder shell
- `crates/engine/src/database/mod.rs` — `Database` struct, `set_subsystems`, `run_freeze_hooks`, `Drop`
- `crates/engine/src/database/lifecycle.rs` — `shutdown`, `freeze_vector_heaps`, `freeze_search_index`
- `crates/engine/src/recovery/subsystem.rs` — `Subsystem` trait
- `crates/engine/src/recovery/participant.rs` — to be deleted
- `crates/engine/src/recovery/mod.rs` — module exports
- `crates/engine/src/search/recovery.rs` — `SearchSubsystem`, `register_search_recovery`, `recover_search_state`

**Vector (subsystem implementor)**
- `crates/vector/src/recovery.rs` — `VectorSubsystem`, `register_vector_recovery`, `recover_vector_state`, `register_vector_refresh_hook`

**Executor (production composition)**
- `crates/executor/src/api/mod.rs` — `Strata::open`, `open_with`, `cache`, `ensure_vector_recovery`
- Follower open code paths (executor-side wrappers around `Database::open_follower`)

**Tests**
- `crates/engine/tests/recovery_tests.rs` — already uses `DatabaseBuilder` with `[VectorSubsystem, SearchSubsystem]` (lines 1246-1250, 1363-1367, 1509-1513, 1539-1543); these become the "builder regression" baseline
- `crates/executor/tests/` — needs new production-shaped restart + follower regressions

---

## Epics

### Epic 1 — Subsystem-aware engine open plumbing

**Goal:** make `Database` open paths recover from a caller-supplied subsystem list, not from a global registry, and stop hardcoding `SearchSubsystem`.

**Changes**

1. Add new internal entry points in `crates/engine/src/database/open.rs`:
   - `Database::open_internal_with_subsystems(path, mode, cfg, subsystems: Vec<Box<dyn Subsystem>>)`
   - `Database::open_follower_internal_with_subsystems(path, cfg, subsystems)`
   - Existing `open_internal` / `open_follower_internal` become thin wrappers that pass an empty list (preserving the engine-only convenience contract; they will *not* hardcode search anymore — see step 4).

2. New recovery sequence inside the `_with_subsystems` variants, replacing the current lines 217-238 / 381-395:
   - Run generic open work: `repair_space_metadata_on_open` (still skipped on followers internally).
   - Install the subsystem list on `db` via `set_subsystems(subsystems)` *before* recovery runs, so the same list drives recovery and freeze.
   - Iterate `db.subsystems` in order and call `subsystem.recover(&db)` for each. Stop on first error.
   - **Do not** call `recover_all_participants`.
   - **Do not** call `register_search_recovery` from inside open.
   - **Do not** manually `index.enable()` (move that responsibility into `SearchSubsystem::recover`, see step 5).

3. Cache path (`Database::cache`, lines 725-792): keep as-is for now — cache databases construct an empty `subsystems` vec. The builder/executor will install subsystems explicitly through a new cache-with-subsystems path in Epic 2 if needed. (Cache currently directly calls `index.enable()` at line 789; that line stays for now to avoid scope creep — it's only the disk paths that need to stop double-enabling.)

4. Update the existing engine-level `Database::open` / `open_with_config` / `open_follower` (the public engine APIs) to call the new `_with_subsystems` variants with an empty list. These remain engine-level convenience APIs as the design doc requires (§ Composition Model item 4); they no longer pretend to be production-complete.

5. In `crates/engine/src/search/recovery.rs`, make `SearchSubsystem::recover` solely responsible for enabling the inverted index. Move the defensive `index.enable()` logic from `open.rs:232-235` into `recover_search_state` (or assert it already happens via `reconcile_index` at line 92 and remove the duplicate). Confirm `recover_search_state` is idempotent.

6. Confirm `run_freeze_hooks` (`mod.rs:448-468`) iterates in reverse order — already correct, no change needed.

**Risk:** moving search-enable into the subsystem changes *where* the index becomes enabled. Mitigate by exercising the existing search-open tests in `recovery_tests.rs` after this epic.

---

### Epic 2 — DatabaseBuilder owns disk-backed open

**Goal:** make `DatabaseBuilder` the authoritative explicit-composition path. Stop the "open then overwrite" anti-pattern.

**Changes** (file: `crates/engine/src/database/builder.rs`)

1. `DatabaseBuilder::open(path)` and `open_with_config(path, cfg)` must call `Database::open_internal_with_subsystems` directly with `self.subsystems`. Remove the post-open `db.set_subsystems(self.subsystems)` override.

2. Add `DatabaseBuilder::open_follower(path)` and `open_follower_with_config(path, cfg)` that call `Database::open_follower_internal_with_subsystems`. This replaces the executor's current direct call to `Database::open_follower`.

3. Keep `DatabaseBuilder::cache(enable_search)` for now — but simplify so it relies on the same subsystem-driven recovery as disk paths. The current `run_subsystem_recovery` private method (lines 91-106) becomes redundant once disk-open uses subsystems natively; review whether to delete it after Epic 1 lands.

4. Update builder docs to accurately describe what gets installed: the same list drives both recovery and freeze.

**Result:** the builder is the only API that guarantees recovery/freeze symmetry. Engine-level `Database::open*` are documented as engine-only convenience APIs.

---

### Epic 3 — Executor production composition via builder

**Goal:** move `Strata::open`, `Strata::open_with`, `Strata::cache`, and follower open over to `DatabaseBuilder` with explicit `[VectorSubsystem, SearchSubsystem]`.

**Changes** (file: `crates/executor/src/api/mod.rs`)

1. Replace `ensure_vector_recovery()` (lines 70-89). Split it into:
   - `ensure_merge_handlers_registered()` — keeps the `Once`-guarded calls to `strata_graph::register_graph_semantic_merge()` and `strata_vector::register_vector_semantic_merge()`. **Do not** delete these — they are merge handler registrations, not recovery, and they remain global.
   - Remove `strata_vector::register_vector_recovery()` and `strata_engine::register_search_recovery()` calls entirely.

2. `Strata::open_with(path, opts)` (lines 141-290):
   - Call `ensure_merge_handlers_registered()` instead of `ensure_vector_recovery()`.
   - **Primary branch (lines 163-289):** replace the `Database::open_with_config(...)` call with:
     ```rust
     DatabaseBuilder::new()
         .with_subsystem(VectorSubsystem)
         .with_subsystem(SearchSubsystem)
         .open_with_config(&data_dir, cfg)?
     ```
   - **Follower branch (lines 146-161):** replace `Database::open_follower(&data_dir)` with the new `DatabaseBuilder::open_follower(&data_dir)` (same subsystem set).

3. `Strata::cache()` (lines 303-308): use the builder's cache path with the same `[VectorSubsystem, SearchSubsystem]` ordering.

4. Subsystem order is fixed: **`VectorSubsystem` then `SearchSubsystem`** for recovery (freeze runs in reverse). This matches the design doc § Phase 2 and the existing `recovery_tests.rs` ordering.

**Risk:** if any other crate imports `register_vector_recovery` or `register_search_recovery`, the build will break. Audit before deleting (see Epic 4).

---

### Epic 4 — Delete the legacy participant registry

**Goal:** remove the second lifecycle mechanism so the bug class cannot recur.

**Changes**

1. Delete `crates/engine/src/recovery/participant.rs` and remove its `pub mod participant;` line from `crates/engine/src/recovery/mod.rs`. Also remove any `pub use` re-exports of `RecoveryParticipant`, `register_recovery_participant`, `recover_all_participants`.

2. In `crates/engine/src/search/recovery.rs`, delete `register_search_recovery` (lines 431-433). `recover_search_state` itself is still called from `SearchSubsystem::recover`, so it stays.

3. In `crates/vector/src/recovery.rs`, delete `register_vector_recovery` (lines 468-470). `recover_vector_state` still drives `VectorSubsystem::recover`, so it stays.

4. Remove all remaining call sites of `recover_all_participants`. After Epic 1 there should be none — this step is a verification/cleanup pass.

5. Audit downstream crates and tests for any straggler imports of the deleted symbols. The `Once`-based startup init in `executor/api/mod.rs` was the primary consumer; tests in `crates/engine/tests/` mostly use the builder directly.

**Order:** must land *after* Epics 1-3 so the executor and builder no longer depend on participants. The design doc § Implementation Sequence is explicit about this.

---

### Epic 5 — Production-shaped regression tests

**Goal:** prove that drop-time freeze works *through the production code path*, not just through the builder. Today's `recovery_tests.rs` proves the builder path works — that test stays as-is, but it does not catch the production gap.

**Changes**

1. **New executor restart regression** in `crates/executor/tests/` (new file, e.g. `lifecycle_regression.rs`):
   - Open a database via `Strata::open(path)` (no builder, no manual subsystem wiring).
   - Create a vector collection, insert several embeddings.
   - Drop the `Strata` handle without calling `freeze_vector_heaps()` or `shutdown()` explicitly.
   - Reopen via `Strata::open(path)`.
   - Assert vector data is queryable (i.e. heap was frozen on drop and recovered on reopen).
   - Assert the expected `.vec` cache file exists on disk in the collection's directory.

2. **New executor follower regression** in the same file:
   - Primary opens via `Strata::open` and writes vectors.
   - Follower opens via `Strata::open_with(path, OpenOptions::default().follower(true))`.
   - Trigger `db.refresh()`.
   - Assert vector state is consistent on the follower.

3. **Builder regression** (existing): the current `recovery_tests.rs` cases at lines 1246-1250, 1363-1367, 1509-1513, 1539-1543 stay as-is. They become the "builder explicitly drives recovery+freeze" regression for the half-migration bug.

4. **Audit pass:** grep `crates/engine/tests/`, `crates/vector/tests/`, `crates/executor/tests/` for any test that calls `freeze_vector_heaps()` directly. For each:
   - If the test is *about* the helper, leave it.
   - If the test is *using* the helper to mask the missing drop-time freeze, remove the manual call. The new lifecycle should make it unnecessary.

---

### Epic 6 — Documentation cleanup

**Goal:** kill stale docs that describe the participant model as canonical.

**Changes**

1. Update doc comments in `crates/engine/src/database/builder.rs` to describe the actual contract (one list, recovery + freeze).
2. Search `docs/` for "participant", "recovery participant", "register_vector_recovery" — update or remove. The design doc itself can be marked as implemented.
3. Update `crates/engine/src/recovery/mod.rs` module-level docs if they reference participants.
4. If any `CLAUDE.md` files mention the participant model, fix them.

---

## Implementation order

The design doc § Implementation Sequence is the authoritative ordering. To restate it concretely:

1. **Epic 1** — engine plumbing (`open.rs` + `SearchSubsystem::recover`). Codebase still builds; old participant path still works for executor.
2. **Epic 2** — `DatabaseBuilder` rewires to subsystem-aware open. Existing builder tests still pass (they already use the right shape).
3. **Epic 3** — executor moves `Strata::open` / `cache` / follower paths to the builder. Production now goes through the new lifecycle.
4. **Epic 5** — land production-shaped regressions immediately so the deletion in Epic 4 cannot regress them.
5. **Epic 4** — delete `participant.rs`, the `register_*_recovery` helpers, and their last call sites.
6. **Epic 6** — docs.

This ordering keeps `cargo check -p strata-engine -p strata-executor -p strata-vector` green at every step.

---

## Verification

Run after each epic and again at the end:

```bash
cargo check -p strata-engine -p strata-vector -p strata-executor
cargo test -p strata-engine recovery
cargo test -p strata-vector
cargo test -p strata-executor
```

End-to-end checks specific to this work:

- The new executor restart regression in Epic 5 must pass *without* calling `freeze_vector_heaps()` manually anywhere.
- The follower regression in Epic 5 must pass.
- After Epic 4, `rg "recover_all_participants|register_vector_recovery|register_search_recovery|RecoveryParticipant"` across the workspace must return zero hits.
- After Epic 4, the `.vec` cache file produced by the new restart regression must exist on disk between sessions, proving drop-time freeze ran through `VectorSubsystem::freeze`.

---

## Acceptance criteria (mirrors design doc § Acceptance)

1. There is exactly one lifecycle mechanism (`Subsystem`).
2. The same ordered subsystem list drives recovery and freeze for any given `Database` instance.
3. `Strata::open`, `Strata::open_with`, and follower open all compose subsystems via `DatabaseBuilder`.
4. The executor production path no longer depends on `register_vector_recovery` or `register_search_recovery`.
5. A production-shaped regression proves vector freeze runs on ordinary drop without an explicit `freeze_vector_heaps()` call.
6. `DatabaseBuilder` doc comments match its real behavior.
7. No doc still describes `RecoveryParticipant` as canonical.
