# ST2 Primitive Transaction Semantics Extraction Plan

## Purpose

`ST2` is the first code-moving epic in the storage consolidation workstream.

Its purpose is to remove primitive-aware transaction semantics from
`strata-concurrency` before that crate is merged downward into `storage`.

This document is intentionally based on the current code, not just the storage
charter. The point is to name the exact fields, helper methods, validation
hooks, and commit-path call sites that must move.

Read this together with:

- [storage-minimal-surface-implementation-plan.md](./storage-minimal-surface-implementation-plan.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [storage-charter.md](./storage-charter.md)
- [concurrency-crate-map.md](./concurrency-crate-map.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)

## ST2 Verdict

The following are engine-owned concerns and must leave `strata-concurrency`
before `ST3` starts:

- event continuity bookkeeping across engine transaction wrappers
- JSON write materialization policy
- JSON path/patch conflict policy
- the JSON semantic state that exists only to support those policies

The following remain lower-layer concerns and will later merge into `storage`:

- generic read/write/delete tracking
- generic CAS tracking
- generic read-set validation
- generic commit/version sequencing
- generic WAL/replay runtime

## Why `ST2` Must Happen Before `ST3`

If `concurrency` is merged into `storage` in its current form, the lower
substrate would absorb:

- event sequence/hash continuity
- JSON document snapshot-version policy
- JSON patch buffering
- overlapping-path conflict rules
- commit-time JSON document materialization

That would make the architecture worse. `ST2` is the extraction step that
makes the later `concurrency -> storage` merge honest.

## Current Code Map

The primitive-aware seams are not theoretical. They live in concrete places.

### Lower-Layer Ownership Today

#### `crates/concurrency/src/transaction.rs`

This file currently owns all of the following:

- event continuity state on `TransactionContext`
  - `event_sequence_count: Option<u64>`
  - `event_last_hash: Option<[u8; 32]>`
  - constructor/reset wiring for those fields
  - `event_sequence_count()`
  - `event_last_hash()`
  - `set_event_state()`

- JSON semantic state on `TransactionContext`
  - `json_reads: Option<Vec<JsonPathRead>>`
  - `json_writes: Option<Vec<JsonPatchEntry>>`
  - `json_snapshot_versions: Option<HashMap<Key, CommitVersion>>`
  - `ensure_json_reads()`
  - `ensure_json_writes()`
  - `record_json_read()`
  - `record_json_write()`
  - `record_json_snapshot_version()`
  - `ensure_json_snapshot_tracked()`

- JSON transaction types
  - `JsonPathRead`
  - `JsonPatchEntry`

- JSON document storage helpers duplicated in the lower layer
  - `StoredJsonDoc`
  - `serialize_stored_json_doc()`
  - `deserialize_stored_json_doc()`

- the lower JSON transaction trait and implementation
  - `JsonStoreExt`
  - `impl JsonStoreExt for TransactionContext`
  - `json_get()`
  - `json_set()`
  - `json_delete()`
  - `json_get_document()`
  - `json_exists()`

- commit-time JSON materialization
  - `materialize_json_writes()`

#### `crates/concurrency/src/validation.rs`

This file currently owns JSON-specific validation hooks:

- `validate_json_set()`
- `validate_json_paths()`
- `ConflictType::JsonDocConflict`
- `ConflictType::JsonPathWriteWriteConflict`
- `validate_transaction()` merges both JSON validators into generic commit
  validation

#### `crates/concurrency/src/conflict.rs`

This file currently owns JSON path-overlap policy:

- `check_write_write_conflicts()`
- overlap defined as `w1.patch.path().overlaps(w2.patch.path())`

#### `crates/concurrency/src/manager.rs`

The commit path still has a primitive-specific hook:

- `txn.materialize_json_writes()` is called directly before WAL serialization

This file also contains the most important missing-document JSON OCC
regressions:

- `test_issue_1915_json_missing_doc_read_invisible_to_occ`
- `test_issue_1915_json_get_missing_doc_records_read`
- `test_issue_1915_json_set_missing_doc_records_read`

### Engine-Side Consumers Today

#### `crates/engine/src/transaction/context.rs`

The engine transaction wrapper still depends directly on lower-layer semantic
state:

- `Transaction::new()` reads:
  - `ctx.event_sequence_count()`
  - `ctx.event_last_hash()`

- `event_append()` writes the continuity state back into the lower context via
  `ctx.set_event_state()`

- JSON transaction methods still delegate to the lower JSON semantic layer:
  - `json_create()`
  - `json_get()`
  - `json_get_path()`
  - `json_set()`
  - `json_delete()`
  - `json_exists()`

#### `crates/engine/src/transaction/owned.rs`

The owned manual transaction handle currently owns only the pooled lower
`TransactionContext`:

- `Transaction { db, ctx }`

It does not yet own an engine-side semantic state object. That matters because
`TransactionContext` is currently being used as the persistence location for
event continuity and, indirectly, JSON delta state.

#### `crates/engine/src/primitives/json/mod.rs`

This file already contains engine-owned JSON helpers that should become the
source of truth for materialization:

- `JsonDoc`
- `JsonStore::serialize_doc()`
- `JsonStore::deserialize_doc_with_fallback_id()`
- path mutation helpers such as `delete_at_path()`

Those helpers should replace the duplicate `StoredJsonDoc` path in
`concurrency`.

#### `crates/engine/src/primitives/extensions.rs`

This file defines the engine-facing transaction extension traits. The public
transaction API should stay anchored here while the lower semantic seams are
removed.

#### `crates/engine/src/database/tests/regressions.rs`

This file contains the critical event continuity regression that must survive
the move:

- `test_issue_1914_sequence_from_persisted_meta`

It also contains the event append OCC regression that ensures concurrent
appenders still conflict through the metadata read path.

## Foundational Refactor Required Up Front

Before the three semantic extractions can finish cleanly, engine needs an
engine-owned transaction-semantic state carrier.

That is the main code reality the high-level plan did not capture.

### Why It Is Required

Today `ScopedTransaction::new()` in
[crates/engine/src/transaction/context.rs](../../crates/engine/src/transaction/context.rs)
reconstructs semantic state from the lower `TransactionContext`.

That works only because `TransactionContext` currently stores:

- event continuity
- JSON semantic state

Once those fields are removed from `TransactionContext`, engine still needs a
place to keep semantic state alive across:

- multiple `ScopedTransaction` wrappers inside one outer manual transaction
- any internal engine path that re-wraps the same lower context

### Recommended Shape

Introduce an engine-owned semantic sidecar under
[crates/engine/src/transaction/](../../crates/engine/src/transaction):

- one state object owned by the manual transaction handle in
  [owned.rs](../../crates/engine/src/transaction/owned.rs)
- borrowed by scoped wrappers in
  [context.rs](../../crates/engine/src/transaction/context.rs)

That sidecar is the natural future home for:

- event continuity state in `ST2A`
- JSON delta/read tracking state in `ST2B` and `ST2C`

This is not a detour. It is the enabling change that prevents semantic state
from falling back into `TransactionContext` under a different name.

## Execution Order

`ST2` should run as three ordered sub-epics:

1. `ST2A` — event continuity extraction
2. `ST2B` — JSON write materialization extraction
3. `ST2C` — JSON path/patch conflict policy extraction

Do not start with JSON conflict policy. It is the most entangled seam and it
depends on where engine-owned JSON transaction state lives after `ST2A` and
`ST2B`.

## ST2A — Event Continuity Extraction

### Goal

Move event continuity bookkeeping out of the lower transaction context and into
the engine transaction layer.

### Current Problem

The only reason `TransactionContext` stores `event_sequence_count` and
`event_last_hash` is to let multiple engine `ScopedTransaction` wrappers
continue one event stream correctly inside one outer transaction.

That is engine primitive semantics, not generic OCC machinery.

### Exact Changes Needed

#### In `strata-concurrency`

Remove from
[crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs):

- fields:
  - `event_sequence_count`
  - `event_last_hash`
- constructor wiring in `new()` and `with_store()`
- reset handling when pooled contexts are reused
- methods:
  - `event_sequence_count()`
  - `event_last_hash()`
  - `set_event_state()`

No replacement lower-layer event state should be added.

#### In `strata-engine`

Create engine-owned continuity state under
[crates/engine/src/transaction/](../../crates/engine/src/transaction):

- add an engine semantic sidecar type
- make [owned.rs](../../crates/engine/src/transaction/owned.rs) own it
- make [context.rs](../../crates/engine/src/transaction/context.rs) borrow it

Change the scoped wrapper constructors:

- `ScopedTransaction::new(...)`
- `ScopedTransaction::with_base_sequence(...)`

so they stop reading continuity from `TransactionContext` and instead use the
engine-owned sidecar.

Update `event_append()` in
[context.rs](../../crates/engine/src/transaction/context.rs) so it updates the
engine-owned sidecar rather than calling `ctx.set_event_state()`.

Update engine call sites that construct scoped wrappers directly from lower
contexts, especially:

- [crates/engine/src/database/tests/regressions.rs](../../crates/engine/src/database/tests/regressions.rs)

If internal engine paths still need a direct `ScopedTransaction::new(&mut
TransactionContext, ...)` form, they must also supply or create the semantic
sidecar explicitly.

### Acceptance

- no event continuity fields remain on `TransactionContext`
- engine owns all sequence/hash continuity across wrapper instances
- sequence allocation still derives from persisted metadata on first append
- the event hash chain remains correct

### Critical Tests

These must stay green or be replaced with stronger engine-owned coverage:

- `test_issue_1914_sequence_from_persisted_meta`
- the adjacent concurrent append OCC regression in
  [crates/engine/src/database/tests/regressions.rs](../../crates/engine/src/database/tests/regressions.rs)

## ST2B — JSON Write Materialization Extraction

### Goal

Move JSON write materialization out of `concurrency` and into engine-owned JSON
transaction semantics.

### Current Problem

Today the lower layer decides:

- how buffered JSON patch intent becomes a full stored document
- how patch sequences bump document version
- how root delete/create/update map onto the stored `JsonDoc` shape
- how the stored document bytes are serialized before generic commit

That logic is currently split between:

- `materialize_json_writes()` in
  [crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs)
- the direct manager hook in
  [crates/concurrency/src/manager.rs](../../crates/concurrency/src/manager.rs)
- the lower duplicate `StoredJsonDoc` encode/decode helpers

### Exact Changes Needed

#### In `strata-concurrency`

Remove lower-layer ownership of:

- `StoredJsonDoc`
- `serialize_stored_json_doc()`
- `deserialize_stored_json_doc()`
- `materialize_json_writes()`
- the direct `txn.materialize_json_writes()` call in
  [crates/concurrency/src/manager.rs](../../crates/concurrency/src/manager.rs)

After this step, the commit manager should see only generic writes, deletes,
and CAS state.

#### In `strata-engine`

Introduce engine-owned JSON transaction materialization that:

- buffers JSON mutation intent in engine-owned semantic state
- reconstructs the base document from snapshot state
- applies pending patches in order
- preserves the existing stored `JsonDoc` encoding
- writes the final generic `Key -> Value` result into the underlying lower
  transaction context before generic commit

The expected reuse points are already present in
[crates/engine/src/primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs):

- `JsonDoc`
- `JsonStore::serialize_doc()`
- `JsonStore::deserialize_doc_with_fallback_id()`
- existing path mutation helpers

The main engine touch points are likely:

- [crates/engine/src/transaction/context.rs](../../crates/engine/src/transaction/context.rs)
- [crates/engine/src/transaction/owned.rs](../../crates/engine/src/transaction/owned.rs)
- [crates/engine/src/primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs)
- [crates/engine/src/database/transaction.rs](../../crates/engine/src/database/transaction.rs)

The important commit-path change is that engine must finalize semantic JSON
state into generic mutations before handing the lower context to the generic
commit manager.

### Acceptance

- `concurrency` no longer materializes JSON writes
- the manager commit path no longer has primitive-specific JSON hooks
- engine owns how JSON patch intent becomes stored document bytes
- stored `JsonDoc` shape and legacy read compatibility remain unchanged

### Critical Tests

These behaviors must stay covered:

- `test_materialize_json_writes_emits_json_doc_shape` from
  [crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs)
  or an equivalent stronger engine-owned replacement
- the adjacent legacy raw JSON compatibility tests in that same file
- commit-path tests that prove the generic commit still persists the canonical
  document shape

## ST2C — JSON Path/Patch Conflict Policy Extraction

### Goal

Move JSON document OCC tracking and overlapping-path conflict policy out of
`concurrency` and into engine-owned JSON transaction semantics.

### Current Problem

The lower transaction layer currently owns both:

- JSON document snapshot-version tracking
- overlapping write-path conflict rules

The relevant state and validators are:

- `JsonPathRead`
- `JsonPatchEntry`
- `json_reads`
- `json_writes`
- `json_snapshot_versions`
- `validate_json_set()`
- `validate_json_paths()`
- `check_write_write_conflicts()`

This is JSON-specific concurrency policy living below engine.

### Exact Changes Needed

#### In `strata-concurrency`

Strip JSON-specific semantic state and validation out of:

- [crates/concurrency/src/transaction.rs](../../crates/concurrency/src/transaction.rs)
- [crates/concurrency/src/validation.rs](../../crates/concurrency/src/validation.rs)
- [crates/concurrency/src/conflict.rs](../../crates/concurrency/src/conflict.rs)
- [crates/concurrency/src/lib.rs](../../crates/concurrency/src/lib.rs)

After this step:

- `validate_transaction()` should be generic again
- `ConflictType` should no longer need JSON-specific variants
- the lower public surface should not export a JSON semantic trait

#### In `strata-engine`

Move JSON transactional validation into engine-owned semantic state:

- track observed document versions in engine-owned state
- track pending JSON path reads/writes in engine-owned state
- preserve the missing-document version-zero rule for create-if-absent races
- validate overlapping writes on the same document before generic commit

The main engine touch points are likely:

- [crates/engine/src/transaction/context.rs](../../crates/engine/src/transaction/context.rs)
- [crates/engine/src/transaction/owned.rs](../../crates/engine/src/transaction/owned.rs)
- [crates/engine/src/primitives/json/mod.rs](../../crates/engine/src/primitives/json/mod.rs)
- [crates/engine/src/database/transaction.rs](../../crates/engine/src/database/transaction.rs)

The commit order after `ST2C` should be:

1. engine validates JSON semantic state
2. engine materializes JSON deltas into generic writes/deletes
3. lower runtime performs only generic validation and generic commit

### Acceptance

- `concurrency` no longer exports JSON transaction-semantic state
- `concurrency` no longer validates JSON-specific conflicts
- engine owns JSON document OCC and path-overlap rules
- lower transaction validation is primitive-agnostic again

### Critical Tests

The issue-1915 regression family must survive intact:

- `test_issue_1915_json_missing_doc_read_invisible_to_occ`
- `test_issue_1915_json_get_missing_doc_records_read`
- `test_issue_1915_json_set_missing_doc_records_read`

The overlapping-path tests currently in
[crates/concurrency/src/conflict.rs](../../crates/concurrency/src/conflict.rs)
must also survive, ideally as engine-owned JSON semantic tests.

## Recommended Commit Shape

Execute `ST2` as three small commits, one semantic seam at a time:

1. `ST2A` — add the engine semantic sidecar and move event continuity into it
2. `ST2B` — move JSON materialization into engine and remove the manager hook
3. `ST2C` — move JSON read/write conflict policy into engine and make the
   lower validator generic again

Do not combine `ST2B` and `ST2C`. Materialization and conflict policy touch
the same state, but they are separable and should be verified independently.

## Compatibility Rules

### 1. Preserve JSON Storage Shape

Do not change the stored `JsonDoc` encoding or legacy raw JSON compatibility in
this epic.

### 2. Preserve Public Engine Transaction Behavior

The public engine transaction API should behave the same while the semantic
ownership moves underneath it.

### 3. Do Not Recreate Primitive Semantics Below Engine

Do not remove JSON/event seams from `concurrency` and then replace them with
new lower-layer semantic adapters under different names.

### 4. Move Semantic Tests Up With Ownership

If a lower-layer test is really proving JSON/event semantics rather than
generic OCC behavior, it should move upward with the owning code or be
replaced by a stronger engine-owned test.

## Verification Gates

Every sub-epic should prove correctness with:

- targeted seam regressions for the code being moved
- `cargo test -p strata-concurrency --quiet`
- `cargo test -p strata-engine --quiet`
- `cargo test -p stratadb --test engine --quiet`
- any targeted executor/integration suites affected by JSON or event behavior

Minimum required checks by sub-step:

1. `ST2A`
   - event sequence/hash regressions
   - manual transaction wrapper tests if constructor signatures change

2. `ST2B`
   - JSON document shape/materialization regressions
   - legacy raw JSON compatibility tests
   - commit-path tests proving generic writes still land in canonical form

3. `ST2C`
   - issue-1915 missing-document OCC regressions
   - overlapping write-path regressions
   - any higher-level JSON transaction tests that exercise create/update/delete
     within a transaction

## Definition Of Done

`ST2` is complete only when all of the following are true:

1. `concurrency` no longer stores event continuity state.
2. `concurrency` no longer stores JSON semantic delta/read state.
3. `concurrency` no longer materializes JSON writes.
4. `concurrency` no longer validates JSON-specific conflicts.
5. engine owns the equivalent event and JSON semantics through an engine-owned
   transaction-semantic layer.
6. lower-layer transaction validation is generic again.

That is the point at which `ST3` becomes safe.
