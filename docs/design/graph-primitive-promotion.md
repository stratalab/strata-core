# Graph Primitive Promotion: TypeTag::Graph (GRF-DEBT-001)

**Issue:** #1974
**Status:** Implemented
**Priority:** P0 architectural debt

## Problem

Graph stores all data as `TypeTag::KV (0x01)` under the `_graph_` namespace. The storage engine cannot distinguish graph data from user KV data. This means:

- Branch diff shows graph entries mixed with regular KV
- `describe()` must heuristically scan `_graph_` space
- Export/import has no dedicated Graph section
- No graph-aware compaction or statistics
- Space deletion doesn't sweep graph keys
- Checkpoint bundles graph data into KV section

## Solution

Add `TypeTag::Graph = 0x07` and `PrimitiveType::Graph`, making Graph a first-class primitive. No backward compatibility concerns.

---

## Epic 1: Core Type System

**Scope:** Add the new enum variants, key constructor, and make the compiler tell us what's missing.

### 1.1 TypeTag::Graph = 0x07

**File:** `crates/core/src/types.rs`

- Add `Graph = 0x07` to `TypeTag` enum
- Add `0x07 => Some(TypeTag::Graph)` to `from_byte()`
- Add `Key::new_graph(ns, user_key)` constructor
- Update ordering doc comment

**Tests to update:**
- `test_typetag_from_byte` — `from_byte(0x07)` changes from `None` to `Some(Graph)`
- `test_typetag_no_collisions` — add Graph to array
- `test_typetag_as_byte` — add Graph assertion
- `test_typetag_ordering` — add `Json < Graph`
- `test_typetag_as_byte_from_byte_roundtrip_exhaustive` — 0x07 now returns Some
- `test_typetag_ordering_matches_byte_values` — add Graph
- Serialization/deserialization/hashset tests — add Graph variant

**New tests:**
- `test_key_new_graph()` — verify Key::new_graph produces TypeTag::Graph
- `test_key_graph_does_not_match_kv_prefix()` — Graph and KV keys are distinct

### 1.2 PrimitiveType::Graph

**File:** `crates/core/src/contract/primitive_type.rs`

- Add `Graph` variant to enum
- Update module doc: "six primitives"
- `ALL: [PrimitiveType; 6]`
- Match arms: `name()` -> "GraphStore", `id()` -> "graph", `from_id("graph")`, `supports_crud()` -> true, `entry_type_range()` -> (0x60, 0x6F), `primitive_id()` -> 6

**Tests to update:**
- `test_primitive_type_all` — 5 -> 6
- `test_primitive_type_const_all` — 5 -> 6
- `test_primitive_type_hash` — 5 -> 6
- `test_primitive_id_uniqueness` — 5 -> 6
- Names/ids/from_id/roundtrip/entry_type_range/primitive_id — add Graph

### 1.3 EntityRef::Graph

**File:** `crates/core/src/contract/entity_ref.rs`

- Add `Graph { branch_id, graph_key }` variant
- Add match arms in `branch_id()`, `primitive_type()`, `Display`

**Tests to update:**
- `test_all_primitive_types_covered` — 5 -> 6

### 1.4 Durability primitive tag

**File:** `crates/durability/src/format/primitive_tags.rs`

- Add `pub const GRAPH: u8 = 0x06;`
- `ALL_TAGS: [u8; 6]`
- `tag_name()` — add Graph

**Tests to update:**
- `all_tags_unique`, `all_tags_complete`, `tag_names`

**Completion signal:** `cargo build --workspace` compiles. Non-exhaustive match warnings guide Epic 2.

---

## Epic 2: Graph Key Migration

**Scope:** Switch graph crate from `TypeTag::KV` to `TypeTag::Graph`. This is the semantic core of the change.

### 2.1 graph_key() constructor

**File:** `crates/graph/src/keys.rs`

- Line 113: `Key::new_kv(...)` -> `Key::new_graph(...)`

**Tests to update:**
- `assert_kv_tag` helper (line 362) -> `assert_graph_tag` / `TypeTag::Graph`
- `storage_key_has_kv_type_tag` (line 504) -> expect `TypeTag::Graph`

### 2.2 Direct Key::new_kv calls in lib.rs

**File:** `crates/graph/src/lib.rs`

- 14+ direct `Key::new_kv(ns, ...)` calls in bulk insert paths (lines ~1007-1325)
- 8+ prefix scan calls building prefix keys
- ALL must change to `Key::new_graph(...)` or `Key::new(ns, TypeTag::Graph, ...)`

### 2.3 Ontology prefix scans

**File:** `crates/graph/src/ontology.rs`

- 3 prefix scan calls at lines ~78, 168, 783
- These go through `storage_key()` -> `graph_key()` so auto-fix from 2.1

### 2.4 Doc updates

- `keys.rs` module doc: "KV-type keys" -> "Graph-type keys"
- `lib.rs` module doc: update TypeTag::KV references

**Completion signal:** `cargo test -p strata-graph` passes. Graph data now uses TypeTag::Graph.

---

## Epic 3: Cross-Cutting Iteration Sites

**Scope:** Make graph data visible to diff, merge, space operations, and compaction.

### 3.1 Branch operations

**File:** `crates/engine/src/branch_ops.rs`

- `DATA_TYPE_TAGS` -> `[TypeTag; 5]` adding `TypeTag::Graph`
- `primitive_to_type_tags()`: add `PrimitiveType::Graph => vec![TypeTag::Graph]`
- `type_tag_to_primitive()`: add `TypeTag::Graph => PrimitiveType::Graph`

### 3.2 Space deletion sweep

**File:** `crates/executor/src/handlers/space.rs` (line 60)

- Add `TypeTag::Graph` to inline array

### 3.3 Other inline TypeTag arrays (grep and fix)

- `crates/engine/src/primitives/branch/index.rs` ~line 393 — branch deletion sweep
- `crates/engine/src/primitives/space.rs` ~line 114 — space emptiness check
- `crates/engine/src/bundle.rs` ~line 154 — bundle branchlog

### 3.4 Checkpoint data collection

**File:** `crates/engine/src/database/compaction.rs`

- Add `TypeTag::Graph` loop alongside KV loop, pushing into same `kv_entries`

### 3.5 Key encoding tests

**File:** `crates/storage/src/key_encoding.rs`

- Add `Just(TypeTag::Graph)` to property-based test arrays

### 3.6 Module documentation

**File:** `crates/engine/src/primitives/mod.rs`

- Add "Graph storage is provided by the `strata-graph` crate."

**Completion signal:** `cargo test -p strata-engine -p strata-storage` passes. Graph data appears in diffs and survives checkpoint.

---

## Epic 4: Search Integration

**Scope:** Make graph node properties discoverable by full-text search.

### 4.1 Search recovery

**File:** `crates/engine/src/search/recovery.rs`

- Add `TypeTag::Graph` loop in `recover_from_db()` (~line 140)
- Add `TypeTag::Graph` loop in `reconcile_index()` (~line 227)
- Skip graph metadata keys: `__meta__`, `__catalog__`, `__types__/`, `__by_type__/`, `__edge_count__/`, `__ref__/`

### 4.2 Post-commit search indexing

**File:** `crates/engine/src/database/lifecycle.rs`

- Lines 222-230 and 259-264: Add `TypeTag::Graph` case
- Same metadata key skip list as 4.1

### 4.3 Search handler primitive parsing

**File:** `crates/executor/src/handlers/search.rs`

- Add `"graph" => Some(PrimitiveType::Graph)` (~line 70)

**Completion signal:** Graph node properties are searchable. `cargo test -p strata-engine` search tests pass.

---

## Epic 5: Export & CLI

**Scope:** Make graph data exportable and CLI-accessible.

### 5.1 ExportPrimitive

**File:** `crates/executor/src/types.rs`

- Add `Graph` variant to `ExportPrimitive` enum (~line 799)

### 5.2 Export handler

**File:** `crates/executor/src/handlers/export.rs`

- Add `ExportPrimitive::Graph => collect_graph(...)` case
- Implement `collect_graph()` — scan `TypeTag::Graph` keys, return as ExportRows

### 5.3 CLI support

**Files:** `crates/cli/src/parse.rs`, `main.rs`, `repl.rs`

- Add Graph to CLI primitive dispatch (if applicable)

**Completion signal:** `EXPORT graph` command works. `cargo test --workspace` passes.

---

## Epic 6: Integration Tests & Cleanup

**Scope:** Verify end-to-end correctness and fill test gaps.

### 6.1 Test utilities

- `tests/common/mod.rs` — update `all_primitive_types()` if exists
- `tests/intelligence/architectural_invariants.rs` — currently expects 6 primitives (will auto-pass)

### 6.2 New integration tests

- `test_graph_data_in_branch_diff()` — graph changes appear in diff
- `test_graph_data_survives_checkpoint()` — checkpoint/restore preserves graph data
- `test_graph_data_deleted_with_space()` — space deletion sweep cleans up graph data

### 6.3 Final sweep

- `cargo fmt --all && cargo clippy --workspace` — clean
- Verify no remaining `Key::new_kv` in graph crate
- Verify no remaining `TypeTag::KV` assertions in graph tests

**Completion signal:** `cargo test --workspace` green. `cargo clippy --workspace` clean.

---

## Epic Dependency Graph

```
Epic 1 (Core Types)
    |
    v
Epic 2 (Graph Key Migration) -----> Epic 3 (Cross-Cutting)
                                         |
                                         v
                              Epic 4 (Search) --> Epic 5 (Export/CLI)
                                                      |
                                                      v
                                               Epic 6 (Integration)
```

Epics 1 and 2 are the foundation. Epic 3 can start once Epic 2 is done. Epics 4 and 5 are independent of each other but both depend on Epic 3. Epic 6 is the final sweep.

## Files Touched (by epic)

| Epic | Files | Estimate |
|------|-------|----------|
| 1 | `types.rs`, `primitive_type.rs`, `entity_ref.rs`, `primitive_tags.rs` | 4 files |
| 2 | `graph/keys.rs`, `graph/lib.rs`, `graph/ontology.rs` | 3 files |
| 3 | `branch_ops.rs`, `space.rs` (2), `bundle.rs`, `compaction.rs`, `key_encoding.rs`, `primitives/mod.rs`, `branch/index.rs` | 8 files |
| 4 | `search/recovery.rs`, `lifecycle.rs`, `handlers/search.rs` | 3 files |
| 5 | `types.rs`, `export.rs`, CLI files | 3-5 files |
| 6 | `tests/` directory | 2-3 files |
| **Total** | | **~23 files** |
