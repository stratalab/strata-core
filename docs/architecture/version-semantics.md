# Version Semantics Correctness

## 1. Version Type System

```
  enum Version {
      Txn(u64),        ← Transaction-level MVCC version (global monotonic)
      Sequence(u64),   ← Append-only position in event log (per-branch monotonic)
      Counter(u64),    ← Per-entity mutation count (per-cell/doc/record)
  }
```

**Location**: `crates/core/src/contract/version.rs`

### Key Properties

| Method | Behavior |
|--------|----------|
| `as_u64()` | Returns raw u64 — **strips variant tag** |
| `PartialEq` / `Eq` | Full enum comparison — `Counter(5) != Txn(5)` |
| `Ord` | Discriminant first (Txn < Sequence < Counter), then value |
| `increment()` | Preserves variant — `Counter(5).increment() → Counter(6)` |
| `From<u64>` | Creates `Version::Txn(v)` — defaults to Txn |

## 2. Two-Level Versioning Architecture

```
  STORAGE LAYER (MVCC)                   VALUE LAYER (Primitive)
  ────────────────────                   ───────────────────────

  VersionChain for key "state:my-cell"
  ┌─────────────────────────────────┐
  │ StoredValue                     │
  │   version: Version::Txn(42)    │ ← Outer: MVCC snapshot version
  │   value: Value::String(         │
  │     '{"value": 7,              │
  │      "version": Counter(3),    │ ← Inner: Per-cell mutation count
  │      "updated_at": 1234}'      │
  │   )                            │
  ├─────────────────────────────────┤
  │ StoredValue                     │
  │   version: Version::Txn(38)    │ ← Outer: from earlier transaction
  │   value: Value::String(         │
  │     '{"value": 5,              │
  │      "version": Counter(2),    │ ← Inner: earlier mutation count
  │      "updated_at": 1230}'      │
  │   )                            │
  └─────────────────────────────────┘
```

**The outer version** (Txn) is used for:
- MVCC snapshot reads (`get_at_version(max_version)`)
- Transaction conflict detection (read-set validation)
- Global ordering of commits

**The inner version** (Counter/Sequence) is used for:
- Client-facing version numbers (CAS expected_counter)
- Primitive-specific semantics (event ordering, state mutation count)

## 3. Version Usage by Primitive

| Primitive | Inner Version | Source | Counter Space | Where Created |
|-----------|--------------|--------|---------------|---------------|
| KV | Txn | Transaction commit version | Global AtomicU64 | `kv.rs:122` |
| Event | Sequence | Per-branch `EventLogMeta.next_sequence` | Per-branch monotonic | `event.rs:378` |
| State | Counter | Per-cell counter in `State` struct | Per-cell, starts at 1 | `state.rs:125,218,244` |
| JSON | Counter | Per-document counter in `JsonDoc` struct | Per-document, starts at 1 | `json.rs:267,395,448` |
| Vector | Counter | Per-record version in `VectorRecord` struct | Per-record | `store.rs:459` |
| Branch | Counter | Per-branch metadata counter | Per-branch | `index.rs:149` |

### KV is the exception

KV is the only primitive where the inner version == the outer version. KV returns `Version::Txn(commit_version)` directly from `kv_put()`. All other primitives maintain their own version counters independent of the MVCC transaction version.

## 4. Storage Layer Invariant

```rust
// crates/storage/src/sharded.rs:83-88
pub fn get_at_version(&self, max_version: u64) -> Option<&StoredValue> {
    debug_assert!(
        self.versions.iter().all(|sv| sv.version().is_txn()),
        "Storage layer should only contain Txn versions"
    );
    self.versions.iter().find(|sv| sv.version().as_u64() <= max_version)
}
```

**All storage-layer versions are `Version::Txn`**. This is enforced by:

1. `put_with_version(key, value, version: u64)` wraps as `Version::txn(version)` — `sharded.rs:1169`
2. `delete_with_version(key, version: u64)` creates tombstone with `Version::txn(version)` — `sharded.rs:372`
3. Debug assertion in `get_at_version()` — `sharded.rs:85-87`
4. Debug assertion in `history()` — `sharded.rs:141-143`

**Comparison uses `as_u64()`** (line 92), which is correct because all entries in a chain are the same variant (Txn).

## 5. Key Namespace Separation

Different primitives use different `TypeTag` values in their keys, preventing cross-primitive version chain mixing:

```
  Key structure: (BranchId, TypeTag, user_key_bytes)

  KV:     (branch, 0x01, "my-key")      → VersionChain A
  Event:  (branch, 0x02, "00000005")     → VersionChain B
  State:  (branch, 0x03, "my-cell")      → VersionChain C
  Branch: (branch, 0x05, "my-branch")    → VersionChain D
  Vector: (branch, 0x10, "my-vec")       → VersionChain E
  JSON:   (branch, 0x11, "my-doc")       → VersionChain F
```

**Location**: `crates/core/src/types.rs:131-171`

A KV key `"foo"` and a State cell `"foo"` are different keys in storage (`(branch, 0x01, "foo")` vs `(branch, 0x03, "foo")`). They occupy separate VersionChains. Mixed version types cannot collide.

## 6. Version Comparison Semantics by Context

| Context | Comparison Method | Correct? |
|---------|------------------|----------|
| Storage `get_at_version()` | `as_u64()` — raw numeric | Yes — all entries are Txn |
| Storage `get_at_timestamp()` | `u64::from(sv.timestamp())` — raw numeric | Yes — compares microsecond timestamps |
| Storage `gc()` | `as_u64()` — raw numeric | Yes — all entries are Txn |
| Storage `history()` | `as_u64()` — raw numeric | Yes — all entries are Txn |
| Read-set validation | `u64 == u64` | Yes — both from same version space |
| State CAS (engine) | `Version == Version` — full enum | Yes — both always Counter |
| State CAS (TransactionOps) | `Version != Version` — full enum | Yes — both always Counter |
| `Ord` trait | Discriminant then value | N/A — not used in production paths |

**Note on timestamps**: `get_at_timestamp()` compares `StoredValue.timestamp()` (a microsecond-precision wall-clock timestamp) rather than the MVCC version. This is used for time-travel queries where the user provides a wall-clock timestamp rather than a version number.

## 7. Version at the Executor Boundary

```
  Engine returns Version enum
       │
       │  extract_version() / version_to_u64()
       │  Strips variant, returns raw u64
       ▼
  Client receives u64
       │
       │  Executor reconstructs variant
       │  (hardcoded per command type)
       ▼
  Engine receives Version enum
```

**Location**: `crates/executor/src/bridge.rs:273-280`, `crates/executor/src/convert.rs:135-142`

### Reconstruction by command type

| Command | Client sends | Executor wraps as |
|---------|-------------|-------------------|
| StateCas | `expected_counter: Option<u64>` | `Version::Counter(v)` |
| EventGet | `sequence: u64` | Raw u64 (sequence number, not Version) |
| KvPut | N/A (no expected version) | N/A |

The executor layer knows which variant each command uses and reconstructs correctly. A client cannot cause a variant mismatch because the wrapping is hardcoded.

## 8. Problems Found

### Problem 1: VectorStore insert() and get() return different Version variants

**Severity**: Medium
**Status**: **FIXED** (confirmed 2026-04-06 during Phase 4 of the MVCC time-travel verification work). All `VectorStore` read and write paths now consistently return `Version::counter(record.version)`. Verified at `crates/vector/src/store/crud.rs` lines 165, 226, 294, 522 and `crates/vector/src/ext.rs` line 239 — six independent paths, all uniform.

Historical context for the original bug:

```rust
// store.rs:459 — insert returned Counter
Ok(Version::counter(record_version))

// store.rs:519 — get returned Txn
version: Version::txn(record.version),
```

The same `record.version` (a raw `u64`) was wrapped as `Version::counter()` on insert but `Version::txn()` on get. The client saw the same numeric value, but:

- Code comparing these with full enum equality wouldn't match
- The Version variant carries semantic meaning — Counter means "per-entity mutation count" while Txn means "transaction commit version"

### Problem 2: EventGetByType silently returns version 0 for non-Sequence variants

**Severity**: Low

```rust
// handlers/event.rs:57-59
version: match e.version {
    Version::Sequence(s) => s,
    _ => 0,                    // Silent fallback
},
```

If an event's version is not `Version::Sequence` (which should never happen in practice), the handler silently returns 0 instead of using `as_u64()` or returning an error. All other event handlers use `extract_version()` which handles all variants. This handler has a special-case that could mask bugs.

### Problem 3: Client loses version type information (existing #930)

**Severity**: Low (already filed)

`extract_version()` and `version_to_u64()` strip the variant tag. The client receives `VersionConflict { expected: 5, actual: 7 }` with no indication of whether these are counters, transaction IDs, or sequence numbers. Already documented in issue #930.

### Problem 4: Debug assertion is the only guard on storage version type

**Severity**: Low

The invariant that all storage-layer versions are `Version::Txn` is protected only by `debug_assert!`, which is stripped in release builds. If a code path ever stores a non-Txn version, the assertion would catch it in debug mode but silently corrupt version ordering in release mode.

In practice, this is safe because `put_with_version()` unconditionally wraps as `Version::txn()`. But the guard relies on the API contract, not on type-system enforcement.

## 9. Correctness Assessment

### Can mixed version types produce wrong snapshot reads?

**No.** The design prevents this through three layers of defense:

1. **Key namespace separation**: Different primitives use different TypeTags, so their VersionChains never overlap.

2. **Storage API wraps as Txn**: `put_with_version()` always creates `Version::txn(version)`, regardless of what the primitive uses internally.

3. **Primitive versions are inside the Value**: Counter and Sequence versions are serialized inside the Value payload (State struct, Event struct, etc.), not at the storage version level.

### Can version counter spaces collide?

**No.** Each counter space is independent:

| Counter | Source | Scope |
|---------|--------|-------|
| MVCC version (Txn) | `TransactionManager.version` AtomicU64 | Global — shared by all primitives |
| Event sequence | `EventLogMeta.next_sequence` | Per-branch, per-event-log |
| State counter | `State.version` Counter(n) | Per-cell |
| JSON counter | `JsonDoc.version` | Per-document |
| Vector counter | `VectorRecord.version` | Per-record |
| Branch counter | `BranchMetadata.version` | Per-branch |

The MVCC version and the primitive-specific version are stored at different levels (outer vs inner). A State cell with Counter(3) stored at Txn(42) in the storage layer has no interaction with a KV key stored at Txn(43).

## 10. Summary

| # | Finding | Severity | Type |
|---|---------|----------|------|
| 1 | VectorStore insert returns Counter, get returns Txn for same version | Medium | Bug — inconsistent variant |
| 2 | EventGetByType silently returns version 0 for non-Sequence variants | Low | Defensive coding gap |
| 3 | Client loses version type information (existing #930) | Low | Context loss |
| 4 | Storage version type invariant protected only by debug_assert | Low | Design — not type-enforced |

**Overall**: The version system is correctly designed. The two-level architecture (outer Txn for MVCC, inner Counter/Sequence for primitive semantics) is sound. Key namespace separation prevents cross-primitive contamination. The only real bug is the vector variant inconsistency (#1), which affects correctness if code ever inspects the variant rather than the raw value.
