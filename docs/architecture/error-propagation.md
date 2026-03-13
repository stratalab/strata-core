# Error Propagation Trace

## 1. Error Type Inventory

| Crate | Type | Variants | Role |
|-------|------|----------|------|
| core | `StrataError` | 21 | Canonical engine error. All engine operations return `StrataResult<T>`. |
| executor | `Error` | 30 | Client-facing error. All handler/session operations return `Result<T>`. |
| engine | `VectorError` | 16 | Vector-specific errors. Converted to `StrataError` via `From` impl. |
| concurrency | `CommitError` | 4 | Transaction commit failures. Converted to `StrataError` via `From` impl. |
| concurrency | *(removed — conflicts use `ConflictType` directly)* | — | — |
| concurrency | `PayloadError` | 1 | MessagePack deserialization. Internal, not exposed. |
| durability | `BranchBundleError` | 14 | Bundle import/export errors. Converted to `StrataError::Storage` at engine boundary. |
| durability | `RecoveryError` | 7+ | Database recovery errors. Not converted — fatal at startup. |
| durability | `SnapshotReadError` | 11 | Disk snapshot read errors. Internal to recovery. |
| durability | `CodecError` | 3 | Codec encode/decode errors. Converted to `StrataError::Serialization`. |

**Total: ~105 error variants across 10 error types in 5 crates.**

## 2. Conversion Chain

```
                          ORIGIN ERRORS
                          ─────────────
  io::Error    serde_json::Error    bincode::Error    CodecError
      │              │                    │                │
      └──────────────┴────────────────────┴────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   StrataError   │   21 variants
                     │   (core crate)  │   Canonical engine error
                     └────────┬────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
            ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ VectorError  │  │ CommitError  │  │  BundleError │
    │ (16 variants)│  │ (4 variants) │  │(14 variants) │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │                 │                 │
           │   From impl     │  From impl      │  .map_err()
           └─────────────────┴─────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   StrataError   │   (merged back)
                     └────────┬────────┘
                              │
                              │  convert_result() in executor
                              │  From<StrataError> for Error
                              ▼
                     ┌─────────────────┐
                     │ executor::Error │   30 variants
                     │ (client-facing) │
                     └────────┬────────┘
                              │
                              ▼
                          CLIENT
```

**Two conversion paths exist** (this is a problem — see Section 4):

```
PATH A: Executor (non-transactional)
  engine returns StrataResult<T>
    → convert_result() in handler
    → From<StrataError> for Error (in convert.rs)
    → executor::Result<T>

PATH B: Session (transactional)
  ctx.get()/ctx.put() returns StrataResult<T>
    → .map_err(Error::from) in dispatch_in_txn
    → From<StrataError> for Error (same From impl, BUT different
      intermediate error types from TransactionContext)
    → executor::Result<T>
```

## 3. Context Loss at Each Boundary

### Boundary: StrataError → executor::Error

**Location**: `crates/executor/src/convert.rs`

| StrataError field | What happens | Impact |
|-------------------|-------------|--------|
| `entity_ref: EntityRef` in NotFound | String-parsed by prefix (`"kv:"`, `"branch:"`, etc.) to select Error variant | New entity types silently fall through to KeyNotFound |
| `entity_ref` in VersionConflict | **Discarded** | Client doesn't know which entity had the conflict |
| `entity_ref` in WriteConflict | Formatted into reason string | Unstructured — client must parse string |
| `entity_ref` in InvalidOperation | Formatted into reason string | Unstructured |
| `source: Box<Error>` in Storage | **Discarded** | Underlying OS error (disk full, permission denied) lost |
| `Version` enum in VersionConflict | Converted to raw u64 via `version_to_u64()` | Counter(5) and Txn(5) and Sequence(5) all become `5` |
| `duration_ms` in TransactionTimeout | Formatted into reason string | Numeric data lost |
| `resource`/`limit`/`requested` in CapacityExceeded | Formatted into reason string | Structured data lost |

### Boundary: VectorError → StrataError

**Location**: `crates/engine/src/primitives/vector/error.rs`

| VectorError field | What happens | Impact |
|-------------------|-------------|--------|
| `name` in CollectionNotFound | Mapped to `EntityRef::new("collection", name)` | Preserved |
| `name` in CollectionAlreadyExists | Mapped to `EntityRef::new("collection", name)` | Preserved |
| `key` in VectorNotFound | Mapped to `EntityRef::new("vector", key)` | Preserved |
| tuple strings in Storage/Transaction/Internal/Io/Database | Mapped to message strings | Preserved but untyped |

### Boundary: CommitError → StrataError

**Location**: `crates/concurrency/src/transaction.rs`

| CommitError variant | Maps to | Impact |
|--------------------|---------|--------|
| ValidationFailed(result) | TransactionAborted { reason: result.to_string() } | **Conflict details (keys, paths) lost** |
| InvalidState(msg) | TransactionNotActive { state: msg } | Preserved |
| WALError(msg) | Storage { message: msg } | **Error type lost** — WAL failure becomes generic storage |
| StorageError(msg) | Storage { message: msg } | Preserved |

## 4. Error Swallowing Inventory

### CRITICAL: Errors silently converted to success values

| Location | Code | What's swallowed | Severity |
|----------|------|------------------|----------|
| `handlers/state.rs` line ~79 | `Err(_) => Ok(Output::MaybeVersion(None))` | ALL errors from `state.init()` | **High** — storage failures, serialization errors, everything becomes None |
| `handlers/state.rs` line ~90 | `Err(_) => Ok(Output::MaybeVersion(None))` | ALL errors from `state.cas()` | **High** — version conflicts, not-found, storage failures all become None |
| `handlers/vector.rs` line ~86 | `let _ = p.vector.create_collection(...)` | ALL errors from `create_collection` | **Medium** — storage failures silently ignored, next insert fails confusingly |

### LOW: Cleanup/non-critical error ignoring

| Location | Code | What's swallowed | Severity |
|----------|------|------------------|----------|
| `durability/src/branch_bundle/writer.rs` | `let _ = std::fs::remove_file(...)` | Temp file removal | Low — cleanup |
| `storage/src/sharded.rs` | Thread join result ignored | Thread join errors | Low — shutdown path |

### MODERATE: `.ok()` converting Result to Option

| Location | What's lost | Severity |
|----------|------------|----------|
| `storage/src/sharded.rs` — `SnapshotView::get()` | Storage read errors become None | Medium — read failure looks like key-not-found |
| `durability/src/format/snapshot.rs` | Binary parse errors | Low — internal parsing |
| `durability/src/format/wal_record.rs` | Header parse errors | Low — internal parsing |
| `durability/src/retention/mod.rs` | UTF-8/hex parse errors | Low — file enumeration |

## 5. Inconsistency Table

### How each primitive handles the same error scenario

**Scenario: Operation fails due to storage/IO error**

| Primitive | Handler behavior | Client receives |
|-----------|-----------------|-----------------|
| KV | `convert_result()` propagates | `Error::Io { reason }` |
| Event | `convert_result()` propagates | `Error::Io { reason }` |
| State (read/init/set) | `convert_result()` propagates | `Error::Io { reason }` |
| State (cas) | **Swallowed** | `Output::MaybeVersion(None)` — looks like CAS failure |
| JSON | `convert_result()` propagates | `Error::Io { reason }` |
| Vector | `convert_vector_result()` propagates | `Error::Io { reason }` |
| Vector (upsert auto-create) | **Swallowed** | Subsequent insert fails with collection-not-found |
| Search | Mapped to Internal | `Error::Internal { reason }` |

**Scenario: Operation inside session transaction**

| Primitive | Error conversion path | Differs from non-txn? |
|-----------|----------------------|----------------------|
| KV get | `ctx.get().map_err(Error::from)` | Different `.map_err` chain than `convert_result` |
| KV put | `txn.kv_put().map_err(Error::from)` | Different chain |
| State read | `ctx.get().map_err(Error::from)` then `serde_json::from_str().map_err(Internal)` | **Yes** — JSON parse errors become Internal |
| JSON get (root) | `ctx.get().map_err(Error::from)` then `serde_json::from_str().map_err(Internal)` | **Yes** — same issue |
| JSON get (path) | `txn.json_get_path().map_err(Error::from)` | Different chain |

**Scenario: Entity not found**

| Error source | `convert_result` routing | Correct? |
|-------------|-------------------------|----------|
| KV key not found | `EntityRef` starts with `"kv:"` → `Error::KeyNotFound` | Yes |
| Branch not found | `EntityRef` starts with `"branch:"` → `Error::BranchNotFound` | Yes |
| Collection not found | `EntityRef` starts with `"collection:"` → `Error::CollectionNotFound` | Yes |
| Event stream not found | `EntityRef` starts with `"event:"` → `Error::StreamNotFound` | Yes |
| State cell not found | `EntityRef` starts with `"state:"` → `Error::CellNotFound` | Yes |
| New future entity type | No prefix match → **`Error::KeyNotFound`** (wrong variant) | **No** — silent misrouting |

## 6. Panic Risk Assessment

### Production code `.expect()` calls

| File | Count | Pattern | Risk |
|------|-------|---------|------|
| `executor.rs` | 27 | `branch.expect("resolved by resolve_default_branch")` | Medium — invariant violation panics the thread |
| `session.rs` | 1 | `.expect("txn_branch_id set when txn_ctx is Some")` | Medium — state corruption panics |
| `session.rs` | 1 | `.unwrap()` on `txn_ctx.take()` | Medium — state corruption panics |

**Total: 29 panic points in production code.**

All 27 executor `.expect()` calls assume `cmd.resolve_default_branch()` was called before dispatch. If a code path skips resolution, the executor panics rather than returning an error.

The 2 session panic points assume internal state consistency between `txn_ctx` and `txn_branch_id`. If one is set without the other (e.g., after a partial failure in `handle_begin`), the session panics.

## 7. Diagnostics Assessment

### What users CAN diagnose from errors

| Scenario | Error received | Diagnosable? |
|----------|---------------|-------------|
| Key not found | `KeyNotFound { key: "user:123" }` | Yes — key name included |
| Branch not found | `BranchNotFound { branch: "my-branch" }` | Yes |
| Wrong value type | `WrongType { expected: "Int", actual: "String" }` | Yes |
| Invalid key format | `InvalidKey { reason: "key too long (1025 > 1024)" }` | Yes |
| Dimension mismatch | `DimensionMismatch { expected: 128, actual: 256 }` | Yes |
| Invalid JSON path | `InvalidPath { reason: "..." }` | Yes |
| History trimmed | `HistoryTrimmed { requested: 1, earliest: 5 }` | Yes |

### What users CANNOT diagnose

| Scenario | Error received | What's missing |
|----------|---------------|----------------|
| State CAS version mismatch | `MaybeVersion(None)` | Everything — was it conflict? not-found? IO error? |
| State CAS on non-existent cell | `MaybeVersion(None)` | Same None as version mismatch |
| Storage error during state CAS | `MaybeVersion(None)` | Same None as everything else |
| Disk full during write | `Io { reason: "..." }` | Cannot distinguish from permission denied or corruption |
| WAL failure during commit | `TransactionConflict { reason: "..." }` | Cannot distinguish from read-set conflict |
| Search invalid query | `Internal { reason: "..." }` | Cannot distinguish from budget exceeded or backend failure |
| Data corruption in txn read | `Internal { reason: "..." }` | Cannot distinguish from code bug |
| Version conflict (which type?) | `VersionConflict { expected: 5, actual: 7 }` | Is 5 a counter, txn ID, or sequence number? |

## 8. Summary of Problems

| # | Problem | Severity | Type |
|---|---------|----------|------|
| 1 | state_cas() swallows ALL errors | High | Swallowed errors |
| 2 | Session vs executor use different error paths | Medium | Inconsistency |
| 3 | Storage error source chain discarded at executor boundary | Medium | Context loss |
| 4 | Search errors all collapse to Internal | Medium | Context loss |
| 5 | VersionConflict loses version type information | Low | Context loss |
| 6 | Commit failures all become TransactionConflict | Medium | Context loss |
| 7 | vector_upsert ignores create_collection errors (not just AlreadyExists) | Medium | Swallowed errors |
| 8 | Session deserialization errors map to Internal | Low | Wrong error type |
| 9 | 27 .expect() calls in executor can panic | Medium | Panic risk |
| 10 | NotFound routing depends on string prefix parsing | Low | Fragile design |
