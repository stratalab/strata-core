# Session Transaction Completeness Audit

> Status note (March 6, 2026): this document is a historical audit snapshot.
> Parts of section 2 are stale relative to current code, especially `Flush`,
> `Compact`, and `RetentionApply` behavior. See
> `docs/internal/architecture-audit/api-semantics-drift-report.md` for the
> current evidence-based matrix.

## 1. How Session Routing Works

```
  Command arrives at Session::execute()
  │
  ├─ Transaction lifecycle?
  │   TxnBegin / TxnCommit / TxnRollback / TxnInfo / TxnIsActive
  │   └─ Dedicated handler (handle_begin, handle_commit, etc.)
  │
  ├─ Explicitly non-transactional? (24 commands)
  │   Branch, Vector, Database, Retention, version history
  │   └─ Always → executor.execute(cmd)
  │      Regardless of whether a transaction is active.
  │
  └─ Everything else (18 commands)
      ├─ Transaction active?
      │   └─ YES → dispatch_in_txn(cmd)
      │           ├─ 13 commands: handled via TransactionContext
      │           └─ 5 commands: catch-all → executor.execute(cmd)
      │
      └─ NO → executor.execute(cmd)
```

**Location**: `crates/executor/src/session.rs:70-118`

## 2. Complete Command Routing Table

### Transaction lifecycle (5) — dedicated handlers

| Command | Handler | Notes |
|---------|---------|-------|
| `TxnBegin` | `handle_begin()` | Creates TransactionContext, stores in session |
| `TxnCommit` | `handle_commit()` | Validates read-set, WAL, apply writes |
| `TxnRollback` | `handle_abort()` | Returns context to pool without applying |
| `TxnInfo` | `handle_txn_info()` | Returns txn_id and status |
| `TxnIsActive` | Inline | Returns `Bool(in_transaction())` |

### Handled inside transaction (13) — via TransactionContext

These commands use the transaction's write-set and snapshot for read-your-writes semantics.

| Command | Mechanism | Read-your-writes? |
|---------|-----------|-------------------|
| `KvGet` | `ctx.get()` → write_set → delete_set → snapshot | Yes |
| `KvList` | `ctx.scan_prefix()` → merged view | Yes |
| `KvPut` | `Transaction::kv_put()` → ctx.put() | Yes |
| `KvDelete` | `ctx.exists()` + `ctx.delete()` | Yes |
| `StateGet` | `ctx.get()` + JSON deserialize | Yes |
| `StateInit` | `Transaction::state_init()` | Yes |
| `StateCas` | `Transaction::state_cas()` | Yes |
| `JsonGet` | Root: `ctx.get()` + JSON deserialize. Path: `Transaction::json_get_path()` | Yes |
| `JsonSet` | `Transaction::json_set()` | Yes |
| `JsonDelete` | `Transaction::json_delete()` | Yes |
| `EventAppend` | `Transaction::event_append()` (hash chaining) | Yes |
| `EventGet` | `Transaction::event_get()` | Yes |
| `EventLen` | `Transaction::event_len()` | Yes |

**Location**: `crates/executor/src/session.rs:214-361`

### Explicitly routed to executor (24) — bypass transaction silently

These commands go to `executor.execute()` regardless of whether a transaction is active. No error or warning is returned.

| Command | Has side effects? | Sees uncommitted writes? |
|---------|-------------------|-------------------------|
| `BranchCreate` | **WRITE** — creates branch | No |
| `BranchGet` | Read | No |
| `BranchList` | Read | No |
| `BranchExists` | Read | No |
| `BranchDelete` | **WRITE** — deletes branch + data | No |
| `VectorUpsert` | **WRITE** — inserts/updates embedding | No |
| `VectorGet` | Read | No |
| `VectorDelete` | **WRITE** — deletes embedding | No |
| `VectorSearch` | Read | No |
| `VectorCreateCollection` | **WRITE** — creates collection | No |
| `VectorDeleteCollection` | **WRITE** — deletes collection + data | No |
| `VectorListCollections` | Read | No |
| `Ping` | None | N/A |
| `Info` | Read (metadata) | N/A |
| `Flush` | None (TODO) | N/A |
| `Compact` | None (TODO) | N/A |
| `RetentionApply` | Returns error ("not yet implemented") | N/A |
| `RetentionStats` | Returns error ("not yet implemented") | N/A |
| `RetentionPreview` | Returns error ("not yet implemented") | N/A |
| `KvGetv` | Read (version history) | No |
| `StateGetv` | Read (version history) | No |
| `JsonGetv` | Read (version history) | No |
| `JsonList` | Read (document listing) | No |
| `EventGetByType` | Read (type-filtered events) | No |

**Location**: `crates/executor/src/session.rs:81-108`

### Catch-all in dispatch_in_txn (5) — escape to executor during active transaction

These commands are NOT in the explicit outer routing list, so they reach `dispatch_in_txn` when a transaction is active. But they have no explicit handler there, so the catch-all at line 365 sends them to `executor.execute()`.

| Command | What executor does | Problem |
|---------|-------------------|---------|
| `StateSet` | Writes state via implicit single-op transaction | **WRITE bypasses session transaction** |
| `Search` | Runs HybridSearch across all primitives | Read only — doesn't see uncommitted writes |
| `BranchExport` | Exports branch to file | File I/O — safe |
| `BranchImport` | Imports branch from file | **WRITE bypasses session transaction** |
| `BranchBundleValidate` | Validates bundle file | File I/O only — safe |

**Location**: `crates/executor/src/session.rs:363-365`

## 3. Problems Found

### Problem 1: StateSet bypasses transaction scope

**Severity**: High
**Existing issue**: #837

```
TxnBegin
  KvPut("key", 1)        ← buffered in transaction
  StateSet("cell", "x")  ← IMMEDIATELY committed to storage
TxnRollback
  "key" → rolled back     ✓
  "cell" → still "x"      ✗ NOT rolled back
```

`StateSet` is the only write command for a transactional primitive (State) that bypasses the session transaction. `StateInit` and `StateCas` are handled correctly inside the transaction. `StateSet` falls through the catch-all to `executor.execute()`, which creates its own implicit single-operation transaction.

**Root cause**: `dispatch_in_txn` has explicit handlers for `StateInit` and `StateCas` but not `StateSet`. The comment at line 363 says "includes batch operations, history, CAS, scan, incr, etc." suggesting `StateSet` was overlooked.

### Problem 2: Vector writes silently bypass transaction — no error

**Severity**: Medium

```
TxnBegin
  KvPut("key", 1)                    ← buffered in transaction
  VectorUpsert("vec", [0.1, 0.2])    ← IMMEDIATELY committed
TxnRollback
  "key" → rolled back     ✓
  "vec" → still exists     ✗ NOT rolled back
```

All 7 vector commands are explicitly routed to executor at the outer dispatch level (session.rs:87-93). The user receives a success response with no indication that the operation is outside the transaction scope.

Vector operations are non-transactional by design (the vector backend cannot participate in transactions — see issue #937). But the session should either:
- Return an error when vector writes are attempted inside a transaction, OR
- Document the behavior explicitly in the response

### Problem 3: Branch writes silently bypass transaction — no error

**Severity**: Medium

```
TxnBegin
  KvPut("key", 1)         ← buffered in transaction
  BranchCreate("new")     ← IMMEDIATELY committed
TxnRollback
  "key" → rolled back     ✓
  "new" branch → exists   ✗ NOT rolled back
```

`BranchCreate` and `BranchDelete` are write operations that are explicitly routed to executor (session.rs:82-86). Like vectors, branches are not currently transactional, but the user has no way to know this.

### Problem 4: Read commands inside transaction don't see uncommitted writes

**Severity**: Medium

```
TxnBegin
  JsonSet("doc", "$", {"a": 1})    ← buffered in transaction
  JsonList()                        ← reads from committed store
                                      "doc" NOT in results
  EventAppend("type1", payload)    ← buffered in transaction
  EventGetByType("type1")         ← reads from committed store
                                      event NOT in results
TxnCommit
```

Five read commands for transactional primitives bypass the transaction context:

| Command | Transactional equivalent exists? | Why bypassed |
|---------|--------------------------------|--------------|
| `JsonList` | No `ctx.scan_prefix` equivalent for JSON | No JSON-specific list in TransactionContext |
| `EventGetByType` | No type-filtered read in TransactionContext | Requires scan across all events |
| `KvGetv` | No version history in TransactionContext | Requires storage-layer version chains |
| `StateGetv` | No version history in TransactionContext | Requires storage-layer version chains |
| `JsonGetv` | No version history in TransactionContext | Requires storage-layer version chains |

The version history commands (Getv) are inherently non-transactional — they read the committed version chain, which is a reasonable design. But `JsonList` and `EventGetByType` are regular read commands whose transactional counterparts (JsonGet, EventGet) DO use the transaction context. The inconsistency is confusing.

### Problem 5: BranchImport bypasses transaction via catch-all

**Severity**: Low

`BranchImport` falls through `dispatch_in_txn`'s catch-all to `executor.execute()`. Unlike the other branch commands (which are explicitly routed at the outer level), Import reaches the catch-all accidentally — it was not listed in the explicit non-transactional block at lines 82-86.

This works correctly (the executor handles it fine), but the routing is inconsistent. BranchExport and BranchBundleValidate have the same issue.

## 4. Routing Consistency Matrix

### Write operations

| Primitive | Write commands | In dispatch_in_txn? | Consistent? |
|-----------|---------------|---------------------|-------------|
| KV | KvPut, KvDelete | Yes, Yes | Yes |
| State | StateInit, StateCas, **StateSet** | Yes, Yes, **No** | **No** — StateSet escapes |
| JSON | JsonSet, JsonDelete | Yes, Yes | Yes |
| Event | EventAppend | Yes | Yes |
| Vector | VectorUpsert, VectorDelete, Create/DeleteCollection | No (all 4) | Yes (consistently non-transactional) |
| Branch | BranchCreate, BranchDelete, BranchImport | No (all 3) | Yes (consistently non-transactional) |

### Read operations

| Primitive | Read commands | In dispatch_in_txn? | Consistent? |
|-----------|-------------|---------------------|-------------|
| KV | KvGet, KvList, **KvGetv** | Yes, Yes, **No** | Partial — Getv intentionally excluded |
| State | StateGet, **StateGetv** | Yes, **No** | Partial — Readv intentionally excluded |
| JSON | JsonGet, **JsonList**, JsonDelete, **JsonGetv** | Yes, **No**, Yes, **No** | **No** — JsonList should be transactional |
| Event | EventGet, EventLen, **EventGetByType** | Yes, Yes, **No** | **No** — ReadByType should be transactional |
| Vector | VectorGet, VectorSearch, ListCollections | No (all 3) | Yes (consistently non-transactional) |
| Branch | BranchGet, BranchList, BranchExists | No (all 3) | Yes (consistently non-transactional) |

## 5. TransactionContext Capability Gaps

The `Transaction` wrapper (engine's `TransactionOps` trait) supports these operations:

| Operation | Supported | Used by dispatch_in_txn |
|-----------|-----------|------------------------|
| `kv_get` | Yes | Yes (via ctx.get) |
| `kv_put` | Yes | Yes |
| `kv_delete` | Yes | Yes (via ctx.delete) |
| `kv_exists` | Yes | Yes (via ctx.exists) |
| `kv_list` | Yes | Yes (via ctx.scan_prefix) |
| `event_append` | Yes | Yes |
| `event_get` | Yes | Yes |
| `event_range` | Yes | Not used |
| `event_len` | Yes | Yes |
| `state_get` | Yes | Yes (via ctx.get) |
| `state_init` | Yes | Yes |
| `state_cas` | Yes | Yes |
| `json_create` | Yes | Not used directly |
| `json_get` | Yes | Yes |
| `json_get_path` | Yes | Yes |
| `json_set` | Yes | Yes |
| `json_delete` | Yes | Yes |
| `json_exists` | Yes | Not used directly |
| `json_destroy` | Yes | Not used directly |
| `vector_insert` | **Stub — returns error** | No |
| `vector_get` | **Stub — returns error** | No |
| `vector_delete` | **Stub — returns error** | No |
| `vector_search` | **Stub — returns error** | No |
| `vector_exists` | **Stub — returns error** | No |
| `branch_metadata` | **Stub — returns error** | No |
| `branch_update_status` | **Stub — returns error** | No |

**Location**: `crates/engine/src/transaction/context.rs:158-662`

Notable gaps:
- No `state_set` (unconditional write) — only init and CAS
- No `json_list` (document enumeration)
- No `event_get_by_type` (type-filtered scan)
- No version history methods (getv) — by design, these read committed chains

## 6. Summary

| # | Problem | Severity | Type |
|---|---------|----------|------|
| 1 | StateSet bypasses transaction scope (issue #837) | High | Write escapes transaction |
| 2 | Vector writes silently bypass transaction — no error | Medium | Silent non-transactional writes |
| 3 | Branch writes silently bypass transaction — no error | Medium | Silent non-transactional writes |
| 4 | JsonList doesn't see uncommitted JSON documents in transaction | Medium | Inconsistent read visibility |
| 5 | EventGetByType doesn't see uncommitted events in transaction | Medium | Inconsistent read visibility |
| 6 | BranchExport/Import/Validate route inconsistently (catch-all vs explicit) | Low | Inconsistent routing |
