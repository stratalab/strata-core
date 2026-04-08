# Transactions (OCC): Focused Analysis

This note is derived from the code, not from the existing docs.
Scope for this pass:

- `crates/concurrency/src/manager.rs`
- `crates/concurrency/src/validation.rs`
- `crates/concurrency/src/transaction.rs`
- `crates/concurrency/src/conflict.rs`
- `crates/engine/src/coordinator.rs`
- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/primitives/extensions.rs`
- `crates/engine/src/primitives/kv.rs`
- `crates/engine/src/primitives/event.rs`
- `crates/engine/src/primitives/json/mod.rs`
- `crates/engine/src/transaction/pool.rs`

## Executive view

The transaction core is a real OCC implementation with snapshot isolation, not
serializable isolation.

The runtime model is:

1. begin from a fixed snapshot version
2. buffer reads and writes in `TransactionContext`
3. validate read-set and CAS set at commit
4. write the WAL before storage apply
5. only advance global visibility after storage install completes

The implementation is stronger than the inventory suggests in a few places:

- it separates allocated `version` from externally safe `visible_version`
- it treats storage-apply failure after WAL commit as `DurableButNotVisible`
- it uses per-branch commit locks for the entire validation-to-apply window

But there are also major semantic gaps:

- the guarantee is snapshot isolation, and write skew is intentionally allowed
- the advertised JSON path conflict story is overstated
- the raw concurrency layer and the engine wrapper disagree on how snapshot start versions are chosen
- timeout is enforced only when a transaction tries to commit
- the manual transaction API has no complete abort/cleanup path, so caller misuse can leave GC bookkeeping active indefinitely

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 44 | Optimistic Concurrency Control | True | The commit path buffers mutations and validates only at commit time through `TransactionManager::commit_inner()` and `validate_transaction()` (`crates/concurrency/src/manager.rs`, `crates/concurrency/src/validation.rs`). |
| 45 | Snapshot Isolation | True, with an important split | The engine-facing `Database::begin_transaction()` gives a fixed snapshot view and waits for `visible_version` to catch up before handing the transaction out. The raw `TransactionContext::with_store()` constructor uses `store.version()` directly, so the full snapshot contract is enforced by the engine wrapper, not by the low-level constructor alone (`crates/engine/src/database/transaction.rs`, `crates/concurrency/src/transaction.rs`). |
| 46 | Read-Set Validation | True | Read-set entries record the version seen at read time, and commit fails if the current storage version differs (`crates/concurrency/src/validation.rs`). |
| 47 | Per-Branch Commit Locks | True | Every mutating commit takes a branch-scoped mutex from validation through version allocation, WAL append, and apply. Blind writes still take this lock (`crates/concurrency/src/manager.rs`). |
| 48 | Compare-and-Swap (CAS) | True | `cas()` and `cas_with_read()` are separate from the normal read-set and are validated against `expected_version` at commit (`crates/concurrency/src/transaction.rs`, `crates/concurrency/src/validation.rs`). |
| 49 | JSON Path Conflict Detection | Partially true, and overstated | The lower-level JSON patch path has ancestor/descendant path overlap checks, but only for overlapping writes inside the same transaction. Cross-transaction JSON conflicts are still document-version conflicts. The engine's public JSON primitive path does whole-document read/modify/write through `get()` and `put()`, bypassing the patch-based path entirely (`crates/concurrency/src/conflict.rs`, `crates/concurrency/src/validation.rs`, `crates/engine/src/primitives/json/mod.rs`). |
| 50 | Blind Write Optimization | True | Commits skip OCC validation only when there are no reads, no CAS ops, no JSON snapshots, and no JSON patch writes. The commit still takes the branch lock and still does version/WAL/apply work (`crates/concurrency/src/manager.rs`). |
| 51 | Cross-Primitive Atomicity | True, but the inventory location is misleading | KV, event, and engine JSON primitives all stage their work into one `TransactionContext`, then share a single commit boundary. The trait definitions in `extensions.rs` are not the atomicity mechanism; the common transaction buffer and commit path are (`crates/engine/src/primitives/kv.rs`, `crates/engine/src/primitives/event.rs`, `crates/engine/src/primitives/json/mod.rs`, `crates/engine/src/database/transaction.rs`). |
| 52 | Transaction Timeout | Partially true | There is a real five-minute default timeout, but it is enforced only at commit entry. There is no background reaper and no production config path for changing it beyond the hardcoded default (`crates/engine/src/coordinator.rs`). |
| 53 | Write Buffer Entry Limits | True | Each transaction can be capped by `max_write_buffer_entries`; the limit counts unique put/delete keys plus all CAS operations (`crates/concurrency/src/transaction.rs`, `crates/engine/src/coordinator.rs`, `crates/engine/src/database/transaction.rs`). |
| 54 | Commit Quiesce | True, with a broader role than the inventory says | The quiesce lock is used to drain in-flight commits for stable version-sensitive work such as checkpoints and branch operations, not only for shutdown (`crates/concurrency/src/manager.rs`, `crates/engine/src/coordinator.rs`). |
| 55 | GC Safe Version | Mostly true | The coordinator does not simply track "oldest active snapshot." It maintains a conservative GC-safe lower bound that advances incrementally as transactions finish (`crates/engine/src/coordinator.rs`). |

## Real execution model

### 1. The actual guarantee is snapshot isolation, not serializability

`validation.rs` states the intended contract directly:

- read from a consistent snapshot
- validate only the read-set and CAS set at commit
- allow blind writes to proceed without write-set conflicts
- allow write skew by design

That is standard snapshot isolation with first-committer-wins on reads.
It is not serializable isolation.
If correctness depends on multi-key invariants, the application must force a
shared key dependency with `cas()` or `cas_with_read()`, or use higher-level
coordination.

### 2. Snapshot semantics are split across two layers

At the low level, `TransactionContext::with_store()` sets `start_version` to
`store.version()`.
That is only a counter snapshot.
It does not itself ensure that all writes up to that version are fully applied.

The engine wrapper in `Database::begin_transaction()` adds the missing rule:

- read `storage.version()` as the intended snapshot
- wait until `coordinator.visible_version()` catches up
- record the transaction as active for GC tracking
- override the pooled context's `start_version` with the chosen snapshot

So the real user-facing snapshot contract lives in `Database::begin_transaction()`,
not in the raw transaction constructor.

This split matters because the codebase has more than one way to mint a
`TransactionContext`:

- `Database::begin_transaction()` uses the stronger engine contract
- `TransactionCoordinator::start_transaction()` uses the raw constructor
- `TransactionPool::acquire()` also starts from the raw constructor

In practice, the public engine path is coherent.
Architecturally, the snapshot rule is not centralized in one place.

### 3. Commit has a strict visibility pipeline

The core mutating commit flow in `TransactionManager::commit_inner()` is:

1. fast-path read-only transactions
2. acquire the shared quiesce read lock
3. acquire the per-branch commit lock
4. reject commits on deleting branches
5. skip validation only for true blind writes
6. allocate a commit version
7. materialize JSON patches into normal writes
8. append to the WAL
9. apply writes to storage
10. mark the version applied and advance `visible_version`

Two version counters matter:

- `version`: highest allocated commit version
- `visible_version`: highest version for which every earlier version is fully applied

That separation is the reason snapshot reads can safely wait on visibility
without forcing the whole commit pipeline to serialize behind a single global
lock.

There is also an important failure mode in this design:

- if WAL append succeeds and storage apply fails, commit returns `DurableButNotVisible`
- the data is durable and expected to reappear on restart through WAL recovery
- but the writes are not visible in the current process

That is a deliberate crash-safety choice, and it is part of the transaction
contract today.

### 4. Blind writes are optimized, but not lock-free

Blind writes skip validation only when all of the following are empty:

- `read_set`
- `cas_set`
- `json_snapshot_versions`
- `json_writes`

That is a real OCC optimization for append-only and overwrite workloads.

But blind writes still:

- take the branch commit lock
- allocate a version
- optionally write the WAL
- apply to storage

So this is not "blind writes bypass commit coordination."
It is "blind writes bypass validation."

### 5. CAS is a separate correctness tool, not just a variant of `put`

`cas()` and `cas_with_read()` are treated as distinct operations:

- `cas()` validates only `expected_version`
- `cas_with_read()` first populates the read-set, then also validates `expected_version`

The implementation also prevents mixing ambiguous write modes on the same key:

- a key already in `write_set` or `delete_set` cannot also receive a CAS op
- a key already in `cas_set` cannot also receive a normal put/delete

That separation is important because SI alone allows write skew.
CAS is one of the explicit escape hatches the code expects applications to use
when they need stronger invariants.

### 6. Cross-primitive atomicity is real

The engine does support atomic transactions across multiple primitive families,
but the mechanism is simple:

- KV methods call `TransactionContext::get/put/delete`
- event methods append event data and metadata through the same `put()` path
- engine JSON methods read and rewrite document values through the same `get()` and `put()` path

All of those buffered writes live inside the same `TransactionContext`.
They become visible together only when the single commit path succeeds.

So the atomicity claim is correct for KV, event, and engine JSON.
The important nuance is that `extensions.rs` only defines the traits.
The atomicity comes from the shared write buffer and shared commit pipeline.

### 7. JSON conflict detection is much weaker than the inventory suggests

There are really two JSON transaction models in the codebase.

The lower-level concurrency JSON API in `strata_concurrency`:

- tracks document snapshot versions
- records JSON patch writes
- detects overlapping path writes within the same transaction
- materializes patches into a final document at commit

But even there, concurrent transaction conflicts are still checked at the
document-version level via `validate_json_set()`.
`validate_json_paths()` is only catching overlapping writes inside one
transaction.

The engine's public JSON primitive path is weaker still.
Its `JsonStoreExt for TransactionContext` implementation:

- does `self.get(&key)?`
- deserializes the whole document
- mutates it in memory
- serializes the whole document back
- calls `self.put(key, serialized)?`

That means the public engine JSON path is effectively whole-document OCC,
not path-granular OCC.

### 8. Timeout and GC bookkeeping are coordinator-managed

The coordinator tracks:

- `active_count`
- `active_snapshots: txn_id -> start_version`
- `gc_safe_version`

When a transaction finishes, the safe point advances to:

- `current_version()` if no active snapshots remain
- otherwise `min(active start versions) - 1`

That is a conservative GC lower bound, not a direct "oldest active version"
value.

Timeout is also enforced here, but only at commit entry.
If a transaction remains open and never attempts to commit, the timeout does
not proactively remove it from active bookkeeping.

### 9. Closure API is safe; manual API is easy to misuse

The closure APIs:

- `transaction()`
- `transaction_with_version()`

begin the transaction, run the user closure, commit or abort through the
coordinator, then always return the context to the pool with `end_transaction()`.

The manual API is weaker:

- `begin_transaction()` records the transaction as active
- `commit_transaction()` cleans up only if the caller actually commits
- `end_transaction()` only returns the context to the pool

There is no matching `abort_transaction()` helper on `Database`.
There is also no `Drop`-based coordinator cleanup when a `TransactionContext`
is simply abandoned.

So the manual API depends on caller discipline more than the inventory implies.

## Major architectural gaps in this section

### 1. JSON conflict detection is not path-granular on the public engine path

This is the biggest gap in the OCC section.

The inventory implies fine-grained JSON concurrency control based on
ancestor/descendant path analysis.
The code does not provide that as a user-facing engine guarantee.

What exists:

- document-version conflict detection across transactions
- overlapping-path conflict detection only within one transaction
- a lower-level patch-buffering API in `strata_concurrency`

What the engine public API actually does:

- whole-document read
- whole-document rewrite
- normal KV-style commit through `put()`

So the engine exposes conservative whole-document JSON OCC, not true
cross-transaction path-granular JSON OCC.

### 2. Snapshot-start semantics are not enforced in one place

The code relies on `visible_version` for correct snapshot handoff, but that
logic lives in `Database::begin_transaction()`, not in the low-level
transaction constructor.

That means the engine wrapper is doing semantic repair on top of the raw
concurrency layer.
The public path is correct, but the architectural boundary is weak:

- low-level callers can create snapshot contexts without the visibility wait
- the snapshot guarantee is partly in `strata_concurrency` and partly in `strata_engine`

That split is survivable, but it is a real design seam.

### 3. Timeout is commit-time admission control, not lifecycle enforcement

`TransactionCoordinator::TRANSACTION_TIMEOUT` is real, but its scope is narrow.

The code checks it only when commit begins.
There is no background expiration pass, no forced abort, and no production
configuration path for changing it.

So the inventory statement that timeout "prevents long-running txns from
blocking GC" is too strong.
It prevents expired transactions from committing.
It does not guarantee that abandoned transactions stop influencing GC state.

### 4. Manual transaction cleanup is incomplete

The manual API has an asymmetry:

- there is `begin_transaction()`
- there is `commit_transaction()`
- there is `end_transaction()`
- there is no database-level `abort_transaction()`

Returning a context to the pool is not the same thing as removing it from the
coordinator's active transaction tracking.
That only happens through `record_commit()` or `record_abort()`.

So a caller that begins a transaction and then just drops or pools it without
going through the proper abort bookkeeping can leave:

- `active_count` inflated
- `active_snapshots` retaining the transaction snapshot
- `gc_safe_version` more conservative than necessary

That makes the manual API a real operational footgun.

### 5. The inventory locations for key semantics are misleading

Two examples matter:

- cross-primitive atomicity is attributed to `engine/primitives/extensions.rs`, but the real mechanism is the shared `TransactionContext` plus commit path
- JSON path conflict detection is attributed to `concurrency/conflict.rs`, but that file only handles overlapping writes within a single transaction

This is not just a documentation nit.
It reflects that several important transaction guarantees are emergent behavior
across multiple layers, rather than being owned in one obvious place.

## Bottom line

The OCC engine is solid as a snapshot-isolation transaction core.
The commit ordering, visibility barrier, branch-scoped locking, CAS semantics,
and cross-primitive buffering all make sense.

The weakest part of this section is the semantic boundary around JSON and
transaction lifecycle management.
The engine promises a more fine-grained JSON concurrency story than it actually
delivers, and the manual transaction API leaves too much cleanup correctness in
user hands.
