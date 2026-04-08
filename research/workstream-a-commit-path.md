# Workstream A: Commit Path

This document traces the live commit path and identifies where commit
correctness is actually owned.

The goal is not just to describe the happy path.
It is to locate the seams where commit semantics are split across layers,
shadowed by alternate entrypoints, or overstated by comments and API shape.

## Scope

- `crates/engine/src/database/transaction.rs`
- `crates/engine/src/coordinator.rs`
- `crates/concurrency/src/manager.rs`
- `crates/concurrency/src/transaction.rs`
- `crates/concurrency/src/validation.rs`
- `crates/durability/src/wal/writer.rs`
- `crates/engine/src/transaction/pool.rs`

## Authoritative live commit path

For product writes, the live path is:

1. `Database::transaction()` or `Database::commit_transaction()`
2. `Database::begin_transaction()`
3. engine snapshot gating:
   - allocate `txn_id`
   - choose `snapshot_version = storage.version()`
   - wait until `visible_version >= snapshot_version` or hit the safety valve
   - call `coordinator.record_start(txn_id, snapshot_version)`
   - acquire a pooled `TransactionContext`
   - override `txn.start_version` to the gated snapshot
4. user code mutates `TransactionContext`
5. engine admission control:
   - shutdown gate
   - write backpressure
6. `Database::commit_internal()`
7. `TransactionCoordinator::commit_with_wal_arc()`
8. `TransactionManager::commit_inner()`
9. concurrency-layer commit protocol:
   - read-only fast path, or
   - quiesce read lock
   - per-branch commit lock
   - branch deletion check
   - OCC validation or blind-write skip
   - version allocation
   - JSON materialization
   - WAL append
   - storage apply
   - `mark_version_applied()`
10. coordinator bookkeeping:
   - `record_commit()` or `record_abort()`
11. engine post-commit work:
   - schedule flush
   - schedule compaction
12. caller eventually invokes `end_transaction()` or the closure API does it automatically

## Ownership by layer

### Engine database layer

Owns:

- accepting-transactions gate
- transaction start shape used by the live product
- snapshot selection and visible-version wait
- transaction pooling
- write backpressure and L0/memtable stall policy
- flush and compaction scheduling after write commit
- follower write rejection

Does not own:

- OCC validation
- version allocation
- pending-version tracking
- WAL-before-storage ordering
- storage apply ordering

### Coordinator layer

Owns:

- timeout check at commit time
- active transaction metrics
- active snapshot tracking for GC
- commit/abort bookkeeping after manager result

It is a lifecycle wrapper around `TransactionManager`, not the real commit core.

### Concurrency manager

Owns:

- commit serialization on the branch
- quiesce coordination with checkpoint-like readers
- validation
- version allocation
- pending-version tracking
- visible-version advancement
- WAL append ordering relative to storage apply

This is the real commit core.

### TransactionContext

Owns:

- transactional read/write/delete/CAS state
- validation state transition
- batched storage apply
- JSON patch materialization into full-document writes

### WAL writer

Owns:

- actual sync semantics for `Cache`, `Standard`, and `Always`
- segment rotation
- append buffering
- fsync policy

This matters because the concurrency layer talks about "durability", but the
WAL writer is where the real durability contract is decided.

## Confirmed commit-path seams

### 1. Transaction start semantics are split, and the live path shadows the coordinator API

`TransactionCoordinator::start_transaction()` exists, but the live engine path
does not use it.
`Database::begin_transaction()` manually performs:

- `next_txn_id()`
- snapshot selection from `storage.version()`
- visible-version waiting
- `record_start()`
- pooled context acquisition
- manual `start_version` override

`TransactionCoordinator::start_transaction()` is only referenced in
`coordinator.rs` tests.

Why this matters:

- there are two different transaction-start semantics inside the engine
- only one is live
- coordinator-level callers would miss the engine's visible-version gate and
  pooling behavior

Severity:

- `high`

### 2. Snapshot correctness is emergent across engine, storage, and concurrency

The snapshot rule used by the live engine is not owned by one module.
The engine chooses `storage.version()` as the snapshot, then waits for
`coordinator.visible_version()` to catch up before letting the transaction run.

That means:

- storage owns the raw highest applied version it knows about
- concurrency owns the safe visible contiguous prefix
- engine owns the policy that combines them into the actual transaction snapshot

Why this matters:

- direct use of `TransactionContext::with_store()` or coordinator-only paths is
  weaker than the live engine path
- snapshot semantics are real, but they are not encapsulated in one boundary

Severity:

- `high`

### 3. Only one commit entrypoint is live; the others are effectively shadow interfaces

The live engine path uses `TransactionCoordinator::commit_with_wal_arc()`.

Other public commit variants exist:

- `TransactionCoordinator::commit()`
- `TransactionCoordinator::commit_with_version()`
- `TransactionManager::commit()`
- `TransactionManager::commit_with_version()`

In practice:

- `commit_with_wal_arc()` is the live product path
- `commit()` and `commit_with_version()` are used in tests and lower-level APIs
- `commit_with_version()` looks like a coordinated multi-process hook, but is
  not part of the normal runtime spine

Why this matters:

- there are multiple commit surfaces, but not one authoritative public contract
- behavior like narrow WAL lock scope belongs only to one of them

Severity:

- `medium`

### 4. "Durability point" is mode-sensitive, but the concurrency layer treats WAL append as if it were uniformly durable

`TransactionManager::commit_inner()` models the WAL append as the durability
step and logs `"WAL durable"` after append success.

But `WalWriter::append_pre_serialized()` only guarantees:

- `Always`: sync now
- `Standard`: append now, sync later unless the interval safety net fires
- `Cache`: no WAL at all

So the real crash-durability point is not owned by the concurrency layer.
It depends on `DurabilityMode`, which is implemented in `WalWriter`.

Why this matters:

- comments and logs in the commit core overstate durability under `Standard`
- commit semantics are mode-sensitive in a deeper way than the manager API suggests

Severity:

- `high`

### 5. The system explicitly supports "durable but not visible" failures

If WAL append succeeds but `txn.apply_writes()` fails, the manager returns
`CommitError::DurableButNotVisible`.

That means:

- the transaction is already persisted in WAL
- the current process did not make it visible in storage
- restart recovery is expected to apply it later

This is a real architectural contract, not a hypothetical edge case.
The error is then converted to `StrataError::Storage`.

Why this matters:

- the caller sees an error for an operation that may later reappear after restart
- "commit failed" does not always mean "transaction was not committed"
- the recovery model is being used to repair in-process commit failures

Severity:

- `critical`

### 6. Active transaction bookkeeping is external to the transaction object

The transaction object does not own its own lifecycle bookkeeping.

The live flow is:

- engine calls `record_start()`
- coordinator calls `record_commit()` or `record_abort()`
- caller must still call `end_transaction()` to return the context to the pool

There is no `Drop`-based cleanup on `TransactionContext` that reconciles missed
coordinator bookkeeping.

Why this matters:

- low-level or manual misuse can leak active snapshot tracking
- GC safety and shutdown-drain behavior depend on callers using the lifecycle correctly

Severity:

- `high`

### 7. Timeout enforcement is commit-time only and not part of runtime policy configuration

`TransactionCoordinator::enforce_timeout()` runs only when commit is attempted.
There is no background abort, cancellation, or lease expiry mechanism.

Also, the timeout override surface appears to be test-only:

- `set_transaction_timeout()` is only referenced in coordinator tests

Why this matters:

- long-running abandoned transactions are not proactively reaped
- timeout is a commit gate, not a full transaction lifecycle policy

Severity:

- `medium`

### 8. Backpressure is engine-owned, so lower layers can bypass it

Write stall policy and post-commit flush/compaction scheduling live in
`engine::database::transaction`, not in the coordinator or concurrency manager.

Why this matters:

- lower-level commit surfaces do not inherently participate in the same
  operational control loop
- "a commit" does not imply the same runtime behavior across entrypoints

Severity:

- `high`

### 9. Error taxonomy is blurry around pre-WAL and post-validation failures

Two examples:

- JSON materialization failure is returned as `CommitError::WALError`
- `DurableButNotVisible` is collapsed into `StrataError::Storage`

Why this matters:

- the caller-facing error model loses important semantic distinctions
- recovery-relevant failures are flattened into generic storage errors

Severity:

- `medium`

### 10. Read-only fast path is behavioral, not API-driven

The manager checks:

- `txn.is_read_only()`
- `txn.json_writes().is_empty()`

But `txn.is_read_only()` is a behavioral query based on whether mutation sets
are empty, not merely whether `set_read_only(true)` was called.

Why this matters:

- "read-only commit" is a behavior optimization, not a separate isolation mode
- comments or callers that reason about configured read-only mode alone can be misleading

Severity:

- `low`

## Architectural summary

The commit path is credible, but not self-contained.

The live product behavior depends on cross-layer cooperation:

- engine chooses the safe snapshot shape and enforces backpressure
- coordinator owns timeout and GC bookkeeping
- concurrency owns commit serialization and visibility rules
- WAL writer owns the actual durability contract

That means the correctness story is real, but the architectural ownership is
too fragmented.

## Most important follow-up questions

1. Should the engine be the only supported transaction entrypoint?
2. Should coordinator-level `start_transaction()` and `commit()` be demoted,
   narrowed, or deleted if they are not part of the live runtime spine?
3. Should `DurableButNotVisible` become a first-class surfaced outcome instead
   of a generic storage error?
4. Should durability terminology in the concurrency layer be downgraded to
   "WAL appended" unless `DurabilityMode::Always` is active?
5. Should snapshot-start semantics move into a single constructor instead of
   being composed by the engine around `TransactionContext::with_store()`?

## Immediate next reads

- branch delete / fork / merge paths that rely on quiesce or branch locks
- recovery path interaction with version gaps and `DurableButNotVisible`
- any direct lower-layer consumers outside the engine tests
