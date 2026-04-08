# Workstream A: Follower Refresh Correctness and Lifecycle

This note audits follower open and `Database::refresh()`.

The core question is not whether follower replay works at all.
It does.
The real question is whether follower refresh is:

- convergent
- composition-stable
- lifecycle-complete

It is not.

## Scope

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/lifecycle.rs`
- `crates/engine/src/database/refresh.rs`
- `crates/durability/src/wal/reader.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/vector/src/recovery.rs`
- `crates/engine/src/search/recovery.rs`
- `crates/executor/src/api/mod.rs`
- `crates/executor/src/tests/lifecycle_regression.rs`
- `crates/engine/src/database/tests.rs`

## Runtime shape

Follower mode is a separate runtime:

- no lock file
- no WAL writer
- no path registry entry
- read-only initial recovery
- explicit polling via `refresh()`

Open-time follower recovery is read-only and uses
`RecoveryCoordinator::recover()` against the WAL plus segment directories.
The resulting `max_txn_id` becomes the follower watermark.

Incremental catch-up later happens only through `Database::refresh()`.

## What the refresh path gets right

There are real strengths in the refresh design:

- it reuses WAL commit order via `txn_id` / commit-version ordering
- it preserves original commit timestamps from the WAL record
- it applies storage mutations with `apply_recovery_atomic()`
- it updates visible version only after search and refresh-hook updates
- it tolerates a partial tail record in the active WAL segment

So this is not a fake feature.
It is a real incremental replay path.

## Confirmed architecture

### 1. Follower refresh is manual polling, not autonomous replication

The follower does not tail continuously.
Callers must invoke `refresh()` explicitly.

That means freshness depends on:

- how often the caller polls
- whether the primary has made WAL bytes visible to readers

Conclusion:

- follower mode is pull replication, not continuous replication

### 2. Visibility is flush-bound, not commit-bound

The primary writes WAL records through `WalWriter`, which buffers writes and
syncs them according to durability mode.

In `Standard` mode, the WAL record may be logically committed but not yet
visible to a follower reading the WAL directory.
The executor regression test explicitly forces `primary.database().flush()`
before follower `refresh()` for exactly this reason.

Conclusion:

- follower visibility is bounded by WAL flush timing, not just commit timing
- in `Standard` mode, refresh lag can persist until background or manual flush

### 3. Refresh progress is not contiguous-success based

`Database::refresh()` does this:

1. read all WAL records with `txn_id > wal_watermark`
2. loop records in order
3. skip bad payloads or storage failures
4. track `max_txn_id` across all seen records
5. store `wal_watermark = max_txn_id`

That is the sharpest correctness problem in this audit.

If one record is skipped because of:

- corrupt payload decode
- storage apply failure

the follower still advances its watermark past that record if a later record
was seen.

That means the skipped record is not retried on the next refresh.

Conclusion:

- refresh is not truly convergent
- local replay failures can create permanent holes

### 4. Refresh ignores `allow_lossy_recovery`

Follower open respects `cfg.allow_lossy_recovery` by configuring
`RecoveryCoordinator`.

But `Database::refresh()` constructs `WalReader::new()` directly and never
applies `with_lossy_recovery()`, even if the follower was opened with
`allow_lossy_recovery = true`.

So follower lifecycle is asymmetric:

- initial open may succeed in lossy mode
- later refresh is always strict

Conclusion:

- follower recovery policy changes after open
- `allow_lossy_recovery` is not consistently honored across the follower lifecycle

### 5. Vector refresh only works for already-known collections

`VectorSubsystem::recover()` registers a `VectorRefreshHook`.
That hook updates vector backends only if a backend for the target collection
already exists in `VectorBackendState`.

If a vector put arrives for an unknown collection, the hook logs and skips it:

- "Skipping vector insert for unknown collection (will be picked up on restart)"

This is not theoretical.
The executor regression test seeds the collection before follower open.
That test shape is necessary because incremental refresh does not create new
vector collection backends after the follower is already running.

Conclusion:

- follower refresh can observe new vectors in existing collections
- follower refresh cannot fully materialize newly-created vector collections
- restart is required for those collections to become live

### 6. Follower composition still changes refresh semantics

Raw `Database::open_follower()` installs only `SearchSubsystem`.
Executor follower open uses the builder path with:

- `VectorSubsystem`
- `SearchSubsystem`

That means:

- raw follower open gets no vector refresh hook
- executor follower open does

Search is also composition-dependent, but in a different way:

- if `SearchSubsystem` was not recovered, `extension::<InvertedIndex>()`
  lazily creates a disabled default index
- refresh then silently skips search updates because `is_enabled() == false`

Conclusion:

- follower refresh semantics depend on which opener composed the runtime
- the same on-disk database can have different follower behavior

### 7. Secondary-state refresh is best-effort and non-authoritative

Search updates inside `refresh()` are best-effort parsing and indexing logic.
Vector refresh hooks are infallible by trait contract and log warnings on local
failures.

But visible version still advances afterward.

So follower readers can converge on KV state while:

- search index state is incomplete
- vector backend state is incomplete

Conclusion:

- secondary indexes are not part of a strict replicated-state contract
- refresh can report success while derived state is locally stale

### 8. `refresh()` is unsynchronized across callers

I did not find any in-flight guard, mutex, or compare-exchange around
`Database::refresh()`.
Multiple threads can call it concurrently on the same follower instance.

That means two refresh calls can read the same watermark and replay the same
records concurrently.

This is an inference from the absence of synchronization, not a reproduced bug.
I did not verify all downstream effects.

Likely risk:

- duplicate replay into storage or derived indexes
- inconsistent watermark movement between competing refresh calls

## Mode-specific summary

### Raw engine follower

- initial recovery: search only
- no vector incremental refresh
- strict `refresh()` even if lossy open was requested

### Executor follower

- initial recovery: vector + search
- vector incremental refresh available
- still subject to watermark-hole and collection-creation gaps

## Main risks

### Risk 1: Permanent divergence after skipped records

This is the top issue.
Refresh advances watermark past skipped records, so the follower can miss data
forever without restart or manual intervention.

### Risk 2: New vector collections do not arrive incrementally

A follower can stay blind to collections created after it opened, even if
later vector inserts for those collections are present in the WAL.

### Risk 3: Follower lag depends on WAL flush behavior

In `Standard` mode, a logically committed transaction may still be invisible
to the follower until the WAL buffer is flushed.

### Risk 4: Open-time and refresh-time policies do not match

Initial follower recovery can be lossy.
Incremental refresh is strict.

### Risk 5: Follower semantics are open-path dependent

Raw follower open and executor follower open are not the same product.

## Architectural judgment

Follower mode is currently a useful read replica mechanism, but not a fully
authoritative convergence model.

It is best described as:

- manual pull replication
- storage-first convergence
- best-effort derived-state refresh

That is a legitimate design if it is stated clearly.
It is a problem if it is presented as a stronger replica contract.

## Recommended follow-up

1. Make watermark advancement contiguous-success based.
2. Honor `allow_lossy_recovery` consistently in `refresh()`, or explicitly
   reject lossy follower refresh.
3. Add incremental vector collection creation handling, not just vector insert
   handling for pre-existing backends.
4. Decide whether follower freshness is allowed to be flush-bound, and document
   that contract clearly if yes.
5. Add a refresh in-flight guard if concurrent refresh calls are supposed to be
   supported.
