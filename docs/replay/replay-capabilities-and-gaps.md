# Replay Capabilities And Gaps

## Purpose

This document audits Strata's current replay, time-travel, and recovery
surface as of the post-`ST7` tree, names the gaps that block the product's
core demo class, and proposes a bounded capability tranche to close those
gaps.

It is grounded in the current code, with concrete file/line references,
not aspirational architecture.

Read this together with:

- [../storage/storage-charter.md](../storage/storage-charter.md)
- [../storage/storage-minimal-surface-implementation-plan.md](../storage/storage-minimal-surface-implementation-plan.md)
- [../engine/engine-pending-items.md](../engine/engine-pending-items.md)
- [../engine/engine-crate-map.md](../engine/engine-crate-map.md)

## Scope

"Replay" in Strata covers six related but distinct concepts. This document
audits all six and treats them as one capability surface because the gaps
in one block the demos that compose across the others.

1. **WAL recovery** — re-applying durably-committed transactions on
   `Database::open`.
2. **Follower replay** — re-applying WAL records on a replica to mirror
   primary state.
3. **MVCC time-travel reads** — reading a key, scan, or document as of a
   specific historical commit version.
4. **Event-stream replay** — re-walking the engine's `EventLog` primitive
   either to verify chain integrity or to derive state from events.
5. **Branch operations** — fork, revert, cherry-pick, diff, merge as
   structured time-travel and selective re-application.
6. **DAG history** — observational log of branch lifecycle events
   (creates, forks, merges, reverts, cherry-picks).

## Current State

### 1. WAL Recovery

Recovery on database open uses the storage-owned coordinator absorbed in
`ST4`.

| Surface | Location | Notes |
|---|---|---|
| `RecoveryCoordinator::recover_typed(...)` | `crates/storage/src/durability/recovery.rs:399` | Typed callback-driven recovery from MANIFEST + snapshot + WAL. |
| `RecoveryCoordinator::recover(...)` | `crates/storage/src/durability/recovery.rs:524` | Untyped variant. |
| `RecoveryCoordinator::recover_into_memory_storage()` | `crates/storage/src/durability/recovery.rs:635` | Convenience wrapper returning a populated `SegmentedStore`. |
| `apply_wal_record_to_memory_storage(...)` | `crates/storage/src/durability/recovery.rs:666` | Single-record application helper. |
| `RecoveryResult { storage, stats }` | `crates/storage/src/durability/recovery.rs:694` | Output of the coordinator. |
| `RecoveryStats { txns_replayed, writes_applied, deletes_applied, ... }` | `crates/storage/src/durability/recovery.rs:720` | Per-recovery telemetry. |

WAL records are committed-only by design (per the segmented-WAL invariant
locked in earlier epics), so `incomplete_txns` and `aborted_txns` always
report zero on recovery. Used by `Database::open`; not user-facing.

### 2. Follower Replay Observer

Used by replicas to observe WAL records as they are applied to local
storage.

| Surface | Location | Notes |
|---|---|---|
| `ReplayInfo { branch_id, commit_version, entry_count, puts, deleted_values }` | `crates/engine/src/database/observers.rs:202` | Per-record context. |
| `ReplayObserver` trait | `crates/engine/src/database/observers.rs:223` | `on_replay(&ReplayInfo)`. Fires only on followers, not primaries. |
| `ReplayObserverRegistry` | `crates/engine/src/database/observers.rs:638` | |
| `Database::replay_observers()` | `crates/engine/src/database/mod.rs:997` | Public registration surface. |

Use cases: index rebuild on replicas, replication metrics. Not a
user-facing "re-execute a transaction with new inputs" feature.

### 3. MVCC Time-Travel Reads

The MVCC machinery is real and bounded by `max_version` end-to-end at the
storage layer.

| Surface | Location | Notes |
|---|---|---|
| `Storage::get_versioned(key, max_version)` | `crates/storage/src/traits.rs:26` | Latest visible value at or before `max_version`. |
| `Storage::get_history(key, limit, before_version)` | `crates/storage/src/traits.rs:33` | Full version chain for one key, newest-first. |
| `Storage::scan_prefix(prefix, max_version)` | `crates/storage/src/traits.rs:41` | Range scan at or before `max_version`. |
| `Storage::current_version()` | `crates/storage/src/traits.rs:47` | Highest committed version. |
| `KvStore::get_versioned(branch_id, space, key)` | `crates/engine/src/primitives/kv.rs:106` | Reads at the transaction's `start_version` (always current at txn begin). |
| `KvStore::get_versioned_direct(...)` | `crates/engine/src/primitives/kv.rs:123` | Bypasses the transaction layer; same version semantics. |
| `KvStore::getv(branch_id, space, key)` | `crates/engine/src/primitives/kv.rs:137` | Returns `VersionedHistory<Value>`. |
| `JsonStore::get_versioned(...)` | `crates/engine/src/primitives/json/mod.rs:431` | Same shape for JSON documents. |
| `SegmentedStore::get_history(...)` | `crates/storage/src/segmented/mod.rs:5223` | Storage-level history walk. |

The infrastructure is end-to-end. The user-facing surface, however, only
exposes "current snapshot" and "full history" — there is no top-level
`Database::view_at(branch, version)` or per-primitive `get_at(version)`
helper that takes an explicit historical version. See
[Gaps](#gaps) section for impact.

### 4. Event Stream Replay

| Surface | Location | Notes |
|---|---|---|
| `EventLog::verify_chain(branch_id, space)` | `crates/engine/src/primitives/event.rs:558` | Re-walks the stored event log; verifies `prev_hash` linkage (SHA-256). Returns `ChainVerification`. |
| `EventLog::len_at(branch_id, space, as_of_ts)` | `crates/engine/src/primitives/event.rs:634` | Count of events as of a wall-clock timestamp. |
| `EventLog::range(...)` and adjacent read helpers | `crates/engine/src/primitives/event.rs` | Range reads over the event log. |
| `ChainVerification { ... }` | `crates/engine/src/semantics/event.rs:25` | Return type for `verify_chain`. |

The event log is hash-chained and verifiable. Events are stored as a
versioned sequence; reading them back yields the canonical sequence.
There is no built-in **event-replay-as-state-machine** harness — events
are stored, not applied to derive state.

### 5. Branch Operations

`BranchService` exposes the structured time-travel and selective
re-application surface. All entry points live in
`crates/engine/src/database/branch_service.rs`.

| Surface | Line | Behavior |
|---|---|---|
| `fork(source, destination)` | 628 | Forks at the source's current state. |
| `fork_with_options(source, destination, options)` | 639 | Strategy-aware fork; **does not accept a historical version**. |
| `merge(source, target)` | 771 | Three-way merge from `merge_base`; LWW default. |
| `merge_with_options(source, target, options)` | 787 | LWW or Strict strategy. |
| `revert(branch, from_version, to_version)` | 915 | Reverts a version range; creates a revert commit. |
| `cherry_pick(source, target, keys)` | 983 | Copies current values of explicit `(space, key)` pairs. |
| `cherry_pick_from_diff(source, target, filter)` | 1069 | Filtered diff replay from `merge_base`; LWW. |
| `diff(source, target)` | 601 | Two-way branch diff. |
| `diff_with_options(...)` | 606 | Diff with options. |
| `diff3(source, target)` | 619 | Three-way diff. |
| `log(branch, limit)` | 1215 | DAG event log for a branch. |
| `ancestors(branch)` | 1239 | Ancestry chain. |
| `merge_base(left, right)` | 1178 | Computes the merge-base. |

These compose MVCC under the hood but are exposed at branch granularity.

### 6. Cherry-Pick (Selective Replay)

Listed under branch operations above. Note the naming asymmetry already
flagged for the SDK design pass: `cherry_pick` takes explicit keys (pure
copy, not history-aware), while `cherry_pick_from_diff` is history-aware
filtered diff. Both record `LineageEdgeRecord::CherryPick`.

### 7. Snapshot / Checkpoint

| Surface | Location | Notes |
|---|---|---|
| `Database::checkpoint()` | `crates/engine/src/database/compaction.rs:85` | Creates a new snapshot; advances watermark. Internal to compaction/durability, not user-facing replay. |

## Gaps

The following gaps are blocking, in approximate order of severity for the
demo class.

### G1. No top-level "view at version" API

**Severity: high.**

`Storage::get_versioned(key, max_version)` and
`Storage::scan_prefix(prefix, max_version)` exist at the storage trait,
but no public engine-level surface lets a caller say "give me a read
handle into branch B at version V."

The transaction system always reads at the txn's `start_version`, which
is captured at `begin_transaction` and is always the current visible
version. There is no `start_version` override on the public path.

Impact: blocks the MVCC-scrubber demo class, including the knowledge-
graph evolution scrubber that is the most visually compelling artifact
Strata can produce.

### G2. No `fork_at_version`

**Severity: high.**

`BranchService::fork_with_options` does not accept a source version. All
forks are taken from the source's current state.

Impact: blocks "fork an agent's memory at step 5 and replay with a
different prompt" — the core agent-replay demo. Also blocks "show me the
state at version 100, then fork from there to explore an alternate
timeline" interactions in the `strata ui` time-travel pane.

### G3. No read-only transaction at historical version

**Severity: high.**

Pairs with G1. There is no `Database::transaction_at(branch, version,
|txn| ...)` that opens a read-only transaction with `start_version` set
to a historical commit version.

Impact: any caller that wants multi-key snapshot consistency at a
historical version has to drop down to direct `Storage` calls and lose
the transaction-layer ergonomics.

### G4. No event-replay-as-state-machine

**Severity: medium.**

`EventLog::verify_chain` walks the event log to validate hashes, but
there is no `EventLog::replay(branch, from_version, handler)` that
drives a state machine through the event sequence.

Impact: the more general "given this event log, what is the resulting
state" projection is unavailable. Practical workaround for v1 demos: use
G1 + G2 to fork at a version and let the agent re-execute from there,
without needing event-level replay.

### G5. No replay with input substitution

**Severity: high (for the agent-replay demo specifically).**

There is no harness that replays a recorded sequence of inputs against a
state, allowing one or more inputs to be substituted mid-stream.

Impact: the "production agent failed at step 47, scrub backwards, swap
the prompt at step 33, see the divergent outcome" demo cannot be done
via pure event replay. The achievable v1 substitute is fork-at-version
plus letting the agent run from there with a new prompt — which is
weaker but visibly equivalent for end-user demos.

### G6. No high-level scan-at-version

**Severity: medium.**

`Storage::scan_prefix(prefix, max_version)` exists. There is no
`KvStore::list_at(...)` / `JsonStore::list_at(...)` user-facing wrapper.

Impact: pairs with G1 — a `view_at` handle would naturally include
scan/list helpers, which closes this gap as a side effect.

## Demo Implications

The demos discussed in product strategy that depend on these gaps:

| Demo | Required gaps closed |
|---|---|
| MVCC scrubber on knowledge graph | G1 |
| Time-travel debug an agent failure (scrub + view) | G1, G3 |
| Fork an agent's memory mid-conversation, replay with new prompt | G1, G2 |
| Diff two agents' brains at the same historical version | G1, G6 |
| 100 sandboxed agents converging | none of the above (uses current branch fork + CRDT, both extant) |
| Knowledge graph evolving over MVCC time | G1 |
| Branch comparison view side-by-side at version V | G1, G6 |

Closing G1 + G2 + G3 unlocks the entire scrubber and fork-replay demo
class. G4 and G5 are deferred research-grade items that the v1 demos do
not require.

## Recommended Capability Tranche

A bounded epic to close G1 / G2 / G3 / G6 in a single coherent slice.
G4 and G5 are explicitly out of scope.

### Proposed API surface

#### Engine surface (canonical)

```rust
impl Database {
    /// Open a read-only handle into a branch at a historical commit version.
    ///
    /// All reads through the returned handle are bounded by `version`.
    /// Returns an error if `version` is below the branch's GC safe point
    /// or above its current visible version.
    pub fn view_at(
        &self,
        branch_id: &BranchId,
        version: CommitVersion,
    ) -> StrataResult<HistoricalView<'_>>;

    /// Run a read-only transaction with `start_version` pinned to a
    /// historical commit version on the given branch.
    pub fn transaction_at<F, T>(
        &self,
        branch_id: &BranchId,
        version: CommitVersion,
        f: F,
    ) -> StrataResult<T>
    where
        F: FnOnce(&HistoricalTxn) -> StrataResult<T>;
}

/// Read-only view bound to a (branch, version) pair.
pub struct HistoricalView<'db> { /* ... */ }

impl HistoricalView<'_> {
    pub fn version(&self) -> CommitVersion;
    pub fn branch_id(&self) -> BranchId;

    pub fn kv_get(&self, space: &str, key: &str) -> StrataResult<Option<Value>>;
    pub fn kv_list(&self, space: &str, prefix: Option<&str>) -> StrataResult<Vec<String>>;
    pub fn kv_scan(&self, space: &str, prefix: Option<&str>) -> StrataResult<Vec<(String, Value)>>;

    pub fn json_get(&self, space: &str, doc_id: &str) -> StrataResult<Option<JsonValue>>;
    pub fn json_get_path(&self, space: &str, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>>;
    pub fn json_list(&self, space: &str, prefix: Option<&str>) -> StrataResult<Vec<String>>;

    pub fn event_get(&self, space: &str, sequence: u64) -> StrataResult<Option<Event>>;
    pub fn event_range(&self, space: &str, from: u64, to: u64) -> StrataResult<Vec<Event>>;
    pub fn event_len(&self, space: &str) -> StrataResult<u64>;

    // Vector / Graph / Search analogues using the same version bound.
}
```

`HistoricalView` is the natural place to land G1 and G6 together. G3 is
the same machinery exposed through the transaction surface for callers
that want multi-key snapshot semantics at a historical version.

#### Branch service surface

```rust
impl BranchService {
    /// Fork a branch from a specific historical commit version of the source.
    ///
    /// Equivalent to `fork_with_options` but with `source_version` set.
    /// Errors if `source_version` is below GC safe point or unreachable
    /// from the source branch's lineage.
    pub fn fork_at(
        &self,
        source: &str,
        destination: &str,
        source_version: CommitVersion,
    ) -> StrataResult<ForkInfo>;
}

// Or equivalently, extend ForkOptions:
pub struct ForkOptions {
    pub message: Option<String>,
    pub creator: Option<String>,
    pub source_version: Option<CommitVersion>, // None = current; Some(v) = fork at v
}
```

The standalone `fork_at` function is preferred for SDK ergonomics; the
`ForkOptions` extension is the engine-internal mechanism.

### Sequencing

Three small slices, executed in order. Each must pass its own gate
before the next begins.

#### TT1 — Engine-level historical reads (G1 + G6)

Add `Database::view_at` and `HistoricalView`. Wire it through to
`Storage::get_versioned` / `scan_prefix` with the explicit version. Add
unit tests for: read at version on a branch with multiple commits, read
before GC safe point fails cleanly, read above current version fails
cleanly, scan/list at version returns the right keyspace.

Acceptance:
- `Database::view_at` exists and returns a valid handle for any
  reachable version.
- `HistoricalView` exposes per-primitive read helpers that match the
  current `KvStore` / `JsonStore` / `EventLog` shapes but bounded by
  version.
- Tests cover correctness of historical reads under branched MVCC.

#### TT2 — Read-only historical transactions (G3)

Add `Database::transaction_at`. This is largely a thin wrapper over G1
that opens a `TransactionContext` with `start_version` set to the
historical version. The transaction must be read-only — any write
attempt errors.

Acceptance:
- `Database::transaction_at` exists.
- Multi-key reads at a historical version observe the same snapshot.
- Writes inside a `transaction_at` callback are rejected with a clear
  error.

#### TT3 — Fork at historical version (G2)

Extend `ForkOptions` with `source_version: Option<CommitVersion>`. Add
public helper `BranchService::fork_at`. Update lineage edge writing to
record the source version in the new fork's `LineageEdgeRecord::Fork`
metadata.

Acceptance:
- `fork_at` produces a new branch initialized from `source` at the
  specified version.
- Lineage records include the source version.
- DAG `log` correctly attributes the fork to the source version.
- Reads on the new branch see exactly the state of `source` at
  `source_version`.

## Constraints

### 1. No on-disk format change

Adding `view_at` and `transaction_at` is a read-only surface. No new
storage formats, no new WAL records, no manifest changes.

### 2. No new public Database surface beyond what is named here

The proposed API surface is intentionally narrow. Do not bundle
unrelated additions ("while we are touching transactions, also add X")
into this tranche.

### 3. Honor the GC safe point

The engine already exposes `gc_safe_version` through the transaction
coordinator. Reads below this version are not supported by storage
because compaction may have collected the data. `view_at` and
`fork_at` must error cleanly with a `HistoryTrimmed` error rather than
returning partial state.

### 4. No event-replay engine in this tranche

G4 and G5 are explicitly out of scope. The constrained version of the
agent-replay demo (fork at version + agent re-runs from there) is
achievable with G1 + G2 + G3 alone. The general event-replay engine is
a separate, multi-month research item that should not be conflated with
this surface work.

### 5. Existing branch semantics unchanged

`fork`, `fork_with_options`, `merge`, `revert`, `cherry_pick` keep
their current behavior. `fork_at` is additive.

## Verification Gates

Per slice, at minimum:

- `cargo test -p strata-engine`
- `cargo test -p strata-storage`
- `cargo test -p stratadb --test engine`
- `cargo test -p stratadb --test integration`

Tranche-specific tests:

- TT1: `historical_reads_at_version_match_storage_layer`,
  `historical_reads_below_gc_safe_point_error`,
  `historical_scan_returns_keyspace_as_of_version`.
- TT2: `transaction_at_observes_consistent_historical_snapshot`,
  `transaction_at_rejects_writes`.
- TT3: `fork_at_initializes_destination_at_source_version`,
  `fork_at_records_source_version_in_lineage`,
  `fork_at_below_gc_safe_point_errors`.

## Definition Of Done

The tranche is complete when all of the following are true:

1. `Database::view_at(branch, version)` exists and returns a
   `HistoricalView` with the per-primitive read helpers listed above.
2. `Database::transaction_at(branch, version, f)` exists, is read-only,
   and provides multi-key snapshot consistency at the historical version.
3. `BranchService::fork_at(source, destination, version)` exists and
   records the source version in the resulting fork's lineage.
4. `HistoryTrimmed` and equivalent errors fire cleanly when callers
   request a version below the GC safe point.
5. Test coverage exists for each slice as listed under Verification
   Gates.
6. No on-disk format has changed.
7. The `strata ui` MVCC scrubber demo can be built against the new
   surface end-to-end without further engine work.

## Out Of Scope

Recorded here so future work does not get pulled into this tranche:

- **Event-replay engine** (G4 / G5) — separate research-grade effort.
  Will require an event-handler abstraction, deterministic replay
  harness, and input substitution semantics. Decide scope after the
  demo class lands.
- **Branch CRDT-replicated time travel** — depends on the StrataHub
  CRDT layer, which is its own future epic.
- **Visualization-side caching for graph viz scrub performance** — a UI
  concern, addressed in `strata ui` design work.
- **Replay of branch operations (DAG replay)** — the DAG today is
  observational, not replayable. Lifting this would require
  significant additional design and is not justified by current demo
  needs.

## Cross-References

- WAL recovery internals: [../storage/storage-charter.md](../storage/storage-charter.md)
- MVCC and segmented storage: storage and engine crate maps
- Branch service surface: `crates/engine/src/database/branch_service.rs`
- Existing engine pending items: [../engine/engine-pending-items.md](../engine/engine-pending-items.md)
