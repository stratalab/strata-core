# Engine Test Plan

Status: V1 actionable plan (grounded in current `crates/engine` source)

## 1. Purpose And Relationship To The Conformance Contract

`testing-and-conformance-plan.md` is the *aspirational contract* — it describes the
full engine test surface including buckets not yet built (retrieval, orchestration,
entity, clone, command/IPC). This document is the *operational plan*: it maps that
contract onto the code that exists today, states what is already covered, and lists
the concrete tests and harnesses to add, in priority order.

Rule of engagement: write tests against the **implemented** surface; scope tests for
absent surfaces as *pending-on-implementation* so they are not mistaken for gaps.

### 1.1 Implemented vs deferred (the scope boundary)

Module inventory under `src/`: `api`, `branch`, `commit`, `config` (stub),
`control`, `data/{kv,json,event,vector,graph}`, `diagnostics`, `persistence`,
`runtime` (stub), `test_support`, `testkit`.

| Area | Status in crate | Test posture |
| --- | --- | --- |
| Open modes: Cache, DurableLocal | Implemented | Full coverage |
| Read-only mode, IPC, clone/`.strata`, retrieval/search, orchestration/derived-state jobs, entity module | **Absent** | Pending-on-implementation; do **not** write tests against them |
| Branch: create, fork(current/version/time), list, get, delete | Implemented | Full coverage |
| Branch: merge/promote/copy/cherry-pick/restore/revert/diff/conflict-preview | **Absent** | Pending; guard tests must keep them absent |
| Temporal: latest / version / timestamp / history | Implemented (asymmetric per capability) | Full coverage + model tests |
| Data capabilities KV/JSON/event/vector/graph | Implemented | Consolidate into shared conformance + per-capability suites |
| Vector index/artifact/manifest/HNSW | Implemented, large | Already heavy; close determinism/seal gaps |
| Diagnostics: `(class, code, retryable, message, source)` | Implemented (narrow vs contract) | Add registry/redaction/retry tests; flag contract gaps |
| Fake/faulting persistence, deterministic clock | **Absent** | **Build first — gates many P0 tests** |

`config/mod.rs` and `runtime/mod.rs` are doc-only stubs; the runtime/lifecycle
surface lives entirely in `api/database.rs`. Plan tests against `api/`, not against
those stubs.

## 1.2 Implementation Log

Slices landed so far (each fmt + clippy clean under default and `testkit` features,
full suite green):

- **Slice 1 — Diagnostics test foundation (§3.3, §3.6, §7).** `diagnostics/registry.rs`
  — canonical table of all 133 engine error codes by class, plus a
  `debug_assert_registered` in both error constructors so every *exercised*
  `(code, class)` is validated at debug/test time. Four self-checking tests:
  no-duplicates, class-follows-prefix, source-scan completeness (every
  `*.engine.*` literal in `src/` is registered — auto-catches new/typo'd codes),
  and reverse no-dead-entries. Shared helpers in `tests/common/mod.rs`:
  `assert_status`, `assert_no_storage_leak` (walks the source chain),
  `assert_no_secret_leak`.
- **Slice 2 — Storage-error mapping fidelity (§5.1).** Nine `map_storage_error`
  unit tests covering every previously-untested arm (InvalidArgument, Unsupported,
  NotFound, AlreadyExists, retained/timestamp history, MaintenanceRejected,
  InvalidRuntimeState).
- **§11.1 fix (bug, not pin).** `ResourceExhausted` now maps to retryable
  `unavailable.engine.persistence_budget` instead of non-retryable
  `internal.engine.persistence`. New code registered.
- **Slice 3 — Persistence fault seam (§3.1, §5.2).** `persistence/fault.rs` adds a
  `FaultOp` + testkit-gated `StorageFaultKind`/`FaultSchedule`. `StoragePersistence`
  carries a testkit-only fault schedule; `guard_fault` (a no-op in production
  builds) fires an injected `StorageApiError` through the real `map_storage_error`
  at the top of `commit`/`read_row`/`read_history`/`scan_*`/`branch_action`.
  `Database::inject_{commit,read,scan,branch}_fault_for_test` arm it.
  `tests/persistence_faults.rs` proves: commit-fault mapping (resource/ambiguous/
  recovery), read- and scan-fault mapping, fire-once semantics, and that a failed
  commit leaves no visible row (write-before-mutation atomicity). Note: true
  mid-batch partial-apply atomicity needs storage internals not reachable from
  engine; the seam models failure *before* mutation.

- **Slice 4 — Shared capability conformance harness (§6.1).** `tests/capability_conformance.rs`
  — a `CapabilityFixture` trait + a macro generating `capability × invariant`
  tests across KV/JSON/event/vector/graph (30 tests). Shared invariants proven
  uniformly: write-then-read-visible, fork inherit + isolate, space isolation,
  missing-branch → `not_found.engine.branch`, closed-runtime rejection, durable
  reopen. Capability-specific divergences (event's keyless append → seed encoded
  in type+payload; vector/graph container setup) are handled inside each fixture,
  not in the shared bodies — so a new capability cannot drift on the skeleton.

- **Slice 5 — Property/model tests (§8).** Added `proptest` (dev-dep) and two
  model suites. `tests/branch_dag_model.rs`: a random create/fork/delete sequence
  applied to the engine and a reference model in lockstep; after every op the
  model predicts the exact outcome (success or error code) and the full active
  branch set with generations, and the engine must agree — pinning generation
  monotonicity, active-set correctness, the undeletable default, and the
  create/fork/delete error precedence (fork checks dst-duplicate before source;
  delete checks default then last-active-count before existence).
  `tests/temporal_timeline_model.rs`: a KV put/delete commit sequence builds a
  per-key timeline oracle; `get` / `get_at_version` / `get_at` are checked
  against it across the whole version range plus boundaries; and a
  fork-equivalence property (`fork_at_version(V)` child latest == source
  `get_at_version(V)`) links the branch and temporal models.
  - **Finding (§5.3, pinned not fixed):** `get_at_version` is inclusive, and a
    version **past the latest commit reads as `None`** — it does **not** fall
    back to latest the way an `as_of` timestamp does (timestamp past-latest →
    latest). This asymmetry is defensible (a future version does not exist) and
    is now pinned by the timeline model. Flag for a product decision if symmetry
    with `as_of` is wanted.

- **Slice 6 — Seam-free coverage batch.** `tests/runtime_lifecycle.rs` — §4
  closed-handle accessor matrix: every accessor (branch/space/admin/diagnostics
  and all five data capabilities) rejects a closed handle with
  `failed_precondition.engine.runtime_closed`, plus close idempotency. (Note: the
  pre-implementation map was wrong about `admin()` — it calls `require_open()` like
  the rest, so the behavior is uniform, not asymmetric.) `engine_json.rs` — §6.3
  JSON `batch_delete_entries` (positional path/array/whole-doc deletes; empty
  no-op). `engine_graph.rs` — §6.6 graph dangling-endpoint-under-temporal: across
  a node/edge/delete-cascade history, historical neighbor/edge reads track exactly
  the edges whose endpoints are both visible, and **no version ever raises a
  dangling-endpoint corruption** (the fragile `visible_node_or_corruption` path),
  plus a timestamp-temporal read.
  - **Finding (pinned):** a root-path (`$`) entry in `batch_delete_entries`
    deletes the **whole document** (returns `deleted=true`) — it is not the no-op
    the single-path delete is. Pinned by the new test.

- **Slice 7 — Runtime/open breadth (§4).** `tests/runtime_lifecycle.rs`:
  configured non-default branch is the database default (created at gen 1; literal
  `default` absent); reopen with a mismatched default →
  `failed_precondition.engine.default_branch` while the persisted default reopens;
  memory budget below the 1 MiB minimum → `invalid_argument.engine.persistence`
  (no storage leak); durable close reports `durable_synced`. `admin_and_space.rs`:
  `config_value` allowlist (`target`/`created`/`durable`, trimmed + case-insensitive)
  + empty-key `invalid_argument.engine.config_key`; `SpaceService::usage` (all 8
  counts) + `list`.
  - **§11.5 characterized — NOT a bug:** a committed write survives dropping the
    durable handle without `close()` (commits are durable on success). Pinned by
    `dropped_durable_handle_preserves_committed_data`; no `Drop` impl needed.

- **§11.2 resolved (wired, per owner decision).** `admin_status` now maps a
  fail-closed/`Unavailable` control plane → `AdminHealthStatus::Degraded` (it can
  still answer reads and inspection; writes need recovery), while `Missing`/
  `Corrupt` stay `Unhealthy`. The previously-dead `Degraded` variant is now
  reachable; unit-tested in `api/admin.rs`.
- **Slice 8 — Control-plane lifecycle (§5.5 / §9).** Extended the fault seam with
  a skip count (`inject_commit_fault_after_for_test`) so a *later* commit can fail
  while earlier commits succeed. `tests/control_plane_lifecycle.rs` (testkit):
  (1) a branch op whose pending-cleanup commit also fails forces the control plane
  closed — diagnostics report every area `Unavailable`, `admin().health()` is
  `Degraded` (the §11.2 wiring exercised through a real integration path), and
  writes are rejected with `unavailable.engine.control_plane`; (2) the
  recovery-oracle case — a branch creation interrupted after its durable pending
  marker is written is detected on reopen as `data_loss.engine.branch_create_pending`
  rather than silently resuming.

- **Slice 9 — Vector filter scalar matrix (§6.5).** `tests/vector_filter_scalars.rs`
  pins `Eq` filter semantics across scalar types: null matches a JSON null; integer
  and float metadata normalize through `as_f64` (so `5` matches both `5` and `5.0`);
  number comparison is **bit-exact** (`-0.0` does not match a stored `+0.0`); a
  string filter does not match a numeric value; boolean equality is exact.
- **Slice 10 — Error redaction + retry policy (§7).** `tests/error_redaction.rs`:
  every storage fault mapped through a real commit redacts its internals through
  the **whole source chain** (the raw storage error is retained as a source but
  never leaks storage type names or secrets) and carries the documented retry
  policy (`ResourceExhausted`/`AmbiguousCommit`/`Unavailable` retryable;
  `RecoveryDegraded`/`NotFound`/`Conflict` not); read-path and engine-owned
  (validation, not-found, open-surface) errors redact too.
- **Slice 11 — JSON path mutation errors (§6.3).** `engine_json.rs`: descending a
  key into a scalar (or indexing a non-array) → `invalid_argument.engine.json_path_type`;
  an out-of-bounds array index → `invalid_argument.engine.json_path_not_found`;
  `set` on a missing document → `not_found.engine.json_document`.
  - **Finding (pinned):** `set` vs `set_or_create` is **document-level only** —
    the path segments are always auto-created (`set_at_path(..., create_missing =
    true)`), so a missing *key* is created rather than rejected. `json_path_not_found`
    is therefore reachable only via an out-of-bounds array index, never a missing
    key. Pinned by the new test.
- **Slice 12 — KV pagination across the raw-page boundary (§6.2).**
  `tests/kv_pagination.rs`: with 200 keys (> the 64-row internal raw-page clamp),
  a cursor walk of `list_page` returns every key exactly once in order, and a
  scan past 100 interleaved tombstones returns the live keys in order across the
  raw pages — paths smaller suites never exercised.

Decision of record (from the implementing session): §11 items that are latent
bugs are **fixed**, not just pinned (per user direction). Resolved: §11.1
(`ResourceExhausted` remapped), §11.2 (`Unavailable` → `Degraded` wired), §11.5
(drop-without-close is safe, characterized). Remaining two are **decisions, not
clean test fixes** — surface for an owner call: §11.3 unread `EventIndex` rows
(remove the dead write path, or wire `get_by_type` to use the index?); §11.4
vector auto-seal (the seal path is `#[allow(dead_code)]`/testkit-only — is it
meant to be wired into production maintenance? if not, that is a product gap, not
a test gap).

## 2. Current Coverage Snapshot

Existing integration suites (`tests/`): `admin_and_space`, `branch_and_kv`,
`branch_semantics`, `control_plane`, `dependency_guards`, `engine_event`,
`engine_graph`, `engine_json`, `engine_vector` (+ `engine_vector/` subdir),
`persistence_adapter`. Strong unit coverage lives in-module in `persistence/key.rs`,
`persistence/adapter.rs` (error mapping), `control/{bootstrap,records,space}.rs`,
`data/*/record.rs`, `data/event/hash.rs`.

| Subsystem | Coverage today | Headline gaps |
| --- | --- | --- |
| KV | Strong (happy paths, history, isolation, batch, reopen) | Multi-page pagination, corruption path, no-limit value size |
| JSON | Strong | `batch_delete_entries`, path-type errors, index value correctness |
| Event | Strong (incl. partial-batch, chain unit tests) | `verify_chain` vs *real* tampered storage; unused `EventIndex` rows |
| Vector | Very strong (index/artifact/HNSW matrix) | Filter scalar matrix, automatic sealing wiring, watermark accounting |
| Graph | Strong on version-temporal + bindings | Timestamp-temporal variants, dangling endpoints under temporal reads, non-JSON bindings, class-only assertions |
| Branch | Strong on create/fork/delete/generation | Crash-interrupted pending op, multi-cycle generation, pruned-fork rejection |
| Temporal | Good for KV/JSON | Event post-filter divergence, retained-bound eviction, same-timestamp |
| Persistence/commit | Error-mapping unit tests + reopen | Commit atomicity, 4/10 storage classes unmapped, ambiguous/durable-not-visible through real commit |
| Control plane | Strong load-corruption matrix | Bootstrap idempotency, fail-closed → diagnostics, pending lifecycle end-to-end |
| Diagnostics | Per-call asserts | No code registry, no redaction-through-source-chain, no retry matrix |
| Guards | DAG + many removed-surface guards | Follower/disk-cache/tags/maintenance tokens, public-surface snapshot, message-only-assertion ban |
| Crash recovery (product-level) | **Effectively none** | The top gap — see §9 |

## 3. Test Infrastructure To Build First (P0 Prerequisites)

Several high-value tests are **unwritable today** because the engine drives a
concrete `StorageRuntime` with no seam. Build these first; they unblock the commit,
fault-injection, crash-recovery, and error-mapping work.

### 3.1 Faulting / fake persistence seam — **P0, highest leverage**

`StoragePersistence` (`persistence/adapter.rs:51`) is a newtype over a concrete
`StorageRuntime<'static>` with no trait boundary. Add a test-only injection seam so
engine tests can force failures through a real `commit()`/`read()` call:

- Inject: read failure, write-validation failure, write failure before mutation,
  ambiguous commit (`durable_uncertain`), durable-but-not-visible, post-commit/
  maintenance failure, recovery-degraded, lower-layer error, `ResourceExhausted`.
- Two flavors per the contract: (a) a **fake L9** for fast semantic tests; (b) a
  **faulting wrapper** around real storage for boundary tests.
- Preferred shape: a storage-runtime fault hook surfaced through open options, or a
  `pub(crate)` trait at the persistence boundary, kept behind `testkit`. Do **not**
  leak it into the product API (forbidden-shape rule).

Closes: commit-atomicity, ambiguous-commit propagation, storage-error mapping
fidelity through real commits, durable-but-not-visible, recovery degradation.

### 3.2 Deterministic clock / version source — **P0**

No injectable clock or `CommitVersion` source exists in `test_support`/`testkit`.
Provide: fixed clock, step clock, commit-version allocator, timeline fixture
builder, retention-window fixture, timestamp-collision and timestamp-gap fixtures.
No temporal test may depend on wall-clock timing.

Closes: timeline→version model tests, same-timestamp/gap resolution, retained-bound
eviction tests, Event monotonic-timestamp post-filter tests.

### 3.3 Shared status / leak assertion helpers — **P0**

Today every test inlines `class()/code()/retryable()` asserts and the only leak
guard (`common/mod.rs:60`) checks Display text for storage type names only. Add:

- `assert_status(err, class, code, retryable)` — one helper, used everywhere.
- `assert_no_storage_leak(err)` that walks the **source chain** (`Error::source`),
  not just `Display` — the current guard misses the raw `StorageApiError` exposed
  via `source_arc()`.
- `assert_no_secret_leak(err)` — credential/URL/token/env-value scan (forward-looking
  for clone/provider work; cheap to add now).

### 3.4 Shared data-capability conformance harness — **P0/P1**

Per-capability tests are bespoke files. Build one parameterized driver over a small
trait surface `(write, read_latest, read_at_version, read_at_timestamp, history,
delete, list/count, branch_scope, space_scope, error_map)` and run it for every
capability. See §6.1 for what is shareable vs what must stay capability-specific.

### 3.5 Branch DAG + timeline model (proptest) — **P1**

`proptest` is the chosen framework (testing-and-conformance-plan Open Questions §2).
Add it as a dev-dependency. Build the two reference models in §8.

### 3.6 Error-code registry — **P1**

There are ~150 distinct `*.engine.*` code literals across ~20 files with no central
table. Introduce a registry (single source) + a test that every emitted code is
registered and maps to exactly one `EngineErrorClass`. This is contract conformance
req. 1–2 and is currently unmet.

## 4. Runtime / Open / Lifecycle

Current coverage (`admin_and_space.rs`, `control_plane.rs`): cache+durable open
summaries, default-branch bootstrap, close idempotency, admin info/health/metrics/
describe/config, durable reopen preserves control plane.

Required new tests:

- **Open with non-default `with_default_branch`** (e.g. `"main"`): bootstrap via
  `BranchCatalogRecord::root` (gen=1), and `info.default_branch` / `default_branch()`
  reflect it. (Cache covered for selection; durable + info reflection untested.)
- **Mismatched default on reopen** → `failed_precondition.engine.default_branch`
  (`IncompatibleLayout`). Untested end-to-end.
- **`with_memory_budget` too low** → mapped `invalid_argument.engine.persistence`.
  Untested (the budget surface landed in commit `aab82e87`).
- **Closed-handle accessor rejection**: every accessor (`kv/json/vector/event/graph/
  branches/spaces/control_diagnostics`) after `close()` returns
  `failed_precondition.engine.runtime_closed` (`ClosedRuntime`). Only close
  idempotency is tested today.
- **`admin()` on a closed handle succeeds and reports `open=false`** — pin this
  asymmetry deliberately (it only calls `require_open`, not `require_healthy`); decide
  whether `info/health/metrics` on a closed DB is intended.
- **Fail-closed control plane → admin/diagnostics**: drive `terminal_error` via a
  failed branch op, then assert `require_healthy` paths return
  `unavailable.engine.control_plane` and `diagnostics()` returns all-`Unavailable`.
- **`SpaceService::usage`** (all 8 counts) — method entirely untested.
- **`SpaceService::list`, `config_value` allowlist** (`target/created/durable`),
  empty config key → `invalid_argument.engine.config_key`.
- Chunked space deletion (#3574): the one-commit / three-commit boundary at the sweep chunk, and resume after an interrupted sweep.
- **Durable close asserts `durable_synced()==true`** (only `durable()` is asserted).
- **Drop-without-close**: characterize durability on a dropped handle (there is no
  `Drop` impl) — a potential data-loss surprise; pin the behavior.

Apply `assert_no_storage_leak` to every open/load/admin error.

Priority: closed-handle rejection + fail-closed diagnostics + mismatched-default =
**P0**; the rest **P1**.

## 5. Persistence Adapter, Commit, Control Plane

### 5.1 Storage-error mapping fidelity — **P0**

`map_storage_error` (`adapter.rs:897`) has 3 special cases + 8 class arms; only 6 of
10 storage classes have an asserted mapping. Add unit assertions (constructing
`StorageApiError` directly — no fake runtime needed) for the missing arms:
`InvalidArgument`, `NotFound`, `AlreadyExists`, `Unsupported`, `HistoryUnavailable`,
`Internal`, `MaintenanceRejected`/`InvalidRuntimeState` (→ `failed_precondition`),
and the bare `_ =>` fallback.

- **`ResourceExhausted` has no explicit arm** → falls into `_ =>` ⇒
  `internal.engine.persistence`. This is almost certainly wrong (a memory-budget
  exhaustion reported as `internal`). Write the test that pins current behavior **and
  file a scope decision** (see §10) — likely should map to retryable `unavailable`.

### 5.2 Commit atomicity & batch semantics — **P0 (needs §3.1 seam)**

- Multi-mutation `CommitPlan` is all-or-nothing: a mutation that fails mid-batch
  leaves **zero** visible rows. (No way to test without the fault seam.)
- `CommitOutcome.put_count`/`delete_count` match a known plan composition (note
  control-plane space-registration rows are excluded from the counts).
- TTL is always `None` (`to_storage_mutation`) — guard against silent regression.
- Empty `CommitPlan` behavior.
- Ambiguous commit through a real `commit()` surfaces retryable +
  `ambiguous_commit.engine.persistence` (today only the pure mapping fn is tested).

### 5.3 Read selectors & history fallback — **P0/P1, fragile**

- `is_outside_retained_history` and `should_fall_back_to_latest` (`adapter.rs:856–875`)
  depend on **storage reason strings** (substring `"outside retained"`/`"before
  retained"`, and the **exact** string `"timestamp is after latest retained timeline
  history"`). A storage wording change silently flips behavior. Add tests that force
  eviction and read below the bound (→ `Ok(None)`/empty) and read past latest (→
  latest), so the contract is pinned. Flag the string-coupling as a risk (§9).
- `AtVersion` past-latest has **no fallback** (only `AtTimestamp` does) — pin the
  behavior or decide it.
- `scan_prefix_after_version` (active-delta path), `scan_immutable_sources` /
  `PersistenceImmutableSource` round-trip, `read` (drops tombstones) vs `read_row`
  (exposes tombstones), `limit==Some(0)` short-circuit — all untested at this layer.

### 5.4 Key codec & storage-space registry — **P1**

- **Cross-check**: nothing fails if a control `RowClass` id changes without updating
  `CORE_CONTROL_STORAGE_SPACE_IDS` (`records.rs:28`), or vice versa. Add a single test
  asserting the live `RowClass::storage_space_id()` control set equals the persisted
  registry constant.
- Empty product-space (`space_len==0`) encode/decode; control-key builder max-length
  (graph/event keys test 256/1024; control keys don't).
- Extend `row_class_storage_id_for_test` string→id coverage to vector/event classes.

### 5.5 Control plane — **P1 (some P0)**

- **Bootstrap idempotency / wrong-branch**: `bootstrap_new_database` is only invoked
  when `created==true`; a regression invoking it on reopen would double-seed. Pin the
  `bootstrap_or_load` dispatch.
- **Pending-branch lifecycle end-to-end**: only the "pending present ⇒ fail-closed"
  half is covered. Test begin→persist→clear, and `clear_pending_branch_operation`
  rollback on storage error.
- **`fail_closed_after_branch_operation_error` → `require_healthy`/`diagnostics`
  return Unavailable** (P0; shared with §4).
- `next_generation_for_name` monotonicity across create→delete→recreate at this layer.
- Corrupt **pending** branch *record* (vs index) decode path.
- Space `deletion_mutations` "would remove default ⇒ corruption" guard, and the
  None-when-absent path.

## 6. Data Capabilities

### 6.1 Shared conformance suite — **P0/P1**

Drive ONE parameterized suite per capability for the genuinely shared skeleton:

1. Branch resolution + `not_found.engine.branch` + `ClosedRuntime` via
   `require_healthy`.
2. Space isolation + control-row hiding (space-registration rows excluded from
   put/delete counts).
3. Branch fork inheritance + empty-source isolation + COW divergence.
4. Latest / version / timestamp / history (newest-first incl. tombstones).
5. Cursor pagination contract (`limit+1`, `has_more`, strictly-after cursor).
6. Empty/duplicate batch validation (capability-specific codes).
7. Engine-owned error mapping (no storage leak) + `#[non_exhaustive]` class asserts.
8. Durable reopen preserves read surface.

**Divergences that must stay in capability-specific suites** (a naive shared suite is
wrong for these):

- **Temporal model differs.** KV/JSON use real storage selectors
  (`AtVersion`/`AtTimestamp`). **Event has no version read at all** and implements
  every temporal read as a **post-filter on `event.timestamp() <= ts` over Latest
  rows** (`event/service.rs:165,187,259,296,310`). A shared `get_at_version` test has
  no Event analog; a shared as-of test will mis-predict Event.
- **Event timestamps are engine-synthesized + strictly monotonic**; KV/JSON carry
  storage commit timestamps (many rows can share one commit ts).
- **Mutation/identity differ**: KV overwrite+tombstone; JSON sub-document path
  semantics + `document_version` + secondary indexes; Event append-only, no
  delete/update, hash chain.
- **Batch failure differs**: KV/JSON hard-`Err` on bad batch; Event partial-success
  (valid entries commit, invalid become item-level error messages).
- **Value validation differs**: KV none; JSON 4 structural limits enforced on write
  *and* mutation; Event object-root + finite-floats.

### 6.2 KV — **P1**

Already the best-covered. Close: multi-raw-page pagination (`scan`/`list_page` never
cross the 64-row clamp); `next_prefix` 0xFF rollover; `data_loss.engine.kv_value`
corruption path (needs forced missing-value row); `get_versioned` on latest-tombstone;
cursor-before-prefix clamp.

### 6.3 JSON — **P1 (one P0)**

- **`batch_delete_entries` is entirely untested** — the only batch path with no test;
  the `BatchDeleteState` fold (`service.rs:41`) is complex. **P0.**
- **Path-type errors**: `invalid_argument.engine.json_path_type` and
  `json_path_not_found` not asserted at integration level (set into scalar/array
  mismatch; set terminal missing without create).
- Post-mutation value-limit rejection (a `set` that pushes a doc past 16 MiB / depth
  100 / array 1e6 — the `validate()` at `types.rs:541`).
- Index **value correctness** (Numeric order-preserving bytes, Tag lowercasing, Text
  char-boundary truncation incl. multibyte) — today only entry *counts* are checked.
- `get` type-mismatch returning `None`; `create_index` backfill over mixed docs.

### 6.4 Event — **P1 (one P0)**

- **`verify_chain` against real tampered storage** — today only `verify_chain_rows`
  is unit-tested with hand-built rows. Mutate a committed event row / metadata in a
  live durable DB and verify the service surfaces `first_invalid` + corruption. **P0**
  (this is the Event integrity oracle).
- **Unread `EventIndex` rows**: written on append (`service.rs:135`) but never read by
  any query (all type filters post-filter event rows). Decide whether they are dead
  weight; add a consistency test or a scope decision (§10).
- `event_payload_too_large` (16 MiB) + `too long/large to hash`; reverse-direction
  cursor continuation; multi-raw-page `range` (never crosses 4096).
- **Not testable, and no longer claimed:** non-finite float rejection. A
  `serde_json::Value` cannot hold a `NaN` or `±Inf` — `Number::from_f64` returns
  `None`, `Number::deserialize` refuses a raw f64 NaN, and JSON text has no
  non-finite literal — so `EventPayload::new` never saw one and the check that
  looked for them could not fire (#3193). It is deleted; the fact that made it
  unreachable is pinned by `serde_json_cannot_represent_a_non_finite_float`, and
  the coercion callers must guard against is documented on `EventPayload::new`.

### 6.5 Vector — **P1**

Already heavy (index_planner/policy/manifest/artifacts/hnsw/sources). Close:

- **Filter scalar matrix**: only `Eq` exists and is tested for string/i32/bool. Add
  null, f32/f64 (incl. `-0.0` vs `0.0`, integer-vs-float JSON representation), and
  large-int filter scalars — `VectorScalar::matches_json` compares by `to_bits`.
- **Automatic sealing**: `active_delta_seal_threshold=16` + `should_seal_*` exist but
  all sealing in tests is manual (`seal_*_for_test`); the seal fns are
  `#[allow(dead_code)]`. **Verify whether sealing is wired into engine maintenance at
  all** (scope decision §10); if wired, test the auto-seal round.
- Embedding NaN + exact 32768/32769 boundary; `active_delta_count` watermark
  asymmetry (`min` in `active_delta_count_for_refs:2707` vs `max` in
  `active_delta_watermark:60`) — pin the count through a multi-source seal.
- HNSW graph reconstruction determinism across reopen (recall is gated; byte-identical
  graph is not).

### 6.6 Graph — **P1 (one P0)**

- **Dangling/deleted endpoints under temporal reads** — **P0, most fragile spot.**
  `neighbors_at_version` / `get_edge_at_version` can legitimately surface an edge whose
  endpoint node is invisible at that version, and `visible_node_or_corruption`
  (`service.rs:1287`) would raise `data_loss.engine.graph_index` on healthy data. Test
  reading edges/neighbors at a version *before* an endpoint existed.
- **Timestamp-temporal variants untested**: only `get_node_at` (timestamp) is tested;
  `get_edge_at`, `neighbors_at`, `bindings_for_entity_at`, `list_nodes_at`,
  `graph_info_at`, `list_graphs_at` have only `_at_version` coverage.
- **Bindings to non-JSON primitives**: integration uses only
  `GraphBindingPrimitive::Json`; bind+lookup for Kv/Vector/Event/Graph targets (rule
  17 "binding to each capability").
- **Assert code strings, not just class**: graph integration tests assert
  `EngineErrorClass` only (violates CLAUDE.md rule 29 / standards). Add code-string
  assertions for `graph_edge_endpoint`, `not_found.engine.graph`, conflict.
- `limit=0` for every list surface; reverse-edge index consistency asserted directly
  (not only via incoming traversal).

## 7. Diagnostics And Errors

- **Code registry test** (§3.6): every emitted code registered, one class per code.
- **Redaction through the source chain**: `Error::source` exposes the raw
  `StorageApiError` Display, which is never leak-checked today. Add a guard that walks
  the chain for storage type names and secrets, applied across every public boundary.
- **Retry matrix**: `retryable` is a bare `bool`; several public constructors
  hard-code `false` (e.g. `closed_runtime`). Add a per-code retryable golden so retry
  semantics can't drift; verify storage-mapped retryable codes
  (`unavailable.engine.persistence`, `ambiguous_commit.*`, `failed_precondition.*`).
- **Flag (do not silently "test around") the contract-vs-code divergences** as scope
  decisions (§10): `EngineError` is not serializable, not `#[non_exhaustive]`, carries
  no `commit_outcome`/`details`/`hints`/`trace_id`; codes use a fixed `.engine.` area
  segment rather than the contract's domain area; the contract's open/temporal/
  retrieval/clone/IPC code domains are entirely unimplemented.

## 8. Property / Model Tests (proptest)

1. **Branch-DAG model**: model `name → (generation, status, parent, fork_version)`;
   apply random create/fork/delete/recreate. Invariants: generations strictly
   monotonic per name; no active duplicates; sibling-branch key isolation; parent
   `fork_version ≤ source head` at fork; default and last-active never deletable;
   **every error leaves the catalog unchanged (pending cleared)**.
2. **Timeline→version model**: drive put/delete commit sequences, build a reference
   (version→value, timestamp→value with tombstones); fuzz
   `get_at_version`/`get_at`/`list_at` against the oracle — before-earliest (→None),
   after-latest (→latest), EPOCH, MAX, exact-boundary, same-timestamp. Extend across
   KV/JSON/Graph/Event to **expose the Event post-filter divergence** explicitly.
3. **Fork-equivalence property**: a branch forked at version V, read latest, equals
   the source read `AtVersion(V)` for the same keys (links models 1 and 2).
4. **Relationship-graph reachability model** (graph): random bindings/edges preserve
   reachability and dangling/deleted diagnostics against a simple model.
5. **Vector search determinism**: byte-identical results for equal scores; flat==exact
   across mutation/timestamp/branch (partly present — generalize via proptest).

## 9. Fault Injection And Crash-Recovery (Product-Level)

Crash-recovery at the **product** surface is the single biggest gap (consistent with
the storage testing taxonomy's "recovery oracle" being the top class-4 gap). All of
this needs the §3.1 seam and/or a crash/reopen harness.

- **Interrupted branch op**: crash/reopen between `begin_branch_operation` and
  `persist_branch_record` leaves **no half-created branch** (pending index drives
  fail-closed on load). Highest-priority recovery test.
- Committed KV/JSON/event/vector/graph rows survive reopen (mostly covered per
  capability; fold into one matrix).
- Branch DAG + metadata + timeline bounds survive reopen.
- Vector derived state after reopen: stale manifest dropped, artifacts rebuilt or
  marked; fork-inherited refs resolve from parent store.
- Ambiguous-commit / durable-but-not-visible remain visible through engine status
  (not collapsed to generic IO).
- Recovery degradation maps to `data_loss.engine.persistence_recovery` and appears in
  diagnostics.

Fault-injection deterministic cases (via §3.1): persistence read failure; write
failure before mutation; ambiguous commit; durable-but-not-visible; post-commit/
maintenance failure; recovery degraded; `ResourceExhausted`; `LowerLayer` (retryable).

## 10. Removed-Surface & Dependency Guards

Existing `dependency_guards.rs` is strong on the DAG, storage-import containment, and
many deferred-surface bans (`TransactionSession`, `begin_transaction`, merge/cherry/
revert/restore, IPC/Export/Retrieval, deferred graph/vector/control surfaces,
explicit open options). Add the missing bans:

- **Follower mode** (`follower`/`Follower`), **disk-backed cache mode**, **tags/notes**
  (`Tag`/`Note`), **manual maintenance commands** (`compact`/`checkpoint`/`flush` as
  user surface) — all "removed" per charter but unguarded; a regression would pass CI.
- **General process-global-state ban** (`static mut`/`OnceLock`/`lazy_static`) across
  the whole crate (today only `data/vector/` is guarded) — charter hard rule 9.
- **Public-surface (D4) snapshot** guard (e.g. `cargo-public-api`) — new `pub` items
  currently land without a diff catching them (rules 32–34).
- **Message-only-assertion ban**: a guard rejecting `contains("...prose...")` error
  assertions in tests where class/code are available (contract conformance req. 15).

## 11. Open Scope Decisions (need an owner, not just a test)

These are behaviors the tests will *pin*, but which look like latent bugs or
unfinished wiring — resolve the intended behavior before locking a golden:

1. **`ResourceExhausted` → `internal.engine.persistence`** (memory-budget exhaustion
   reported as internal). Likely should be retryable `unavailable`.
2. **`AdminHealthStatus::Degraded` is dead** — defined but never produced
   (`admin_status` maps every non-Healthy to `Unhealthy`). Remove or wire it.
3. **`EventIndex` rows are written but never read** — dead derived rows, or future
   query path? Test consistency or delete.
4. **Vector auto-sealing not wired** — seal fns are `#[allow(dead_code)]`, only
   reachable from testkit. Is active-delta ever sealed in production?
5. **No `Drop` on `Database`** — drop-without-`close()` durability is unspecified.
6. **Diagnostics vs contract**: serializable error / `#[non_exhaustive]` struct /
   commit-outcome-on-error / redaction pass are all absent. Decide V1 scope.
7. **History fallback string-coupling** — pin via storage constants instead of prose
   substrings, or accept the coupling with a guard.

## 12. Risk Register (fragile spots to pin early)

- Reason-string matching in history fallback (`adapter.rs:856–875`).
- `_ =>` catch-all in `map_storage_error` swallowing `ResourceExhausted`.
- Locked-but-uncoordinated registry id constant vs `RowClass` ids.
- `min`/`max` watermark asymmetry in vector active-delta accounting.
- Graph `visible_node_or_corruption` raising corruption on healthy historical reads.
- Event post-filter temporal model diverging from KV/JSON MVCC selectors.
- JSON `batch_set_or_create` first-touch (`apply_set`) vs later raw `set_at_path`.
- Many `.expect("validated …")` length panics trusting upstream validators.

## 13. Prioritization & Phasing

**Phase 0 — harnesses (unblocks everything):** §3.1 faulting/fake persistence, §3.2
deterministic clock/version, §3.3 status/leak helpers, §3.6 code registry.

**Phase 1 — correctness-critical (P0):** storage-error mapping fidelity (§5.1),
commit atomicity + ambiguous commit (§5.2), interrupted-branch-op recovery (§9),
graph dangling-endpoint-under-temporal (§6.6), closed-handle rejection + fail-closed
diagnostics (§4/§5.5), JSON `batch_delete_entries` (§6.3), Event `verify_chain` vs
tampered storage (§6.4), shared capability conformance skeleton (§6.1).

**Phase 2 — breadth (P1):** per-capability gaps (§6.2–6.6), temporal model + branch
DAG model (§8), control-plane lifecycle (§5.5), runtime/open breadth (§4), redaction/
retry matrix (§7), missing removed-surface guards (§10).

**Phase 3 — depth (P2):** golden/fuzz (command/CLI/IPC goldens scoped pending those
surfaces; fuzz the EntityRef/JSON-path/branch-name/recipe parsers that exist),
long-running/adversarial mixed-capability/crash-cycle tests, perf-adjacent multi-page
scale.

## 14. Exit Gates (scoped to the implemented surface)

Engine test conformance for V1 surface freeze (implemented subset) is green when:

1. Shared data-capability conformance passes for KV/JSON/event/vector/graph.
2. Branch (create/fork/delete) + temporal (latest/version/timestamp/history) model
   tests pass, including the Event post-filter divergence.
3. Storage-error mapping is exhaustive across all 10 storage classes, with the
   `ResourceExhausted` decision resolved.
4. Commit atomicity, ambiguous-commit, and durable-but-not-visible are tested through
   a real `commit()` via the fault seam.
5. Interrupted-branch-op and reopen recovery product tests pass.
6. Every emitted error code is registered, one class per code, no storage/secret leak
   through Display **or** source chain.
7. Removed-surface guards (including follower/disk-cache/tags/maintenance/process-
   global/public-surface/message-only) pass.
8. All gated paths run under both default (`localfs`) and `--features testkit`.

Deferred buckets (read-only, IPC, clone, retrieval, orchestration, branch merge/
copy/restore) carry their conformance gates **with** their implementation slices, per
the conformance contract — they are out of scope for this freeze.
