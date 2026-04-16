# Durability and Lifecycle: Tranche 3 Implementation Epics

## Context

Tranche 2 creates the engine-owned runtime spine: one `open_runtime`, per-database subsystem registries, `BranchService`, observer traits, and session cleanup. Tranche 3 hardens the durability and recovery behavior that now sits behind that runtime spine.

The engine can commit and recover data today, but several durability paths are still split, incomplete, or fail-open:

| Area | Current shape | Problem |
|------|---------------|---------|
| WAL sync bookkeeping | Counters can reset before `sync_all` succeeds | Active data-loss hole on fsync failure |
| Flush thread | Two implementations | `.meta` timing and future fixes can drift |
| WAL writer errors | Sync failure logged and swallowed | Writer keeps accepting commits after durability failure |
| Commit outcome | `DurableButNotVisible` becomes generic storage error | Callers cannot distinguish recovery handoff from ordinary failure |
| Follower refresh | Single watermark advances before hooks prove success | Derived indexes can silently fall behind |
| Refresh hooks | Infallible API | No way to block on secondary maintenance failure |
| Recovery coordinator | WAL-only | Checkpoints are written but not loaded |
| Directory layout | Ad-hoc path construction | Engine, coordinator, and tests can disagree |
| Shutdown | Timeout logs warning and proceeds | Freeze hooks can run while transactions are live |
| Codec validation | Side check in open path | WAL/checkpoint/MANIFEST do not share one validation point |
| Config surface | Durability knobs unclassified | Callers cannot tell open-time from live-safe settings |

**Root causes:**

1. The WAL sync optimization broke the three-phase contract: snapshot, fsync, then reset counters.
2. Recovery stopped at the M2 WAL-only path; snapshot loading was never brought onto the shipped path.
3. Follower refresh treats "received" and "fully applied" as one watermark.
4. Shutdown exists, but it is not an authoritative close barrier.
5. Durability configuration and codec identity are not validated through one control surface.

**Reference docs:**

- `docs/requirements/durability-recovery-requirements.md`
- `docs/design/architecture-cleanup/durability-recovery-scope.md`
- `docs/design/execution/tranche-3-durability.md`
- `docs/design/architecture-cleanup/runtime-consolidation-scope.md`
- `docs/design/architecture-cleanup/non-regression-requirements.md`

---

## Epic 1: WAL Sync Correctness

**Goal:** Fix the counter-reset data-loss bug, introduce a correct three-phase WAL sync API, and delete the duplicated flush-thread implementation.

**Why first:** This is the only tranche 3 item that closes an active data-loss hole. Halt/resume, shutdown, and recovery hardening all depend on correct sync bookkeeping.

### Changes

**1. Pre-flight and characterization tests** (~150-250 lines)

- Confirm Tranche 2 lifecycle hooks and observer traits are available.
- Confirm follower-mode product decision remains approved for refresh semantics.
- Inventory committed `.chk` fixtures that recovery must keep consumable.
- Characterize current `prepare_background_sync` behavior.
- Characterize both flush-thread implementations.
- Characterize `maybe_sync` behavior when unsynced data exists and when no sync is needed.

The characterization tests should prove that the fix changes the known-broken behavior, not simply encode the new design.

**2. `SyncHandle` and `SyncSnapshot` in `crates/durability/src/wal/writer.rs`** (~80 lines)

```rust
pub struct SyncHandle {
    fd: File,
    snapshot: SyncSnapshot,
}

struct SyncSnapshot {
    bytes_since_sync: u64,
    writes_since_sync: u64,
    last_sync_time: Instant,
    has_unsynced_data: bool,
}
```

- `SyncHandle` is move-only and `#[must_use]`.
- Dropping an uncommitted handle is a test failure or debug panic.
- The handle carries the snapshot needed to commit or abort the background sync.

**3. Three-phase sync API on `WalWriter`** (~120 lines)

Add:

- `begin_background_sync(&mut self) -> io::Result<Option<SyncHandle>>`
- `commit_background_sync(&mut self, handle: SyncHandle) -> io::Result<()>`
- `abort_background_sync(&mut self, handle: SyncHandle, error: io::Error)`

Rules:

- `begin_background_sync` snapshots counters and sets `sync_in_flight = true`.
- `begin_background_sync` does not clear counters or `has_unsynced_data`.
- `commit_background_sync` runs only after `sync_all` succeeds.
- `abort_background_sync` preserves counters and unsynced state.
- `abort_background_sync` records `bg_error: Option<BgError>` for Epic 2.
- `maybe_sync` skips cleanly while `sync_in_flight` is true.

Delete:

- `prepare_background_sync`
- premature `reset_sync_counters` usage

**4. Rewrite the primary flush thread** (~80 lines)

The single flush loop becomes:

```text
lock writer
begin_background_sync
unlock writer
sync_all outside lock
lock writer
commit or abort handle
write .meta sidecar only after successful commit
```

Add a `#[cfg(test)]` sync-failure injection point at the `sync_all` call site.

**5. Delete duplicated flush loop** (~40 lines removed)

- Delete the second flush loop in `crates/engine/src/database/mod.rs`.
- `set_durability_mode` stops the existing loop and calls the shared `spawn_wal_flush_thread`.
- Grep for flush-loop structure should find one production implementation.

### Verification

- Test: injected `sync_all` failure preserves `has_unsynced_data` and counters.
- Test: next scheduled sync retries the same unsynced data.
- Test: concurrent append during in-flight background sync does not force redundant inline fsync.
- Test: `prepare_background_sync` no longer exists.
- Test: exactly one flush-thread implementation remains.
- Benchmark: WAL latency, YCSB, redb.

### Effort: 3-5 days

---

## Epic 2: WAL Writer Halt and Resume

**Goal:** Make the WAL writer fail closed on fsync failure and surface durable-but-not-visible as a first-class commit outcome.

**Why:** Epic 1 records background sync failure. Epic 2 turns that signal into an operational state: stop accepting new commits, report health, and require explicit resume.

### Changes

**1. `WalWriterHealth` and database health field** (~80 lines)

```rust
pub enum WalWriterHealth {
    Healthy,
    Halted {
        reason: String,
        first_observed_at: SystemTime,
        failed_sync_count: u64,
    },
}
```

- Add `wal_writer_health: Mutex<WalWriterHealth>` on `Database`.
- The health state is per database instance, not process-global.

**2. Halt behavior in the flush thread** (~60 lines)

After `abort_background_sync` records `bg_error`:

- Flip `accepting_transactions` to false.
- Set health to `Halted`.
- Increment failed sync count.
- Keep the flush thread alive so explicit resume can retry.

**3. Public health and resume API** (~100 lines)

Add:

- `Database::wal_writer_health(&self) -> WalWriterHealth`
- `Database::resume_wal_writer(&self, confirm_reason: &str) -> StrataResult<()>`

Resume:

- Attempts a sync.
- Clears `bg_error` only if sync succeeds.
- Returns health to `Healthy` only if sync succeeds.
- Leaves writer halted if the underlying issue remains.

**4. Public error variants** (~60 lines)

Add structured public errors:

- `StrataError::WriterHalted { reason, first_observed_at }`
- `StrataError::DurableButNotVisible { txn_id: TxnId, commit_version: CommitVersion }`

Rules:

- Do not collapse these into `Storage`.
- Keep these aligned with the error taxonomy scope.

**5. Commit-path checks** (~80 lines)

- `Database::transaction`
- `Transaction::commit`
- `commit_internal`

All must return `WriterHalted` when the writer is halted.

**6. Stop flattening `DurableButNotVisible`** (~60 lines)

Change the concurrency error shape from string-only to structured fields if needed. The conversion to `StrataError` must preserve `txn_id` and `commit_version`.

### Verification

- Test: first injected sync failure halts writer.
- Test: repeated sync failures increment `failed_sync_count`.
- Test: commits after halt return `WriterHalted`.
- Test: resume succeeds after clearing the fault and commits work again.
- Test: resume fails while the underlying fault remains and health stays halted.
- Test: storage-apply failure after WAL append returns `DurableButNotVisible`.
- Test: crash/reopen after `DurableButNotVisible` makes the durable write visible.
- Benchmark: WAL latency, YCSB, redb.

### Effort: 3-5 days

---

## Epic 3: Follower Refresh Convergence

**Goal:** Make follower refresh contiguous, fallible, single-flight, and observable.

**Why:** Followers currently advance a single watermark before proving that storage apply and secondary maintenance succeeded. That can make a follower claim progress while derived state is behind.

### Changes

**1. `ContiguousWatermark` and refresh status types** (~250 lines)

Add:

- `ContiguousWatermark`
- `RefreshOutcome`
- `BlockedTxn`
- `BlockReason`
- `FollowerStatus`
- `AdvanceError`
- `UnblockError`

Required shape:

```rust
pub enum RefreshOutcome {
    CaughtUp { applied: usize, applied_through: TxnId },
    Stuck { applied: usize, applied_through: TxnId, blocked_at: BlockedTxn },
    NoProgress { applied_through: TxnId, blocked_at: BlockedTxn },
}
```

- Mark `RefreshOutcome` `#[must_use]`.
- `ContiguousWatermark` is the only type allowed to advance the applied watermark.
- `BlockReason` must distinguish decode, codec, storage apply, and secondary-index failures.

**2. Split received and applied watermarks** (~80 lines)

Replace the single `wal_watermark` with:

- `received_watermark`: max decoded/received WAL txn id.
- `applied_watermark`: contiguous fully applied watermark.

Reads and visibility honor `applied_watermark`. `received_watermark` is telemetry.

**3. Make `RefreshHook::apply_refresh` fallible** (~100 lines)

Change:

```rust
fn apply_refresh(&self, puts: &[(Key, Value)], deletes: &[(Key, Vec<u8>)]);
```

to:

```rust
fn apply_refresh(
    &self,
    puts: &[(Key, Value)],
    deletes: &[(Key, Vec<u8>)],
) -> Result<(), RefreshHookError>;
```

Update vector, search, and graph implementations in the same PR anywhere they own correctness-critical replay state. A fallible hook signature cannot land with stale implementors.

**4. `RefreshGate`** (~50 lines)

Add one single-flight gate per follower database. `Database::refresh()` acquires it before replay begins and releases it after producing the final `RefreshOutcome`.

**5. Rewrite `Database::refresh()`** (~250 lines net)

New record loop:

```text
acquire RefreshGate
if already blocked, return NoProgress
read records after received_watermark
decode record
apply storage atomically
apply fallible refresh hooks
advance ContiguousWatermark
advance visible version
fire ReplayObserver
return CaughtUp or Stuck
```

Rules:

- Do not advance `applied_watermark` before hook success.
- Do not fire `ReplayObserver` before contiguous advancement.
- Do not continue past a blocked record.
- Do not assume `received_watermark == applied_watermark`.

**6. Public follower APIs** (~80 lines)

Add:

- `Database::follower_status(&self) -> FollowerStatus`
- `Database::admin_skip_blocked_record(&self, txn_id: TxnId, reason: &str) -> Result<(), UnblockError>`

Admin skip:

- Requires exact blocked txn id.
- Fails closed on mismatch.
- Writes audit to tracing target `strata::follower::audit`.
- Writes durable audit to `<data_dir>/follower_audit.log`.

**7. Update refresh callers** (~100 lines)

Every in-tree `refresh()` caller must pattern-match `RefreshOutcome`. `#[must_use]` should catch ignored results.

### Verification

- Test: happy-path refresh returns `CaughtUp`.
- Test: decode error blocks at txn N; records after N do not advance `applied_watermark`.
- Test: storage apply failure blocks.
- Test: refresh hook failure blocks with secondary-index reason.
- Test: admin skip unblocks only the exact txn id and writes both audit sinks.
- Test: wrong skip id returns `UnblockError::Mismatch`.
- Test: already-blocked refresh returns `NoProgress`.
- Test: concurrent refresh calls serialize and replay each record once.
- Test: blocked follower can have `received_watermark > applied_watermark`.
- Test: `ReplayObserver` fires only for applied records.
- Benchmark: YCSB follower path, follower refresh latency, BEIR if vector/search hook behavior changes.

### Effort: 1-2 weeks

---

## Epic 4: DatabaseLayout and Snapshot Install

**Goal:** Introduce one canonical directory layout type and a snapshot install path that the recovery coordinator can call, with retention-complete payload fidelity for tombstones and TTL.

**Why:** Epic 5 needs a layout object shared by engine, coordinator, and tests. It also needs a way to install decoded checkpoint entries into `SegmentedStore`. Tranche 4 now depends on Tranche 3 to preserve tombstone and TTL state faithfully through checkpoints and restart recovery, not just branch/space/type identity.

**Implementation split:**

- **PR 1:** layout and coordinator cleanup only. Land `DatabaseLayout`, move open/recovery call sites to it, and collapse `RecoveryCoordinator` construction onto one layout-based shape.
- **PR 2:** storage-side snapshot install contract plus checkpoint payload fixes/tests so branch, space, type, raw key bytes, per-entry MVCC metadata, tombstones, and TTL are explicit instead of inferred or dropped.
- **Non-goal for Epic 4:** do not change production recovery behavior, callback shape, or codec validation flow. Snapshot loading and delta-WAL replay remain Epic 5.
- **Compatibility constraint:** snapshot payload and storage install semantics must be lossless for branch/space/type identity, raw user keys where applicable, tombstone state, TTL state, and the MVCC version/timestamp needed to reinstall each logical entry. Recovery artifacts taken together must also preserve fork-frontier, inherited-layer, and reachability state needed for restart-correct branch retention semantics.

### Changes

**1. `DatabaseLayout` type** (~80 lines)

```rust
pub struct DatabaseLayout {
    pub root: PathBuf,
    pub wal_dir: PathBuf,
    pub snapshots_dir: PathBuf,
    pub segments_dir: PathBuf,
    pub manifest_path: PathBuf,
}
```

- Constructor: `DatabaseLayout::from_root(root)`.
- Directory names match the existing on-disk convention: `wal/`, `snapshots/`, `segments/`, and root `MANIFEST`.
- Place the type where both engine and `strata_concurrency::RecoveryCoordinator` can consume it without creating a crate cycle.

**2. Use `DatabaseLayout` in open** (~80 lines)

- Replace ad-hoc path construction in engine open code.
- Pass layout through recovery and storage setup.
- Update test harnesses to use the same type.

**3. Update `RecoveryCoordinator::new`** (~100 lines)

Change from separate `wal_dir` plus optional segment directory builder to:

```rust
RecoveryCoordinator::new(layout: DatabaseLayout, write_buffer_size: usize)
```

- Delete the optional `with_segments` path once all call sites move.
- Delete `with_snapshot_path()` once tests and stubs no longer need it.
- Epic 4 does not thread codec identity through the coordinator constructor yet. That validation moves in Epic 5 when recovery planning reads `MANIFEST`.

**4. Fix and characterize the checkpoint payload** (~140-220 lines)

- Update checkpoint DTOs so branch ID, namespace space, KV-vs-Graph type identity, raw KV/Graph user-key bytes, per-entry MVCC metadata, tombstone state, and TTL state are serialized explicitly.
- Add tests that prove checkpoint payload/install semantics are retention-complete for the branch-aware storage retention work in Tranche 4.
- Cover at least:
  - branch/space/type identity is preserved explicitly in checkpoint DTOs
  - raw KV/Graph user-key bytes survive checkpoint serialization
  - event/branch/vector entries carry the MVCC version/timestamp Epic 5 will need to reinstall them
  - vector snapshot entries preserve the raw serialized `VectorRecord` bytes instead of a lossy projection
  - tombstone state is serialized and re-installable instead of being dropped by live-only collection
  - TTL state is serialized and re-installable instead of being omitted from snapshot DTOs
- These tests are groundwork for Epic 5 and Tranche 4. They prevent the storage install API and later branch-retention work from being defined around a lossy snapshot wire format. They do not replace the need to preserve fork-frontier, inherited-layer, and reachability state through the non-checkpoint portions of the recovery chain.

**5. `SegmentedStore::install_snapshot_entries` contract** (~150 lines)

Add a test-covered storage install surface for decoded checkpoint entries by branch and primitive type.

Required shape:

```rust
SegmentedStore::install_snapshot_entries(
    branch_id: BranchId,
    type_tag: TypeTag,
    entries: &[DecodedSnapshotEntry],
)
```

where `DecodedSnapshotEntry` preserves:

- namespace space
- user key bytes
- value or tombstone state
- commit version
- timestamp
- TTL state

The storage layer constructs `Key` values from `(branch_id, space, type_tag, user_key)`; it does not require snapshot sections to arrive with fully materialized `Key` objects.

Rules:

- Preserve timestamp invariants.
- Preserve TTL invariants.
- Preserve tombstone invariants.
- No production recovery behavior changes in this epic.

### Verification

- Test: `DatabaseLayout::from_root` produces expected paths.
- Test: coordinator accepts `DatabaseLayout`; no `wal_dir` plus `with_segments` construction remains on the active production path.
- Test: checkpoint characterization and regression tests prove tombstone and TTL fidelity before Epic 5 consumes checkpoints.
- Test: `install_snapshot_entries` round-trips entries.
- Test: checkpoint/install preserves tombstone state needed for restart correctness.
- Test: checkpoint/install preserves TTL behavior needed for restart correctness.
- Test: WAL-only recovery still passes unchanged.
- Benchmark: YCSB, redb, open latency smoke.

### Effort: 3-5 days

---

## Epic 5: Recovery Coordinator Extension

**Goal:** Extend the existing `strata_concurrency::RecoveryCoordinator` with snapshot loading, codec validation, delta-WAL replay, and `.meta` sidecar rebuild.

**Why:** This is the main recovery correctness fix. Checkpoints are written but not consumed. Recovery currently replays WAL only, so checkpoint coverage and WAL retention cannot become truthful. Tranche 4 branch-retention semantics also depend on restart preserving tombstone and TTL barriers exactly.

### Changes

**1. Snapshot loading in `crates/concurrency/src/recovery.rs`** (~150 lines)

- Read latest snapshot id and snapshot watermark from MANIFEST.
- Load snapshot via durability snapshot reader.
- Validate magic, CRC, and codec.
- Pass a `LoadedSnapshot` to the engine callback.
- Preserve the branch/inherited-layer/reachability state already carried by `MANIFEST` and segment recovery so fork-frontier visibility and safe shared-segment deletion remain restart-correct.

No new recovery coordinator type. Extend the existing coordinator in place.

**2. Codec validation in `plan_recovery()`** (~60 lines)

- Read MANIFEST codec id.
- Compare to requested codec before WAL bytes are read.
- Return a clear codec mismatch error on mismatch.
- Remove redundant open-path side validation after coordinator validation lands.

**3. Delta-WAL replay** (~60 lines)

- After snapshot install, replay WAL records where `txn_id > snapshot_watermark`.
- Keep WAL-only restart working when no snapshot exists.
- Keep partial-tail truncation behavior.

**4. `.meta` sidecar rebuild** (~100 lines)

- Detect missing or stale sidecar.
- Rebuild from segment headers/properties.
- Keep CRC failure behavior explicit.

**5. Callback-driven recovery API** (~150 lines)

Change recovery from "coordinator returns constructed engine state" to "engine owns stores and coordinator drives callbacks":

```rust
pub fn recover(
    self,
    on_snapshot: impl FnOnce(LoadedSnapshot) -> StrataResult<()>,
    on_record: impl FnMut(WalRecord) -> StrataResult<()>,
) -> StrataResult<RecoveryStats>;
```

Rules:

- Engine owns `SegmentedStore` construction.
- `on_snapshot` installs decoded checkpoint entries.
- `on_record` preserves the current inline-decode invariants.
- Crash recovery rebuilds derived state through subsystem `recover`; it does not fire `ReplayObserver`.

**6. Engine snapshot install decoder** (~300 lines, new file)

Add `crates/engine/src/database/snapshot_install.rs`:

- Decode KV.
- Decode Event.
- Decode Branch metadata.
- Decode JSON.
- Decode Vector state.
- Decode Graph state as appropriate through namespace/prefix ownership.

Mostly inverse of checkpoint collection.

**7. Rewire engine open** (~150 lines)

Open sequence:

```text
build DatabaseLayout
construct SegmentedStore
RecoveryCoordinator::new(layout, codec)
recover(on_snapshot, on_record)
run subsystem recover
continue runtime lifecycle
```

Align with T2 `open_runtime` recovery ordering.

**8. Revert the `#1730` defensive hardening** (~10 lines)

After recovery consumes snapshots, WAL retention can trust composite recovery coverage again. `effective_watermark` should include snapshot coverage rather than ignoring it.

**9. Invert the `test_issue_1730` negative test** (~30 lines)

The old test asserted that checkpoint compact recovery must fail. It now asserts compact succeeds and reopened data is present.

### Verification

- Test: WAL-only restart still works.
- Test: checkpoint-only restart works.
- Test: checkpoint-only restart preserves tombstones.
- Test: checkpoint-only restart preserves TTL behavior.
- Test: restart preserves inherited-layer/fork-frontier visibility across reopen.
- Test: restart preserves safe shared-segment deletion behavior.
- Test: checkpoint plus delta-WAL replay works.
- Test: partial WAL tail truncation still works.
- Test: missing/stale `.meta` sidecar rebuild works.
- Test: codec mismatch rejected before WAL bytes are read.
- Test: non-identity codec write, crash, reopen, read works.
- Test: pre-DR-5 `.chk` fixtures remain consumable.
- Test: vector double-recovery does not conflict with `VectorSubsystem::recover`.
- Test: `test_issue_1730` is inverted to success.
- Benchmark: WAL latency, YCSB, redb, open-time latency.

### Effort: 1-2 weeks

---

## Epic 6: Shutdown Hardening

**Goal:** Harden the existing `Database::shutdown()` into an authoritative ordered close barrier.

**Why:** The current shutdown path can time out waiting for transactions, log a warning, and continue into freeze hooks. That is not a safe close contract.

### Changes

**1. Return `ShutdownTimeout` instead of warn-and-proceed** (~50 lines)

- If `wait_for_idle(deadline)` times out, return `Err(StrataError::ShutdownTimeout { active_txn_count })`.
- Do not join flush thread.
- Do not run final flush.
- Do not run subsystem freeze hooks.
- Leave the database usable so the caller can complete transactions and retry.

Forced transaction abort is out of scope.

**2. Registry slot release in `shutdown()`** (~60 lines)

- Release `OPEN_DATABASES` slot after successful shutdown.
- Keep Drop release as a best-effort fallback only.
- Make successful shutdown deterministic without waiting for Drop.

**3. MANIFEST fsync** (~20 lines)

Add explicit MANIFEST fsync after final WAL flush and before subsystem freeze loop.

**4. Public deadline API** (~60 lines)

Add:

- `Database::shutdown_with_deadline(Duration)`
- `Database::shutdown()` as default-deadline wrapper
- `Strata::close(self)` executor wrapper

**5. Test harness migration** (~80 lines)

- Update tests that rely on Drop timing to call `shutdown()` explicitly.
- Audit ignored shutdown results.

### Verification

- Test: write, shutdown, reopen returns same state.
- Test: second shutdown is idempotent.
- Test: Drop fallback still cleans up if shutdown is skipped.
- Test: long transaction causes `shutdown_with_deadline` to return `ShutdownTimeout`.
- Test: freeze hooks do not run after shutdown timeout.
- Test: after transaction completes, retry shutdown succeeds.
- Test: successful shutdown releases registry slot immediately.
- Test: Drop fallback releases registry slot if shutdown skipped.
- Test: shutdown fsyncs MANIFEST.
- Benchmark: YCSB, redb. No hot-path regression expected.

### Effort: 3-5 days

---

## Epic 7: Codec Uniformity and Config Matrix

**Goal:** Align codec identity across MANIFEST, recovery coordinator, and `CompatibilitySignature`; classify every durability/recovery config knob.

**Why:** Codec mismatch and runtime config drift are open-time correctness issues. They should be rejected deterministically, not discovered by partial recovery or silent behavior changes.

### Changes

**1. Codec cross-validation** (~80 lines)

- Coordinator rejects codec mismatch in `plan_recovery`.
- Registry reuse compares `CompatibilitySignature.codec_id`.
- Reopen with different codec returns a clear error.
- Follower and primary use the same codec policy.

**2. Config matrix document** (~200 lines, mostly docs)

Create `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`.

Classify every `StrataConfig` durability/recovery field as:

- Open-time only.
- Live-safe.
- Unsupported/deferred.

Rules:

- Open-time-only fields must participate in reuse validation when they affect runtime identity.
- Live-safe fields must have explicit setter semantics.
- Unsupported/deferred fields must be documented as such.

**3. Matrix enforcement test** (~100 lines)

Add a regression test that parses the matrix and verifies every `StrataConfig` durability/recovery field is classified.

**4. Delete silently unused knobs** (~20-60 lines)

If a knob is not implemented and is not explicitly deferred, delete it. Do not leave undocumented no-op controls.

### Verification

- Test: non-identity codec survives write, crash, reopen, read.
- Test: reopen with different codec is rejected.
- Test: registry reuse with codec mismatch returns incompatible reuse.
- Test: primary/follower codec mismatch behavior matches.
- Test: every config field appears in the matrix.
- Test: no silently unused durability knobs remain.
- Benchmark: YCSB, redb, open latency smoke.

### Effort: 3-5 days

---

## Epic 8: Historical Cleanup and Docs

**Goal:** Remove stale M2-era comments/stubs and rewrite architecture docs to describe shipped durability behavior.

**Why:** After the recovery path changes, stale architecture docs become dangerous. They can cause future work to target the wrong recovery chain.

### Changes

**1. Recovery historical cleanup** (~20-40 lines removed)

- Delete M2-phase comments in `crates/concurrency/src/recovery.rs`.
- Delete or rename dead snapshot-path stubs replaced by `DatabaseLayout`.
- Mark old research gaps resolved where the shipped path now closes them.

**2. Research and design doc updates** (~50-100 lines)

- Update durability/recovery research gap notes.
- Mark old checkpoint recovery plan references as superseded by this tranche if the files exist.
- Keep source-plan references in scope docs honest if files have moved or been removed.

**3. Rewrite `docs/architecture/durability-and-recovery.md`** (~200 lines)

Describe the shipped path:

- Snapshot loading.
- Delta-WAL replay.
- Codec validation.
- Follower contiguous refresh.
- Replay observer insertion point.
- WAL writer halt/resume.
- Authoritative shutdown.

Add a "current state as of commit/date" banner. Any remaining target-state language must be explicitly labeled.

**4. Doc truth regression** (~50 lines)

Add a grep-style regression test that fails on unlabeled target-state phrases in the durability architecture doc.

### Verification

- Test: zero M2-phase comments remain in recovery coordinator.
- Test: architecture doc has no unlabeled target-state claims.
- Test: `/scope-review durability-recovery` passes.
- No benchmark needed for doc-only changes.

### Effort: 2-3 days

---

## Dependency Graph and Execution

```text
Epic 1 (WAL sync correctness)
    -> Epic 2 (halt/resume)

Epic 3 (follower refresh convergence)
    -> Epic 5 (recovery coordinator extension)

Epic 4 PR 1 (DatabaseLayout + coordinator cleanup)
    -> Epic 4 PR 2 (snapshot install contract + characterization)
        -> Epic 5 (recovery coordinator extension)
        -> Epic 7 (codec + config matrix)
            -> Epic 8 (cleanup + docs)

Epic 1 + T2-E5 (session shape)
    -> Epic 6 (shutdown hardening)
```

**Sprint 1:** Epic 1 and Epic 3 in parallel. Epic 1 is the urgent data-loss fix; Epic 3 is independent follower work.

**Sprint 2:** Epic 2 and Epic 4 PR 1 in parallel. Epic 2 consumes `bg_error`; Epic 4 PR 1 prepares the layout/coordinator structure.

**Sprint 3:** Epic 4 PR 2, then Epic 5. PR 2 makes the storage install contract and checkpoint payload retention-complete for tombstones/TTL; Epic 5 is the main load-bearing recovery PR and gets the heaviest review.

**Sprint 4:** Epic 6 and Epic 7. Shutdown depends on T2-E5 and Epic 1; codec matrix depends on Epic 5.

**Sprint 5:** Epic 8, integration testing, benchmark stabilization, and bug fixes.

### Lockstep Sets

| Invariant | Must Land Together |
|-----------|-------------------|
| `RecoveryCoordinator` callback API shape | E4 PR 1 adds `DatabaseLayout` and constructor cleanup; E5 changes `recover()` signature — tightly coupled PR series |
| `CompatibilitySignature.codec_id` validation | E7 validates it; T2 runtime scope defines it — coordinate if both in flight |
| `RefreshHook` fallibility change | E3 changes trait signature; vector and search implementations must update in same PR |

### Rollback

| Epic | Reversible | Notes |
|------|-----------|-------|
| E1 | Partially | Counter-reset fix changes WAL write ordering; reverting reintroduces data-loss hole |
| E2 | Yes | New error variants and health API are additive |
| E3 | Partially | `RefreshHook` signature change breaks vector/search implementations on revert |
| E4 | Yes | New types only |
| E5 | Partially | Recovery fidelity now carries tombstone/TTL state as well as snapshot coverage; reverting would reintroduce lossy reopen semantics |
| E6 | Mostly | Reverting re-introduces unordered close |
| E7 | Yes | Validation-only, additive |
| E8 | Yes | Doc-only |

Storage format note: T3 now tightens checkpoint payload fidelity for tombstones and TTL in addition to the branch/space/type and MVCC metadata already needed for recovery. This is an intentional durability-format change in service of restart correctness and the later T4 branch-retention contract.

### Expected Results

| Area | Before | After Tranche 3 |
|------|--------|-----------------|
| WAL sync | Counters can reset before fsync succeeds | Three-phase sync; counters reset only after success |
| Flush thread | Two loops | One shared flush loop |
| Writer failure | Logged and swallowed | Writer halts; health API and explicit resume |
| Commit outcome | `DurableButNotVisible` flattened | Structured first-class error |
| Follower watermark | One watermark | Received and applied watermarks split |
| Refresh hook failure | Silent or impossible to report | Blocks contiguous advancement |
| Concurrent refresh | Unspecified | Single-flight refresh gate |
| Recovery | WAL-only | Snapshot load plus delta-WAL replay |
| Directory layout | Ad-hoc paths | Shared `DatabaseLayout` |
| Checkpoint payload | Lossy for delete/expiry barriers | Retention-complete for tombstones and TTL |
| WAL retention | Snapshot coverage distrusted | Real recovery coverage reflected |
| Shutdown | Warn-and-proceed on timeout | Error without freeze; retryable |
| MANIFEST close path | No explicit fsync | MANIFEST fsynced on shutdown |
| Codec validation | Non-uniform | Coordinator-centered and signature-aligned |
| Config knobs | Unclassified | Matrix-enforced classification |

---

## Verification and Benchmark Gate

Every code-changing PR runs the non-regression gate for its touched area:

- PR-grade YCSB for all epics.
- WAL latency for Epics 1, 2, 5, and 6.
- Redb-style recovery/storage benchmarks for Epics 1, 2, 4, 5, 6, and 7.
- Open-time latency for Epics 4, 5, and 7.
- Follower refresh latency for Epic 3.
- BEIR if Epic 3 changes vector/search refresh hook behavior.

Required WAL correctness tests:

- Sync failure preserves counters and unsynced state.
- In-flight sync blocks redundant inline sync.
- Writer halts on sync failure.
- Resume clears halt only after successful sync.
- `DurableButNotVisible` recovers on reopen.

Required follower correctness tests:

- Contiguous watermark never skips.
- Hook failure blocks advancement.
- Operator skip requires exact txn id and writes audit.
- Concurrent refresh serializes.
- `ReplayObserver` fires only after contiguous advancement.

Required recovery correctness tests:

- WAL-only restart still works.
- Checkpoint-only restart works.
- Checkpoint-only restart preserves tombstones.
- Checkpoint-only restart preserves TTL behavior.
- Checkpoint plus delta-WAL replay works.
- Partial tail truncation still works.
- Pre-DR-5 `.chk` fixtures remain consumable.
- Codec mismatch is rejected before WAL bytes are read.

Performance regressions are not waived. Durability correctness that materially regresses throughput, commit latency, or open latency does not satisfy T3.

---

## What Comes After (Not in This Plan)

From sibling scopes and post-cleanup backlog:

- Runtime consolidation follow-through if any T2 session/open integration remains.
- Branch primitives: `BranchRef`, lifecycle generations, primitive lifecycle contracts, branch mutation control surfaces, and branch-aware storage retention semantics above the recovery fidelity delivered here.
- Product control surface: `ControlRegistry`, product open plans, config overlays, and doc gates.
- Search/intelligence/inference: auto-embedding, HNSW and BM25 index creation, native inference providers, built-in and custom recipes.
- Quality cleanup: full error taxonomy, visibility tightening, unsafe documentation, file decomposition, and catalog-to-test mapping.
- Forced transaction abort on shutdown timeout. T3 deliberately returns `ShutdownTimeout` and leaves retry/abort decisions to callers.
- Further checkpoint-format optimization beyond the retention-complete fidelity introduced here. T3's concern is restart correctness, not later throughput or storage-efficiency tuning.
