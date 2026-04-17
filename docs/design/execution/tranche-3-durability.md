# Tranche 3: Durability and Lifecycle

**Status:** NOT STARTED

| Field | Value |
|-------|-------|
| Goal | Unified recovery, WAL correctness, follower convergence, authoritative shutdown, retention-complete checkpoint fidelity |
| Owning scopes | durability-recovery |
| Contributing phases | DR-0 through DR-10 |
| Prerequisite tranches | T1 (CI); T2 (RC-1 observer traits, RC-6 session shape for DR-8) |
| Change class | cutover (DR-1, DR-5) + intentional (DR-2 new errors, DR-3 new types) |
| Assurance class | S4 (WAL correctness, recovery, commit ordering) |
| Benchmark required | Yes — after every epic |
| Exit gate | SyncHandle in WAL, RecoveryCoordinator snapshot-aware, follower contiguous, shutdown authoritative, codec uniform, checkpoint/recovery preserve tombstone and TTL barrier state |

**Preparation note:** T3-E4 and T3-E5 now explicitly prepare the branch-aware storage retention work planned in Tranche 4 by making checkpoint/install/recovery fidelity complete for tombstones and TTL, not just branch/space/type identity.

---

## Epic T3-E1: WAL Sync Correctness

**Phases:** DR-0 (pre-flight), DR-1
**Violations:** V-DR-4 (counter-reset data-loss bug — CRITICAL), V-DR-5 (duplicated flush threads)
**Decisions:** D-DR-3 (halt on fsync failure), D-DR-4 (single flush-thread)
**Assurance:** S4 — characterization tests required; this fixes an active production data-loss hole

### Tasks

- [ ] **Pre-flight (DR-0):**
  - Confirm lifecycle unification Epics 1–6 merged (git log check)
  - Confirm `follower-mode-product-decision.md` merged
  - Inventory committed `.chk` fixture files that DR-5 must keep consumable
  - Cross-check RC Phase 1 timeline (Subsystem hooks must exist before DR-5)
- [ ] **Characterization tests** (S4 requirement):
  - Test current WAL sync behavior including the counter-reset path
  - Test current flush-thread lifecycle (both `open.rs` and `mod.rs` loops)
  - Record baseline behavior to prove the fix changes the right thing
- [ ] Introduce `SyncHandle` + `SyncSnapshot` types on `WalWriter`
- [ ] Add three-phase sync API: `begin_background_sync()`, `commit_background_sync()`, `abort_background_sync()`
- [ ] Add `sync_in_flight: bool` field; `maybe_sync()` checks it instead of reset counters
- [ ] Add `bg_error: Option<BgError>` field (consumed by DR-2)
- [ ] Delete `prepare_background_sync()` (fixes counter-reset bug)
- [ ] Rewrite primary flush thread in `crates/engine/src/database/open.rs:595-655` to use three-phase API
- [ ] Delete duplicated flush loop in `crates/engine/src/database/mod.rs:1005-1046`; `set_durability_mode()` calls `spawn_wal_flush_thread` directly
- [ ] Add `#[cfg(test)]` fault-injection hook inside flush thread's `sync_all()` call

### Acceptance Criteria

- [ ] Regression test matrix catches commit `962ac0b7` counter-reset as wrong
- [ ] No public API change — no caller migration needed
- [ ] Single flush-thread implementation (zero code duplication)
- [ ] Fault-injection test verifies sync failure propagates to `bg_error`
- [ ] `/regression-check` passes

---

## Epic T3-E2: WAL Writer Halt and Resume

**Phases:** DR-2
**Violations:** V-DR-6 (writer doesn't fail closed), V-DR-15 (DurableButNotVisible flattened)
**Decisions:** D-DR-3 (halt on fsync failure), D-DR-13 (DurableButNotVisible first-class)
**Prerequisite:** T3-E1 (three-phase sync + `bg_error` field)

### Tasks

- [ ] Add `WalWriterHealth` enum: `Healthy | Halted { reason, first_observed_at, failed_sync_count }`
- [ ] Add `wal_writer_health: Mutex<WalWriterHealth>` field on `Database`
- [ ] Flush thread reads `bg_error`, flips `accepting_transactions` → false, sets health → Halted
- [ ] Add `Database::wal_writer_health()` public API
- [ ] Add `Database::resume_wal_writer(confirm_reason: &str)` public API
- [ ] Add `StrataError::WriterHalted { reason, first_observed_at }` variant
- [ ] Add `StrataError::DurableButNotVisible { txn_id: TxnId, commit_version: CommitVersion }` variant
- [ ] Update `transaction()`, `commit()`, `commit_internal()` to return `WriterHalted` when halted
- [ ] Stop collapsing `CommitError::DurableButNotVisible` into generic storage error

### Acceptance Criteria

- [ ] Halt-on-first-error test passes
- [ ] Halt-on-repeated-error test passes
- [ ] Resume-success test passes
- [ ] Resume-while-still-failing test passes
- [ ] Caller-visible error propagation test passes
- [ ] `DurableButNotVisible` surfaced distinctly (not generic storage error)
- [ ] `/regression-check` passes

---

## Epic T3-E3: Follower Refresh Convergence

**Phases:** DR-3
**Violations:** V-DR-9 (silent advancement past failures), V-DR-10 (infallible RefreshHook), V-DR-16 (concurrent refresh unspecified)
**Decisions:** D-DR-5 (fallible hooks), D-DR-6 (structurally contiguous), D-DR-7 (narrow operator skip), D-DR-14 (single-flight refresh)
**Cross-scope:** RC Phase 2 `ReplayObserver` insertion point must exist

### Tasks

- [ ] Introduce `ContiguousWatermark` with `try_advance()`, `block_at()`, `unblock_exact()`
- [ ] Introduce `RefreshOutcome` enum: `CaughtUp | Stuck | NoProgress`
- [ ] Introduce `BlockedTxn` struct + `BlockReason` enum
- [ ] Introduce `FollowerStatus` struct (received/applied watermarks, blocked_at)
- [ ] Introduce `AdvanceError` and `UnblockError` enums
- [ ] Introduce `RefreshGate` for single-flight serialization
- [ ] Split `Database::wal_watermark` into `received_watermark` and `applied_watermark`
- [ ] Change `RefreshHook::apply_refresh` to fallible — update vector, search, and graph implementations that own correctness-critical replay state
- [ ] Rewrite `Database::refresh()`: decode → apply storage → apply hooks → `try_advance` → fire `ReplayObserver`; failure before `try_advance` sets block and returns `Stuck`
- [ ] Add `Database::follower_status()` public API
- [ ] Add `Database::admin_skip_blocked_record(txn_id: TxnId, reason)` with audit logging (tracing + durable `follower_audit.log`)
- [ ] Make `RefreshOutcome` `#[must_use]`; update all ~14 in-tree callers to pattern-match

### Acceptance Criteria

- [ ] Contiguous watermark never skips a record
- [ ] Blocked record detected and `Stuck` returned
- [ ] Operator skip requires exact `txn_id` match (mismatch → `UnblockError::Mismatch`)
- [ ] Audit logged to both tracing and durable file
- [ ] `RefreshGate` serializes concurrent callers
- [ ] `FollowerStatus` reports correct watermarks and blocked_at
- [ ] All ~14 callers updated (compiler enforces via `#[must_use]`)
- [ ] `/regression-check` passes

---

## Epic T3-E4: DatabaseLayout and Snapshot Install

**Phases:** DR-4
**Violations:** V-DR-2 (ad-hoc directory layout), V-DR-3 (optional WAL segments arg)
**Decisions:** D-DR-2 (one canonical DatabaseLayout), D-DR-1 (extend RecoveryCoordinator)

Implementation note:

- Land this epic as two PRs.
- PR 1 is layout and coordinator cleanup only: `DatabaseLayout`, open-path adoption, `RecoveryCoordinator::new(layout, write_buffer_size)`, and harness migration.
- PR 2 adds the storage-side snapshot install contract and fixes the checkpoint payload so it preserves branch/space/type identity, raw KV/Graph key bytes, per-entry MVCC metadata, tombstones, and TTL.
- Neither PR changes production recovery behavior or callback shape. Snapshot loading, codec validation, and delta-WAL replay remain Epic T3-E5.

### Tasks

- [ ] **PR 1: layout and coordinator cleanup**
  - Introduce `DatabaseLayout` struct: `root`, `wal_dir`, `snapshots_dir`, `segments_dir`, `manifest_path`; constructor `from_root()`
  - Update `crates/engine/src/database/open.rs` to construct `DatabaseLayout` and pass to coordinator
  - Update `RecoveryCoordinator::new()` signature to accept `DatabaseLayout` plus write-buffer sizing (replaces `new(wal_dir).with_segments(...)`)
  - Delete the production `with_segments` builder path once all call sites move; delete `with_snapshot_path()` if no longer needed
  - Update test harnesses to use `DatabaseLayout`
- [ ] **PR 2: snapshot-install groundwork**
  - Add `SegmentedStore::install_snapshot_entries(branch_id, type_tag, entries)` with unit tests covering tombstones and TTL
  - Entry shape must preserve namespace space, raw user key bytes, timestamp, commit version, TTL, and tombstone state; it must not assume snapshot sections already contain fully reconstructed `Key` values
  - Checkpoint DTOs and collection paths must preserve branch, space, type, tombstone, and TTL identity explicitly so restart correctness does not depend on live-only collection behavior
  - Recovery artifacts taken together must not regress fork-frontier, inherited-layer, or reachability state that remains persisted outside checkpoint payloads
  - Add characterization and regression tests proving checkpoint payload/install semantics are retention-complete for the T4 branch-aware storage retention work

### Acceptance Criteria

- [ ] `DatabaseLayout::from_root(tmpdir)` constructs correct subdirectory paths
- [ ] `RecoveryCoordinator` accepts `DatabaseLayout`
- [ ] No `new(wal_dir).with_segments(...)` construction remains on the active production path
- [ ] PR 1 has no regression in WAL-only recovery
- [ ] `install_snapshot_entries` unit tests pass
- [ ] Checkpoint payload preserves tombstone state needed for restart correctness
- [ ] Checkpoint payload preserves TTL state needed for restart correctness
- [ ] Checkpoint/recovery preparation does not regress inherited-layer or reachability metadata needed for safe reopen semantics
- [ ] Characterization and regression tests prove delete/TTL fidelity before Epic T3-E5 consumes checkpoints
- [ ] `/regression-check` passes

---

## Epic T3-E5: Recovery Coordinator Extension

**Phases:** DR-5
**Violations:** V-DR-1 (coordinator WAL-only — CRITICAL), V-DR-7 (checkpoints never loaded — CRITICAL), V-DR-8 (#1730 defensive hardening masks bug), V-DR-11 (codec validation not uniform)
**Decisions:** D-DR-1 (extend in place), D-DR-8 (uniform codec), D-DR-9 (lossy opt-in)
**Prerequisite:** T3-E4 (DatabaseLayout + install_snapshot_entries)
**Assurance:** S4 — this is the main correctness fix for recovery

### Tasks

- [ ] **Coordinator changes** (`crates/concurrency/src/recovery.rs`):
  - Add snapshot loading: read MANIFEST's snapshot_id/watermark, load via `SnapshotReader`, validate magic/CRC/codec
  - Add codec validation in `plan_recovery()`: reject mismatch BEFORE any WAL bytes read
  - Add delta-WAL replay: after snapshot install, replay only `txn_id > snapshot_watermark`
  - Preserve inherited-layer and reachability metadata from `MANIFEST` / segment recovery so fork-frontier visibility and safe shared-segment deletion remain restart-correct
  - Add `.meta` sidecar rebuild when missing or stale
  - Change API to callback-driven: `recover(on_snapshot, on_record) -> RecoveryStats`
- [ ] **Engine changes** (`crates/engine/src/database/`):
  - Add `snapshot_install.rs` — per-primitive decoder for KV, Event, Branch, JSON, Vector, Graph
  - Rearrange `open_finish`: build layout → construct SegmentedStore → call `recover(on_snapshot, on_record)`
  - Remove redundant `open_finish`-side codec check (now in coordinator)
- [ ] Revert `#1730` defensive hardening in `wal_only.rs:316`
- [ ] Invert `test_issue_1730_checkpoint_compact_recovery_data_loss` — `compact()` now succeeds
- [ ] **Test matrix** (`engine/tests/checkpoint_recovery_tests.rs`):
  - WAL-only restart (regression guard)
  - Checkpoint-only restart
  - Checkpoint-only restart with tombstones preserved
  - Checkpoint-only restart with TTL preserved
  - Restart preserves inherited-layer visibility/fork-frontier behavior
  - Restart preserves safe shared-segment deletion behavior
  - Checkpoint + delta-WAL replay
  - Partial WAL tail truncation (regression guard)
  - `.meta` sidecar rebuild
  - Codec mismatch rejection
  - Non-identity codec round-trip
  - Snapshot format version enforcement (v1 rejected on load; clean break per pre-release policy — see DR-5 notes in the architecture scope)
  - Vector double-recovery (no state conflicts)

### Acceptance Criteria

- [ ] All 13 checkpoint recovery test scenarios pass
- [ ] Snapshot format version enforcement rejects stale formats on load (no v1 compatibility shim while pre-release — see DR-5 notes in the architecture scope)
- [ ] `test_issue_1730` inverted (compact succeeds)
- [ ] Codec mismatch rejected with clear error at open
- [ ] Non-identity codec survives write→crash→reopen→read
- [ ] Checkpoint/reopen preserves delete state
- [ ] Checkpoint/reopen preserves TTL behavior
- [ ] Restart preserves inherited-layer visibility across reopen
- [ ] Restart preserves safe shared-segment deletion behavior
- [ ] `/regression-check` passes

---

## Epic T3-E6: Shutdown Hardening

**Phases:** DR-8
**Violations:** V-DR-12 (shutdown not authoritative)
**Decisions:** D-DR-10 (authoritative ordered close barrier)
**Cross-scope:** RC Phase 6 session shape (T2-E5 must be complete)

### Tasks

- [ ] Change `shutdown()` to return `Err(ShutdownTimeout)` on `wait_for_idle` timeout — do NOT proceed through freeze hooks
- [ ] Move `OPEN_DATABASES` registry slot release from `Drop` into end of successful `shutdown()`
- [ ] Add MANIFEST fsync after final WAL flush, before subsystem freeze loop
- [ ] Add `Database::shutdown_with_deadline(Duration)` public variant
- [ ] Existing `shutdown()` becomes `shutdown_with_deadline(default_30s)` internally
- [ ] Add `StrataError::ShutdownTimeout { active_txn_count }` variant
- [ ] Add `Strata::close(self)` wrapper in executor
- [ ] Update test harnesses to call `shutdown()` explicitly instead of relying on Drop

### Acceptance Criteria

- [ ] `shutdown_is_ordered_and_deterministic` — write → shutdown → reopen returns same state
- [ ] `shutdown_twice_is_idempotent` — second call is no-op
- [ ] `drop_without_shutdown_still_cleans_up` — Drop fallback works
- [ ] `shutdown_timeout_returns_error` — long txn held; shutdown returns error; freeze hooks NOT run
- [ ] `shutdown_retry_after_txn_completes` — complete txn → retry → succeeds
- [ ] `shutdown_releases_registry_slot` — fresh open on same path succeeds immediately
- [ ] `drop_releases_registry_slot_if_shutdown_skipped` — Drop fallback releases slot
- [ ] `/regression-check` passes

---

## Epic T3-E7: Codec Uniformity and Config Matrix

**Phases:** DR-7, DR-9
**Violations:** V-DR-11 (non-uniform codec validation), V-DR-14 (unclassified control knobs)
**Decisions:** D-DR-8 (uniform codec), D-DR-11 (truth table is load-bearing)

### Tasks

- [ ] **Codec uniformity (DR-7):**
  - Verify `RecoveryCoordinator::plan_recovery()` rejects codec mismatch (from E5)
  - Cross-validate `CompatibilitySignature.codec_id` against MANIFEST at reuse time
  - Add test: non-identity codec write → crash → reopen → read
  - Add test: reopen with different codec → rejected
  - Add test: registry reuse with mismatched codec → `IncompatibleReuse`
- [ ] **Config matrix (DR-9):**
  - Classify every `StrataConfig` durability/recovery field as: open-time-only, live-safe, or unsupported/deferred
  - Create `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`
  - Add regression test: parse matrix, verify every `StrataConfig` field is classified
  - Update `StrataConfig` doc comments to reflect classification inline
  - Delete any silently unused knobs

### Acceptance Criteria

- [ ] Non-identity codec database survives write→crash→reopen→read
- [ ] Codec mismatch on reopen rejected with clear error
- [ ] Registry reuse with mismatched codec returns `IncompatibleReuse`
- [ ] Every `StrataConfig` field appears in config matrix
- [ ] Matrix regression test passes
- [ ] No silently unused knobs remain
- [ ] `/regression-check` passes

---

## Epic T3-E8: Historical Cleanup and Docs

**Phases:** DR-6, DR-10
**Violations:** V-DR-13 (architecture docs describe unshipped behavior)
**Decisions:** D-DR-12 (docs follow shipped path)
**Prerequisite:** All other T3 epics complete

### Tasks

- [ ] **Historical cleanup (DR-6):**
  - Delete M2-phase comments from `crates/concurrency/src/recovery.rs`
  - Delete dead `with_snapshot_path()` stub if still present
  - Update research docs: mark Gap 1 resolved, update prioritized-fixes, refresh architectural-gaps
  - Delete or mark `checkpoint-recovery-unification-plan.md` as superseded
- [ ] **Doc alignment (DR-10):**
  - Add "current state as of [commit/date]" banner
  - Remove stale snapshot-based-restart language
  - Label any remaining target-state claims explicitly
  - Add regression test: grep doc for target-state phrases without qualifier → test fails

### Acceptance Criteria

- [ ] Zero M2-phase comments in recovery coordinator
- [ ] Research docs updated with resolution markers
- [ ] Architecture doc accurately describes shipped recovery chain
- [ ] No unlabeled target-state claims in architecture doc
- [ ] Regression test passes (doc phrases check)
- [ ] `/scope-review durability-recovery` passes

---

## Tranche 3 Sequencing

```
T3-E1 (WAL sync)
    ↓
T3-E2 (halt/resume)                T3-E3 (follower refresh) — parallel with E1/E2
    ↓                                    ↓
T3-E4 (DatabaseLayout)                  │
    ↓                                    │
T3-E5 (recovery coordinator) ←──────────┘  (E3 wires ReplayObserver insertion)
    ↓
T3-E6 (shutdown) ←── requires T2-E5 (session shape)
    ↓
T3-E7 (codec + config matrix)
    ↓
T3-E8 (cleanup + docs)
```

- **E1 → E2:** halt/resume consumes `bg_error` field from E1
- **E3 parallel with E1/E2:** independent code area (follower refresh vs WAL write path)
- **E4 → E5:** snapshot install and retention-complete payload fidelity needed before recovery extension
- **E5 depends on E3:** ReplayObserver insertion point wired in E3
- **E6 depends on T2-E5:** session shape must be fixed for shutdown integration
- **E7 after E5:** codec validation moved into coordinator in E5
- **E8 after all:** cleanup and docs require stable shipped behavior

---

## Lockstep Sets

| Invariant | Must Land Together |
|-----------|-------------------|
| `RecoveryCoordinator` callback API shape | E4 adds `DatabaseLayout`; E5 changes `recover()` signature — tightly coupled PR series |
| `CompatibilitySignature.codec_id` validation | E7 validates it; RC scope defines it — coordinate with T2 if both in flight |
| `RefreshHook` fallibility change | E3 changes trait signature; vector + search implementations must update in same PR |

---

## Cross-Scope Dependencies

- **T2 (runtime):** E6 (shutdown) depends on T2-E5 (RC-6 session shape fix)
- **T2 (runtime):** E3 (follower refresh) needs RC Phase 2 `ReplayObserver` trait from T2-E2
- **T2 (runtime):** E5 (recovery) coordinates with RC `open_runtime` step 3 sequence
- **T4 (branch):** branch-aware storage retention depends on E4/E5 carrying tombstones and TTL faithfully through checkpoint/install/recovery
- **T4 (branch):** BP-5 adds `Subsystem::lifecycle_contracts()` which does NOT block T3, but T3's subsystem freeze hooks must be forward-compatible
- **T5 (product):** PCS-2 (ControlRegistry) consumes DR-9 config matrix — T5-E2 depends on T3-E7

---

## Rollback

| Epic | Reversible | Notes |
|------|-----------|-------|
| E1 | Partially | Counter-reset fix changes WAL write ordering — reverting reintroduces data-loss hole |
| E2 | Yes | New error variants + health API — additive |
| E3 | Partially | `RefreshHook` signature change — reverting breaks vector/search implementations |
| E4 | Yes | New types only |
| E5 | Partially | Recovery path change — pre-DR-5 databases remain compatible; post-DR-5 snapshots would be unused on revert |
| E6 | Mostly | Shutdown semantics change — reverting re-introduces unordered close |
| E7 | Yes | Validation-only — additive |
| E8 | Yes | Doc-only |

**Storage format note:** DR-5 does NOT change the snapshot file format — it reads existing snapshots that were already being written but never loaded. No migration needed.

---

## Benchmark Obligations

Every code-changing PR runs PR-grade YCSB + BEIR per the universal gate. The table below lists **additional** tranche-specific suites beyond the universal gate, including area-specific obligations per NRR §10.4.

| Epic | Suites | Rationale |
|------|--------|-----------|
| E1 | redb + ycsb + WAL latency | WAL write path changed — critical for throughput and latency |
| E2 | redb + ycsb + WAL latency | Commit path gains health check — WAL write latency must hold |
| E3 | ycsb | Follower refresh path — read path may be affected |
| E4 | redb + ycsb | Recovery coordinator signature change |
| E5 | redb + ycsb + WAL latency | Recovery path rewrite — critical, includes WAL replay |
| E6 | redb + ycsb | Shutdown path + MANIFEST fsync |
| E7 | redb + ycsb | Codec validation at open |
| E8 | — | Doc-only, no benchmark needed |

---

## Closeout Addendum (review-driven)

**Status:** in progress (2026-04-17)

After the original 8 epics shipped (#2411–#2421), independent review against
the 17 DR-### requirements in `docs/requirements/durability-recovery-requirements.md`
found that per-epic mechanical acceptance tests had passed but **4 aggregate
contract gaps remained**:

- DR-009 codec uniformity — non-identity codec + WAL durability rejected at
  open time despite the acceptance clause saying it must work "without
  special-case rejection."
- DR-011 lossy recovery — wipe-and-reopen on any recovery error with no
  programmatic observability; failed the "surfaced in status/telemetry"
  clause.
- DR-012 `ContiguousWatermark` — `try_advance` used `fetch_max` with no
  contiguity check; the documented structural invariant lived only in
  refresh-loop discipline, not on the type.
- DR-015 follower close-barrier residual — follower `refresh()` and
  `admin_skip_blocked_record()` did not honor `shutdown_complete`.

The epics below are the review-driven closeout track that lands the
corresponding fixes. They are **not part of the original tranche plan**;
they are numbered contiguously (E9 onwards) only so PR tracking stays
simple. The original 8-epic plan is the normative record for the tranche's
design intent; the closeout epics are post-hoc corrections to the shipped
contract.

### Epic T3-E9: Contract Hardening

**Status:** shipped 2026-04-17 (PR #2422)
**Findings closed:** DR-012 (contiguity on the type), DR-015 follower close residual
**Change class:** additive
**Assurance:** S3
**Benchmark:** YCSB smoke (cold path)

#### Tasks

- [x] Make `ContiguousWatermark::try_advance` enforce `txn_id == applied + 1`; return new `AdvanceError::NonContiguous { expected, provided }` on any gap, duplicate, or stale id.
- [x] Use `checked_add` on the increment so `applied == u64::MAX` returns `NonContiguous` rather than panicking in debug or wrapping in release.
- [x] Verify the admin-skip flow (`unblock_exact` → next cycle's `try_advance`) still produces contiguous advances.
- [x] Add `UnblockError::DatabaseClosed` variant for admin_skip on a shut-down follower.
- [x] Add `shutdown_complete` guard to `Database::refresh()` — short-circuit to `RefreshOutcome::CaughtUp { applied: 0, applied_through: wm.applied() }` when the follower is closed (matches the non-follower / ephemeral branch).
- [x] Add `shutdown_complete` guard to `Database::admin_skip_blocked_record()` — return `UnblockError::DatabaseClosed` before any watermark / audit mutation.

#### Acceptance Criteria

- [x] `try_advance(applied + 1)` succeeds; `try_advance(applied + 2)`, `try_advance(applied)`, `try_advance(stale)` all return `NonContiguous` with correct expected/provided
- [x] Blocked state still takes precedence over contiguity
- [x] Skip flow preserves contiguity after unblock_exact
- [x] Follower `refresh()` post-`shutdown()` is a no-op; watermark stable
- [x] Follower `admin_skip_blocked_record` post-`shutdown()` returns `UnblockError::DatabaseClosed`; state-invariance assertions pass
- [x] `try_advance` at `u64::MAX` returns `NonContiguous`, not panic
- [x] `cargo test -p strata-engine --lib -- --test-threads=1` — 952→955 passing

### Epic T3-E10: Lossy Recovery Telemetry + Pinned Contract

**Status:** shipped 2026-04-17 (PR #2423)
**Findings closed:** DR-011 observability (partial — policy-matrix half is T3-E11)
**Change class:** additive
**Assurance:** S3
**Benchmark:** none (cold open-time path)

#### Tasks

- [x] Add `LossyRecoveryReport` struct to the engine public surface with fields `error: String`, `error_kind: LossyErrorKind`, `records_applied_before_failure: u64`, `version_reached_before_failure: CommitVersion`, `discarded_on_wipe: bool`.
- [x] Add `LossyErrorKind` enum (`Corruption`, `Storage`, `Other`) with a `from_strata_error(&StrataError)` mapper, per CLAUDE.md Quality Rule §26 (typed reason enums replace string-factory methods).
- [x] Add field to `Database`: `last_lossy_recovery_report: Arc<ParkingMutex<Option<LossyRecoveryReport>>>`, initialized in all three constructors (primary, follower, cache).
- [x] Add accessor `Database::last_lossy_recovery_report(&self) -> Option<LossyRecoveryReport>`.
- [x] Wrap the engine's `on_record` closure in both primary and follower open paths with an `Arc<AtomicU64>` counter; capture `storage.version()` before the wipe.
- [x] Add tracing target `strata::recovery::lossy` (warn-level, structured fields `error`, `error_kind`, `records_applied_before_failure`, `version_reached_before_failure`, `discarded_on_wipe`, `follower`).
- [x] Export `LossyRecoveryReport` and `LossyErrorKind` from `crates/engine/src/lib.rs`; add both to the CLAUDE.md D4 durability surface list and to `durability-recovery-scope.md`.
- [x] Rewrite DR-011 "Acceptance" in `docs/requirements/durability-recovery-requirements.md` to pin the "whole-database wipe-and-reopen" semantic and list the two observability surfaces.
- [x] Update D-DR-9 in `durability-recovery-scope.md` to match.
- [x] Update the `allow_lossy_recovery` row in `durability-recovery-config-matrix.md` with the observability pointer.

#### Acceptance Criteria

- [x] Existing `test_lossy_recovery_discards_valid_data_before_corruption` passes with new report assertions (records ≥ 1, discarded_on_wipe, non-empty error, version > 0, `error_kind == Storage`)
- [x] New `test_non_lossy_recovery_has_no_report` — strict open + cache open both leave the slot `None`
- [x] New `test_lossy_recovery_report_on_immediate_failure` — failure before any record applied produces a report with zero counters and `CommitVersion::ZERO`
- [x] New `test_lossy_error_kind_mapping_covers_relevant_variants` — direct unit test covering `Corruption`, `Storage`, `Other` mapping
- [x] New `test_follower_lossy_recovery_populates_report` — follower-path coverage; confirms typed `error_kind` populates on the follower branch too
- [x] 955→956 engine lib tests pass; clippy + fmt clean

### Epic T3-E11: Follower Open/Refresh Policy Matrix

**Status:** pending
**Findings closed:** DR-011 (the policy-matrix clause that T3-E10 deferred)
**Change class:** doc-heavy + minimal code alignment
**Assurance:** S2 (doc) + S3 (any code alignment)
**Benchmark:** none

#### Tasks

- [ ] Document the mixed policy in `docs/design/architecture-cleanup/durability-recovery-scope.md` — a new subsection (under DR-3 or adjacent to D-DR-9) stating: follower open honors `allow_lossy_recovery` via whole-DB fallback; follower refresh is strict-only (blocks on failure; operator-gated via `admin_skip_blocked_record`). Include a rationale paragraph explaining why the mixed policy is deliberate (open is one-time and caller-authorized; refresh is live and operator-observable).
- [ ] Mirror the policy matrix in `docs/architecture/durability-and-recovery.md` (lands with T3-E8 / PR 5).
- [ ] Add an integration test `follower_lossy_open_and_strict_refresh_coexist` — opens a follower with `allow_lossy_recovery=true` against a corrupt WAL (verifies open falls back and report populates), then corrupts a later record and confirms `refresh()` returns `RefreshOutcome::Stuck`, not a silent skip.
- [ ] No code change required: exploration confirmed `refresh()` at `crates/engine/src/database/lifecycle.rs:281` already ignores `allow_lossy_recovery`. The policy is implemented; only undocumented.

#### Acceptance Criteria

- [ ] Policy matrix section exists in `durability-recovery-scope.md` with the rationale paragraph
- [ ] Integration test demonstrates lossy open + strict refresh coexist on the same follower instance
- [ ] DR-011's "follower open and follower refresh use a consistent documented policy matrix" acceptance clause is now satisfied
- [ ] `cargo test -p strata-engine --test follower_tests` — all tests green

### Epic T3-E12: WAL Codec Threading

**Status:** pending
**Findings closed:** DR-009 codec uniformity
**Change class:** intentional semantic change (new WAL envelope format) + cutover (WAL reader signature)
**Assurance:** S4 (WAL read path correctness)
**Benchmark:** **required** — full PR-grade YCSB + WAL latency + redb + open-time + BEIR smoke; characterization tests before refactor; differential test identity-codec vs before/after

#### Tasks

- [ ] Design: pick WAL record envelope format (`[u32 length][codec-encoded bytes]` after the post-encode step); decide clean-break vs dual-path for identity codec.
- [ ] Bump `WAL_FORMAT_VERSION` in `crates/durability/src/format/wal_record.rs`.
- [ ] Add codec decode to `crates/durability/src/wal/reader.rs` — `codec` field on `WalReader`, `with_codec(codec)` builder, decode step before `WalRecord::from_bytes`.
- [ ] Add length-prefix write wrapper to `crates/durability/src/wal/writer.rs` (writer already calls `codec.encode_cow(...)` — add the envelope).
- [ ] Wire codec through `RecoveryCoordinator::recover()` — construct `WalReader` with the coordinator's installed codec.
- [ ] Delete the open-time rejection blocks at `crates/engine/src/database/open.rs:847-858` (primary) and `:1475-1485` (follower).
- [ ] Add the deferred test `write_non_identity_codec_then_crash_then_reopen_then_read` in `crates/engine/src/database/tests/codec.rs`; remove the deferral comment at lines 12-17.
- [ ] Update `durability-recovery-config-matrix.md` — remove the "target-state note" about WAL codec; update the codec row to note all durability modes support non-identity codecs.
- [ ] Capture a pre-PR baseline via `cargo run --release -p strata-benchmarks --bin regression -- --capture-baseline` before code changes land; gate merge on `--tranche 3 --epic "T3-E12"` within thresholds.

#### Acceptance Criteria

- [ ] Identity codec round-trip: existing tests pass unchanged (regression guard)
- [ ] Non-identity codec write → crash → reopen → read succeeds with the same data visible
- [ ] Codec mismatch on reopen is still rejected (MANIFEST codec check preserved)
- [ ] Large record stress: envelope handles multi-MB records without overflow
- [ ] Partial-tail truncation still works with envelope (CRC + length boundary)
- [ ] Codec decode error surfaces distinctly from corruption; does not trigger lossy wipe unless `allow_lossy_recovery`
- [ ] WAL latency benchmark: ≤5% regression median, ≤10% tail
- [ ] Redb 5M benchmark: ≤5% throughput regression
- [ ] Open-time latency: ≤5% regression

### Closeout Dependency Graph

```
T3-E9  (contract hardening)  ──┐
T3-E10 (lossy telemetry)     ──┼─→ T3-E11 (follower policy matrix)
                                │
                                └─→ T3-E12 (WAL codec threading) ──→ T3-E8 (arch doc closeout)
```

- T3-E9 and T3-E10 are independent and shipped in parallel.
- T3-E11 depends on T3-E10 (lossy observability surfaces referenced in the policy doc).
- T3-E12 is the largest-scope closeout epic and the longest pole; independent of T3-E11.
- T3-E8 (historical cleanup + architecture doc) lands last so the architecture doc reflects all closeout-era state. See the original Epic T3-E8 entry above.

### Closeout Rollback

| Epic | Reversible | Notes |
|------|-----------|-------|
| E9 | Yes | Additive new error variants + guards; reverting restores prior behavior without on-disk changes |
| E10 | Yes | Additive new type + accessor + tracing target; reverting removes observability but no semantic regression |
| E11 | Yes | Doc + additive test |
| E12 | Partially | WAL format version bump — reverting leaves pre-PR databases readable, post-PR databases unreadable. Same playbook as DR-5 snapshot v2 (pre-release clean break acceptable) |
