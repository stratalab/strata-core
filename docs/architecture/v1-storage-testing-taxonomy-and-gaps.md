# Storage Testing Charter: Bug Classes, Live Map, and Operating Discipline

Status: living document
Re-audited: 2026-06-17 (supersedes the 2026-06-12 snapshot; see "What changed since the last audit")
Companion to: `docs/architecture/v1-existing-test-inventory-and-porting-plan.md`,
`docs/architecture/v1-storage-testing-gold-standard-delta.md` (the SQLite/peer
delta), and `docs/architecture/archive/implementation-plans/storage-testing/` (the STH
execution program)

This is the charter behind the M\*T test tracks. It answers three questions and
keeps answering them as the system moves:

1. **Philosophy** — what makes a storage test suite world-class, stated as
   principles a reviewer can hold a PR against.
2. **The live map** — which *bug classes* exist, how the gold-standard engines
   cover each, and exactly where storage is strong or exposed today.
3. **The discipline** — the operating rules and gates that keep the suite
   world-class instead of merely large, and keep this map from drifting.

The porting plan inventories *what tests exist*. This charter classifies *what
bug classes exist* and *what bar we hold ourselves to*. Cite it when scoping a
test slice so coverage is reasoned about by bug class, not by file count.

Source basis: a deep, adversarially-verified audit of SQLite (the documented gold
standard) and the durability-critical peers — FoundationDB, TigerBeetle, RocksDB,
etcd/Antithesis — plus the crash-consistency literature (ALICE/OSDI'14,
CrashMonkey+ACE/OSDI'18, Pathfinder/OOPSLA'25) and recent DBMS-testing work
(WriteCheck/PVLDB'25, QPG/ICSE'23), mapped against a full re-audit of the
storage test surface (39 integration targets, 28 fuzz targets, 70 inline
test modules, 39 golden vectors, the testkit, and the fault seams). The
SQLite/peer comparison and its prioritized adoptions live in the companion
gold-standard delta doc.

## Philosophy: what makes a test suite world-class

A world-class suite is not the largest one; it is the one that holds these
principles without exception. Each is a question a reviewer may ask of any
storage change.

1. **Test by bug class, not by file count.** Coverage in one class says nothing
   about another — the techniques barely overlap. Every feature is scoped
   against the twelve classes below, and a green suite in nine of them is not
   evidence about the other three.
2. **The oracle is the test.** A test that asserts "it ran" or "it reopened" is
   theatre. Assert the *right* result against an independent model. Silent
   wrong-result and silent data-loss bugs die only to oracles, never to
   smoke tests.
3. **Sweep, don't sample — and where the space is bounded, exhaust it.** Where a
   fault can occur at operation N, fail *every* N and verify integrity each time;
   hand-picked windows pass while the bug lives two operations over. And exploit
   the *small-scope hypothesis* (CrashMonkey/ACE): most crash bugs reproduce with
   ≤3 durable operations after an `fsync`, so for short durable sequences
   exhaustively enumerate the crash states rather than sampling them — random
   sweeps are for the long tail.
4. **Determinism is a contract, not a convenience.** Every failure must replay
   bit-exact from a printed seed. All nondeterminism — threads, time, I/O,
   randomness — sits behind a swappable seam (`Backend`, `MaintenanceExecutor`,
   `MaintenanceClock`, `CommitTimestampSource`). New nondeterminism that does not
   go behind a seam is a defect.
5. **The suite strengthens monotonically.** Every real bug becomes (a) a
   permanent regression test that fails before the fix, and (b) a new seed or
   input in the relevant generator corpus — so the bug can never be re-lost and
   the generators get smarter over time.
6. **Extending the harness is part of shipping.** A change's definition of done
   includes the matching coverage in every class it touches. The M\*T track is
   not a follow-up; it converges within the milestone. (RocksDB's rule: you do
   not ship a feature without extending the stress test.)
7. **Design arguments are not coverage.** "Safe by construction" earns an
   adversarial test or it does not count. LMDB argued safety from design and
   shipped a toy suite; ALICE then found a real crash-consistency vulnerability
   in exactly the place the argument hand-waved.
8. **The map may not lie.** A testing map that has drifted is worse than none,
   because slices get scoped off it. Status is anchored to named evidence and
   re-audited on a cadence. This document itself drifted on two of its sharpest
   claims in under two weeks — see the corrections below — which is the whole
   reason principle 8 exists.

## The bug-class taxonomy

Testing across the gold-standard systems converges on ~12 distinct bug classes,
each needing a different technique.

| # | Bug class | Technique that catches it | Exemplar |
|---|---|---|---|
| 1 | Contract violations (component does the wrong thing per spec) | Unit/integration tests, property tests vs. a model | Everyone |
| 2 | Silent wrong results | Differential testing (run two ways, diff outputs) | SQLite SLT (7.2M queries vs. 4 other DBs); DuckDB optimized-vs-unoptimized |
| 3 | Crash/durability bugs (torn writes, fsync ordering, OS write reordering) | Crash-simulation backend: snapshot FS at op N, inject power-loss damage, verify atomicity; plus a write-ordering watchdog asserting no data write precedes its journal sync | SQLite (states any DB without this "likely contains undetected corruption bugs") |
| 4 | Silent data loss / recovery holes | Expected-state oracle: shadow model of what every key should contain; after crash, verify recovered state is a *prefix of acknowledged history* — not just "it reopened" | RocksDB db_stress (caught 3 real bugs incl. an undetected recovery hole) |
| 5 | Error-path bugs (I/O error, OOM, disk-full mid-operation) | Systematic injection sweeps: fail the Nth operation, verify integrity, increment N until clean — not hand-picked windows | SQLite (fail-once and fail-continuously modes, then `integrity_check`) |
| 6 | Failure-during-failure | Compound anomaly tests (I/O error *during* crash recovery) | SQLite |
| 7 | Hostile-input bugs (corrupt files, malformed data) | Fuzzing (incl. structure-aware DB-file fuzzing) + deliberate byte-flip corruption tests | SQLite dbsqlfuzz (~500M cases/day); LevelDB corruption_test |
| 8 | Trajectory/liveness bugs (backlog outgrows drain cadence, starvation, unbounded resources, deadlock-by-contract) | Closed-loop sustained workloads with liveness assertions — a dedicated liveness mode ("system eventually makes progress") separate from safety mode | TigerBeetle VOPR; RocksDB db_stress |
| 9 | Rare-interleaving and fault-combination bugs | Deterministic simulation: all nondeterminism behind seeded abstractions; any failure replays exactly; a driver sweeps interleavings + fault combinations | FoundationDB; TigerBeetle |
| 10 | Unstated filesystem-assumption bugs (atomic rename, ordered appends that POSIX doesn't promise) | ALICE-style crash-state enumeration under different FS persistence models — found 60 vulnerabilities across 11 mature systems incl. SQLite, LevelDB, LMDB | ALICE; adopted by hashicorp/raft-wal |
| 11 | Weak-test bugs (code executed but effects unchecked) | Coverage gates (SQLite: 100% MC/DC) + mutation testing (SQLite mutates ~20k branches and verifies the suite kills each mutant) | SQLite |
| 12 | Memory-safety / UB / races | Sanitizers, Miri, leak checks after every test | RocksDB (ASAN/TSAN/UBSAN continuously); SQLite (valgrind + leak checks per test) |

Two cultural facts worth internalizing: SQLite's test-to-source ratio is
**~590:1**, and RocksDB treats "extend the stress test" as a **required part of
shipping any feature**. Storage's ratio is **~0.8:1** (~127k lines of test
to ~158k of source) — healthy for a memory-safe, property-tested Rust core, but
a reminder that the leverage here is *technique density*, not volume.

## The live map: storage today (re-audited 2026-06-17)

The honest picture has moved. As of June 12 the system was "strong in
state-space, near-zero in time, scale, and oracle-verified recovery." It has
since **crossed into time and become simulation-capable**: the closed-loop
endurance suite (class 8) and the deterministic-simulation substrate (class 9)
both landed. Since then the **recovery oracle (4), systematic fault sweeps (5),
and FS-assumption enumeration (10)** have all landed (STH-1 / STH-2 / STH-3). The
former frontier — failure-during-failure (6), the write-ordering watchdog
(class 3 residual), the DST sweep driver (9), and the discipline / process
layer (11 / 12) — **closed 2026-07-16** (STH-5, STH-3b, STH-4, STH-7 under the
test coverage program). **All twelve bug classes now sit at their exit bar**;
what remains is tracked headroom inside individual rows, never an uncovered
class.

| Class | Status | Evidence (verified 2026-06-17) |
|---|---|---|
| 1. Contract | ✅ Strong | 8 property suites (~93 cases) with model-parity oracles (`src/testkit/api/{model,commit,branch,maintenance,diagnostics}.rs`, branch-LSM reference); ~348 source-guard/closeout checks; 2 conformance suites |
| 2. Differential | ✅ Strong *(was 🟡)* | Model-parity differential vs. a reference model, plus STH-6's config-sweep differential (`testkit/config_differential`): one seeded two-branch workload under {cache, durable-Standard, durable-Always} × {default, low-memory} asserting identical logical snapshots (keys, values, versions) at every checkpoint, a NoREC-style metamorphic point-read oracle inside each config, and model equality — green across the matrix + 16-seed nightly soak. **Its pressure cell found issue #2609** (EvaluateAndEnqueue livelock under sustained low-memory pressure), **fixed by #2613** via the shared `decide_flush_rotation` policy; the regression tests are live (un-ignored) and the mutation-on-diff gate vetted the fix. Cross-engine differential remains out of scope (no shared dialect) |
| 3. Crash/durability | ✅ Strong *(was 🟡)* | 8 crash windows + 19 service-fault routes cover *chosen* transitions; STH-3 wired the reordering/tearing backend into the durable path (the torn-write/truncate primitives are now **activated**, not dormant) and added the FS-model enumeration (see class 10), and STH-2 *swept* (not just enumerated) the backend-op crash points. The remaining bar — the write-ordering watchdog (STH-3 slice 3b) — landed 2026-07-16: `testkit/write_ordering_watchdog` observes the real operation stream and files a typed `WriteOrderingViolation` on any manifest/snapshot/table publish over unsynced WAL-segment bytes; violation detection is non-vacuously proven, and Always/Standard/rotation/recovery streams run clean (`tests/write_ordering.rs`, nightly). Process-level kill/reopen (the T5 bar) landed 2026-07-16: `testkit/process_crash` SIGKILLs a journaling child at chosen ack thresholds and holds the reopen to the recovery oracle — per-PR rounds + 200-round nightly soak |
| 4. Recovery oracle | ✅ Strong *(was ❌)* | STH-1 landed the shadow expected-state model (`testkit/recovery_oracle::{model, verify, workload}`): a durable workload records every acknowledged + in-doubt commit, and `classify_recovered` proves the reopened database is a *prefix of acknowledged history* — typed `LostAck`/`Phantom`/`TornBatch`/`Gap` violations, with `CrashFamily::{ZeroLoss, OnDiskDamage}` separating zero-loss from on-disk-damage kills. Crash mechanisms: clean `Drop`, `WalTruncate`, `WalCorruptByte` (`tests/crash_recovery_oracle.rs`). It is the reusable recovery post-condition for STH-2 (fault sweeps) and STH-3 (FS models). Per-transition recovery (~70 cases in `lifecycle/tests/recovery.rs`) remains the fine-step regression subset |
| 5. Error-path sweeps | ✅ Strong *(was 🟡)* | STH-2 systematic sweep (`testkit/fault_sweep`): a baseline trace drives "fail the Nth backend op, verify via the STH-1 recovery oracle, increment N" over the V1-reachable ops `{append, sync, publish, delete}`, fail-once *and* fail-continuously; plus disk-full (NoSpace position sweep + byte-quota, WAL uncertain-commit recovery) and budget/memory exhaustion (`LowMemory` → retryable `StoragePressure` → drain → resume), each oracle-verified. Seeds scale with the case budget for a deep `#[ignore]` soak; the 19 enumerated windows remain as the fine-step regression subset. Post-V1 (unreachable in V1, durable path publishes via `publish_object`): `ConditionalCreate/Update`, `WriteObject`; deferred compaction-input deletion → STH-5 |
| 6. Failure-during-failure | ✅ Strong *(was ❌)* | STH-5 landed the compound harness (`testkit/compound_faults`): stage a first failure (faulted checkpoint publish + crash without close), then sweep a *second* fault across every backend op the reopen's recovery path performs — once and continuously, each case reopening a byte-copy of the same crashed store — plus fault sweeps inside every maintenance publish transition (flush/checkpoint/compact/prune windows measured per-op on a clean baseline). Every case asserts a typed trace (drain error, failed summary, or a recorded source error on a completed pass — the manifest-publish-debt path), a clean reopen that is oracle-valid (STH-1 prefix check), and a successful resume commit. Bounded grid per-PR (in-crate tests), full grid + seed-scaled soak nightly (`tests/compound_faults.rs`); a 2,000-case local soak ran clean |
| 7. Hostile input | ✅ Strong *(now continuous)* | 30 fuzz targets (decoders + generated-script state machines — the script-through-harness targets are the structure-aware layer) with corpora; 41 golden vectors; deliberate XOR bit-flip corruption tests (`table/tests/reader.rs`) and corrupt-log/corrupt-snapshot recovery cases. STH-7c wired continuous execution: `.github/workflows/fuzz.yml` runs every target nightly with a persistent corpus (coverage compounds across nights); crash artifacts upload on failure |
| 8. Trajectory/liveness | ✅ Strong *(was ❌)* | `api/tests/background_scale.rs` runs closed-loop sustained load against a real `StorageRuntime` with thresholds scaled ~1000× (`scaled_closed_loop_test_profile`, ~4 MB budget). Asserts: commits never permanently fail, queue drains (`pending==0`, `queue_full==0`), WAL bounded (peak ≤16 / final ≤4 segments, ≤128 KB), shape converges (L0 ≤3). `stress.rs` still drives only service scripts — its role is now superseded for liveness |
| 9. Deterministic simulation | ✅ Strong *(was 🟡)* | The retrofit landed (`MaintenanceExecutor`/`InlineMaintenanceExecutor`, `MaintenanceClock`/`ManualMaintenanceClock`); `DeterministicInline` drives the *production* `Background` path with no worker threads, replay-identical and threaded-vs-inline parity proven. STH-4 (4b+4c+4d) then built the **seeded sweep driver** (`testkit/simulation`): client-op × maintenance-cadence × clock interleavings over the production path crossed with the fault-combination dimension (STH-2/STH-3 fault + crash substrates), each step safety- (recovery oracle) + liveness-checked, bit-exact replay (the `same_seed_replays_bit_exact` guard) + seed-scaled `#[ignore]` soaks. **The DST found and the team fixed two durability bugs.** (1) A publish fault during a batched `[Checkpoint, Flush]` drain advanced the WAL-replay floor past rows whose table manifest never published → reopen recovered nothing — **fixed (2026-06-18):** a checkpoint defers on outstanding table-manifest publish debt. (2) A power-loss `SplitRename` crash dropped a delta checkpoint's table-manifest base → recovery installed an orphaned delta (a non-contiguous `Gap`) — **fixed (2026-06-19):** the checkpoint records its delta base floor durably and recovery requires the base, recovering a clean prefix as `DataLoss`. Both regressions are un-ignored + green and the 3000-seed fault soak runs clean end-to-end. Residual perf-trace clock injection (4a) is descoped |
| 10. FS-assumption enumeration | ✅ Strong *(was ❌)* | STH-3 `ReorderingBackend` (`testkit/reordering_backend.rs`) records each object's unsynced boundary and materializes all four FS persistence models — ordered+atomic loss of the unsynced tail, reordered/partial appends, garbage (torn) unsynced tail, split rename (vanished publish) — on the real files via the now-activated `truncate_object` / `corrupt_object_byte` / `drop_object_file` primitives. `testkit/fs_models` sweeps seed × model × crash point × {Standard, Always}, each oracle-verified: `Always` loses nothing under any model; `Standard` recovers a clean prefix; a torn tail is fail-loud or a clean prefix, never silently wrong (`tests/fs_persistence_models.rs`, seed-scaled `#[ignore]` soak). No vanishing-WAL incident is on record — only the generic missing-object fallback |
| 11. Coverage/mutation | ✅ *(was ❌; MC/DC remains headroom)* | STH-7b: nightly coverage job publishes the per-crate table and gates the workspace region floor (73.0%, ratchet-up-only, baseline 73.6% 2026-07-16); per-PR `mutation-on-diff` CI job runs `cargo-mutants` over exactly the lines each PR touches — an un-killed mutant fails the job. Full-tree mutation campaigns and MC/DC on the durable core remain tracked headroom, not claimed. **Caveat (#3227):** cargo-mutants recognizes a `Result` return only by the literal type name (`fnvalue.rs`, `path_ends_with(path, "Result")`), so a storage function returning a `Result` *alias* — `BackendResult`, `BranchRuntimeResult`, `StorageApiResult`, `WalServiceResult`, `LayoutResult`, `CheckpointServiceResult` (~890 signatures) — gets only *unviable* whole-body-replacement mutants (`StorageApiResult::new()` and the like never compile), so the "does any test notice if this function does nothing?" mutant is not judged for them. That body-replacement assurance rests instead on this crate's own strength mechanisms — the deterministic simulation, dual-mutation fuzz, golden-format and conformance suites and their exact-facts pins (rows 1–10, which the exclusion configs already name as the strength mechanism) — plus the per-run `mutation-vacuity` job (#3350), which fails any diff whose storage mutants are *all* unviable. Engine and executor instead spell the return type out as `Result<T, E>`, guarded against regressing by `return_types_spell_the_result_out_for_the_mutation_gate` (#3343), so the gap is storage-only and accepted deliberately rather than closed with a ~890-signature rewrite of the most correctness-sensitive crate |
| 12. Memory safety | ✅ *(was 🟡)* | `#![deny(unsafe_code)]` + STH-7a nightly lanes: Miri (strict provenance) over core + the storage format layer, ASAN over storage and engine, LSAN leak gates (storage via the `leak_static` registry, engine bare), TSAN over storage. Per-test allocator counters remain headroom; LSAN whole-process leak checking is the shipped leak assertion |

Reading of the map: storage has **world-class state-space correctness**
(classes 1, 7, format stability), **now-strong trajectory coverage** (class 8),
**oracle-verified recovery (4), systematic and resource-exhaustion fault
injection (5), durability realism via FS-model enumeration (10), and the
deterministic-simulation driver (class 9)** — the last found and drove fixes for
two durability bugs and now passes a clean 3000-seed soak.
The remaining exposure is concentrated in **compound failure (6)**, the
**write-ordering watchdog (the residual of class 3)**, and the **process
gates (11/12)**.

### What changed since the last audit (and why principle 8 exists)

- **Class 9 was "Missing — window closing."** The five-step retrofit it
  prescribed (executor trait, `Arc<dyn>` controller, inline executor, clock
  handle, re-express `Background` as the deterministic path) has all landed. The
  warning is obsolete; the door is open.
- **Class 8 was "Missing."** The priority-#1 endurance suite it called for now
  exists in `background_scale.rs`.
- **Class 4 was rated "Partial."** Sharpened to **Missing**: per-transition
  recovery exists but the *defining* oracle does not, and rating a class by
  adjacent coverage is exactly how a map starts to lie. **Since closed by STH-1**
  (the prefix-of-acknowledged-history oracle) — now ✅.
- The old priorities list is therefore stale at the top (its #5 is done, its #1
  is done). Re-derived priorities are below.
- **The STH program then closed the recovery + fault + FS-realism frontier
  (2026-06):** STH-1 landed the recovery oracle (class 4 → ✅), STH-2 the
  systematic fault sweeps (class 5 → ✅), and STH-3 the reordering/tearing backend
  + FS-model enumeration (class 10 → ✅; class 3 advanced — residual is the
  write-ordering watchdog, STH-3 slice 3b).
- **STH-4 (4b+4c+4d) then built the deterministic-simulation sweep driver** over the
  production path — seeded client-op × maintenance-cadence × clock interleavings
  crossed with the fault-combination dimension, oracle- + liveness-checked each step,
  bit-exact replay + soak. **The DST found and the team fixed two durability bugs.** (1) A
  publish fault during a batched checkpoint+flush drain lost committed data — **fixed
  (2026-06-18):** a checkpoint defers on outstanding table-manifest publish debt. (2) A
  power-loss `SplitRename` crash dropped a delta checkpoint's table-manifest base, so
  recovery installed an orphaned delta (a non-contiguous `Gap`) — **fixed (2026-06-19):**
  the checkpoint records its delta base floor durably and recovery requires the base,
  recovering a clean prefix as `DataLoss`. Both regressions are un-ignored + green and the
  3000-seed fault soak runs clean end-to-end.

### Calibrated against the gold standard (2026-06-17)

A deep audit of SQLite and the durability-critical peers produced the companion
delta doc. Its load-bearing conclusions, folded into the exit bars and discipline
below:

- **Two camps, three oracle styles.** Coverage-maximizing (SQLite: 100% MC/DC +
  deterministic fault sweeps; oracle = a self-contained `integrity_check`) and
  deterministic-simulation (FoundationDB/TigerBeetle/etcd; oracle = declarative
  invariants), with RocksDB in the middle (oracle = an external "expected-values"
  model). Our recovery oracle is external-model; world-class is having all three —
  so we also build a *self-contained structural integrity check* and assert
  *declarative invariants* inside the DST driver.
- **Bound, then exhaust.** The small-scope hypothesis (≤3 durable ops after an
  `fsync`) makes bounded *exhaustive* crash enumeration provably high-yield;
  folded into principle 3 and the class-3/4 exit bars.
- **Our substrate choice is externally validated.** ALICE tested 11 mature DBs;
  SQLite-in-WAL was the *only* one with zero crash-consistency vulnerabilities, and
  our single-threaded core + landed executor/clock seams put us where TigerBeetle
  is — a DST position most databases never retrofit into.
- **Durability is *our* space.** The modern query-fuzzing literature optimizes for
  logic/crash/perf bugs and contains *zero* treatment of durability or crash
  recovery; StrataDB's hardest problem lives in the crash-consistency lineage that
  owns it. This is the spine of the "How we test StrataDB" story.
- **Net-new techniques to adopt** (detail + priority in the delta doc): 100% MC/DC
  on the durable core via `testcase!`/`always!`/`never!` macros run three ways; a
  self-contained integrity check; bounded-exhaustive crash enumeration; metamorphic
  oracles (no reference engine needed); a final-state WSS oracle for branch-aware
  MVCC concurrency; requirements-to-test traceability; and a human release
  checklist.

## Deterministic simulation: the door is open (class 9)

Deterministic simulation testing (DST) is the single most powerful technique in
the taxonomy — it catches the rare interleavings and fault combinations nothing
else reaches, and makes every failure replay exactly. It is normally a
near-impossible retrofit, because it requires *all* nondeterminism behind
swappable abstractions.

Storage now satisfies the preconditions **by construction**, verified
against the current tree:

- **I/O** is behind the `Backend` trait.
- **Threads** are behind `trait MaintenanceExecutor` (`lifecycle/background.rs`);
  `BackgroundRuntimeController` holds `Arc<dyn MaintenanceExecutor>`
  (`api/runtime/background.rs`), with `InlineMaintenanceExecutor` running drains
  synchronously under step control and `ThreadedMaintenanceExecutor` for
  production.
- **Decision time** is behind `trait MaintenanceClock` with `RealMaintenanceClock`
  / `ManualMaintenanceClock`. Block-wait deadlines, pressure slowdown, and
  drain-round limits all read the clock — proven by the manual-clock tests
  (`deterministic_inline_block_pressure_wait_uses_manual_clock_executor`,
  `…manual_clock_runtime_limit_stops_and_resumes_drain_round`).
- **Data-plane time** is behind `CommitTimestampSource`.
- Crucially, `DeterministicInline` drives the **production** `Background` lifecycle
  path (`deterministic_inline_uses_background_drive_path_without_worker_threads`),
  and `threaded_and_inline_background_executors_converge_on_compaction_shape`
  shows the two executors reach the same state — so deterministic tests exercise
  real code, not a parallel path.

**What remains** is to *use* the substrate:

1. **A seeded sweep driver** — the VOPR-equivalent. A loop that, under a seed,
   randomizes background task ordering, clock advancement, and fault combinations
   against the production path, asserting safety + liveness invariants every step.
   The primitives exist (`run_inline_replay_scenario` proves bit-exact replay);
   there is no explorer on top of them yet.
2. **Replay-on-failure** — print the seed on every failure; a failing seed
   reproduces the exact trajectory.
3. **Residual clock injection** — a handful of production `Instant::now()` calls
   remain in `lifecycle/{cache,durable/maintenance,compaction,rewrite_publication}.rs`.
   These are perf-trace *duration measurements* (`inline_start.elapsed()`), not
   control-flow decisions, so they do not affect state determinism — but they do
   make perf-trace timing facts non-reproducible. Route them through the clock
   for fully deterministic replay of timing, not just state.

The strategic point has inverted: the executor refactor was the hard part and it
is done. The remaining work is additive and bounded, and it unlocks the most
valuable class in the taxonomy.

## Operating discipline: how we stay world-class

Technique without discipline decays. These rules turn the philosophy into
enforced practice.

- **Definition of done.** A storage change names the bug classes it touches and
  lands the matching coverage in the same milestone. A reviewer may reject a
  change that adds a durable path without a crash/recovery case, or a background
  behavior without a liveness assertion.
- **Regression protocol.** Every real bug (production, audit, or fuzz find)
  produces a test that fails *before* the fix and a new seed/input in the
  relevant corpus. No bug is fixed without both.
- **Determinism contract.** New nondeterminism goes behind one of the four seams.
  Simulation-mode tests run the production path. Failures print their seed.
- **Assert effects, not execution.** Reject tests whose only assertion is that an
  operation returned `Ok`. Tests assert against a model or an oracle, and on
  error *class and code*, never display text.
- **Anti-drift.** Each status cell above is anchored to named artifacts (files,
  guard tests, seams). The map is re-audited at every milestone close, and the
  audit is a deliverable, not a vibe. Where feasible, prefer *derivable* status:
  the source-guard pattern already pins architecture; extend a closeout guard to
  assert the testing map's named artifacts exist so a deleted suite breaks CI,
  not a reader's trust. The same idea, applied to requirements (SQLite's
  requirements-to-test traceability): each clause of the storage-format spec and
  each code in the error contract carries a stable ID, and a guard asserts a test
  exercises it — so an unverified requirement is a CI failure, not a surprise.
- **CI gates.** Today CI enforces fmt, `clippy --all-targets --all-features`,
  `cargo deny`, dependency-direction guards, feature-powerset check+test
  (`cargo hack`), the full workspace test, and doc tests. The world-class
  additions — coverage gate, mutation testing, a Miri job on the unsafe-free
  core, and a flake-detecting test runner — are tracked as classes 11/12 below.
- **Coverage standard.** The durable core (format, WAL, recovery, commit) is held
  to **100% MC/DC** — the bar SQLite holds to DO-178B — because a missed branch
  there loses data. The enabling machinery is the `testcase!`/`always!`/`never!`
  macros and running the suite three ways (release / debug-assert / coverage) with
  identical results, which also separates defensive code from logic errors. Outer
  layers stay on line/branch coverage.
- **Release gate.** Beyond the automated gates, a storage release passes a short
  human checklist (the Checklist Manifesto model SQLite uses) — the place to ask
  "is this really right?" about durability and format invariants that no automated
  check fully owns. The checklist grows as new problems are discovered.

### Per-class ownership and exit bar

The exit bar is what "world-class for this class" concretely means. Status is
from the live map.

| Class | Status | Exit bar (definition of world-class) |
|---|---|---|
| 1 Contract | ✅ | Every public operation has a model-parity oracle; source guards lock the D4 surface. *(held)* |
| 2 Differential | ✅ | Config-sweep differential: cache vs. durable (and budget variants) produce identical logical results on the same workload, plus the prefix-scan == point-read metamorphic oracle — landed (STH-6); found #2609 on its first sustained-pressure run (fixed, #2613; regressions live). Remaining headroom (tracked, not blocking): read@V == replay-to-V metamorphic pair, scheduling-policy axis beyond EvaluateAndEnqueue |
| 3 Crash | ✅ | Reordering/tearing `Backend` wrapper wired in (STH-3, done); crash states *bounded-exhaustively* enumerated for short durable sequences (≤3 ops post-`fsync`), randomly swept for the tail; and the write-ordering watchdog asserting WAL sync (or authorized segment publish) precedes dependent publishes — landed (STH-3b) |
| 4 Recovery oracle | ✅ | Shadow expected-state model landed (STH-1): after a kill at a *random* / bounded-exhaustive point, recovered state is a verified prefix of acknowledged history across every durable operation. *(Follow-on: a self-contained structural integrity-check analog validating on-disk invariants without the workload model.)* |
| 5 Error sweep | ✅ | "Fail backend op N, sweep N, verify via the recovery oracle each" over the V1-reachable steps, fail-once + continuously, plus ENOSPC and budget-exhaustion modes — landed (STH-2) |
| 6 Failure-during-failure | ✅ | Fault injected during recovery, compaction, and checkpoint; integrity and the recovery oracle still hold — landed (STH-5): second-fault sweep over the traced recovery op stream + per-window maintenance publish faults, every case typed + oracle-valid + resumed |
| 7 Hostile input | ✅ | Every decoder + state machine fuzzed continuously — landed (STH-7c: nightly persistent-corpus lane). Script-through-harness targets are the structure-aware layer; `arbitrary`-derived whole-DB-file generators remain headroom |
| 8 Liveness | ✅ | Closed-loop endurance per mode and per maintenance kind, with bounded-resource + progress assertions, in CI seconds. *(held; broaden coverage)* |
| 9 DST | ✅ | Seeded interleaving + fault-combination driver over the production path; replay-on-failure; nightly long-seed soak. Full driver landed (STH-4 4b+4c+4d) **and found + fixed two durability bugs**: a publish fault during checkpoint+flush (checkpoint defers on table-manifest debt) and a power-loss `Gap` at seed 155 (checkpoint records its delta base floor; recovery requires the base, recovering a clean prefix as `DataLoss`). Both regressions green; the 3000-seed fault soak runs clean end-to-end |
| 10 FS-assumption | ✅ | ALICE-style enumeration over rename/append persistence models on the durable path — landed (STH-3): ordered+atomic, reordered/partial appends, garbage torn tail, split rename, each oracle-verified across crash points and durability modes |
| 11 Coverage/mutation | ✅ | Coverage floor gate (ratchet-up-only) + diff-scoped mutation testing per PR — landed (STH-7b). The suite already runs three ways (debug-asserts per-PR, release + coverage nightly, 7a). Headroom (tracked, not claimed): 100% MC/DC on the durable core, `testcase!`/`always!`/`never!` macros, full-tree mutation campaigns |
| 12 Memory safety | ✅ | Miri on the unsafe-free core + storage format layer, ASAN/LSAN/TSAN lanes, leak checking via the registered-fixture discipline — landed (STH-7a + TCP1.7). Per-test allocator counters remain headroom |

## Priorities (re-derived 2026-06-17, cheapest leverage first)

These priorities are executed by the **Storage Testing Hardening (STH)** program —
a sequenced series of implementation plans, one per gap, each driving a class to
its exit bar: `docs/architecture/archive/implementation-plans/storage-testing/README.md`.

**Progress (2026-06):** priorities 1 (class 4), 2 (class 5), and 3 (DST sweep driver,
class 9) are **done** — the DST also found and drove fixes for two durability bugs and
now passes a clean 3000-seed soak — and priority 4 is **partial** (reordering backend +
FS-model enumeration done, class 10 → ✅; the write-ordering watchdog is the residual).
**Progress (2026-07-16):** the remaining priorities are done — the priority-4
residual (write-ordering watchdog, TCP1.3), priority 5 (failure-during-failure,
TCP1.2), and priority 6 (discipline gates, TCP1.1/1.5: Miri/ASAN/LSAN/TSAN
lanes, coverage floor, mutation-on-diff, scheduled fuzz, charter guard). The
priority list below is retained as the program's historical rationale.

1. **Recovery oracle (class 4). ✅ Done — STH-1.** The deepest remaining correctness hole now that
   liveness has landed. Build a shadow expected-state model on top of the existing
   crash harness and assert prefix-of-acknowledged-history under kill points that
   are bounded-exhaustive for short durable sequences and random for the tail; pair
   it with a self-contained structural integrity check usable without the model.
   This is the technique that catches silent recovery holes — the one bug class
   where "it reopened" hides real data loss.
2. **Error-injection sweeps (class 5). ✅ Done — STH-2.** Generalize the 19 enumerated windows and
   8 backend steps into "fail op N, sweep N, verify integrity each time." The
   seams already exist; this is mostly a loop. Add disk-full (ENOSPC) and
   budget-exhaustion modes.
3. **DST sweep driver (class 9). ✅ Strong — STH-4.** The full seeded explorer over
   the inline-executor + manual-clock substrate **landed** (4b+4c+4d): client-op ×
   maintenance-cadence × clock interleavings crossed with the fault-combination
   dimension (STH-2/STH-3 fault + crash substrates), each step oracle- +
   liveness-checked, with bit-exact replay + seed-scaled soaks. **The DST found and the
   team fixed two durability bugs:** (1) a publish fault during a batched checkpoint+flush
   drain lost committed data — **fixed** (checkpoint defers on table-manifest publish
   debt); (2) a power-loss `SplitRename` crash dropped a delta checkpoint's table-manifest
   base, so recovery installed an orphaned delta — **fixed** (the checkpoint records its
   delta base floor durably; recovery requires the base and recovers a clean prefix as
   `DataLoss`). Both regressions un-ignored + green; the 3000-seed fault soak runs clean
   end-to-end — class 9 closed.
4. **Torn-write / reordering backend + write-ordering watchdog (classes 3, 10). 🟡 Partial — STH-3.**
   The reordering/tearing `Backend` wrapper (reorders unsynced writes, tears them,
   fills unsynced regions with garbage) and the FS-model enumeration **landed**
   (slices 3a/3c; class 10 → ✅), activating the `corrupt_object_byte` /
   `truncate_object` primitives. **Residual:** the watchdog asserting WAL sync
   precedes dependent writes (slice 3b), which closes class 3. (No vanishing-WAL-
   segment incident is on record — only the generic missing-object fallback; see
   the correction in the STH-3 plan.)
5. **Failure-during-failure (class 6).** Compose the now-rich fault and crash
   harnesses: inject a fault during recovery/compaction/checkpoint and assert the
   recovery oracle from priority 1 still holds.
6. **Discipline gates (classes 11, 12).** MC/DC on the durable core via the
   `testcase!`/`always!`/`never!` macros, mutation testing (`cargo-mutants`), a Miri
   job on the unsafe-free core, and the self-verifying testing-map + requirements
   guard. M10-shaped overall, but the Miri/sanitizer job is cheap to stand up now
   and pays back immediately.

The remaining gold-standard adoptions that are not yet sequenced into a priority —
metamorphic oracles (class 2), the final-state WSS concurrency oracle, and the
human release checklist — are detailed with their slot-in points in the companion
delta doc; fold them in as the owning classes are worked.
