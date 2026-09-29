# #3663: product-surface model-based testing (TCP4.16)

Status: **PROPOSED, 2026-09-28** (milestone 1.2.7). This extends Phase 4 of the V1 test-coverage program (`docs/architecture/v1-test-coverage-program.md`) with one new slice family, TCP4.16. Every existing generator named below was verified on `main` @ `7a6ab80e`.

## 1. Problem

The question that started this: could we write scripts of expected behavior for every command, every permutation of its options, and their combinations, perhaps millions of cases, and run them against every release?

The answer this document takes is yes to the goal and no to the method.

**Enumeration does not work here, for two reasons.**

1. **The oracle is the bottleneck, not the volume.** A script that says "run X, expect Y" needs someone to author Y. The IDL holds about 138 commands (derive the count from `generated/command-index.json`; never restate it). Each has options, and each interacts with branches, `as_of`, the other capabilities and the session lifecycle. That state space cannot be enumerated, and hand-authored expectations top out in the thousands. A million scripted cases would mostly re-test the same code paths.
2. **The bugs are not where enumeration looks.** The two defects the second #3596 review found in 1.2.6 were not about which command or which flags:
   - #3661 needed a checkpoint, a manifest publish reported uncertain after it became visible, a reopen, and the queued reconcile. The reconcile deleted a live timeline segment, and a strict reopen then refused.
   - #3662 needed a crash between the segment publish and the snapshot publish, then a read-only reopen. The segments were stranded.

   Both were **sequences across a session boundary with a fault in the middle**.

**What Phase 4 already covers, and where each generator stops:**

| slice | what it generates | where it stops |
|---|---|---|
| 4.1a IDL conformance (`generated/conformance_cases.rs`) | per-command wire round trips, unknown-key rejection, declared error envelopes | one command, no state |
| 4.2 differential (`differential_{kv,json,event,graph,vector}.rs`) | seeded op sequences diffed against RocksDB and other reference stores | one capability, one session, no lifecycle, nightly only |
| 4.10 logic oracles (`oracle_{pivot,partition,dqe,deopt,graph_mr}.rs`) | containment and partition identities | one capability, one query shape |
| 4.11 whole-DB DST (`storage/src/testkit/simulation/whole_db.rs`) | commits, forks, maintenance, clock, faults, crash epochs | the storage layer, below engine semantics and the wire |
| 4.12 histories (`testkit/simulation/history.rs`) | lineage and isolation histories | the storage session layer |

**The gap.** Nothing drives long multi-command sequences through the real product surface (the executor, the JSON wire), across capabilities, with close, reopen, crash, idle and faults as ordinary steps, and checks the whole database against a model of its semantics.

## 2. Design

### 2.1 Shape

```text
seed ─► generator (IDL-driven) ─► action ─► executor (real product path)
                                    │                 │
                                    └─► reference model ◄── compare response
                                                      │
                            periodic: full-state digest, as_of spot checks,
                            invariants that need no model
failure ─► shrink the action sequence ─► minimized trace ─► corpus regression
```

The harness lives in `crates/executor/tests/model/` behind a `model-testing` feature, alongside the parity and differential suites, and drives `Executor` through `Command` values built from wire JSON, as `parity/support.rs` does. Wire JSON is the input form, so what is tested is what the SDKs and CLI send.

### 2.2 The reference model

The model is a deliberately naive, in-memory implementation of product semantics. It needs to be obviously correct, not fast.

- **Versions.** One global commit counter, and a per-branch history per key of `(version, value | tombstone)`. `as_of` by version is a lookup.
- **Time.** Commit timestamps are read back from the real receipts, never predicted, and recorded against the version. `as_of` by time is resolved through that record, following the locked boundary contract (out-of-window raises; after-latest does not clamp).
- **Branches.** A fork copies the parent's visible state and history at the fork version. Delete and recreate follow the branch-generation rules. Compare, preview and promote are modelled as set operations over the two branches' states, with `Strict` refusing on conflict and `SourceWins` taking the source.
- **Capabilities.** Each capability is layered over one state, one module per capability:
  - **KV:** an ordered map per space.
  - **JSON:** document-level values, matching the V1 document-level merge.
  - **Event:** per-type append logs.
  - **Graph:** node and edge maps with typed adjacency.
  - **Vector:** exact brute-force search over small collections. Approximate indexes get containment properties instead of equality (§2.5).
- **Errors.** For each action the model predicts success or a specific error code, such as `not_found`, `already_exists` or `invalid_argument`. It compares the code and class, never display text (hard rule 29).

The model is the investment. It is also a feature: a semantic change that does not update the model fails the per-PR lane, so the model doubles as an executable statement of the semantics.

### 2.3 Coverage is derived from the IDL, not asserted

Every command in `command-index.json` must be in exactly one bucket, recorded in `idl/v1/model-coverage.yaml`:

- **modelled:** response equality against the model;
- **property:** checked only by invariants, such as analytics, sampling, diagnostics and `admin.*` summaries;
- **excluded, with a reason:** for example, network or provider-bound commands (`hub.*`, `inference.*`) or build-gated ones (`arrow.*`).

A guard fails when a command is missing from the ledger. The **excluded** list is shrink-only, following the precedent of `uncovered-commands.yaml` and `unreplayed-error-codes.yaml`. A new command therefore ships with a model entry or an explicit, reviewed exclusion.

### 2.4 The action alphabet

Actions come in four groups. The generator's weights are per group and per family.

1. **Commands.** Commands are generated from the IDL argument schemas over small, edge-heavy domains, so that operations collide. The domains reuse `differential_kv.rs`'s alphabet: NUL, 0xff, shared prefixes, empty and maximum lengths. Key, document and branch pools are small on purpose.
2. **Invalid commands.** These are schema-guided mutations: a wrong type, a missing field, an out-of-range value, an unknown key, or a reference to a deleted branch. Each must produce the error code the model predicts. This extends 4.8 from fixed fixtures to generated inputs.
3. **Lifecycle:**
   - `Close` and `Reopen`, in durable mode;
   - `Abandon`, a drop without close (the in-process crash);
   - `Idle(ms)`, an advance of the manual maintenance clock followed by a drain;
   - `Checkpoint` and `Flush` requests;
   - `ReopenReadOnly`.
   Cache-mode runs omit the durable actions.
4. **Faults.** A seeded fault is armed on the storage backend at a chosen operation: an I/O failure, a `VisibilityUnknown` publish, disk full, or a torn write. These use the 4.9 taxonomy and the storage testkit decorators that 4.11 already stacks.

### 2.5 Oracles

- **Step equality.** Each command's response is normalized and compared with the model's. Normalization maps versions and timestamps through the receipt record, and IDs through a harness-side table, instead of stripping them, so a wrong version still fails.
- **State digest.** Every N steps, and after every lifecycle action, every branch and capability is listed through the product surface and compared with the model's full state. A few `as_of` reads at randomly chosen recorded versions are compared too.
- **Uncertain outcomes.** After a fault, or an `Abandon` during a write, the model does not guess. It keeps both candidate states: the operation applied and not applied. The next digest must equal one of them, the matching one becomes the model, and the run continues. A digest that matches neither is a failure. This is what makes #3661-class bugs visible from the product surface: after the reconcile and reopen, the database either refuses to open or loses history, and neither candidate state predicts that.
- **Model-free invariants,** checked on every run:
  - A reopen is idempotent: the digest before `Close` equals the digest after `Reopen`.
  - `as_of(v)` answers the same before and after any lifecycle action.
  - A fork's child reads equal the parent's reads at the fork version.
  - After `Close`, `Reopen` and `Idle` drain, `admin.storage --audit` reports unreferenced bytes 0 and at most one superseded snapshot, and its totals match the files on disk. #3662 fails this invariant.
- **Approximate search.** For vector collections under an ANN index, the result count is at most k, every hit exists in the model, and every exact-mode result is contained (the 4.10 pivot pattern). Equality is used only for exact collections.

### 2.6 Pairwise option coverage

For each command, the harness derives from the IDL argument schema the option dimensions: enum values, booleans, and optional fields present or absent. It builds a covering array in which every pair of option values appears at least once. That comes to tens of cases per command, not a cross product, and it catches most flag-interaction bugs. The arrays drive two things:

- a stateless per-PR sweep, run against a small seeded fixture state;
- a share of the argument choices in the stateful generator.

The arrays are generated at test build time from the IDL, never committed, and never counted in prose.

### 2.7 Determinism, shrinking, replay

- **Determinism.** One seed determines the action sequence, the argument values and the fault schedule. Maintenance runs `DeterministicInline` under the manual clock.
  - Commit timestamps are the only wall-clock input. The model reads them back rather than predicting them.
  - A time-travel action that needs a specific instant uses one the harness has observed.
  - Every failure message carries the seed and the step index.
- **Shrinking.** Delta debugging over the action sequence: drop chunks and then single steps while the same failure persists, re-running the model each time. Then simplify the argument values toward the smallest ones in their domain.
- **Replay.** The minimized trace is written as JSON Lines in the `corpus/` format, and committed traces are replayed on every PR as fixed regressions. A shrunk failure becomes the bug's regression test and is filed under the bugs-get-issues rule.

### 2.8 Tiers

| tier | where | budget | faults |
|---|---|---|---|
| per-PR | the `test` lane | fixed seeds, about 3 minutes total, plus committed traces and the pairwise sweep | a fixed subset |
| nightly | the soak lane | fresh seeds, long sequences, both modes | full taxonomy |
| pre-release | a `workflow_dispatch` on the release-prep branch | at least 1 M actions across seeds, report attached to the release PR | full taxonomy |

The pre-release campaign becomes a step in the local `/release` runbook. A red campaign blocks the tag the same way a red `format-gate` does.

### 2.9 Backtest before trusting it

A harness that finds nothing proves nothing, which is the program's gate 7 (seed re-find).

Before the lane is declared live, it runs against the parent commits of fixed bugs from the 1.2.4–1.2.6 milestones that are reachable from the product surface. #3661 and #3662 are the first two targets. The re-find rate is recorded in this document.

A bug the harness cannot re-find is a missing action, domain or invariant, and it is fixed before the lane goes live.

## 3. Engine testkit hooks required

The executor lane needs four hooks that do not exist above storage today. Each sits behind the engine `testkit` feature, with a mutation `exclude_re` and a gating guard, following the #3290 dev-dependency lesson:

1. **Deterministic maintenance and a manual clock** on the durable open options, forwarded to storage's `DeterministicInline` and `MaintenanceClock`.
2. **Opening over a caller-supplied storage backend,** so the storage testkit fault and reordering decorators can be stacked under a product-level database.
3. **Abandon.** Tear down without the close path, leaving what a crash leaves on disk. Workers stop and the lock releases, but there is no drain, no checkpoint and no WAL close.
4. **Arming a fault on the backend** at a seeded operation index, exposed through hook 2's decorator handle.

None of these hooks is reachable from the wire, and each is a `pub` item only under the `testkit` feature.

## 4. Slices (each PR pairs implementation with its tests)

| slice | implementation | tests and exit |
|---|---|---|
| S0 | This design and a TCP4.16 row in the coverage-program ledger | Docs. Exit: merged. |
| S1 | Harness skeleton: action enum, executor driver, seed and step reporting, JSON Lines traces, shrinker, KV and branch model (fork, delete, recreate), state digest, `model-coverage.yaml` and its guard | Per-PR lane over KV and branches in cache mode. The shrinker is proven on a planted engine bug: it reduces a long sequence to at most 10 steps. The guard is proven by removing a ledger row. |
| S2 | Engine testkit hooks 1 and 3; durable mode; lifecycle actions; reopen idempotence, `as_of` stability and footprint invariants | Plants: a reopen that drops a branch fails idempotence, and a close that skips the reclaim drain fails the footprint invariant. |
| S3 | JSON and event models | Differential equality with the model; a planted document-merge bug is found. |
| S4 | Graph model: nodes, edges, typed neighbors and cursors. Analytics are property-only. | A planted cursor off-by-one is found (the #3458 and #3489 class). |
| S5 | Vector and space models: exact equality, and ANN containment properties | The ANN property holds under an HNSW build; a planted exact-search bug is found. |
| S6 | Engine testkit hooks 2 and 4; fault actions; the two-candidate oracle for uncertain outcomes | Backtest: #3661 and #3662 are re-found at their pre-fix commits. |
| S7 | Invalid-input generation with predicted error codes; pairwise option arrays and their stateless sweep | A planted wrong error class is found. The pairwise array is proven to cover every pair (a unit test on the array builder). |
| S8 | Nightly soak and pre-release campaign wiring; `/release` runbook step; backtest report over the 1.2.4–1.2.6 fixed bugs | Re-find rate recorded here; every bug that was not re-found has a follow-up. |

Each slice stays at or below 1,500 lines, and each touched pure decision function gets its own truth table and a call-site test (the mutation gate). S1 is the only slice that must land first; S3–S5 and S7 are independent once S2 has landed.

## 5. Out of scope

- **Concurrency.** Multi-session interleavings belong to 4.3 (loom and shuttle) and 4.12 (histories). This lane is one sequential session.
- **Multi-process IPC and the CLI text layer.** 4.1b owns REPL and pipe fidelity.
- **Commands that need a network or a provider** (`hub.*`, `inference.*`). They are excluded in the ledger with that reason.
- **Performance** (Phase 5).
- **Replacing 4.2.** A reference database built by other people remains the stronger single-capability oracle. This lane covers composition, which 4.2 cannot.

## 6. Open questions

1. **Retention policies other than `KeepAll`.** Modelling pruning would add `history_unavailable` predictions. The proposal is to defer that past S8 and run `KeepAll` only.
2. **Pagination.** The model can predict full result sets, but cursor pages depend on key encoding order. Either the model paginates in the product's string order, or the oracle compares the concatenated pages with the model's full set. The proposal is to concatenate, plus one page-boundary invariant.
3. **Where the model lives.** It could stay in executor tests, or become a small `strata-model` dev-only crate that the Python SDK tests could also drive. The proposal is to start in executor tests and extract the model only when a second consumer exists.
4. **Sizing the pre-release campaign.** One million actions is a starting figure. The campaign should be sized from the backtest: the budget at which the re-find rate plateaus.
