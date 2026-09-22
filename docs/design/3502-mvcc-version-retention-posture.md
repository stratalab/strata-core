# #3502 — MVCC version-retention default posture (decision)

**Status:** DECIDED 2026-09-22. V1 ships MVCC version pruning **opt-in**, with
`KeepAll` (unbounded time-travel history) as the default. This note records the
decision and its rationale; the mechanism is implemented and proven across
slices A–E (ledger below).

## Decision

- The default retention policy is `VersionRetention::KeepAll`
  (`crates/engine/src/api/options.rs`) / `StorageVersionRetentionPolicy::KeepAll`
  (`crates/storage/src/lifecycle/config.rs`). A database opened with plain
  `DurableLocalOpenOptions::new()` **never prunes** — unbounded time-travel
  history is preserved.
- Pruning is opted into per database with
  `DurableLocalOpenOptions::with_version_retention(VersionRetention::KeepRecentVersions { window })`,
  which retains versions newer than `visible - window` per key and prunes the
  rest during compaction.
- **Default-on is NOT adopted for V1.** The machinery and the measured evidence
  exist (below), so the posture can be revisited later, but the default is not
  flipped in this line.

## Why opt-in

Time travel — reading any branch `as_of` any past version or timestamp — is a
headline Strata capability. Pruning is fundamentally a **trade of history for
disk**: once a branch publishes a retained-history floor `F`, an `as_of` read or
a fork below `F` raises `history_unavailable.engine.persistence_history`
(MVCC-009) rather than serving a stale value. Making that default-on would
silently bound the time-travel promise for **every** durable database — a
semantic behavior change, not a free optimization. Leaving pruning opt-in keeps
the promise intact by default and lets an operator who values disk over deep
history make that trade deliberately, per database.

## The trade, measured

The re-write-heavy amplification A/B (`benchmarks/src/bin/storage_amplification.rs`,
slice E2; 50k keys × 64 B, 10 rewrites → 11 versions/key, `window = 1`):

| Arm | Table bytes | Amplification |
|---|---|---|
| `KeepAll` (default) | 6,038,660 | 1.51× |
| `KeepRecentVersions{1}` | 2,006,680 | 0.50× |

→ **~67% smaller table bytes** for a workload that rewrites the same keys. The
win scales with version churn and is nil for write-once data. (The raw on-disk
total is dominated by the WAL until it is reclaimed at checkpoint — layer 1 /
#3494, orthogonal to version retention.)

So the trade is real and material for re-write-heavy stores, but it is a trade,
and V1 leaves the choice to the operator.

## Proven

- **Default-off is behavior-neutral:** `default_options_never_prune_history`
  (`crates/engine/tests/version_retention.rs`) — plain `new()`, re-write churn,
  forced compaction, every version (including the oldest) still reads exactly.
- **Opt-in prunes and enforces the floor:**
  `opt_in_retention_prunes_old_versions_end_to_end` (same file) and the storage
  `api_opt_in_version_retention_prunes_old_versions` /
  `api_shared_tables_block_version_pruning` /
  `api_fork_below_retained_history_floor_is_rejected`.
- **DST:** `pruning_enforces_the_retained_floor_on_reads_and_forks`
  (`crates/storage/src/testkit/simulation/whole_db.rs`, slice E1).
- **Invariants:** MVCC-009 (history-access ops honor the published floor),
  ARCH-005/008 (shared-table pruning safety), CMP-002 (below-floor survivor).

## Slice ledger (#3502)

| Slice | What |
|---|---|
| A | Read-path raise on `as_of` below the retained floor |
| B | Publish the in-memory version floor in lockstep with the prune |
| C | Persist and restore the floor across reopen |
| D0 | Complete timestamp coverage for born-in-process branches |
| D | Enable opt-in pruning at the compaction dispatch |
| D2 | Engine opt-in surface + real shared-table gate + fork-floor guard (#3509) |
| E2 | Amplification benchmark measuring the layer-2 disk win |
| E1 | DST proof of pruning + hardened the temporal oracle |
| E3 | This decision + the default-off proof |

## Follow-ups (not blocking the decision)

- **E1b** — exhaustive pruning fuzzing through the full whole-DB sweep (seed-derived
  retention across the fault matrix; teach `history.rs`'s lineage checker the floor).
- A ratcheted CI amplification metric (`BenchmarkMetrics.amplification` +
  `perf_floors.py`) so a version-retention regression is caught automatically.
- Autonomous/background pruning dispatch and a reopen-completeness marker
  (created-never-pruned durable DBs recover `Unknown` coverage → currently
  unprunable after restart).
