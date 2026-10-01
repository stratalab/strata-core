# Write Amplification And Flash Endurance

Status: derived from the 1.2.x code (#2906), then **measured** at the
maintenance fixed point up to 3.5 GiB (512 MiB budget) and 1.5 GiB (64 MiB
budget) for #3710; see section 5. Every number is **derived** from constants
unless it is marked **measured**. The measurement harness is not in the tree;
no CI lane measures write amplification at scale (#3709).

This document backs catalog entry SCALE-005
(`docs/audit/ENGINE_INVARIANTS.md`). It explains how many bytes a durable
database writes to its device for each byte a caller commits, and what that
means for SD-card and eMMC endurance. File anchors are as of `bac31c29`; the
property is the invariant, not the line.

## 1. The write path, byte by byte

A committed row is written to the device by these stages:

| # | Stage | What is written | Compressed? |
|---|---|---|---|
| 1 | WAL append | Every committed row, once, at commit | No |
| 2 | Checkpoint snapshot | The rows still in memtables when a checkpoint runs (the delta) | No |
| 3 | Flush | Each row once, into an L0 table, when the active memtable rotates | Yes (Zstd) |
| 4 | L0 → L1 compaction | Each L0 row once more, plus every L1 row its pass overlaps | Yes |
| 5 | L*n* → L*n*+1 compaction, n = 1..6 | Each row once per level crossing, plus the overlapped rows of the next level | Yes |
| 6 | Bottommost consolidation (L7) | Runs of at most 4 terminal tables repacked into fewer tables | Yes |
| 7 | Quarantine stage of reclaim | Each table a compaction retires, moved into `quarantine/` before its source is deleted. On local-fs this is a hard link (no payload bytes, #3721). Backends without a durable link copy the bytes. | As stored |

Manifest, catalog and watermark objects are also written. They are metadata,
proportional to the number of tables and commits rather than to bytes, and are
ignored below.

## 2. The constants

The formula uses these constants. Its symbols are defined in section 3.

| Symbol | Default | Source |
|---|---|---|
| WAL buffering | 128 KiB append buffer, 500 ms staleness window | `crates/storage/src/service/wal.rs:113`, `:119` |
| Checkpoint trigger | retained WAL > 256 MiB or > 64 segments (no commit-count trigger) | `crates/storage/src/lifecycle/config.rs:8-9`, `:566-577` |
| Checkpoint delta cap | 64 MiB (a larger delta flushes first) | `crates/storage/src/format/snapshot.rs:17` |
| `R`, memtable rotation | `min(ActiveMutable pool, 64 MiB)`; the pool is `budget x 64/512` | `crates/storage/src/lifecycle/budget.rs:1302-1308`, `:65`, `:68`, `:283-302` |
| `T`, compaction output table | `min(64 MiB, GeneratedArtifact pool / 2)`; the pool is `budget x 64/512` | `crates/storage/src/table/config.rs:14`, `crates/storage/src/lifecycle/compaction.rs:574`, `budget.rs:71` |
| L0 trigger | 4 tables | `lifecycle/compaction.rs:41` |
| `P`, L0 pass bound | 256 MiB of L0 input + L1 overlap, oldest suffix first | `lifecycle/compaction.rs:60`, `crates/storage/src/branch/state/compaction.rs:1196-1206` |
| `G`, grandparent cut | output tables are cut to overlap at most 256 MiB of the level below the output | `lifecycle/compaction.rs:72` |
| Non-final level trigger | 4 tables **or** bytes >= level target | `lifecycle/compaction.rs:117`, `:2618` |
| Level growth factor | 10; base clamped to 1 MiB .. 256 MiB (dynamic, CMP-005) | `lifecycle/compaction.rs:119-121`, `:688-744` |
| Levels | 8 (L0 .. L7); L7 is never compacted downward | `crates/storage/src/branch/config.rs:5`, `lifecycle/compaction.rs:2901` |
| Metadata-only move | one input table, zero overlap in the output level, no row pruning in the request: moved, not rewritten | `branch/state/compaction.rs:2089-2115` |
| Bottommost run | at most 4 tables, only when it reduces table count; only an explicit level-scoped task selects it | `lifecycle/compaction.rs:139`, `:2831-2877` |
| Table compression | Zstd, level 0 (the library default level) on every durable open | `crates/storage/src/api/options.rs:301`, `crates/storage/src/format/table/mod.rs:44` |
| Version retention | `KeepAll` by default: compaction drops nothing on overwrite | `crates/engine/src/api/options.rs:194-200` |

At the default 512 MiB profile, `R = 64 MiB` and `T = 32 MiB`, because the
artifact pool is 64 MiB and `T` is half of it. At a budget of 1 GiB or more,
both are 64 MiB. At a 64 MiB edge budget, `R = 8 MiB` and `T = 4 MiB`.

## 3. The formula

Symbols:

- `B`: logical bytes committed (encoded rows).
- `c`: on-disk table bytes per logical byte after Zstd. Use 1.0 for
  incompressible data.
- `D`: bytes resident in the terminal level (roughly the live dataset, times `c`).

Physical bytes written per logical byte:

```text
WA = W_wal + W_ckpt + c × (1 + W_L0 + Σ W_mid + W_term) [+ W_bottom] + W_q
```

| Term | Derivation | Bound |
|---|---|---|
| `W_wal` | Each commit is appended once, uncompressed, plus record framing | `≈ 1` |
| `W_ckpt` | A checkpoint runs at most once per 256 MiB of retained WAL. It writes the delta that is not yet flushed: at most one rotation plus the frozen backlog, capped at 64 MiB. One more checkpoint runs per clean close (DUR-019). | `≤ 64 / 256 = 0.25`, plus one delta per session |
| flush | Each row enters exactly one L0 table | `1` |
| `W_L0` | A pass rewrites its L0 input plus every L1 table it overlaps. For uniform keys an L0 table spans the whole keyspace, so it overlaps all of L1, which holds `≤ 3T` at the fixed point. | `1 + |L1| / (P − |L1|)`: `≤ 1.6` at 512 MiB, `≤ 4` at `T = 64 MiB` |
| `W_mid` (n = 1..5) | A one-table pass moves `S ≤ T` bytes and rewrites its overlap in a next level that holds `≤ 3T` at the fixed point. It is `0` when the pass is a metadata-only move. | `0 .. 1 + 3T/S` each, so `≤ 4` each for full tables and `≤ 20` over five crossings |
| `W_term` (L6 → L7) | See below | `≈ 1 + D / P` |
| `W_bottom` | Repacks under-filled terminal tables. Only an explicit level-scoped task runs it. | small; not bounded here |
| `W_q` | The quarantine stage moves every retired compaction input (inputs plus overlapped tables) into `quarantine/`. On local-fs the move is a hard link (`DurableLink`, #3721 / #3723) and writes no payload. Backends without a durable link, and filesystems that refuse the link (EXDEV/ENOTSUP/EPERM), copy the bytes durably. | `0` on local-fs (measured ≤ 0.01, inventory metadata). With a copy it equals compaction input bytes per logical byte, which is `≈ c × (W_L0 + Σ W_mid + W_term)`, and it roughly doubles the compaction share. It was measured at 43–46 % of total WA before #3723. |

### Why the terminal term is `D / P` and not the classical `≈ 10`

The textbook leveled-LSM bound is `W ≈ F × (L − 1)` with fanout `F = 10`. It
assumes every level holds about `F` times the bytes of the level above it.

Strata's defaults do not keep that shape at the fixed point:

1. A non-final level becomes compaction-eligible at **4 tables** whatever its
   byte target. Only the final configured level (L7) is exempt. When maintenance
   keeps up, L1 .. L6 each settle at **≤ 3 tables**, and the durable closed-loop
   test asserts exactly that (`max_clearable_nonzero_fanout <= 3`,
   `crates/storage/src/api/tests/background_scale.rs`). Almost all data lives
   in L7.
2. An L0 pass spans the whole keyspace (uniform keys) and moves `P` bytes. Its
   output tables of `T` bytes each cover about `T / P` of the keyspace, and a
   chunk keeps roughly that share as it drains through the thin middle levels.
3. At L6 → L7, that chunk overlaps about `D × T / P` bytes of L7. The grandparent
   cut `G` limits each *pass* to about `G + T` bytes. It does this by splitting
   the chunk into more, smaller passes, so it does not limit the total rewritten
   per byte moved. Per byte into L7, compaction rewrites about `D / P` bytes of
   L7.

So `W_term ≈ 1 + D / P`, where `P ≈ min(256 MiB, 4R + |L1|)`. This is the
cost of a two-level LSM: each new `P`-sized batch rewrites its share of the
whole terminal level. It grows **linearly** with the dataset. It only dominates
once `D` exceeds a few times `P`: about 2.5 GiB at the default profile, but only
about 400 MiB at a 64 MiB edge budget, where `P ≈ 40 MiB`.

**Measured (section 5):** the shape holds, but the linear magnitude
overpredicts. At the fixed point, L1-L6 do sit at ≤ 3 tables and nearly all
data is in L7. But the L7 write term grew at about 2.1 per GiB at 512 MiB, about
half the predicted `1/P = 4` per GiB. At 64 MiB it **plateaued at about 10**
from `D ≈ 0.6` GiB to 1.5 GiB, where `1 + D/P` predicts about 15–45. The
per-pass L6 → L7 overlap was still growing when the term levelled off. The
likely cause is the grandparent cut and the round-robin compact pointer, but
that is not proven. Points at 5 and 10 GiB are unmeasured, so whether either
curve steps up again is open. #3710 stays open for the count-versus-byte policy
redesign.

**Where the linear term does not apply:**

- *Sequential or time-ordered keys* (event logs, append-mostly KV). New tables
  do not overlap old ones, so every one-table crossing is a metadata-only move
  and `W_mid` and `W_term` collapse towards 0. `W_L0` stays about 1, because an
  L0 pass merges several tables and a multi-table pass is never a move. That
  leaves `WA ≈ 1.25 + 2c`, or about 3.25 for `c = 1`.
- *A starved maintenance lane.* Under sustained ingest that outruns compaction,
  levels grow past their targets. Passes are then larger and fewer, which
  changes the constants, not the order. That is the regime measured in
  `docs/design/performance/durable-load-amplification-evidence.md`.

## 4. Worked example: 1 GiB of logical writes

The example assumes uniform random keys, unique inserts, maintenance at the
fixed point, and the default 512 MiB profile (`R = 64 MiB`, `T = 32 MiB`,
`P ≈ 256 MiB`). It uses `c = 1.0` for incompressible data. Each row is one
term, in GiB of device writes.

| Term | Into an empty database (`D` 0 → 1 GiB, mean `D/P ≈ 2`) | Into a 10 GiB database (`D/P ≈ 40`) |
|---|--:|--:|
| WAL | 1.00 | 1.00 |
| Checkpoint | ≤ 0.25 | ≤ 0.25 |
| Flush | 1.00 | 1.00 |
| L0 → L1 | 1.0 – 1.6 | 1.0 – 1.6 |
| L1 → … → L6 (5 crossings) | 0 – 20 | 0 – 20 |
| L6 → L7 | ≈ 3 | ≈ 41 |
| **Total (WA)** | **≈ 6 – 27** | **≈ 44 – 65** |

The same 1 GiB into a 10 GiB database at a **64 MiB edge budget**
(`P ≈ 40 MiB`, `D/P ≈ 256`) comes to about **260 – 280**.

The wide middle range is the loose `W_mid` bound. The low end assumes every
middle crossing is a metadata-only move, and the high end assumes each one
rewrites three full tables. Section 5 has the measured values, which replace
this example for uniform random keys.

Compression scales every table term by `c`. The engine footprint ratchet
(`crates/engine/tests/storage_footprint_ratchet.rs`) was run for this document
(`cargo test -p strata-engine --features localfs,testkit --test storage_footprint_ratchet`,
9 passed). It **measured** these on-disk-to-logical ratios after a clean close,
for its synthetic 2 x 4 x 256-record loads:

| Primitive | Ratio |
|---|--:|
| KV | 0.141 |
| JSON | 0.196 |
| Event | 0.455 |
| Graph | 0.327 |
| Vector | 0.690 |

These are space ratios on repetitive generated values. They show that Zstd
reaches the tables (`c < 1`), not what `c` real data will get, and they are not a
write-volume measurement.

### How this relates to `SCALED_COMPACTION_AMPLIFICATION_GATE = 4`

The gate (`crates/storage/src/api/tests/mod.rs:124`) asserts that compaction
*input* bytes are at most 4x logical bytes, on a 50,000 x 150 B (~7 MB) cache
workload. At that size `D / P ≈ 0` and few levels populate, so the derivation
predicts compaction well under 4x. The gate is therefore a workload-specific
observation, **not** a bound the theory gives at scale.

It is also not enforced today. Its only caller is an `#[ignore]`d test, which
the nightly perf-trace lane runs without `--ignored` (#3709).

## 5. Measured at the maintenance fixed point (#3710)

Every number in this section is **measured**.

**Setup.** The harness was a worktree-only `#[ignore]`d storage test, not in the
tree, run at `81a9efbe` (before) and `5abae992` (after #3723). It opens a
durable local-fs `StorageRuntime` on ext4/NVMe with `Standard` durability and the
product background maintenance. The workload is uniform random 16 B keys with
**incompressible** random 1 KiB values (`c ≈ 1`, 1,040 logical bytes per row),
committed in 1,024-row batches. After **every** commit the harness waits for the
background lane to go idle. At each checkpoint it also waits until no compaction
completes across a 1.5 s window. The run therefore stays in the trickle regime:
L1-L6 held ≤ 3 tables at every checkpoint, and L0 held ≤ 3 in all but one.

Physical bytes are counted per object class at the local-fs backend (write,
append, publish). They agree with `/proc/self/io` `write_bytes` to within 0.3 %,
and `cancelled_write_bytes` was 0. Runs are deterministic: the same seed gives
identical table bytes.

**Marginal WA** is the device bytes written over an interval divided by the
logical bytes added in it. The component columns are per logical byte over the
same interval. `L7` is bytes written as L7 tables. `q` is the quarantine term,
before → after #3723. Checkpoint snapshots did not run in any interval
(`W_ckpt = 0`; the checkpoint-execution counter stayed 0).

### 512 MiB budget (default profile: `R` = 64 MiB, `T` = 32 MiB, `P ≈ 256` MiB)

| D (GiB) | cum WA before → after | **marginal WA before → after** | WAL | flush | L1 | L2–L6 | L7 | `q` before → after | L1–L6 / L7 tables |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--|
| 0.25 | 3.68 → 2.80 | 3.68 → **2.80** | 1.06 | 0.87 | 0.87 | 0 | 0 | 0.87 → 0 | ≤ 1 / 8 |
| 0.5 | 3.78 → 2.91 | 3.88 → **3.01** | 1.06 | 1.08 | 0.87 | 0 | 0 | 0.87 → 0 | ≤ 3 / 8 |
| 1.0 | 6.69 → 4.39 | 9.61 → **5.88** | 1.06 | 1.08 | 1.41 | 0.81 | 1.51 | 3.73 → 0 | ≤ 3 / 22 |
| 1.5 | 9.00 → 5.54 | 13.62 → **7.83** | 1.06 | 0.97 | 1.35 | 1.02 | 3.42 | 5.79 → 0 | ≤ 3 / 44 |
| 2.0 | 10.45 → 6.28 | 14.80 → **8.48** | 1.06 | 1.08 | 1.36 | 0.96 | 4.01 | 6.33 → 0 | ≤ 3 / 63 |
| 2.5 | 11.35 → 6.72 | 14.93 → **8.49** | 1.06 | 0.97 | 1.36 | 1.07 | 4.02 | 6.44 → 0 | ≤ 3 / 79 |
| 3.5 | 14.29 → 8.19 | 21.64 → **11.88** | 1.06 | 1.03 | 1.58 | 1.31 | 6.88 | 9.77 → 0 | ≤ 3 / 121 |

At 3.5 GiB the device bytes written fell from 53.71 GB to 30.79 GB (−42.7 %).
The difference is exactly the pre-fix quarantine bytes, 22.92 GB, which equal
compaction input bytes. Every other column is byte-identical before and after.

### 64 MiB budget (edge: `R` = 8 MiB, `T` = 4 MiB, `P ≈` 32–44 MiB)

| D (GiB) | cum WA before → after | **marginal WA before → after** | WAL | flush | L1 | L2–L6 | L7 | `q` before → after | L1–L6 / L7 tables |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--|
| 0.25 | 9.83 → 5.98 | 9.83 → **5.98** | 1.06 | 1.03 | 1.27 | 0.70 | 1.88 | 3.85 → 0.00 | ≤ 3 / 63 |
| 0.5 | 14.02 → 8.09 | 18.21 → **10.20** | 1.06 | 1.03 | 1.35 | 0.85 | 5.80 | 8.01 → 0.01 | ≤ 3 / 139 |
| 0.75 | 18.41 → 10.31 | 27.20 → **14.74** | 1.06 | 1.03 | 1.35 | 0.80 | 10.32 | 12.47 → 0.01 | ≤ 3 / 216 |
| 1.0 | 20.34 → 11.28 | 26.11 → **14.20** | 1.06 | 1.03 | 1.06 | 0.62 | 10.23 | 11.92 → 0.01 | ≤ 3 / 274 |
| 1.5 | 21.77 → *n/m* | 24.62 → *n/m* | 1.06 | 1.03 | 1.22 | 0.28 | 9.58 | 11.09 → *n/m* | ≤ 3 / 446 |

After #3723, the residual `q` of ≤ 0.01 is quarantine-inventory metadata. The
post-fix run stopped at 1.0 GiB because it hit its time box. At 1.5 GiB only
the pre-fix row was measured (*n/m* = not measured after the fix). Because every
non-quarantine column was identical wherever both runs exist, the post-fix
value is expected to be about 24.6 − 11.1 ≈ 13.5, but that is inferred.

**Where the runs stopped.** Neither budget reached 5 or 10 GiB. The largest
measured points are 3.5 GiB at 512 MiB and 1.5 GiB (before) / 1.0 GiB (after) at
64 MiB. Section 3's large-`D` predictions (10 and 100 GiB) remain unmeasured.

### What the measurements say about the derivation

- **Fixed-point shape: confirmed.** L1-L6 sat at ≤ 3 tables at every
  checkpoint, and nearly all data sat in L7. WAL (1.06), flush (≈ 1) and
  `W_L0` (0.9-1.6) matched their bounds. `W_mid` was far below its ≤ 20 bound:
  0.3-1.3 in total, because most middle crossings are metadata-only moves.
- **The terminal term overpredicts.** At 512 MiB the L7 term grew at about
  2.1 per GiB, fitted over 1-3.5 GiB. The derivation's `1/P` gives 4 per GiB.
  The growth came in steps: flat from 1.5 to 2.5 GiB, then a rise. At 64 MiB the
  L7 term **plateaued at about 10** from about 0.6 to 1.5 GiB, where `1 + D/P`
  predicts about 15-45. The per-pass overlap was still growing at 0.75 GiB (3 →
  39 L7 tables per L6 → L7 pass), so the plateau is not yet explained. Larger
  sizes may step up again.
- **The quarantine copy was the largest single term (43-46 %) until #3723.**
  On local-fs it is now 0. Backends without a durable link still pay it.
- **Byte targets alone are not the fix (control experiment).** Dropping only the
  count arm (`table_count >= 4`) made marginal WA 1.5-2.2x *worse* at 512 MiB
  over 1-3.5 GiB, for example 32.5 against 21.6 at 3.5 GiB, both before #3723.
  The terminal term fell, but the dynamic targets give the levels above the
  dynamic base level (L1-L3 here) the 256 MiB maximum. L0 always compacts into
  L1, so data is rewritten through three fat levels. A byte-driven policy also
  needs L0 to compact directly into the dynamic base level. That is the #3710
  redesign.

## 6. Flash / SD endurance envelope

```text
device bytes/day = logical bytes/day × WA × FTL_WA
lifetime (days)  = rated TBW / device bytes/day
```

- **FTL write amplification** (`FTL_WA`) is the card's own internal rewrite
  factor. It is typically 1-2 for large sequential writes and higher for small
  synced writes. Strata's table and snapshot writes are large and sequential.
  WAL appends coalesce into 128 KiB buffers under `Standard` durability. Under
  `Always` durability, every commit fsyncs, so small commits become small synced
  writes. On SD media, prefer `Standard` unless per-commit durability is required.
- **Rated TBW.** Consumer SD cards often publish no TBW at all. "High
  endurance" and industrial (pSLC) cards do. Use the vendor datasheet; the
  figures below are an **assumed** rating for illustration only.

*Example.* Every input is assumed except WA, which is the **measured** marginal
WA from section 5, after #3723, on local-fs:
- The device logs 1 GiB/day of uniform-key, incompressible writes, on a card
  rated 40 TB written, with `FTL_WA = 2`.

| Dataset and budget | Measured WA | Device writes/day | Lifetime |
|---|--:|--:|--:|
| ≈ 1 GiB, 512 MiB budget | 5.9 | 11.8 GiB | **≈ 8.6 years** |
| 2-2.5 GiB, 512 MiB budget | 8.5 | 17 GiB | **≈ 6 years** |
| 3.5 GiB, 512 MiB budget | 11.9 | 23.8 GiB | **≈ 4.3 years** |
| 0.75-1 GiB, 64 MiB edge budget | 14.2-14.7 | ≈ 29 GiB | **≈ 3.5 years** |
| same, before #3723 (the quarantine copy) | 26-27 | ≈ 53 GiB | ≈ 1.9 years |

- A backend without a durable link still pays the quarantine copy: use the
  "before" column of section 5.
- **10 GiB datasets are unmeasured.** The derived figures (section 4: WA ≈
  44-65 at 512 MiB, ≈ 260-280 at 64 MiB) overpredicted every size that was
  measured. Treat them as an upper envelope, not an estimate.
- **Append-mostly keys** (`WA ≈ 3.25`, derived and unmeasured): about
  6.5 GiB/day, which is **about 17 years**.

The dataset-proportional terminal term is what decides SD endurance. Key
locality and memory budget matter far more than any other setting.

## 7. Operator levers

What an embedding application can change, from the most effective to the least:

1. **Key design.** Time-ordered or append-mostly keys turn compaction below L1
   into metadata-only moves, giving `WA ≈ 1.25 + 2c`. This is the largest lever and
   costs nothing.
2. **`memory_budget`** (`DurableLocalOpenOptions::with_memory_budget`,
   `crates/engine/src/api/options.rs:230`; host-derived by default, SCALE-001).
   It sets `R` and `T`, and through `R` it sets `P ≈ 4R`. The terminal term
   `D / P` falls as the budget rises, up to 512 MiB where `R` reaches its 64 MiB
   cap. A budget below 512 MiB raises write amplification in inverse proportion.
   Edge deployments should give storage the largest budget they can afford.
3. **`version_retention`** (`with_version_retention`, `options.rs:198`;
   `KeepAll` by default, #3502). For overwrite-heavy workloads, a window lets
   compaction drop superseded versions, which shrinks `D` and every overlap. A
   compaction request that prunes rows is never a metadata-only move
   (`branch/state/compaction.rs:2096`), so an append-only workload gains nothing
   from it.
4. **`durability`** (`with_durability`). This does not change bytes; it changes
   write granularity and therefore `FTL_WA`, as in section 6.
5. **Data compressibility.** This is `c`. Compression is always Zstd on durable
   opens and cannot be switched off from the engine.

Not operator-configurable in 1.2.x: level count (8), L0 trigger (4), non-final
table-count trigger (4), growth factor (10), `P` and `G` (256 MiB), and the
64 MiB caps on `R` and `T`. The issue's suggested lever of "a smaller
`max_level_count`" does not exist as an option. Under the fixed-point shape
above it would also not help: a shallower stack still funnels into one
terminal level.

`data_block_bytes` (`with_data_block_bytes`) trades read amplification against
index metadata. It does not materially change write amplification.
