# Write Amplification And Flash Endurance

Status: derived from the 1.2.x code (#2906). Every number here is **derived**
from constants unless it is marked **measured**. No harness in the tree measures
write amplification at a scale where it matters yet (#3709, #3710).

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
WA = W_wal + W_ckpt + c × (1 + W_L0 + Σ W_mid + W_term) [+ W_bottom]
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

This is derived, not measured. #3710 tracks measuring it and deciding the
count-versus-byte trigger for non-final levels.

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
rewrites three full tables. The measurement in #3709 / #3710 should narrow it.

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

## 5. Flash / SD endurance envelope

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

*Example, with all inputs assumed except WA, which comes from section 4:*
- The device logs 1 GiB/day of uniform-key writes, on a card rated 40 TBW, with
  `FTL_WA = 2`.
- **Small dataset** (`WA ≈ 6 – 27`): 12 – 54 GiB/day, which is **2 – 9 years**.
- **10 GiB dataset** (`WA ≈ 44 – 65`): 88 – 130 GiB/day, which is **about
  0.9 – 1.3 years**.
- **Same 10 GiB dataset at a 64 MiB budget** (`WA ≈ 270`): about 540 GiB/day,
  which is **about 2.5 months**.
- **Append-mostly keys** (`WA ≈ 3.25`): about 6.5 GiB/day, which is **about
  17 years**.

The dataset-proportional terminal term is what decides SD endurance. Key
locality and memory budget matter far more than any other setting.

## 6. Operator levers

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
   write granularity and therefore `FTL_WA`, as in section 5.
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
