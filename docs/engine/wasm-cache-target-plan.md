# WASM Cache Target Plan

## Purpose

Make `wasm32-unknown-unknown` a first-class supported compile target for
`strata-engine` in cache mode (`Database::cache()`).

Strata's product positioning is "embedded, deployable anywhere." Today that
claim is true for every native target, but the engine does not compile to
`wasm32-unknown-unknown` because the cache-mode and disk-backed code paths
share modules and pull in OS-coupled crates (`memmap2`, `fs2`, `zstd-sys`,
`rayon`, `getrandom` without `js`). Cache mode is already a first-class
runtime mode (`DatabaseMode::Cache`) with its own constructor; this plan
makes the *compile graph* match the *runtime graph* so that the cache-only
subset of the engine is buildable without any OS-coupled dependency.

The artifact at the end of this work is:

- `cargo check --target wasm32-unknown-unknown -p strata-engine` succeeds
- A `Database::cache()` instance can be opened, mutated, and queried inside
  a `wasm-bindgen-test` harness running under `wasm-pack test --node` and
  `--headless --chrome`
- No native-target behavior changes; no storage, WAL, manifest, checkpoint,
  snapshot, or recovery format changes
- The supported-target invariant is enforced by CI

This is not a landing-page project. The landing-page demo is a downstream
beneficiary; the load-bearing claim is platform reach.

## Scope

This plan owns:

- Adding `target_arch = "wasm32"` cfg gates to engine and storage modules
  whose code paths are unreachable from `Database::cache()`
- Replacing or cfg-gating wasm-hostile transitive deps (`getrandom`,
  `zstd`, `rayon`, `fs2`, `memmap2`, `libc`)
- Adding a single-threaded fallback for `BackgroundScheduler` on wasm32
- Adding a `wasm-bindgen-test` integration test exercising kv, vector, and
  search against `Database::cache()`
- Adding a CI job that runs `cargo check --target wasm32-unknown-unknown`
  and the wasm test suite on every PR
- Documenting the wasm-supported subset of the engine public API

This plan does not own:

- Compiling disk-backed (`DatabaseMode::Primary`, `DatabaseMode::Follower`)
  paths to wasm32 — those remain native-only
- Browser-persistent storage backends (IndexedDB, OPFS); cache mode is
  ephemeral and that property is preserved
- Multi-threaded wasm via `wasm-bindgen-rayon` / SharedArrayBuffer
- WebGPU acceleration of vector search
- Building the landing page or any host-side JS/TS bindings
- Compiling `strata-cli`, `strata-executor`, `strata-intelligence`, or
  `strata-inference` to wasm32
- Any change to the engine D4 public API surface

## Load-Bearing Constraints

1. **No runtime behavior change on native targets.** This is a refactor-only
   change in the sense of CLAUDE.md's non-regression protocol. Native
   benchmarks (`redb`, `ycsb_compare`, `beir`) must show no regression
   beyond the noise thresholds.

2. **One canonical cache path.** `Database::cache()` remains the single
   entry point. No new `Database::wasm_cache()` constructor, no parallel
   "browser mode" public API. The wasm target is a *deployment property*
   of the existing cache mode, not a new mode.

3. **No `target_arch` gates leaking into the public API.** Public types,
   methods, and trait signatures must be identical on wasm32 and native.
   Methods that cannot be implemented on wasm32 (e.g. anything that
   requires opening a path) must already be gated to disk-backed modes
   at the type level — they should never have been callable on a cache
   instance regardless of platform.

4. **Cargo features over `target_arch` where possible.** A
   `[target.'cfg(target_arch = "wasm32")'.dependencies]` table is fine
   for swapping `zstd` for `ruzstd`, but engine's own conditional
   compilation should prefer `#[cfg(feature = "...")]` gates when the
   division is "cache-only build" rather than "wasm-specific behavior."
   This keeps the option open to ship a no-disk native build later
   (embedded targets, hostile sandboxes) without a second cfg axis.

5. **CI enforces the target.** Once green, regressions on
   `wasm32-unknown-unknown` are PR blockers, identical to clippy or
   native test failures. A target that is supported "when someone
   remembers to check" is not supported.

## Current Compile-Graph Hostility Map

Direct wasm-hostile dependencies declared in `crates/engine/Cargo.toml`:

- `memmap2` — used in `crates/engine/src/search/segment.rs`,
  `crates/engine/src/vector/{mmap,mmap_graph,hnsw}.rs`
- `fs2` — used in `crates/engine/src/database/{open.rs,product_open.rs,mod.rs}`
  and `crates/engine/src/vector/{mmap,mmap_graph}.rs`
- `zstd` (C lib via `zstd-sys`) — used in
  `crates/engine/src/bundle/{reader.rs,writer.rs}` and
  `crates/storage/src/{segment.rs,segment_builder.rs}`
- `rayon` — used in `crates/engine/src/vector/segmented.rs`
- `libc` — small surface; usage to be enumerated in W1

Transitive issues:

- `getrandom` (via `rand`, `uuid`, `aes-gcm`) requires the `js` feature
  on wasm32 or compilation fails before any engine code is checked.

Filesystem and threading touchpoints inside engine:

- `std::fs` appears in 62 files across `crates/engine` and
  `crates/storage`. The reachability question — how many of those are
  reachable from `Database::cache()` — is answered in W2.
- `BackgroundScheduler` (`crates/engine/src/background.rs`) spawns OS
  threads. Coordinator, transaction pool, and compaction also rely on
  `std::thread`. Wasm32 (without rayon/SharedArrayBuffer) is
  single-threaded.

This map is the input to the phased work below. It is current as of the
date this document is committed and should be re-validated at the start
of each phase.

## Phasing

The work is broken into five phases, W1 through W5. Each phase is a
single PR or a small contiguous PR sequence. A phase does not start
until the previous phase is merged and CI is green.

### W1 — Trivial dependency fixes

Goal: get `cargo check --target wasm32-unknown-unknown -p strata-engine`
past the `getrandom` failure and produce a complete list of remaining
compile errors.

- Add `getrandom` with `features = ["js"]` to engine and storage under
  `[target.'cfg(target_arch = "wasm32")'.dependencies]`
- Cfg-gate `rayon` usage in `crates/engine/src/vector/segmented.rs`
  behind `#[cfg(not(target_arch = "wasm32"))]` with a single-threaded
  iteration fallback
- Replace `zstd` with `ruzstd` (decode-only, pure Rust) on wasm32, or
  cfg-gate the bundle reader/writer paths if zstd-encoded bundles are
  unreachable from cache mode (W2 will confirm)
- Audit and cfg-gate `libc` usage

Exit criteria: the wasm32 build proceeds far enough to surface the next
class of failures (mmap, fs, threads). Errors are documented and
quantified for W2.

Assurance class: S2 (build-graph only).

### W2 — Cache-mode reachability audit

Goal: prove which engine modules are unreachable from `Database::cache()`,
so they can be cfg-gated whole-file rather than line-by-line.

- Trace all functions reachable from `Database::cache()` and the cache
  product-open path (`crates/engine/src/database/open.rs:869`,
  `crates/engine/src/database/product_open.rs`)
- Classify each module under `crates/engine/src/{vector,search,bundle,
  recovery,wal,manifest,checkpoint,snapshot}` as:
  - "cache-reachable" — must compile on wasm32
  - "disk-only" — can be cfg-gated entirely on wasm32
  - "mixed" — needs surgical separation (W3)
- Produce a table in this document mapping every wasm-hostile usage
  site from the hostility map to its classification

Exit criteria: the table is complete and reviewed. No code changes in
this phase; the artifact is the audit table.

Assurance class: S3 (informs S4 work in W3).

### W3 — Cfg-gate disk-only modules

Goal: every module classified "disk-only" in W2 is gated behind
`#[cfg(not(target_arch = "wasm32"))]` (or a `disk` Cargo feature, if
that is chosen in W2).

- Apply gates at module level in `lib.rs` / `mod.rs` files, not at
  individual function level — module-level gates are easier to audit
  and harder to break inadvertently
- Update `crates/engine/src/database/open.rs` so the disk-backed
  constructors (`open`, `open_with_options`, follower constructors)
  are gated; `Database::cache()` remains unconditional
- Update the engine `prelude` / re-exports so wasm32 callers see only
  the cache-supported subset of public types

Exit criteria: `cargo check --target wasm32-unknown-unknown -p
strata-engine` succeeds. `cargo build --workspace` on native targets
is unaffected. Native test suite passes unchanged.

Assurance class: S4 (touches open paths and durability module
organization). Requires a second reviewer per the assurance protocol.

### W4 — Surgical separation of mixed modules

Goal: handle modules classified "mixed" in W2 — code paths where cache
and disk logic are interleaved.

- Expected sites (subject to W2's actual findings): `database/open.rs`
  (already partially split via the `cache()` constructor), parts of
  `search/segment.rs` (in-memory vs mmap-backed segment readers),
  `vector/segmented.rs` (rayon parallelism)
- Refactor each site to extract the cache-reachable code into a
  wasm-buildable inner module, with the disk-coupled code in a
  sibling cfg-gated module
- The refactor must be characterization-tested before the move per
  CLAUDE.md's "characterization-before-refactor rule" for S4 surface

Exit criteria: no `target_arch` cfg gates remain inside function bodies.
Every gate is at module or item boundary. Native behavior unchanged
(verified by full test suite + benchmarks).

Assurance class: S4. Requires characterization tests, second reviewer,
benchmark report.

### W5 — Wasm test harness and CI enforcement

Goal: lock the target in. Without CI enforcement, this work decays
within one quarter.

- Add `crates/engine/tests/wasm_cache.rs` exercising:
  - opening a cache database
  - kv, json, events, vector, graph primitive ops
  - search with at least one recipe
  - shutdown / drop semantics
- Configure the test to run under `wasm-bindgen-test` with both
  `--node` and `--headless --chrome`
- Add a CI job that runs:
  - `cargo check --target wasm32-unknown-unknown -p strata-engine`
  - `wasm-pack test --node crates/engine`
- Add the wasm32 target to the documented supported-target list in
  the workspace README and `engine-crate-map.md`

Exit criteria: a PR that introduces a wasm32 regression fails CI.

Assurance class: S2 (CI / lint).

## Engine Public API Subset on wasm32

The wasm32 build exposes the cache-supported subset of the D4 public
API:

- `Database`, `DatabaseMode::Cache`, `Database::cache()`
- `OpenSpec::cache()` and the cache product-open entry points
- `Transaction` and per-primitive transaction extensions
- `Subsystem` trait and `SearchSubsystem`, `VectorSubsystem`,
  `GraphSubsystem`, `IntelligenceSubsystem`
- `BranchService`, `RetrievalService` and their request/response types
- All public error types

The wasm32 build does not expose:

- `Database::open`, follower constructors, any path-taking constructor
- `WalWriterHealth`, `FollowerStatus`, `ContiguousWatermark`,
  `RefreshOutcome`, `BlockedTxn`, `BlockReason`, `DatabaseLayout`,
  `RefreshHookError`, `AdvanceError`, `UnblockError`,
  `LossyRecoveryReport`, `LossyErrorKind`, `RecoveryHealth`,
  `DegradationClass`
- `RetentionReport` and related retention types
- Bundle import/export (uses `zstd-sys` and file IO)

The "wasm32 does not expose X" rule is structural: those items live in
modules that are cfg-gated out, so they are not absent by convention,
they are absent by compilation. This is checked by W5's `cargo check`
job.

## Risks and Open Questions

- **`zstd` decode on wasm32.** If bundle reading needs to work on
  wasm32 (it likely does not, since cache databases have nothing to
  bundle from), `ruzstd` is sufficient. If it does, this is a
  larger swap. W2 settles this.

- **`std::time::Instant` and monotonic clocks on wasm32.** Available
  on `wasm32-unknown-unknown` via `web-time` shim or `instant` crate.
  Engine usage to be audited in W1.

- **`parking_lot` on wasm32.** Works without features on recent
  versions; the `arc_lock` feature used by storage may need
  verification.

- **Memory pressure.** A wasm32 linear memory cap (typically 4 GiB,
  often less in browsers) means the cache mode block-cache sizing
  derived from `apply_hardware_profile_if_defaults` must produce
  sensible defaults on wasm32. The Pi Zero profile is a reasonable
  starting point; W3 should verify or add a wasm-specific profile.

- **Single-threaded `BackgroundScheduler`.** Compaction, flush, and
  background indexing all run through the scheduler. On wasm32 these
  run cooperatively on the main thread or are no-ops. Cache mode does
  no compaction or flush (no disk to flush to), so this is mostly a
  question of whether any cache-reachable code path enqueues work to
  the scheduler. W2 settles this.

- **`SharedArrayBuffer` and threading later.** This plan does not pursue
  multi-threaded wasm. If later needed, `wasm-bindgen-rayon` is the
  standard route, but it requires COOP/COEP headers in the host
  environment, which is a deployment constraint we do not want to
  impose on every embedder.

## Acceptance

Per CLAUDE.md's non-regression protocol:

- **Change class:** refactor-only with platform expansion. No runtime
  behavior change on native targets. New compile target supported.
- **Assurance class:** S4 overall (touches open paths, durability
  module organization). Per-phase classes are listed above.
- **Tests:** native test suite unchanged and passing; new wasm
  integration test passing under `wasm-bindgen-test`.
- **Benchmarks:** full regression suite (`redb`, `ycsb_compare`,
  `beir`) run after W3 and W4 with no regression beyond noise
  thresholds. Wasm32 has no benchmark obligation; performance tuning
  on wasm32 is out of scope for this plan.
- **Reviewer:** W3 and W4 require a second reviewer per S4 protocol.

## Out of Scope, For Reference

The following are explicit non-goals for this plan but are natural
follow-ons and should not be confused with it:

- Persistent browser storage (IndexedDB / OPFS) backend
- WebGPU vector search acceleration
- Browser-side bindings, JS/TS API, or TypeScript types
- Landing-page demo, documentation site demo, or any host
  application
- Mobile (iOS/Android) embedding via wasm or otherwise
- `wasm32-wasi` target (server-side wasm; would benefit from the same
  cfg structure but has different OS-syscall affordances)

These should each get their own plan once the wasm32-unknown-unknown
target is stable.
