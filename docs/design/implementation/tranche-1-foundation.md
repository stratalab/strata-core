# Tranche 1: Foundation, Quality Enablers, and Program Infrastructure

## Context

Tranche 0 bootstrapped the program (CLAUDE.md, benchmarks, skills). Tranche 1 locks the workspace foundation that every subsequent tranche depends on: centralized lint config, CI pipeline, crate DAG enforcement, acceptance matrix, and two quality enablers that must land before architecture work begins.

**Why quality enablers in T1:** ET-1 (`#[non_exhaustive]`) is a hard prerequisite for every architecture phase that adds `StrataError` variants (T2–T6). TS-1 (`TxnId`/`CommitVersion`) is a prerequisite for runtime, durability, and branch phases that touch transaction/version IDs. Deferring these to T7 would force architecture phases to work against the old untyped error API and bare `u64` signatures, then migrate again later.

**Current state (verified 2026-04-10):**
- `rust-version = "1.70"` (Cargo.toml:29) — needs bump to 1.74 for `[workspace.lints]`
- No `[workspace.lints]` section exists
- No `rust-toolchain.toml` exists
- No `deny.toml` exists
- CI has 3 flat jobs with no caching, no cargo-hack, no deny
- 12 per-crate `#![warn(...)]` attributes across 6 crates
- 3 crates (graph, cli, vector) don't inherit `version.workspace`/`edition.workspace`
- Engine has dev-dep on `strata-vector` for a benchmark that belongs in the vector crate
- 0 of 30 public error enums have `#[non_exhaustive]`
- 0 newtype wrappers for the 88 bare `u64` semantic parameters

**Issues:** None (program-internal work)

---

## Epic T1-E1: Pre-flight and MSRV (#1517)

**Goal:** Validate 1.74 builds, capture baseline inventories, bump MSRV.

**Why first:** `[workspace.lints]` (E2) requires Rust 1.74. Everything else depends on E2.

### Changes

**1. Baseline inventory capture** (save as PR artifacts — no code changes)

```bash
# Lint attributes to delete in E2
grep -rn '#!\[warn\|#!\[deny\|#!\[allow' crates/*/src/lib.rs
# Expected: 12 attributes across 6 files (engine, core, executor, security, durability, storage, concurrency)

# Crate DAG
cargo tree --format="{p}" --edges=normal | sort -u

# License baseline
cargo metadata --format-version 1 | jq '.packages[] | {name, version, license}'

# MSRV build
cargo +1.74 check --workspace --all-targets --all-features

# Clippy baseline
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Feature matrix baseline
cargo hack --feature-powerset --depth 2 check --workspace --exclude strata-inference
```

**2. MSRV bump in `Cargo.toml:29`** (~1 line)
```toml
# Before:
rust-version = "1.70"

# After:
rust-version = "1.74"
```

**3. Add `rust-toolchain.toml` at workspace root** (new file, ~4 lines)
```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

### Verification
- `cargo +1.74 check --workspace --all-targets --all-features` passes
- `Cargo.toml:29` reads `rust-version = "1.74"`
- `rust-toolchain.toml` exists at workspace root

### Effort: 1-2 hours (baseline capture may surface 1.74 compat issues)

---

## Epic T1-E2: Workspace Configuration Centralization

**Goal:** Single `[workspace.lints]` block, centralized deps, normalized package metadata. After this epic, adding a lint or changing a dep version requires exactly one edit.

**Why:** 12 per-crate lint attributes, 6+ deps duplicated across crates, 3 crates with independent version/edition. This creates config drift and makes program-wide lint changes require editing every crate.

### Changes

**1. Add `[workspace.lints]` block to root `Cargo.toml`** (~20 lines, after line 92)

```toml
[workspace.lints.rust]
unused_must_use = "deny"
unsafe_op_in_unsafe_fn = "warn"
missing_docs = "warn"
unreachable_pub = "warn"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }
pedantic = { level = "warn", priority = -1 }
# Relaxations for Strata patterns
module_name_repetitions = "allow"
too_many_arguments = "allow"
must_use_candidate = "allow"
missing_errors_doc = "allow"
missing_panics_doc = "allow"
```

**Lint level note:** `missing_docs` and `unreachable_pub` are intentionally `warn`, not `deny`. They surface debt without blocking builds. The cleanup phases (visibility scope, doc pass) will clear the warnings; the visibility scope escalates `unreachable_pub` to `deny` in Phase VM-6. During E2, `cargo clippy` runs WITHOUT `-- -D warnings` — the `[workspace.lints]` levels are the authority. The `clippy::all = { level = "deny" }` setting already denies clippy-category warnings; the rust-level `warn` lints remain warnings until their owning scopes clear them.

**2. Add shared deps to `[workspace.dependencies]`** (~10 lines, additions to `Cargo.toml:34-92`)

These deps currently live in individual crate Cargo.tomls with version pinned locally:

| Dep | Current location(s) | Version |
|-----|---------------------|---------|
| `fs2` | durability, engine, vector (each pins `0.4`) | `"0.4"` |
| `libc` | durability, engine, cli, executor (each pins `0.2`) | `"0.2"` |
| `sha2` | engine (`0.10.9`), intelligence (`0.10`) | `"0.10"` |
| `crc32fast` | storage (`1.3`), durability (`1.3`), root dev-deps (`1.3`) | `"1.3"` |
| `arc-swap` | storage (`1`) | `"1"` |
| `crossbeam-skiplist` | storage (`0.1.3`) | `"0.1.3"` |
| `proptest` | storage dev (`1.7.0`) | `"1.7.0"` |
| `static_assertions` | concurrency dev (`1.1`) | `"1.1"` |
| `aes-gcm` | durability (`0.10`) | `"0.10"` |
| `criterion` | root dev-deps (`0.5`), engine dev-deps (`0.5`) | `"0.5"` |
| `zstd` | storage (`0.13`) — already in workspace but storage uses local spec | switch to `{ workspace = true }` |

Add to `[workspace.dependencies]` (zstd already exists in workspace — only needs per-crate switch):
```toml
fs2 = "0.4"
libc = "0.2"
sha2 = "0.10"
crc32fast = "1.3"
arc-swap = "1"
crossbeam-skiplist = "0.1.3"
proptest = "1.7.0"
static_assertions = "1.1"
aes-gcm = "0.10"
```
(`criterion` is already at root level — switch engine's copy to `{ workspace = true }`)

**3. Switch per-crate dep specs to `{ workspace = true }`** (~20 lines across 8 files)

For each dep centralized above, edit the owning crate's Cargo.toml. Example for `crates/durability/Cargo.toml`:
```toml
# Before:
crc32fast = "1.3"
fs2 = "0.4"
aes-gcm = "0.10"

# After:
crc32fast = { workspace = true }
fs2 = { workspace = true }
aes-gcm = { workspace = true }
```

Same pattern for: storage (crc32fast, arc-swap, crossbeam-skiplist, proptest, zstd), engine (fs2, sha2, libc, criterion), vector (fs2), intelligence (sha2), executor (libc), cli (libc), concurrency (static_assertions).

**4. Normalize graph, cli, vector package metadata** (~9 lines across 3 files)

These 3 crates hardcode `version = "0.6.1"` and `edition = "2021"` instead of inheriting. Per D-WF-7, they must match the pattern already used by `crates/engine/Cargo.toml:3-8`:

`crates/graph/Cargo.toml` (lines 3-4):
```toml
# Before:
version = "0.6.1"
edition = "2021"

# After (match engine pattern per D-WF-7):
version.workspace = true
edition.workspace = true
rust-version.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
```

Same for `crates/cli/Cargo.toml` and `crates/vector/Cargo.toml`.

**5. Add `default-members` to `[workspace]`** (~1 line, after `Cargo.toml:24`)
```toml
default-members = ["crates/executor", "crates/cli", "crates/engine"]
```

**6. Add `[lints] workspace = true` to every member** (15 files — 13 member crates + root + benchmarks)

Every `crates/*/Cargo.toml` gets:
```toml
[lints]
workspace = true
```

Full list of files to edit:
- `Cargo.toml` (root stratadb package)
- `crates/core/Cargo.toml`
- `crates/storage/Cargo.toml`
- `crates/concurrency/Cargo.toml`
- `crates/durability/Cargo.toml`
- `crates/engine/Cargo.toml`
- `crates/graph/Cargo.toml`
- `crates/vector/Cargo.toml`
- `crates/search/Cargo.toml`
- `crates/intelligence/Cargo.toml`
- `crates/inference/Cargo.toml`
- `crates/executor/Cargo.toml`
- `crates/cli/Cargo.toml`
- `crates/security/Cargo.toml`
- `benchmarks/Cargo.toml`

**7. Delete per-crate lint attributes** (12 deletions across 7 files)

| File | Lines | Attribute |
|------|-------|-----------|
| `crates/core/src/lib.rs` | 14-15 | `#![warn(missing_docs)]`, `#![warn(clippy::all)]` |
| `crates/storage/src/lib.rs` | 13-14 | `#![warn(missing_docs)]`, `#![warn(clippy::all)]` |
| `crates/concurrency/src/lib.rs` | 13-14 | `#![warn(missing_docs)]`, `#![warn(clippy::all)]` |
| `crates/durability/src/lib.rs` | 16-17 | `#![warn(missing_docs)]`, `#![warn(clippy::all)]` |
| `crates/engine/src/lib.rs` | 23-24 | `#![warn(missing_docs)]`, `#![warn(clippy::all)]` |
| `crates/executor/src/lib.rs` | 48 | `#![warn(missing_docs)]` |
| `crates/security/src/lib.rs` | 6 | `#![warn(missing_docs)]` |

### Files touched
- `Cargo.toml` (root — lints block, deps, default-members, `[lints]`)
- 14 member crate `Cargo.toml` files (`[lints]` + dep switches)
- 7 `lib.rs` files (attribute deletion)

### Verification
- `cargo clippy --workspace --all-targets --all-features` passes (no `-- -D warnings` — workspace.lints is the authority; `warn`-level lints like `missing_docs` and `unreachable_pub` are expected and allowed during WF-2)
- `cargo check --workspace --all-targets --all-features` passes
- `grep -rn '#!\[warn' crates/*/src/lib.rs` returns zero results
- Every member `Cargo.toml` contains `[lints]\nworkspace = true`
- `cargo metadata` shows all 11 centralized deps with consistent versions (10 new + zstd switch)

### Effort: 3-4 hours (mostly mechanical, but clippy pedantic may surface new warnings to address)

---

## Epic T1-E3: CI Pipeline and Feature Matrix

**Goal:** Replace the 3-job CI with a 4-stage pipeline + parallel MSRV. Add `deny.toml`, cargo-hack, doc tests, lockfile check, and build caching.

**Why:** Current CI (`ci.yml`) has no caching (slow), no cargo-hack (feature combinations untested), no cargo-deny (advisory/license unchecked), no doc tests, and reinstalls cargo-audit on every run.

### Changes

**1. Add `deny.toml` at workspace root** (new file, ~35 lines)

Exact copy of D-WF-2 from `workspace-foundation-scope.md:318-358`:

```toml
[graph]
targets = []

[advisories]
vulnerability = "deny"
unmaintained = "warn"
yanked = "deny"
notice = "warn"

[licenses]
allow = [
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Unicode-3.0",
    "Unicode-DFS-2016",
    "Zlib",
    "0BSD",
]
exceptions = [
    { allow = ["BUSL-1.1"], crate = "strata-intelligence" },
    { allow = ["BUSL-1.1"], crate = "strata-search" },
    { allow = ["BUSL-1.1"], crate = "strata-inference" },
]
confidence-threshold = 0.8

[bans]
multiple-versions = "warn"
wildcards = "deny"
highlight = "all"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-registry = ["https://github.com/rust-lang/crates.io-index"]
allow-git = []
```

**2. Rewrite `.github/workflows/ci.yml`** (full rewrite, ~90 lines replacing current 45)

Current structure (3 flat jobs, no deps, no caching):
```
test (fmt + clippy + test)
build (release build)
security (install cargo-audit + run)
```

New structure (4 stages + parallel MSRV):
```
Stage 1: check (no needs)
  - cargo fmt --all -- --check
  - cargo clippy --workspace --all-targets --all-features  (workspace.lints is authority)
  - cargo metadata --locked  (lockfile consistency)
  - cargo deny check         (advisory + license + bans)
  - cargo tree DAG guard     (E4 step, see below)

Stage 2: test (needs: check)
  - cargo test --workspace
  - cargo test --workspace --doc --all-features --exclude strata-inference

Stage 3: feature-matrix (needs: check, parallel with Stage 2)
  - cargo hack --feature-powerset --depth 2 check --workspace --exclude strata-inference
  - cargo hack --each-feature test --workspace --exclude strata-inference

Stage 4: build (needs: test)
  - cargo build --release --all

Parallel: msrv (no needs)
  - cargo +1.74 check --workspace --all-targets --all-features
```

Key implementation details:
- Every job uses `Swatinem/rust-cache@v2` for build caching
- `cargo deny` and `cargo hack` are installed via `taiki-e/install-action@v2`
- `--exclude strata-inference` on hack jobs (CUDA/runtime deps don't build in CI)
- Doc tests run with `--all-features` to test feature-gated doc examples
- Stage 3 (feature-matrix) is parallel with Stage 2 (test) — both need only check

```yaml
name: CI

on:
  pull_request:
    branches: [develop, main]
  push:
    branches: [develop, main]

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { submodules: true }
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@cargo-deny
      - name: Format check
        run: cargo fmt --all -- --check
      - name: Clippy
        run: cargo clippy --workspace --all-targets --all-features
        # No -- -D warnings: workspace.lints is the authority. clippy::all = deny
        # handles clippy warnings; warn-level rust lints (missing_docs, unreachable_pub)
        # are allowed until their owning scopes clear them.
      - name: Lockfile consistency
        run: cargo metadata --locked --format-version 1 > /dev/null
      - name: Advisory + license check
        run: cargo deny check
      - name: DAG guard (normal deps only — manifest guard added in E4)
        run: |
          if cargo tree --package strata-engine --edges=normal --format="{p}" | grep -q strata-vector; then
            echo "ERROR: strata-engine has normal dep on strata-vector" && exit 1
          fi

  test:
    needs: check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { submodules: true }
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: Run tests
        run: cargo test --workspace
      - name: Doc tests
        run: cargo test --workspace --doc --all-features --exclude strata-inference

  feature-matrix:
    needs: check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { submodules: true }
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@cargo-hack
      - name: Feature powerset check
        run: cargo hack --feature-powerset --depth 2 check --workspace --exclude strata-inference
      - name: Per-feature test
        run: cargo hack --each-feature test --workspace --exclude strata-inference

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { submodules: true }
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: Release build
        run: cargo build --release --all

  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { submodules: true }
      - uses: dtolnay/rust-toolchain@1.74
      - uses: Swatinem/rust-cache@v2
      - name: MSRV check
        run: cargo check --workspace --all-targets --all-features
```

**3. Delete the `security` job** (subsumed by `cargo deny check` in the `check` stage)

### Files touched
- `deny.toml` (new, ~35 lines)
- `.github/workflows/ci.yml` (full rewrite, ~90 lines)

### Verification
- `cargo deny check` passes locally
- `cargo hack --feature-powerset --depth 2 check --workspace --exclude strata-inference` passes locally
- CI has 5 jobs (check, test, feature-matrix, build, msrv) with correct dependency graph
- Every job uses `Swatinem/rust-cache@v2`
- Doc tests run in CI (`--doc --all-features`)
- No `cargo-audit` in CI (replaced by deny)

### Effort: 3-4 hours (deny.toml tuning may require license/ban exceptions)

### Additional Work (owner-requested scope expansion)

The following work was added to T1-E3 at owner request during implementation:

**1. License standardization**
- Changed all BUSL-1.1 licenses to Apache-2.0 (strata-inference, strata-intelligence, strata-search)
- Added missing `license = "Apache-2.0"` to strata-graph, strata-vector, strata-cli, stratadb root
- Added `publish = false` to unpublished crates for cargo-deny compatibility

**2. Security advisory remediation**
- Updated tar 0.4.44 → 0.4.45 (fixed RUSTSEC-2026-0068, RUSTSEC-2026-0097)
- Updated rustls-webpki 0.103.9 → 0.103.11 (fixed RUSTSEC-2026-0049)
- Updated rand 0.8 → 0.9.3 (fixed RUSTSEC-2026-0097)
- Updated proptest 1.10.0 → 1.11.0 (transitive rand fix)

**3. Dependency consolidation**
- Updated base64 0.21 → 0.22 (eliminated duplicate with ureq's transitive dep)

**4. Tracked for follow-up**
- Opened #2391: bincode migration (RUSTSEC-2025-0141, unmaintained, no safe upgrade)

---

## Epic T1-E4: Crate DAG Guard

**Goal:** Move `vector_benchmarks.rs` from engine to vector crate. Delete engine's dev-dep on `strata-vector`. Investigate and delete unused `strata-graph` dev-dep. Add CI guard.

**Why:** Engine → vector dev-dep creates a hidden DAG cycle that could become a real dependency. Research confirms:
- `vector_benchmarks.rs` uses 6 imports from `strata_vector` (backend, hnsw, segmented, brute_force)
- Neither `primitive_benchmarks.rs` nor `transaction_benchmarks.rs` imports from `strata-graph`
- The `strata-graph` dev-dep is unused by any engine benchmark

### Changes

**1. Move benchmark file** (1 file, rename only)

Move `crates/engine/benches/vector_benchmarks.rs` → `crates/vector/benches/vector_benchmarks.rs`

The file's imports need no changes — they already use `strata_vector::*` and `strata_core::*`, both of which are available from the vector crate's perspective:

```rust
// crates/engine/benches/vector_benchmarks.rs (lines 10-16)
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashSet;
use strata_core::primitives::vector::{DistanceMetric, VectorConfig, VectorId};
use strata_vector::backend::{SegmentCapable, VectorIndexBackend};
use strata_vector::hnsw::HnswConfig;
use strata_vector::segmented::{SegmentedHnswBackend, SegmentedHnswConfig};
```

These imports all resolve from the vector crate (strata-core is a direct dep, strata_vector is `self`).

**2. Update `crates/vector/Cargo.toml`** (~5 lines added)

```toml
[dev-dependencies]
tempfile = { workspace = true }
criterion = { workspace = true }    # NEW
strata-core = { path = "../core" }  # NEW (if not already present — needed for VectorConfig, etc.)

[[bench]]
name = "vector_benchmarks"
harness = false
```

**3. Update `crates/engine/Cargo.toml`** (~5 lines deleted)

Remove from `[dev-dependencies]` (currently line 48):
```toml
# DELETE:
strata-vector = { path = "../vector" }
strata-graph = { path = "../graph" }   # also unused
```

Remove from `[[bench]]` sections (currently lines 59-61):
```toml
# DELETE:
[[bench]]
name = "vector_benchmarks"
harness = false
```

**4. Extend CI DAG guard with manifest check** (update E3's `check` job)

E3 ships the `cargo tree` normal-dep guard. E4 adds the manifest-level guard after the dev-deps are actually removed:

```yaml
      - name: DAG guard
        run: |
          if cargo tree --package strata-engine --edges=normal --format="{p}" | grep -q strata-vector; then
            echo "ERROR: strata-engine has normal dep on strata-vector" && exit 1
          fi
          if grep -q 'strata-vector\|strata-graph' crates/engine/Cargo.toml; then
            echo "ERROR: engine Cargo.toml still references vector or graph" && exit 1
          fi
```

### Files touched
- `crates/engine/benches/vector_benchmarks.rs` (deleted)
- `crates/vector/benches/vector_benchmarks.rs` (moved, unchanged content)
- `crates/engine/Cargo.toml` (remove 2 dev-deps + 1 bench section)
- `.github/workflows/ci.yml` (extend DAG guard step with manifest check)
- `crates/vector/Cargo.toml` (add criterion, bench section)

### Verification
- `cargo bench --package strata-vector --bench vector_benchmarks -- --test` compiles
- `crates/engine/Cargo.toml` has no `strata-vector` or `strata-graph` entry (deps or dev-deps)
- `cargo tree --package strata-engine --edges=normal` shows no strata-vector
- `cargo bench --package strata-engine` still builds (primitive + transaction benchmarks unaffected)

### Effort: 1-2 hours

---

## Epic T1-E5: Acceptance Matrix Scaffolding

**Goal:** Create `docs/design/quality-cleanup/acceptance-matrix.toml` seeded with entries from all 10 scope documents + 3 verification tracks + the product capability inventory. ~250 entries estimated.

**Why:** Machine-readable progress tracking. Every subsequent epic marks its entries as `passing`. T9 verifies zero `pending` or `blocked` entries remain. The capability inventory entries prevent the cleanup from accidentally preserving architecture while deleting user-facing product behavior.

### Changes

**1. Create `acceptance-matrix.toml`** (new file, ~1200 lines estimated)

Schema per entry (from test-coverage-scope.md D-TC-1):
```toml
[[tests]]
name = "retrieval_uniform_authority"
scope = "search-intelligence-inference"
phase = "SII-1"
tranche = 6
status = "pending"           # pending | passing | non_goal | blocked
evidence_kind = "rust_test"  # rust_test | ci_job | script_check | doc_gate
                             # feature_matrix | benchmark | structural | manual_review
file = ""
notes = ""
```

**2. Extraction methodology**

Read each scope document's acceptance criteria sections and Phase definitions. For each named test/assertion, create one `[[tests]]` entry. Sources:

| Source | Estimated entries |
|--------|-------------------|
| runtime-consolidation-scope.md | ~25 |
| durability-recovery-scope.md | ~30 |
| branch-primitives-scope.md | ~30 |
| product-control-surface-scope.md | ~20 |
| search-intelligence-inference-scope.md | ~20 |
| workspace-foundation-scope.md | ~15 |
| error-taxonomy-scope.md | ~20 |
| type-safety-scope.md | ~15 |
| visibility-and-module-structure-scope.md | ~20 |
| test-coverage-scope.md | ~10 |
| 3 verification tracks (V-Track 1,2,3) | ~10 |
| product-capability-inventory.md capability families | ~35 |
| **Total** | **~250** |

The capability inventory entries do not need one test per capability ID in T1.
They need one family-level matrix entry for each capability family in
`docs/design/architecture-cleanup/product-capability-inventory.md` section 6,
plus specific entries for known open gaps such as hardware profile open-path
coverage. Later tranches may split a family entry into more granular entries as
they touch the code.

**3. Pre-populate already-passing entries**

Grep the workspace for tests matching matrix entry names. Any test that already exists and passes gets `status = "passing"` with its file path in the `file` field.

### Files touched
- `docs/design/quality-cleanup/acceptance-matrix.toml` (new, ~1200 lines)

### Verification
- File parses as valid TOML (`python3 -c "import tomllib; tomllib.load(open(...))"` or `cargo test` with inline parser)
- Contains entries for all 13 scope values (10 scopes + 3 verification tracks)
- Contains capability-family entries from `product-capability-inventory.md`
- Every entry has `status`, `evidence_kind`, `scope`, `name` fields
- At least some entries are `passing` (existing tests)

### Effort: 4-6 hours (document extraction is the bottleneck)

---

## Epic T1-E6: `#[non_exhaustive]` Rollout (Quality Enabler)

**Goal:** Add `#[non_exhaustive]` to every public error enum in the workspace and add catch-all `_ => ...` arms at every exhaustive match site. Attribute-only change — no variant restructuring.

**Why:** Every architecture phase (T2–T6) that adds error variants would break downstream `match` without `#[non_exhaustive]`. Landing this now means architecture phases can freely add variants without coordinating catch-all migration.

### Inventory (verified 2026-04-10)

**29 public error enums across 8 crates** (scope expansion from ET-1's original 20-enum list — see below).

The error-taxonomy scope (ET-1, line 1074) lists 20 enums grounded in `grep -rn "pub enum .*Error"`. Deep code research found 9 additional enums matching that grep: LimitError, JsonLimitError, JsonPathError, PathParseError, BranchNameError (core), CommitError, PayloadError (concurrency), EventLogValidationError (engine), LlmClientError (search). Plus ErrorSeverity (explicitly listed in the scope but not matching the grep pattern). ErrorCode and ConstraintReason are classification enums, not error enums — excluded from this rollout.

**Scope sync status:** The execution doc (`tranche-1-foundation.md` E6) and the error-taxonomy scope (ET-1 phase list, D-ET-4) have been updated to 29 enums with explicit ErrorCode/ConstraintReason exclusion (amended 2026-04-10).

**Core crate — `crates/core/src/error.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `StrataError` | 452 | 21 | Unified API error type, 38 factory methods |

**Excluded from rollout** (classification enums, not error enums): `ErrorCode` (line 96, 10 variants — wire-frozen), `ConstraintReason` (line 265, 18 variants — structured reasons). **Scope amendment required:** D-ET-4 says "every public error enum" which could be read to include these. Amend D-ET-4 to explicitly exclude classification/reason enums, or include them — decide before E6 starts.

**Core crate — `crates/core/src/limits.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `LimitError` | 275 | ~5 | Resource limit violations |

**Core crate — `crates/core/src/primitives/json.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `JsonLimitError` | 62 | ~11 | JSON document limits |
| `PathParseError` | 411 | ~11 | JSONPath parsing |
| `JsonPathError` | 990 | ~11 | JSONPath evaluation |

**Core crate — `crates/core/src/contract/branch_name.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `BranchNameError` | 57 | 6 | Branch name validation |

**Executor crate — `crates/executor/src/error.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `Error` | 45 | 29 | Command execution errors (serializable) |
| `ErrorSeverity` | ~300 | 3 | UserError / SystemFailure / InternalBug |

**Vector crate — `crates/vector/src/error.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `VectorError` | 8 | 15+ | Has `From<VectorError> for StrataError` |

**Inference crate — `crates/inference/src/error.rs`:**

| Enum | Line | Variants | Notes |
|------|------|----------|-------|
| `InferenceError` | 3 | 5 | 33+ match sites (many in tests) |

**Concurrency crate:**

| Enum | File | Line | Variants | Notes |
|------|------|------|----------|-------|
| `CommitError` | `transaction.rs` | 28 | 13 | Has `From<CommitError> for StrataError` |
| `PayloadError` | `payload.rs` | 149 | 2 | WAL payload serialization |

**Durability crate — 15 enums:**

| Enum | File | Line | Variants |
|------|------|------|----------|
| `CompactionError` | `compaction/mod.rs` | 117 | 5 |
| `TombstoneError` | `compaction/tombstone.rs` | 438 | 3 |
| `BranchBundleError` | `branch_bundle/error.rs` | 8 | 14 |
| `CodecError` | `codec/traits.rs` | 54 | 3 |
| `ManifestError` | `format/manifest.rs` | 322 | 6 |
| `WatermarkError` | `format/watermark.rs` | 146 | 1 |
| `SnapshotHeaderError` | `format/snapshot.rs` | 127 | 2 |
| `SegmentMetaError` | `format/segment_meta.rs` | 199 | 5 |
| `WritesetError` | `format/writeset.rs` | 477 | 5 |
| `WalRecordError` | `format/wal_record.rs` | 807 | 5 |
| `PrimitiveSerializeError` | `format/primitives.rs` | 443 | 3 |
| `WalConfigError` | `wal/config.rs` | 75 | 2 |
| `WalReaderError` | `wal/reader.rs` | 579 | 4 |
| `CheckpointError` | `disk_snapshot/checkpoint.rs` | 220 | 1 |
| `SnapshotReadError` | `disk_snapshot/reader.rs` | 242 | 11 |

**Engine crate:**

| Enum | File | Line | Variants |
|------|------|------|----------|
| `EventLogValidationError` | `primitives/event.rs` | 160 | 10 |

**Search crate:**

| Enum | File | Line | Variants |
|------|------|------|----------|
| `LlmClientError` | `llm_client.rs` | 14 | 4 |

### Changes

**1. Add `#[non_exhaustive]` attribute to each enum** (29 edits, one per enum)

Each public error enum gets the attribute on the line immediately before `pub enum`:

```rust
// Before (crates/core/src/error.rs:452):
#[derive(Debug, Error)]
pub enum StrataError {

// After:
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StrataError {
```

Same pattern for all 29 enums. No variant changes.

**2. Add catch-all `_ => ...` arms to exhaustive match statements**

Per the error-taxonomy scope (ET-1, line 1082), catch-all arms must either:
- **Log:** `_ => { tracing::warn!(target: "strata::error::exhaustive", "unhandled variant in {location}"); <default> }`
- **Panic:** `_ => unreachable!("all variants handled above — new variant introduced without match update")` (only for code paths where every variant is known to be handled)

**Prerequisite for core catch-alls:** `strata-core` currently has no `tracing` dependency. Add `tracing = { workspace = true }` to `crates/core/Cargo.toml` `[dependencies]` as part of this epic (tracing is already a workspace dep used by 6+ crates). This is the minimal addition needed to support `tracing::warn!` in catch-all arms.

Key match sites to update (non-test code — test code can use `matches!()` or specific patterns):

**`crates/core/src/error.rs` — StrataError impl methods:**
- `code()` (~line 1248): exhaustive match mapping variants to `ErrorCode` → add:
  ```rust
  _ => { tracing::warn!(target: "strata::error::exhaustive", "unmapped StrataError variant in code()"); ErrorCode::InternalError }
  ```
- `details()` (~line 1300): exhaustive match returning `ErrorDetails` → add:
  ```rust
  _ => { tracing::warn!(target: "strata::error::exhaustive", "unmapped StrataError variant in details()"); ErrorDetails::new() }
  ```
- `is_not_found()`, `is_conflict()`, `is_transaction_error()`, `is_validation_error()`, `is_storage_error()`, `is_retryable()`, `is_serious()`: all exhaustive boolean matches → add:
  ```rust
  _ => { tracing::warn!(target: "strata::error::exhaustive", "unmapped StrataError variant in is_*()"); false }
  ```
  Rationale: `false` is the conservative safe default for all classifiers (not-found=false, conflict=false, retryable=false are all safe). Logging ensures the miss is visible rather than silently swallowed.

**`crates/vector/src/error.rs` — VectorError conversion:**
- `into_strata_error()` (~line 161): exhaustive match → add:
  ```rust
  _ => { tracing::warn!(target: "strata::error::exhaustive", "unmapped VectorError variant"); StrataError::internal(format!("unmapped vector error: {self}")) }
  ```
- `From<VectorError> for StrataError` (~line 189): same pattern

**`crates/vector/src/heap.rs`, `crates/vector/src/store/mod.rs`:**
- VectorError match sites → add logging `_ =>` arms

**`crates/inference/src/` — InferenceError matches:**
- 33+ match sites (many in tests using `matches!()` — these don't need changes)
- Non-test exhaustive matches: add logging `_ =>` arm

**Estimated total catch-all additions:** ~25-35 across non-test code.

### Files touched
- `crates/core/Cargo.toml` (add `tracing = { workspace = true }` dependency)
- ~25 files containing the 29 public error enums (some files contain multiple enums, e.g. `crates/durability/src/format/` has several)
- ~15-20 files containing exhaustive match statements (catch-all arms)

### Verification
- **Exact-list check:** grep `#[non_exhaustive]` immediately before each of the 29 named enums from the ET-1 scope inventory (not just a count — verify the specific enums, and verify `ErrorCode` and `ConstraintReason` do NOT have the attribute)
- `cargo test --workspace` passes (catch-all arms don't break existing behavior)
- `cargo clippy --workspace --all-targets --all-features` clean (no `-- -D warnings`)
- No variant restructuring — diff shows only attribute additions and `_ =>` arm additions

**Benchmark note:** ET-1 is code-changing (attribute additions + catch-all arms) but does not affect runtime behavior — benchmark exempted per NRR S2 classification.

### Effort: 4-6 hours (mechanical but many files)

---

## Epic T1-E7: TxnId and CommitVersion (Quality Enabler)

**Goal:** Introduce `TxnId(u64)` and `CommitVersion(u64)` newtypes in `strata_core::id`. Migrate all bare semantic `u64` parameters. Eliminate the class of bugs where `txn_id` and `timestamp` (both `u64`) can be silently swapped.

**Why:** 88 bare `u64` parameters represent 4 distinct semantic domains (transaction ID, commit version, timestamp, fork version). The WAL hot path has signatures like `append_inner(encoded, txn_id: u64, timestamp: u64)` where swapping two adjacent parameters silently corrupts WAL metadata and causes permanent data loss.

### Inventory (verified 2026-04-10)

**88 bare `u64` semantic parameters across 5 crates:**

| Domain | Identifier patterns | Count | Crates |
|--------|-------------------|-------|--------|
| Transaction ID | `txn_id`, `max_txn_id`, `min_txn_id` | 30 | concurrency (8), durability (15), engine (7) |
| Commit Version | `commit_version`, `start_version`, `snapshot_version`, `fork_version`, `max_version`, `min_version`, `version` (commit ctx) | 46 | storage (33), engine (9), concurrency (3 non-test), core (4), search (1), durability (2) — note: 4 concurrency sites are test-only |
| Commit ID | `commit_id`, `max_commit_id` (semantically CommitVersion — assigned at commit) | 12 | storage (11), durability (1) |
| **Total** | | **88** (not counting `timestamp: u64` which stays bare) | |

**No existing newtypes:** Neither `TxnId` nor `CommitVersion` exist anywhere in the workspace. No `crates/core/src/id.rs` module exists.

**StrataError status:** `VersionConflict` already uses a `Version` enum (not bare `u64` — verified at `crates/core/src/error.rs:550`), so no cross-scope edit is needed for this variant. `BlockedRecordRequiresAdminIntervention` does not exist yet — it will be added in T2 with `TxnId` fields from the start.

**Scope sync status:** The type-safety scope (V-TS-10) and execution doc (T1-E7) have been amended (2026-04-10) to reflect that `VersionConflict` already uses `Version` enum and `BlockedRecordRequiresAdminIntervention` is a T2 addition.

### Dangerous parameter-swap sites (motivating examples)

These are real signatures in the codebase where two adjacent `u64` parameters represent different semantic domains:

```rust
// crates/durability/src/format/wal_record.rs:585
// Swapping txn_id and timestamp corrupts WAL metadata PERMANENTLY
pub fn new(txn_id: u64, branch_id: [u8; 16], timestamp: u64, writeset: Vec<u8>) -> Self

// crates/durability/src/format/segment_meta.rs:72
// Swapping txn_id and timestamp corrupts segment bounds tracking
pub fn track_record(&mut self, txn_id: u64, timestamp: u64)

// crates/durability/src/wal/writer.rs:278
// WAL WRITE HOTPATH — swap = silent corruption
fn append_inner(&mut self, encoded: &[u8], txn_id: u64, timestamp: u64)

// crates/concurrency/src/payload.rs:100-107
// THREE bare u64s in sequence: version, txn_id, timestamp
pub fn serialize_wal_record_into(
    record_buf: &mut Vec<u8>, msgpack_buf: &mut Vec<u8>,
    txn: &TransactionContext,
    version: u64, txn_id: u64, branch_id: [u8; 16], timestamp: u64,
)

// crates/engine/src/coordinator.rs:299
// HashMap<txn_id, start_version> — swap breaks GC safe-point calculation
pub fn record_start(&self, txn_id: u64, start_version: u64)
```

After migration, these become:
```rust
pub fn new(txn_id: TxnId, branch_id: [u8; 16], timestamp: u64, writeset: Vec<u8>) -> Self
pub fn track_record(&mut self, txn_id: TxnId, timestamp: u64)
fn append_inner(&mut self, encoded: &[u8], txn_id: TxnId, timestamp: u64)
pub fn serialize_wal_record_into(..., version: CommitVersion, txn_id: TxnId, ...)
pub fn record_start(&self, txn_id: TxnId, start_version: CommitVersion)
```

The compiler now catches any `TxnId`/`u64`/`CommitVersion` swap at compile time.

### Changes

**1. Create `crates/core/src/id.rs`** (new file, ~120 lines)

```rust
//! Strongly-typed identifiers for transaction and version tracking.
//!
//! These newtypes prevent accidental parameter swaps between
//! transaction IDs, commit versions, and timestamps — all of which
//! are `u64` at the wire level.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a transaction.
///
/// Assigned by `TransactionManager` at transaction start. Monotonically
/// increasing within a database instance. Used in WAL records, segment
/// metadata, watermark tracking, and GC coordination.
///
/// Wire format: bare `u64` (via `#[repr(transparent)]` + `#[serde(transparent)]`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct TxnId(pub u64);

/// Monotonic commit version (MVCC sequence number).
///
/// Assigned at commit time. Used for snapshot isolation (`start_version`),
/// version-bounded reads (`max_version`), fork points (`fork_version`),
/// and garbage collection (`min_version`).
///
/// Wire format: bare `u64` (via `#[repr(transparent)]` + `#[serde(transparent)]`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct CommitVersion(pub u64);

impl TxnId {
    /// The zero transaction ID (used as "no transaction" sentinel).
    pub const ZERO: Self = Self(0);

    /// Create the next sequential transaction ID.
    #[inline]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Return the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl CommitVersion {
    /// The zero version (used as "no version" or "before any commit" sentinel).
    pub const ZERO: Self = Self(0);

    /// The maximum version (used for "latest" reads: `get_versioned(key, CommitVersion::MAX)`).
    pub const MAX: Self = Self(u64::MAX);

    /// Create the next sequential version.
    #[inline]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Return the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({})", self.0)
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

impl fmt::Debug for CommitVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommitVersion({})", self.0)
    }
}

impl fmt::Display for CommitVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u64> for TxnId {
    #[inline]
    fn from(v: u64) -> Self { Self(v) }
}

impl From<TxnId> for u64 {
    #[inline]
    fn from(id: TxnId) -> Self { id.0 }
}

impl From<u64> for CommitVersion {
    #[inline]
    fn from(v: u64) -> Self { Self(v) }
}

impl From<CommitVersion> for u64 {
    #[inline]
    fn from(v: CommitVersion) -> Self { v.0 }
}
```

Note: `#[repr(transparent)]` guarantees identical layout to `u64`. `#[serde(transparent)]` ensures `TxnId(42)` serializes as `42`, not `{"TxnId": 42}` — critical for WAL/MANIFEST backward compatibility. Serde derives are unconditional because `strata-core` has `serde` as a normal dependency (not feature-gated). The `From<u64>` impls ease migration at call sites that construct from literals.

**2. Register module in `crates/core/src/lib.rs`** (~2 lines)

```rust
// Add after line 27 (pub mod value;):
pub mod id;

// Add to re-exports:
pub use id::{CommitVersion, TxnId};
```

**3. Migrate concurrency crate — 8 TxnId sites + 7 CommitVersion sites** (~15 edits)

`crates/concurrency/src/transaction.rs`:
```rust
// Line 402-413 (TransactionContext struct):
pub struct TransactionContext {
    pub txn_id: TxnId,               // was: u64
    pub branch_id: BranchId,
    pub start_version: CommitVersion, // was: u64
    pub read_set: HashMap<Key, u64>,  // ← stays u64 (these are per-key observed versions, not semantic CommitVersions)
    // ...
}

// Line 536 (constructor):
pub fn new(txn_id: TxnId, branch_id: BranchId, start_version: CommitVersion)

// Line 584:
pub fn with_store(txn_id: TxnId, branch_id: BranchId, store: Arc<SegmentedStore>)

// Line 1794:
pub fn reset(&mut self, txn_id: TxnId, branch_id: BranchId, ...)

// Line 130 (ApplyResult):
pub struct ApplyResult { pub commit_version: CommitVersion }
```

`crates/concurrency/src/manager.rs`:
```rust
// Line 206:
pub fn with_txn_id(initial_version: CommitVersion, max_txn_id: TxnId)
```

`crates/concurrency/src/recovery.rs`:
```rust
// Line 306:
pub max_txn_id: TxnId  // was u64
```

`crates/concurrency/src/payload.rs`:
```rust
// Lines 100-107:
pub fn serialize_wal_record_into(
    record_buf: &mut Vec<u8>, msgpack_buf: &mut Vec<u8>,
    txn: &TransactionContext,
    version: CommitVersion,  // was u64
    txn_id: TxnId,           // was u64
    branch_id: [u8; 16],
    timestamp: u64,           // stays u64 (not a semantic ID)
)
```

**4. Migrate durability crate — 15 TxnId sites + 2 CommitVersion sites** (~17 edits)

`crates/durability/src/format/wal_record.rs`:
```rust
// Line 569-571 (WalRecord struct):
pub struct WalRecord {
    pub txn_id: TxnId,  // was u64
    // ...
}

// Line 585 (constructor):
pub fn new(txn_id: TxnId, branch_id: [u8; 16], timestamp: u64, writeset: Vec<u8>) -> Self
```

**Binary format note:** `WalRecord` is manually serialized via `encode()`/`decode()` methods (not serde). The `txn_id` field is written as `txn_id.as_u64().to_le_bytes()` and read as `TxnId(u64::from_le_bytes(...))`. Wire format unchanged.

`crates/durability/src/format/segment_meta.rs`:
```rust
// Lines 48-51 (SegmentMeta struct):
pub min_txn_id: TxnId,  // was u64
pub max_txn_id: TxnId,  // was u64

// Line 72:
pub fn track_record(&mut self, txn_id: TxnId, timestamp: u64)

// Lines 190-191 (deserialization):
min_txn_id: TxnId(u64::from_le_bytes(...)),
max_txn_id: TxnId(u64::from_le_bytes(...)),
```

`crates/durability/src/format/watermark.rs`:
```rust
// Lines 69, 79:
pub fn is_covered(&self, txn_id: TxnId) -> bool
pub fn needs_replay(&self, txn_id: TxnId) -> bool
```

`crates/durability/src/wal/writer.rs`:
```rust
// Line 278:
fn append_inner(&mut self, encoded: &[u8], txn_id: TxnId, timestamp: u64)
```

`crates/durability/src/format/manifest.rs`:
```rust
// Line 307:
pub fn set_flush_watermark(&mut self, commit_id: CommitVersion)
```

**5. Migrate storage crate — 33 CommitVersion sites** (~33 edits)

The storage crate has the most sites because it implements the version-bounded read/write paths.

Key changes in `crates/storage/src/segmented/mod.rs`:
```rust
// Line 846:
pub fn set_version(&self, version: CommitVersion)

// Line 855:
pub fn advance_version(&self, version: CommitVersion)

// Line 4216 (READ HOTPATH — trait impl):
fn get_versioned(&self, key: &Key, max_version: CommitVersion) -> StrataResult<...>

// Line 4359 (WRITE HOTPATH):
fn apply_batch(&self, writes: Vec<(Key, Value, WriteMode)>, version: CommitVersion) -> ...

// Line 4403 (DELETE HOTPATH):
fn delete_batch(&self, deletes: Vec<Key>, version: CommitVersion) -> ...
```

`crates/storage/src/memtable.rs`:
```rust
// Lines 73, 82, 147, 165:
pub fn to_versioned(&self, commit_id: CommitVersion) -> ...
pub fn into_versioned(self, commit_id: CommitVersion) -> ...
pub fn put(&self, key: &Key, commit_id: CommitVersion, value: Value, is_tombstone: bool)
pub fn put_entry(&self, key: &Key, commit_id: CommitVersion, entry: MemtableEntry)
```

`crates/storage/src/key_encoding.rs`:
```rust
// Lines 142, 152:
pub fn encode(key: &Key, commit_id: CommitVersion) -> ...
pub fn from_typed_key_bytes(typed_key: &[u8], commit_id: CommitVersion) -> ...
```

**6. Migrate core crate trait — 4 sites** (~4 edits)

`crates/core/src/traits.rs`:
```rust
// Line 86 (THE canonical read trait):
fn get_versioned(&self, key: &Key, max_version: CommitVersion) -> StrataResult<Option<VersionedValue>>;
```

This is the root trait definition — all implementations in storage, engine, and executor must match.

**7. Migrate engine crate — 7 TxnId sites + 9 CommitVersion sites** (~16 edits)

`crates/engine/src/coordinator.rs`:
```rust
// Line 274: txn_id: TxnId (TransactionMetrics)
// Line 299: pub fn record_start(&self, txn_id: TxnId, start_version: CommitVersion)
// Line 317: pub fn record_commit(&self, txn_id: TxnId)
// Line 333: pub fn record_abort(&self, txn_id: TxnId)
// Line 349: fn finish_transaction(&self, txn_id: TxnId)
```

**8. Migrate search crate — 1 site** (~1 edit)

`crates/search/src/substrate.rs`:
```rust
// Line 88:
pub snapshot_version: CommitVersion  // was u64
```

### Hot path performance note

`CommitVersion` and `TxnId` are `#[repr(transparent)]` wrappers around `u64`. The compiler generates identical machine code for `CommitVersion(42).raw()` as for a bare `42u64`. Verified by:
- `size_of::<CommitVersion>() == size_of::<u64>()` (static_assertions)
- `align_of::<CommitVersion>() == align_of::<u64>()` (static_assertions)
- No pointer indirection, no heap allocation, no virtual dispatch

The `get_versioned` read hot path (called millions of times) passes `CommitVersion` by value (Copy trait), which is identical to passing `u64` by value.

### Files touched
- `crates/core/src/id.rs` (new, ~120 lines)
- `crates/core/src/lib.rs` (2 lines: mod + re-export)
- `crates/core/src/traits.rs` (~4 edits)
- `crates/concurrency/src/transaction.rs` (~8 edits)
- `crates/concurrency/src/manager.rs` (~2 edits)
- `crates/concurrency/src/recovery.rs` (~1 edit)
- `crates/concurrency/src/payload.rs` (~2 edits)
- `crates/concurrency/src/validation.rs` (~1 edit)
- `crates/durability/src/format/wal_record.rs` (~4 edits)
- `crates/durability/src/format/segment_meta.rs` (~5 edits)
- `crates/durability/src/format/watermark.rs` (~2 edits)
- `crates/durability/src/format/manifest.rs` (~1 edit)
- `crates/durability/src/wal/writer.rs` (~2 edits)
- `crates/storage/src/segmented/mod.rs` (~25 edits)
- `crates/storage/src/memtable.rs` (~4 edits)
- `crates/storage/src/key_encoding.rs` (~3 edits)
- `crates/storage/src/merge_iter.rs` (~4 edits)
- `crates/storage/src/seekable.rs` (~3 edits)
- `crates/engine/src/coordinator.rs` (~5 edits)
- `crates/engine/src/database/mod.rs` (~3 edits)
- `crates/engine/src/database/lifecycle.rs` (~1 edit)
- `crates/engine/src/branch_ops/mod.rs` (~3 edits)
- `crates/engine/src/search/index.rs` (~1 edit)
- `crates/engine/src/transaction/pool.rs` (~1 edit)
- `crates/search/src/substrate.rs` (~1 edit)
- Test files (~15-20 files with `u64` literal → `.into()` or `TxnId(42)` changes)

### Verification

**Build and test:**
- `cargo test --workspace` passes
- `cargo clippy --workspace --all-targets --all-features` clean (no `-- -D warnings`)

**Zero bare-u64 semantic parameters** (comprehensive grep across all affected crates):
- `grep -rn 'txn_id: u64' crates/concurrency/src/ crates/durability/src/ crates/engine/src/` returns zero results (excluding test literal construction)
- `grep -rn 'max_version: u64\|min_version: u64\|start_version: u64\|snapshot_version: u64\|fork_version: u64\|commit_version: u64' crates/storage/src/ crates/engine/src/ crates/concurrency/src/ crates/core/src/ crates/search/src/` returns zero results (excluding test literal construction)
- `grep -rn 'commit_id: u64\|max_commit_id: u64' crates/storage/src/ crates/durability/src/` returns zero results (excluding test literal construction)

**Core trait signature:**
- `grep -n 'fn get_versioned' crates/core/src/traits.rs` shows `max_version: CommitVersion` (not `u64`)

**Wire format unchanged:**
- `assert_eq!(bincode::serialize(&TxnId(42)).unwrap(), bincode::serialize(&42u64).unwrap())`
- `assert_eq!(bincode::serialize(&CommitVersion(42)).unwrap(), bincode::serialize(&42u64).unwrap())`
- Static assertions: `static_assertions::assert_eq_size!(TxnId, u64); static_assertions::assert_eq_align!(TxnId, u64);`
- Static assertions: `static_assertions::assert_eq_size!(CommitVersion, u64); static_assertions::assert_eq_align!(CommitVersion, u64);`

**Benchmark:**
- `/regression-check` passes (newtypes are on every hot path — performance must not degrade)

### Effort: 1.5-2 days (88 sites + ~50 test sites, all mechanical but in hot code)

---

## Dependency Graph & Execution

```
E1 (MSRV) ──→ E2 (workspace config) ──→ E3 (CI pipeline)
                      │                        │
                      ↓                        ↓
               E6 (#[non_exhaustive])    E4 (DAG guard)
                      │
                      ↓
               E7 (TxnId/CommitVersion)

E5 (acceptance matrix) ─── independent, parallel with E1–E4
```

**Sprint 1** (days 1-2): E1 + E5 in parallel
**Sprint 2** (days 3-5): E2 → E3 → E4 (sequential — E3 ships CI with normal-dep guard; E4 removes dev-deps and extends CI with manifest guard)
**Sprint 3** (days 6-7): E6 (#[non_exhaustive])
**Sprint 4** (days 8-10): E7 (TxnId/CommitVersion) + regression benchmarks

### Lockstep Sets

| Invariant | Must Land Together |
|-----------|-------------------|
| `#[non_exhaustive]` + catch-all arms | E6 — single PR touching all 29 enums |
| TxnId/CommitVersion + binary format reads | E7 — `.as_u64().to_le_bytes()` writes and `TxnId(u64::from_le_bytes(...))` reads must land together |
| `[workspace.lints]` + per-crate `[lints]` + attribute deletion | E2 — if lints and inheritors are split, you get either double-warnings or no warnings |

### Rollback

| Epic | Reversible | Notes |
|------|-----------|-------|
| E1 (MSRV) | Yes | Revert `rust-version` to 1.70 |
| E2 (workspace config) | Yes | Revert workspace lints, re-add per-crate attributes |
| E3 (CI pipeline) | Yes | Revert ci.yml |
| E4 (DAG guard) | Yes | Move benchmark back, re-add dev-dep |
| E5 (acceptance matrix) | Yes | Delete TOML file |
| E6 (#[non_exhaustive]) | Yes | Remove attributes + catch-all arms; additive change |
| E7 (TxnId/CommitVersion) | Mostly | ~88 sites + tests; wire format unchanged (`#[repr(transparent)]`) |

No storage format or persistence changes in any epic. TxnId/CommitVersion use `#[repr(transparent)]` + `#[serde(transparent)]` — on-disk format is identical to bare u64.

---

## Benchmark Obligations

E6 is benchmark-exempt per NRR S2 classification (no runtime behavior change).

E7 is the only epic that touches hot paths and requires regression benchmarks:

| Epic | Suite | Rationale |
|------|-------|-----------|
| E1-E4 | — | Infrastructure only, no runtime code |
| E5 | — | Doc/data only |
| E6 | — | Exempt per S2 (attribute-only, no runtime change) |
| E7 | YCSB + BEIR | Newtypes on every hot path (get_versioned, apply_batch, WAL write, commit) |
