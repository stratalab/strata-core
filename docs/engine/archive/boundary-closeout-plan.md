# Boundary Closeout Plan

## Purpose

This is the closeout plan for the engine/storage boundary normalization
workstream.

The earlier cleanup moved checkpoint, WAL compaction, snapshot pruning, generic
MANIFEST mechanics, generic decoded-row snapshot install, storage recovery
bootstrap, and storage runtime configuration application into storage. Engine
kept primitive snapshot decode, recovery policy, and public config.

Boundary closeout should not be another large mechanics migration. The goal is
to prove the new boundary is coherent, remove temporary residue where that is
safe, and add guardrails so the next engine cleanup phase does not accidentally
move storage concerns back into engine or engine semantics down into storage.

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [boundary-baseline-and-guardrails-plan.md](./boundary-baseline-and-guardrails-plan.md)
- [checkpoint-wal-compaction-mechanics-plan.md](./checkpoint-wal-compaction-mechanics-plan.md)
- [snapshot-decode-install-mechanics-plan.md](./snapshot-decode-install-mechanics-plan.md)
- [recovery-bootstrap-mechanics-plan.md](./recovery-bootstrap-mechanics-plan.md)
- [storage-config-application-plan.md](./storage-config-application-plan.md)
- [../storage/storage-engine-ownership-audit.md](../../storage/storage-engine-ownership-audit.md)
- [../storage/storage-charter.md](../../storage/storage-charter.md)

## Boundary Rule

Boundary closeout closes the boundary. It does not redraw it.

Storage owns:

- generic persistence mechanics
- generic transaction/runtime substrate mechanics
- WAL, snapshot, checkpoint, manifest, recovery, and storage config mechanics
- storage-local runtime defaults and validation
- raw substrate facts and storage-local errors

Engine owns:

- database orchestration and public database APIs
- product and open/bootstrap policy
- public config format and compatibility
- primitive semantics
- branch semantics and workflow policy
- recovery policy, lifecycle policy, and operator-facing reporting
- search/runtime behavior
- conversion from storage facts into engine behavior and `StrataError`

Storage must not learn:

- `StrataConfig` or engine `StorageConfig`
- primitive semantics or primitive section meaning outside storage-owned
  durable format surfaces
- graph, JSON, event, vector, search, executor, CLI, intelligence, or security
  policy vocabulary outside storage-owned durable format surfaces
- `TransactionCoordinator`
- operator-facing recovery/report language
- product hardware-profile policy

Engine should not regain:

- hand-written `SegmentedStore::set_*` storage-runtime application lists
- low-level checkpoint/WAL/snapshot/manifest/recovery mechanics
- storage codec registry construction logic outside the narrow public-config
  adapter and open orchestration paths
- direct inspection of storage test-only runtime fields from production code

The boundary closeout target is:

```text
storage owns substrate mechanics
engine owns semantics, policy, and orchestration
the remaining boundary calls are explicit, narrow, and guarded
```

## Starting State

At the end of the storage-runtime-config cleanup, the intended implementation shape is:

- engine public APIs still expose checkpoint, compact, open, recovery, config,
  and retention behavior
- storage owns checkpoint and WAL compaction runtime helpers
- storage owns generic decoded-row snapshot install
- storage owns storage recovery bootstrap and replay mechanics
- storage owns `StorageRuntimeConfig`, storage runtime derivation, store-local
  application, and global block-cache capacity application
- engine adapts public `StrataConfig.storage` into storage runtime config
- engine still owns primitive snapshot decode, recovery policy, subsystem
  recovery, transaction coordinator bootstrap, lifecycle checks, and public
  errors

Known closeout residue from the storage-runtime-config review:

- storage `_for_test` accessors are reachable to engine when the storage crate
  is built with `fault-injection`; engine production code does not currently
  call them, but the surface is broader than ideal
- `StorageConfig::effective_*` compatibility wrappers remain even though this
  repo now uses them only in tests
- snapshot-retention types clamp invalid zero values at construction and
  runtime application, but public fields can still be set to zero by direct
  struct literal
- tiny `memory_budget = 1` is covered at the storage builder level but not by
  an engine open end-to-end characterization
- recovery runtime accessors have correct visibility, but their comments do not
  fully document why `strata_config()` is private while
  `storage_runtime_config()` is `pub(crate)`
- block-cache capacity remains process-global; sequential updates overwrite
  earlier values, while concurrent opens can race with no deterministic winner.
  This is a pre-existing design constraint, not a storage-runtime-config
  regression

## Non-Negotiables

Do not move primitive semantics into storage.

Do not move `TransactionCoordinator` into storage as part of boundary closeout.
Coordinator ownership is a separate architecture question if we ever reopen it.

Do not change on-disk WAL, snapshot, manifest, or segment formats.

Do not change recovery loss/degradation policy.

Do not change public config serialization without an explicit compatibility
story.

Do not treat every remaining cross-boundary call as residue. Some seams are
intentional:

- engine primitive snapshot decode calling storage decoded-row install
- engine recovery policy calling storage recovery bootstrap
- engine open policy adapting public config into storage runtime config
- engine public APIs wrapping storage checkpoint/compact mechanics
- engine reports mapping raw storage facts into user-facing language

Do not broaden boundary closeout into graph/vector/search/security/executor
absorption. Boundary closeout should finish the storage boundary so that later
engine consolidation can start from a stable substrate split.

## Phase A - Boundary Audit And Guardrails

### Goal

Turn the final storage/engine boundary into runnable checks and a current
inventory.

### Scope

- audit current imports and dependency graph
- audit moved storage modules for primitive or engine vocabulary
- audit engine production code for direct storage test-accessor calls
- audit engine production code for duplicated storage setter lists
- record guard commands in this plan or a closeout note

### Guard Commands And Expected Results

Dependency direction:

```bash
cargo tree -p strata-storage --edges normal,build --depth 2
cargo tree -p strata-engine --depth 2
cargo tree -p strata-storage --edges normal,build --depth 4 | rg "strata-(engine|executor|executor-legacy|graph|search|vector|intelligence|security)|stratadb"
```

The final command should return no matches. The first two commands are
inspection commands for reviewers; the storage tree should contain only lower
crates and third-party dependencies, while the engine tree should show the
intended `engine -> storage` edge. The final guard rejects storage learning any
upper-crate dependency, not only `strata-engine`.

The inverse tree is useful context, but it is not a clean Phase A guard yet:

```bash
cargo tree -i strata-storage --workspace --depth 2
```

As of Phase A, this still shows direct upper-crate users such as graph, search,
vector, executor, and stratadb. That is existing pre-engine-consolidation
crate shape. Phase A only guards the storage/engine boundary direction; later
engine consolidation should reduce those upper-layer direct storage edges.

Primitive vocabulary below moved generic storage runtime/config surfaces:

```bash
rg -n "SnapshotSerializer|primitive_tags|KvSnapshotEntry|GraphSnapshotEntry|EventSnapshotEntry|JsonSnapshotEntry|VectorSnapshotEntry|VectorConfigSnapshotEntry" crates/storage/src/durability/decoded_snapshot_install.rs crates/storage/src/durability/recovery_bootstrap.rs crates/storage/src/runtime_config.rs
```

This is intentionally not a repo-wide storage grep. Storage still owns snapshot
format modules that contain primitive section tags and primitive snapshot DTO
codecs. The boundary violation would be those concepts leaking into the generic
decoded install, recovery bootstrap, or runtime config helpers moved by
the snapshot-install through storage-runtime-config cleanup.

Engine production use of storage test accessors:

```bash
rg -n "write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test" crates/engine/src --glob '*.rs' \
  | rg -v '^crates/engine/src/database/tests/open.rs:' \
  | rg -v '^crates/engine/src/database/recovery.rs:[0-9]+:\s+outcome\.storage\.(write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test)\(\),'
```

This allowlist guard should return no matches. The broad inventory still returns ten
expected matches, all in engine test code:

```bash
rg -n "write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test" crates/engine/src --glob '*.rs'
```

- `crates/engine/src/database/tests/open.rs`
- the `#[cfg(test)]` module in `crates/engine/src/database/recovery.rs`

Matches outside test modules are boundary regressions. Phase C decides whether
to narrow the storage test-accessor surface further.

Duplicated storage setter application in engine:

```bash
rg -n "set_max_branches|set_max_versions_per_key|set_max_immutable_memtables|set_write_buffer_size|set_target_file_size|set_level_base_bytes|set_data_block_size|set_bloom_bits_per_key|set_compaction_rate_limit" crates/engine/src --glob '*.rs'
```

Storage mentions of engine-owned config, error, and coordinator type names:

```bash
rg -n "StrataConfig|StorageConfig|StrataError|StrataResult|TransactionCoordinator" crates/storage/src --glob '*.rs'
```

### Deliverables

1. A current boundary audit section in this document or a successor closeout
   note.
2. Runnable guard commands with expected results.
3. Any necessary code or doc fixes for false positives that obscure real
   regressions.

### Acceptance

- guard commands are documented
- known expected matches are explained
- no unexplained storage-to-engine dependency exists
- no storage module moved by the earlier mechanics cleanup phases imports primitive snapshot DTOs or engine
  policy types

### Phase A Current State

As of Phase A:

- `cargo tree -p strata-storage --edges normal,build --depth 4 | rg "strata-(engine|executor|executor-legacy|graph|search|vector|intelligence|security)|stratadb"` returns no matches
- the moved generic storage runtime/config primitive-vocabulary guard returns
  no matches
- the duplicated engine storage-setter guard returns no matches
- the storage engine-owned type/config/error guard returns no matches
- the storage runtime test-accessor production guards return no matches; the
  broad inventory returns ten expected matches, all in engine tests

Phase A also removed legacy storage-local `StrataError` / `StrataResult`
terminology from `txn/context.rs` and `txn/validation.rs`, plus stale comments
that mentioned engine error or coordinator names. Those aliases were local
names for storage errors, not actual engine dependencies, but they made the
guard noisy and obscured real future regressions.

## Phase B - Compatibility Residue

### Goal

Decide which migration-era compatibility helpers should remain public API,
which should be deprecated, and which can be deleted.

### Scope

- audit `StorageConfig::effective_block_cache_size`
- audit `StorageConfig::effective_write_buffer_size`
- audit `StorageConfig::effective_max_immutable_memtables`
- audit any other helper whose only remaining purpose was bridging old
  engine-side derivation to storage-owned runtime config
- check downstream public API exposure before deletion

### Decision Rule

If a helper is crate-private or test-only, delete it after replacing tests with
direct storage runtime config assertions.

If a helper is public and may have downstream callers, prefer one of:

- keep it as a compatibility wrapper with explicit docs
- deprecate it with a migration note
- remove it only in a planned breaking-change window

Do not reimplement storage derivation in engine to preserve a helper.

### Deliverables

1. A compatibility decision for each `effective_*` wrapper.
2. Tests updated to avoid hiding production ownership behind compatibility
   wrappers where possible.
3. Public docs/comments updated if wrappers remain.

### Acceptance

- no engine production code relies on `effective_*` wrappers for storage
  runtime behavior
- remaining wrappers delegate to storage-owned runtime config
- deleted wrappers do not break intended public compatibility

### Phase B Current State

As of Phase B, the compatibility decision is:

| Helper | Decision | Rationale |
|---|---|---|
| `StorageConfig::effective_block_cache_size` | Keep as compatibility wrapper | Public method; deleting it would be a breaking API change. |
| `StorageConfig::effective_write_buffer_size` | Keep as compatibility wrapper | Public method; deleting it would be a breaking API change. |
| `StorageConfig::effective_max_immutable_memtables` | Keep as compatibility wrapper | Public method; deleting it would be a breaking API change. |

These helpers are explicitly documented as compatibility helpers. They must
continue delegating to `storage_runtime_config_from`; they must not reintroduce
engine-owned effective-value derivation.

Repo-local production code no longer calls the wrappers. Tests generally assert
through `storage_runtime_config_from` so storage remains the owner of runtime
derivation; the only wrapper calls are in one compatibility test that proves the
public helpers still delegate to the storage runtime adapter. The enforcement
guard is:

```bash
if rg -n "effective_(block_cache_size|write_buffer_size|max_immutable_memtables)" crates/engine/src --glob '*.rs' \
  | rg -v '^crates/engine/src/database/config.rs:[0-9]+:\s+pub fn effective_' \
  | rg -v '^crates/engine/src/database/config.rs:[0-9]+:\s+cfg\.effective_(block_cache_size|write_buffer_size|max_immutable_memtables)\(\),'
then
  exit 1
fi
```

This exits non-zero only when unapproved wrapper calls are present.

## Phase C - Test Accessor Surface

### Goal

Make storage test-only observability harder to use from engine production code.

### Scope

- audit `SegmentedStore::*_for_test` accessors
- decide whether `fault-injection` should expose runtime introspection to
  engine production builds
- consider moving test accessors into a storage test-utils module or feature
  with a narrower name
- preserve current tests that need characterization access

### Design Options

Option 1: keep current accessors and add guardrails.

This is low risk and likely sufficient if Phase A guard commands become part of
review practice.

Option 2: move accessors behind a dedicated `test-utils` feature.

This makes intent clearer but may require feature plumbing in engine tests.

Option 3: expose structured storage runtime snapshots only under test builds.

This can reduce the number of individual `_for_test` methods, but it is a
larger API shape change and should be justified by actual misuse risk.

### Deliverables

1. A chosen test-accessor strategy.
2. Code changes if the chosen strategy is more than guardrails.
3. A guard proving engine production code does not call storage `_for_test`
   methods.

### Acceptance

- engine tests can still characterize storage runtime application
- engine production code does not call storage test accessors
- storage does not expose test accessors as an accidental product API

### Phase C Current State

Phase C chooses option 2: storage runtime introspection accessors are behind a
dedicated `test-utils` feature instead of `fault-injection`.

`fault-injection` remains available for tests that need actual failure hooks,
such as storage-apply failure injection. Engine dev-dependencies enable both
`fault-injection` and `test-utils`; the normal engine dependency on storage
continues to enable only `engine-internal`.

The storage-side feature split guard is:

```bash
if rg -n 'feature = "fault-injection"' crates/storage/src/segmented/mod.rs
then
  exit 1
fi
```

This exits non-zero only when segmented runtime introspection is still gated by
`fault-injection`.

The normal engine dependency feature guard is:

```bash
if cargo tree -p strata-engine --edges normal -e features \
  | rg 'strata-storage feature "(fault-injection|test-utils)"'
then
  exit 1
fi
```

This exits non-zero if normal engine builds expose storage fault hooks or
runtime test introspection. Dev builds intentionally enable both features for
tests.

The engine production guard is:

```bash
if rg -n "write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test" crates/engine/src --glob '*.rs' \
  | rg -v '^crates/engine/src/database/tests/open.rs:' \
  | rg -v '^crates/engine/src/database/recovery.rs:[0-9]+:\s+outcome\.storage\.(write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test)\(\),'
then
  exit 1
fi
```

This exits non-zero only when production engine code or an unapproved test path
calls the storage runtime introspection methods.

## Phase D - Retention API Tightening

### Goal

Clarify whether snapshot retention types expose raw configured values or only
valid effective values.

### Scope

- audit `SnapshotRetentionPolicy`
- audit `StorageSnapshotRetention`
- decide whether public fields should remain directly constructible
- decide whether runtime clamps are the authoritative compatibility behavior

### Current Contract

At the storage-runtime-config closeout point, `retain_count = 0` is tolerated
and clamped to one by constructor and runtime application paths. This preserves
the invariant that storage should retain at least one snapshot when snapshot
pruning runs.

### Design Options

Option 1: keep public fields and document raw-versus-effective behavior.

This is maximally compatible and matches current runtime clamps.

Option 2: make fields private and expose constructors plus
`effective_retain_count()`.

This prevents direct invalid literals but may be a public API break.

Option 3: introduce a validated newtype for effective retention count.

This is the strongest type-level model, but likely more ceremony than closeout
needs unless retention becomes a larger public API.

### Deliverables

1. A retention API decision.
2. Docs/comments updated to match the decision.
3. Tests proving direct zero inputs still behave according to the chosen
   compatibility story.

### Acceptance

- retention invariants are documented where the types are defined
- runtime behavior for `retain_count = 0` is explicit
- no storage pruning path can delete the last snapshot because of a raw zero
  field

### Phase D Current State

Phase D chooses option 1: keep the public fields and document raw-versus-effective
behavior.

`SnapshotRetentionPolicy::retain_count` remains engine-owned public
configuration. `StorageSnapshotRetention::retain_count` remains storage-owned
runtime input. Both fields accept compatibility values: `0` is accepted and is
clamped to an effective retention count of `1` before storage pruning applies
retention. `StorageSnapshotRetention::new(0)` stores the normalized value `1`,
while direct struct literals can still carry raw `0`; the pruning runtime
clamps again so the safety invariant does not depend on callers using the
constructor.

The storage regression test for direct raw zero is:

```bash
cargo test -p strata-storage prune_storage_snapshots_clamps_direct_zero_retention_to_one
```

The engine public-config regression test is:

```bash
cargo test -p strata-engine --lib retain_count_zero_is_treated_as_one_by_engine_pruning
```

## Phase E - Final Characterization Coverage

### Goal

Close small coverage gaps that are useful for future refactors but were not
required to land the storage-runtime-config cleanup.

### Scope

- add an engine end-to-end open test for `memory_budget = 1`
- consider primary/follower companions for "invalid open does not mutate global
  block cache" if current coverage is too cache-mode-specific
- add a one-line doc comment for recovery runtime accessor visibility
- document the process-global block-cache behavior, including sequential
  overwrite semantics and the pre-existing concurrent-open race

### Non-Goals

- no broad benchmark campaign
- no new storage validation semantics
- no behavior change to concurrent open handling

### Deliverables

1. Focused tests for any remaining high-signal edge cases.
2. Comments/docs for intentional asymmetries.
3. No changes to normal runtime behavior except bug fixes found by the tests.

### Acceptance

- tiny memory budgets are characterized through at least one engine open path
- global block-cache behavior is documented as process-global
- recovery runtime accessor visibility has an explicit comment

### Phase E Current State

Phase E adds the final narrow characterization coverage without changing runtime
behavior:

- primary open now characterizes `memory_budget = 1` end-to-end, proving the
  derived block-cache capacity is explicit `0` rather than auto-detect, the
  active write buffer is `0`, and the immutable-memtable count is `1`
- primary, follower, and cache invalid-codec opens all reject the unknown codec
  with stable, mode-specific errors; engine tests do not assert intermediate
  process-global block-cache capacity because concurrent opens can overwrite it
- `StorageRuntimeConfig::apply_global_runtime` and
  `block_cache::set_global_capacity` document that block-cache capacity is
  process-global, sequential updates overwrite earlier values, and concurrent
  opens can race
- `RecoveryRuntimeConfig::storage_runtime_config()` documents why storage
  runtime config is exposed to open orchestration while the public
  `StrataConfig` accessor remains private to recovery

The focused Phase E tests are:

```bash
cargo test -p strata-engine --lib primary_open_characterizes_tiny_memory_budget
cargo test -p strata-engine --lib rejects_invalid_codec
cargo test -p strata-storage apply_global_runtime_sequential_calls_overwrite_process_global_capacity
```

## Phase F - Final Boundary Map And Closeout

### Goal

Refresh the docs so they describe the settled architecture, not the migration
state.

### Scope

- update `engine-crate-map.md`
- update `../storage/storage-crate-map.md`
- update `../storage/storage-engine-ownership-audit.md` or add a successor
  closeout note
- update `engine-storage-boundary-normalization-plan.md` with the
  storage-boundary normalization implementation status
- record intentionally remaining seams

### Final Boundary Map

The closeout doc should explicitly classify these surfaces:

| Surface | Storage Owns | Engine Owns | Intentional Seam |
|---|---|---|---|
| Checkpoint | File construction, raw checkpoint facts, manifest/watermark mechanics | `Database::checkpoint()`, lifecycle checks, public result/error mapping | Engine API calls storage runtime helper |
| WAL compaction | WAL pruning and manifest active-segment mechanics | `Database::compact()`, lifecycle checks, public result/error mapping | Engine API calls storage runtime helper |
| Snapshot pruning | Filesystem pruning and raw prune facts | lifecycle-aware retention report | Engine maps raw prune facts to public report |
| Snapshot install | Generic decoded-row validation and install | primitive section decode, primitive install stats, recovery policy | Engine builds decoded-row plan, storage installs it |
| Recovery bootstrap | MANIFEST prep, codec validation, replay, raw recovery facts | open policy, degraded/lossy policy, subsystem recovery, coordinator bootstrap | Engine calls storage recovery and interprets outcome |
| Storage config | storage runtime config builder, effective storage values, store/global application | public `StrataConfig`, product profiles, config compatibility | Engine adapts public config into storage runtime config |
| Retention | storage pruning mechanics and minimum-retain invariant | public retention policy and lifecycle behavior | Engine passes storage-shaped retention input |
| Test observability | storage-local runtime snapshots/accessors under test gates | characterization tests | Guardrails prevent production dependence |

### Deliverables

1. Refreshed crate maps.
2. Refreshed audit or closeout note.
3. Updated main normalization plan status.
4. A final list of accepted boundary seams and why they remain.

### Acceptance

- docs and code agree on the engine/storage split
- there is no stale doc claiming engine owns moved storage mechanics
- every remaining seam has a short justification
- future engine consolidation work can cite this closeout as the storage
  boundary baseline

### Phase F Current State

Phase F refreshes the canonical boundary docs so they describe the settled
architecture after the storage-boundary normalization:

- [engine-crate-map.md](../engine-crate-map.md) now describes engine as the
  database orchestration and semantics layer over storage-owned mechanics
- [../storage/storage-crate-map.md](../../storage/storage-crate-map.md) now
  describes storage as the persistence, transaction, durability, recovery, and
  storage-runtime config substrate
- [../storage/storage-engine-ownership-audit.md](../../storage/storage-engine-ownership-audit.md)
  is the closeout audit for the accepted seams
- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
  records checkpoint/WAL, snapshot-install, recovery-bootstrap,
  storage-runtime-config, and closeout implementation status

Phase F is a documentation closeout. It does not by itself satisfy the final
broad closeout gate below; the full workstream should not be declared complete
until that gate is run and recorded.

The closeout intentionally keeps these seams:

| Surface | Storage Owns | Engine Owns | Why It Remains Split |
|---|---|---|---|
| Checkpoint | File construction, raw checkpoint facts, manifest/watermark mechanics | public API, lifecycle checks, public result/error mapping | Engine API over storage mechanics |
| WAL compaction | WAL pruning and manifest active-segment mechanics | public API, lifecycle checks, public result/error mapping | Engine decides when, storage executes how |
| Snapshot pruning | Filesystem pruning and raw facts | lifecycle-aware retention report | Storage counts files; engine reports policy |
| Snapshot install | Generic decoded-row validation/install | primitive decode, primitive stats, recovery policy | Primitive meaning stays in engine |
| Recovery bootstrap | MANIFEST prep, codec validation, replay, raw facts | open policy, degraded/lossy policy, subsystem recovery, coordinator bootstrap | Storage recovers state; engine decides database behavior |
| Storage config | effective storage values and application | public config, product profiles, compatibility | Engine adapts user config into storage runtime config |
| Retention | prune mechanics and minimum-retain invariant | public retention policy and lifecycle behavior | Storage preserves invariants; engine owns reports |
| Test observability | storage-local accessors under gates | characterization tests and production-use guards | Tests need visibility without production dependence |

Phase F does not change runtime behavior.

## Test Plan

Use focused tests for each boundary closeout change plus one final broad gate.

Focused commands likely include:

```bash
cargo fmt --all --check
cargo test -p strata-storage runtime_config
cargo test -p strata-storage snapshot_retention
cargo test -p strata-engine --lib database::tests::open::storage_runtime
cargo test -p strata-engine --lib database::config
cargo test -p strata-engine --lib database::tests::snapshot_retention
git diff --check
```

Final closeout gate should include the broader program checks where the local
environment supports them:

```bash
cargo check --workspace --all-targets
cargo test -p strata-storage --quiet
cargo test -p strata-engine --quiet
cargo test -p strata-executor --quiet
cargo test -p strata-intelligence --quiet
cargo test -p stratadb --tests --quiet
```

If known architecture-cleanup-period failures remain outside boundary closeout
scope, record the exact failing tests and verify they reproduce on the
pre-closeout baseline.

## Done Criteria

Boundary closeout is complete when:

1. The moved mechanics surfaces are guarded against boundary regression.
2. Compatibility helpers are deleted, deprecated, or explicitly documented.
3. Storage test observability cannot quietly become an engine production
   dependency.
4. Snapshot retention raw/effective behavior is explicit and tested.
5. Small storage-runtime-config edge coverage gaps are closed or intentionally
   deferred with rationale.
6. The main boundary plan and crate maps describe the settled architecture.
7. The remaining engine/storage seams are few, intentional, and documented.

The end state is not "engine has no storage calls." The end state is that every
engine-to-storage call is an explicit orchestration boundary from engine-owned
semantics and policy into storage-owned substrate mechanics.
