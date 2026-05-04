# ES5 Storage Config Application Plan

## Purpose

`ES5` is the storage-configuration application epic in the engine/storage
boundary normalization workstream.

`ES2` moved checkpoint, WAL compaction, snapshot pruning, and generic MANIFEST
sync mechanics toward storage. `ES3` moved generic decoded-row snapshot install
mechanics into storage while keeping primitive snapshot decode in engine. `ES4`
moved recovery bootstrap mechanics into storage and introduced the first
storage-owned `StorageRuntimeConfig` needed by recovery.

`ES5` completes that configuration boundary across the remaining open paths.
The goal is not to redesign public configuration. The goal is to stop
`strata-engine` from hand-applying storage setter lists and deriving
storage-local effective values while keeping engine in charge of public config
files, product profiles, compatibility behavior, and database open policy.

Read this together with:

- [engine-storage-boundary-normalization-plan.md](./engine-storage-boundary-normalization-plan.md)
- [es1-boundary-baseline-and-guardrails-plan.md](./es1-boundary-baseline-and-guardrails-plan.md)
- [es2-es4-storage-runtime-boundary-api-sketch.md](./es2-es4-storage-runtime-boundary-api-sketch.md)
- [es4-recovery-bootstrap-mechanics-plan.md](./es4-recovery-bootstrap-mechanics-plan.md)
- [../storage/storage-engine-ownership-audit.md](../storage/storage-engine-ownership-audit.md)
- [../storage/storage-charter.md](../storage/storage-charter.md)

## Boundary Rule

Storage owns storage runtime configuration mechanics.

Engine owns product configuration meaning.

Storage may know:

- `SegmentedStore`
- storage-local defaults for storage runtime knobs
- storage-local validation of storage runtime knob combinations
- effective-value derivation for storage resources
- global block-cache capacity application
- WAL writer config structs and durability mechanics
- storage codec ids and codec construction
- storage runtime config structs, builders, and application helpers

Storage must not know:

- `StrataConfig` or engine `StorageConfig`
- `strata.toml` file layout
- executor, CLI, intelligence, or profile-level config vocabulary
- product hardware-profile policy
- engine compatibility signatures
- `TransactionCoordinator`
- primitive snapshot section meaning
- primary/follower database open policy except where ES4 already requires a
  storage-neutral recovery mode

The ES5 target is:

```text
engine adapts public config into storage runtime config
storage derives and applies storage runtime mechanics
```

## Starting State

The important asymmetry after ES4 was:

- disk recovery already passes a storage-owned `StorageRuntimeConfig` into
  `run_storage_recovery`
- normal persistent open still configures block cache in
  `database/open.rs`
- ephemeral/cache open still calls `apply_storage_config` from
  `database/open.rs`
- engine still owns `StorageConfig::effective_block_cache_size`,
  `StorageConfig::effective_write_buffer_size`, and
  `StorageConfig::effective_max_immutable_memtables`
- engine still hand-applies store setters in `apply_storage_config`
- `StorageRuntimeConfig` currently lives under recovery bootstrap even though
  its purpose is broader than recovery

That means storage runtime configuration has two paths:

```text
persistent recovery:
    StrataConfig.storage -> engine helper -> StorageRuntimeConfig
    -> storage recovery applies to SegmentedStore

ephemeral/cache open:
    StrataConfig.storage -> engine effective values
    -> engine apply_storage_config -> SegmentedStore setters
```

ES5 makes those paths converge.

## Non-Negotiables

`StrataConfig` remains engine-owned.

The public config format remains stable unless a later PR explicitly adds a
compatibility migration. ES5 should not force users to edit existing
`strata.toml` files.

Storage must not depend on engine.

Storage must not learn product profiles. Hardware-profile adjustment may still
mutate engine `StrataConfig` before adaptation; storage should receive a
storage-shaped runtime input after that product decision has been made.

`TransactionCoordinator` write-buffer-entry limits stay engine-owned for ES5.
`max_write_buffer_entries` limits transaction coordinator behavior, not
`SegmentedStore` mechanics.

Ephemeral/cache databases remain truly in-memory. ES5 must not introduce a
persistent cache mode or any terminology that implies one exists.

WAL flush-thread lifecycle stays engine-owned unless a later ES5 step
explicitly designs a smaller storage-owned worker surface. The first ES5 pass
should not move shutdown latches, WAL writer health, accepting-transaction
halts, or database lifecycle callbacks into storage.

## Target API Sketch

The exact naming belongs to implementation, but the shape should be close to:

```rust
pub struct StorageRuntimeConfig {
    pub block_cache: StorageBlockCacheConfig,
    pub write_buffer_size: usize,
    pub max_branches: usize,
    pub max_versions_per_key: usize,
    pub max_immutable_memtables: usize,
    pub target_file_size: u64,
    pub level_base_bytes: u64,
    pub data_block_size: usize,
    pub bloom_bits_per_key: usize,
    pub compaction_rate_limit: u64,
}

pub enum StorageBlockCacheConfig {
    Auto,
    Bytes(usize),
}

impl StorageRuntimeConfig {
    pub fn builder() -> StorageRuntimeConfigBuilder;
    pub fn apply_to_store(&self, store: &SegmentedStore);
    pub fn apply_global_runtime(&self);
}
```

Engine should adapt public config by passing raw storage-facing fields into a
storage builder:

```rust
fn storage_runtime_config_from(config: &StorageConfig) -> StorageRuntimeConfig {
    StorageRuntimeConfig::builder()
        .memory_budget(config.memory_budget)
        .block_cache_size(config.block_cache_size)
        .write_buffer_size(config.write_buffer_size)
        .max_immutable_memtables(config.max_immutable_memtables)
        .max_branches(config.max_branches)
        .max_versions_per_key(config.max_versions_per_key)
        .target_file_size(config.target_file_size)
        .level_base_bytes(config.level_base_bytes)
        .data_block_size(config.data_block_size)
        .bloom_bits_per_key(config.bloom_bits_per_key)
        .compaction_rate_limit(config.compaction_rate_limit)
        .build()
}
```

This keeps the public field names in engine while moving the storage meaning of
those fields into storage.

## Config Classification

| Public config surface | ES5 target |
|---|---|
| `StorageConfig::memory_budget` | Engine-owned public field; storage-owned derivation into block cache, write buffer, and immutable memtable count. |
| `StorageConfig::max_branches` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::max_write_buffer_entries` | Stay engine; transaction coordinator limit. |
| `StorageConfig::max_versions_per_key` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::block_cache_size` | Engine-owned public field; storage-owned effective block-cache config and global application. |
| `StorageConfig::write_buffer_size` | Engine-owned public field; storage-owned effective write-buffer derivation. |
| `StorageConfig::max_immutable_memtables` | Engine-owned public field; storage-owned effective value derivation and store application. |
| `StorageConfig::l0_slowdown_writes_trigger` | Stay engine; transaction runtime slowdown policy. |
| `StorageConfig::l0_stop_writes_trigger` | Stay engine; transaction runtime stall policy. |
| `StorageConfig::background_threads` | Stay engine; scheduler sizing. |
| `StorageConfig::target_file_size` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::level_base_bytes` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::data_block_size` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::bloom_bits_per_key` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::compaction_rate_limit` | Engine-owned public field; storage-owned store application. |
| `StorageConfig::write_stall_timeout_ms` | Stay engine; transaction runtime timeout policy. |
| `StorageConfig::codec` | Split; engine owns public string compatibility, storage owns registry/construction and the current AES environment requirement. Future product secret-source policy remains engine-owned. |
| `StrataConfig::durability_mode` | Split; engine owns public string compatibility, storage owns WAL durability mechanics. |
| `SnapshotRetentionPolicy` | Split; engine owns public retention policy, storage owns snapshot prune mechanics. |

## ES5A - Inventory and Characterization

Create a focused inventory of every storage config application path.

Deliverables:

- map all `StorageConfig::effective_*` users
- map all `apply_storage_config` users
- map all direct `SegmentedStore::set_*` calls outside storage
- map block-cache global capacity configuration
- map WAL config construction and durability mode parsing
- identify which config fields are live, unused, engine-owned, or storage-owned
- add or update characterization tests before moving behavior

Expected characterization:

- persistent open with explicit storage config preserves current applied values
- persistent open with `memory_budget` preserves derived storage values
- ephemeral/cache open with explicit storage config preserves current applied
  values
- ephemeral/cache open with `memory_budget` preserves current derived storage
  values where they are currently applied
- ephemeral/cache open explicitly characterizes the current first-open
  `write_buffer_size` gap
- explicit write-buffer/max-immutable values still override defaults when
  `memory_budget == 0`
- compaction-related setters still land on `SegmentedStore`
- configured codec validation behavior remains unchanged
- block-cache global capacity is mapped and deferred from order-dependent
  assertions until ES5F because it is process-wide state

Acceptance:

- the ES5 plan has a concrete field-by-field map against the current code
- characterization fails if persistent and ephemeral/cache paths diverge for
  characterized common knobs
- known path divergence is captured as an explicit characterization test and
  follow-up gap
- no behavior movement happens before the current behavior is observable

### ES5A Current Inventory

As of ES5A, the live storage-configuration application surfaces are:

| Surface | Current owner | Current call sites | ES5 disposition |
|---|---|---|---|
| `StorageConfig::effective_block_cache_size` | Engine | `database/open.rs` persistent open, `database/open.rs` cache open, `database/mod.rs` runtime `update_config`, config tests | Move the derivation into storage runtime config; keep public input in engine. |
| `StorageConfig::effective_write_buffer_size` | Engine | `database/recovery.rs` recovery input, `database/mod.rs` runtime `update_config`, `database/transaction.rs` write-pressure checks, config tests | Move storage derivation into storage runtime config; keep transaction-pressure policy explicit in engine until redesigned. |
| `StorageConfig::effective_max_immutable_memtables` | Engine | `database/recovery.rs` runtime config adapter, `database/open.rs` cache open, `database/mod.rs` runtime `update_config`, `database/transaction.rs` write-pressure checks, config tests | Move storage derivation into storage runtime config; keep transaction-pressure policy explicit in engine until redesigned. |
| `database/open.rs::apply_storage_config` | Engine | Cache/ephemeral store construction | Replace with storage-owned runtime config application. |
| `database/mod.rs::apply_storage_config_inner` | Engine | Runtime `Database::update_config` path | Replace storage setter portion with storage-owned runtime config application; leave coordinator write-entry limit in engine. |
| `durability/recovery_bootstrap.rs::apply_storage_runtime_config` | Storage | ES4 recovery bootstrap | Promote out of recovery bootstrap and make it the common storage apply helper. |
| Direct `SegmentedStore::set_*` storage-knob calls in engine | Engine | `database/open.rs`, `database/mod.rs` | Remove or route through storage runtime config. |
| `SegmentedStore::set_snapshot_floor` | Engine | `database/transaction.rs` before compaction | Intentional engine exception; snapshot floor is engine MVCC policy, not static config application. |
| Block-cache global capacity | Mixed | `database/open.rs` persistent open, `database/open.rs` cache open, `database/mod.rs` runtime `update_config` | Move capacity selection/application into storage runtime config; engine keeps product profile mutation and operator-facing logging. |
| `WalConfig::default()` for primary WAL writer | Engine call site, storage type | `database/open.rs` | Audit in ES5G; do not move WAL lifecycle in the first ES5 pass. |
| `StrataConfig::durability_mode` | Engine public parser | config validation, primary/follower/cache open, runtime durability changes | Split later only if storage gets a lower runtime durability adapter; public string compatibility stays engine-owned. |
| `StorageConfig::l0_*` and `write_stall_timeout_ms` | Engine | `database/transaction.rs` write-stall policy | Stay engine for ES5 unless a later storage backpressure design moves the mechanism. |

ES5A characterization added/extended:

- persistent open applies observable store runtime knobs, including write
  buffer, branch/version limits, immutable-memtable limits, compaction rate
  limit, target file size, level-base bytes, block size, and bloom bits
- persistent open applies `memory_budget` derivations for write buffer and
  immutable-memtable limits
- cache/ephemeral open applies observable store runtime knobs
- cache/ephemeral open applies memory-budget-derived write buffer and
  immutable-memtable limits
- runtime `update_config` reapplies observable store runtime knobs, including
  write buffer and compaction rate limit
- recovery runtime config now characterizes the full storage runtime setter set
  that is safely inspectable from engine tests

Known ES5 follow-up gaps:

- block-cache global capacity remains a shared process-wide side effect, so
  tests should avoid order-dependent assertions until storage owns an explicit
  runtime helper for it

## ES5B - Promote StorageRuntimeConfig

Move `StorageRuntimeConfig` out of the recovery bootstrap module into a stable
storage runtime config module.

Likely target:

```text
crates/storage/src/runtime_config.rs
```

or:

```text
crates/storage/src/durability/runtime_config.rs
```

The module should be storage-neutral and reusable by recovery, persistent open,
and ephemeral/cache open.

Deliverables:

- move `StorageRuntimeConfig` and its defaults out of
  `durability/recovery_bootstrap.rs`
- re-export the type from an intentional storage module path
- keep `#[non_exhaustive]` on public config structs/enums
- preserve the ES4 recovery API behavior
- update docs and imports so recovery bootstrap is a consumer, not the owner

Acceptance:

- `StorageRuntimeConfig` no longer lives in recovery bootstrap
- storage recovery still accepts a storage runtime config
- engine still adapts from public `StorageConfig`
- no storage dependency on engine appears

### ES5B Current State

As of ES5B:

- `StorageRuntimeConfig` lives in `crates/storage/src/runtime_config.rs`
- storage re-exports `StorageRuntimeConfig` from the crate root and from
  `durability` for recovery-call-site compatibility
- recovery bootstrap imports the runtime config type instead of defining it
- `StorageRuntimeConfig::apply_to_store` owns the existing storage setter list
  used by recovery
- `SegmentedStore` constructors initialize storage runtime fields from
  `StorageRuntimeConfig::default()` so runtime-config defaults are the source
  of truth for those knobs
- runtime-config defaults and setter application are tested in the owning
  storage module
- recovery bootstrap tests only recovery input construction and recovery-time
  application behavior

## ES5C - Move Effective Storage Values Into Storage

Move the storage meaning of memory-budget derivation into the storage runtime
config builder.

Current engine helpers:

- `StorageConfig::effective_block_cache_size`
- `StorageConfig::effective_write_buffer_size`
- `StorageConfig::effective_max_immutable_memtables`

Target:

- engine passes public field values into a storage builder
- storage computes effective block cache, write buffer, and immutable memtable
  values
- engine may keep compatibility helpers temporarily if public tests depend on
  them, but production open paths should use the storage runtime config output

Acceptance:

- recovery write-buffer size comes from storage runtime config
- persistent block-cache capacity comes from storage runtime config
- ephemeral/cache store creation uses storage runtime config
- engine no longer needs to know the memory-budget derivation formula in open
  and recovery code

### ES5C Current State

As of ES5C:

- `StorageRuntimeConfig::builder()` accepts raw storage-facing fields plus
  `memory_budget`
- storage derives effective block-cache size, write-buffer size, and
  immutable-memtable limit inside the builder
- default storage runtime config preserves a store's constructor-provided
  write-buffer size unless a caller explicitly sets `write_buffer_size` or
  supplies `memory_budget`
- engine `StorageConfig::effective_*` helpers remain as compatibility wrappers
  that delegate to the storage builder instead of carrying the formula
- recovery no longer passes a separate write-buffer size; storage recovery uses
  `StorageRecoveryInput::runtime_config.write_buffer_size`
- persistent and follower open resolve block-cache capacity through
  `StorageRuntimeConfig`
- cache/ephemeral open applies `StorageRuntimeConfig` directly to the new
  `SegmentedStore`, fixing the ES5A write-buffer first-open gap
- runtime `update_config` applies `StorageRuntimeConfig` directly and resolves
  block-cache capacity through it
- engine write-pressure checks use `StorageRuntimeConfig` for the effective
  write-buffer and immutable-memtable values while retaining engine-owned
  transaction pressure policy

## ES5D - Centralize Store Application In Storage

Close out the remaining store-application cleanup around the storage-owned
application helper introduced in ES5B and used by ES5C.

Existing target API shape:

```rust
runtime_config.apply_to_store(&storage);
```

Deliverables:

- keep storage as the owner of the `SegmentedStore::set_*` list for storage
  runtime knobs
- recovery bootstrap continues using the same apply helper
- ephemeral/cache open continues using the same apply helper
- direct engine-side setter lists stay gone
- remaining direct `SegmentedStore::set_*` calls in engine are documented as
  engine-owned exceptions

Acceptance:

- `rg "fn apply_storage_config|apply_storage_config" crates/engine/src` has no
  production matches
- direct `SegmentedStore::set_*` calls in engine are either gone or documented
  as engine-owned exceptions
- recovery and ephemeral/cache open apply the same runtime config mechanics

### ES5D Current State

As of ES5D:

- `StorageRuntimeConfig::apply_to_store` is the storage-owned application point
  for static `SegmentedStore` runtime knobs
- recovery bootstrap, cache/ephemeral open, and runtime `update_config` all use
  `StorageRuntimeConfig::apply_to_store`
- engine has no `apply_storage_config` helper and no hand-written store setter
  list for storage runtime knobs
- the only remaining production engine `SegmentedStore::set_*` calls are
  documented exceptions:
  - `Database::schedule_flush_if_needed` calls `set_snapshot_floor` because the
    floor is engine MVCC policy computed from active transactions before
    compaction
  - follower open calls `set_version` only when restoring validated persisted
    follower state, paired with coordinator visible-version restore
- block-cache global application remains intentionally out of ES5D and is
  handled by ES5F

## ES5E - Route All Open Paths Through Runtime Config

Make persistent open and ephemeral/cache open share one adapter from public
engine config to storage runtime config.

Deliverables:

- introduce one engine-local adapter function from `StorageConfig` to
  `StorageRuntimeConfig`
- call it before persistent recovery
- call it before ephemeral/cache store construction
- pass the effective write-buffer size to `SegmentedStore::with_dir` through
  storage runtime config
- apply store-level runtime config through the storage-owned helper
- configure global block-cache capacity through the storage-owned runtime
  helper

Acceptance:

- persistent open, follower refresh, and ephemeral/cache open use the same
  storage runtime config adapter where applicable
- no open path recomputes storage effective values by hand
- no new disk behavior is introduced for ephemeral/cache databases

### ES5E Current State

As of ES5E:

- `database/config.rs::storage_runtime_config_from` is the single engine-local
  adapter from public `StorageConfig` to storage-owned `StorageRuntimeConfig`
- primary and follower open compute `StorageRuntimeConfig` before calling
  `Database::run_recovery` and pass that value through to storage recovery
- `Database::run_recovery` no longer adapts public engine config into storage
  runtime config internally
- storage recovery constructs `SegmentedStore::with_dir` from
  `StorageRecoveryInput::runtime_config.write_buffer_size` and then applies the
  same `StorageRuntimeConfig::apply_to_store` helper used by cache open and
  runtime `update_config`
- primary open, follower open, cache/ephemeral open, and runtime
  `update_config` configure global block-cache capacity through
  `StorageRuntimeConfig::apply_global_runtime`
- engine open code no longer calls `strata_storage::block_cache::set_global_capacity`
  directly outside tests
- ES5E includes a serial characterization for default cache open so
  `block_cache_size == 0` no longer silently inherits stale process-wide
  capacity; explicit profile-derived values are asserted exactly, while
  host-dependent auto-detect mode is asserted as a positive non-stale capacity
- `Database::run_recovery` accepts an engine-owned recovery config bundle that
  pairs `StrataConfig` with the `StorageRuntimeConfig` derived from the same
  public storage config, preventing future call-site drift

## ES5F - Block Cache Boundary

Normalize block-cache configuration as storage runtime mechanics.

The public input remains `StorageConfig::block_cache_size` and
`StorageConfig::memory_budget`. Storage should own:

- `Auto` versus explicit-byte runtime representation
- auto-detect capacity call
- global block-cache capacity application
- validation for impossible storage-local capacity values if needed

Engine should still own:

- hardware-profile mutations before adaptation
- operator-facing comments in `strata.toml`
- logging that describes product-level memory-budget behavior

Acceptance:

- block-cache global capacity is not configured by ad hoc engine code
- `memory_budget > 0` still derives the same block-cache capacity
- `block_cache_size > 0` with `memory_budget == 0` still behaves the same
- `block_cache_size == 0` with no memory budget still uses storage auto-detect

### ES5F Current State

As of ES5F:

- storage runtime config represents block-cache sizing as
  `StorageBlockCacheConfig::{Auto, Bytes(usize)}` instead of carrying the public
  `0 == auto` sentinel as the runtime representation
- engine public config remains unchanged: `StorageConfig::block_cache_size == 0`
  is still the compatibility spelling for automatic sizing in `strata.toml`
- `StorageRuntimeConfig::builder().block_cache_size(0)` converts raw public zero
  into `StorageBlockCacheConfig::Auto`
- `memory_budget > 0` derives an explicit `StorageBlockCacheConfig::Bytes(...)`
  value, including the degenerate `Bytes(0)` case for extremely small budgets,
  so a tiny derived budget cannot accidentally expand into host-level
  auto-detection
- `StorageRuntimeConfig::apply_global_runtime` remains the storage-owned global
  block-cache application point, and it resolves `Auto` through storage's
  `block_cache::auto_detect_capacity`
- engine logging and legacy `StorageConfig::effective_block_cache_size` use
  `StorageRuntimeConfig::block_cache_configured_bytes()` so operator-facing
  compatibility remains `0 == auto`

## ES5G - Codec and WAL Runtime Boundary

Audit codec and WAL writer settings after the storage runtime config path is
centralized.

This step should be conservative. The first target is not moving the WAL flush
thread. The first target is deciding which runtime settings are storage-owned
mechanics and which are engine-owned policy.

Likely storage-owned:

- codec registry lookup
- storage codec id validation against MANIFEST
- `WalConfig` defaults and storage-local validation
- durability mode mechanics inside `WalWriter`

Likely engine-owned:

- public durability string parsing and compatibility
- environment-secret override behavior for encrypted codecs
- WAL writer health latch
- accepting-transaction halt policy
- background flush thread lifecycle
- resume-after-halt public API

Acceptance:

- codec and WAL settings have an explicit field-by-field owner map
- no WAL lifecycle code moves without a narrower follow-up design
- any API changes preserve existing config-file compatibility

### ES5G Current State

As of ES5G:

| Surface | Owner | Current state |
|---|---|---|
| `StorageConfig::codec` public string | Engine | Remains a `strata.toml` field and compatibility-signature input. Engine owns when the public string is read, persisted, and compared for primary/follower/cache open compatibility. |
| Codec registry lookup and construction | Storage | `durability::get_codec` remains the storage-owned constructor. `durability::validate_codec_id` is the storage-owned preflight helper for call sites that need validation but not a codec value. |
| Encrypted codec secret source | Split/future | The current AES codec constructor still reads its storage-local environment requirement. ES5G does not add a second engine override path; future product-level secret-source policy stays engine-owned and should be designed separately from registry construction. |
| MANIFEST codec validation | Storage | `run_storage_recovery` validates the configured codec before recovery-managed side effects, checks existing MANIFEST codec ids, creates missing primary MANIFESTs with the configured codec id, and returns the resolved WAL codec. |
| Cache-mode `wal_codec` field population | Split | Engine keeps the uniform `Database::wal_codec` field because follower refresh/lifecycle code lives in engine; storage constructs the codec value via `get_codec`. |
| Checkpoint codec construction | Split | Engine chooses the public codec id from its config snapshot; storage constructs the codec and owns checkpoint byte-format mechanics. |
| `StrataConfig::durability` public string | Engine | Public values remain `"standard"` and `"always"`; `"cache"` stays rejected as a disk durability string. Existing config files remain compatible. |
| `DurabilityMode::Cache` | Storage type, engine policy input | Storage owns the no-WAL mechanics; engine owns where cache mode is allowed and how it maps to public open modes. `WalWriter::new` intentionally skips WAL config validation and file creation in this mode. |
| `DurabilityMode::Always` | Storage type, engine policy input | Storage owns per-append fsync mechanics; engine owns where the mode is allowed, runtime switching policy, compatibility signatures, and lifecycle decisions. |
| `DurabilityMode::Standard::interval_ms` | Storage type, engine policy input | Storage uses this as the active inline fallback deadline. Engine owns background flush thread scheduling and halt/resume policy. |
| `DurabilityMode::Standard::batch_size` | Storage type, compatibility field | Retained in the public type and compatibility signatures, but the current writer does not use it as an inline fsync trigger. Re-activating or removing it needs a separate WAL-runtime design. |
| `WalConfig::segment_size` | Storage | Storage owns the default, minimum-size validation, and segment-rotation interpretation. `WalWriter::new` validates this active non-cache invariant before creating the WAL directory. |
| `WalConfig::buffered_sync_bytes` | Storage compatibility field | Storage owns the default and full `WalConfig::validate()` consistency check, but the current writer does not consume this field as an inline fsync trigger. `WalWriter::new` does not reject a writer solely because this inert field exceeds `segment_size`. |
| WAL writer construction | Split | Engine constructs the primary `WalWriter` because it owns database open/lifecycle, lock ownership, health latches, and background flush thread wiring; storage owns writer internals, durability mechanics, segment rotation, and config validation. |
| Background WAL flush thread | Engine | No code moved. Engine still owns thread spawning, shutdown coordination, WAL writer health halt/resume policy, and accepting-transaction latches. |

ES5G intentionally does not move WAL lifecycle code. A future WAL-runtime epic
would need a narrower design before storage owns a worker surface.

## ES5H - Retention and Residual Config Cleanup

Close out the storage config application epic by documenting or moving the
remaining storage-shaped config surfaces.

Candidate surfaces:

- `SnapshotRetentionPolicy`
- compaction trigger knobs that are currently configured but may not be
  applied
- write-stall timeout ownership
- storage-local validation for impossible runtime combinations
- doc comments in public config that describe storage behavior now owned by
  storage

Acceptance:

- every storage-shaped field in `StorageConfig` has an explicit owner
- residual engine-owned fields are documented as product policy or transaction
  runtime policy
- storage-owned fields are either applied by storage runtime config or tracked
  as a future implementation gap

### ES5H Current State

As of ES5H, the final ownership map for public storage-shaped configuration is:

| Public surface | Owner | Current state |
|---|---|---|
| `StorageConfig::memory_budget` | Split | Engine owns the serialized public field. Storage derives effective block-cache, write-buffer, and immutable-memtable runtime values in `StorageRuntimeConfig::builder`. |
| `StorageConfig::max_branches` | Split | Engine owns the public field. Storage applies the branch limit through `StorageRuntimeConfig::apply_to_store`. |
| `StorageConfig::max_write_buffer_entries` | Engine | Transaction coordinator write-entry limit; not part of storage runtime config. |
| `StorageConfig::max_versions_per_key` | Split | Engine owns the public field. Storage applies MVCC version retention through `StorageRuntimeConfig::apply_to_store`. |
| `StorageConfig::block_cache_size` | Split | Engine owns `0 == auto` public compatibility. Storage owns `StorageBlockCacheConfig::{Auto, Bytes}` and global capacity application. |
| `StorageConfig::write_buffer_size` | Split | Engine owns the public field. Storage applies the effective memtable rotation threshold. |
| `StorageConfig::max_immutable_memtables` | Split | Engine owns the public field. Storage applies the effective frozen-memtable limit. Engine reads the same effective storage value only to drive transaction backpressure policy. |
| `StorageConfig::l0_slowdown_writes_trigger` | Engine | Transaction runtime slowdown policy in `database/transaction.rs`; disabled by default. |
| `StorageConfig::l0_stop_writes_trigger` | Engine | Transaction runtime stall policy in `database/transaction.rs`; disabled by default. |
| `StorageConfig::background_threads` | Engine | Database lifecycle scheduler sizing, open-time compatibility dimension, and hardware-profile product policy. |
| `StorageConfig::target_file_size` | Split | Engine owns the public field. Storage applies segment output sizing. |
| `StorageConfig::level_base_bytes` | Split | Engine owns the public field. Storage applies level sizing. |
| `StorageConfig::data_block_size` | Split | Engine owns the public field. Storage applies segment data-block sizing. |
| `StorageConfig::bloom_bits_per_key` | Split | Engine owns the public field. Storage applies segment bloom-filter sizing. |
| `StorageConfig::compaction_rate_limit` | Split | Engine owns the public field. Storage applies compaction I/O rate limiting. |
| `StorageConfig::write_stall_timeout_ms` | Engine | Transaction runtime timeout while waiting for L0 compaction; not part of storage runtime config. |
| `StorageConfig::codec` | Split | Engine owns the public string, compatibility signatures, and when it is read. Storage owns registry validation and codec construction. |
| `StrataConfig::durability` | Split | Engine owns public string compatibility and where modes are allowed. Storage owns WAL durability mechanics. |
| `SnapshotRetentionPolicy::retain_count` | Split | Engine owns the public policy. Storage owns pruning mechanics through `StorageSnapshotRetention`, including the minimum one-snapshot retention invariant. |

ES5H also leaves one intentional validation gap: storage runtime config now owns
where segment sizing and bloom/filter compaction knobs are applied, but ES5 does
not introduce new config-file rejection for historically accepted values such
as zero-sized segment-layout knobs. Tightening those values should be a future
storage validation PR with explicit compatibility notes.

Public `StorageConfig` comments and generated `strata.toml` comments were
updated to describe this final ownership split instead of embedding storage
derivation formulas or stale external-default claims in engine docs.

## Test Plan

Run focused characterization before and after each code-moving step:

```text
cargo test -p strata-engine --lib database::config
cargo test -p strata-engine --lib database::recovery
cargo test -p strata-engine --lib database::open
cargo test -p strata-engine --test recovery_storage_policy
cargo test -p strata-engine --test recovery_parity
cargo test -p strata-storage runtime_config
cargo test -p strata-storage recovery_bootstrap
```

Run package checks for each implementation PR:

```text
cargo fmt --all --check
cargo check -p strata-storage --all-targets
cargo check -p strata-engine --all-targets
```

Run broader suites when behavior moves across open/recovery paths:

```text
cargo test -p strata-storage
cargo test -p strata-engine
```

If known unrelated failures remain in the full engine suite, document the exact
failure count and confirm the focused ES5 tests pass.

## Boundary Guards

Useful guard commands:

```text
rg -n "strata_engine|strata-engine" crates/storage/src crates/storage/Cargo.toml
rg -n "StorageConfig|StrataConfig|strata.toml" crates/storage/src
rg -n "fn apply_storage_config|apply_storage_config" crates/engine/src
rg -n "set_max_branches|set_max_versions_per_key|set_max_immutable_memtables|set_write_buffer_size|set_target_file_size|set_level_base_bytes|set_data_block_size|set_bloom_bits_per_key|set_compaction_rate_limit" crates/engine/src
rg -n "storage\\.set_[A-Za-z0-9_]+|storage\\(\\)\\.set_[A-Za-z0-9_]+|self\\.storage\\.set_[A-Za-z0-9_]+|db\\.storage\\.set_[A-Za-z0-9_]+" crates/engine/src
rg -n "strata_storage::block_cache::set_global_capacity" crates/engine/src -g'!**/tests/**'
rg -n "StorageRuntimeConfig" crates/storage/src crates/engine/src
```

Expected end state:

- storage has no dependency on engine
- storage does not mention `StorageConfig` or `StrataConfig`
- engine has no hand-written `SegmentedStore` storage setter list
- the broad engine `storage.set_*` guard only reports documented non-runtime
  exceptions, currently `set_snapshot_floor` and follower `set_version`
- engine production code does not call the block-cache global setter directly
- `StorageRuntimeConfig` appears in storage as the owner type and in engine as
  an adapter target

## Risks

The main risk is moving product policy into storage under the label of
"defaults." Storage defaults are acceptable only when they describe storage
mechanics. Product profiles, operator-facing comments, compatibility
fingerprints, and public config migration stay in engine.

The second risk is accidentally changing open behavior while converging the
paths. This is why ES5A should characterize persistent and ephemeral/cache open
before any code movement.

The third risk is broadening ES5 into WAL lifecycle redesign. WAL writer
runtime construction can be cleaned up, but WAL health, shutdown, transaction
halt, and resume policy are engine lifecycle concerns unless a later subplan
draws a smaller callback-based surface.

## Done Criteria

ES5 is done when:

- all open paths use a storage-owned runtime config application path
- engine owns only the public config adapter, not storage setter mechanics
- storage owns effective storage resource derivation
- block-cache capacity application is no longer ad hoc in engine open code
- recovery still preserves ES4 ordering guarantees
- existing config files remain compatible
- tests cover persistent open, ephemeral/cache open, recovery, and memory-budget
  behavior
- residual WAL/retention/config fields are either resolved or explicitly
  documented for the next cleanup phase
