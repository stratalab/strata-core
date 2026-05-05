# V1 Storage Consumption Contract

## Purpose

This document defines the storage operations that the consolidated engine is
allowed to consume.

It is intentionally narrower than the full public Rust surface of
`strata-storage`. The storage crate exposes many implementation types because
the current crate is still mid-consolidation and because storage has its own
tests. That does not mean upper layers should treat every exported type as a
supported engine boundary.

The target stack is:

```text
core -> storage -> engine -> intelligence -> executor -> cli
```

In that stack, `strata-engine` is the only normal production crate above
storage that may depend on `strata-storage`. Graph, vector, search, security,
legacy bootstrap, executor, intelligence, CLI, and product code must consume
storage-backed behavior through engine-owned APIs.

This contract is a V1 boundary. It should be tightened as engine consolidation
and storage-next proceed.

Read this with:

- [storage-charter.md](./storage-charter.md)
- [storage-crate-map.md](./storage-crate-map.md)
- [storage-engine-ownership-audit.md](./storage-engine-ownership-audit.md)
- [storage-minimal-surface-implementation-plan.md](./storage-minimal-surface-implementation-plan.md)
- [../engine/engine-consolidation-plan.md](../engine/engine-consolidation-plan.md)
- [../engine/archive/engine-storage-boundary-normalization-plan.md](../engine/archive/engine-storage-boundary-normalization-plan.md)

## Contract Status

This is an allowlist for consolidated engine consumption.

Allowed means:

- engine production code may call the operation directly
- engine may adapt the returned storage fact into engine policy or public
  errors
- engine may hide the storage type behind an engine-owned public API

Allowed does not mean:

- executor, intelligence, CLI, or product code may import storage
- storage owns the higher-level semantics that motivated the call
- engine should re-export the storage type as a permanent public API
- the operation should survive storage-next unchanged

Anything not named in this document is not part of the engine/storage
consumption contract. It may still be public for storage tests, storage-local
modules, or transitional compatibility, but engine code should not add new use
without updating this document.

## Direct Consumer Rule

Only `strata-engine` may consume this contract in normal production code.

Allowed exceptions:

- `strata-storage` itself
- storage tests, benches, fuzz targets, and diagnostic tools
- engine tests that intentionally inspect storage behavior
- future storage-next migration or verification tools
- documented temporary migration shims during engine consolidation

Not allowed:

- `strata-executor`
- `strata-intelligence`
- `strata-cli`
- `stratadb`
- consolidated graph/vector/search/product code after it has moved out of peer
  crates

Upper crates should not call `Database::storage()` or import
`strata_storage::*`. If they need a storage-backed fact, engine should expose a
semantic API.

## Boundary Principles

### 1. Storage Owns Mechanics

Storage owns:

- physical keyspace
- MVCC state
- generic transactions
- OCC validation
- WAL, manifest, snapshot, checkpoint, compaction, and recovery mechanics
- storage runtime config application
- storage-local errors and raw health facts

### 2. Engine Owns Interpretation

Engine owns:

- primitive semantics
- branch and lifecycle semantics
- product open/bootstrap policy
- search, graph, vector, JSON, event, and KV behavior
- user-facing error conversion
- operator-facing reports
- public database APIs

Storage can return raw facts. Engine decides what they mean.

### 3. Storage Format Is Not Product Policy

Storage owns the byte format. Engine may read or populate storage-owned format
types only where the format is the durability interchange boundary.

Examples:

- engine may build checkpoint DTOs from primitive state
- engine may decode primitive snapshot sections during snapshot install
- engine may inspect primitive section tags while interpreting a snapshot

Storage must not learn JSON paths, event-chain meaning, vector model policy,
graph semantics, search policy, or executor behavior.

### 4. Engine APIs Should Hide Storage Types Above Engine

Some storage types are currently re-exported by engine for compatibility.
Those re-exports should be reviewed during consolidation. A storage type should
remain visible above engine only if it is explicitly part of an engine public
contract.

The long-term default is:

```text
storage type -> engine internal adapter -> engine-owned public type
```

## Allowed Production Operations

The following categories list the operations consolidated engine is allowed to
consume.

### 1. Addressing, Layout, And Errors

Allowed storage types:

- `Key`
- `Namespace`
- `TypeTag`
- `validate_space_name`
- `Storage`
- `WriteMode`
- `StorageError`
- `StorageResult`

Allowed addressing operations:

- `Namespace::new`
- `Namespace::for_branch`
- `Namespace::for_branch_space`
- `TypeTag::as_byte`
- `TypeTag::from_byte`
- `Key::new`
- `Key::new_kv`
- `Key::new_graph`
- `Key::new_event`
- `Key::new_event_meta`
- `Key::new_event_type_idx`
- `Key::new_event_type_idx_prefix`
- `Key::new_branch`
- `Key::new_branch_with_id`
- `Key::new_branch_index`
- `Key::new_json`
- `Key::new_json_prefix`
- `Key::new_vector`
- `Key::new_vector_config`
- `Key::new_vector_config_prefix`
- `Key::new_space`
- `Key::new_space_prefix`
- `Key::user_key_string`
- `StorageError::corruption`
- `StorageError::invalid_input`
- `StorageError::capacity_exceeded`
- `StorageError::kind`

Allowed usage:

- engine primitive modules may construct storage keys for engine-owned
  primitive data
- engine branch/search/vector/graph modules may inspect key namespace, type
  tag, and user-key bytes when translating physical rows into engine meaning
- engine may validate space names through the storage-owned physical layout
  helper until that validation is wrapped in an engine API
- engine may convert `StorageError` into `StrataError`, `RecoveryError`, or an
  engine-owned error/report type

Not allowed:

- upper crates importing `Key`, `Namespace`, `TypeTag`, or `validate_space_name`
  directly
- storage accepting engine primitive objects instead of generic keys and values
- new storage type tags without an explicit format and engine-meaning review

### 2. Store Construction And Runtime Configuration

Allowed storage types and functions:

- `SegmentedStore::new`
- `SegmentedStore::with_dir`
- `SegmentedStore::with_dir_and_pressure`
- `StorageRuntimeConfig`
- `StorageRuntimeConfig::builder`
- `StorageRuntimeConfig::apply_to_store`
- `StorageRuntimeConfig::apply_global_runtime`
- `StorageRuntimeConfig::resolved_block_cache_capacity`
- `StorageRuntimeConfig::block_cache_configured_bytes`
- `StorageBlockCacheConfig`
- `StorageBlockCacheConfig::from_raw_size`
- `StorageBlockCacheConfig::bytes`
- `StorageBlockCacheConfig::resolved_capacity`
- `StorageBlockCacheConfig::configured_bytes`
- `StorageBlockCacheConfig::is_auto`

Allowed usage:

- engine open/recovery may create the store for cache, primary, and follower
  modes
- engine may adapt public `StrataConfig.storage` into `StorageRuntimeConfig`
- engine may apply process-global storage runtime state before exposing an
  opened database
- engine may apply store-local runtime state to the `SegmentedStore`

Rules:

- storage owns the setter application list
- engine must not duplicate `SegmentedStore::set_*` application lists in
  production code
- global block-cache capacity is process-global and last-writer-wins; engine
  tests must not assume isolation without a guard

### 3. Generic Transaction Runtime

Allowed storage types:

- `TransactionManager`
- `TransactionContext`
- `TransactionStatus`
- `PendingOperations`
- `ApplyResult`
- `CASOperation`
- `CommitError`
- `ConflictType`
- `ValidationResult`

Allowed transaction-manager operations:

- `TransactionManager::new`
- `TransactionManager::with_txn_id`
- `TransactionManager::current_version`
- `TransactionManager::visible_version`
- `TransactionManager::mark_version_applied`
- `TransactionManager::bump_version_floor`
- `TransactionManager::quiesced_version`
- `TransactionManager::quiesce_commits`
- `TransactionManager::next_txn_id`
- `TransactionManager::allocate_version`
- `TransactionManager::commit`
- `TransactionManager::commit_with_version`
- `TransactionManager::commit_with_hook`
- `TransactionManager::remove_branch_lock`
- `TransactionManager::mark_branch_deleting`
- `TransactionManager::is_branch_deleting`
- `TransactionManager::unmark_branch_deleting`
- `TransactionManager::branch_commit_lock`
- `TransactionManager::catch_up_version`
- `TransactionManager::set_visible_version`
- `TransactionManager::catch_up_txn_id`
- `TransactionManager::has_branch_lock`

Allowed transaction-context operations:

- `TransactionContext::new`
- `TransactionContext::with_store`
- `TransactionContext::snapshot_store`
- `TransactionContext::get`
- `TransactionContext::get_versioned`
- `TransactionContext::exists`
- `TransactionContext::scan_prefix`
- `TransactionContext::get_read_version`
- `TransactionContext::set_max_write_entries`
- `TransactionContext::set_read_only`
- `TransactionContext::set_branch_generation_guard`
- `TransactionContext::set_branch_generation_guard_key`
- `TransactionContext::branch_generation_guard_is_seeded`
- `TransactionContext::set_allow_cross_branch`
- `TransactionContext::is_read_only_mode`
- `TransactionContext::put`
- `TransactionContext::put_with_ttl`
- `TransactionContext::put_replace`
- `TransactionContext::delete`
- `TransactionContext::cas`
- `TransactionContext::cas_with_read`
- `TransactionContext::clear_operations`
- `TransactionContext::is_active`
- `TransactionContext::is_committed`
- `TransactionContext::is_aborted`
- `TransactionContext::can_rollback`
- `TransactionContext::is_expired`
- `TransactionContext::elapsed`
- `TransactionContext::ensure_active`
- `TransactionContext::mark_validating`
- `TransactionContext::mark_committed`
- `TransactionContext::mark_aborted`
- `TransactionContext::pending_operations`
- `TransactionContext::commit`
- `TransactionContext::apply_writes`
- `TransactionContext::read_count`
- `TransactionContext::write_count`
- `TransactionContext::delete_count`
- `TransactionContext::cas_count`
- `TransactionContext::has_pending_operations`
- `TransactionContext::is_read_only`
- `TransactionContext::abort_reason`
- `TransactionContext::reset`
- `TransactionContext::capacity`

Allowed validation functions:

- `validate_read_set`
- `validate_cas_set`
- `validate_transaction`

Allowed usage:

- engine `TransactionCoordinator` may wrap these lower mechanics
- engine primitive modules may implement transactional behavior over
  `TransactionContext`
- engine may use transaction-manager branch locks to coordinate branch
  deletion, branch mutation, and commit visibility

Rules:

- product code should not receive raw `TransactionManager`
- raw `TransactionContext` exposure above engine should be treated as
  transitional unless a public engine transaction API explicitly needs it
- engine primitive code should prefer transactional reads through
  `TransactionContext` when OCC tracking matters

### 4. MVCC Reads, Writes, And Iteration

Allowed `Storage` trait operations:

- `get_versioned`
- `get_history`
- `scan_prefix`
- `current_version`
- `put_with_version_mode`
- `delete_with_version`
- `apply_batch`
- `delete_batch`
- `apply_writes_atomic`
- `get_version_only`

Allowed `SegmentedStore` read/list operations:

- `version`
- `current_version`
- `next_version`
- `branch_count`
- `branch_ids`
- `get_value_direct`
- `get_versioned_direct`
- `list_by_type`
- `list_own_entries`
- `list_by_type_at_version`
- `list_by_type_at_timestamp`
- `get_at_timestamp`
- `scan_prefix_at_timestamp`
- `scan_range`
- `count_prefix`
- `new_storage_iterator`
- `time_range`
- `branch_entry_count`
- `list_branch`

Allowed `SegmentedStore` write/version operations:

- `set_version`
- `advance_version`
- `clear_branch`
- `apply_recovery_atomic`
- `put_recovery_entry`
- `delete_recovery_entry`

Allowed iterator/result types:

- `StorageIterator`
- `VersionedEntry`
- `DecodedSnapshotEntry`
- `DecodedSnapshotValue`

Allowed usage:

- engine primitives may read and write storage rows through transactions or
  storage trait methods
- engine branch operations may use version-scoped listings for diff, merge,
  cherry-pick, revert, checkpoint, and bundle workflows
- engine recovery and follower refresh may apply recovered writes through
  recovery-specific atomic paths
- engine diagnostic/report code may use read-only listing and counting helpers

Rules:

- normal writes should go through engine transaction orchestration
- recovery-specific writes must only be used by recovery, snapshot install, or
  follower refresh code
- direct store reads must not replace transaction reads when read tracking is
  required

### 5. Branch, Version, And Copy-On-Write Mechanics

Allowed operations:

- `SegmentedStore::fork_branch`
- `SegmentedStore::get_fork_info`
- `SegmentedStore::inherited_layer_count`
- `SegmentedStore::branches_needing_materialization`
- `SegmentedStore::materialize_layer`
- `SegmentedStore::clear_branch`
- `SegmentedStore::gc_branch`
- `SegmentedStore::expire_ttl_keys`

Allowed result types:

- `MaterializeResult`
- `StorageInheritedLayerInfo`

Allowed usage:

- engine branch APIs may drive storage-level branch fork, clear, and
  materialization mechanics
- engine branch diff/merge/revert/cherry-pick code may inspect fork points and
  inherited-layer state
- engine lifecycle may trigger storage GC and TTL expiry

Rules:

- storage owns copy-on-write mechanics
- engine owns branch lifecycle, branch naming, branch control records, merge
  policy, and user-facing branch behavior
- storage branch deletion must be coordinated with engine branch lifecycle and
  transaction-manager branch locks

### 6. Durability Layout, Codec, WAL, And Commit Lifecycle

Allowed layout and codec types/functions:

- `DatabaseLayout`
- `DatabaseLayout::from_root`
- `DatabaseLayout::root`
- `DatabaseLayout::wal_dir`
- `DatabaseLayout::segments_dir`
- `DatabaseLayout::snapshots_dir`
- `DatabaseLayout::manifest_path`
- `DatabaseLayout::follower_state_path`
- `DatabaseLayout::follower_audit_path`
- `DatabaseLayout::create_dirs`
- `DatabaseLayout::create_non_segment_dirs`
- `DatabaseLayout::create_segments_dir`
- `DatabaseLayout::wal_exists`
- `DatabaseLayout::manifest_exists`
- `StorageCodec`
- `IdentityCodec`
- `get_codec`
- `validate_codec_id`
- `clone_codec`
- `CodecError`

Allowed WAL/durability types:

- `DurabilityMode`
- `WalConfig`
- `WalWriter`
- `WalReader`
- `WalCounters`
- `WalDiskUsage`
- `WalReaderError`
- `ReadStopReason`
- `TruncateInfo`
- `WatermarkBlockedRecord`
- `WatermarkReadResult`
- `WalRecordIterator`

Allowed durability-mode operations:

- `DurabilityMode::requires_wal`
- `DurabilityMode::requires_immediate_fsync`
- `DurabilityMode::description`
- `DurabilityMode::standard_default`

Allowed WAL config operations:

- `WalConfig::new`
- `WalConfig::with_segment_size`
- `WalConfig::with_buffered_sync_bytes`
- `WalConfig::validate`

Allowed WAL and payload operations:

- `WalWriter::new`
- `WalWriter::append`
- `WalWriter::append_pre_serialized`
- `WalWriter::flush`
- `WalWriter::sync_if_overdue`
- `WalWriter::durability_mode`
- `WalWriter::set_durability_mode`
- `WalWriter::current_segment`
- `WalWriter::current_segment_size`
- `WalWriter::counters`
- `WalWriter::wal_disk_usage`
- `WalWriter::flush_active_meta`
- `WalWriter::snapshot_active_meta`
- `WalWriter::wal_dir`
- `WalWriter::current_segment_meta`
- `WalWriter::list_segments`
- `WalWriter::reopen_if_needed`
- `WalWriter::append_and_flush`
- `WalWriter::close`
- `WalReader::new`
- `WalReader::with_codec`
- `WalReader::with_lossy_recovery`
- `WalReader::read_all`
- `WalReader::read_all_after_watermark`
- `WalReader::read_all_after_watermark_contiguous`
- `WalReader::list_segments`
- `WalReader::max_txn_id`
- `WalReader::iter_all`
- `TransactionPayload::from_transaction`
- `TransactionPayload::from_bytes`
- `TransactionPayload::to_bytes`
- `serialize_wal_record_into`
- `commit_with_wal`
- `commit_with_wal_arc`
- `commit_with_version`

Allowed engine-internal WAL extension:

- `durability::__internal::WalWriterEngineExt`
- `BackgroundSyncError`
- `BackgroundSyncHandle`

Allowed usage:

- engine open may create the WAL writer and codec from public config
- engine commit path may use storage's WAL-aware commit adapter
- engine shutdown and background sync may use the engine-internal WAL writer
  extension
- engine follower refresh may read WAL after watermarks
- engine health/metrics may read WAL counters, disk usage, and writer health

Rules:

- engine owns public durability mode selection and user-facing health
  classification
- storage owns WAL byte format, read/write mechanics, checksums, and sync
  mechanics
- raw WAL format operations should not leak above engine

### 7. Storage Recovery

Allowed recovery entry point:

- `run_storage_recovery`

Allowed recovery inputs and outputs:

- `StorageRecoveryInput`
- `StorageRecoveryMode`
- `StorageRecoveryOutcome`
- `StorageRecoveryError`
- `StorageLossyWalReplayFacts`
- `RecoverySnapshotInstallCallback`

Allowed recovery input/output operations:

- `StorageRecoveryInput::new`
- `StorageRecoveryOutcome::new`
- `StorageLossyWalReplayFacts::new`
- `RecoveryResult::empty`
- `RecoveryStats::total_operations`
- `RecoveryStats::total_transactions`
- `RecoveredState::empty`
- `RecoveryHealth::from_faults`
- `DegradationClass::worst`
- `RecoveryFault::class`

Allowed recovery facts and errors:

- `RecoveredState`
- `RecoveryHealth`
- `DegradationClass`
- `RecoveryFault`
- `RecoveryResult`
- `RecoveryStats`
- `CoordinatorPlanError`
- `CoordinatorRecoveryError`
- `WalReaderError`
- `ManifestError`
- `SnapshotReadError`

Allowed usage:

- engine open may call `run_storage_recovery` for primary and follower modes
- engine may provide a primitive snapshot-install callback
- engine may convert storage recovery errors into `RecoveryError` and
  `StrataError`
- engine may apply `RecoveredState` to the engine transaction coordinator
- engine may enforce strict, lossy, and degraded-recovery policy from raw
  storage facts

Rules:

- production engine should prefer `run_storage_recovery` over raw
  `RecoveryCoordinator` orchestration
- storage owns manifest preparation, snapshot loading mechanics, WAL replay,
  and segment recovery
- engine owns whether degraded recovery is accepted and how it is reported

### 8. Checkpoint, WAL Compaction, Manifest Sync, And Snapshot Pruning

Allowed checkpoint and compaction entry points:

- `run_storage_checkpoint`
- `compact_storage_wal`
- `sync_storage_manifest`
- `truncate_storage_wal_after_flush`
- `prune_storage_snapshots`

Allowed input/output/error types:

- `StorageCheckpointInput`
- `StorageCheckpointOutcome`
- `StorageCheckpointError`
- `StorageWalCompactionInput`
- `StorageWalCompactionOutcome`
- `StorageWalCompactionError`
- `StorageManifestSyncInput`
- `StorageManifestRuntimeError`
- `StorageFlushWalTruncationInput`
- `StorageFlushWalTruncationOutcome`
- `StorageFlushWalTruncationError`
- `StorageSnapshotRetention`
- `StorageSnapshotPruneError`

Allowed checkpoint data and snapshot DTOs:

- `CheckpointData`
- `KvSnapshotEntry`
- `EventSnapshotEntry`
- `BranchSnapshotEntry`
- `JsonSnapshotEntry`
- `VectorCollectionSnapshotEntry`
- `VectorSnapshotEntry`

Allowed usage:

- engine may collect primitive data and pass `CheckpointData` into storage
- engine may pass active WAL segment and database UUID into storage checkpoint
  and compaction helpers
- engine shutdown may sync the storage manifest through
  `sync_storage_manifest`
- engine may request snapshot pruning with engine-owned retention policy
  adapted into `StorageSnapshotRetention`
- engine may map storage checkpoint/compaction/prune errors into public
  database errors or warnings
- engine may use `StorageSnapshotRetention::new` to clamp product retention
  policy into storage's lower runtime input

Rules:

- engine owns `Database::checkpoint()` and `Database::compact()` public APIs
- engine owns primitive checkpoint materialization
- storage owns snapshot file creation, manifest watermark update, WAL
  compaction, truncation, and snapshot pruning mechanics
- raw `ManifestManager` mutation should not be used in production engine paths
  when a storage runtime helper exists

### 9. Snapshot Read, Decode, And Generic Install

Allowed snapshot reader/writer types:

- `SnapshotReader`
- `SnapshotWriter`
- `SnapshotSection`
- `LoadedSnapshot`
- `LoadedSection`
- `SnapshotInfo`
- `SnapshotReadError`

Allowed snapshot reader/writer operations:

- `SnapshotReader::new`
- `SnapshotReader::load`
- `SnapshotReader::codec`
- `SnapshotWriter::new`
- `SnapshotWriter::snapshots_dir`
- `SnapshotWriter::create_snapshot`
- `SnapshotWriter::cleanup_temp_files`
- `SnapshotWriter::temp_file_exists`
- `SnapshotSection::new`
- `LoadedSnapshot::snapshot_id`
- `LoadedSnapshot::watermark_txn`
- `LoadedSnapshot::created_at`
- `LoadedSnapshot::database_uuid`
- `LoadedSnapshot::find_section`
- `LoadedSnapshot::section_types`
- `LoadedSection::primitive_name`

Allowed snapshot format helpers:

- `snapshot_path`
- `list_snapshots`
- `find_latest_snapshot`
- `parse_snapshot_id`
- `primitive_tags`
- `SnapshotSerializer`
- `PrimitiveSerializeError`
- `SnapshotWatermark`
- `CheckpointInfo`

Allowed snapshot serializer operations:

- `SnapshotSerializer::new`
- `SnapshotSerializer::canonical_primitive_section`
- `SnapshotSerializer::codec_id`
- `SnapshotSerializer::serialize_kv`
- `SnapshotSerializer::deserialize_kv`
- `SnapshotSerializer::serialize_events`
- `SnapshotSerializer::deserialize_events`
- `SnapshotSerializer::serialize_branches`
- `SnapshotSerializer::deserialize_branches`
- `SnapshotSerializer::serialize_json`
- `SnapshotSerializer::deserialize_json`
- `SnapshotSerializer::serialize_vectors`
- `SnapshotSerializer::deserialize_vectors`

Allowed decoded install entry point:

- `install_decoded_snapshot_rows`

Allowed decoded install types:

- `StorageDecodedSnapshotInstallPlan`
- `StorageDecodedSnapshotInstallGroup`
- `StorageDecodedSnapshotInstallInput`
- `StorageDecodedSnapshotInstallStats`
- `StorageDecodedSnapshotInstallError`

Allowed decoded install operations:

- `StorageDecodedSnapshotInstallPlan::new`
- `StorageDecodedSnapshotInstallPlan::is_empty`
- `StorageDecodedSnapshotInstallGroup::new`

Allowed usage:

- engine recovery may receive `LoadedSnapshot` from storage recovery and decode
  primitive sections
- engine may use storage-owned primitive snapshot serializers to decode and
  encode durable snapshot section payloads
- engine may build a generic decoded-row install plan and hand it to storage
- storage may install generic decoded rows into `SegmentedStore`

Rules:

- engine owns primitive section interpretation
- storage owns snapshot byte format and generic decoded-row install mechanics
- storage decoded install must stay primitive-agnostic except for opaque
  storage-family routing through `TypeTag`

### 10. Retention, Reclaim, And Recovery Health

Allowed operations:

- `SegmentedStore::retention_snapshot`
- `SegmentedStore::gc_orphan_segments`
- `SegmentedStore::purge_all_quarantines`
- `SegmentedStore::last_recovery_health`
- `SegmentedStore::reset_recovery_health`
- `StorageBranchRetention::empty`

Allowed result types:

- `StorageBranchRetention`
- `StorageInheritedLayerInfo`
- `PurgeReport`
- `RecoveryHealth`
- `RecoveryFault`
- `DegradationClass`

Allowed usage:

- engine retention reports may join storage retention facts with engine branch
  lifecycle facts
- engine lifecycle may trigger orphan/quarantine GC
- engine may expose recovery health and reclaim refusal as engine-owned
  operator reports
- engine may reset recovery health only through documented recovery-health
  policy

Rules:

- storage owns raw byte attribution for segment directories, inherited layers,
  and quarantine inventory
- engine owns lifecycle attribution and user-facing retention reports
- reclaim must remain gated by storage recovery health

### 11. Metrics, Health, And Runtime Facts

Allowed storage facts:

- `StorageMemoryStats`
- `BranchMemoryStats`
- `MemoryPressure`
- `PressureLevel`
- `PublishHealth`
- `RecoveryHealth`
- `WalCounters`
- `WalDiskUsage`

Allowed operations:

- `block_cache::global_capacity`
- `SegmentedStore::memory_stats`
- `SegmentedStore::shard_stats_detailed`
- `SegmentedStore::publish_health`
- `SegmentedStore::latch_publish_health`
- `SegmentedStore::pressure_level`
- `SegmentedStore::total_memtable_bytes`
- `SegmentedStore::total_frozen_count`
- `SegmentedStore::total_segment_metadata_bytes`
- `SegmentedStore::total_tracked_bytes`
- `SegmentedStore::has_frozen_memtables`
- `SegmentedStore::branches_needing_flush`
- `SegmentedStore::has_frozen`
- `SegmentedStore::branch_frozen_count`
- `SegmentedStore::branch_segment_count`
- `SegmentedStore::max_flushed_commit`
- `SegmentedStore::segments_dir`
- `SegmentedStore::has_segments_dir`
- `SegmentedStore::l0_segment_count`
- `SegmentedStore::l1_segment_count`
- `SegmentedStore::level_segment_count`
- `SegmentedStore::level_bytes`
- `SegmentedStore::max_l0_segment_count`

Allowed usage:

- engine health reports may include storage memory, pressure, WAL, and publish
  health facts
- engine background maintenance may use flush/segment counters to decide when
  to rotate, flush, compact, or report pressure
- engine tests may assert specific low-level counters when characterizing
  storage behavior

Rules:

- storage metrics are raw facts
- engine owns health classification and public report wording
- metrics should not become hidden policy knobs in upper crates

### 12. Flush, Compaction, And Bulk-Load Mechanics

Allowed operations:

- `SegmentedStore::begin_bulk_load`
- `SegmentedStore::end_bulk_load`
- `SegmentedStore::is_bulk_loading`
- `SegmentedStore::rotate_memtable`
- `SegmentedStore::flush_oldest_frozen`
- `SegmentedStore::compact_branch`
- `SegmentedStore::pick_and_compact`
- `SegmentedStore::compact_l0_to_l1`

Allowed result types:

- `CompactionResult`
- `PickAndCompactResult`

Allowed usage:

- engine lifecycle/background maintenance may drive storage flush and compaction
- engine branch/materialization workflows may use bulk-load windows where
  storage explicitly supports them
- engine tests may force rotation/flush/compaction to characterize persistence
  and recovery behavior

Rules:

- storage owns flush and compaction mechanics
- engine owns when maintenance is scheduled and how failures affect database
  health

## Fault And Test Hooks

Fault and test hooks are not production engine contracts.

Allowed only in engine tests or fault-injection builds:

- `durability::__internal::inject_apply_failure_once`
- `durability::__internal::clear_apply_failure_injection`
- `durability::__internal::WalWriterEngineExt` fault-observation methods when
  compiling engine's WAL health tests
- block-cache `set_global_capacity`
- `WalConfig::for_testing`
- `SegmentedStore::*_for_test` accessors:
  - `write_buffer_size_for_test`
  - `max_branches_for_test`
  - `max_versions_per_key_for_test`
  - `max_immutable_memtables_for_test`
  - `compaction_rate_limit_for_test`

Rules:

- production engine code must not call storage `_for_test` accessors
- tests that mutate process-global block-cache capacity must serialize or avoid
  asserting isolation against concurrent opens
- new fault hooks should live behind `cfg(test)` or
  `feature = "fault-injection"` and should be named as hooks

Storage-local `pub(crate)` test hooks, including manifest publish/fsync failure
injection and install-pause hooks, are not part of this cross-crate contract.
If engine needs them, promote them through a documented fault-injection surface
instead of reaching around module visibility.

## Explicitly Not In Contract

The following storage surfaces are not engine consumption APIs unless a future
contract revision names them:

- raw `KVSegment` construction and segment block internals
- `SegmentBuilder` and `SplittingSegmentBuilder` outside storage tests or
  storage-owned compaction paths
- bloom filter internals
- memtable internals
- key encoding internals
- storage ref-registry internals
- quarantine internals beyond `retention_snapshot`, `gc_orphan_segments`, and
  `purge_all_quarantines`
- raw manifest mutation when a runtime helper exists
- raw WAL segment file mutation outside storage durability code
- raw format constants in production engine code except where the engine is
  decoding or validating storage-owned durability format in recovery or tests

## Current Transitional Notes

The current codebase still has storage exposure that should be tightened during
engine consolidation:

- `Database::storage()` exposes `Arc<SegmentedStore>` and is used by peer
  crates today. After graph/vector/search/executor storage bypasses are
  removed, this should be narrowed or documented as an engine-internal/testing
  escape hatch.
- Engine currently re-exports a small number of storage types. Each re-export
  should either become an engine-owned type or remain documented as an explicit
  public engine contract.
- Graph, vector, search, and executor currently import storage directly. Those
  imports are migration debt, not an extension of this contract.
- Engine tests may inspect storage format details. That is acceptable when the
  test is explicitly characterizing recovery, checkpoint, manifest, snapshot,
  WAL, or storage-runtime behavior.

## Guard Commands

Final upper-crate storage bypass guard:

```bash
rg -n "strata_storage::|use strata_storage|strata-storage" \
  crates/{graph,vector,search,executor,executor-legacy,intelligence,cli} \
  -g 'Cargo.toml' -g '*.rs'
```

At engine-consolidation closeout this should return no production matches.

Engine production use of storage test accessors:

```bash
rg -n "_for_test\\(" crates/engine/src --glob '*.rs' \
  | rg "write_buffer_size_for_test|max_branches_for_test|max_versions_per_key_for_test|max_immutable_memtables_for_test|compaction_rate_limit_for_test"
```

This should return only test-module matches.

Storage inverse dependency guard:

```bash
cargo tree -i strata-storage --workspace --edges normal --depth 2
```

At closeout, normal production dependents should be storage-specific tools/tests
and `strata-engine`; product/runtime crates above engine should not appear.

Manifest/runtime helper drift guard:

```bash
rg -n "ManifestManager::(create|load|persist|set_active_segment|set_snapshot_watermark)" \
  crates/engine/src --glob '*.rs'
```

Production matches should be justified. Engine should prefer
`run_storage_checkpoint`, `compact_storage_wal`, `sync_storage_manifest`,
`run_storage_recovery`, and other storage runtime helpers where those helpers
exist.

## Updating This Contract

When engine needs a new storage operation:

1. Confirm the operation is substrate mechanics, not engine semantics.
2. Add the operation to the right section of this document.
3. State whether the operation is production, test-only, or fault-injection.
4. Add or update a guard if the operation widens the boundary.
5. Add characterization tests if the operation affects recovery, checkpoint,
   WAL, branch COW, retention, or transaction correctness.

When storage-next changes implementation details, this document should be used
as the migration checklist for engine compatibility. Storage-next may replace
the physical implementation, but it must intentionally support or retire each
operation listed here.
