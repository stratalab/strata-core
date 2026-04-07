# Command Reference

The `Command` enum is the instruction set of StrataDB. Every operation that can be performed on the database is represented as a variant. Commands are self-contained, serializable, and typed.

This reference is primarily for SDK builders and contributors. Most users should use the typed `Strata` API instead.

## Command Categories

| Category | Count | Description |
|----------|-------|-------------|
| KV | 5 | Key-value operations |
| JSON | 5 | JSON document operations |
| Event | 4 | Event log operations |
| State | 5 | State cell operations |
| Vector | 9 | Vector store operations |
| Branch | 17 | Branch lifecycle, fork/diff/merge, cherry-pick, revert, tags, notes |
| Space | 4 | Space management operations |
| Transaction | 5 | Transaction control |
| Retention | 3 | Retention policy |
| Database | 5 | Database-level operations |
| Bundle | 3 | Branch export/import |
| Intelligence | 2 | Cross-primitive search and model config |

## KV Commands

| Command | Fields | Output |
|---------|--------|--------|
| `KvPut` | `branch?`, `space?`, `key`, `value` | `Version(u64)` |
| `KvGet` | `branch?`, `space?`, `key`, `as_of?` | `Maybe(Option<Value>)` |
| `KvDelete` | `branch?`, `space?`, `key` | `Bool(existed)` |
| `KvList` | `branch?`, `space?`, `prefix?`, `as_of?` | `Keys(Vec<String>)` |
| `KvGetv` | `branch?`, `space?`, `key`, `as_of?` | `VersionHistory(Option<Vec<VersionedValue>>)` |

## JSON Commands

| Command | Fields | Output |
|---------|--------|--------|
| `JsonSet` | `branch?`, `space?`, `key`, `path`, `value` | `Version(u64)` |
| `JsonGet` | `branch?`, `space?`, `key`, `path`, `as_of?` | `Maybe(Option<Value>)` |
| `JsonDelete` | `branch?`, `space?`, `key`, `path` | `Uint(count)` |
| `JsonGetv` | `branch?`, `space?`, `key`, `as_of?` | `VersionHistory(Option<Vec<VersionedValue>>)` |
| `JsonList` | `branch?`, `space?`, `prefix?`, `cursor?`, `limit`, `as_of?` | `JsonListResult { keys, cursor }` |

## Event Commands

| Command | Fields | Output |
|---------|--------|--------|
| `EventAppend` | `branch?`, `space?`, `event_type`, `payload` | `Version(u64)` |
| `EventGet` | `branch?`, `space?`, `sequence`, `as_of?` | `MaybeVersioned(Option<VersionedValue>)` |
| `EventGetByType` | `branch?`, `space?`, `event_type`, `as_of?` | `VersionedValues(Vec<VersionedValue>)` |
| `EventLen` | `branch?`, `space?` | `Uint(count)` |

## State Commands

| Command | Fields | Output |
|---------|--------|--------|
| `StateSet` | `branch?`, `space?`, `cell`, `value` | `Version(u64)` |
| `StateGet` | `branch?`, `space?`, `cell`, `as_of?` | `Maybe(Option<Value>)` |
| `StateCas` | `branch?`, `space?`, `cell`, `expected_counter?`, `value` | `MaybeVersion(Option<u64>)` |
| `StateInit` | `branch?`, `space?`, `cell`, `value` | `Version(u64)` |
| `StateGetv` | `branch?`, `space?`, `cell`, `as_of?` | `VersionHistory(Option<Vec<VersionedValue>>)` |
| `StateList` | `branch?`, `space?`, `prefix?`, `as_of?` | `Keys(Vec<String>)` |

## Vector Commands

| Command | Fields | Output |
|---------|--------|--------|
| `VectorCreateCollection` | `branch?`, `space?`, `collection`, `dimension`, `metric` | `Version(u64)` |
| `VectorDeleteCollection` | `branch?`, `space?`, `collection` | `Bool(existed)` |
| `VectorListCollections` | `branch?`, `space?` | `VectorCollectionList(Vec<CollectionInfo>)` |
| `VectorCollectionStats` | `branch?`, `space?`, `collection` | `VectorCollectionList(Vec<CollectionInfo>)` |
| `VectorUpsert` | `branch?`, `space?`, `collection`, `key`, `vector`, `metadata?` | `Version(u64)` |
| `VectorBatchUpsert` | `branch?`, `space?`, `collection`, `entries` | `Versions(Vec<u64>)` |
| `VectorGet` | `branch?`, `space?`, `collection`, `key`, `as_of?` | `VectorData(Option<VersionedVectorData>)` |
| `VectorDelete` | `branch?`, `space?`, `collection`, `key` | `Bool(existed)` |
| `VectorSearch` | `branch?`, `space?`, `collection`, `query`, `k`, `filter?`, `metric?`, `as_of?` | `VectorMatches(Vec<VectorMatch>)` |

## Branch Commands

For the full semantics of each cross-branch operation (per-primitive merge rules, error cases, return field schemas), see the [Branching API Reference](branching-api.md).

### Lifecycle

| Command | Fields | Output |
|---------|--------|--------|
| `BranchCreate` | `branch_id?`, `metadata?` | `BranchWithVersion { info, version }` |
| `BranchGet` | `branch` | `BranchInfoVersioned(info)` or `Maybe(None)` |
| `BranchList` | `state?`, `limit?`, `offset?` | `BranchInfoList(Vec<VersionedBranchInfo>)` |
| `BranchExists` | `branch` | `Bool(exists)` |
| `BranchDelete` | `branch` | `Unit` |

### Cross-branch operations

| Command | Fields | Output |
|---------|--------|--------|
| `BranchFork` | `source`, `destination`, `message?`, `creator?` | `BranchForked(ForkInfo)` |
| `BranchDiff` | `branch_a`, `branch_b`, `filter_primitives?`, `filter_spaces?`, `as_of?` | `BranchDiff(BranchDiffResult)` |
| `BranchDiffThreeWay` | `branch_a`, `branch_b` | `ThreeWayDiff(ThreeWayDiffResult)` |
| `BranchMergeBase` | `branch_a`, `branch_b` | `MergeBaseInfo(Option<MergeBaseInfo>)` |
| `BranchMerge` | `source`, `target`, `strategy`, `message?`, `creator?` | `BranchMerged(MergeInfo)` |

### Selective application

| Command | Fields | Output |
|---------|--------|--------|
| `BranchCherryPick` | `source`, `target`, `keys?`, `filter_spaces?`, `filter_keys?`, `filter_primitives?` | `BranchCherryPicked(CherryPickInfo)` |
| `BranchRevert` | `branch`, `from_version`, `to_version` | `BranchReverted(RevertInfo)` |

### Tags and notes

| Command | Fields | Output |
|---------|--------|--------|
| `TagCreate` | `branch`, `name`, `version?`, `message?`, `creator?` | `TagInfo(TagInfo)` |
| `TagDelete` | `branch`, `name` | `Bool(existed)` |
| `TagList` | `branch` | `TagList(Vec<TagInfo>)` |
| `TagResolve` | `branch`, `name` | `TagInfo(Option<TagInfo>)` |
| `NoteAdd` | `branch`, `version`, `message`, `author?`, `metadata?` | `NoteInfo(NoteInfo)` |
| `NoteGet` | `branch`, `version?` | `NoteList(Vec<NoteInfo>)` |
| `NoteDelete` | `branch`, `version` | `Bool(existed)` |

> **Note**: `materialize_branch` is engine-only. It is not exposed via the Command enum or the CLI. Call `strata_engine::branch_ops::materialize_branch` directly when you need it.

## Space Commands

| Command | Fields | Output |
|---------|--------|--------|
| `SpaceList` | `branch?` | `SpaceList(Vec<String>)` |
| `SpaceCreate` | `branch?`, `space` | `Unit` |
| `SpaceDelete` | `branch?`, `space`, `force` | `Unit` |
| `SpaceExists` | `branch?`, `space` | `Bool(exists)` |

## Transaction Commands

| Command | Fields | Output |
|---------|--------|--------|
| `TxnBegin` | `branch?`, `options?` | `TxnBegun` |
| `TxnCommit` | (none) | `TxnCommitted { version }` |
| `TxnRollback` | (none) | `TxnAborted` |
| `TxnInfo` | (none) | `TxnInfo(Option<TransactionInfo>)` |
| `TxnIsActive` | (none) | `Bool(active)` |

## Database Commands

| Command | Fields | Output |
|---------|--------|--------|
| `Ping` | (none) | `Pong { version }` |
| `Info` | (none) | `DatabaseInfo(info)` |
| `Flush` | (none) | `Unit` |
| `Compact` | (none) | `Unit` |
| `TimeRange` | `branch?` | `TimeRange { oldest_ts, latest_ts }` |

## Bundle Commands

| Command | Fields | Output |
|---------|--------|--------|
| `BranchExport` | `branch_id`, `path` | `BranchExported(result)` |
| `BranchImport` | `path` | `BranchImported(result)` |
| `BranchBundleValidate` | `path` | `BundleValidated(result)` |

## Retention Commands

| Command | Fields | Output |
|---------|--------|--------|
| `RetentionApply` | `branch?` | (retention result) |
| `RetentionStats` | `branch?` | (retention stats) |
| `RetentionPreview` | `branch?` | (retention preview) |

## Intelligence Commands

| Command | Fields | Output |
|---------|--------|--------|
| `Search` | `branch?`, `space?`, `search: SearchQuery` | `SearchResults(Vec<SearchResultHit>)` |
| `ConfigureModel` | `endpoint`, `model`, `api_key?`, `timeout_ms?` | `Unit` |

### SearchQuery Object

The `search` field is a structured `SearchQuery` object:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | string | *required* | Natural-language or keyword query |
| `k` | integer? | 10 | Number of results to return |
| `primitives` | string[]? | all | Restrict to specific primitives |
| `time_range` | TimeRangeInput? | none | Filter results to a time window |
| `mode` | string? | `"hybrid"` | Search mode: `"keyword"` or `"hybrid"` |
| `expand` | boolean? | auto | Enable query expansion (requires model) |
| `rerank` | boolean? | auto | Enable result reranking (requires model) |

### TimeRangeInput Object

| Field | Type | Description |
|-------|------|-------------|
| `start` | string | Range start (inclusive), ISO 8601 datetime |
| `end` | string | Range end (inclusive), ISO 8601 datetime |

### ConfigureModel Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | *required* | OpenAI-compatible API endpoint URL |
| `model` | string | *required* | Model name (e.g. `"qwen3:1.7b"`) |
| `api_key` | string? | none | Optional bearer token |
| `timeout_ms` | integer? | 5000 | Request timeout in milliseconds |

The model configuration is persisted to `strata.toml` and survives database restarts.

## Branch Field Convention

Data-scoped commands have an optional `branch` field. When `None`, it defaults to the "default" branch. Branch lifecycle commands (BranchGet, BranchDelete, etc.) have a required `branch` field.

## Space Field Convention

Data-scoped commands have an optional `space` field. When `None`, it defaults to the current space on the handle (initially `"default"`). Space lifecycle commands (`SpaceList`, `SpaceCreate`, `SpaceDelete`, `SpaceExists`) do not have a `space` field — they operate on spaces within the specified branch.

## `as_of` Field Convention

Read commands accept an optional `as_of` field — a timestamp in microseconds since epoch. When present, the command returns the state as it existed at that time instead of the current state. When absent (or `null`), the command behaves normally and returns the current state.

The `as_of` field uses `#[serde(default, skip_serializing_if = "Option::is_none")]` for full backward compatibility — existing clients that don't send `as_of` continue to work unchanged.

## Serialization

All commands implement `Serialize` and `Deserialize` with `deny_unknown_fields`. The format uses serde's externally tagged representation:

```json
{"KvPut": {"key": "foo", "value": {"Int": 42}}}
{"KvGet": {"key": "foo"}}
{"KvGet": {"key": "foo", "as_of": 1700002000}}
{"Search": {"search": {"query": "error handling", "k": 10}}}
{"Search": {"search": {"query": "errors", "time_range": {"start": "2026-02-07T00:00:00Z", "end": "2026-02-09T00:00:00Z"}}}}
{"TimeRange": {"branch": "default"}}
{"ConfigureModel": {"endpoint": "http://localhost:11434/v1", "model": "qwen3:1.7b"}}
{"ConfigureModel": {"endpoint": "http://localhost:11434/v1", "model": "qwen3:1.7b", "api_key": "sk-...", "timeout_ms": 10000}}
{"TxnCommit": null}
```
