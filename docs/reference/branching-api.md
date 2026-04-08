# Branching API Reference

Complete reference for every public branching operation in Strata. For *what* each operation does semantically, see [Merge Semantics](../guides/merge-semantics.md). For *when* to use which operation, see [Branching Strategies](../guides/branching-strategies.md). For the conceptual overview, see [Concepts: Branches](../concepts/branches.md).

## Conventions

Every operation appears in three forms where applicable:

1. **CLI** — `strata branch <subcommand>` from the shell or REPL.
2. **Rust SDK** — `db.branches().<method>(...)` on `strata_executor::Strata`.
3. **Engine API** — `branch_ops::<function>(...)` on `Arc<Database>` (lower-level, used by other crates).

The CLI exposes a curated subset (`fork`, `diff`, `merge`, plus the lifecycle commands and bundle export/import). Cherry-pick, materialize, revert, tag, note, three-way diff, and merge-base are SDK-only as of this writing.

## Index

**Lifecycle**
- [Create](#create) · [Get](#get) · [List](#list) · [Exists](#exists) · [Delete](#delete)

**Cross-branch operations**
- [Fork](#fork) · [Diff](#diff) · [Diff (three-way)](#diff-three-way) · [Merge base](#merge-base) · [Merge](#merge)

**Selective application**
- [Cherry-pick](#cherry-pick) · [Cherry-pick (filtered)](#cherry-pick-filtered)

**Maintenance**
- [Materialize](#materialize) · [Revert](#revert)

**Annotation**
- [Tag](#tags) · [Note](#notes)

**Portability**
- [Export](#export) · [Import](#import) · [Validate bundle](#validate)

**Type appendix**
- [Result types](#result-types) · [`MergeStrategy`](#mergestrategy) · [`ConflictEntry`](#conflictentry) · [`ThreeWayChange`](#threewaychange) · [`CherryPickFilter`](#cherrypickfilter) · [`DiffOptions`](#diffoptions)

---

## Lifecycle

### Create

Register a new empty branch.

```bash
strata> branch create my-branch
```

```rust
db.branches().create("my-branch")?;
```

| Parameter | Type | Notes |
|---|---|---|
| `name` | `&str` | Branch name. Must be unique. Cannot contain `:`. |

**Returns** unit. `default` is the only branch that exists at startup.

**Errors**
- `BranchExists` if a branch with that name already exists.

### Get

Get metadata for a branch.

```bash
strata> branch info my-branch
```

```rust
let branch = db.branches().get("my-branch")?;
```

**Returns** `Option<BranchInfo>` with `id`, `status`, `created_at`. `None` if the branch doesn't exist.

### List

List all branch names.

```bash
strata> branch list
```

```rust
let names: Vec<String> = db.branches().list()?;
```

**Returns** `Vec<String>` containing every registered branch name. Always includes `"default"`.

### Exists

Check whether a branch exists.

```bash
strata> branch exists my-branch
```

```rust
if db.branches().exists("my-branch")? { ... }
```

**Returns** `bool`.

### Delete

Delete a branch and all its data.

```bash
strata> branch del my-branch
```

```rust
db.branches().delete("my-branch")?;
```

**Errors**
- Cannot delete the current branch (switch first)
- Cannot delete `"default"`
- Cannot delete a branch that other branches still inherit from via fork chain (delete the children first or materialize them)

---

## Cross-branch operations

### Fork

Create a copy-on-write copy of a branch. The new branch shares storage with the source via inherited segment layers — no data is copied.

```bash
strata> branch fork experiment-1
```

```rust
let info = db.branches().fork("source", "destination")?;
let info = db.branches().fork_with_options(
    "source",
    "destination",
    Some("trying new pricing".into()),  // message
    Some("alice".into()),                // creator
)?;
```

```rust
// Engine-level
use strata_engine::branch_ops;
let info = branch_ops::fork_branch(&db, "source", "destination")?;
```

**Returns** `ForkInfo`:

| Field | Type | Notes |
|---|---|---|
| `source` | `String` | Source branch name |
| `destination` | `String` | Destination branch name |
| `keys_copied` | `u64` | Always `0` for COW fork |
| `spaces_copied` | `u64` | Number of spaces registered on destination |
| `fork_version` | `Option<u64>` | MVCC version at the fork point |

**Errors**
- Source branch does not exist
- Destination branch already exists
- Destination name validation fails

**Cost**: O(1). Storage cost: a single branch metadata entry plus per-space registration. No data is copied. See [Branching Strategies §Performance and cost model](../guides/branching-strategies.md#performance-and-cost-model).

### Diff

Compare two branches and return their per-space differences.

```bash
strata> branch diff branch-a branch-b
```

```rust
let diff = db.branches().diff("branch-a", "branch-b")?;

// With filtering and time-travel:
use strata_engine::branch_ops::DiffOptions;
let diff = db.branches().diff_with_options("branch-a", "branch-b", DiffOptions {
    filter: None,
    as_of: Some(1_700_000_000_000_000),
})?;
```

**Returns** `BranchDiffResult`:

| Field | Type | Notes |
|---|---|---|
| `branch_a` | `String` | First branch name |
| `branch_b` | `String` | Second branch name |
| `spaces` | `Vec<SpaceDiff>` | Per-space diffs |
| `summary` | `DiffSummary` | Aggregate counts |

`SpaceDiff` per space:

| Field | Type | Notes |
|---|---|---|
| `space` | `String` | Space name |
| `added` | `Vec<BranchDiffEntry>` | In B but not A |
| `removed` | `Vec<BranchDiffEntry>` | In A but not B |
| `modified` | `Vec<BranchDiffEntry>` | In both, different values |

`BranchDiffEntry` per key:

| Field | Type | Notes |
|---|---|---|
| `key` | `String` | UTF-8 user key (hex-encoded for binary keys) |
| `raw_key` | `Vec<u8>` | Binary user key bytes |
| `primitive` | `PrimitiveType` | Which primitive |
| `type_tag` | `TypeTag` | Storage-level type tag |
| `space` | `String` | Space name |
| `value_a` | `Option<Value>` | Value in branch A (`None` if absent) |
| `value_b` | `Option<Value>` | Value in branch B (`None` if absent) |

`DiffSummary`:

| Field | Type |
|---|---|
| `total_added` | `usize` |
| `total_removed` | `usize` |
| `total_modified` | `usize` |
| `spaces_only_in_a` | `Vec<String>` |
| `spaces_only_in_b` | `Vec<String>` |

### Diff (three-way)

Compute a three-way diff against the computed merge base. Returns the raw 14-case classification per key without applying any merge strategy. Useful for previewing what a merge would do.

```rust
let result = db.branches().diff_three_way("source", "target")?;
for entry in &result.entries {
    println!("{}: {:?}", entry.key, entry.change);
}
```

**Returns** `ThreeWayDiffResult`:

| Field | Type | Notes |
|---|---|---|
| `source` | `String` | Source branch name |
| `target` | `String` | Target branch name |
| `merge_base_branch` | `String` | Branch name at the merge base |
| `merge_base_version` | `u64` | Version at the merge base |
| `entries` | `Vec<ThreeWayDiffEntry>` | Differing entries (Unchanged is filtered out) |

`ThreeWayDiffEntry`:

| Field | Type |
|---|---|
| `key` | `String` |
| `raw_key` | `Vec<u8>` |
| `space` | `String` |
| `primitive` | `PrimitiveType` |
| `type_tag` | `TypeTag` |
| `change` | [`ThreeWayChange`](#threewaychange) |

### Merge base

Compute the merge base (common ancestor) of two branches.

```rust
let base: Option<MergeBaseInfo> = db.branches().merge_base("source", "target")?;
```

**Returns** `Option<MergeBaseInfo>`:

| Field | Type | Notes |
|---|---|---|
| `branch` | `String` | Branch name where the ancestor lives |
| `version` | `u64` | MVCC version at the ancestor |

`None` if the branches have no fork or merge relationship (they are unrelated).

The merge base is computed by walking the **branch DAG**, which is stored as a graph called `_branch_dag` on the `_system_` branch. The DAG records every fork and merge event in the database with their MVCC versions. For a freshly forked pair, the merge base is the fork point. For branches that have been merged before, the merge base is the version of the most recent merge — this is what makes repeated merges of the same source/target pair fast and correct (only the changes since the last merge are reconsidered, not the full history). See [Concepts: The `_system_` branch](../concepts/branches.md#the-_system_-branch).

### Merge

Merge data from `source` into `target` using a three-way merge over their merge base.

```bash
strata> branch merge source --strategy lww
strata> branch merge source --strategy strict
```

```rust
use strata_engine::MergeStrategy;
let info = db.branches().merge("source", "target", MergeStrategy::LastWriterWins)?;
let info = db.branches().merge_with_options(
    "source",
    "target",
    MergeStrategy::Strict,
    Some("merging feature-x".into()),
    Some("alice".into()),
)?;
```

| Parameter | Type | Notes |
|---|---|---|
| `source` | `&str` | Source branch name |
| `target` | `&str` | Target branch name (mutated by the merge) |
| `strategy` | [`MergeStrategy`](#mergestrategy) | `Strict` or `LastWriterWins` |

**Returns** `MergeInfo`:

| Field | Type | Notes |
|---|---|---|
| `source` | `String` | Source branch name |
| `target` | `String` | Target branch name |
| `keys_applied` | `u64` | Number of put actions applied |
| `keys_deleted` | `u64` | Number of delete actions applied |
| `conflicts` | `Vec<`[`ConflictEntry`](#conflictentry)`>` | Cell-level conflicts (empty for clean merges; populated even on success under LWW) |
| `spaces_merged` | `u64` | Number of distinct spaces touched |
| `merge_version` | `Option<u64>` | MVCC version of the merge transaction |

**Errors**
- `Cannot merge: no fork or merge relationship found` — branches have no merge base
- `Merge conflict: N keys differ` — Strict mode + conflicts
- `event merge unsupported: divergent appends to stream X` — both sides appended to the same event stream
- `merge unsupported: vector collection X has incompatible dimensions` — vector dimension mismatch
- `dangling edge reference: edge X → Y but Y missing` — graph referential integrity
- `merge target concurrently modified` — OCC race; retry

See [Merge Semantics](../guides/merge-semantics.md) for the full per-primitive behavior and the error reference.

---

## Selective application

### Cherry-pick

Cherry-pick specific keys from source to target. Lower-level than `cherry_pick_filtered` — you specify exact `(space, key)` pairs and the operation just copies the current source value.

```rust
let info = db.branches().cherry_pick(
    "source",
    "target",
    &[
        ("default".to_string(), "key-1".to_string()),
        ("products".to_string(), "key-2".to_string()),
    ],
)?;
```

**Returns** `CherryPickInfo` — see the [filtered variant below](#cherry-pick-filtered) for the field schema.

### Cherry-pick (filtered)

Cherry-pick using a [`CherryPickFilter`](#cherrypickfilter) over the source-vs-target three-way diff. Goes through the same per-handler dispatch as `merge`, so JSON path-level merge, vector HNSW rebuild, and graph referential integrity all still apply to the picked subset.

```rust
use strata_engine::branch_ops::CherryPickFilter;
use strata_core::PrimitiveType;

let filter = CherryPickFilter {
    spaces: Some(vec!["products".to_string()]),
    primitives: Some(vec![PrimitiveType::Json]),
    keys: None,
};
let info = db.branches().cherry_pick_filtered("source", "target", filter)?;
```

**Returns** `CherryPickInfo`:

| Field | Type |
|---|---|
| `source` | `String` |
| `target` | `String` |
| `keys_applied` | `u64` |
| `keys_deleted` | `u64` |
| `cherry_pick_version` | `Option<u64>` |

**Errors**
- All `merge_branches` errors (cherry-pick goes through the same handlers)
- `Graph cherry-pick atomicity violation: filter would split cell ...` — the `keys` filter would split a graph cell across the filter boundary; widen the filter or remove `keys`

**Storage**

Both cherry-pick variants record a `cherry_pick` event in the branch DAG (`_branch_dag` graph on the `_system_` branch), connected via `cherry_pick_source` / `cherry_pick_target` edges. Distinct edge types let DAG walks distinguish a partial cherry-pick from a full merge of the same source/target pair.

---

## Maintenance

### Materialize

Collapse a branch's inherited COW layers into own segments. Used to flatten deep fork chains so reads stop walking inherited layers.

Materialize is engine-level only — it is not exposed on the `Branches` API or via the CLI. Call it through `strata_engine::branch_ops::materialize_branch`:

```rust
use strata_engine::branch_ops;

let info = branch_ops::materialize_branch(&engine_db, "leaf")?;
```

**Returns** `MaterializeInfo`:

| Field | Type | Notes |
|---|---|---|
| `branch` | `String` | Branch name |
| `entries_materialized` | `u64` | Number of KV entries copied |
| `segments_created` | `usize` | Number of new own segments |
| `layers_collapsed` | `usize` | Number of inherited layers collapsed |

**Errors**
- Branch does not exist
- Database is ephemeral (in-memory only — has no segment files)

**Cost**: O(data) — actually copies the inherited data into own segments.

### Revert

Revert a version range on a branch. For each key modified in `[from_version, to_version]`, restores its value to what it was at `from_version - 1`. Only reverts keys whose current value matches the state at `to_version` — keys modified after `to_version` are left untouched (preserving subsequent work).

```rust
let info = db.branches().revert("main", 100, 150)?;
```

**Returns** `RevertInfo`:

| Field | Type |
|---|---|
| `branch` | `String` |
| `from_version` | `u64` |
| `to_version` | `u64` |
| `keys_reverted` | `u64` |
| `revert_version` | `Option<u64>` |

**Errors**
- `from_version` is 0
- `from_version > to_version`
- `to_version` exceeds the current database version
- Branch does not exist

**Storage**

Every successful revert is recorded in the branch DAG (`_branch_dag` graph on the `_system_` branch) as a `revert` event node attached to the branch via a `reverted` edge. The event carries `from_version`, `to_version`, `revert_version`, `keys_reverted`, and any optional `message` / `creator` metadata. This means revert history is queryable through the same DAG walk used for fork/merge lineage.

---

## Annotation

### Tags

Bookmark a branch at a specific MVCC version. Tags are immutable once created (delete and recreate if you need to move one).

```rust
// Tag the current version
let tag = db.branches().create_tag("main", "v1.0", Some("first GA release"))?;

// Tag a specific historical version
let tag = db.branches().create_tag_at_version(
    "main",
    "v0.9",
    100,                                    // version
    Some("RC build".into()),                // message
    Some("release-bot".into()),             // creator
)?;

// Look up
let tag = db.branches().resolve_tag("main", "v1.0")?;

// List
let tags: Vec<TagInfo> = db.branches().list_tags("main")?;

// Delete
let existed = db.branches().delete_tag("main", "v0.9")?;
```

**Returns** `TagInfo`:

| Field | Type | Notes |
|---|---|---|
| `name` | `String` | Tag name (cannot contain `:`) |
| `branch` | `String` | Branch name (cannot contain `:`) |
| `version` | `u64` | MVCC version this tag points to |
| `message` | `Option<String>` | Human-readable message |
| `timestamp` | `u64` | Microseconds since epoch when the tag was created |
| `creator` | `Option<String>` | Optional creator identifier |

**Storage**: tags are stored as KV entries on the `_system_` branch (the engine-internal branch that's filtered out of `branch list` — see [Concepts: The `_system_` branch](../concepts/branches.md#the-_system_-branch)) under the key pattern `tag:{branch}:{name}`. Deleting a branch does **not** delete its tags — tags become orphaned references and need to be deleted explicitly via `delete_tag`.

### Notes

Attach a version-annotated message to a branch. Notes are queryable per-version and ordered by version.

```rust
// Add a note at the current version
let note = db.branches().add_note(
    "main",
    db.current_version(),
    "Approved by SOC2 review",
    Some("alice"),
    None,                                   // optional structured metadata
)?;

// Get all notes for a branch
let notes: Vec<NoteInfo> = db.branches().get_notes("main", None)?;

// Get the note at a specific version
let notes: Vec<NoteInfo> = db.branches().get_notes("main", Some(123))?;

// Delete
let existed = db.branches().delete_note("main", 123)?;
```

**Returns** `NoteInfo`:

| Field | Type | Notes |
|---|---|---|
| `branch` | `String` | Branch name |
| `version` | `u64` | MVCC version this note is attached to |
| `message` | `String` | Human-readable message |
| `author` | `Option<String>` | Optional author identifier |
| `timestamp` | `u64` | Microseconds since epoch when the note was added |
| `metadata` | `Option<Value>` | Optional structured metadata payload |

**Storage**: notes are stored as KV entries on the `_system_` branch (see [Concepts: The `_system_` branch](../concepts/branches.md#the-_system_-branch)) under the key pattern `note:{branch}:{version}`. Like tags, notes survive branch deletion as orphaned references and must be deleted explicitly.

---

## Portability

### Export

Export a branch to a `.branchbundle.tar.zst` archive. The bundle contains the branch metadata plus per-version transaction payloads, suitable for backup or transfer to another Strata instance.

```bash
strata> branch export my-branch /tmp/my-branch.branchbundle.tar.zst
```

```rust
use strata_engine::bundle;
use std::path::Path;

let info = bundle::export_branch(&db, "my-branch", Path::new("/tmp/my-branch.branchbundle.tar.zst"))?;
println!("Exported {} entries, {} bytes", info.entry_count, info.bundle_size);
```

**Returns** `ExportInfo`:

| Field | Type |
|---|---|
| `branch_id` | `String` |
| `path` | `PathBuf` |
| `entry_count` | `u64` |
| `bundle_size` | `u64` |

### Import

Import a branch from a `.branchbundle.tar.zst` archive into the current database.

```bash
strata> branch import /tmp/my-branch.branchbundle.tar.zst
```

```rust
use strata_engine::bundle;
let info = bundle::import_branch(&db, Path::new("/tmp/my-branch.branchbundle.tar.zst"))?;
```

**Returns** `ImportInfo`:

| Field | Type |
|---|---|
| `branch_id` | `String` |
| `transactions_applied` | `u64` |
| `keys_written` | `u64` |

### Validate

Validate a bundle file without importing.

```bash
strata> branch validate /tmp/my-branch.branchbundle.tar.zst
```

**Returns** `BundleInfo`:

| Field | Type |
|---|---|
| `branch_id` | `String` |
| `format_version` | `u32` |
| `entry_count` | `u64` |
| `checksums_valid` | `bool` |

See [Branch Bundles](../guides/branch-bundles.md) for the full bundle format spec.

---

## Type appendix

### Result types

All result structs above (`ForkInfo`, `MergeInfo`, `BranchDiffResult`, `CherryPickInfo`, `MaterializeInfo`, `RevertInfo`, `TagInfo`, `NoteInfo`, `ExportInfo`, `ImportInfo`, `BundleInfo`) are `Debug + Clone + PartialEq` and serialize to/from JSON via `serde`. They are stable across patch releases.

### `MergeStrategy`

```rust
pub enum MergeStrategy {
    /// Auto-resolve conflicts by taking source's value. The merge always
    /// succeeds. Conflicts are reported in `MergeInfo.conflicts` for
    /// inspection.
    LastWriterWins,
    /// Fail the merge if any conflict is detected. Target is unchanged.
    Strict,
}
```

Some conflicts are *always* fatal regardless of strategy: event hash-chain divergence, vector dimension mismatch, graph referential integrity. See [Merge Semantics](../guides/merge-semantics.md) for the per-primitive rules.

### `ConflictEntry`

```rust
pub struct ConflictEntry {
    /// User key (UTF-8 or hex-encoded for binary keys). For JSON
    /// path-level conflicts, formatted as `{doc_id}@{path.to.field}`.
    pub key: String,
    /// Primitive type
    pub primitive: PrimitiveType,
    /// Space name
    pub space: String,
    /// Value in source (None if source deleted the key)
    pub source_value: Option<Value>,
    /// Value in target (None if target deleted the key)
    pub target_value: Option<Value>,
}
```

### `ThreeWayChange`

The 14-case decision matrix from `diff_three_way`. One variant per case:

```rust
pub enum ThreeWayChange {
    Unchanged,                      // a == s == t  (filtered out of results)
    SourceChanged { value: Value }, // s ≠ a, t == a
    TargetChanged,                  // t ≠ a, s == a
    BothChangedSame,                // s == t ≠ a
    Conflict {                      // s ≠ a, t ≠ a, s ≠ t
        source_value: Value,
        target_value: Value,
    },
    SourceAdded { value: Value },   // a == None, s present, t None
    TargetAdded,                    // a == None, s None, t present
    BothAddedSame,                  // a == None, s == t
    BothAddedDifferent {            // a == None, s ≠ t
        source_value: Value,
        target_value: Value,
    },
    SourceDeleted,                  // a present, s None, t == a
    TargetDeleted,                  // a present, s == a, t None
    BothDeleted,                    // a present, s None, t None
    DeleteModifyConflict {          // a present, s None, t ≠ a
        target_value: Value,
    },
    ModifyDeleteConflict {          // a present, s ≠ a, t None
        source_value: Value,
    },
}
```

### `CherryPickFilter`

```rust
pub struct CherryPickFilter {
    /// Only pick changes in these spaces. None = all spaces.
    pub spaces: Option<Vec<String>>,
    /// Only pick changes for these user_key strings. None = all keys.
    pub keys: Option<Vec<String>>,
    /// Only pick changes of these primitive types. None = all primitives.
    pub primitives: Option<Vec<PrimitiveType>>,
}
```

The three filters are AND-combined: a change is picked only if it matches every set filter.

> **Graph atomicity guard**: the `keys` filter cannot split a graph cell. If a cell's actions would partially pass and partially fail the filter, the cherry-pick fails fast with a structured error. Use `spaces` or `primitives` (which partition cells atomically) if you need this behavior.

### `DiffOptions`

```rust
pub struct DiffOptions {
    /// Optional filter to narrow the diff by primitive type or space.
    pub filter: Option<DiffFilter>,
    /// Optional timestamp (microseconds) for point-in-time comparison.
    /// When set, both branches are read as they looked at this timestamp.
    pub as_of: Option<u64>,
}

pub struct DiffFilter {
    /// Only include these primitive types.
    pub primitives: Option<Vec<PrimitiveType>>,
    /// Only include these spaces.
    pub spaces: Option<Vec<String>>,
}
```

`DiffOptions` is `Default` — `DiffOptions::default()` produces an unfiltered, current-time diff equivalent to `diff()`.

---

## See also

- [Merge Semantics](../guides/merge-semantics.md) — what each merge does, per primitive
- [Branching Strategies](../guides/branching-strategies.md) — when to use which operation
- [Branch Management](../guides/branch-management.md) — CLI walkthrough
- [Concepts: Branches](../concepts/branches.md) — the mental model
- [Branch Bundles](../guides/branch-bundles.md) — bundle file format spec
- [Design: Primitive-Aware Merge](../design/branching/primitive-aware-merge.md) — implementation deep dive
