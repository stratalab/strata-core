# Spaces Guide

Spaces are an organizational layer within branches. Each branch contains one or more spaces, and each space has its own independent instance of every primitive (KV, Event, State, JSON, Vector). Think of spaces like schemas in PostgreSQL — they organize data within a database (branch) without creating full isolation boundaries.

## Command Overview

| Command | Syntax | Returns |
|---------|--------|---------|
| `use` | `use <branch> [space]` | Switches branch and/or space |
| `space list` | `space list` | All space names in current branch |
| `space create` | `space create <name>` | Creates a space |
| `space del` | `space del <name> [--force]` | Deletes a space |
| `space exists` | `space exists <name>` | Whether the space exists |

## Default Space

Every branch starts with a `default` space. When you open the CLI, all operations target this space automatically. You never need to create or switch to it explicitly.

```
$ strata --cache
strata:default/default> kv put key value
(version) 1
strata:default/default> event append log '{"msg":"hello"}'
(seq) 1
```

The `default` space cannot be deleted.

## Creating and Switching Spaces

Use `use <branch> <space>` to switch to a space. Spaces are auto-registered on first write — no explicit create step is needed:

```
$ strata --cache
strata:default/default> use default conversations
strata:default/conversations> kv put msg_001 hello
(version) 1
strata:default/conversations> use default tool-results
strata:default/tool-results> kv put task_42 done
(version) 1
strata:default/tool-results> space list
- conversations
- default
- tool-results
```

You can also create a space explicitly:

```
strata:default/default> space create my-space
OK
```

## Data Isolation Between Spaces

Each space has its own independent data. The same key in different spaces refers to different values:

```
$ strata --cache
strata:default/default> kv put config default-config
(version) 1
strata:default/default> use default experiments
strata:default/experiments> kv put config experiment-config
(version) 1
strata:default/experiments> use default
strata:default/default> kv get config
"default-config"
strata:default/default> use default experiments
strata:default/experiments> kv get config
"experiment-config"
```

This applies to all primitives — events, state cells, JSON documents, and vector collections are all space-scoped.

## Cross-Space Transactions

Transactions can span multiple spaces within the same branch. This is useful when you need atomic operations across organizational boundaries:

```
$ strata --cache
strata:default/default> begin
OK
strata:default/default> use default billing
strata:default/billing> kv put credits 99
(version) 1
strata:default/billing> use default api-logs
strata:default/api-logs> event append api_call '{"endpoint":"/search"}'
(seq) 1
strata:default/api-logs> commit
OK
```

## Space Naming Rules

Space names follow these conventions:

| Rule | Details |
|------|---------|
| **Start with** | Lowercase letter `[a-z]` |
| **Allowed characters** | Lowercase letters, digits, hyphens, underscores `[a-z0-9_-]` |
| **Max length** | 64 characters |
| **Reserved prefix** | `_system_` (reserved for internal use) |
| **Reserved name** | `default` (cannot be deleted) |

Valid names: `conversations`, `tool-results`, `agent_memory_v2`

Invalid names: `Conversations` (uppercase), `123-invalid` (starts with digit), `_system_internal` (reserved prefix)

## Deleting Spaces

Delete a space with `space del` (must be empty) or `space del --force` (deletes all data):

```
$ strata --cache
strata:default/default> use default temp
strata:default/temp> kv put key value
(version) 1
strata:default/temp> use default
strata:default/default> space del temp
(error) space is non-empty
strata:default/default> space del temp --force
OK
```

The `default` space cannot be deleted.

### What `--force` actually removes

A force-delete is more than a primary-data wipe. In one operation it removes:

| Layer | What gets cleaned up |
|---|---|
| **Primary data** | All KV, Event, JSON, Vector, and Graph entries in the space (atomic, in a single MVCC transaction) |
| **BM25 search index** | Every inverted-index document whose `EntityRef` lives in the space — search will not return stale hits after the delete |
| **Vector backends** | Every in-memory vector collection backend in `(branch, space)` is evicted from the running process |
| **Vector disk caches** | The `.vec` mmap heap and `_graphs/` directory for every vector collection in the space are removed from the data directory, so recovery cannot resurrect a deleted collection from a stale cache |
| **Shadow embeddings** | Every entry in `_system_embed_kv`, `_system_embed_json`, and `_system_embed_event` whose source key prefix matches the deleted space, plus any pending entries in the in-memory embed buffer |
| **Space metadata key** | Removed last, so external observers (`space list`, `space exists`) keep reporting the space as "exists" until all secondary cleanup has had a chance to run |

The primary data wipe runs inside a single MVCC transaction, so it is atomic and isolated. The five secondary cleanups are **best-effort**: any failure logs a warning and the delete still succeeds. This means:

- A `--force` delete that succeeds at the command level may have left a warning in the logs about a single failing cleanup step. Check `strata::space`, `strata::vector`, and `strata::embed` log targets if you suspect a partial cleanup.
- Cleanup failures never roll back the primary delete. If any of the secondary state is left behind on disk (e.g., a `.vec` file the process couldn't unlink), it is harmless: the next recovery pass will either rebuild it from KV or ignore it as orphaned.

### What `--force` does **not** do

- It does not affect other spaces in the same branch.
- It does not affect the same-named space in any other branch.
- It does not pause concurrent writes to a different space — the per-space cleanup is scoped by `(branch, space)` and never touches anything outside that pair.

### Concurrent writes during a force-delete

If a write to the same space lands between the primary delete and the metadata-key delete, the space will reappear via Strata's union-of-metadata-and-data discovery (`space list` will list it again). This matches "delete then immediately recreate" semantics — there is no fence that locks out new writes during the delete.

## Common Patterns

### Agent Memory Organization

```
$ strata --db ./data
strata:default/default> use default conversations
strata:default/conversations> event append user_message '{"content":"What is the weather?"}'
(seq) 1
strata:default/conversations> event append tool_call '{"tool":"weather_api"}'
(seq) 2
strata:default/conversations> use default tool-results
strata:default/tool-results> kv put weather_api:call_1 '{"temp":72,"conditions":"sunny"}'
(version) 1
strata:default/tool-results> use default user-context
strata:default/user-context> state set preferences '{"units":"fahrenheit"}'
(version) 1
```

### Multi-Tenant Data

```bash
#!/bin/bash
set -euo pipefail

for tenant in acme-corp globex initech; do
    strata --db ./data --space "$tenant" kv put config '{"plan":"enterprise"}'
    strata --db ./data --space "$tenant" vector create docs 384 --metric cosine
done

strata --db ./data space list
```

### Experiment Tracking

```
$ strata --db ./data
strata:default/default> use default hyperparams
strata:default/hyperparams> kv put config '{"lr":0.001,"epochs":10}'
(version) 1
strata:default/hyperparams> use default experiment-001
strata:default/experiment-001> kv put metrics '{"loss":0.42,"accuracy":0.87}'
(version) 1
strata:default/experiment-001> use default experiment-002
strata:default/experiment-002> kv put metrics '{"loss":0.38,"accuracy":0.89}'
(version) 1
```

## Compatibility

Spaces became a first-class identity dimension in v0.6.2. Databases created on older versions are still openable, but a few categories of pre-v0.6.2 data are not space-correct after upgrade. Strata does not migrate them — there is no automatic conversion code, and no plan to add one. The remediation is always "reindex from KV" or "ignore the orphan."

| Surface | Pre-v0.6.2 behaviour | After upgrade |
|---|---|---|
| **BM25 search index** | Stored space-blind `EntityRef` values | The format version was bumped (`MANIFEST_VERSION = 2`); the old checkpoint is rejected on first open and the index is rebuilt from KV with space-aware refs. **No action required** — rebuild happens automatically, may take a few seconds on the first open of a large database. |
| **Generic persisted `EntityRef`** | Stored without a `space` field | Decoded with `space = "default"`. Entries that originally referred to a non-default space cannot be reconstructed without an independent source of truth. Affects user-exported structured references and the small number of internal call sites that persist generic `EntityRef`. |
| **Shadow embeddings** (`_system_embed_*`) | Composite key prefix used the source space, but the embedded `source_ref` was space-blind | Cleanup paths and per-key delete continue to work because they use the composite key prefix. However, hybrid search hydration that resolves a shadow vector back to its source via `source_ref` will look up `space = "default"`. If your pre-v0.6.2 database had auto-embed enabled with non-default spaces, hybrid search results for those entries may resolve to the wrong source. Remedy: re-run auto-embed (the new entries are written with space-aware `source_ref`s). |
| **Vector cache files** | Stored at `{data_dir}/vectors/{branch_hex}/{collection}.vec` (no space subdirectory) | Recovery uses the new path `{data_dir}/vectors/{branch_hex}/{space}/{collection}.vec` and never reads the legacy path. Old `.vec` files at the legacy location are orphaned but harmless. Manual cleanup: `rm -rf {data_dir}/vectors/{branch_hex}/*.vec` (only the files directly under `{branch_hex}/`, not the per-space subdirectories). |
| **`SpaceIndex` metadata** | Some legacy databases have data in spaces that were never explicitly registered in the metadata index | Phase 3's startup repair (`repair_space_metadata`) detects these on every open and registers them automatically. **No action required.** |

### Brand-new databases

If you create a fresh v0.6.2+ database, none of the above applies. All entries are written with space-aware identities from day one.

### Downgrading

Downgrading from v0.6.2+ to an older binary is **not supported**. v0.6.2 changed several on-disk formats (BM25 manifest, vector cache paths) and an older binary may either reject the new files outright or interpret them in undefined ways. Always upgrade in one direction.

## Spaces vs Branches

| | Branches | Spaces |
|--|----------|--------|
| **Purpose** | Isolation | Organization |
| **Data visibility** | Fully isolated | Fully visible within branch |
| **Transactions** | Cannot span branches | Can span spaces |
| **Analogy** | Git branches | PostgreSQL schemas |
| **Use case** | Separate experiments, sessions | Organize data within a session |

Use branches when you need full data isolation. Use spaces when you need to organize related data within a single branch.

## Next

- [KV Store](kv-store.md) — key-value operations
- [Branch Management](branch-management.md) — branch isolation
- [Sessions and Transactions](sessions-and-transactions.md) — cross-space atomicity
