# Transactions And Branching

This document covers the consistency model above the storage engine.

## 1. OCC is centered in `TransactionManager`

The main coordination state lives in `crates/concurrency/src/manager.rs`.
The manager owns:

- global version and txn id counters
- per-branch commit locks (`crates/concurrency/src/manager.rs:125`)
- `visible_version`, the highest gap-free fully-applied version (`crates/concurrency/src/manager.rs:153`)
- `pending_versions`, the set of allocated but not yet fully-applied versions (`crates/concurrency/src/manager.rs:160`)
- `commit_quiesce`, the lock that lets checkpoints drain in-flight commits before choosing a safe watermark

The engine is not using "latest allocated version" as a safe read snapshot.
It explicitly separates allocated versions from fully visible versions.

## 2. Transaction visibility is snapshot isolation, not serializable execution

Validation is split across three paths in `crates/concurrency/src/validation.rs`:

- read-set validation (`validate_read_set`)
- CAS validation (`validate_cas_set`)
- JSON document/version and JSON path validation (`validate_json_set`, `validate_json_paths`)

The core rule is first-committer-wins for conflicting reads.
Blind writes can skip normal OCC validation when there were no reads, no CAS operations, and no JSON snapshot dependencies.

That is an intentional throughput optimization, not an accident.

## 3. Commit path is WAL-first and visibility-last

`commit_inner` in `crates/concurrency/src/manager.rs` is the engine's most important consistency function.
The effective sequence is:

1. fast-path read-only transactions
2. take the shared quiesce lock so checkpoints can later drain commits safely
3. take the branch commit lock
4. reject invalid branch states such as branch deletion races
5. run OCC validation unless this is a pure blind-write case (`crates/concurrency/src/manager.rs:418`)
6. allocate a version and mark it pending
7. materialize JSON updates into the final write set
8. append the WAL record
9. apply writes to storage
10. mark the version applied and advance `visible_version` if no earlier gaps remain (`crates/concurrency/src/manager.rs:585`)

This is why readers that use `visible_version()` avoid observing version gaps across branches or across partially-applied commits.

## 4. Cross-primitive atomicity is implemented at the transaction layer

The engine can commit KV, JSON, event, graph, and vector-related writes in one transaction.
That atomicity is not provided by separate primitive stores.
It comes from one commit record, one write set application, and one visibility boundary.

The storage layer only sees a typed key/value write set.
The transaction layer is where "one logical unit across primitives" is enforced.

## 5. Branches are storage snapshots plus metadata, not copied datasets

Forking is primarily a storage operation.
`fork_branch` in `crates/storage/src/segmented/mod.rs:1500` captures a copy-on-write inherited layer instead of cloning all keys.
From the engine side, branch APIs in `crates/engine/src/branch_ops/mod.rs` wrap that storage behavior with branch metadata and user-facing results.

The consequences are:

- branch fork is O(1) in data movement
- child reads can see parent state through inherited layers
- child writes override parent state locally
- deep ancestry may later be materialized into local segments

## 6. Merge base and diff are version-aware, but not fully self-contained

Branch merge is not a simple "copy source into target".
The branch layer computes a merge base and then classifies source/target changes relative to that ancestor (`crates/engine/src/branch_ops/mod.rs:1446`, `crates/engine/src/branch_ops/mod.rs:1847`).

Main operations:

- two-way diff: current source vs current target
- three-way diff: ancestor vs source vs target
- merge base lookup: fork point plus DAG-aware history when available
- merge strategies: `LastWriterWins` and `Strict`

`Strict` refuses any conflicts.
`LastWriterWins` resolves conflicts by picking the source-side write when both sides changed the same cell.

Important boundary:

- `compute_merge_base` treats executor-provided `merge_base_override` as priority 1 and uses it to cover repeated merges and materialized branches (`crates/engine/src/branch_ops/mod.rs:1439`)
- without that override, the engine falls back to storage-level fork info only

So the plain engine merge API is not fully self-sufficient for every branch topology the production stack supports.

## 7. Event merge coverage is intentionally incomplete

Events do not currently have a true semantic merge.
`check_event_merge_divergence` rejects merges when both branches appended to the same event space since the fork (`crates/engine/src/branch_ops/mod.rs:245`).

That protects the event hash chain and per-space sequence metadata from corruption, but it means event branching support currently has a hard boundary:

- single-sided event merges work
- divergent event appends in the same space do not

## 8. Primitive-aware merge is only partly intrinsic to the engine

The engine core can merge generic typed entries itself, but graph and vector semantics are delegated through registration hooks.

Graph:

- `register_graph_semantic_merge` installs the real graph merge planner (`crates/graph/src/merge_handler.rs:39`)
- if nothing is registered, engine fallback rejects divergent graph merges rather than risk corruption (`crates/engine/src/branch_ops/mod.rs:299`)

Vector:

- `register_vector_semantic_merge` installs vector precheck and post-commit rebuild callbacks (`crates/vector/src/merge_handler.rs:55`)
- the precheck prevents incompatible collection configs from merging
- the post-commit step rebuilds affected HNSW collections best-effort (`crates/vector/src/merge_handler.rs:185`)

This gives the engine a good abstraction boundary, but it also means some branch correctness depends on how the engine is embedded.

## 9. Branch DAG is an optional enhancement layer over core branching

Branch DAG hooks are fired from the core branch operations (`crates/engine/src/branch_ops/mod.rs:878`, `crates/engine/src/branch_ops/mod.rs:2108`).
The graph crate implements those hooks and records fork/merge/cherry-pick/revert activity in the `_system_` branch DAG.

Important detail:

- the hook calls are best-effort
- failures are logged but not propagated
- the engine can still function without the DAG layer, but lineage enrichment and repeated-merge merge-base tracking benefits disappear

## 10. Operational consistency model

The code as written delivers this model:

- snapshot-isolated reads within a transaction
- first-committer-wins for read/write conflicts
- optional blind-write fast path for append-heavy or write-only workloads
- metadata-only branch fork
- typed three-way merge with selective semantic extensions
- post-commit visibility only after storage install completes
