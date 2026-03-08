# Architecture Overview

StrataDB is a layered embedded database built as a Rust workspace of 7 crates. This section describes the system from the top down.

## High-Level Diagram

```
+-----------------------------------------------------------+
|  Strata API                                                |
|  (KV, Event, State, JSON, Vector, Branch)                     |
+-----------------------------------------------------------+
|  Executor (Command dispatch)                               |
|  Session (Transaction lifecycle + read-your-writes)         |
+-----------------------------------------------------------+
|  Engine                                                    |
|  (Database, Primitives, Transaction coordination)          |
+-----+-----------------------+-----------------------------+
      |                       |
+-----v-------+  +------------v----------+  +--------------+
| Concurrency |  |  Durability           |  | Intelligence |
| OCC, CAS    |  |  WAL, Snapshots       |  | BM25, RRF    |
| Validation  |  |  Recovery, BranchBundle  |  | Hybrid Search|
+------+------+  +----------+------------+  +------+-------+
       |                     |                      |
       +----------+----------+----------------------+
                  |
        +---------v---------+
        |  Storage          |
        |  ShardedStore     |
        |  DashMap-based    |
        +-------------------+
        |  Core             |
        |  Value, Types     |
        +-------------------+
```

## Key Design Decisions

### Unified Storage

All six primitives store their data in a single `ShardedStore` (DashMap-based). Keys are prefixed with the branch ID and primitive type. This enables:

- **Atomic multi-primitive transactions** — a single OCC validation covers KV, State, Event, and JSON
- **Simple storage layer** — one sorted map, no separate data files per primitive
- **Branch deletion** — scan and delete by prefix

### Branch-Tagged Keys

Every key in storage includes the branch ID: `{branch_id}:{primitive}:{user_key}`. This makes:

- **Branch isolation** automatic — no filtering needed
- **Branch replay** O(branch size) instead of O(total database size)
- **Branch deletion** a prefix scan

### Optimistic Concurrency Control

Transactions use OCC rather than locks:

- Begin: take a snapshot version
- Execute: read from snapshot, buffer writes
- Validate: check that reads haven't been modified by concurrent commits
- Commit: apply writes atomically

This works well for AI agents because they rarely conflict (different keys, different branches).

### Stateless Primitives

The `Strata` struct is a thin wrapper around an `Executor`. Primitives don't hold state — they just translate method calls into `Command` enums and dispatch them. This means:

- Multiple `Strata` instances can safely share a `Database`
- No warm-up or initialization state
- Idempotent retry works correctly

## Deep Dives

- [Crate Structure](crate-structure.md) — the 7 crates and their responsibilities
- [Storage Engine](storage-engine.md) — ShardedStore, MVCC, key structure
- [Durability and Recovery](durability-and-recovery.md) — WAL, snapshots, recovery flow
- [Concurrency Model](concurrency-model.md) — OCC lifecycle, conflict detection
- [Architecture Book](book/00-system-map.md) — internal, evidence-linked layer-by-layer reference
