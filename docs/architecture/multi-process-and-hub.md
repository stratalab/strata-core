# Multi-Process Access & Hub Architecture

Status: **Design document** (not yet implemented)

## Context

Today, Strata acquires an exclusive file lock (`{db_path}/.lock`) on open, preventing any other process from accessing the same database. This blocks two important use cases:

1. **Desktop** — Multiple Strata Foundry (MacOS app) windows opening the same database
2. **Hub** — Multiple API workers handling concurrent requests against a canonical database

The vision for Strata is git-like: an embedded database that works across cloud and on-prem, where multiple processes can create branches, execute transactions, and diff/merge — without a central server process.

## Current Single-Process Architecture

```
Process A opens DB
  → Acquires exclusive .lock (held for lifetime)
  → Replays WAL into ShardedStore (in-memory)
  → All reads/writes go through in-memory state
  → Commits: validate (OCC) → append WAL → apply to memory
  → Close: release .lock

Process B tries to open same DB
  → try_lock_exclusive() fails
  → Error: "database at '...' is already in use by another process"
```

### Why the lock exists

Six components assume single-process access:

| Component | File | Problem if removed |
|-----------|------|--------------------|
| WAL Writer | `durability/database/handle.rs:35` | Two processes appending to same segment = interleaved bytes |
| Version Counter | `concurrency/manager.rs:65` | `AtomicU64` is process-local — two processes allocate same versions |
| ShardedStore | `storage/src/sharded.rs` | Process B can't see Process A's committed writes |
| Search Index | `search/index.rs:238-240` | Active segment (DashMap) is in-memory only |
| Vector Index | `primitives/vector/hnsw.rs:72-100` | HNSW graph is in-memory, rebuilt on startup |
| Registry | `database/registry.rs:30` | Static HashMap is process-local |

## Proposed Multi-Process Architecture

Make the WAL the inter-process communication channel. The filesystem becomes the source of truth, and in-memory state becomes a cache refreshed from the WAL.

### New process lifecycle

```
Process A opens DB
  → Acquires SHARED lock on .lock
  → Replays WAL into ShardedStore
  → Tracks WAL watermark (last position read)

Process B opens same DB
  → Acquires SHARED lock on .lock (succeeds — shared, not exclusive)
  → Replays WAL into its own ShardedStore
  → Tracks its own watermark

Process A commits a transaction:
  1. Refresh: replay new WAL entries since watermark → update in-memory state
  2. Validate: OCC check against refreshed state
  3. Acquire SHORT-LIVED EXCLUSIVE lock on WAL
  4. Re-validate (someone may have committed between step 2 and 3)
  5. Append WAL record
  6. Release exclusive lock
  7. Apply to local in-memory state, advance watermark

Process B reads:
  1. Tail WAL since watermark → picks up Process A's commit
  2. Read from refreshed in-memory state
```

The critical section shrinks from "entire database lifetime" to "single WAL append + fsync."

### Component changes

| Component | Current | Proposed |
|-----------|---------|----------|
| `.lock` file | Exclusive, held for lifetime | Shared for opens; exclusive briefly for WAL append |
| Version counter | `AtomicU64` in process memory | Read from WAL metadata (last committed version on disk) |
| ShardedStore | Source of truth | Local cache, refreshed from WAL tail |
| Commit path | validate → WAL append → apply | refresh → validate → acquire WAL lock → re-validate → append → release → apply locally |
| Search/vector indexes | Built once at startup | Incrementally updated when new WAL entries are replayed |

### What doesn't change

- Transaction API (begin / read / write / commit)
- Branch isolation, fork, merge
- All primitives (KV, JSON, Event, State, Vector, Graph, Ontology)
- OCC semantics (first committer wins, conflicts abort)
- Bundle export/import
- The `Strata` public API surface

## Hub Architecture

Strata Hub is the GitHub equivalent: a multi-tenant service for hosting, syncing, and collaborating on Strata databases.

### System topology

```
┌─────────────┐   ┌─────────────┐
│  Foundry A  │   │  Foundry B  │     (desktop apps, each with local DB)
│  (MacBook)  │   │  (Cloud VM) │
└──────┬──────┘   └──────┬──────┘
       │ push/pull        │ push/pull
       │ (bundles)        │ (bundles)
       ▼                  ▼
┌──────────────────────────────────────────────────┐
│                   Strata Hub                      │
│                                                   │
│  ┌───────────┐  ┌───────────┐  ┌──────────────┐ │
│  │ API Worker │  │ API Worker │  │ Background   │ │  (stateless processes)
│  │           │  │           │  │ Jobs         │ │
│  └─────┬─────┘  └─────┬─────┘  └──────┬───────┘ │
│        └──────────────┼───────────────┘          │
│                       ▼                          │
│            ┌─────────────────────┐               │
│            │   Canonical DBs     │               │  (one directory per database)
│            │   /data/org/repo-a/ │               │
│            │   /data/org/repo-b/ │               │
│            └─────────────────────┘               │
└──────────────────────────────────────────────────┘
```

Workers are stateless: open the DB, do the work, close. The WAL coordinates concurrent access. No long-lived server process needed.

### Operations

**Push (user -> Hub):**
1. User exports a branch as a bundle (`branch_export`)
2. Uploads bundle to Hub via HTTP
3. Hub worker opens canonical DB (shared lock), imports bundle
4. Import writes to WAL under short exclusive lock
5. Other workers see the new branch on next WAL refresh

**Pull (Hub -> user):**
1. Hub worker opens canonical DB (shared lock)
2. Exports requested branch as a bundle — pure read, no WAL lock needed
3. User downloads and imports locally

**Concurrent pushes to same branch:**
1. Worker A imports bundle for `main` — starts transaction
2. Worker B imports bundle for `main` — starts transaction
3. Worker A commits first — acquires WAL lock, appends, releases
4. Worker B refreshes WAL, re-validates — either succeeds (no conflict) or aborts
5. On abort: user gets a merge error, must rebase (same as `git push` rejection)

**Web UI queries:**
- Hub worker opens DB with shared lock, reads data, closes
- Periodic WAL tail keeps view fresh for long-lived connections

### Hub application layer (not in the Strata engine)

| Feature | Description |
|---------|-------------|
| Auth & permissions | Who can push/pull which databases |
| Database registry | Maps `org/db-name` to a disk path |
| Bundle transfer | HTTP API for upload/download of `.strata-bundle` files |
| Webhooks / events | Triggered on push, merge, branch create |
| Web UI | Browse data, view diffs, run queries |
| PR / review flow | Branch diff is already in Strata; Hub adds the collaboration UX |

### Multi-database / multi-tenant

Each database is a separate directory with its own WAL and lock file. A Hub server with 10,000 databases is 10,000 independent directories. Workers accessing different databases don't interact at all. This scales horizontally.

## Architecture Assessment

### Strengths

**Embedded-first, no-server model.** Most databases force client-server even when unnecessary. SQLite proved embedded wins for a huge class of applications. Strata extends that with branching, MVCC, and multi-primitive storage. For agent workflows — spin up a branch, do work, merge results — server overhead per operation is pure friction.

**In-memory-first with WAL durability.** AI agent sessions are write-heavy, short-lived, and latency-sensitive. Paying disk I/O on every read would be wasteful when most sessions fit in memory. The WAL provides crash recovery without sacrificing read speed.

**Multi-primitive design avoids impedance mismatch.** KV, JSON, events, state cells, vectors, and graphs — each with the right semantics. An agent can append to an event log, update a state cell with CAS, and query a vector store without translating between paradigms.

**Branching as a first-class concept.** No other embedded database provides fork-branch-merge at the data level. For A/B testing agent strategies, sandboxing untrusted operations, and schema evolution — this is genuinely novel and useful.

**OCC for multi-process fits naturally.** The existing optimistic concurrency control already detects conflicts at commit time. Extending it across processes via WAL coordination preserves the same semantics without adding distributed consensus complexity.

### Tradeoffs and risks

**In-memory storage is a scalability ceiling.** `ShardedStore` holds all live data in `DashMap`. Works for agent sessions (small to medium datasets), but loading a 50GB knowledge graph exceeds memory. The graph/ontology features pull toward larger datasets where a tiered storage model (hot in memory, warm on mmap, cold on disk) may eventually be needed.

**WAL coordination adds real complexity.** The multi-process change turns a clean single-writer architecture into something closer to SQLite's WAL mode — with edge cases around reader staleness, checkpoint coordination, and crash recovery with multiple partial writers. This must be bulletproof or it undermines the ACID guarantees.

**Bundle-based sync is simple but not incremental.** Pushing a branch with 1M keys and then adding one key transfers the whole branch again. Git solved this with packfile negotiation ("I have commits A, B, C — what do you have?"). Strata will need something similar, likely WAL segment-based: "send me WAL entries after watermark X for branch Y."

**No read replica story yet.** The multi-process change lets multiple workers hit one database, but they all do full WAL replay into memory. For Hub with heavy read traffic, workers that maintain a read-only in-memory replica refreshed from WAL — without ever taking the write lock — should be an explicit design goal.

**Surface area is large.** KV + JSON + Events + State Cells + Vectors + Search + Graphs + Ontology + Branches + Spaces + Bundles + Transactions is a lot. Each primitive is another thing to test, optimize, and maintain through architectural changes. Git won with three object types and one transport protocol. SQLite won by being one thing done perfectly. The multi-primitive approach is a real DX win, but the maintenance cost is real.

### Competitive positioning

|  | SQLite | Strata | Git |
|--|--------|--------|-----|
| Embedded, no server | Yes | Yes | Yes |
| Branching | No | Yes | Yes |
| Merge | No | Yes | Yes |
| Multi-primitive | No | Yes | No (blobs only) |
| ACID transactions | Yes | Yes | No |
| Multi-process writes | Yes (WAL mode) | Proposed | Yes (lockfile per-ref) |
| Sync/transport | No | Bundles | Packfiles |

Strata occupies a unique position: "SQLite with git semantics and native support for AI data types."

## Target Architecture Summary

```
┌──────────────────────────────────────────────────────────────┐
│                        Strata Engine                         │
│          Embedded library, multi-process safe via            │
│                    WAL coordination                          │
└──────┬──────────────────┬──────────────────┬─────────────────┘
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│   Foundry   │    │     Hub     │    │  Your App   │
│ Desktop app │    │ Web service │    │ Embedded    │
│ Opens local │    │ Stateless   │    │ via crate   │
│ DBs directly│    │ workers open│    │ dependency  │
│             │    │ canonical   │    │             │
│             │    │ DBs directly│    │             │
└──────┬──────┘    └──────┬──────┘    └─────────────┘
       │                  │
       │    ┌─────────┐   │
       └───►│ Bundles ├◄──┘
            └─────────┘
         Transport layer
       (like git packfiles)
```

- **Strata engine** — embedded library, multi-process safe via WAL coordination
- **Foundry** — desktop app, opens local databases directly
- **Hub** — web service with stateless workers, opens canonical databases directly
- **Bundles** — the transport layer between Foundry and Hub (like git packfiles)
- **WAL** — the coordination layer when multiple workers touch the same database

## Implementation Priorities

1. **WAL-based multi-process coordination** — the enabling change for everything else
2. **Incremental WAL replay** — fast "tail since watermark X" (segmented WAL + `.meta` sidecars have the metadata for this)
3. **Incremental index refresh** — search and vector indexes updated from new WAL entries (insert/remove APIs exist)
4. **Staleness configuration** — how often processes tail the WAL (per-read, periodic, on-demand `db.refresh()`)
5. **Incremental sync** — WAL segment-based push/pull to replace full-branch bundle transfers
