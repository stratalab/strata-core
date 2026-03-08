# Crate Structure

StrataDB is a Rust workspace with 8 member crates. Dependencies flow downward — higher-level crates depend on lower-level ones.

## Dependency Graph

```
stratadb (root)
  └── strata-executor
        ├── strata-engine
        │     ├── strata-concurrency
        │     │     ├── strata-storage
        │     │     │     └── strata-core
        │     │     └── strata-durability
        │     │           └── strata-core
        │     ├── strata-storage
        │     └── strata-durability
        ├── strata-intelligence
        │     ├── strata-core
        │     └── strata-engine
        └── strata-security
              └── strata-core
```

## Crate Descriptions

### `strata-core`

**Purpose:** Shared types and traits used by all other crates.

**Key types:**
- `Value` — the 8-variant canonical value type
- `BranchId`, `Key`, `Namespace`, `TypeTag` — storage key types
- `Versioned<T>`, `Version`, `Timestamp` — versioning contracts
- `PrimitiveType` — enum of primitive types
- `Storage`, `SnapshotView` — storage traits
- Event, State, JSON, Vector type definitions

**Dependencies:** uuid, serde, serde_json, thiserror, chrono, bincode

### `strata-storage`

**Purpose:** Low-level storage backend and indexing.

**Key types:**
- `ShardedStore` — DashMap-based concurrent storage (the main data structure)
- `BranchRegistry` — branch/namespace management
- `InvertedIndex` — text index for search
- `StoredValue` — serialized value wrapper
- TTL/retention handling

**Dependencies:** strata-core, dashmap, rustc-hash, thiserror

### `strata-concurrency`

**Purpose:** Optimistic Concurrency Control (OCC) implementation.

**Key responsibilities:**
- Transaction context creation and management
- Read-set and write-set tracking
- Commit-time conflict detection and validation
- Version counter allocation
- CAS (compare-and-swap) support

**Dependencies:** strata-core, strata-storage, strata-durability, dashmap, parking_lot

### `strata-durability`

**Purpose:** Write-ahead logging, snapshots, and crash recovery.

**Key types:**
- `WAL` — write-ahead log file operations
- `WALEntry` — all possible log entry types
- `DurabilityMode` — None/Batched/Strict
- `SnapshotWriter` / `SnapshotReader` — snapshot I/O
- `BranchBundleWriter` / `BranchBundleReader` — portable branch archives
- `replay_wal()` — crash recovery

**Dependencies:** strata-core, serde, rmp-serde, crc32fast, tar, zstd, xxhash-rust

### `strata-engine`

**Purpose:** Database orchestration and primitive implementations.

**Key types:**
- `Database` — the central database struct (thread-safe, opened once)
- `Transaction` / `TransactionContext` — transaction interface
- `TransactionOps` — trait for transactional operations
- Primitive implementations: KVStore, EventLog, StateCell, JsonStore, VectorStore, BranchIndex

**Dependencies:** strata-core, strata-storage, strata-concurrency, strata-durability

### `strata-intelligence`

**Purpose:** Search, scoring, and hybrid retrieval.

**Key types:**
- Hybrid search coordinator
- BM25 keyword scorer
- RRF (Reciprocal Rank Fusion) result merger
- Query tokenizer

**Dependencies:** strata-core, strata-engine, dashmap, serde

### `strata-security`

**Purpose:** Access control and security policies.

**Key responsibilities:**
- Read-only access mode for database connections
- Per-connection access control (read-write vs read-only)
- Future: role-based access control, authentication

**Dependencies:** strata-core

### `strata-executor`

**Purpose:** Public API and command dispatch. **This is the only crate users import.**

**Key types:**
- `Strata` — the main user-facing API with typed methods
- `Session` — stateful transaction support
- `Executor` — command dispatcher
- `Command` — all operations as enum variants (114 commands across core + graph + intelligence categories)
- `Output` — all results as enum variants
- `Error` — structured error type
- Re-exports `Value` from strata-core

**Dependencies:** strata-core, strata-engine, strata-intelligence, strata-security

## Root Crate: `stratadb`

The workspace root re-exports `strata-executor`, so users add `stratadb` to their `Cargo.toml` and get the full API.

```toml
# Users write:
[dependencies]
stratadb = "0.1"
```

```rust
// Users import:
use stratadb::{Strata, Value};
```

## Build Configuration

- **Edition:** 2021
- **Minimum Rust:** 1.70
- **License:** MIT
- **Release profile:** LTO enabled, single codegen unit
