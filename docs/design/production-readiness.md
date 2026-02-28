# Strata Production Readiness — Architecture Review

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-02-23

---

## 1. Executive Summary

Strata is a well-designed embedded database with strong Rust type safety, correct MVCC fundamentals, and a thoughtful modular architecture. However, **it is not yet production-ready**. A crate-by-crate audit plus deep-dive reviews of the graph and vector subsystems found **17 critical, 50 high, 43 medium, and 12 low issues** (122 total) across the codebase.

The most important gaps fall into five cross-cutting themes:

| Theme | Severity | Production DB Benchmark |
|-------|----------|------------------------|
| **Unbounded resource growth** (memory, versions, shards) | CRITICAL | SQLite/Redis/RocksDB all have hard limits |
| **Silent data loss paths** (recovery fallback, shutdown races) | CRITICAL | SQLite refuses to open on corruption; Redis has `appendfsync always` |
| **Missing crash safety guarantees** (Standard mode fsync, stale locks) | CRITICAL | LMDB is fully ACID; SQLite WAL is crash-safe by design |
| **No encryption at rest** | CRITICAL | SQLite has SEE/sqlcipher; Redis has TLS + ACLs |
| **Incomplete observability** (no metrics, no health checks) | HIGH | Every production DB exposes metrics |

**Bottom line:** Strata has strong foundations but needs ~8-12 weeks of hardening work before it can safely run production embedded workloads.

---

## 2. Crate-by-Crate Audit

### 2.1 core — Type System & Contracts

**Verdict:** Solid foundation with a few safety gaps in unsafe code, validation boundaries, and version semantics.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| C-1 | Unsafe JSON transmute without compile-time layout assertion | **CRITICAL** | `primitives/json.rs:1028, 1088` | SQLite never uses unsafe for JSON | #1300 |
| C-2 | Constraint validation not enforced at all write boundaries | **CRITICAL** | `limits.rs`, `error.rs` | SQLite enforces `SQLITE_MAX_LENGTH` at every entry point | #1299 |
| C-3 | Cross-variant Version comparison (`Txn(5)` == `Counter(5)`) | **HIGH** | `contract/version.rs:186-208` | PostgreSQL uses single monotonic XID type | #1300 |
| C-4 | Namespace components accept empty strings | **HIGH** | `types.rs` | Redis requires non-empty key names; DuckDB validates schema names | #1299 |
| C-5 | Conflict errors lack forensic context (no structured data) | **HIGH** | `error.rs` | PostgreSQL conflict errors include table, constraint, column | #1301 |
| C-6 | Deprecated TypeTag variant (`Trace = 0x04`) deserializes silently | **HIGH** | `types.rs` | SQLite schema versioning rejects unknown types | #1300 |
| C-7 | `Storage`/`SnapshotView` trait concurrency contracts underspecified | **HIGH** | `traits.rs` | LMDB documents exact thread-safety guarantees per operation | #1301 |
| C-8 | Value validation and `max_value_bytes_encoded` are separate checks | **MEDIUM** | `limits.rs` | — | #1299 |
| C-9 | `BranchEventOffsets::push()` accepts out-of-order offsets | **MEDIUM** | `branch_types.rs` | — | #1299 |
| C-10 | `EntityRef` fields are plain Strings with no validation | **MEDIUM** | `contract/entity_ref.rs` | — | #1299 |

**Strengths:** Excellent newtype pattern (`BranchId`, `TypeTag`). Comprehensive `Limits` framework. Strong `Serialize`/`Deserialize` coverage. Excellent error code design (10 canonical codes). Extensive test coverage (300+ tests across `types.rs` alone).

> **Key comparison:** SQLite enforces size and type constraints at every API entry point. Strata's `Limits` framework exists but is not wired into all write paths, meaning data can bypass validation at the storage layer.

---

### 2.2 storage — Sharded KV Store

**Verdict:** Correct MVCC semantics, but unbounded by design — no memory limits, no automatic GC, TTL exists but is not integrated.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| S-1 | Unbounded version chain growth; GC never called automatically | **CRITICAL** | `sharded.rs:56, 115` | Redis has `maxmemory` + eviction policies | #1303 |
| S-2 | No shard/branch count limit; DashMap grows without bound | **CRITICAL** | `sharded.rs:260-265` | LMDB has `max_dbs` limit | #1306 |
| S-3 | O(v) linear scan in `get_at_version()` | **HIGH** | `sharded.rs:84-93` | RocksDB uses binary search within SST blocks | #1306 |
| S-4 | TTL index implemented but never integrated with ShardedStore | **HIGH** | `ttl.rs:12-22`, `stored_value.rs:125` | Redis combines lazy + active expiration | #1303 |
| S-5 | `apply_batch` allows partial failure across branches (no rollback) | **HIGH** | `sharded.rs:489-517` | Redis `MSET` is atomic | #1306 |
| S-6 | GC always keeps >= 1 version, even if it's a tombstone | **MEDIUM** | `sharded.rs:115-135` | PostgreSQL vacuum removes dead tuples entirely | #1303 |
| S-7 | `ordered_keys` BTreeSet never pruned on delete | **MEDIUM** | `sharded.rs:212-216` | — | #1304 |
| S-8 | TTL `saturating_add()` can produce u64::MAX (key never expires) | **MEDIUM** | `stored_value.rs:137` | — | #1303 |
| S-9 | Every returned value is cloned; no zero-copy path | **MEDIUM** | `sharded.rs:1136-1140` | LMDB returns zero-copy memory-mapped slices | #1305 |

**Strengths:** Lock-free reads via DashMap. O(1) lookups via FxHashMap. Correct MVCC snapshot semantics via Arc-based snapshots. No unsafe code. Atomic version counter prevents lost updates.

> **Key comparison:** Redis enforces `maxmemory` with configurable eviction policies and combines lazy + active key expiration. Strata has no memory bound on any data structure, and TTL expiration is lazy-only with no active cleanup.

---

### 2.3 concurrency — Transaction System

**Verdict:** Correct Snapshot Isolation via OCC, but version overflow panics the process, no transaction timeout, and clone-based snapshots are O(n).

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| T-1 | Version counter overflow panics (`expect()` on `checked_add`) | **CRITICAL** | `manager.rs:146-151` | PostgreSQL has `txid_wraparound` prevention with automated vacuum | #1307 |
| T-2 | Transaction ID overflow — same panic risk | **CRITICAL** | `manager.rs:122-126` | — | #1307 |
| T-3 | Implements Snapshot Isolation but write-skew is silently allowed | **CRITICAL** | — | SQLite documents `DEFERRED`/`IMMEDIATE`/`EXCLUSIVE` isolation | #1309 |
| T-4 | No abandoned transaction detection or timeout | **CRITICAL** | `transaction.rs:1038` (helper exists, not enforced) | PostgreSQL has `idle_in_transaction_session_timeout` | #1308 |
| T-5 | `ClonedSnapshotView` deep-copies entire branch — O(data_size) | **HIGH** | `snapshot.rs:69-74` | SQLite/LMDB snapshots are O(1); RocksDB uses reference counting | #1309 |
| T-6 | Per-branch commit lock serializes all commits on same branch | **HIGH** | `manager.rs:216` | RocksDB supports concurrent memtable writes | #1308 |
| T-7 | CAS operations don't track reads (not added to read_set) | **HIGH** | `transaction.rs:845-854` | — | #1309 |
| T-8 | JSON document-level conflict detection is overly conservative | **HIGH** | `validation.rs:241-275` | — | #1309 |
| T-9 | No deadlock/livelock detection or metrics | **HIGH** | — | PostgreSQL tracks lock waits and reports deadlocks | #1308 |
| T-10 | No savepoints or nested transactions | **MEDIUM** | `transaction.rs` (state machine) | SQLite supports `SAVEPOINT` | #1309 |

**Strengths:** Correct OCC protocol. Deadlock impossible by design (single lock per branch, no nested acquisition). Clean state machine transitions with guards. Idempotent recovery.

> **Key comparison:** SQLite's WAL mode provides Snapshot Isolation with O(1) zero-copy snapshots (readers see old pages via WAL). Strata's `ClonedSnapshotView` is O(n) in data size — this is the single biggest architectural debt for concurrent workloads.

---

### 2.4 durability — WAL & Recovery

**Verdict:** Good crash recovery fundamentals with per-record CRC and atomic manifest updates, but Standard mode defers fsync to a background thread with no health monitoring.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| D-1 | Standard mode fsync deferred to background thread; no health check | **CRITICAL** | `wal/writer.rs:252-256` | SQLite documents `PRAGMA synchronous` levels with exact data-loss windows | #1310 |
| D-2 | Segment header CRC missing from record integrity chain | **HIGH** | `format/wal_record.rs:32-51` | RocksDB uses per-record CRC + block checksums | #1310 |
| D-3 | Stale WAL lock: no PID validation, crashed process blocks others | **HIGH** | `coordination.rs:14-67` | PostgreSQL validates PID in postmaster.pid | #1310 |
| D-4 | Snapshot parent directory fsync may fail silently | **HIGH** | `disk_snapshot/writer.rs:127-132` | ext4/XFS require parent dir fsync for rename durability | #1311 |
| D-5 | Recovery doesn't validate watermark-snapshot consistency | **HIGH** | `recovery/coordinator.rs:103-109` | SQLite cross-validates WAL checkpoints | #1311 |
| D-6 | Compaction not atomic with concurrent writes (race on segment list) | **HIGH** | `compaction/wal_only.rs:81-147` | RocksDB uses version edits with manifest lock | #1312 |
| D-7 | Active segment never gets `.meta` sidecar until rotation | **HIGH** | `wal/reader.rs:205-260` | — | #1311 |
| D-8 | 1 MB corruption scan window; data beyond may be lost | **MEDIUM** | `wal/reader.rs:14, 94-122` | — | #1312 |
| D-9 | No disk space monitoring or emergency compaction trigger | **MEDIUM** | — | PostgreSQL monitors `pg_tablespace` usage | #1312 |
| D-10 | Format version checking incomplete (v1 without CRC still supported) | **MEDIUM** | `format/wal_record.rs:130-143` | — | #1312 |

**Crash Safety Assessment:**

| Scenario | Risk |
|----------|------|
| Crash during WAL write | LOW — partial writes detected by CRC |
| Crash during snapshot | MEDIUM — temp file cleanup on next recovery |
| Crash during compaction | MEDIUM — partial segment deletion possible |
| Crash in Standard mode flush | **CRITICAL** — data loss up to `interval_ms` + `batch_size` |
| Crash with stale lock file | MEDIUM — requires manual intervention |

**Strengths:** Per-record CRC checksums. Segmented WAL with metadata optimization. Multi-process writer support. Atomic MANIFEST updates via write-fsync-rename. Comprehensive recovery algorithm (deterministic, idempotent).

> **Key comparison:** SQLite's WAL is the gold standard for embedded crash safety — single-file, automatic checkpointing, reader/writer concurrency without external locks. Strata's segmented WAL is more complex (multi-file, external metadata, separate compaction) which creates more failure modes.

---

### 2.5 engine — Database Orchestrator

**Verdict:** Core orchestration works, but recovery failure silently returns empty state (data loss), and shutdown has a 30-second timeout that ignores in-flight transactions.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| E-1 | Recovery failure returns `RecoveryResult::empty()` — silent data loss | **CRITICAL** | `database/mod.rs:466-476` | SQLite refuses to open a corrupt database | #1313 |
| E-2 | Shutdown timeout ignores in-flight transactions after 30s | **CRITICAL** | `database/mod.rs:1954-1959` | PostgreSQL's `smart` shutdown waits for all sessions | #1314 |
| E-3 | Search index updated non-atomically during refresh | **HIGH** | `database/mod.rs:1004-1091` | — | #1322 |
| E-4 | Vector recovery requires manual `register_vector_recovery()` | **HIGH** | `primitives/vector/recovery.rs:42-44` | — | #1313 |
| E-5 | WAL flush thread shutdown race (flag check without lock) | **HIGH** | `database/mod.rs:538-546` | — | #1314 |
| E-6 | Vector heap freeze errors silently ignored in Drop | **HIGH** | `database/mod.rs:1985-1986` | — | #1313 |
| E-7 | Multi-process counter file seeding failure allows wrong starting versions | **HIGH** | `database/mod.rs:505-521` | — | #1313 |
| E-8 | Branch lock removal not coordinated with in-flight transactions | **HIGH** | `database/mod.rs:893-901` | — | #1314 |
| E-9 | No health check or liveness probe API | **HIGH** | — | Redis has `PING`; RocksDB has `GetProperty()` | #1320 |
| E-10 | No metrics export for observability | **HIGH** | — | Redis has 200+ `INFO` metrics; RocksDB has 100+ properties | #1320 |
| E-11 | Transaction coordinator metrics use `Relaxed` ordering | **MEDIUM** | `coordinator.rs:30-34` | — | #1320 |
| E-12 | Search index fast path doesn't validate manifest version | **MEDIUM** | `search/recovery.rs:72-81` | — | #1322 |

**Strengths:** Clean primitive handler architecture. Background scheduler with priority-based task execution. Auto-embed hook pattern. Comprehensive command dispatch.

> **Key comparison:** SQLite refuses to open a corrupt database — the user is forced to acknowledge the problem. Strata logs a warning and opens with empty state, which means a corruption event during recovery silently discards all committed data.

---

### 2.6 executor — Command Dispatch

**Verdict:** Well-designed API layer with comprehensive typed command enum, but batch operation semantics are undocumented and the Output type has no compile-time enforcement.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| X-1 | Batch operations can partially succeed (not atomic, not documented) | **CRITICAL** | `handlers/kv.rs:180-265` | Redis `MSET` is atomic; DynamoDB `BatchWriteItem` documents partial success | #1319 |
| X-2 | Handler can return wrong Output variant — compiles without error | **CRITICAL** | `executor.rs:170-1313`, `output.rs:32-217` | — | #1319 |
| X-3 | Embedding write-behind consistency model undocumented | **HIGH** | `embed_hook.rs:110-173` | Elasticsearch documents NRT (near-real-time) behavior explicitly | #1319 |
| X-4 | Vector writes blocked inside transactions | **HIGH** | `session.rs:100-110` | — | #1326 |
| X-5 | Transaction commands split between Executor and Session (confusing) | **HIGH** | `executor.rs:858-864` | — | #1326 |
| X-6 | Embed buffer backpressure falls back to synchronous flush | **MEDIUM** | `embed_hook.rs:162-170` | — | #1326 |
| X-7 | Branch name resolution (UUID v5 hashing) undocumented | **MEDIUM** | `bridge.rs:93-105` | — | #1326 |

**Strengths:** Exhaustive typed command enum (70+ variants). Comprehensive input validation at API boundary. Stateless executor design (thread-safe by construction). `#[serde(deny_unknown_fields)]` rejects malformed JSON. Extensive test suite (3928 lines across 9 test files).

> **Key comparison:** Redis `MSET` is atomic — either all keys are set or none are. DynamoDB explicitly documents `BatchWriteItem` as non-atomic. Strata's batch operations are non-atomic but this is undocumented, which violates the principle of least surprise for users coming from SQL databases.

---

### 2.7 security — Access Control

**Verdict:** Minimal — binary read/write access mode only, no cryptography, no RBAC, no audit logging.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| SEC-1 | No encryption at rest — zero cryptographic implementation | **CRITICAL** | `lib.rs` (entire crate, 137 lines); `durability/src/codec/identity.rs` | SQLite has SEE/sqlcipher AES-256; RocksDB has env-level encryption | #1321 |
| SEC-2 | API keys stored as plain `String` — no zeroize, appears in debug | **HIGH** | `lib.rs:46-48` (`OpenOptions.model_api_key`) | Redis redacts `requirepass` in `CONFIG GET` | #1321 |
| SEC-3 | Binary ReadOnly/ReadWrite only — no per-collection or per-user RBAC | **MEDIUM** | `lib.rs:10-18` | Redis has per-command, per-key ACLs | #1321 |
| SEC-4 | Durability mode string validation missing | **MEDIUM** | `lib.rs` (durability method) | — | #1321 |

**Strengths:** None beyond basic type safety. **0 tests** in the crate.

> **Key comparison:** The durability crate's codec system (`codec/mod.rs`) explicitly reserves a slot for `AesGcmCodec` and uses `IdentityCodec` (no-op pass-through) today. The seam exists but no encryption is implemented. For mobile/edge embedded use cases, encryption at rest is table stakes.

---

### 2.8 intelligence — Embedding & Generation

**Verdict:** Functional but fragile under failure conditions — `OnceCell` caches errors permanently, and text extraction silently returns `None` on depth limit.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| I-1 | Model load failure cached permanently via `OnceCell` | **CRITICAL** | `embed/mod.rs:39-51` (test at lines 150-163 confirms) | Any production ML pipeline retries with backoff | #1318 |
| I-2 | Text extraction returns `None` on depth limit (indistinguishable from "no text") | **CRITICAL** | `embed/extract.rs:6-9` (`MAX_DEPTH=16`, `MAX_TEXT_LEN=1024`) | Elasticsearch rejects docs exceeding nesting limits with explicit error | #1318 |
| I-3 | No model hash/signature validation after download | **HIGH** | `embed/download.rs` | Production ML pipelines validate model checksums | #1327 |
| I-4 | Poisoned Mutex recovery uses potentially corrupted state | **HIGH** | `generate.rs:52, 66` | — | #1318 |
| I-5 | Unbounded model cache (`HashMap` with no eviction) | **MEDIUM** | `generate.rs` (`GenerateModelState`) | — | #1327 |
| I-6 | Batch embedding doesn't validate uniform output dimensions | **MEDIUM** | `embed/mod.rs:99-119` | — | #1327 |

**Strengths:** Comprehensive unit tests (14+ test functions). Lazy model loading (zero overhead for non-embedding workloads). Proper depth and text length limits in extraction (the issue is the error reporting, not the limit itself).

> **Key comparison:** A transient network error during model download permanently breaks inference for the lifetime of the process. Every production system retries with exponential backoff for transient failures.

---

### 2.9 search — Hybrid Search & Fusion

**Verdict:** Sophisticated design with weighted RRF fusion, but prompt injection risk in LLM-based query expansion and NaN scores can propagate silently.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| SR-1 | RRF fusion creates placeholder rank-0 hits before reassignment | **HIGH** | `fuser.rs:188`, `hybrid.rs:268, 285` | Standard RRF implementations validate rank > 0 | #1322 |
| SR-2 | User query injected directly into LLM prompt (prompt injection) | **HIGH** | `expand/prompt.rs:26-31` | Production RAG systems sanitize/escape user queries | #1322 |
| SR-3 | NaN scores not rejected; `partial_cmp()` falls back silently | **HIGH** | `fuser.rs:74-96`, `rerank/api.rs:96-104` | Lucene rejects NaN scores explicitly | #1322 |
| SR-4 | LLM response parser silently drops invalid lines | **HIGH** | `expand/parser.rs:36-37` | — | #1322 |
| SR-5 | Reranker partial failures silently keep original scores | **MEDIUM** | `rerank/api.rs:98-106`, `blend.rs:40` | Production rerankers validate output count | #1322 |
| SR-6 | Budget allocation doesn't account for vector search timing | **MEDIUM** | `hybrid.rs:335-351` | — | #1322 |
| SR-7 | RRF k=60 hardcoded, not configurable | **LOW** | `hybrid.rs:479` | — | #1322 |

**Strengths:** Excellent architecture documentation with ASCII diagrams. Comprehensive unit tests (60+ test functions). Feature-flagged HTTP dependencies (`expand` and `rerank` features). Weighted RRF fusion is well-designed. Sort determinism with EntityRef hash tie-breaking.

> **Key comparison:** Both the query expansion prompt (`expand/prompt.rs:29`) and reranker prompt (`rerank/prompt.rs:24-26`) embed raw user input and document content directly into LLM messages with no escaping. An attacker controlling document content in the database could craft snippets that manipulate LLM scoring.

---

### 2.10 cli — Command-Line Interface

**Verdict:** Thin wrapper over the executor. No independent structural concerns.

---

### 2.11 engine/graph — Graph Subsystem

**Verdict:** Zero unsafe code and zero production panics (excellent), but pervasive O(E) full-scans, unbounded result sets, and silent corruption swallowing.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| G-1 | `unwrap_or_default()` silently replaces corrupted edge/node data | **HIGH** | `graph/mod.rs:460, 501, 531, 564` | Neo4j/JanusGraph surface deserialization errors | #1315 |
| G-2 | `subgraph()` full-scans all edges O(E) then filters client-side | **HIGH** | `graph/traversal.rs:127-153` | Neo4j uses per-node index lookups for subgraph projection | #1316 |
| G-4 | BFS default `max_depth: usize::MAX` — unbounded traversal on connected graphs | **HIGH** | `graph/types.rs:193-202` | Neo4j/DGraph require explicit depth/result limits | #1316 |
| G-3 | `count_edges_by_type()` full-scans all edges per type — O(L * E) for summary | **MEDIUM** | `graph/ontology.rs:506-529` | Production graph DBs maintain per-type count metadata | #1316 |
| G-5 | `list_graphs()` scans entire `_graph_` namespace including all data | **MEDIUM** | `graph/mod.rs:88-108` | Production DBs maintain a graph catalog/registry | #1316 |
| G-6 | No max-length on node IDs, graph names, edge types, or entity_ref URIs | **MEDIUM** | `graph/keys.rs:19-56` | Neo4j limits label names to 65534 bytes | #1315 |
| G-7 | Edges allowed between non-existent nodes (dangling edges) | **MEDIUM** | `graph/mod.rs:348-382` | Neo4j requires nodes to exist before creating relationships | #1315 |
| G-8 | `on_entity_deleted()` cascade errors logged but not returned | **MEDIUM** | `graph/integrity.rs:47-55` | — | #1323 |
| G-11 | `delete_graph()` loads all graph data into memory in a single scan | **MEDIUM** | `graph/mod.rs:111-149` | Neo4j uses batched deletion | #1316 |
| G-12 | `snapshot()` loads entire graph into memory (all nodes + all edges) | **MEDIUM** | `graph/mod.rs:518-581` | Neo4j/DGraph stream results; never load full graph | #1323 |
| G-14 | Frozen ontology uses open-world semantics — undeclared types bypass all checks | **MEDIUM** | `graph/ontology.rs:307-309, 344-347` | — | #1315 |
| G-18 | `list_nodes()` and `list_graphs()` return unbounded `Vec<String>` | **MEDIUM** | `graph/mod.rs:88-108, 252-268` | All production graph DBs support pagination | #1316 |
| G-9 | `degree()` materializes all neighbors just to count them | **LOW** | `graph/traversal.rs:43-52` | Neo4j returns degree count without materialization | #1323 |
| G-10 | `AdjacencyIndex` allows duplicate edges and has no size bound | **LOW** | `graph/adjacency.rs:57-68` | — | #1323 |
| G-13 | Bulk insert does per-node KV read for ontology validation | **LOW** | `graph/mod.rs:633-635` | — | #1323 |
| G-15 | `freeze_ontology()` read-then-write is not atomic | **LOW** | `graph/ontology.rs:220-285` | Neo4j holds schema write lock for the entire operation | #1323 |
| G-16 | Property types recorded but not enforced, even when ontology is frozen | **LOW** | `graph/types.rs:22-30` | Neo4j property type constraints enforce at write time | #1323 |
| G-17 | Hand-rolled URI encoding/decoding is fragile | **LOW** | `graph/keys.rs:164-171` | — | #1323 |

**Strengths:** Zero `unsafe` code in 6,917 lines. Zero panics in production paths — all error handling uses `?` propagation. Comprehensive input validation on write paths. Atomic forward+reverse edge updates within a single transaction. Well-designed ontology lifecycle (Draft -> Frozen with cross-reference validation). Referential integrity hooks with configurable cascade policies (Cascade/Detach/Ignore). Extensive test coverage across all 9 files.

> **Key comparison:** The graph subsystem's safety profile is excellent — it has the best `unsafe`/panic discipline of any subsystem in Strata. The primary concern is performance: nearly every read operation involves a full prefix scan of the KV namespace, making performance O(total data) rather than O(relevant data). Production graph databases like Neo4j use native index structures (B+ trees, skip lists) that make traversal O(degree) per hop. At small scale this is invisible; at >100K edges, graph queries will become a bottleneck.

---

### 2.12 engine/vector — Vector Subsystem

**Verdict:** Sophisticated segmented HNSW architecture with strong determinism guarantees, but `unsafe` alignment checks disappear in release builds, production `panic!()` calls in the heap, and no SIMD or quantization for competitive performance.

| # | Finding | Severity | Code Reference | Comparison | Tracked |
|---|---------|----------|----------------|------------|---------|
| V-1 | `debug_assert!` guards alignment before `unsafe` pointer cast — disappears in release | **CRITICAL** | `vector/hnsw.rs:958-967` | Qdrant uses `safe_transmute` with runtime validation | #1317 |
| V-2 | f32 alignment not validated on mmap open; corrupt file causes UB | **HIGH** | `vector/mmap.rs:228` | pgvector uses typed storage guaranteeing alignment | #1324 |
| V-5 | `panic!("cannot mutate mmap-backed VectorHeap")` in production path | **HIGH** | `vector/heap.rs:268` | Production DBs never panic on data-plane operations | #1317 |
| V-6 | `panic!("raw_data() not available")` for Mmap and Tiered variants | **HIGH** | `vector/heap.rs:520-521` | — | #1317 |
| V-7 | `.expect()` in hot iteration path — panics on stale `id_to_offset` | **HIGH** | `vector/heap.rs:490, 501` | — | #1317 |
| V-13 | `delete()` removes from backend first, then KV — reversed from insert ordering | **HIGH** | `vector/store.rs:697-711` | Pinecone uses single transactional delete | #1317 |
| V-15 | `delete()` has no lock around check-then-mutate (TOCTOU race) | **HIGH** | `vector/store.rs:677-714` | Insert path correctly holds lock (citing issue #936) | #1317 |
| V-17 | No `fsync` before atomic rename of mmap files — crash can lose cache | **HIGH** | `vector/mmap.rs:334-338`, `mmap_graph.rs:152-156` | SQLite/RocksDB/Qdrant all fsync before rename | #1317 |
| V-19 | `debug_assert_eq!` for dimension mismatch in distance computation | **HIGH** | `vector/distance.rs:19-23` | FAISS uses runtime assertions; Qdrant validates at all layers | #1324 |
| V-23 | No SIMD distance computation — scalar f32 loops only | **HIGH** | `vector/distance.rs` | Qdrant/FAISS/Milvus all use AVX2/NEON SIMD | #1325 |
| V-25 | No quantization support — all vectors stored as full-precision f32 | **HIGH** | entire subsystem | Qdrant supports SQ/PQ/binary; Milvus supports IVF_SQ8/PQ | #1325 |
| V-27 | No pre-filtering for metadata — post-filter only with adaptive over-fetch | **HIGH** | `vector/store.rs:872-970`, `vector/filter.rs` | Pinecone/Qdrant/Weaviate support pre-filtering | #1325 |
| V-3 | `&[f32]` to `&[u8]` cast lacks endianness guard (mmap.rs only; mmap_graph.rs has it) | **MEDIUM** | `vector/mmap.rs:330` | — | #1324 |
| V-4 | Mmap lifetime relies on external file not being modified/deleted | **MEDIUM** | `vector/mmap.rs:124`, `mmap_graph.rs:176` | Qdrant uses advisory file locks | #1324 |
| V-8 | `.unwrap()` on `entry_point` after None check — fragile to refactoring | **MEDIUM** | `vector/hnsw.rs:508` | — | #1324 |
| V-9 | Thread pool `expect()` at first search — delayed crash under resource constraints | **MEDIUM** | `vector/segmented.rs:51` | — | #1324 |
| V-10 | Unbounded `inline_meta: BTreeMap` — always in anonymous memory | **MEDIUM** | `vector/heap.rs:101-103` | Qdrant stores metadata with disk backing | #1324 |
| V-11 | WAL embeds full vectors (~6KB per 1536-dim entry) | **MEDIUM** | `vector/wal.rs` | Milvus uses separate binlog for vector data | #1325 |
| V-12 | O(n) KV scan fallback for VectorId-to-key resolution | **MEDIUM** | `vector/store.rs:1167-1216` | — | #1325 |
| V-16 | `ensure_collection_loaded()` has TOCTOU window — two threads can double-init | **MEDIUM** | `vector/store.rs:1377-1406` | — | #1324 |
| V-18 | No parent directory fsync after rename | **MEDIUM** | same as V-17 | LevelDB/RocksDB fsync directory after rename | #1317 |
| V-20 | No NaN/Infinity validation in distance functions | **MEDIUM** | `vector/distance.rs` | Qdrant rejects NaN/Infinity at ingestion | #1324 |
| V-21 | Temporal search uses fixed 4x over-fetch (no adaptive retry like `search()`) | **MEDIUM** | `vector/store.rs:1026` | Pinecone/Weaviate apply filters during traversal (pre-filter) | #1325 |
| V-22 | `search()` doesn't validate query vector for NaN (but `search_at()` does) | **MEDIUM** | `vector/store.rs:842-987` | — | #1324 |
| V-24 | BTreeSet visited set in HNSW search — O(log n) vs HashSet O(1) | **MEDIUM** | `vector/hnsw.rs` | FAISS/Qdrant use flat bitsets or hash sets | #1325 |
| V-26 | Active buffer uses O(n) brute-force scan (seal threshold 50K) | **MEDIUM** | `vector/segmented.rs` | Qdrant builds small HNSW even for mutable segment | #1325 |
| V-28 | No sparse, multi-vector, or binary vector support | **MEDIUM** | entire subsystem | Qdrant/Weaviate/Milvus support sparse + multi-vector | #1325 |
| V-14 | Silent failure on `create_dir_all` and `flush_heap_to_disk_if_needed` | **LOW** | `vector/store.rs:1119-1121` | — | #1324 |
| V-29 | No per-collection namespace or tenant isolation | **LOW** | entire subsystem | Pinecone supports namespaces within an index | #1325 |
| V-30 | No index build progress reporting | **LOW** | `vector/segmented.rs` | Milvus reports index build progress | #1325 |

**Strengths:** Deterministic results by design — BTreeMap iteration, fixed RNG seeds, and facade-level tie-breaking produce byte-identical output across runs (rare among vector DBs). Temporal search (time-travel queries) is a unique feature not offered by Pinecone/Qdrant/Milvus. Graceful mmap degradation — corrupt or missing `.vec`/`.hgr` files fall back to full KV rebuild with zero data loss. KV-first insert pattern ensures durability before visibility. Slot reuse without ID reuse avoids phantom result bugs. Compile-time endianness guard in `mmap_graph.rs`. Data zeroing on delete prevents information leakage. Comprehensive `VectorError` enum with 15+ contextual variants.

> **Key comparison:** The most urgent issue is V-1 — a `debug_assert!` guarding an `unsafe` pointer cast at `hnsw.rs:958` disappears in release builds. This is the same class of bug as C-1 (JSON transmute) but in a hotter code path (every mmap-backed HNSW search). Beyond safety, the lack of SIMD (V-23) and quantization (V-25) means Strata's vector search is 4-8x slower and uses 4-32x more memory than Qdrant or FAISS at equivalent scale.

---

## 3. Cross-Cutting Comparison

### Feature Matrix

| Capability | SQLite | LMDB | RocksDB | Redis | DuckDB | **Strata** |
|-----------|--------|------|---------|-------|--------|------------|
| **Memory bounded** | Yes (page cache) | Yes (OS mmap) | Yes (block cache + memtable) | Yes (`maxmemory`) | Yes (buffer pool) | **No** |
| **Crash-safe WAL** | Yes (always) | Yes (mmap COW) | Yes (per-record CRC) | Configurable | Yes | **Partial** (Standard mode gap) |
| **Encryption at rest** | Yes (sqlcipher) | No (OS-level) | Yes (env encryption) | Yes (TLS + ACLs) | No | **No** |
| **Resource limits** | Yes (many PRAGMAs) | Yes (`max_dbs`, map size) | Yes (rate limiter, write stall) | Yes (`maxmemory`, `maxclients`) | Yes | **No** |
| **Snapshot cost** | O(1) (shared pages) | O(1) (COW) | O(1) (ref counting) | O(n) (BGSAVE fork) | O(1) | **O(n) (deep clone)** |
| **Automatic maintenance** | Yes (auto-checkpoint) | Yes (implicit via COW) | Yes (auto-compaction) | Yes (lazy + active expiry) | Yes (auto-checkpoint) | **No** (design doc exists) |
| **Health checks** | `PRAGMA integrity_check` | `mdb_env_stat()` | `GetProperty()` | `INFO`, `PING` | `PRAGMA database_list` | **None** |
| **Metrics / observability** | `sqlite3_status()` | `mdb_stat()` | 100+ `GetProperty()` metrics | 200+ `INFO` metrics | `PRAGMA database_size` | **Minimal** |
| **Max tested scale** | Terabytes | Terabytes | Petabytes | ~25 GB (memory) | Terabytes | **Unknown** |

### Five Things Every Production Embedded DB Has That Strata Doesn't

**1. Hard memory limits.** Every production embedded database lets you say "use at most X MB." SQLite has `PRAGMA cache_size`. Redis has `maxmemory` with six eviction policies. RocksDB has `block_cache` size and `write_buffer_size`. LMDB has a fixed map size. Strata has no bound on any data structure — memory grows linearly with keys, versions, branches, and vectors.

**2. Crash safety guarantee with documentation.** SQLite publishes exact data-loss windows for each `PRAGMA synchronous` level. Redis documents `appendfsync always|everysec|no` trade-offs. Strata's Standard mode defers fsync to a background thread with no health monitoring, and the data-loss window is undocumented. Users who call `commit()` and receive `Ok(())` reasonably expect their data is durable.

**3. Automatic resource reclamation.** SQLite auto-checkpoints the WAL at 1,000 pages. RocksDB auto-compacts when L0 files accumulate. Redis actively expires keys every 100ms. Strata does none of these — version chains, WAL segments, tombstones, and expired keys all grow without bound. A maintenance design doc exists (`docs/design/maintenance.md`) but is not yet implemented.

**4. Corruption detection and refusal.** SQLite refuses to open a corrupt database and returns `SQLITE_CORRUPT`. Redis can refuse to start with a corrupt AOF (`aof-load-truncated no`). Strata logs a warning and opens with empty state if recovery fails (`database/mod.rs:466-476`), silently discarding all committed data.

**5. Operational observability.** Redis exposes 200+ metrics via `INFO`. RocksDB has 100+ `GetProperty()` metrics covering compaction, cache hits, stall durations, and more. SQLite has `sqlite3_status()` for memory, page cache, and schema stats. Strata exposes only `SchedulerStats` (queue depth, active tasks) with no subsystem-level introspection.

---

## 4. Prioritized Remediation Roadmap

### Phase 1: Must-Fix Before Any Production Use (Weeks 1-4)

| # | Item | Crate | Rationale | Tracked |
|---|------|-------|-----------|---------|
| 1 | **Refuse to open on recovery failure** — return error instead of `RecoveryResult::empty()` | engine | Data loss is the worst possible outcome for a database. Silent data loss is worse — the caller doesn't even know it happened. | #1313 |
| 2 | **Add memory limits** — configurable max memory for version chains, branches, and vectors | storage | An embedded database sharing a host process cannot OOM the host. At minimum, document the growth model so users can plan. | #1306 |
| 3 | **Fix Standard mode fsync** — health-check the background flush thread, or sync inline | durability | Users who receive `Ok(())` from `commit()` expect durability. An unhealthy flush thread silently violates this contract. | #1310 |
| 4 | **Replace version counter `expect()` with error return** | concurrency | `expect()` on `checked_add` at `manager.rs:146` panics the entire host application. Return `Err(VersionOverflow)` instead. | #1307 |
| 5 | **Add compile-time layout assertion for JSON transmute** | core | The `unsafe` block at `json.rs:1028` relies on `#[repr(transparent)]` but has no `static_assert` verifying layout. UB is unacceptable in a database. | #1300 |
| 6 | **Promote HNSW mmap alignment check to runtime assertion** | engine/vector | `debug_assert!` at `hnsw.rs:958` guards an `unsafe` pointer cast but disappears in release builds. Same class as #5 — UB on misaligned data. | #1317 |
| 7 | **Replace `panic!()` calls in VectorHeap with error returns** | engine/vector | `heap.rs:268, 520-521` panic on mmap/tiered variant mismatch. Process crash on a data-plane operation is unacceptable. | #1317 |
| 8 | **Document isolation level** — "Snapshot Isolation; write-skew is possible" | concurrency | Users must know what guarantees they get. Every production database documents its isolation levels. | #1309 |
| 9 | **Fix model load permanent failure** — replace `OnceCell` with retry + backoff | intelligence | A transient network error during model download permanently breaks inference for the process lifetime. | #1318 |

### Phase 2: Should-Fix Before GA (Weeks 5-8)

| # | Item | Crate | Rationale | Tracked |
|---|------|-------|-----------|---------|
| 10 | **Implement automatic maintenance** (GC, compaction, TTL enforcement) | engine | Version chains, WAL segments, tombstones, and expired keys grow without bound. Design doc exists (`maintenance.md`). | #1303 |
| 11 | **Add transaction timeout and abandoned transaction detection** | concurrency | Leaked transactions pin the GC safe point forever. `is_expired()` helper exists at `transaction.rs:1038` but is never enforced. | #1308 |
| 12 | **Fix stale WAL lock detection** — validate PID, add timeout | durability | A crashed process leaves an orphaned lock file that blocks all other processes from opening the database. | #1310 |
| 13 | **Add encryption at rest** (AES-256 via the existing codec seam) | security | The `IdentityCodec` seam at `durability/src/codec/mod.rs` reserves a slot for `AesGcmCodec`. Required for mobile/edge use cases. | #1321 |
| 14 | **Add health check API** (`db.health()` returning subsystem status) | engine | Host applications need a liveness probe. No mechanism exists to detect a stuck WAL flush thread or hung recovery. | #1320 |
| 15 | **Document batch operation semantics** (non-atomic, partial success) | executor | `KvBatchPut` at `handlers/kv.rs:235` is non-atomic across branches. Users from SQL backgrounds assume atomicity unless told otherwise. | #1319 |
| 16 | **Fix shutdown coordination** — drain in-flight transactions before final flush | engine | The 30-second timeout at `database/mod.rs:1954` ignores in-flight transactions. Uncommitted work may be flushed in undefined state. | #1314 |
| 17 | **Fix vector delete ordering and add lock** — delete KV first (like insert), hold lock | engine/vector | `store.rs:697` deletes backend before KV (opposite of insert). `delete()` also lacks the TOCTOU lock that insert has. | #1317 |
| 18 | **Add fsync before mmap file rename** | engine/vector | `mmap.rs:334` and `mmap_graph.rs:152` call `flush()` but not `sync_all()` before rename. Crash can leave zero-length files. | #1317 |
| 19 | **Add per-subsystem memory metrics** (`Command::MemoryStatus`) | engine | Can't manage what you can't measure. Version chain sizes, shard counts, and vector heap usage should be queryable. | #1320 |

### Phase 3: Scale Readiness (Weeks 9-12)

| # | Item | Crate | Rationale | Tracked |
|---|------|-------|-----------|---------|
| 20 | **Replace `ClonedSnapshotView` with lazy/COW snapshots** | concurrency | O(n) snapshot cost at `snapshot.rs:69` limits concurrency. 100 MB branch with 100 concurrent transactions = 10 GB. `ShardedSnapshot` at `sharded.rs:892` is already O(1) — make it the default. | #1309 |
| 21 | **Implement KV page cache** (tiered storage Phase 2) | storage | Unbounded memory is the #1 scaling blocker. Design doc exists (`tiered-storage.md`). | #1291 |
| 22 | **Add constraint validation at all write boundaries** | core | `validate_value()` and `validate_key_length()` at `limits.rs` are separate checks not enforced at the storage layer. Defense in depth against data corruption. | #1299 |
| 23 | **Make vector Tiered mode the default** (spill embeddings to mmap) | engine | Easy win for memory reduction. Tiered mode already exists but InMemory is the default. | #1325 |
| 24 | **Auto-persist sealed HNSW graphs to `.shgr` files** | engine | Sealed HNSW segments are in-memory only. A crash after sealing loses the graph, requiring expensive rebuild from vectors. | #1317 |
| 25 | **Add SIMD distance computation** | engine/vector | Scalar f32 loops are 4-8x slower than AVX2/NEON. At >100K vectors this dominates search latency. Every production vector DB uses SIMD. | #1325 |
| 26 | **Add scalar/product quantization** | engine/vector | Full f32 storage uses 4-32x more memory than quantized representations. Required for >500K vectors in embedded contexts. | #1325 |
| 27 | **Replace graph full-scans with per-node adjacency lookups** | engine/graph | `subgraph()`, `count_edges_by_type()`, `delete_graph()`, and `snapshot()` all scan O(E_total). At >100K edges, graph queries become a bottleneck. | #1316 |
| 28 | **Add default BFS depth/node limits and graph pagination** | engine/graph | Default `max_depth: usize::MAX` and unbounded `list_nodes()` can OOM on large connected graphs. | #1316 |
