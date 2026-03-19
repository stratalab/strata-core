# Library Hardening

Improvements that strengthen Strata as an embedded library under real workloads — large datasets, concurrent access, long-running AI pipelines, resource-constrained environments. None require a network boundary or distributed coordination.

**Timing:** After benchmark sanity work is completed.

---

## 1. Streaming iterators

`scan_prefix` returns a `Vec` — the entire result set materializes in memory before the caller sees a single item. For a graph with 5M edges or a KV scan over 100K keys, that's a needless memory spike.

**Change:** Cursor/iterator-based API (`fn scan_prefix(&self, ...) -> impl Iterator<Item = ...>`) that lets callers process results incrementally, stop early, and stay within a memory budget.

**Impact:** Every primitive benefits. `all_edges()` currently collects into a Vec before building the adjacency index. Event stream reads, KV range scans, node listing — all have the same pattern of "collect everything, then process." Streaming fixes all of them.

## 2. Cancellation

AI workloads have timeouts. An embedding pipeline calls vector search, the user navigates away, the application wants to abort. Today there's no way to cancel a scan or BFS mid-flight — it runs to completion.

**Change:** Thread a lightweight cancellation token (even just an `AtomicBool`) through long-running operations. Check it periodically in hot loops (every N iterations of a scan, every BFS level, every ANN search candidate).

**Impact:** Makes Strata a better citizen in async applications. Cheap to implement — the check is a single atomic load, branch-predicted to not-cancelled.

## 3. Per-operation memory budgets

A query like "BFS the entire graph" or "scan all events" can consume unbounded memory. Even embedded, the library shouldn't let one call OOM the host process.

**Change:** A soft limit — "abort this operation if it would allocate more than N bytes." Configurable per-operation or as a global default. Return an error (not a panic) when exceeded.

**Precedent:** SQLite has `SQLITE_LIMIT_LENGTH`, RocksDB has `block_cache` size limits.

**Impact:** Defensive hygiene. Particularly important for vector search (large result sets), graph traversals (unbounded BFS), and bulk scans.

## 4. Observability hooks

Not logging — callbacks. Let the application register a trait impl that gets called with structured events.

**Change:** Define an `EventListener` trait with methods like:
- `on_operation(op_type, duration, bytes_read, bytes_written)`
- `on_cache_access(hit: bool, segment_id)`
- `on_compaction(segments_in, segments_out, bytes_reclaimed)`
- `on_transaction(branch_id, ops_count, duration)`

The application routes these to its own metrics system (Prometheus, OpenTelemetry, whatever). Zero cost when no listener is registered.

**Precedent:** RocksDB's `Statistics` and `EventListener`, SQLite's `sqlite3_profile`.

**Impact:** Invaluable for performance tuning. Today the only way to understand Strata's internal behavior is to read the source code and add ad-hoc timing.

## 5. Segment-level encryption at rest

Even embedded, the data is on disk. If Strata segments are encrypted with a key the application provides at open time, you get encryption at rest with no full-disk encryption dependency.

**Change:** Encrypt segment data with AES-256-GCM (or XChaCha20-Poly1305) using a key derived from a caller-provided secret. Each segment gets its own nonce. The WAL is also encrypted. Key is never persisted — the application provides it on every open.

**Impact:** Matters for desktop apps (like StrataFoundry), edge deployments, mobile, and any context where the device might be lost or compromised. No OS-level trust required.

## 6. Disaggregated segment mindset

Not actually sending segments to S3, but treating them as independently addressable, self-contained units.

**Change:** Each segment carries its own metadata (bloom filters, min/max keys, schema version), can be memory-mapped or loaded independently, and can be shared read-only between processes.

**Impact:**
- **Multi-process read access.** Multiple processes can open the same Strata database read-only (useful for analytics alongside a write process).
- **Lazy segment loading.** Only load/mmap the segments a query actually touches instead of opening everything at startup. Reduces cold-start time for large databases.
- **Portable segments.** A segment is a self-describing file that can be copied, backed up, or inspected independently.

---

## Sequencing

These are roughly ordered by impact and dependency:

```
1. Streaming iterators        (highest impact, unblocks memory budgets)
2. Cancellation               (low effort, high value for AI workloads)
3. Per-operation memory budgets (builds on streaming iterators)
4. Observability hooks         (independent, can parallelize)
5. Encryption at rest          (independent, can parallelize)
6. Disaggregated segments      (largest scope, benefits from 1-4 being done)
```

Items 1-3 are tightly related and should be done together. Items 4-5 are independent and can be built in parallel. Item 6 is the largest effort and benefits from the foundation laid by 1-4.
