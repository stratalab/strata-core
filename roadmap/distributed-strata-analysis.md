# Distributed Strata: Architecture Analysis

Two-part analysis exploring what it would take to evolve Strata from an embedded single-process database into a sharded, multi-node, serverless system capable of supporting modern data workloads.

---

## Part 1: Sharded, Multi-Node, Serverless

### What Strata is today

Single-process embedded database. Local WAL, local segments, local compaction. Everything shares one address space. Branching and time-travel are implemented via MVCC on a single-node LSM. This is a strength (zero network hops, zero serialization overhead) but it's fundamentally a single-machine architecture.

### The layers that would need to change

#### 1. Storage: disaggregate compute from storage

This is the prerequisite for everything else. The pattern is well-established now (Aurora, Neon, ClickHouse Cloud, TiKV):

- **Segments go to object storage** (S3/R2). Strata's segments are already immutable once sealed — that's the easy part. The hard part is the WAL. You'd need either a distributed shared log (Corfu/DELOS-style) or a per-shard WAL that replicates before acknowledging writes.
- **Page cache becomes a networked tier.** Compute nodes hold hot segments in memory, fetch cold ones from object storage. This is where Strata's mmap layer gets replaced with an async fetch + local LRU cache. Latency goes from ~100ns (local mmap) to ~1ms (network page fetch) to ~50ms (S3 cold read). That's 3-5 orders of magnitude — the entire performance profile of every primitive changes.
- **Compaction becomes a background service.** Separate compaction workers that read segments from object storage, merge, write back. This is actually easier than the current in-process model because you don't compete with foreground queries for CPU/memory.

#### 2. Sharding: each primitive has a different natural partition key

This is where it gets interesting because Strata isn't a single data model — it's five or six:

| Primitive | Natural partition | Hard problem |
|-----------|------------------|-------------|
| KV | Hash(key) or range(key) | Cross-shard transactions |
| JSON | Hash(doc_id) | Path queries that span docs |
| Events | Partition by stream | Ordered delivery across partitions |
| State | Hash(key) | Merge semantics across shards |
| Vectors | ??? | ANN search requires scatter-gather across ALL shards |
| Graph | ??? | Graph partitioning is NP-hard; any cut creates cross-shard edges |
| Search | Index sharding (like Elasticsearch) | Index builds need to see all data |

**Graph is the nightmare.** In a sharded graph, every edge that crosses a partition boundary is a network round-trip. The adjacency index trick only works when the full graph fits on one node. At scale you'd need something like:
- PowerGraph-style vertex-cut partitioning (replicate high-degree vertices across shards)
- Or give up on single-hop latency and do asynchronous/batch traversals (Pregel/BSP model)
- Or accept that graph stays single-shard per graph instance (practical for most graphs under 100M edges)

**Vectors are almost as hard.** ANN search is inherently global — you need to check every shard to guarantee recall. The options are:
- Scatter-gather to all shards, merge top-K results (simple, high fan-out)
- A routing layer that knows which shards likely contain nearest neighbors (IVF-style clustering at the shard level)
- Accept higher latency for distributed vector search vs. keeping vector indices per-tenant on single nodes

#### 3. Branching & time-travel: the hardest distributed problem

This is Strata's most distinctive feature and the one that's hardest to distribute.

**Branching today:** A branch is a logical fork in the MVCC version chain. Creating a branch is O(1) — it's just a new pointer into the version history. Reads on a branch see the snapshot at fork time plus any branch-local writes.

**Branching distributed:** Now a branch needs to be a consistent snapshot across ALL shards simultaneously. This requires:

- **Globally synchronized timestamps.** Either TrueTime (hardware atomic clocks, Google-only), HLC (hybrid logical clocks, what CockroachDB uses), or an epoch-based scheme. Branch-create becomes "take a snapshot at global timestamp T across all shards."
- **Cross-shard snapshot isolation.** Every read on a branch needs to see exactly the data that existed at fork-time across every shard. This is basically distributed MVCC — CockroachDB and Spanner both solve this, but it's the hardest part of their systems.
- **Branch-local writes need coordination.** If a branch writes to shard A and shard B, and then merges back to main, you need distributed conflict detection. This is analogous to distributed merge in git — but for structured data with schema constraints.

**Distributed branching at the level Strata currently supports is a research-grade problem.** No production database does branches + time-travel + multi-model + distributed. Pragmatic compromises would be needed:
- Branches are shard-local by default (branch a single graph, not the entire database)
- Cross-shard branches exist but with restricted semantics (read-only snapshots, no cross-shard branch writes)
- Time-travel uses HLC timestamps with bounded staleness

#### 4. Coordination & transactions

- **Metadata service.** Shard map, schema registry, branch catalog. This needs to be highly available (Raft/Paxos replicated). etcd or a custom Raft group.
- **Distributed transactions.** KV writes that span shards need 2PC or something better (Percolator-style, like TiDB). For the common case of single-shard writes, you avoid this entirely — which is why good shard key design matters enormously.
- **Query routing.** A stateless proxy layer that parses requests, determines which shards are involved, fans out, and merges results. For simple KV ops this is trivial. For graph BFS or vector search, it's a distributed execution engine.

#### 5. Serverless: scale-to-zero and back

The disaggregated storage from step 1 is what enables this:

- **Compute nodes are stateless** (or soft-state — they cache segments but can reconstruct from object storage). This means they can scale to zero and cold-start by fetching segments on demand.
- **Cold start latency** is the killer. First query after scale-to-zero needs to: resolve shard map, connect to storage, fetch relevant segments, build in-memory indices. For vectors, rebuilding the HNSW index from segments could take seconds. You'd need pre-computed index files in object storage (which the mmap graph layer kind of already does).
- **Billing model** changes everything about optimization priorities. In serverless, you optimize for total bytes scanned and compute-seconds, not throughput. This favors columnar storage, aggressive pruning, and caching — different trade-offs than the current embedded model.

### Feasibility assessment

| Layer | Feasibility | Precedent |
|-------|------------|-----------|
| Disaggregated storage | **Doable** (6-12 months) | Neon, Aurora, ClickHouse Cloud |
| KV/JSON/Events sharding | **Doable** (6-12 months) | FoundationDB, TiKV, DynamoDB |
| Distributed vector search | **Hard but solved** (12+ months) | Milvus, Pinecone, Qdrant Cloud |
| Distributed graph | **Very hard** (12-18 months) | Neptune, Neo4j Fabric, Dgraph |
| Distributed branching | **Research-grade** | Nothing does this well |
| Serverless scale-to-zero | **Doable once storage is disaggregated** | Neon, PlanetScale, Turso |

### The honest take

The thing that makes Strata interesting — multi-model with branching and time-travel — is also the thing that makes distribution hardest. A sharded KV store is well-understood. A sharded vector DB is mostly solved. But a sharded multi-model database where you can branch the entire state of KV + graph + vectors + search atomically and then merge it back? That's a new thing.

The pragmatic path would probably be:

1. Disaggregate storage (segments to S3, stateless compute)
2. Single-shard-per-tenant serverless (like Turso/Neon — each tenant gets a single logical node, but it can sleep and wake)
3. Shard KV/JSON/Events horizontally (well-understood)
4. Keep graph and vectors single-node per instance (good enough for most workloads)
5. Branching works per-shard, cross-shard snapshots are read-only

Steps 1-2 get you "serverless" without the distributed systems nightmare. Steps 3-5 get you horizontal scale for the primitives where it matters. And you'd preserve the single-node performance story for workloads that fit on one machine — which is most of them.

---

## Part 2: Server Mode

Strata today is a library — you link it into your Rust process and call functions directly. There's no wire protocol, no authentication, no connection management. The CLI is a REPL that instantiates the engine in-process. Going server mode is the prerequisite for everything else. You can't do multi-node without a network boundary.

### Wire protocol

| Approach | Pros | Cons |
|----------|------|------|
| gRPC + protobuf | Strongly typed, streaming, codegen for every language, good for internal service mesh | Verbose proto definitions, harder to curl/debug |
| HTTP/JSON REST | Universal client support, easy to debug, familiar | No streaming (without SSE/WebSocket), verbose on the wire |
| HTTP + MessagePack/CBOR | Compact binary with HTTP ergonomics | Less tooling than JSON, clients need codec |
| Custom binary protocol | Maximum performance, minimal overhead | Have to build everything from scratch, no ecosystem |
| Postgres wire protocol | Instant compatibility with every SQL tool, driver, ORM on the planet | Strata isn't SQL — you'd be fighting the protocol's assumptions |

For a multi-model database with branching semantics, **gRPC is probably the right primary protocol** — it handles streaming naturally (important for event subscriptions, large scan results, vector search streams), the type system maps well to Strata's multi-primitive API, and you get client libraries for free. Then add a thin REST/JSON gateway on top for curl-ability and browser access.

### The API surface

The current Rust API is ~50-60 methods across primitives. As a network API, you need to think about:

**Resource model.** Something like:
```
/{database}/branches/{branch_id}/kv/{key}
/{database}/branches/{branch_id}/graph/{graph_name}/nodes/{node_id}
/{database}/branches/{branch_id}/vectors/{collection}/search
/{database}/branches/{branch_id}/events/{stream}
```

Branch is a first-class URL component — every operation is implicitly scoped to a branch. That's a strong design choice that preserves Strata's branching model at the API level.

**Batch and transaction semantics.** Single-key ops are straightforward RPCs. But Strata supports transactions (the `db.transaction(branch_id, |txn| { ... })` closure pattern). Over the wire, you need either:
- Interactive transactions (client opens a transaction, sends N operations, commits/aborts) — requires server-side session state, pinned connections
- Bundled transactions (client sends a batch of operations as a single request, server executes atomically) — stateless, simpler, but less flexible

Interactive transactions are needed for read-modify-write patterns. Most distributed databases support both (Spanner has both, FoundationDB is interactive-only, DynamoDB is batch-only via TransactWriteItems).

### Authentication & authorization

The embedded library has none — the caller is trusted by definition. Server mode needs:

- **AuthN:** API keys, JWT/OAuth2 tokens, mTLS for service-to-service.
- **AuthZ:** Per-database, per-branch, per-primitive permissions. At minimum: read, write, admin. Branch-create is a distinct permission (you don't want every reader to be able to fork the database). This maps naturally to a role-based model.
- **Multi-tenancy:** If you're going serverless, you need tenant isolation. Each tenant gets their own database(s), isolated storage, resource limits.

### Connection management & session state

- **Connection pooling.** gRPC uses HTTP/2 multiplexing, so fewer connections needed than traditional connection-per-query models. But you still need to manage backpressure, per-connection memory budgets, and graceful draining on shutdown.
- **Cursor/iterator state.** `scan_prefix` currently returns a `Vec`. At scale, that's untenable — you need server-side cursors or streaming responses. gRPC server streaming fits perfectly here.
- **Event subscriptions.** The Events primitive implies long-lived streaming connections (subscribe to a stream, receive new events as they arrive). This is a fundamentally different connection lifecycle than request-response.

### What changes in the engine

This is the part people underestimate. "Just add a network layer" sounds simple, but:

**Async all the way down.** The current engine is synchronous — `transaction()` takes a closure, blocks, returns. A server needs to handle thousands of concurrent connections, which means `tokio` + async. This doesn't mean rewriting the storage engine to be async (it shouldn't be — disk I/O should use blocking threadpools). It means the request handling, routing, connection management, and response serialization are all async, with a sync-to-async boundary at the engine call site.

**Cancellation.** When a client disconnects mid-query, the server needs to abort work. Today there's no cancellation — a `scan_prefix` runs to completion. In server mode, you need cancellation tokens threaded through long-running operations (large scans, BFS traversals, vector searches).

**Resource limits.** An embedded library trusts the caller not to OOM the process. A server needs per-query memory limits, per-connection timeout enforcement, and backpressure on the write path. Without this, a single bad query takes down all tenants.

**Observability.** Structured logging, distributed tracing (OpenTelemetry), metrics (request latency histograms, active connections, bytes scanned, cache hit rates). The embedded library has none of this — it doesn't need it. The server can't operate without it.

### Sequencing

If sequencing server mode against the distributed work:

```
1. Server mode (gRPC + REST gateway)          <- unlocks everything
2. Auth + multi-tenancy                        <- required for any hosted offering
3. Disaggregated storage (segments to S3)      <- prerequisite for serverless
4. Serverless scale-to-zero                    <- single-node-per-tenant, sleeping/waking
5. Horizontal sharding                         <- only when single-node is insufficient
```

Steps 1-2 get you a hosted single-node database (like early PlanetScale or Turso). Steps 3-4 get you serverless. Step 5 is where the distributed systems complexity kicks in. Most workloads never need step 5 — a single beefy node with disaggregated storage handles a surprising amount.
