# Synchronizing embedded databases across edge fleets

**No production system today implements Git-style branch-and-merge for edge database fleets, but the theoretical foundations and building blocks now exist.** The gap between CRDT-based sync (automatic convergence, limited expressiveness) and branch-based sync (full expressiveness, requires coordination) is precisely characterized by the I-Confluence theorem — and hybrid architectures combining both are emerging as the practical answer. This survey maps the full landscape: from mature CRDT libraries and local-first sync engines to version-controlled databases and offline-first IoT systems, covering architecture details, sync protocols, conflict resolution, and production readiness across **19 systems and frameworks** as of early 2026.

The embedded database sync space has reached an inflection point. Automerge 3.0 achieved a **500× memory reduction** for large documents. ElectricSQL pivoted entirely away from built-in CRDTs toward a read-path-only HTTP streaming engine. MongoDB deprecated Atlas Device Sync after failing to unify Realm's sync protocol with its cloud stack. Meanwhile, Dolt reached **MySQL performance parity** while offering full Git semantics for SQL data. These shifts reveal a maturing field where the tradeoffs between approaches are becoming clearer and more consequential.

---

## CRDTs applied to database synchronization

Four major systems bring CRDT semantics to database sync, each with fundamentally different architectures.

**Automerge** (Ink & Switch, v3.0 released 2025) implements a JSON CRDT with a Rust core compiled to WebAssembly. Its sync protocol uses **Bloom filters** to efficiently identify missing changes between peers, typically converging in 3–4 rounds over any ordered stream (WebSocket, WebRTC, even USB drives). Version 3.0's switch to in-memory columnar compression reduced memory from **700 MB to 1.3 MB** for a Moby Dick–sized document and cut load times from 17 hours to 9 seconds. Automerge retains full operation history as a DAG, enabling time-travel and branching — but this means tombstones accumulate permanently, and document size grows with edit history rather than current content.

**Yjs** (Kevin Jahns) dominates production adoption with **900K+ weekly npm downloads**. Built on the YATA CRDT algorithm, it uses a simpler sync protocol than Automerge: clients exchange compact state vectors (`Map<clientId, clock>`), compute diffs, then stream incremental updates. Updates are commutative, associative, and idempotent — applicable in any order, multiple times. Yjs is the fastest JavaScript CRDT implementation in benchmarks and powers JupyterLab, Typst, and numerous collaborative editors through bindings for ProseMirror, CodeMirror, TipTap, Quill, and Monaco. Its limitation is similar to Automerge: **tombstones grow monotonically** without garbage collection.

**CR-SQLite** (Matt Wonlaw, Vulcan Labs) takes a different approach — it's a runtime-loadable **SQLite extension** that adds CRDT capabilities to existing tables. Calling `SELECT crsql_as_crr('table_name')` upgrades a table to a "Conflict-free Replicated Relation," creating metadata tables and triggers that track per-column versions. Each column independently uses LWW registers, counters, or fractional indexes for ordering. The changeset protocol is elegantly SQL-native: `SELECT FROM crsql_changes WHERE db_version > ?` extracts changes; `INSERT INTO crsql_changes` applies them. Fly.io's **Corrosion** project uses CR-SQLite for gossip-based cluster sync. However, development slowed significantly after Wonlaw joined Rocicorp (makers of Replicache/Zero) in late 2024, and the project remains **pre-production quality** at v0.16.x.

**ElectricSQL** underwent a **complete architectural rebuild** in July 2024. The legacy system (v0.x) was a bidirectional Postgres↔SQLite sync engine with rich CRDT conflict resolution. The current system (v1.0 GA, March 2025) is radically simpler: a read-path-only Elixir service that streams Postgres changes to clients via HTTP using "shapes" (partial table replicas matching WHERE clauses). It contains **no built-in CRDTs** — writes flow through the developer's own backend API. This pivot reflects a pragmatic lesson: building a fully-featured CRDT sync layer proved too complex to ship reliably. ElectricSQL now scales to **1 million concurrent clients** on commodity Postgres, and CRDTs are positioned as an optional composable layer via their new Durable Streams protocol (December 2025).

| System | CRDT approach | Sync protocol | Conflict resolution | Maturity |
|--------|-------------|--------------|-------------------|----------|
| Automerge 3.0 | JSON CRDT (document) | Bloom filter exchange, 3–4 rounds | Lamport timestamps + actor ID | Production-ready |
| Yjs | YATA CRDT (shared types) | State vector + diff, 2 rounds | YATA ordering for sequences, LWW for maps | Production (widely deployed) |
| CR-SQLite | Per-column CRDTs on SQL tables | SQL changeset queries | LWW, counters, fractional index per column | Pre-production, development slowed |
| ElectricSQL v1.x | None in core (read-path only) | HTTP long-polling with offsets | Deferred to application layer | GA (March 2025) |

---

## The local-first movement has matured from manifesto to ecosystem

The **"Local-first software"** paper (Kleppmann, Wiggins, van Hardenberg, McGranaghan; Onward! 2019) articulated seven ideals: fast, multi-device, offline, collaboration, longevity, privacy, and user control. The first four map directly to database sync patterns; the latter three — requiring decentralization, E2EE, and open standards — remain the hardest to achieve. By 2024, Kleppmann himself acknowledged these ideals "aren't really a good definition" but rather describe end-user benefits. His refined definition: **"In local-first software, the availability of another computer should never prevent you from working."**

Ink & Switch has continued pushing boundaries beyond the manifesto. **Peritext** (CSCW 2022) solved collaborative rich text editing for CRDTs. **Keyhive** (2024–2025) addresses the long-standing access control problem with a novel capability model for CRDTs featuring coordination-free revocation and end-to-end encryption. The **Beelay** sync protocol integrates Keyhive with Automerge, enabling encrypted sync where servers cannot decrypt payloads — a pre-alpha open-source release is on GitHub.

Two breakthrough papers from Kleppmann's group deserve attention. **Eg-walker** (EuroSys 2025, Best Artifact Award) avoids maintaining CRDT state in memory entirely, instead replaying an event graph on demand — consuming an **order of magnitude less memory** than existing CRDTs in steady state, with document loading orders of magnitude faster. **The Art of the Fugue** (IEEE TPDS, November 2025) minimizes interleaving anomalies in collaborative text editing and is now used in the Loro CRDT library.

The ecosystem has bifurcated into **sync engines** (PowerSync, ElectricSQL, Zero) that layer over existing Postgres backends, and **full-stack local-first solutions** (Jazz, Triplit, Loro) that provide the complete data layer. Notable new entrants include **Loro** (high-performance Rust CRDTs, 5.2K GitHub stars), **Jazz** (collaborative values framework with React integration), **Zero** (from the Replicache team), and **LiveStore** (reactive layer for Electric and other providers). The standard 2026 pattern: primary reads and writes happen locally against SQLite or CRDTs, background sync pushes to server, and conflict resolution happens via CRDTs or LWW.

---

## Edge-cloud database tiering varies from single-writer to full multi-master

**Turso/libSQL** extends SQLite with network replication through "embedded replicas" — a local SQLite file that syncs with a remote primary database. Reads execute locally with **microsecond latency**; writes are forwarded to the primary. Sync operates on 4 KB frames mapped to WAL pages: replicas request missing frames by number, apply them sequentially, and maintain consistency via generation IDs and checksums. Local offline writes entered beta in March 2025. The single-writer model avoids conflicts entirely but constrains write availability. Turso is best suited for per-user databases at the edge, not shared multi-writer scenarios.

**LiteFS** (Fly.io) used a FUSE filesystem to transparently intercept SQLite I/O and replicate transactions as custom LTX files across a cluster. It enforced single-writer semantics with Consul-based leader election. While architecturally elegant — rolling checksums detected split-brain, and replication latency was subsecond — FUSE overhead capped write throughput at **~100 transactions/second**. Critically, **LiteFS Cloud was shut down in October 2024**, and Fly.io now explicitly states they cannot provide support. The open-source project remains available but is effectively unmaintained.

**PouchDB/CouchDB** implements the most mature multi-master sync protocol in this category. The CouchDB Replication Protocol operates over standard HTTP REST: clients read the `_changes` feed, compute revision differences via `_revs_diff`, fetch missing documents with full revision history, and apply them via `_bulk_docs`. CouchDB's distinctive conflict model uses a **revision tree** where conflicts are detected but never discarded — a deterministic algorithm (longest revision history, then lexicographic hash sort) picks a winner independently on every node without coordination. Losing revisions are preserved in `_conflicts` and can be resolved by the application. PouchDB 9.0.0 (May 2024) brought significant stability improvements, and the project is now under **Apache Incubation**. The main limitations are no partial replication (filter functions are inefficient) and potential accumulation of unresolved "zombie" conflicts.

**PowerSync** handles Postgres-to-SQLite sync through a server-authoritative architecture descended from **15 years of production sync technology** at JourneyApps. The PowerSync Service connects to Postgres via logical replication and streams changes to clients organized into **buckets** — partitions defined by sync rules that control which rows reach which users. Writes go directly to local SQLite (zero latency), then flow through an upload queue to the developer's backend API, which commits to Postgres. Changes propagate back through CDC to all subscribed clients. Write checkpoints ensure clients see their own mutations reflected before applying new server state. PowerSync claims **zero incidents of data loss** since its original 2009 release. Recent developments include Sync Streams (improved developer experience replacing YAML-based sync rules), MongoDB/MySQL backend support, and prioritized sync for large datasets. The main tradeoff: developers must implement their own backend upload logic, though CloudCode now provides turnkey templates.

---

## Realm Sync's lifecycle is a cautionary tale for sync platform design

MongoDB acquired Realm in 2019 for $39 million and attempted to build a seamless edge-to-cloud sync pipeline: Realm database on-device → Realm Sync server → MongoDB Atlas. The sync protocol used **changeset-based operational transformation** over WebSocket connections with zlib-compressed deltas. Four conflict resolution rules governed merges: deletes always win, last-write-wins for scalar fields, intelligent list merging preserves both insertions, and all replicas converge to identical state.

**What worked**: offline-first simplicity was genuinely praised — developers wrote local Realm objects and sync happened automatically. The object-oriented data model with live, auto-updating objects made reactive UI development easy. Built-in authentication and role-based access control reduced backend work. Specialized Data Ingest mode handled write-heavy IoT workloads efficiently.

**What failed**: The "deletes always win" rule caused **unexpected data loss** when one user deleted a shared object while another created relationships to it offline. List ordering could permanently diverge across devices with no fix short of reinstalling the app. Schema migration was the deepest pain point — destructive changes (field renaming, type changes, optionality changes) required "reinitializing sync," which risked data loss for offline users. Developers accumulated schema cruft by never making breaking changes. Reported latency increased from real-time to **~10 seconds** after the migration to MongoDB's infrastructure. The sync server was never open-sourced because it was "deeply intertwined with the control plane."

**MongoDB deprecated Atlas Device Sync in September 2024** (EOL September 2025), recommending migrations to PowerSync, Ditto, ObjectBox, or Couchbase Mobile. The architectural lessons are clear: **don't tightly couple the sync engine to proprietary databases on both ends**; use standard formats (SQLite) at the edges. **Make schema migration a first-class problem** — support additive and breaking changes without client resets. **Keep the sync layer thin and composable** rather than building a monolithic platform. And critically, **design the sync server for open-source-ability from the start** — when MongoDB deprecated the service, users were stranded with no self-hosted alternative.

---

## Git-like push/pull semantics: Dolt is the strongest candidate for branch-based edge sync

**Dolt** (DoltHub) is the only production SQL database that implements full Git semantics — branch, merge, push, pull, clone — as first-class primitives. Built on a **Prolly Tree** (probabilistically balanced Merkle tree with content-defined chunk boundaries), Dolt stores data as a Merkle DAG where each commit points to a root hash representing the entire database state. Adding one row to a 50-million-row table stores only the incremental change via structural sharing.

Dolt's three-way merge operates at **cell-level granularity** — far finer than Git's line-based diffs. The merge algorithm finds the common ancestor commit, computes diffs from ancestor to each branch, and auto-merges changes to different tables, different rows, or different columns of the same row. Only same-cell modifications to different values produce conflicts, which are stored in `dolt_conflicts_<table>` system tables with base/ours/theirs values. A July 2025 tree-aware merging optimization walks differing tree nodes rather than iterating all changed rows, making large non-overlapping merges extremely fast. As of December 2025, **Dolt has reached MySQL performance parity** on sysbench benchmarks (~0.99× multiplier), down from 12× slower in 2021. Doltgres (Postgres-compatible) remains ~7× slower than Postgres.

**lakeFS** brings Git semantics to object storage (S3, GCS, Azure) for data lakes. Using a two-level Merkle tree of SSTables, it provides instant zero-copy branching, three-way merge at the object/file level, and diff computation proportional to the size of the difference. However, lakeFS is **format-agnostic** — it cannot merge row-level changes within Parquet files, only whole-file conflicts. Conflict resolution is limited to `source-wins` or `dest-wins` flags. Production users include Arm, Bosch, Lockheed Martin, NASA, and Netflix. lakeFS acquired DVC in November 2025 and raised $20M in July 2025.

**Neon** provides instant copy-on-write database branching for Postgres via separated compute and storage, but **branches cannot be merged** — this is explicitly intentional. Neon recommends migration scripts to apply schema changes from branches back to main. Similarly, **PlanetScale** offers three-way schema merge for MySQL branches but not data merge. **TerminusDB** provides full Git-like operations for graph data with delta-encoded triple-level diffs, making it a lightweight option for knowledge graph versioning.

**Has anyone built edge-devices-on-branches with three-way merge?** No production system explicitly implements this pattern as a first-class feature. Dolt is the strongest foundation — each edge device could clone a database, operate on its own branch offline, commit locally, and push/pull to sync. Cell-wise three-way merge resolves conflicts at reconvergence, and content-addressed storage ensures only changed chunks transfer. The February 2026 addition of Git remote support (syncing through standard Git repositories) further simplifies infrastructure. The limitation: Dolt is a full database server (~100 MB Go binary), not an embedded database like SQLite, making it viable for edge servers and laptops but heavy for constrained IoT devices.

---

## The theoretical boundary between branch-based and CRDT-based sync is precisely characterized

The **I-Confluence theorem** (Bailis et al., VLDB 2015) is the key formal result: a set of transactions can execute without coordination while maintaining an invariant I *if and only if* for all reachable states S₁ and S₂, their merge also satisfies I. This directly characterizes the expressiveness boundary:

- **CRDTs can express exactly the I-Confluent operations.** Counters, sets, sequences, registers, and any operation where merging two valid states always produces a valid state.
- **CRDTs fundamentally cannot maintain**: non-negative balances, uniqueness constraints, exclusive ownership, referential integrity, or any global predicate requiring knowledge of all replicas.
- **Branch-based sync can express strictly more**, because human-in-the-loop merge resolution can enforce non-I-Confluent invariants. The inclusion is strict — uniqueness and balance constraints witness the separation.

The tradeoff is precise: **branch-based sync trades automation for expressiveness; CRDT-based sync trades expressiveness for automation.** Branch merge may *fail* (producing unresolvable conflicts requiring human intervention), which is a feature for data curation but a limitation for automated edge sync. CRDTs guarantee convergence but cannot ensure the converged state reflects user intent — the classic interleaving anomaly where "Hello Alice!" and "Hello Charlie!" merge to garbled text illustrates this fundamental limit.

**Operational transform** (used by Google Docs, formerly Realm Sync) provides strong intent preservation via a central server but requires coordination. Raph Levien (Google) demonstrated that OT systems satisfying the TP2 correctness property are mathematically equivalent to CRDTs — they compute over the same join-semilattice via different paths. OT has fallen out of favor for new systems because TP2 is notoriously hard to satisfy correctly, and CRDTs offer superior offline and peer-to-peer support.

Three conflict resolution approaches compared in practice:

- **Three-way merge** (Dolt, lakeFS): explicit merge points, full audit trail, domain-specific resolution logic, but requires coordination and may produce unresolvable conflicts. Bandwidth-efficient via content-addressed diffs. Best for human-scale collaboration with review requirements.
- **CRDTs** (Automerge, Yjs, CR-SQLite): automatic convergence, zero coordination, excellent offline support, but limited to semilattice-expressible operations with growing metadata overhead. Best for real-time collaboration and high-frequency low-conflict updates.
- **LWW/server-authoritative** (PowerSync, Turso): simplest to implement, sufficient when overwrites are acceptable, but silently loses concurrent writes. Best for server-centric architectures with rare conflicts.

---

## Delta CRDTs and ConflictSync push bandwidth efficiency forward

Delta-state CRDTs (Almeida, Shoker, Baquero; 2015, 2018) achieve the best of both CRDT worlds: **small messages like operation-based CRDTs** disseminated over **unreliable channels like state-based CRDTs**. Each mutation produces a delta-state — a minimal fragment in the same join-semilattice — that when joined with the current state produces the same result as the full mutator. Multiple deltas can be coalesced into delta-groups, reducing communication overhead by batching operations.

However, Enes et al. (ICDE 2019) identified critical inefficiencies in naive delta-based sync: in cyclic network topologies, redundant delta propagation made it **no better than full state-based sync**. Their fixes — back-propagation prevention and join decomposition to filter only novel parts of deltas — proved essential for real-world performance.

The frontier has moved beyond delta CRDTs. **ConflictSync** (Gomes, Baquero et al., May 2025) introduces **digest-driven synchronization** using Rateless Invertible Bloom Lookup Tables, achieving up to **18× reduction in transmission** versus state-based sync. It eliminates the metadata growth problem of delta-CRDTs entirely — no per-peer delta buffers needed. Production implementations include **Ditto** (mesh networking platform used inside BMW cars), which computes minimal per-peer diffs using version vectors without maintaining delta buffers, and the **DeltaCrdt** Elixir library used by the Horde distributed process registry.

---

## Offline-first production systems span IoT, robotics, and autonomous vehicles

**IoT fleet sync** is a solved problem at the architecture level, with multiple production solutions. **ObjectBox** runs on devices with as little as **16 MB RAM** on ARM-v6 processors (deployed inside BMW cars), providing an object database with built-in peer-to-peer and cloud sync plus native vector search. **Couchbase Mobile** offers a three-tier architecture (Couchbase Lite → Sync Gateway → Couchbase Server) with peer-to-peer sync, vector search from cloud to edge to device, and a C-API for IoT deployment. **AWS IoT Greengrass** provides store-and-forward with configurable priority and Parquet compression. The dominant messaging protocol remains **MQTT**, with sync logic layered on top.

**Robot fleet database sync** operates across three data types: maps, configurations, and telemetry. ROS 2 uses DDS middleware for real-time pub/sub but DDS is not a database sync protocol — persistent data sync requires additional layers. Production systems like **SyncroBot** propagate map updates from one robot detecting an obstacle to the entire fleet for automatic re-routing. **Multi-robot SLAM** is fundamentally a distributed data sync problem: Swarm-SLAM achieves **5–20 Mbps** inter-robot throughput with distributed pose graph optimization, while DDF-SAM 2.0 uses "anti-factors" to prevent information double-counting when fusing local maps.

**Autonomous vehicle fleets** use a three-tier architecture: in-vehicle edge (254 TOPS on NVIDIA Drive Orin, sub-10ms safety-critical decisions), roadside MEC edge (V2X coordination), and cloud (fleet-wide learning). Tesla's fleet learning loop spans **2+ million connected vehicles**: edge cases are uploaded selectively, GPU clusters run automated labeling, and updated neural network weights deploy via staged OTA rollout. Waymo generates **25 TB of sensor data per vehicle per day** and continuously updates HD maps from fleet observations. Safety-critical processing never depends on cloud connectivity.

---

## Multi-model sync remains the unsolved frontier

**No system today syncs KV + graph + vector models with model-specific conflict resolution strategies.** SurrealDB 3.0 (GA February 2026) comes closest — unifying relational, document, graph, time-series, vector, search, geospatial, and key-value data in a single Rust binary with SurrealQL. It supports embedded mode (including WASM) and edge-cloud sync via live queries and WebSocket push. However, its sync mechanism is a single approach for all model types rather than optimizing per-model.

**ObjectBox** and **Couchbase Lite** now support on-device vector search combined with their existing sync engines — the closest to production "vector + structured data sync" for edge devices. ArangoDB offers multi-model capabilities (document + graph + KV) but only datacenter-to-datacenter replication via Kafka, with no edge/embedded mode. Pure vector databases (Qdrant, Weaviate, Milvus) remain cluster-scale systems with no offline-first edge sync.

The practical pattern in production today is **application-layer orchestration**: different databases for different models (SQLite for relational, MQTT for messaging, custom stores for vectors), each with its own sync, coordinated by application code. A unified multi-model sync engine with CRDT-based conflict resolution per data model type — the theoretical ideal — remains unbuilt.

---

## Conclusion: toward hybrid architectures

The evidence across 19 systems and a decade of production experience points toward a clear architectural direction. **Pure CRDT-based sync** excels for real-time collaborative editing, offline-first mobile apps, and peer-to-peer systems where automatic convergence is paramount — but it cannot maintain global invariants like uniqueness constraints or referential integrity. **Pure branch-based sync** (Git for data) excels where human review, audit trails, and complex domain-specific merge logic are needed — but it cannot provide continuous real-time synchronization and may produce unresolvable conflicts.

The emerging consensus is **layered hybrid architectures**: CRDTs for the data plane (ensuring convergence of counters, sets, registers, sequences), branch-based workflows for the control plane (schema evolution, constraint enforcement, data curation), and I-Confluence analysis to classify which operations can safely use weak consistency versus those requiring coordination. Dolt's explicit plans for "type-specific merge-lattices from CRDTs" within its three-way merge framework represent this convergence in practice.

For edge fleet architectures specifically, the decision framework is: use **PowerSync or ElectricSQL** for server-authoritative Postgres-backed apps with reliable connectivity; use **CouchDB/PouchDB or Couchbase Mobile** for multi-master document sync with mature conflict handling; use **Automerge or Yjs** for peer-to-peer collaborative data without a central server; use **Dolt** if you need full version-controlled SQL with three-way merge and can afford the resource overhead on edge devices; and use **ObjectBox or Couchbase Lite** for constrained IoT devices needing embedded sync with vector search. No single system spans the entire design space — the choice depends on where your application falls on the automation-expressiveness spectrum that the I-Confluence theorem precisely defines.