# StrataDB: A Comprehensive Academic and Industry Research Survey

**StrataDB occupies a genuinely novel position in the database design space — no existing system combines MVCC-native snapshot versioning, O(1) git-style branching, six unified data primitives, ACID vector search, and an embedded Rust architecture.** This survey across 10 major research areas reveals that StrataDB's design is validated by recent academic work (particularly the UC Berkeley "Agent-First Databases" paper from 2025 and the GenericVC CIDR 2025 vision paper) while identifying critical techniques to adopt — ACORN filtered search, RaBitQ quantization, S3-FIFO caching, CRDT-based branch merging, and Hekaton-style recovery — that could dramatically accelerate StrataDB's competitive positioning. The rapidly emerging AI agent infrastructure market represents a timing window: frameworks like LangGraph and Letta have standardized the agent orchestration layer, but the persistence layer remains fragmented across generic databases repurposed for agents.

---

## Section 1: LSM-tree storage is evolving toward adaptive, learned, and CXL-aware designs

StrataDB's leveled compaction with partitioned Bloom filters, 4KB blocks, and pread-based I/O represents solid foundational engineering. The research frontier has moved toward **adaptive compaction** and **learned auxiliary structures** that could deliver significant gains.

**Compaction strategies** have advanced beyond static leveled/tiered. Dostoevsky's lazy leveling (Dayan & Idreos, SIGMOD 2018) tiers upper levels while leveling only the largest, reducing write amplification substantially. **Spooky** (Dayan et al., PVLDB 2022) introduces partitioned merge — aligning compaction boundaries to last-level SST boundaries — reducing unnecessary rewrites. K-LSM (Huynh et al., VLDBJ 2024) generalizes per-level run counts, and EcoTune (Wang et al., SIGMOD 2025) jointly optimizes compaction and read resource allocation. SplinterDB's maplet approach (Conway, Farach-Colton, Johnson, SIGMOD 2023) decouples filter compaction from data compaction, achieving **15–61% space overhead vs. RocksDB's 80–117%**. Niv Dayan (Toronto) and Stratos Idreos (Harvard) lead this design-space exploration.

**Learned index structures** now augment or replace traditional filters. SNARF (Vaidya et al., PVLDB 2022) uses CDF models to compress bitmaps, achieving **10× improvement over SuRF/Rosetta** on RocksDB. GRF (Wang et al., SIGMOD 2024) handles MVCC-versioned keys correctly — directly relevant to StrataDB. Mnemosyne (Zhu et al., SIGMOD 2025) dynamically reallocates Bloom filter bits across evolving LSM trees without prior workload knowledge. The PGM-index (Ferragina & Vinciguerra, PVLDB 2020) offers O(log log N) lookup with provable worst-case bounds.

**Block cache management** has been revolutionized by CMU's Juncheng Yang and Rashmi Vinayak. **S3-FIFO** (SOSP 2023) uses three FIFO queues exploiting the "one-hit-wonder" observation — most cached items are accessed only once — achieving lower miss ratios than LRU/ARC while delivering **6× higher throughput** due to lock-free FIFO operations. SIEVE (NSDI 2024, Best Paper) simplifies further with a single bit per entry. Both are adopted by Google, Meta, VMware, and Redpanda. For StrataDB's pread-based concurrent I/O, replacing LRU with S3-FIFO or SIEVE is a high-impact, low-risk improvement.

**CXL-attached memory** is reshaping LSM design post-Intel Optane. dLSM (Wang et al., VLDBJ 2024) achieves **2.3–11.6× higher write throughput** on disaggregated memory via near-data compaction. Samsung, SK Hynix, and Micron are shipping CXL Type-3 devices with 150–400ns latency. StrataDB should abstract its memory allocator to support heterogeneous tiers — persistent skiplist memtables on CXL-PM could eliminate WAL overhead entirely.

**Key techniques for StrataDB**: Lazy leveling on L0–L4 with leveling on L5–L6; Spooky partitioned merge at lower levels; S3-FIFO or SIEVE for block cache; FSST compression (Boncz, Neumann, Leis, PVLDB 2020) for string-heavy keys with per-SST symbol tables; zstd dictionary compression per SST file; SILK-style I/O scheduling (ATC 2019) with separate thread pools for flush, L0→L1, and higher-level compaction achieving **100× lower P99 latency**.

---

## Section 2: Branch-aware MVCC is academically validated but barely explored

StrataDB's git-like branching over MVCC is the system's most distinctive feature, and recent academic work strongly validates this direction.

**TardisDB** (Schüle et al., TU Munich, SSDBM 2019 / SIGMOD 2021) is the closest academic analog — it combines MVCC version chains with per-branch bitmaps and extended SQL for branching, proving that **reusing MVCC chains for branching has zero negative throughput impact**. The Berkeley "Supporting Our AI Overlords" paper (Liu et al., arXiv 2025) reports that Neon observed agents creating **20× more branches and 50× more rollbacks** than human users, arguing databases need "MVCC on steroids" for agentic workloads with "multi-world isolation." **GenericVC** (Yilmaz & Dittrich, CIDR 2025) formally argues that MVCC and Git are different implementations of the same concept, proposing configurable `detect_conflicts` and `reconcile` functions — precisely the extensibility model StrataDB should target.

Industry systems validate different branching approaches. **Dolt** uses Prolly Trees (content-addressed Merkle B-trees with probabilistic chunking) enabling O(1) branch creation, O(diff) merge, and cell-level three-way merge. **Neon** provides WAL-based timeline forking at specific LSNs — branch creation is a metadata operation with parent traversal for unmodified pages. **lakeFS** offers zero-copy branching over object storage for data lakes.

**MVCC garbage collection** remains a fundamental challenge amplified by branching. OneShotGC (Raza et al., SIGMOD 2023) eliminates version chain traversal by clustering versions temporally — achieving **up to 2× transactional throughput improvement**. LeanStore's adaptive version storage (Alhomssi & Leis, PVLDB 2023) introduces a Graveyard Index for deleted tuples. **Branch-aware GC is a completely open research problem** — no published work addresses determining which versions are reachable from which branches. StrataDB could extend epoch-based reclamation with per-branch epoch tracking and adapt SAP HANA's interval-based GC to branch intervals.

**Time-travel queries** are implemented across multiple production systems (CockroachDB's `AS OF SYSTEM TIME`, TiDB's stale reads, Neon's WAL-based page reconstruction at any LSN), but **cross-branch temporal queries** — querying "state of branch X at time T and branch Y at time T'" simultaneously — remain unexplored.

**StrataDB's unique position**: No existing system combines MVCC-native snapshot versioning, O(1) branch creation, per-branch commit isolation, embedded architecture, and Rust implementation. The branch-ID-prefixed key encoding is simpler than Dolt's content-addressed approach but may lack structural sharing for storage efficiency. Consider adopting Dolt's cell-level merge granularity and TardisDB's per-branch bitmaps for O(1) branch membership testing.

---

## Section 3: Concurrency control research enables branch-aware optimistic protocols

StrataDB's OCC with snapshot isolation aligns well with the state of the art, with several techniques available to reduce abort rates and extend capabilities.

**TicToc** (Yu et al., SIGMOD 2016) computes timestamps lazily from data rather than centrally, achieving **92% higher throughput** than prior OCC algorithms by reducing false aborts. **MOCC** (Wang & Kimura, PVLDB 2016) tracks per-record "temperature" to selectively add pessimistic locks only on hot records — **8× faster than OCC and 23× faster than 2PL** under high contention. **Polaris** (Ye et al., SIGMOD 2023) adds transaction priority to OCC — critical for agent workloads where certain operations need preferential commit rights. The newest work, **HDCC** (Hong et al., PVLDB 2025), adaptively assigns Calvin-style deterministic or OCC per transaction, achieving **3.1× over existing hybrids**.

For abort rate reduction, **BCC** (Yuan et al., PVLDB 2016) distinguishes true data dependencies from false aborts, gaining **3.68× throughput** under high contention. **Transaction Healing** (Wu et al., SIGMOD 2016) re-utilizes partial execution results instead of full restart. **Aria** (Lu et al., VLDB 2020) achieves deterministic batch execution without requiring read/write set pre-declaration — highly relevant for agent workloads where sets aren't known a priori.

The **snapshot isolation vs. serializability tradeoff** is well-studied. PostgreSQL's SSI (Ports & Grittner, PVLDB 2012) adds only **~2% overhead** over SI by detecting dangerous rw-antidependency structures. Write-Snapshot Isolation (Yabandeh & Ferro, EuroSys 2012) checks R-W conflicts instead of W-W conflicts for lock-free serializability. Marc Brooker (AWS Aurora DSQL, 2024) argues SI is the right default because write sets are smaller than read sets. The mixed isolation work by Vandevoort et al. (PVLDB 2025) provides theoretical foundations for **per-branch isolation level selection** — different branches could run at different isolation levels.

**Lock-free index techniques** directly applicable to StrataDB include **Optimistic Lock Coupling (OLC)** (Leis et al., DaMoN 2016) — readers proceed without locks using version counters, mapping cleanly to Rust's `AtomicU64`. **OptiQL** (Shi et al., SIGMOD 2023) extends MCS locks with optimistic reads for contention robustness. A new **B-skiplist** (Luo et al., ICPP 2025) achieves **2–9× throughput over Facebook Folly's concurrent skiplist** with cache-optimized blocked nodes.

**Branch-aware concurrency control is a wide-open field.** Dolt is the only production system implementing it, using three-way cell-level merge where concurrent transactions modifying different columns produce no conflict. StrataDB's per-branch commit locks are sound — the key research opportunity is cross-branch transactions (atomically reading/writing across multiple branches) and configurable merge semantics per GenericVC's vision.

---

## Section 4: ACID-transactional vector search is StrataDB's strongest differentiator

StrataDB's MVCC-native HNSW with ACID guarantees occupies a position **no other system provides**. pgvector inherits PostgreSQL MVCC but treats HNSW as a side structure; Milvus offers only bounded staleness; LanceDB achieves ACID via copy-on-write versioning but uses IVF-PQ rather than HNSW.

**ACORN** (Patel et al., Stanford FutureData Lab, SIGMOD 2024) is the most impactful near-term adoption candidate. It extends HNSW with predicate subgraph traversal for filtered search, achieving **2–1,000× higher throughput** than pre/post-filtering baselines. Predicate-agnostic — supports arbitrary filters without reindexing. Already adopted by Weaviate (v1.27+) and Elasticsearch (v9.1).

**RaBitQ** (Gao & Long, NTU Singapore, SIGMOD 2024) quantizes D-dimensional vectors to D-bit strings with proven error bounds — **3× faster than Product Quantization** for distance estimation via bitwise operations. Extended RaBitQ (SIGMOD 2025) supports arbitrary 2–6 bit widths. D-bit strings with SIMD bitwise operations are exceptionally Rust-friendly. Elastic implemented this as "BBQ" (Better Binary Quantization), outperforming float32 search on 9/10 BEIR datasets.

**DiskANN/Vamana** (Subramanya et al., NeurIPS 2019) enables SSD-resident vector indexing at billion scale with sub-10ms latency. **FreshDiskANN** (Singh et al., 2021) extends this for streaming with concurrent insert/delete/search. **SPFresh/LIRE** (Xu et al., SOSP 2023) achieves incremental in-place updates with **2.41× lower P99.9 latency** and **5.3× lower memory** than baselines. StrataDB's soft-delete via MVCC visibility directly addresses the tombstone bloat plaguing FreshDiskANN.

**Learned HNSW shortcuts** (Chen et al., VLDB 2025) use piecewise linear models to skip unnecessary graph levels per query — works on pre-built indexes with no structural changes. Ada-ef (arXiv 2025) provides per-query adaptive ef_search selection. Both are low-hanging fruit for StrataDB.

**TigerVector** (Liu et al., SIGMOD 2025 Industrial Track) demonstrates MVCC-based vector deltas with two-phase vacuum (fast delta flush + slow index rebuild) — directly applicable to StrataDB's architecture. **GaussDB-Vector** (Huawei, VLDB 2025) achieves <50ms latency at >95% recall at billion+ scale.

For StrataDB's pluggable backend architecture, a priority roadmap: (1) ACORN-1 for filtered search; (2) RaBitQ quantization for memory reduction; (3) Adaptive ef_search per query; (4) Vamana/DiskANN backend for disk-resident large collections.

---

## Section 5: The retrieval stack is converging on multi-signal fusion that matches StrataDB's primitives

StrataDB's BM25 + cosine + RRF pipeline is a solid starting point, but the field is rapidly converging on three-way retrieval as optimal for RAG.

**BGE-M3** (BAAI, 2024) produces dense + sparse + multi-vector representations in a single forward pass from one 550M-parameter model supporting 100+ languages and 8192-token input. IBM's **Blended RAG** (2024) definitively demonstrates that three-way retrieval (BM25 + dense + learned sparse) outperforms any pair. This means StrataDB should add a **sparse vector primitive** alongside BM25 — float-weighted inverted indexes for SPLADE-style output that can reuse existing BM25 infrastructure with modified scoring.

**SPLADE-v3** (Naver Labs, 2024) achieves latency within 4ms of BM25 while delivering 13–20% higher MRR. However, BM25 remains irreplaceable for phrase queries, structured search, and domain-specific applications without fine-tuning data. Both should coexist as first-class primitives.

**ColBERT-style late interaction** (Santhanam et al., NAACL 2022) with the PLAID engine (CIKM 2022) provides **7× GPU / 45× CPU speedup** over vanilla ColBERTv2 through centroid interaction pruning. SPLATE (Formal et al., 2024) maps ColBERTv2 token embeddings to sparse vocabulary space, enabling inverted-index retrieval — this could run on StrataDB's existing infrastructure.

For **in-process embedding**, the `ort` crate (ONNX Runtime) is the recommended Rust integration — used by `fastembed-rs` (Qdrant) with production validation. Oracle Database 26ai follows the same pattern with `VECTOR_EMBEDDING` SQL functions. Cross-encoder reranking via **ms-marco-MiniLM-L-6-v2** (22M params, ~6ms per pair on CPU) shares the same ONNX infrastructure.

**RAG optimization at the database level** is a critical frontier. RAPTOR (Sarthi et al., ICLR 2024) builds recursive summarization trees — **+20% absolute accuracy** on QuALITY benchmark. GraphRAG (Microsoft, 2024) uses entity-relationship graphs with community detection. HippoRAG (Gutiérrez et al., NeurIPS 2024) mimics hippocampal indexing via knowledge graphs + personalized PageRank. All these patterns map naturally to StrataDB's multi-primitive architecture: RAPTOR trees as hierarchical documents with branch semantics, GraphRAG entities as graph primitives, HippoRAG scoring via StrataDB's graph analytics.

---

## Section 6: StrataDB's six-primitive design has no direct precedent

**No existing system combines document + graph + vector + event log + KV + state cell in a single embedded transactional engine.** The closest industry analog is SurrealDB (Rust, multi-model, KV substrate, ACID) but it lacks native MVCC, branch-awareness, and event log/state cell primitives. TerminusDB offers git-like branching with MVCC but is graph/document only and in-memory. FoundationDB provides universal transactional guarantees via "layers over ordered KV" but targets distributed infrastructure, not embedded scenarios.

The academic literature on multi-model databases is growing. M2Bench (Kim et al., PVLDB 2023, Seoul National University) is the first benchmark covering relational + document + graph + array models jointly — StrataDB could contribute a benchmark covering its six-primitive workloads. The survey by Lu & Holubová (ACM Computing Surveys 2019) classifies systems along native vs. layered multi-model support.

The **FoundationDB "layers over ordered KV" pattern** (Zhou et al., SIGMOD 2021) works brilliantly for distributed infrastructure where the primary challenge is transactional correctness at scale. StrataDB's native primitives approach is better suited for embedded scenarios where every microsecond matters — no encoding/decoding overhead, purpose-built storage layouts (CSR for graphs, columnar for documents), and cross-primitive query optimization that layers can't provide. CockroachDB, TiDB, and Spanner all validate the "rich model over ordered KV" pattern internally.

**Cross-model query optimization** remains an open problem — no system has a truly unified optimizer that plans across document, graph, vector, and relational models simultaneously. StrataDB's multi-primitive MVCC GC is also unexplored territory: different primitives have different retention needs (event logs keep everything; state cells keep latest; documents are configurable). This represents both a challenge and a publication opportunity.

---

## Section 7: Graph-on-KV storage benefits from hybrid vertex/edge layouts

StrataDB's property graph overlay on KV is well-positioned within an active research area. **Aster/Poly-LSM** (Mo et al., SIGMOD 2025, NTU) introduces graph-oriented LSM storage with hybrid vertex/edge layouts — write edges individually, consolidate during compaction into vertex-based adjacency lists — achieving **17× throughput** over baseline graph DBs on billion-scale graphs. **LSMGraph** (SIGMOD 2024) embeds CSR structures within LSM levels for read performance. **ByteGraph** (Li et al., VLDB 2022, ByteDance) uses edge-trees (B+-tree per adjacency list with nodes as KV pairs) handling billions of edges in production.

For embedded graph processing, **Kùzu** (Jin et al., CIDR 2023, U. Waterloo) provides factorized query processing with ASP-Join and worst-case optimal joins. **DuckPGQ** (ten Wolde et al., CIDR 2023, CWI) implements SQL/PGQ within DuckDB with on-the-fly CSR creation and multi-source BFS via SIMD bitsets — AVX512 tracks **512 concurrent searches simultaneously**. FalkorDB (formerly RedisGraph) uses SuiteSparse:GraphBLAS (Tim Davis, Texas A&M) to express graph algorithms as sparse linear algebra, claiming **6–600× performance** over alternatives.

**Graph + vector hybrid queries** are the frontier driving GraphRAG adoption. TigerVector (Liu et al., SIGMOD 2025 Industrial Track) integrates vector search directly into TigerGraph with MVCC-based vector deltas — outperforming Neo4j, Neptune, and even Milvus on hybrid workloads. For **versioned graph analytics**, AeonG (Hou et al., VLDB 2024, Renmin University) uses an anchor+delta strategy over KV where deltas record differences between versions and periodic anchors preserve complete state. LLAMA (Macko et al., ICDE 2015, Harvard) introduced multi-versioned CSR arrays with delta snapshots — its model maps directly to StrataDB's branch model.

**StrataDB recommendations**: Adopt NebulaGraph's key encoding pattern (`vertexID+TagID` for nodes, `srcID+edgeType+rank+dstID` for edges) extended with branch ID prefix; implement Poly-LSM hybrid write/compaction strategy; build transient CSR from KV adjacency indexes for traversals (DuckPGQ pattern); use AeonG's anchor+delta for versioned graph data.

---

## Section 8: Recovery design should exploit MVCC-native architecture

StrataDB's segmented WAL with three durability modes and multi-participant recovery coordinator is a solid design. Key research findings point toward exploiting the MVCC-native architecture for simpler, faster recovery.

**Hekaton's LSN-free logging** (Diaconu et al., SIGMOD 2013) is the most important influence: since dirty data resides only in memory, Hekaton generates log records only at commit time. Critically, **indexes are not logged — they are rebuilt entirely from checkpoint data + log during recovery**. This is directly applicable to StrataDB: VectorStore and SearchIndex participants could skip index logging entirely, rebuilding from base data during recovery.

**Constant-Time Recovery (CTR)** in Azure SQL Database (Antonopoulos et al., PVLDB 2019) combines ARIES with MVCC for recovery **under 3 minutes for 99.999% of cases** regardless of transaction size. The key insight is that since StrataDB is MVCC-native, abort is trivial (mark versions invisible) and only non-versioned operations need a secondary log (sLog). This could dramatically reduce StrataDB's log volume.

**LeanStore's continuous checkpointing** (Haubenschild et al., SIGMOD 2020) guarantees bounded recovery time proportional to checkpoint interval rather than database size, with **2× improvement over ARIES** for out-of-memory workloads. Parallel per-thread logging with LSN-vector dependency tracking (Taurus, Xia et al., PVLDB 2020) enables **22.9× faster recovery** through parallel replay.

**Recovery-aware vector index reconstruction** is the least mature area. **P-HNSW** (Applied Sciences 2025) is the first crash-consistent HNSW implementation, using NLog (node insertion state machine) and NlistLog (neighbor list tracking) for two-step recovery. Most production systems (Milvus, Weaviate, Qdrant, pgvector) simply rebuild HNSW from stored vectors on restart. StrataDB's multi-participant recovery coordinator should adopt a **hybrid approach**: Cache mode rebuilds fully; Standard mode uses periodic index snapshots + WAL replay; Always mode uses P-HNSW-style fine-grained graph logging for surgical recovery. **Branch-aware recovery** — scoping WAL replay to only branch-relevant mutations — is a unique optimization opportunity.

---

## Section 9: Agent memory infrastructure is the defining market opportunity

StrataDB's positioning as "cognitive infrastructure for AI agents" arrives at a pivotal moment. The agent ecosystem has standardized at the protocol layer (MCP, Agent Skills) and the framework layer (LangGraph, Letta, AutoGen), but **the persistence layer remains generic databases repurposed for agents** — analogous to the pre-SQLite era.

**MemGPT/Letta** (Packer et al., NeurIPS 2023) introduced virtual context management with tiered memory (core/recall/archival), spinning out as a startup with $10M seed funding. It uses PostgreSQL for persistence — a general-purpose RDBMS. **CoALA** (Sumers et al., Princeton NLP, TMLR 2024) provides the foundational cognitive architecture framework with modular memory (working, episodic, semantic, procedural). **A-MEM** (Xu et al., Feb 2025) proposes Zettelkasten-inspired dynamic memory with interconnected knowledge networks. An ICLR 2026 workshop ("MemAgents") is specifically focused on memory for agentic systems, confirming this is an active frontier.

**Temporal decay models** for retrieval are directly relevant. Generative Agents (Park et al., SIGGRAPH 2023) pioneered composite scoring: `score = α·recency + β·relevance + γ·importance` with exponential decay (0.995/hour). MemoryBank uses Ebbinghaus forgetting curves. MaRS (arXiv, Dec 2025) introduces forgetting-by-design with typed memory nodes and configurable retention policies (FIFO, LRU, priority decay, reflective consolidation). **No existing embedded database offers temporal decay scoring as a first-class query primitive** — StrataDB could implement this using MVCC timestamps for recency and access frequency tracking.

**Branching as experimentation** has strong conceptual validation but no database-level implementation. Tree of Thoughts (Yao et al., NeurIPS 2023) explores multiple reasoning branches. ADAS/Meta Agent Search (ICLR 2025) iteratively creates and evaluates new agent designs — essentially branching in design space. **Git-ContextController (GCC, 2025)** reimagines agent memory as version-controlled filesystem and achieved **48% on SWE-Bench-Lite** (vs. 43% next-best), with agents spontaneously adopting disciplined behaviors like branching to explore architectural ideas. **No academic paper proposes database-native branching as infrastructure for agent experimentation** — this is StrataDB's strongest unique positioning and a clear publication opportunity.

All major labs are converging on persistent memory across sessions, agentic tool use, and model-agnostic interoperability. OpenAI, Anthropic, and Google all ship persistent memory features in their frontier models, but the infrastructure remains ad hoc. StrataDB can position as the **purpose-built cognitive persistence layer** that any framework can use.

---

## Section 10: Distributed branching is uncharted territory with clear technical paths

StrataDB's roadmap from embedded to distributed follows well-established patterns, but distributed MVCC branching would be a first.

**Neon's disaggregated architecture** is the most relevant model: stateless compute (Postgres) + Pageserver (LSN-indexed layered storage) + Safekeepers (Paxos WAL consensus) + cloud object storage. Branch creation is a metadata operation with parent traversal for unmodified pages. **Turso/libSQL** provides SQLite-based embedded replicas that sync with a remote primary via WAL replication — reads are local (microsecond latency), writes propagate upstream.

For **distributed vector indexes**, Milvus uses segment-based architecture where workers load segments from object storage into cache (stateless), while Qdrant uses stateful consistent hashing with Raft consensus. StrataDB's multi-primitive design could treat sealed segments as distribution units, with per-branch segment lists enabling copy-on-write at the segment level.

**Edge-to-cloud sync** is a rapidly evolving space. **ElectricSQL** (founded by CRDT inventors Marc Shapiro and Nuno Preguiça) uses shape-based partial replication with "Rich-CRDTs" preserving relational integrity. **CR-SQLite** (Matt Wonlaw) adds CRDT-based multi-master replication to SQLite via per-column CRDT tracking — tables upgraded to Conflict-free Replicated Relations. MongoDB Realm's deprecation argues for building sync into the storage engine itself rather than as a separate service.

**CRDTs are the natural merge mechanism for StrataDB's distributed branches.** Each branch functions as an isolated CRDT replica accumulating operations; merge is mathematically guaranteed to converge. Per-column CRDT selection (counters → PN-Counter, text → RGA, scalars → LWW or MV-Register) following CR-SQLite's model provides granular control. **Delta CRDTs** (Almeida et al., JPDC 2018) enable bandwidth-efficient sync by shipping only deltas. **Automerge** (Kleppmann et al.) explicitly unites Git-like version control with CRDT merge — exactly StrataDB's vision, but for JSON rather than structured database data.

The TARDiS system (Crooks et al., SIGMOD 2016) introduces **conflict→branch as a concurrency control mechanism** — when conflicts are detected, the system logically branches state rather than aborting, achieving **2–8× throughput** for ALPS applications. This directly informs StrataDB's approach to handling sync conflicts: rather than last-writer-wins, create a branch at the conflict point and let the application merge when ready.

---

## Strategic synthesis: ten research directions to pursue or monitor

Drawing from all ten survey areas, these are the highest-impact research directions for StrataDB, ranked by competitive urgency and alignment with the AI agent infrastructure vision.

**1. ACORN-style filtered vector search (adopt immediately).** Filtered hybrid search is the most requested feature in vector databases. ACORN's predicate subgraph traversal delivers 2–1,000× throughput gains and works as an extension to existing HNSW indexes without reindexing. This directly addresses a gap in StrataDB's current HNSW implementation and is well-suited to the metadata-rich queries that agents produce.

**2. Temporal decay and cognitive retrieval primitives (define the category).** No embedded database offers Ebbinghaus-inspired decay scoring, spaced-repetition retrieval, or forgetting-by-design as native query primitives. StrataDB can use MVCC timestamps for recency, access frequency tracking for stability, and configurable decay functions as first-class features. This positions StrataDB as genuinely cognitive infrastructure rather than another database with agent branding.

**3. S3-FIFO/SIEVE block cache replacement (adopt immediately).** A straightforward engineering win — 6× higher throughput than LRU with lower miss ratios, using lock-free FIFO queues that are natural in Rust. The "one-hit-wonder" insight is especially relevant for LSM block caches where compaction reads many blocks once.

**4. RaBitQ vector quantization (adopt near-term).** 32× memory reduction with proven error bounds and D-bit strings implementable via SIMD bitwise operations — exceptionally Rust-friendly. Extended RaBitQ offers tunable 2–6 bit compression. This enables StrataDB to handle larger vector collections within memory budgets that embedded deployments face.

**5. CRDT-based distributed branch merging (research for post-1.0).** GenericVC's vision of configurable `detect_conflicts` and `reconcile` functions, combined with CR-SQLite's per-column CRDT model and TARDiS's conflict→branch paradigm, provides a clear theoretical path for StrataDB's cloud sync protocol. Delta CRDTs enable bandwidth-efficient push/pull. This would make StrataDB the first database with distributed MVCC branching — a significant competitive moat.

**6. Hekaton-style recovery with no-index-logging (adopt near-term).** StrataDB's MVCC-native architecture enables CTR-style constant-time recovery where abort is trivial (mark versions invisible). Not logging VectorStore and SearchIndex operations — rebuilding them from base data during recovery — dramatically simplifies the recovery coordinator. Branch-aware recovery scoping WAL replay to branch-relevant mutations is a unique optimization.

**7. Three-way hybrid retrieval: BM25 + dense + learned sparse (build toward).** The retrieval field has converged on three-way fusion as optimal for RAG. Adding a sparse vector primitive compatible with SPLADE/BGE-M3 output reuses StrataDB's inverted index infrastructure. BGE-M3 produces all three signal types in a single forward pass, which aligns with StrataDB's planned auto-embedding via in-process models.

**8. Graph-aware LSM storage with Poly-LSM hybrid layouts (adopt medium-term).** Poly-LSM's write-edges-individually, consolidate-during-compaction strategy achieves 17× throughput on graph workloads. Combined with DuckPGQ-style on-the-fly CSR construction and SIMD-accelerated multi-source BFS, this would transform StrataDB's graph primitive from an overlay into a high-performance native capability.

**9. Branch-aware garbage collection (research priority).** This is a completely open problem with no published solutions. StrataDB must track which versions are reachable from which branches. Approaches to explore: per-branch epoch tracking extending LeanStore's model, OneShotGC-style temporal clustering per branch for batch reclamation, and configurable per-branch retention policies.

**10. Adaptive compaction with lazy leveling (adopt near-term).** Lazy leveling on upper levels (L0–L4 tiered, L5–L6 leveled) reduces write amplification substantially with minimal read impact. Spooky's partitioned merge further reduces per-compaction I/O. SILK-style I/O scheduling with separate thread pools and preemption cuts P99 tail latency by 100×. These are well-understood techniques that directly improve StrataDB's path toward RocksDB read-path parity at billion-key scale.

These ten directions form a coherent strategy: directions 1–4 and 6 are near-term engineering improvements that strengthen StrataDB's embedded competitive position; direction 2 defines StrataDB's cognitive differentiation; directions 5, 7–8, and 9 build toward the full multi-primitive, distributed vision; and direction 10 ensures the storage engine foundation scales. The overarching insight is that **StrataDB sits at the intersection of three converging research trends — agent-first databases, multi-model unification, and version-controlled data** — a position no other system occupies and that recent academic work increasingly validates as necessary.