# Deep Research Prompts — Round 2

**Date:** 2026-03-20
**Purpose:** Targeted research on Strata's differentiating capabilities. Round 1 was a broad sanity check across all areas. Round 2 goes deep on the novel aspects where we need to understand the landscape precisely.

**How to use:** Send each prompt as a separate Deep Research query on claude.ai.

---

## Prompt 1: Composable Retrieval Pipelines

**Maps to:** #1632 (Composable Search Substrate RFC)

> Survey the academic and industry state of the art for composable, modular retrieval pipelines — systems where the user or an AI agent can assemble a search strategy from interchangeable operators (keyword retrieval, vector similarity, graph traversal, query expansion, fusion, reranking).
>
> Specifically investigate:
> - DSPy (Stanford) and its retrieval module composition model
> - Haystack's pipeline abstraction (deepset)
> - LlamaIndex's query pipeline architecture
> - ARES (automated retrieval evaluation)
> - Any academic work on "retrieval algebra" or "retrieval plan optimization"
> - How do existing systems allow users to define custom retrieval strategies?
> - Is anyone doing automatic retrieval pipeline structure search (analogous to neural architecture search but for retrieval)?
> - What fusion methods beyond RRF exist? (learned fusion, attention-based, Condorcet)
> - What reranking architectures are most efficient for embedded/local use?
>
> For each system or paper, note: what operators are composable, what's hardcoded, whether the pipeline structure itself is optimizable, and whether it works in an embedded (no-server) context.

---

## Prompt 2: Self-Optimizing Search & AutoML for Retrieval

**Maps to:** #1633 (AutoResearch RFC)

> Survey all academic and industry work on automatic optimization of information retrieval systems — where the system tunes its own search quality with minimal human input.
>
> Specifically investigate:
> - AutoML approaches applied to retrieval (hyperparameter optimization for BM25, embedding models, fusion weights)
> - ARES (Stanford, 2024) — automated retrieval evaluation without human labels
> - LLM-as-judge for retrieval evaluation (using LLMs to generate relevance judgments)
> - Bayesian optimization for search parameter tuning
> - Any system that optimizes retrieval pipeline *structure* (not just parameters)
> - Relevance feedback approaches (Rocchio, pseudo-relevance feedback, neural relevance feedback)
> - How do production systems (Elasticsearch, Vespa, Pinecone) handle search quality optimization today?
> - What evaluation metrics are used? (NDCG, MAP, MRR, Recall@k)
> - What is the minimum number of evaluation pairs needed for reliable optimization?
> - Is anyone using data versioning or branching to parallelize retrieval experiments?
>
> Key question: does any system allow a user to provide (query, expected_results) pairs and automatically discover the optimal retrieval configuration? If not, what are the closest approaches?

---

## Prompt 3: AI Agent Memory Architectures (2024-2026)

**Maps to:** Core positioning — "cognitive infrastructure for AI agents"

> Comprehensive survey of AI agent memory systems — how agents persist, organize, retrieve, and forget information across sessions. Focus on 2024-2026 work.
>
> Specifically investigate:
> - MemGPT / Letta — tiered memory (core/recall/archival), virtual context management
> - CoALA (Princeton) — cognitive architecture with modular memory
> - A-MEM — Zettelkasten-inspired agent memory
> - Generative Agents (Stanford) — recency × relevance × importance scoring
> - MaRS — forgetting-by-design with typed memory nodes
> - LangGraph / LangMem — state management and memory modules
> - Mem0 — long-term memory layer for agents
> - Zep — temporal knowledge graphs for agent memory
> - OpenAI's persistent memory in ChatGPT/assistants API
> - Anthropic's approach to tool-use state and memory
> - Google's approach (Gemini context caching, Project Mariner)
>
> For each system: what's the storage backend (Postgres? Redis? Vector DB? Custom?), what memory organization model (flat KV? hierarchical? graph?), does it support forgetting/decay, does it support branching/experimentation, what retrieval mechanism is used?
>
> Key question: what do agent memory systems need from a database that current databases don't provide? Where are the gaps?

---

## Prompt 4: Graph-Augmented Retrieval & Knowledge Graph RAG

**Maps to:** Ontology-grounded search, patent #1594

> Deep survey of graph-augmented retrieval — using knowledge graphs, entity relationships, and graph structure to improve information retrieval and RAG quality. Focus on 2023-2026.
>
> Specifically investigate:
> - GraphRAG (Microsoft, 2024) — community detection, entity extraction, graph-based summarization
> - HippoRAG (NeurIPS 2024) — hippocampal indexing via knowledge graphs + personalized PageRank
> - RAPTOR — recursive summarization trees for retrieval
> - KG-RAG approaches — knowledge graph enhanced retrieval-augmented generation
> - G-Retriever — graph neural network based retrieval
> - GRAG and similar graph-augmented retrieval systems
> - How do systems extract entities and relationships automatically from text? (NER, relation extraction, LLM-based extraction)
> - What graph scoring signals improve retrieval? (PageRank, personalized PageRank, shortest path, community membership)
> - Ontology-guided query decomposition — using typed schemas to break queries into structured graph traversals
> - Graph + vector hybrid queries — combining embedding similarity with graph proximity
>
> Key question: what is the evidence that graph-augmented retrieval actually improves quality over pure BM25+vector hybrid? On which query types does it help most? What is the cost (latency, compute) of adding graph signals?

---

## Prompt 5: Edge-Fleet Data Synchronization with Branching Semantics

**Maps to:** StrataHub, edge story, drone swarms

> Survey architectures for synchronizing embedded databases across edge device fleets — where each device runs its own database instance and periodically syncs with a hub or with peers.
>
> Specifically investigate:
> - CRDTs applied to database synchronization (Automerge, Yjs, CR-SQLite, ElectricSQL)
> - Local-first software movement (Ink & Switch, Martin Kleppmann's work)
> - Edge-cloud database tiering (Turso/libSQL replication, LiteFS, PouchDB/CouchDB sync)
> - Realm (MongoDB) sync protocol — before deprecation, what worked and what didn't?
> - PowerSync — Postgres to local-first sync
> - Git-like push/pull for data (lakeFS, Neon branching, Dolt remote operations)
> - Conflict resolution strategies for branch-based sync (three-way merge, CRDTs, operational transform)
> - Delta CRDTs for bandwidth-efficient sync
> - Offline-first architectures for IoT, robotics, autonomous vehicles
> - Any system that syncs multiple data models (KV + graph + vectors) across nodes
>
> Key question: has anyone built a system where edge devices operate on branches of a shared database and merge via three-way merge? What are the theoretical limits of branch-based sync vs CRDT-based sync?

---

## Prompt 6: Event Sourcing in Embedded Databases

**Maps to:** Event projections RFC (`docs/rfc-event-projections.md`)

> Survey event sourcing patterns and systems, specifically focused on embedded/local-first contexts — not Kafka-scale distributed streaming, but single-process event logs with projection to derived state.
>
> Specifically investigate:
> - EventStoreDB — event sourcing database, projections model
> - Marten (PostgreSQL-based) — event sourcing + document store in one
> - Axon Framework — event sourcing + CQRS
> - SQLite + event sourcing patterns (any implementations?)
> - How do event projections work? (event → materialized view, catch-up replay, cursor tracking)
> - Deterministic replay — guarantees around replaying events to reconstruct state
> - Event sourcing + CQRS in embedded contexts (no message broker, no separate read/write stores)
> - Cross-model projections — events that materialize into multiple data models (documents + graph + vectors)
> - Event-driven knowledge graph construction (events → entity extraction → graph)
>
> Key question: does any system support event projections that automatically materialize into multiple heterogeneous data models (KV + JSON + graph + vectors) within a single embedded database? What would the architecture look like?

---

## Summary

| # | Topic | Strata Feature | Priority |
|---|-------|---------------|----------|
| 1 | Composable Retrieval Pipelines | #1632 Search Substrate | High — active RFC |
| 2 | Self-Optimizing Search | #1633 AutoResearch | High — active RFC |
| 3 | Agent Memory Architectures | Core positioning | High — market understanding |
| 4 | Graph-Augmented Retrieval | Patent #1594, ontology search | High — active design |
| 5 | Edge-Fleet Sync | StrataHub, edge story | Medium — post-1.0 |
| 6 | Event Sourcing Embedded | Event projections RFC | Medium — planned feature |

## Patent Check Reminder

For any technique identified in these surveys that we plan to implement, do a quick patent search on Google Patents: `[author name] [technique name]`. Most academic work is unpatented, but corporate research labs (Google, Meta, Microsoft, Amazon) sometimes patent aggressively. Check before implementing:
- ACORN (Stanford, 2024) — for #1581
- RaBitQ (NTU Singapore, 2024) — for #1582
- S3-FIFO (CMU, 2023) — for #1573
- Any new techniques discovered in these research rounds
