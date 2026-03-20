# Strata Knowledge Repository: Content Plan

**Date:** 2026-03-20
**Purpose:** Canonical source documents from which all external content (website, blogs, videos, social posts, conference talks) is derived.

---

## Layer 1: Identity (2 documents)

| Document | Purpose |
|----------|---------|
| `what-is-strata.md` | The canonical answer at three levels: one sentence, one paragraph, one page. Everything else references this. |
| `positioning.md` | "SQLite for the AI era." Category, tagline, ICP, competitive frame. Internal document that all external messaging derives from. |

## Layer 2: Product (7 documents)

| Document | Purpose |
|----------|---------|
| `primitives.md` | The four data primitives (KV, JSON, Vector, Graph) + Event Log. What each does, why it exists, when to use which. |
| `branching.md` | Branching as foundation. Fork, merge, diff, time-travel. Why it matters for AI workloads. |
| `search.md` | The full retrieval story. BM25 + vector + graph + RRF + expansion + reranking. How everything becomes searchable. |
| `inference.md` | In-process inference via llama.cpp. Embedding, generation, cloud providers. The "no external dependencies" story. |
| `performance.md` | Hard benchmarks. Throughput, latency, hardware range. Durability mode tradeoffs. Honest numbers. |
| `comparison.md` | Strata vs SQLite vs Redis vs Pinecone vs Neo4j vs Dolt vs SurrealDB. Honest tradeoffs, not a kill matrix. |
| `security.md` | Access modes, follower mode, what's protected, what's not yet. |

## Layer 3: Architecture (6 documents)

| Document | Purpose |
|----------|---------|
| `architecture-overview.md` | 11 crates, dependency flow, data path from API to disk. The one diagram. |
| `storage-engine.md` | LSM tree, segments, compaction, bloom filters, block cache, key encoding. Read path and write path. |
| `concurrency-model.md` | OCC, snapshot isolation, per-branch locks. Why these choices. |
| `durability-model.md` | WAL, three modes, crash recovery, snapshots. |
| `vector-search.md` | HNSW, SIMD, deterministic construction, segmented architecture. |
| `graph-engine.md` | Property graph on KV, adjacency index, ontology, analytics. |

## Layer 5: Practical (13 documents)

### Getting Started

| Document | Purpose |
|----------|---------|
| `quickstart.md` | Install → store → search → branch → done. Under 5 minutes. |

### Tutorials (per-primitive deep dives)

| Document | Purpose |
|----------|---------|
| `tutorial-kv.md` | KV store with versioning, prefix scan, history. |
| `tutorial-json.md` | JSON documents, path mutations, atomic updates. |
| `tutorial-vector.md` | Collections, HNSW search, metadata filtering, auto-embed. |
| `tutorial-graph.md` | Nodes, edges, ontology, traversal, analytics. |
| `tutorial-branching.md` | Fork, experiment, diff, merge, time-travel. |
| `tutorial-search.md` | Hybrid search, query expansion, reranking, configuring models. |

### Patterns (real-world use cases)

| Document | Purpose |
|----------|---------|
| `pattern-agent-memory.md` | Using Strata as persistent memory for an AI agent. |
| `pattern-rag.md` | Building a RAG pipeline entirely within Strata. |
| `pattern-multi-agent.md` | Multi-agent coordination with branching. |
| `pattern-event-sourcing.md` | Event log + projections for derived state. |
| `pattern-ab-testing.md` | A/B testing data configurations with branches. |
| `pattern-edge-fleet.md` | Edge deployment: self-driving cars, drone swarms, IoT sensor networks. Local-first operation, branch-based model rollout, fleet sync via StrataHub push/pull. |

### Migration Guides

| Document | Purpose |
|----------|---------|
| `migration-from-sqlite.md` | Coming from SQLite? Here's how to think differently. |
| `migration-from-redis.md` | Coming from Redis? What maps and what doesn't. |
| `migration-from-pinecone.md` | Coming from a vector DB? The unified alternative. |

## Layer 6: Agent-Readable (4 documents)

Coding agents (Claude Code, Cursor, Copilot, Windsurf) are a distinct audience. They don't need to be convinced to use Strata — they need to use it correctly. These documents are optimized for machine consumption: structured, precise, no narrative filler.

| Document | Purpose |
|----------|---------|
| `llms.txt` | The emerging standard. One-page plain text summary of what Strata is, key concepts, and links to detailed docs. Served at `stratadb.org/llms.txt`. First thing a coding agent reads. |
| `api-reference.md` | Every public API method. Signatures, parameters, return types, error cases. No narrative — pure reference. Agents parse this to generate correct code. |
| `constraints.md` | Hard limits and invariants. Document size (16MB), key size, nesting depth (100), array size (1M), branch name rules, value type semantics (`Int(1) != Float(1.0)`), tombstone behavior, transaction isolation guarantees. The stuff that causes bugs when an agent guesses. |
| `examples.md` | 20-30 copy-pasteable code snippets covering every common operation. One snippet per operation, no narrative between them. Agents retrieve these as patterns to adapt. |

## Layer 7: IDE & Agent Integrations (6 items)

These are distribution channels. A developer who discovers Strata inside their existing tool is 10x more likely to adopt than one who reads docs. Each integration consumes the Layer 6 agent-readable docs as its knowledge base.

| Integration | Platform | What It Does |
|-------------|----------|-------------|
| **Claude Code skill** | Claude Code | `/strata` slash command. Full Strata assistant — generate code, debug queries, explain branching, scaffold projects. Uses `llms.txt` + `api-reference.md` + `examples.md` as context. |
| **Claude Code MCP server** | Claude Code | Strata as an MCP tool provider. Claude can directly read/write/search/branch a Strata database during a coding session. Already partially exists (#1274). |
| **Codex plugin** | OpenAI Codex | Same capability as Claude Code skill, adapted for Codex's plugin format. |
| **Cursor rules** | Cursor | `.cursor/rules` file that teaches Cursor how to write correct Strata code. References constraints, API patterns, common pitfalls. |
| **VS Code extension** | VS Code | Strata explorer panel: browse branches, view data, run queries, visualize graph. Lightweight — reads from a running Strata instance or .strata directory. |
| **GitHub Action** | GitHub CI | `stratadb/strata-action` — run Strata tests, validate branch bundles, check data integrity in CI pipelines. |

---

## Summary

| Layer | Count | Priority |
|-------|-------|----------|
| Identity | 2 | Write first — everything else references these |
| Product | 7 | Write second — the substance of what Strata is |
| Architecture | 6 | Partially exists in codebase docs, needs consolidation |
| Practical | 14 | Write alongside product docs, expand over time |
| Agent-Readable | 4 | Write alongside product docs — agents are a day-one audience |
| IDE & Agent Integrations | 6 | Build after Layer 6 — integrations consume agent-readable docs |
| **Total** | **39** | |

## Principles

- **No feature listing.** Every document leads with the problem or outcome, not the feature.
- **Honest.** If we don't do something well, say so. Comparisons include tradeoffs, not just wins.
- **Canonical.** These are the source of truth. Blog posts excerpt from these. Website copy is derived from these. Conference talks reference these.
- **Current.** When the product changes, these documents update first. External content follows.
- **Dual audience.** Every document serves humans or agents or both. Humans need narrative and motivation. Agents need structure and precision. Don't mix the two — separate documents for separate readers.
