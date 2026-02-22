# Strata: Positioning

## The State of AI

LLMs are a powerful cortex. They can reason, plan, generate, and execute. But they're missing the cognitive structures that enable continuity — the hippocampus and prefrontal cortex that turn raw intelligence into something that learns, adapts, and compounds over time.

Every session is anterograde amnesia. The intelligence is real, but nothing persists, nothing is learned, nothing compounds. An AI without these structures can think, but it can't form new associations, can't navigate based on experience, can't experiment safely, can't build on yesterday's work.

This is the bottleneck. Not intelligence. Not tooling. Not compute. The missing piece is the cognitive infrastructure that completes the system.

## Every Paradigm Gets Its Own Database

Mainframes got hierarchical databases. Client-server got relational databases — Oracle, PostgreSQL. Web scale got NoSQL — MongoDB, Redis. Mobile got embedded databases — SQLite, Realm.

Each one was purpose-built for how that paradigm's primary consumer thought about and accessed data. Relational databases assumed a human developer writing SQL. NoSQL assumed a web developer who needed flexible schemas and horizontal scale. SQLite assumed a mobile app that needed a database in a single file with no server.

AI agents are the next paradigm's primary consumer. And they're still using databases built for the previous ones.

## The Trajectory

**Phase 1 (now):** Humans use AI to build traditional applications. AI writes SQL, designs schemas, configures ORMs — doing human developer work, including all the database ceremony.

**Phase 2 (next):** AI builds AI. An AI agent builds another agent that monitors metrics and acts on them. That second agent needs persistence — not for "remembering conversations" but for doing its job. Storing thresholds, tracking history, recording decisions, evolving its own rules.

In phase 2, who designs the schema? Who writes the migrations? Who configures the connection pool? There's no human developer in the loop for those decisions. The entire ceremony that exists because databases were built for human developers becomes an obstacle.

## Not "Memory"

The industry is crowded with products that stitch together a vector database and some glue and call it "agent memory." That framing reduces persistence to recall — "remember what the user said last time." It's building a clipboard and calling it cognition.

The hippocampus does far more than recall. It forms associations, provides temporal context, enables navigation based on experience. The prefrontal cortex enables planning, experimentation, and decision-making. Together they're what separate an organism that reacts from one that learns and plans.

The "memory" products give AI a notepad. That's not what's missing.

## What Strata Is

Strata is the hippocampus and prefrontal cortex of AI.

Not a database the agent connects to. A cognitive component that completes it.

**Hippocampus** — persistence, contextual association, temporal recall. Not just "what was stored" but "what is this related to" and "what did this look like before." Store structured state, search by meaning across everything, recall any key as it was at any past point in time. Every write is versioned. Nothing is ever lost.

**Prefrontal cortex** — working state, planning, safe experimentation, decision tracking. Fork the entire state before risky operations. Try things, evaluate, merge if it works, discard if it doesn't. Record decisions and actions as immutable events that form a permanent audit trail.

Together: an AI that doesn't just reason, but persists, associates, experiments, and learns.

## How It Works

One process, one data directory, zero configuration. An agent connects and immediately has a complete cognitive persistence layer through 8 intent-driven tools:

- **Store** structured JSON by key, with surgical nested updates via JSONPath
- **Recall** by key, with time-travel to any past version
- **Search** by natural language across everything stored
- **Forget** by key, with history preserved
- **Log** immutable events — actions, decisions, errors — that can never be rewritten
- **Branch** the entire state for safe experimentation
- **History** for every key — full version audit, time range discovery
- **Status** to orient at the start of any session

No schemas. No migrations. No query language. No infrastructure. The agent doesn't think about persistence mechanics. It just persists.

## What Makes It Different

**Versioning as the storage model.** Every write creates a new version. Time-travel is a natural consequence, not a bolt-on. This is what temporal context looks like as infrastructure — the ability to understand how state evolved, not just what it is now.

**Branching as a primitive.** Copy-on-write branching of the entire state as a first-class operation. Fork, experiment, merge or discard. This is executive function as infrastructure — the ability to plan, explore alternatives, and evaluate outcomes without risk.

**Search by meaning.** Hybrid keyword and semantic search across all stored data. When the agent doesn't remember the exact key — which is often — it describes what it's looking for. This is associative recall as infrastructure.

**Local inference.** Embedding and search happen in-process. No API calls to external services, no network round-trips. Cognition should not depend on someone else's infrastructure.

**Eight tools, not eighty.** Tool selection accuracy degrades sharply past 10 tools. Strata collapses its full capabilities into 8 tools that map to cognitive intents: store, recall, search, forget, log, branch, history, status.

## The Cognitive Architecture

The hippocampus doesn't work by magic. It's a layered structure — molecular, cellular, circuit, system — where each layer provides the substrate the next one builds on. Skip a layer and the whole thing collapses. Strata follows the same principle.

**Layer 1: Storage primitives.** KV, JSON documents, events, vectors, state cells. These are the raw neural substrate — individual neurons that can hold and fire. This layer exists and works today.

**Layer 2: Cross-primitive relationships.** Explicit, typed, traversable connections between any data across any primitive. This is the wiring between neurons. Without it, every piece of data is an island. With it, you have a connected structure that can be reasoned over. A patient record (JSON) links to lab results (KV) links to clinical notes (vectors) links to diagnosis events — and those relationships are branchable, versionable, and searchable like everything else. This is what's being built next.

**Layer 3: Intelligent retrieval.** Search that goes beyond keyword matching and vector similarity. Three additional scoring signals feed into retrieval: graph proximity (PageRank and traversal distance — data that's structurally connected to what you're looking at ranks higher), temporal reinforcement (data that gets accessed frequently stays salient, data that's never recalled decays — spaced repetition applied to database retrieval), and learned query patterns (the query expansion and reranking models drift based on what the system gets asked repeatedly, so retrieval improves with use). This layer also includes automatic association — the system infers and maintains relationships as a side effect of normal operations, writing edges into the layer 2 graph without being asked. None of this works without the correct mechanical foundation underneath. Without a fast, branchable graph structure, inferred associations have nowhere to go. Without the event log capturing access patterns, there's no signal to learn from.

**Layer 4: Cognitive interface.** The agent says "remember this" and Strata handles storage, keying, categorization, and relationship wiring. The agent says "what do we know about X" and Strata traverses associations, pulls from multiple primitives, and synthesizes. This is where intelligence at the interface layer belongs — not as the storage layer, but on top of correct mechanical and retrieval foundations.

Each layer is useless without the one below it. Layer 4 without layer 2 is an LLM hallucinating relationships. Layer 3 without layer 2 is inferred associations with nowhere to put them and learned patterns with no graph to traverse. You can't skip to the cognitive interface by throwing an LLM at unstructured data and calling it intelligence. That's what everyone else is doing and it's brittle.

The path is: get the wiring right (layer 2), then build intelligent retrieval on top (layer 3), then earn the right to expose a cognitive interface (layer 4).

## Research Agenda

Layers 3 and 4 require sustained research. The following areas are active or planned:

**Beyond hybrid search.** RRF over BM25 + vector similarity is a solid baseline, but it's a rank-level heuristic that doesn't understand the query, the data, or the relationships between results. Strata needs to move toward retrieval models that reason about what to retrieve, not just pattern-match — retrieval-augmented language models (RLMs) that treat retrieval as a reasoning step rather than a lookup. Document chunking with overlap is table stakes; the real question is how retrieval interacts with the graph layer.

**Graph-informed scoring.** Once layer 2 exists, graph signals become a natural input to search. PageRank-style authority scoring — a document referenced by many other documents ranks higher. Traversal proximity — results that are structurally close to the query context rank higher than isolated matches with the same BM25 score. This is where the graph layer pays for itself in retrieval quality, not just relationship queries.

**Temporal reinforcement and decay.** Spaced repetition applied to database retrieval. Data that gets accessed frequently maintains high retrievability; data that's never recalled decays in ranking (not deleted — just deprioritized). The event log already records access patterns. The missing piece is an FSRS-style scheduler that converts access history into a retrieval score signal, and feeds that into the search pipeline alongside BM25, vector similarity, and graph proximity.

**Adaptive query expansion and reranking.** The query expansion and reranking models should not be static. They should drift based on what the system gets asked — learning which expansions lead to results that get used, which reranking decisions correlate with agent satisfaction. This is online learning at the retrieval layer: the base models are general, but each Strata instance develops its own retrieval personality tuned to its workload. The event log provides the feedback signal; the question is how to close the loop without catastrophic drift.

**Learning from the ecosystem.** The agent infrastructure ecosystem is moving fast. Projects like OpenClaw demonstrate that the community is already building extensions to bolt on relationships, temporal knowledge graphs, and multi-layer memory architectures on top of basic storage — exactly the capabilities Strata provides natively. The gap between what agents need and what storage provides is being filled by duct tape. Strata's opportunity is to provide the integrated foundation that makes the duct tape unnecessary.

## The Narrative

Stateless agents do tasks. Stateful agents do work.

But the right state layer isn't a database with a friendlier API. It's not a vector store marketed as "memory." It's a cognitive component — the hippocampus and prefrontal cortex that LLMs are missing. Persistence, association, temporal context, safe experimentation, decision tracking.

AI has the cortex. Strata is the rest.
