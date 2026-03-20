# Composable retrieval pipelines: the state of the art in 2025

**No system today can automatically discover optimal retrieval pipeline topologies.** Every major framework — DSPy, Haystack, LlamaIndex — lets users compose retrieval operators from interchangeable parts, but the pipeline *structure* remains hardcoded by the developer. The closest analog to a formal "retrieval algebra" is PyTerrier's operator-overloading model with eight composable operators and pattern-matching optimization, but nothing approaches the maturity of relational algebra's cost-based query planning. Meanwhile, emerging systems like RAGSmith and AutoRAG have begun evolutionary and greedy searches over pipeline configurations, and lightweight rerankers now make sophisticated multi-stage pipelines viable even on CPU-only embedded hardware. The field sits at an inflection point: the building blocks exist, but the optimizer that assembles them automatically does not.

---

## Three frameworks, three composition philosophies

The three dominant open-source frameworks for composable retrieval — **DSPy**, **Haystack**, and **LlamaIndex** — share a common goal but differ sharply in abstraction level and what they optimize.

**DSPy** (Stanford, 28K+ GitHub stars) adopts a PyTorch-inspired define-by-run model. Users declare modules in `__init__` and compose them with arbitrary Python control flow in `forward`. Retrieval is a thin `dspy.Retrieve(k)` wrapper that dispatches to a globally configured backend — ColBERTv2, ChromaDB, Pinecone, FAISS, Neo4j, or any of **18+ supported retrieval backends**. The power of DSPy lies not in retrieval-specific operators but in its optimizers: MIPROv2 (Bayesian optimization over prompts and demonstrations), BetterTogether (joint prompt + weight optimization), and the newest GEPA (evolutionary prompt optimization, July 2025). These optimizers tune the *parameters* within a fixed pipeline — instructions, few-shot examples, LM weights — but **cannot alter the pipeline topology itself**. There is no built-in module for query expansion, reranking, or fusion; users must compose these from general-purpose `ChainOfThought` modules and custom Python code. For embedded deployment, DSPy works with Ollama + local FAISS via `dspy.retrievers.Embeddings`, though the LM still runs as a local HTTP server rather than truly in-process.

**Haystack 2.x** (deepset, stable at v2.26) takes a more structured approach with typed, explicitly wired components. Every operator is a `@component`-decorated class with declared input/output types, connected via `pipe.connect("embedder.embedding", "retriever.query_embedding")`. This explicit wiring enables type validation at connection time and supports **directed multigraphs with loops** — a significant advance over DAG-only systems. Haystack provides the richest set of built-in retrieval operators: `DocumentJoiner` with RRF/merge/concatenate modes, `TransformersSimilarityRanker` for cross-encoder reranking, `QueryExpander` (added June 2025) for LLM-driven multi-query expansion, `ConditionalRouter` for Jinja2-based branching, and `SuperComponent` for wrapping entire sub-pipelines as single components. Pipelines serialize to YAML natively. For embedded use, `InMemoryDocumentStore` + `SentenceTransformersTextEmbedder` + `HuggingFaceLocalChatGenerator` runs fully offline. However, **no automatic topology optimization exists** — pipeline structure is entirely manual.

**LlamaIndex** has undergone the most dramatic evolution. Its `QueryPipeline` (launched January 2024) provides DAG-based composition with `add_modules()` and `add_link()`, supporting vector retrieval, BM25, knowledge graph traversal, `QueryFusionRetriever` (RRF-based), and various rerankers as composable nodes. But QueryPipeline's DAG-only constraint (no cycles) and complex execution semantics led to its **replacement by Workflows in August 2024** — an event-driven architecture using `@step`-decorated async functions triggered by typed events. Workflows support cycles, explicit control flow, and agentic patterns, but sacrifice the declarative composability of the DAG model. An experimental `ParamTuner` optimizes hyperparameters (chunk size, top-k) within fixed topologies, but **pipeline structure is not automatically optimizable**. LlamaIndex runs fully locally via Ollama, llama-cpp-python, and HuggingFace embeddings.

| Feature | DSPy | Haystack 2.x | LlamaIndex |
|---|---|---|---|
| Composition model | Python control flow | Typed component graph | DAG (legacy) → Event-driven Workflows |
| Built-in retrieval operators | Retrieve only (18+ backends) | Retrievers, Joiners, Rankers, Routers, Expanders | Retrievers, Fusion, Rerankers, KG traversal |
| Query expansion | DIY via ChainOfThought | Built-in QueryExpander (2025) | Via LLM prompt + QueryFusionRetriever |
| Fusion | DIY | DocumentJoiner (RRF, merge, concat) | QueryFusionRetriever (RRF, simple) |
| Reranking | DIY | TransformersSimilarityRanker, LLMRanker | CohereRerank, SentenceTransformerRerank, RankGPT |
| Topology optimization | ❌ (prompt/weight only) | ❌ | ❌ (hyperparameters only) |
| Loop/cycle support | Via Python control flow | ✅ Native (multigraph) | ❌ QueryPipeline; ✅ Workflows |
| Serialization | No standard format | YAML (built-in) | Incomplete |
| Embedded/local | ✅ Ollama + FAISS | ✅ InMemory + HF Local | ✅ Ollama + llama-cpp |

---

## PyTerrier is the closest thing to a retrieval algebra

The concept of a formal "retrieval algebra" — a closed set of composable operators with proven algebraic laws and cost-based optimization, analogous to Codd's relational algebra — **does not exist as a published formalization**. But PyTerrier, developed by Craig Macdonald and Nicola Tonellotto (ICTIR 2020, extended CIKM 2021), provides the most developed practical realization.

PyTerrier defines **transformers** as the fundamental unit — functions mapping queries and result sets to transformed queries and result sets. Eight operators compose these transformers using Python operator overloading: `>>` (sequential composition), `+` (CombSUM linear combination), `*` (scalar weighting), `**` (feature union for learning-to-rank), `|` (set union), `&` (set intersection), `%` (rank cutoff), and `^` (concatenation). The paper uses relational algebra notation — natural joins, grouping with aggregation, selection — to formally define each operator's semantics. A typical pipeline reads naturally: `BM25 % 1000 >> Bo1QueryExpansion >> BM25 % 1000 >> BERT_reranker % 10`.

Critically, PyTerrier implements **DAG-based pipeline optimization** via the MatchPy pattern-matching library. The `compile()` method applies rewriting rules: fusing rank cutoff with dynamic pruning (enabling BlockMaxWAND, yielding **up to 95% speedup**), and merging feature extraction into single "fat postings" passes (**up to 93% speedup**). Modern PyTerrier (v0.13+) adds fusion protocols — `SupportsFuseLeft`, `SupportsFuseRight`, `SupportsFuseRankCutoff` — so transformers can declare how they compose with adjacent operators.

However, PyTerrier's optimization is **rule-based pattern matching, not cost-based optimization** with cardinality estimation. No formal algebraic laws (commutativity, associativity, distributivity) are proven for the operators. The operators are not strictly closed — types vary across transformer classes. This remains the key gap: nobody has built the retrieval equivalent of a database query optimizer that estimates result set sizes, models operator costs, and selects between equivalent execution plans.

Other relevant academic work includes the **Indri/Galago inference network** (Turtle & Croft, 1990–2012), which defines rich within-query operator composition (`#combine`, `#sdm`, `#od`, `#filreq`) but operates at the scoring-function level rather than the pipeline level. **GeeseDB** (Kamphuis & de Vries, DESIRES 2021) bridges IR and relational databases by expressing BM25 as SQL queries over DuckDB, directly leveraging relational algebra. And the **AOP system** (CIDR 2025) introduces cost-based pipeline optimization for LLM-augmented data processing, the most recent work applying query-plan-style reasoning to retrieval-involving pipelines.

---

## Automatic pipeline structure search is nascent but accelerating

The question of whether anyone is doing "NAS for retrieval" — automatically searching over pipeline topologies rather than just hyperparameters — has a nuanced answer. **No system searches over arbitrary DAG topologies**, but several systems search over module selection within fixed stage sequences.

**AutoRAG** (Marker Inc., Korea, 2024) is the closest practical analog to NAS for retrieval. It organizes pipelines into fixed stages — query expansion → retrieval → passage augmentation → reranking → prompt creation → generation — and applies **greedy stagewise search** to select the best module at each node. It searches over BM25 vs. dense vs. hybrid retrieval, multiple embedding models, reranker choices (UPR, TART, monoT5), and hyperparameters. The greedy approach scales linearly but may miss synergistic configurations.

**RAGSmith** (Kartal et al., November 2025) pushes further with **genetic algorithms over 46,080 feasible pipeline configurations** across nine technique families. It explores ~0.2% of the search space (~100 candidates) to find configurations that outperform naive RAG by **+3.8% average across six domains**. A key finding: a robust backbone emerged — vector retrieval plus post-generation reflection/revision — with domain-dependent auxiliary choices. Passage compression was never selected by the optimizer. But RAGSmith's pipeline topology remains a fixed linear sequence of nine stages.

**Self-RAG** (Asai et al., ICLR 2024 Oral) takes a different approach: training the LM itself with special reflection tokens (`Retrieve`, `ISREL`, `ISSUP`, `ISUSE`) that let it dynamically decide *when* to retrieve, *whether* passages are relevant, and *whether* its output is faithful. The pipeline structure is hardcoded (generate → detect need → retrieve → continue), but retrieval frequency and filtering are learned. **Adaptive-RAG** (NAACL 2024) trains a lightweight classifier to route queries to no-retrieval, single-step, or multi-hop strategies based on predicted complexity. **DRAGIN** (ACL 2024 Oral, Tsinghua) detects retrieval needs in real-time based on token uncertainty and self-attention patterns, requiring no fine-tuning.

**ARES** (Stanford, NAACL 2024) provides the evaluation backbone that could enable automated pipeline optimization. It fine-tunes lightweight DeBERTa-v3-Large judges on synthetic data to score **context relevance**, **answer faithfulness**, and **answer relevance**, then applies Prediction-Powered Inference for confidence intervals using as few as 50 human annotations. ARES achieved Kendall's τ of **0.91 for context relevance** ranking, significantly outperforming RAGAS. While ARES doesn't optimize pipelines itself, its precise evaluation could serve as the objective function for any automated search system — a connection no published work has formally made yet.

| System | What it searches over | Method | Topology | Key limitation |
|---|---|---|---|---|
| AutoRAG | Modules + hyperparameters per stage | Greedy stagewise | Fixed linear | Greedy; misses synergies |
| RAGSmith | Full pipeline configs (46K space) | Genetic algorithm | Fixed 9-stage linear | No branching/looping |
| AutoRAG-HP (Microsoft) | Hyperparameters only | Multi-armed bandit | Fixed | No topology search |
| Self-RAG | When/whether to retrieve | Learned reflection tokens | Fixed with adaptive retrieval | Requires LM fine-tuning |
| Adaptive-RAG | Strategy routing per query | Trained classifier | 3 fixed strategies | Only 3 options |
| SmartRAG (ICLR 2025) | Joint module optimization | Reinforcement learning | Fixed | No topology search |

---

## Fusion methods: RRF dominates, but alternatives matter at the margins

**Reciprocal Rank Fusion** (Cormack, Clarke & Büttcher, SIGIR 2009) remains the industry default for combining retrieval results, and for good reason: it requires no score normalization, no training data, no parameter tuning (k=60 works universally), and handles heterogeneous scoring systems naturally. Every major vector database — Elasticsearch, OpenSearch, Qdrant, Weaviate, Pinecone — implements RRF as its standard hybrid search fusion method.

The classical alternatives — **CombSUM** (sum of normalized scores) and **CombMNZ** (CombSUM × count of systems returning the document), both from Fox & Shaw (1993–1994) — exploit score magnitude information that RRF discards, but depend heavily on normalization quality. When scoring systems use incomparable scales (BM25 vs. cosine similarity), CombSUM/MNZ can catastrophically fail. **Condorcet fusion** (Montague & Aslam, CIKM 2002) applies pairwise majority voting from social choice theory but has O(n²·m) complexity and degrades when component systems vary in quality — Cormack et al. showed RRF outperforms it significantly. **Borda count** uses linear rank-to-score transformation; it's simpler than RRF but less selective at top ranks.

More sophisticated approaches include **probabilistic fusion** methods (ProbFuse, SlideFuse) that learn per-position relevance probabilities from training queries, and **LambdaMART-based learned fusion** where each ranker's score becomes a feature in a gradient-boosted tree optimizing NDCG directly. The most interesting neural approach is **Fusion-in-T5 (FiT5)** (LREC-COLING 2024), which uses global attention layers in a T5 model to enable cross-document feature fusion, achieving **NDCG@10 of 0.776 on TREC DL'19** — far above standard cross-encoders (~0.701) with only ~4.5% additional inference cost.

Recent 2024–2025 developments push fusion further. **Exp4Fuse** (ACL Findings 2025) fuses original queries with LLM-expanded queries, achieving up to +8.7 NDCG points. **Dynamic Weighted RRF** adapts per-query weights based on query specificity, reducing hallucination rates in RAG. **Inverse Square Rank (ISR)** fusion (1/rank²) provides sharper selectivity than RRF for long result lists. And **unified dense-sparse indexing** (Zhang et al., 2024) avoids separate-then-merge entirely, improving hybrid accuracy by up to 9%.

For embedded deployment, RRF is the clear winner: it's trivial to implement (a few lines of code), requires zero training, and adds negligible latency. When labeled data is available, a simple convex combination with tuned weight α offers the best quality/complexity tradeoff.

---

## Embedded reranking is now production-viable on CPU

The most practical advance for embedded retrieval systems is the availability of high-quality rerankers that run efficiently on commodity CPUs. The landscape spans five tiers of increasing quality and cost.

**FlashRank** is purpose-built for embedded reranking — no PyTorch or Transformers dependency, pure ONNX Runtime inference. Its default "Nano" model (`ms-marco-TinyBERT-L-2-v2`, **~4MB**) delivers blazing-fast CPU inference at ~5–15ms for 10–20 passages. The "Small" tier (`ms-marco-MiniLM-L-12-v2`, ~33MB) provides significantly better quality while remaining CPU-friendly at ~30–100ms per query. FlashRank is the recommended starting point for serverless, edge, and air-gapped deployments.

**ONNX-optimized cross-encoders** via Sentence Transformers v4.1 (2025) deliver **2–3× speedup** over PyTorch on CPU, with INT8 dynamic quantization pushing to **~3× speedup** with negligible quality loss. Pre-exported ONNX models for ~340 cross-encoders are available on HuggingFace. The sweet spot is `cross-encoder/ms-marco-MiniLM-L-6-v2` (22.7M parameters) with ONNX INT8 quantization: ~50–80ms for 100 passages on CPU, competitive quality.

For GPU-equipped embedded systems, **gte-reranker-modernbert-base** (Alibaba, 149M parameters) is remarkable: it matches the **83% Hit@1** of the 8× larger nemotron-rerank-1b (1.2B) at only ~165ms per 100 documents. This ModernBERT-based architecture represents the new quality frontier for medium-sized models. The **mxbai-rerank-v2** family (mixedbread.ai, 2025) trained with reinforcement learning (GRPO) achieves current open-source SOTA on BEIR at **NDCG@10 of 57.49** (1.5B variant).

**ColBERT's late-interaction model** occupies a unique niche: precomputed per-token document embeddings enable reranking via MaxSim with **180× fewer FLOPs** than full cross-encoders at depth k=10. The `answerai-colbert-small-v1` (130MB, 96-dim, ONNX-ready) makes this viable for embedded use. ColBERT excels when reranking larger candidate sets (hundreds to thousands) where cross-encoders become prohibitively slow.

LLM-based rerankers (RankGPT, RankZephyr, BGE-Gemma-2B) remain **impractical for CPU-only embedded deployment** — even the smallest viable models add seconds of latency. Feature-based rerankers (LambdaMART via XGBoost/LightGBM) offer sub-millisecond inference with <1MB models but require hand-crafted features and cannot do semantic understanding.

---

## Configuration languages and DSLs for retrieval pipelines

How users specify retrieval pipelines varies significantly across systems, reflecting different design philosophies around the declarative-versus-imperative spectrum.

**Haystack** offers the most mature declarative approach: pipelines serialize to YAML with full component configurations, connection maps, and secret handling (API keys as environment variable references). The `SuperComponent` pattern wraps entire sub-pipelines as reusable components. **PyTerrier** uses Python operator overloading as an embedded DSL — `BM25 >> RM3 >> BM25 >> BERT % 10` reads as a declarative pipeline specification while remaining executable Python. **DSPy** is purely imperative: pipelines are Python classes with `forward` methods, composed via standard control flow. **LlamaIndex QueryPipeline** offers both sequential chains (`QueryPipeline(chain=[...])`) and explicit DAG construction, but its successor Workflows use imperative event-driven patterns.

No visual pipeline editor exists in the open-source ecosystem, though **Haystack Enterprise Platform** (deepset's commercial offering) provides a visual pipeline builder. **FlashRAG** (Renmin University, WWW 2025) provides a configuration-driven approach for benchmark reproduction, supporting sequential, conditional, branching, and loop pipeline patterns through its modular toolkit. The **Hayhooks** project serves Haystack pipelines as REST APIs with OpenAI-compatible chat completion endpoints and MCP server support.

---

## The missing piece: cost-based retrieval plan optimization

The most significant gap in the field is the absence of **cost-based retrieval plan optimization** — an optimizer that, given a set of available retrieval operators, a corpus, and a query workload, automatically determines the optimal pipeline topology. Database systems solved this decades ago with cardinality estimation, cost models, and plan enumeration. Retrieval pipelines today are where databases were before System R.

The technical prerequisites are falling into place. PyTerrier demonstrates that retrieval operators can be formalized with relational-algebra-inspired semantics and optimized via rewriting rules. ARES shows that pipeline quality can be evaluated automatically with statistical confidence. AutoRAG and RAGSmith prove that searching over pipeline configurations yields meaningful improvements. The AOP system (CIDR 2025) brings cost-based reasoning to LLM-augmented pipelines. What's missing is the synthesis: a system that combines formal operator semantics, cost models for retrieval operators (latency, memory, quality estimates), cardinality estimation for result set sizes through pipeline stages, and search algorithms that explore the space of equivalent pipeline plans.

The path forward likely requires three developments. First, a **formal retrieval algebra** with proven equivalence rules — extending PyTerrier's practical operators with algebraic laws that enable principled plan rewriting. Second, **cost models** that predict operator latency and quality as a function of corpus statistics, query characteristics, and hardware constraints. Third, **learned optimizers** — potentially using the reinforcement learning approaches already applied to database query optimization — that select pipeline plans based on predicted quality-cost tradeoffs. The building blocks exist across database systems research, IR, and ML optimization. The field is waiting for someone to assemble them.

---

## Conclusion

The composable retrieval pipeline ecosystem in 2025 is rich in building blocks but poor in automatic assembly. **Haystack provides the most complete set of composable retrieval operators** (retrieval, expansion, fusion, reranking, routing) with typed connections and YAML serialization. **DSPy offers the most powerful optimization** of pipeline parameters (prompts, demonstrations, weights) but treats retrieval as a black-box external function. **LlamaIndex has the broadest retrieval integration surface** but is transitioning from DAGs to event-driven Workflows, sacrificing some declarative composability. **PyTerrier remains the only system with formal operator semantics and pipeline-level optimization**, though limited to rule-based rewriting.

For practitioners building embedded retrieval systems today, a practical high-quality stack is: BM25 + dense retrieval fused via RRF, reranked with an ONNX-optimized MiniLM cross-encoder or FlashRank, orchestrated through Haystack or DSPy. For researchers, the most impactful open problems are cost-based retrieval plan optimization, automatic pipeline topology search beyond fixed linear stages, and formal algebraic foundations that would make retrieval pipeline optimization as principled as database query optimization has been for forty years.