# Graph-augmented retrieval: a deep survey of systems, methods, and evidence

**Graph-augmented retrieval delivers substantial gains for multi-hop reasoning and global thematic queries — but consistently underperforms standard vector retrieval on simple factoid questions, at 100–1000× higher indexing cost.** The 2023–2026 period has produced a wave of systems combining knowledge graphs with RAG pipelines, led by Microsoft's GraphRAG, HippoRAG, RAPTOR, and at least a dozen other architectures. The empirical evidence is now clear: graph augmentation is not a universal upgrade but a task-specific capability. The strongest gains appear on entity-dense, multi-hop, and global synthesis queries, while simple single-hop retrieval sees no benefit or even degradation. The most promising direction emerging from the literature is hybrid systems that route queries to graph-based or vector-based retrieval depending on complexity, combined with cost-reduction techniques like LazyGraphRAG that cut indexing expenses by three orders of magnitude.

---

## Microsoft's GraphRAG builds communities from entities, then summarizes them

GraphRAG (arXiv 2404.16130, Edge et al., April 2024) introduced the most architecturally complete graph-augmented retrieval system to date. Its key innovation is a **six-phase indexing pipeline** that converts unstructured text into a hierarchical community structure, enabling both entity-specific and corpus-wide queries.

The pipeline works as follows. Documents are first chunked into TextUnits (default **1,200 tokens**, though 600 tokens proved optimal for entity extraction recall). An LLM — originally GPT-4 Turbo — processes each chunk to extract entities (name, type, description) and relationships (source, target, description) using few-shot prompted extraction. A "gleaning" step re-prompts the LLM to identify missed entities, yielding measurable recall improvements at one additional pass. Subgraphs from each chunk are merged: entities sharing the same name and type have their descriptions combined, then the LLM summarizes these arrays into single concise descriptions. This produces richly described nodes and edges rather than traditional sparse triples.

The **Leiden community detection algorithm** then partitions this entity-relationship graph hierarchically. Applied recursively via the `graspologic` Python library, it discovers communities at Level 0 (broadest thematic clusters) through progressively finer sub-communities until reaching leaf partitions. Each level provides a mutually exclusive, collectively exhaustive partition of all nodes. For each community at every level, the LLM generates a Community Report containing an executive overview, key entities, relationships, and claims. This is the structural heart of GraphRAG — it converts a flat corpus into a navigable hierarchy of themed summaries.

**Two primary query modes** exploit this structure differently. Local search identifies query-relevant entities via vector similarity, expands to their graph neighborhood, retrieves associated source chunks, and generates answers from this combined structured and unstructured context — effective for "who/what/when/where" questions. Global search uses a **map-reduce approach** over community summaries: all reports at a selected hierarchy level are shuffled into context-window-sized groups, the LLM generates partial answers with helpfulness scores for each group, and a reduce step synthesizes the top-scoring partial answers into a final response. This ensures every corner of the corpus is considered for thematic queries like "What are the main themes in this dataset?"

Evaluation on podcast transcripts and news articles showed **70–80% win rates** over naive RAG on comprehensiveness and diversity metrics (LLM-as-judge, p < 0.001). Root-level community summaries achieved competitive performance at **~2–3% of the token cost** per query compared to full source text summarization. However, an independent unbiased evaluation (2025) found that "GraphRAG performance gains are more moderate than reported previously" when using graph-text-grounded question generation rather than the original evaluation methodology.

Subsequent improvements dramatically reduced costs. **Dynamic community selection** (November 2024) prunes irrelevant community reports before traversal, achieving **77% cost reduction** while maintaining quality. **LazyGraphRAG** (November 2024) eliminated pre-computed summaries entirely, reducing indexing cost to **0.1% of full GraphRAG** — a 1000× reduction — while matching or exceeding quality at query time. GraphRAG 1.0 (December 2024) shipped with 80% parquet disk savings and dedicated vector store integration. A recent paper on core-based hierarchies noted that modularity optimization on sparse graphs admits exponentially many near-optimal partitions, raising questions about the determinism of Leiden-based community detection and proposing k-core decomposition as an alternative.

---

## HippoRAG models hippocampal memory with knowledge graphs and PageRank

HippoRAG (arXiv 2405.14831, Gutiérrez et al., NeurIPS 2024) drew directly from neuroscience — specifically Teyler and Discenna's hippocampal memory indexing theory — to create a retrieval system with three interacting components. The **neocortex analogue** is an instruction-tuned LLM (GPT-3.5-turbo) that performs open information extraction, producing schemaless knowledge graph triples from each passage. The **parahippocampal region analogue** is a retrieval encoder (Contriever or ColBERTv2) that detects synonymy between KG nodes by computing cosine similarity and adding edges when similarity exceeds a threshold (τ = 0.8). The **hippocampal analogue** is the knowledge graph itself plus Personalized PageRank, which mimics the densely connected CA3 sub-region's ability to complete partial memory cues.

At query time, the LLM extracts named entities from the query, the retrieval encoder links these to the most similar KG nodes, and **Personalized PageRank** (damping factor 0.5) distributes probability mass from these query nodes through the graph. The resulting probability vector is multiplied by a passage-node matrix to rank passages. This achieves **single-step multi-hop retrieval** — a critical advantage over iterative methods like IRCoT, which require multiple retrieval-generation cycles.

HippoRAG introduced a valuable distinction between **path-following** multi-hop QA (where a specific reasoning chain connects entities, solvable by iterative retrieval) and **path-finding** QA (where the correct path must be identified among many possibilities, requiring the associative structure of a knowledge graph). On multi-hop benchmarks, HippoRAG with ColBERTv2 achieved average Recall@5 of **72.9%** versus 65.6% for ColBERTv2 alone and 62.3% for RAPTOR — up to a **20% improvement** on 2WikiMultiHopQA. It was **10–30× cheaper** and **6–13× faster** than IRCoT iterative retrieval.

**HippoRAG 2** (arXiv 2502.14802, February 2025, targeting ICML 2025) addressed a key weakness of the original: degraded performance on simple factual memory tasks. By integrating deeper passage-level signals into the PPR framework and more effective online LLM usage, it achieved a **7% improvement** on associative memory tasks over state-of-the-art embedding models while maintaining strong performance on factual memory (NaturalQuestions, PopQA) and sense-making (NarrativeQA) — the first graph-augmented system to demonstrate robustness across all three memory dimensions.

---

## RAPTOR builds recursive summarization trees from clustered text

RAPTOR (arXiv 2401.18059, Sarthi et al., ICLR 2024) took a different structural approach: rather than building a knowledge graph, it constructs a **hierarchical tree** through iterative cycles of embedding, clustering, and summarization. Text is segmented into ~100-token chunks, embedded with SBERT, clustered using **Gaussian Mixture Models** (with UMAP dimensionality reduction and BIC-based cluster count selection), summarized by an LLM (GPT-3.5-turbo), re-embedded, and the process repeats recursively until further clustering is infeasible.

The key design choice is **soft clustering via GMMs**, allowing text segments to belong to multiple clusters — important since passages often relate to multiple topics. A two-step hierarchical clustering first identifies global themes (large UMAP `n_neighbors`), then performs local clustering within each global cluster. Average cluster size is approximately 6.7 nodes. The resulting tree has leaf nodes (original chunks) at the bottom and progressively more abstract summaries at higher levels, with a compression rate of roughly **72%** per level.

For retrieval, the **collapsed tree** strategy — flattening all nodes across all levels into a single searchable set and selecting the top-k by cosine similarity — consistently outperformed layer-by-layer tree traversal. This allows the system to dynamically retrieve at the correct granularity level for each query. On QuALITY, RAPTOR + GPT-4 achieved **62.4% accuracy** (versus 57.3% for DPR), representing a **20% absolute improvement** over previous best results. On QASPER, RAPTOR improved F1 from 53.0% (DPR + GPT-4) to **55.7%**. However, HippoRAG's paper showed that on multi-hop QA specifically, RAPTOR underperformed HippoRAG, and HippoRAG 2 noted that RAPTOR's "LLM summarization mechanism" introduces noise that can degrade performance on some task types. RAPTOR also cannot incrementally update its tree when documents change — the full tree must be recomputed.

---

## A growing ecosystem of graph-augmented retrieval systems

Beyond the three flagship systems, the 2023–2026 period produced a rich ecosystem of graph-augmented approaches, each with distinct innovations:

**LightRAG** (arXiv 2410.05779, Guo et al., October 2024) offers a lightweight alternative to GraphRAG with **dual-level retrieval** — low-level for entity-specific queries, high-level for thematic ones — and critically, an **incremental update algorithm** that appends new knowledge via simple graph union operations rather than requiring full reindexing. It achieves what takes 610,000 tokens in GraphRAG retrieval with fewer than 100 tokens, and roughly 30% latency reduction over standard RAG.

**G-Retriever** (arXiv 2402.07630, He et al., NeurIPS 2024) is the first RAG approach for general textual graphs, enabling conversational QA over graph-structured data. Its key innovation is formulating subgraph retrieval as a **Prize-Collecting Steiner Tree** optimization problem, which retrieves connected subgraphs covering maximally relevant nodes while minimizing cost. A Graph Attention Network encodes the retrieved subgraph, and both graph embeddings (soft prompts) and textual descriptions are fed to the LLM.

**GRAG** (arXiv 2405.16506, Hu et al., NAACL 2025 Findings) addresses retrieval over pre-existing graph-structured documents (citation networks, social media) rather than constructing graphs from flat text. It indexes k-hop ego-graphs for each node, ranks them by query relevance, applies soft pruning, and generates answers through **dual prompting** — both text and graph views. Where GraphRAG builds entity KGs and uses community summarization, GRAG preserves existing topology.

**GNN-RAG** (arXiv 2405.20139, Mavromatis & Karypis, ICLR 2025) combines GNN reasoning with LLM language understanding for knowledge graph QA. A GNN reasons over a dense KG subgraph to identify answer candidates, then shortest paths connecting question entities to answers are verbalized into natural language for RAG-based generation. It outperforms or matches GPT-4 with a 7B tuned LLM, excelling on multi-hop questions by **8.9–15.5% F1**.

**MedGraphRAG** (arXiv 2408.04187, Wu et al., ACL 2025) builds a three-tiered graph for medical QA: user documents (e.g., EHR data) at top, foundational medical knowledge (4.8M papers) in the middle, and controlled vocabularies (UMLS) at the bottom. It achieves new state-of-the-art on the medical LLM leaderboard. **KG-RAG for biomedicine** (Soman et al., Bioinformatics 2024) integrates the SPOKE knowledge graph (27M+ nodes, 53M+ edges) with LLM prompting, delivering a **71% boost** in Llama-2 performance on biomedical MCQ.

**StructRAG** (arXiv 2410.08815, ICLR 2025) introduces cognitive-theory-inspired **hybrid structure routing**, dynamically selecting among five knowledge structure types (tables, graphs, algorithms, catalogs, chunks) per query — recognizing that no single structure is optimal for all tasks.

---

## Entity extraction has shifted decisively to LLM-based pipelines

The dominant modern approach for constructing knowledge graphs from text uses LLMs directly for triple extraction, replacing traditional multi-stage NLP pipelines. GraphRAG uses GPT-4 with few-shot prompting and a gleaning step for self-correction. HippoRAG uses open information extraction via GPT-3.5-turbo with 1-shot prompting. LightRAG extracts entities and relationships in a single LLM call per chunk.

The **LangChain LLMGraphTransformer** (used by Neo4j's Knowledge Graph Builder) operates in two modes: a tool-based mode using structured output / function calling to define Node and Relationship schemas, and a prompt-based fallback for LLMs without function calling support. It supports configurable constraints — `allowed_nodes` and `allowed_relationships` — for schema-guided extraction that reduces noise. **KGGen** (arXiv 2502.09956, February 2025) clusters related entities during extraction to reduce KG sparsity, outperforming both OpenIE and GraphRAG's extraction on the MINE benchmark.

The accuracy/recall tradeoff is significant. LLM-based pipelines achieve roughly **89.7% precision and 92.3% recall** with few-shot prompting, but GPT-4 is "more suited as inference assistant rather than few-shot information extractor" — fine-tuned models remain more precise for extraction while LLMs excel at downstream reasoning. Schema-free (open) extraction offers flexibility but produces noisier triples; schema-guided extraction offers precision but limits cross-domain generalization. Dynamic approaches like **ODKE+** (arXiv 2509.04696) generate per-entity-type "ontology snippets" to balance flexibility and precision, processing **150–250K new facts per day** in production.

---

## Graph scoring signals range from PageRank to learned graph embeddings

**Personalized PageRank** is the most widely adopted graph scoring signal, used by HippoRAG, HippoRAG 2, FastGraphRAG, NodeRAG, and TERAG. It distributes probability mass from query-linked seed nodes through the knowledge graph, naturally performing multi-hop exploration in a single step. A related approach, **Diffusion-Aided RAG** (ACL 2025), applies PPR diffusion over a dense-embedding graph where semantically coherent clusters mutually reinforce each other while isolated hits are suppressed — boosting nDCG and MRR.

**Community membership** drives GraphRAG's global search and is used by LightRAG (high-level retrieval) and HiRAG (hierarchical community layers). **Shortest path algorithms** underpin GNN-RAG's verbalized reasoning paths and LEGO-GraphRAG's structure-based retrieval. **Graph embeddings** like TransE (relation as vector translation: h + r ≈ t) and Node2Vec (biased random walks) provide proximity-based retrieval signals, though the Graph of Records paper found Node2Vec produces "unsatisfactory results" for summarization RAG — GNN-based approaches significantly outperform. **GFM-RAG** (arXiv 2502.01113) uses a graph foundation model pretrained on large-scale KG completion tasks as the retriever, claiming more effective multi-hop reasoning than PPR-based approaches.

Node centrality measures (degree, betweenness) are available in graph databases like Neo4j and Memgraph but are not prominently featured in current RAG-specific literature. Their primary role is implicit — high-degree nodes correspond to important entities that naturally surface in graph traversal.

---

## Ontology-guided decomposition and text-to-graph-query approaches are maturing

Systems that translate natural language queries into structured graph queries represent a complementary approach to embedding-based retrieval. **Neo4j's Text2Cypher** pipeline sends user questions plus graph schema to an LLM, which generates Cypher queries executed against the database, with results fed to the LLM for answer generation. Large models show "impressive Cypher pre-training" — fine-tuned CodeLlama-13B achieves **69.2% execution accuracy** on Hetionet, nearly matching GPT-4o's 72.1%. The graphrag.com Pattern Catalog defines a graduated hierarchy: Cypher Templates (predefined, most reliable) → Dynamic Cypher Generation (composable snippets) → Text2Cypher (free-form, least reliable).

For RDF knowledge bases, **SPARQL-LLM** (ACM Transactions on the Web, 2025) provides a production-ready architecture with schema indexing into vector databases, relevant context injection, generation, and validation loops — running up to **36× faster** than agent-heavy approaches. Ontology-guided knowledge graph construction (arXiv 2511.05991) achieved **90% accuracy** on a 20-question benchmark, matching GraphRAG and outperforming vector RAG, by aligning extracted knowledge with predefined ontological schemas. **KAG** (arXiv 2409.13731) integrates logical-form-guided reasoning with structured graph traversals for professional domain knowledge services.

---

## Hybrid vector-graph queries represent the production-ready pattern

The most effective production architecture combines vector similarity search with graph traversal. **Neo4j's VectorCypherRetriever** exemplifies this pattern: vector search finds initial candidate entities, then a Cypher query traverses graph relationships from matched nodes to expand context. This provides two optimization levers unavailable in pure vector search — **breadth** (horizontal traversal across related entities) and **depth** (vertical traversal along relationship chains).

**Reciprocal Rank Fusion (RRF)** is the dominant method for combining ranked lists: `RRF_score(d) = Σ 1/(k + rank_i(d))` with k typically 60. It requires no training and handles different score ranges automatically. Alternatives include weighted linear combination (`hybrid_score = α × vector_score + (1-α) × keyword_score`) and learning-to-rank models trained on relevance labels. The standard multi-stage production pipeline performs broad hybrid retrieval (vector + BM25) first, then re-ranks via cross-encoder or graph expansion, then selects final context for the LLM.

**Allan-Poe** (arXiv 2511.00855) pushes this further with an all-in-one GPU-accelerated graph index unifying dense vector, sparse vector, full-text, and knowledge graph retrieval in a single structure with dynamic fusion. Its finding — dense-only achieves ~0.5 nDCG@10 while three-path fusion reaches 0.56 — quantifies the incremental value of each modality. The consensus from Neo4j and WhyHow.AI is clear: "the winning pattern combines vector search for initial candidates with graph traversal for context expansion. Pure vector fails for relationship questions. Pure graph fails for semantic similarity."

---

## The evidence shows graph augmentation is powerful but task-specific

The most systematic evaluations paint a nuanced picture. **GraphRAG-Bench** (Xiang et al., ICLR 2026) — the most comprehensive dedicated benchmark — reports that GraphRAG achieves **13.4% lower accuracy** on NaturalQuestions and a **16.6% accuracy drop** on time-sensitive queries versus vanilla RAG. On HotpotQA, graph-based retrieval improves reasoning depth by **only 4.5%**, partly because 78.2% of HotpotQA questions are solvable by shallow retrieval. Han et al. (arXiv 2502.11371) found "RAG performs better on single-hop questions and those requiring fine-grained details, whereas GraphRAG is more effective for multi-hop and reasoning-intensive questions." Zhou et al. (PVLDB 2025), comparing 12 graph-based RAG methods across 11 datasets, found that VanillaRAG and ZeroShot remain the most cost-efficient methods, and building rich knowledge graphs requires **up to 40× more tokens** than building trees.

Where graph augmentation shines is unmistakable. On multi-hop QA, recent systems show strong gains: **HopRAG** achieves 76.78% higher answer accuracy versus dense retrievers; **PAR-RAG** delivers +16.39% EM on 2WikiMultiHopQA; **BDTR** improves +11.0% EM over HippoRAG 2 on average. For global thematic queries, GraphRAG wins 70–80% of head-to-head comparisons with naive RAG. In enterprise domains, the Lettria benchmark shows **80% accuracy** for GraphRAG versus 50.83% for traditional RAG across finance, health, industry, and law datasets. Schema-bound queries (KPIs, forecasts) are particularly dramatic — vector RAG scored **0%** in Diffbot's benchmark where GraphRAG scored meaningfully.

The cost story is evolving rapidly. Full GraphRAG indexing with GPT-4 Turbo is expensive — graph extraction accounts for roughly **75% of total indexing cost**, and a 32,000-word book reportedly cost $7. But **LazyGraphRAG reduces indexing to 0.1%** of full GraphRAG cost while matching quality. INSES (2025) demonstrates intelligent routing: on HotpotQA, **86% of queries** are correctly routed to cheap naive RAG, with graph search reserved for the queries that need it. Query-time latency runs approximately **2.3× higher** on average for graph-augmented systems, though local search modes remain competitive.

Ablation studies consistently identify PPR and graph structure as meaningful contributors beyond entity extraction alone. KG²RAG's ablation shows KG-based context organization contributes significantly to precision (+7.9% on HotpotQA) beyond what entity extraction alone provides. However, community detection — while valuable for global queries — is counterproductive for factoid QA and can be eliminated entirely (as LazyGraphRAG and HippoRAG demonstrate) without quality loss for local queries.

---

## Conclusion

The graph-augmented retrieval landscape has matured from a single system (GraphRAG, April 2024) into a rich ecosystem with at least a dozen architecturally distinct approaches. The field's trajectory reveals three important insights that were not obvious from any single paper.

First, **the optimal retrieval architecture depends on query complexity**, and the most promising systems explicitly recognize this through routing mechanisms. The INSES finding that 86% of HotpotQA queries need only shallow retrieval suggests that production systems should invest in query classification before committing to expensive graph traversal, rather than uniformly applying graph augmentation.

Second, **the cost barrier is dissolving faster than expected**. LazyGraphRAG's 1000× indexing cost reduction, combined with HippoRAG's demonstration that single-step PPR can match iterative methods at 10–30× lower cost, means graph augmentation is becoming viable for cost-sensitive deployments. The remaining question is whether LLM-based entity extraction — still the dominant cost center — can be replaced by cheaper NLP-based extraction (as FastGraphRAG attempts) without unacceptable quality loss.

Third, **the strongest evidence for graph augmentation comes from "path-finding" queries** — questions where the correct reasoning path must be identified among many possibilities, rather than followed step-by-step. This is where associative graph structures provide capabilities fundamentally unavailable to vector similarity search, and it maps well to real enterprise use cases: connecting disparate facts across large document collections, synthesizing global themes, and reasoning about entity relationships that span multiple documents. Systems that can identify these query types and selectively apply graph retrieval are likely to dominate production deployments.