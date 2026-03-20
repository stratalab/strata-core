# Self-tuning search: the state of automatic retrieval optimization

**No production system today accepts a set of (query, expected\_results) pairs and automatically discovers the optimal retrieval configuration end-to-end.** This remains an open problem — but the building blocks are largely in place. Academic work on Bayesian optimization for IR parameters dates to 2018, LLM-based evaluation now correlates at **τ ≈ 0.84–0.86** with human judgments, and frameworks like PyTerrier can grid-search over pipeline parameters given relevance labels. The missing piece is integration: nobody has yet shipped a reusable tool that wires together a search configuration space, a Bayesian optimizer, and a standardized evaluation function into one coherent loop. What follows is a comprehensive survey of the research frontier and production landscape across every dimension of this problem.

---

## AutoML and Bayesian optimization for retrieval parameters

The most direct academic attack on automatic retrieval tuning is **Li and Kanoulas (WSDM 2018)**, who proposed Gaussian process–based Bayesian optimization to jointly search over the full hyperparameter space of an IR system — including discrete choices (stemmer, stopword list, retrieval model) and continuous parameters (BM25 **k1** and **b**, number of query expansion terms). On TREC collections, their system outperformed grid search and random search in both final effectiveness and sample efficiency. This remains the seminal paper on the topic and released `pytrec_eval` as a byproduct.

More recent work has shifted toward RAG pipelines. **AutoRAG-HP (EMNLP 2024 Findings)**, from Microsoft, formulates RAG hyperparameter tuning as an online multi-armed bandit problem, introducing a two-level Hierarchical MAB that achieves Recall@5 ≈ 0.8 using only **~20% of the LLM API calls** required by grid search. Separately, **AutoRAG (arXiv 2410.20878, 2024)** takes a modular approach: it organizes RAG pipelines into nodes (query expansion, retrieval, passage augmentation, reranking, prompt creation) and uses a greedy algorithm to select the best module at each stage, evaluated on per-node metrics. Both systems accept query–answer pairs as input and output optimized configurations, making them among the closest practical tools to the auto-configuration vision for RAG specifically.

For hybrid search specifically, **DAT (arXiv 2503.23013, 2025)** abandons fixed fusion weights entirely. Instead of a single α balancing dense and sparse retrieval across all queries, DAT uses an LLM to evaluate the top-1 result from each retrieval method per query and dynamically calibrates the weighting. This per-query approach consistently outperforms fixed-weight hybrids, highlighting that **optimal retrieval configuration is query-dependent**, not just collection-dependent.

On the industry side, the **OpenSearch blog (December 2024)** explicitly recommends Bayesian optimization for hybrid search weight tuning, treating the normalization technique, combination method, and lexical/neural weights as a parameter optimization problem given a judgment list. An earlier but influential approach is **LambdaBM25F (Taylor et al., CIKM 2009)**, which used LambdaRank neural networks to directly optimize BM25F free parameters for NDCG — demonstrating that gradient-based learning-to-rank machinery can tune classical scoring functions.

---

## ARES, LLM-as-judge, and automated evaluation without human labels

### Stanford's ARES framework

**ARES (Saad-Falcon, Khattab, Potts, Zaharia; NAACL 2024)** is a three-stage automated evaluation framework for RAG systems. Stage 1 generates synthetic query–passage–answer triples using an LLM (typically FLAN-T5 XXL), filtering low-quality outputs by checking whether the generated query can retrieve its source passage. Stage 2 fine-tunes lightweight classifier judges (DeBERTa-v3-Large) on these synthetic triples to score context relevance, answer faithfulness, and answer relevance. Stage 3 applies **Prediction-Powered Inference (PPI)** using a small human-annotated validation set (~150 examples) to produce statistically calibrated confidence intervals. Across eight KILT and SuperGLUE tasks, ARES achieves Kendall's τ **0.065 higher** than RAGAS or direct GPT-3.5 judging for ranking RAG configurations. Code is available at `github.com/stanford-futuredata/ARES`.

### The LLM-as-judge debate

A rapidly growing literature examines whether LLMs can replace human relevance assessors. The findings are promising but contested:

- **Thomas et al. (SIGIR 2024)** compared GPT-4 judgments against first-party Bing searcher feedback and found that **LLM labelers outperformed all populations of human labelers** — including trained crowd workers — in predicting real searcher preferences.
- **Faggioli et al. (ICTIR 2023)** applied GPT-3.5 to fully reassess TREC 2021 Deep Learning track runs, achieving Kendall's τ = 0.86 for system rankings on NDCG@10.
- **UMBRELA (Upadhyay et al., ICTIR 2025)** operationalized these findings into an open-source toolkit deployed at TREC 2024's RAG Track across 77 runs from 19 teams. System-level rank correlations exceeded τ = 0.79 across NDCG@20, NDCG@100, and Recall@100.
- However, **Clarke et al. (EVIA 2025)** demonstrated a critical vulnerability: the WaterlooClarke team submitted TREC 2024 runs that explicitly used LLM-generated judgments as a final-stage ranker to game the evaluation. This system ranked first under automatic evaluation but **only fifth under human assessment** — a concrete demonstration of Goodhart's Law.
- **Soboroff (NIST, 2024)** argued that the LLM used for evaluation sets a ceiling: systems retrieving documents the LLM cannot recognize as relevant will be penalized, and LLM false positives tend to be driven by lexical cues rather than true semantic relevance.

The emerging consensus is that LLM judgments are valuable for rapid development-cycle evaluation and for filling judgment "holes" in existing test collections, but should be **calibrated against human gold labels** and not used as the sole evaluation for reusable benchmarks. ARES's PPI approach and **Dietz et al.'s (ICTIR 2025)** guidelines on vigilance tests and automation bias quantification represent best practices.

Other notable evaluation frameworks include **RAGAS (EACL 2024)**, which provides reference-free RAG evaluation via fixed LLM prompts for faithfulness and relevance; **autoqrels (MacAvaney & Soldaini, SIGIR 2023)**, which fills judgment holes using nearest neighbors and LLM prompts; and **RAGElo**, which uses Elo-style tournaments judged by LLMs to rank RAG system variants.

---

## Evaluation metrics and minimum sample sizes

### Which metric for which task

**NDCG** (Järvelin & Kekäläinen, 2002) is the dominant metric in modern IR evaluation. It handles graded relevance, discounts lower positions logarithmically, and normalizes by the ideal ranking, making cross-query averaging meaningful. NDCG@10 is the default on the MTEB Leaderboard and the primary metric for TREC Deep Learning track. **MAP** considers all relevant documents and their positions but requires binary relevance labels; it remains standard for traditional ad-hoc evaluation and provides a full precision-recall summary. **MRR** considers only the rank of the first relevant result, making it ideal for navigational queries and QA but misleading for exploratory search. **Recall@k** measures coverage without regard to ordering within the top-k, making it the preferred metric for first-stage retrieval in two-stage pipelines and for RAG context windows where all relevant passages must be present.

### How many queries are enough?

TREC has used **50 queries** as its standard since 1992, but this is a pragmatic minimum, not a statistical guarantee. Voorhees and Buckley (SIGIR 2002) showed that with 25 queries, an **8–9% absolute MAP difference** is needed for 95% confidence that the system ordering would hold on new queries; with 50 queries, the threshold drops to ~5%. Sanderson and Zobel (SIGIR 2005) found that even with 50 queries, a **15–20% relative difference** is required for 95% confidence without significance testing; when statistical tests are applied (p ≤ 0.05), confidence improves substantially. Spärck Jones and van Rijsbergen (1975) argued that **under 75 queries has limited value** and 250+ is preferable.

For LLM-assisted evaluation, the economics change dramatically. A Vespa case study demonstrated effective calibration with just **26 queries and 90 human-labeled triplets**, then expanded to 386 queries with 10,372 LLM-labeled pairs. The practical recommendation: **50 queries minimum for any meaningful comparison**, **100+ for reliable optimization**, and **statistical significance testing is non-negotiable** regardless of sample size.

---

## Relevance feedback from Rocchio to neural PRF to LLM-generated feedback

### Classical and pseudo-relevance feedback

The **Rocchio algorithm (1971)** remains the foundational approach: move the query vector toward relevant documents and away from non-relevant ones. In practice, most implementations use only positive feedback (γ = 0). **RM3 (Lavrenko & Croft, 2001; Abdul-Jaleel et al., 2004)** replaced geometric vector operations with probabilistic language model estimation, interpolating the original query model with a relevance model estimated from pseudo-relevant documents. RM3 remains one of the strongest PRF baselines in the Anserini toolkit. **Bo1**, from the Divergence from Randomness framework (Amati, 2003), uses Bose-Einstein statistics and is implemented in Terrier. All classical PRF methods share a key limitation: when initial retrieval is poor, assuming the top-k results are relevant causes **topic drift**.

### Neural pseudo-relevance feedback

Dense retrieval has spawned neural PRF variants. **ColBERT-PRF (Wang et al., ACM TWEB 2023)** extracts representative feedback embeddings from pseudo-relevant documents via KMeans clustering and appends them to the query representation, achieving **up to 26% MAP improvement** on TREC 2019 passage ranking over vanilla ColBERT. **ANCE-PRF (Yu et al., CIKM 2021)** concatenates feedback passage text with the query and encodes the combination through a BERT-based query encoder, improving NDCG@10 by 9.3% on DL-HARD's most challenging queries. A comprehensive study by Li et al. (ACM TOIS 2023) found that vector-based PRF improves all seven dense retrievers tested at **1/20th the computation cost** of BM25+BERT reranking.

### LLMs as feedback sources

The most significant recent shift is using LLMs to generate feedback documents instead of relying on corpus retrieval. **HyDE (Gao et al., ACL 2023)** generates a "hypothetical document" from the query using an instruction-following LLM, then encodes it for dense retrieval — achieving performance comparable to fine-tuned retrievers in zero-shot settings. **Generative Relevance Feedback (Mackie et al., SIGIR 2023)** builds RM3-style probabilistic feedback models from LLM-generated text rather than retrieved documents, improving MAP by **5–19%** and NDCG@10 by **17–24%** over standard RM3. In January 2025, **LLM-Assisted PRF** (arXiv 2601.11238) showed that using LLMs to simply *filter* the pseudo-relevant set before RM3 estimation — rather than generating expansion terms — consistently outperforms both standard RM3 and strong reranking models.

Iterative RAG systems represent the modern incarnation of relevance feedback. **Self-RAG (Asai et al., ICLR 2024)** trains models to predict reflection tokens that trigger adaptive retrieval. **FLARE (Jiang et al., EMNLP 2023)** uses generation confidence to proactively retrieve when the model is uncertain. **DeepRetrieval (arXiv 2503.00223, 2025)** uses reinforcement learning with retrieval recall as the reward signal, achieving **60.8% recall** on publication search without supervised data.

---

## How production systems handle search quality today

### Elasticsearch: from manual tuning to agentic optimization

Elasticsearch provides a mature but largely manual relevance tuning stack. The core levers are **function_score** queries (multiplicative boosting with log-scaled business signals), field boosting via `multi_match`, and the **Ranking Evaluation API** (since ES 6.2) for measuring NDCG, MRR, and Precision against judgment lists. The **LTR plugin** (open-source, by OpenSource Connections; also available natively since ES 8.12) trains LambdaMART models externally and deploys them as rerankers. Most significantly, Elastic published in November 2025 an **agentic search autotuning** reference architecture: an LLM agent translates natural language queries into search templates, logs user interactions as automatic judgment lists, retrains an XGBoost LTR model, and redeploys — described as "search that gets smarter automatically, no data scientists needed."

### Vespa: declarative ranking with deep ML integration

Vespa takes a fundamentally different approach. Ranking is expressed as **mathematical expressions in rank profiles** defined in the schema, with native support for multi-stage ranking (first-phase, second-phase per content node, global-phase across nodes). Vespa imports and evaluates ONNX, XGBoost, and LightGBM models directly in ranking expressions. Its **match-features** system logs computed rank features with each hit, enabling training dataset creation for LTR without additional infrastructure. Multiple rank profiles can be defined and switched at query time, making A/B testing trivial.

### Pinecone: limited optimization surface

Pinecone's optimization surface is narrower by design. Tuning levers are limited to embedding model choice, hybrid search **alpha** (balancing dense and sparse scores), reranker selection (BGE, Cohere, or Pinecone's own model), metadata filtering, and chunking strategy. There are no automated tuning capabilities. This reflects Pinecone's position as a managed vector database rather than a full search engine.

### Specialized tools and the relevance engineering ecosystem

The open-source relevance engineering ecosystem includes several important tools. **Quepid** (OpenSource Connections) provides a collaborative UI for creating judgment lists and measuring metrics against Elasticsearch, Solr, OpenSearch, and other backends. **Rated Ranking Evaluator (RRE)** by Sease runs offline evaluation across versioned search configurations with regression tracking. **Querqy** provides a query rewriting engine with a management UI (SMUI) for business rules. Most notable for automatic optimization is **Quaerite (MITRE)**, which uses **genetic algorithms** to search the space of Solr query parameters (field boosts, query types, phrase matching) to optimize IR metrics given judgment lists — making it the closest production-adjacent tool to the auto-configuration vision, though it is relatively unmaintained.

For RAG frameworks, **Haystack** (deepset) offers built-in evaluators for context relevance and faithfulness plus DSPy integration for prompt optimization. **LlamaIndex** provides `RetrieverEvaluator` with synthetic dataset generation and a `ParamTuner` for chunk size optimization.

---

## Data versioning and experiment management for retrieval

**No dedicated tool exists for versioning and branching retrieval experiments.** The current practice is to combine general-purpose ML infrastructure: **DVC** (acquired by lakeFS in November 2025) for versioning corpora and judgment lists, **MLflow 3.0** (with 2025 extensions for generative AI) or **Weights & Biases** for experiment tracking, and RRE's built-in versioned configuration comparison for search-specific regression testing. LakeFS provides git-like branching directly on object storage with zero-copy branching, theoretically enabling parallel retrieval experiments on different index versions, but nobody has built a search-specific workflow around it. The closest integrated approach is Vespa's multiple rank profiles, which allow switching ranking functions at query time without infrastructure changes, and commercial platforms like Coveo and Algolia that offer built-in A/B testing.

---

## The key question: auto-discovering optimal retrieval configuration from examples

The gap between what exists and the full vision — provide (query, expected\_results) pairs, get back the optimal retrieval configuration — can be understood through a taxonomy of what "configuration" means:

**Parameter tuning** (BM25 k1/b, field boosts, fusion weights) is largely solved. PyTerrier's `GridSearch` accepts topics and qrels and exhaustively evaluates parameter combinations. Li and Kanoulas (2018) demonstrated that Bayesian optimization handles this efficiently. The OpenSearch blog prescribes exactly this workflow for hybrid search weight tuning.

**Reranking function learning** (Learning to Rank) is mature and production-deployed. LambdaMART via XGBoost or LightGBM, trained on (query, document, relevance) triples, powers ranking at Bing, Yelp, Wikipedia, and others through the Elasticsearch LTR plugin and Vespa's native model evaluation.

**Pipeline structure selection** — choosing between BM25, dense retrieval, hybrid search, reranking stages, query expansion — remains largely manual. AutoRAG (2024) performs greedy module selection for RAG pipelines. Roitman's unsupervised configuration work (ACM Web Conference 2024) uses Query Performance Prediction to select among retrieval configurations **without any relevance labels at all**, requiring only a sample of queries. Selective Query Processing (Mothe & Ullah, TOIS 2023) adapts configuration on a per-query basis.

**Full end-to-end configuration** — jointly optimizing analyzer chains, tokenizers, field mappings, query type, retrieval model, fusion strategy, and reranking — does not exist as a shipped tool. The practical path to building it would combine **PyTerrier** or the **ES Ranking Eval API** (for pipeline abstraction and evaluation), **Optuna** (for Bayesian optimization over a mixed continuous/categorical space), and **ir\_measures** (for standardized metric computation). The Elastic blog from 2020 demonstrated the core loop of wrapping Optuna-style optimization around the Ranking Eval API; the gap is productizing this into a reusable tool with a broader configuration space definition.

---

## Conclusion

The field of automatic retrieval optimization is fragmented across academic research, open-source tools, and production platforms — with strong solutions for individual subproblems but no unified system. Three developments stand out as most consequential: LLM-based evaluation has made relevance judgment generation **100× cheaper**, removing the annotation bottleneck that historically blocked automated optimization; Bayesian optimization for IR parameters was proven effective in 2018 but never productized; and the RAG ecosystem (AutoRAG, AutoRAG-HP) is independently converging on the same auto-configuration paradigm.

The critical insight is that **the optimization loop itself is straightforward** — the hard part is defining the configuration space. A search system's tunable surface spans dozens of interacting dimensions across indexing, query parsing, retrieval, and reranking, with both continuous and categorical parameters. The most actionable near-term approach is to combine PyTerrier's pipeline abstraction with Optuna's Bayesian optimization and LLM-generated evaluation labels, targeting a specific search platform. Elastic's agentic autotuning reference architecture from 2025 represents the first production-oriented attempt at this integration, though it currently focuses on LTR reranking rather than full configuration search. The team or company that ships a general-purpose "AutoSearch" tool — analogous to what Auto-sklearn did for classification — will address a significant gap in the market.