# EG6 Implementation Plan

## Purpose

`EG6` absorbs the current `strata-search` crate into `strata-engine`.

Search is not an independent layer in the target architecture. It is
engine-owned runtime and orchestration because it:

- fans out across engine-owned primitives
- interprets graph, vector, KV, JSON, and event search results
- uses storage keys and namespaces for temporal visibility checks
- owns deterministic retrieval substrate behavior
- owns fusion and ranking-shape contracts used by executor and intelligence
- participates in product runtime registration through `SearchSubsystem`
- depends on engine-owned graph and vector surfaces after `EG4` and `EG5`

After `EG6`, search code may still use storage types, but only because that
code is physically inside engine. No production crate above engine should
import storage for search behavior, depend on `strata-search`, or instantiate
`SearchSubsystem` as product runtime assembly.

This is the single implementation plan for the `EG6` phase. Lettered sections
such as `EG6A`, `EG6B`, and `EG6C` are tracked in this file rather than in
separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [eg1-implementation-plan.md](./eg1-implementation-plan.md)
- [eg2-implementation-plan.md](./eg2-implementation-plan.md)
- [eg3-implementation-plan.md](./eg3-implementation-plan.md)
- [eg4-implementation-plan.md](./eg4-implementation-plan.md)
- [eg5-implementation-plan.md](./eg5-implementation-plan.md)
- [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)

## Scope

`EG6` owns:

- moving model-free retrieval substrate behavior into engine
- moving fusion behavior into engine
- moving search expansion DTOs, parser support, prompt support, and traits into
  engine where they describe search orchestration rather than model execution
- moving rerank DTOs, blend logic, prompt support, response parsing, and traits
  into engine where they describe search orchestration rather than model
  execution
- preserving search manifest and sealed segment behavior already in engine
- preserving `SearchSubsystem` recovery, initialize, freeze, and branch cleanup
  behavior
- preserving product runtime order:

```text
graph -> vector -> search
```

- cutting executor, intelligence, root tests, and search unit tests over to
  engine-owned search imports
- removing normal production `strata-search` dependencies from workspace crates
- deleting `crates/search`, or reducing it to a zero-logic same-phase shell if
  compile sequencing requires it
- removing the `strata-search` storage bypass allowlist entries
- tightening guard tests so search storage bypasses cannot return outside
  engine

`EG6` does not own:

- redesigning search ranking semantics
- redesigning BM25, RRF, weighted RRF, blend weights, expansion filtering, or
  rerank score parsing
- changing search `.sidx` segment format
- changing search manifest format
- moving model execution, provider credentials, model download, or inference
  ownership into engine
- making engine depend on `strata-intelligence` or `strata-inference`
- removing executor direct storage imports unrelated to search (`EG7`)
- removing intelligence as the model/provider layer (`EG8`)
- redesigning executor command/session APIs
- deleting all `OpenSpec::with_subsystem` test hooks
- changing WAL, manifest, checkpoint, snapshot, or storage segment formats

## Load-Bearing Constraints

There must never be a normal dependency cycle:

```text
strata-engine -> strata-search -> strata-engine
```

Search implementation code has to move physically into engine. A duplicate live
retrieval substrate is not acceptable because it would make search behavior
split-brained. If a temporary `strata-search` shell is needed for compile
sequencing, it must only re-export engine-owned search types and must not
contain retrieval logic, storage imports, HTTP clients, or optional provider
features.

Engine must not become a model-provider crate:

```text
engine -> intelligence
engine -> inference
engine -> provider HTTP client
```

The model-free search contract belongs in engine. The model-backed execution
belongs in intelligence/inference. This means `EG6` can move query expansion
and rerank DTOs, parsers, prompts, and blend logic into engine, but optional
HTTP API clients and credential-bearing model calls should either move to
intelligence or be retired if they have no in-repo consumers.

The intended closeout state is:

- `strata-engine` owns the retrieval substrate
- `strata-engine` owns fusion and search orchestration DTOs
- `strata-engine` owns search expansion and rerank contracts
- `strata-intelligence` imports those contracts from engine
- `strata-executor` imports those contracts from engine
- `strata-search` is deleted
- no production code imports `strata_search`
- no production code outside engine imports storage for search behavior
- no production code above engine names `SearchSubsystem` for runtime
  composition

## Starting Code Map

At the start of `EG6`, `strata-search` owned these files:

- `lib.rs` - public facade, re-exports, and unused embedding adapter trait
- `substrate.rs` - deterministic retrieval substrate, BM25/vector fan-out,
  temporal filtering, RRF fusion helper, and extensive substrate tests
- `fuser.rs` - `Fuser`, `RRFFuser`, `FusedResult`, `weighted_rrf_fuse`,
  `merge_by_score`, top-rank bonus, and fusion tests
- `expand/mod.rs` - `QueryType`, `ExpandedQuery`, `ExpandedQueries`,
  `QueryExpander`, and expansion trait tests
- `expand/parser.rs` - parse and filter generated expansion text
- `expand/prompt.rs` - provider-neutral expansion prompt construction
- `expand/mock.rs` - test-only mock expander
- `expand/api.rs` - optional provider HTTP expander
- `expand/error.rs` - provider-client error alias
- `rerank/mod.rs` - `Reranker`, `RerankScore`, and rerank trait tests
- `rerank/blend.rs` - position-aware blend weights and deterministic rerank
  blending
- `rerank/prompt.rs` - provider-neutral rerank prompt construction
- `rerank/api.rs` - optional provider HTTP reranker and response
  parser
- `rerank/error.rs` - provider-client error alias
- provider-client helper module - optional HTTP helper, API-key handling, timeout handling,
  and retry helper for the HTTP expander/reranker

At the start of `EG6`, engine already owned these search files:

- `search/mod.rs` - engine search facade and public re-exports
- `search/types.rs` - `SearchRequest`, `SearchResponse`, `SearchHit`,
  `SearchStats`, `SearchMode`, `SearchBudget`, filters, and sorting
- `search/searchable.rs` - `Searchable`, search candidates, scorers, BM25-lite
  scoring, response builders, and text extraction
- `search/index.rs` - in-memory and sealed index behavior
- `search/manifest.rs` - persisted search manifest DTOs
- `search/segment.rs` - `.sidx` sealed segment format
- `search/recovery.rs` - `SearchSubsystem`, recovery, refresh hooks, freeze,
  branch cleanup, and storage-backed reconciliation
- `search/recipe.rs` - recipe DTOs and built-in recipes
- `search/tokenizer.rs` and `search/stemmer.rs` - tokenization and stemming

Engine already owns the search index, segment format, manifest, recovery, and
subsystem lifecycle. `EG6` moves the remaining peer-crate substrate and
orchestration code into that module tree.

## Starting Storage Touchpoints

At the start of `EG6`, `strata-search` had these storage touchpoints:

- `crates/search/Cargo.toml` - normal dependency on `strata-storage`
- `crates/search/src/substrate.rs` - `Key`, `Namespace`, and temporal
  visibility checks through `Database::get_at_timestamp`

Those touchpoints are legitimate only after the code moves into engine. They
should not be wrapped in a new upper-crate facade. `EG6B` moved the retrieval
substrate touchpoint into engine and removed the `strata-search` normal
dependency on `strata-storage`.

Engine search recovery already uses storage types directly in
`crates/engine/src/search/recovery.rs`; that is legitimate because it is
engine-owned search runtime behavior.

## Starting Consumer Map

At the start of `EG6`, production consumers of `strata-search` were:

- `crates/executor/src/handlers/search.rs`
  - imports `substrate::{retrieve, RetrievalRequest}`
  - imports `expand::{ExpandedQuery, QueryType}`
  - calls `substrate::rrf_fuse`
- `crates/intelligence/src/expand.rs`
  - imports `ExpandedQuery`, `QueryType`, and expansion parser
- `crates/intelligence/src/expand_cache.rs`
  - serializes/deserializes `ExpandedQuery` and `QueryType`
- `crates/intelligence/src/rerank.rs`
  - imports `BlendWeights`, `RerankScore`, and `blend_scores`

At the start of `EG6`, manifest consumers were:

- root `Cargo.toml`
- `crates/executor/Cargo.toml`
- `crates/intelligence/Cargo.toml`
- `crates/search/Cargo.toml`

At the start of `EG6`, stale feature wiring was:

- `crates/search` has `expand` and `rerank` features used only by optional HTTP
  API client modules.
- `crates/executor` has `expand` and `rerank` features that forward to the
  search crate's old expansion/rerank feature flags, but executor search
  orchestration is actually gated on `feature = "embed"`.

At the start of `EG6`, test consumers were:

- `tests/engine_consolidation_search_characterization.rs`
- `tests/common/mod.rs`
- `tests/intelligence/fusion.rs`
- `tests/intelligence/architectural_invariants.rs`
- `tests/intelligence/budget_semantics.rs`
- `crates/intelligence/tests/expand_cache_fork_test.rs`
- `crates/search` unit tests

`EG6` should distinguish production cutover from test cutover. Tests may keep
inspecting search internals, but they must import those internals from engine
after the move.

## Current Behavior Contract

Unless explicitly called out in an implementation review, `EG6` must preserve:

- retrieval substrate as the single model-free retrieval entry point
- deterministic result ordering for the same recipe and snapshot
- snapshot isolation through one `db.current_version()` per substrate request
- BM25 fan-out across KV, JSON, event, and graph primitives
- vector retrieval through `_system_` shadow collections
- parallel BM25/vector execution behavior
- dynamic discovery of `_system_embed_*` vector collections
- explicit vector collection selection from recipe config
- vector source-ref filtering that drops orphans
- vector source-ref filtering that prevents cross-space leaks
- BM25 temporal filtering using each hit's actual space
- graph BM25 temporal filtering using each hit's actual space
- `as_of` and `time_range` intersection behavior
- graceful primitive-search failure behavior in BM25 fan-out
- budget exhaustion tracking before and after BM25 fan-out/fusion
- transform limit behavior and rank reassignment
- fixed retrieval response shape
- substrate-local `rrf_fuse` behavior and deterministic tie-breaking
- `Fuser` and `RRFFuser` behavior
- `weighted_rrf_fuse` original-query weighting and top-rank bonus behavior
- `merge_by_score` behavior
- expansion DTO names and query-type semantics: `Lex`, `Vec`, and `Hyde`
- expansion parser behavior for valid, invalid, empty, whitespace, and filtered
  lines
- expansion prompt construction, unless deliberately retired with tests proving
  no in-repo consumer
- rerank `RerankScore` behavior
- rerank response parsing behavior
- rerank blend weights, position tiers, partial-score behavior, and deterministic
  tie-breaking
- `SearchSubsystem` recovery, initialize, freeze, and deleted-branch cleanup
- search manifest and `.sidx` sealed segment bytes
- product runtime order:

```text
graph -> vector -> search
```

The search move must not alter storage, WAL, manifest, checkpoint, snapshot, or
recovery file formats.

## Target Engine Shape

Preferred module placement:

```text
crates/engine/src/search/substrate.rs
crates/engine/src/search/fuser.rs
crates/engine/src/search/expand/
crates/engine/src/search/rerank/
```

Preferred public import paths:

```text
strata_engine::search::substrate::retrieve
strata_engine::search::substrate::RetrievalRequest
strata_engine::search::substrate::RetrievalResponse
strata_engine::search::substrate::RetrievalStats
strata_engine::search::substrate::StageStats
strata_engine::search::substrate::rrf_fuse
strata_engine::search::Fuser
strata_engine::search::RRFFuser
strata_engine::search::weighted_rrf_fuse
strata_engine::search::FusedResult
strata_engine::search::expand::ExpandedQuery
strata_engine::search::expand::QueryType
strata_engine::search::expand::ExpandedQueries
strata_engine::search::expand::QueryExpander
strata_engine::search::expand::parser::parse_expansion_with_filter
strata_engine::search::rerank::RerankScore
strata_engine::search::rerank::Reranker
strata_engine::search::rerank::BlendWeights
strata_engine::search::rerank::blend_scores
```

The exact re-export set can be tightened during implementation, but the
ownership shape should not change:

- retrieval substrate lives in engine
- fusion lives in engine
- model-free expansion/rerank contracts live in engine
- model/provider execution stays in intelligence/inference
- executor and intelligence consume search through engine

## Provider Boundary Policy

Do not move provider execution into engine.

The current `ApiExpander`, `ApiReranker`, and shared provider-client modules are optional
HTTP-provider implementations. They carry endpoint, token, timeout, request,
and retry policy. That is intelligence/provider territory, not engine search
runtime.

Implementation should do one of two things:

- If there are no in-repo callers, retire these optional API client modules and
  remove the stale `expand`/`rerank` feature forwarding from executor.
- If a real caller must be preserved, move the API clients to
  `strata-intelligence` and keep engine limited to traits, DTOs, prompts,
  parsers, and blend logic.

Do not add `ureq`, provider credentials, model download, or
`strata-inference` dependencies to engine for EG6.

## Temporary Bridge Policy

Prefer deleting `strata-search` during `EG6`.

If a temporary shell is necessary for compile sequencing, it must be a
zero-logic shell that only re-exports engine-owned search surfaces. It must:

- have no `strata-storage` dependency
- have no HTTP/provider feature flags
- have no retrieval substrate implementation
- have no unit tests that exercise logic in the shell
- have same-phase deletion criteria in `EG6F`

Do not introduce a broad search facade that hides storage access outside
engine. That recreates the storage bypass under a new name.

## Shared Commands

Useful inventories:

```bash
rg -n "strata_search|strata-search" \
  crates tests benchmarks Cargo.toml Cargo.lock \
  -g 'Cargo.toml' -g '*.rs' -g '!target/**'

rg -n "strata_storage::|use strata_storage|strata-storage|strata_storage" \
  crates/executor crates/intelligence crates/cli src \
  -g 'Cargo.toml' -g '*.rs' -g '!target/**'

rg -n "SearchSubsystem|with_subsystem|with_subsystems|Box<dyn Subsystem>" \
  crates tests src -g '*.rs' -g '!target/**'
```

Before `EG6F`, the retired crate-local search suite had to pass. After
`EG6F`, search logic tests live under `strata-engine`; intelligence is verified
as a compile/import boundary until the deferred root `tests/intelligence/*.rs`
harness is registered:

```bash
cargo test -p strata-engine --lib search
cargo check -p strata-intelligence --all-targets
cargo test -p strata-executor search
cargo test -p stratadb --test storage_surface_imports
```

During movement:

```bash
cargo fmt --check
cargo check -p strata-engine --all-targets
cargo check -p strata-intelligence --all-targets
cargo check -p strata-executor --all-targets
cargo test -p strata-engine --lib search
cargo test -p strata-engine --test recovery_tests
cargo test -p stratadb --test storage_surface_imports
```

Closeout guards:

```bash
cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[] | select(.name=="strata-search") | .name'

cargo tree -p strata-intelligence --edges normal | rg "strata-search"
cargo tree -p strata-executor --edges normal | rg "strata-search"
cargo tree -i strata-storage --workspace --edges normal --depth 2

rg -n "strata_search|strata-search" crates tests benchmarks Cargo.toml Cargo.lock \
  -g 'Cargo.toml' -g '*.rs' -g '!target/**'
```

At `EG6` closeout, `strata-executor` can still have direct storage access
unrelated to search because that is removed in `EG7`.

## Section Status

| Section | Status | Purpose |
| --- | --- | --- |
| `EG6A` | Complete | Search code map and characterization |
| `EG6B` | Complete | Retrieval substrate and fusion absorption |
| `EG6C` | Complete | Expansion/rerank contract and provider-boundary cleanup |
| `EG6D` | Complete | Intelligence cutover |
| `EG6E` | Complete | Executor, tests, and feature cutover |
| `EG6F` | Complete | Retire search crate and tighten guards |
| `EG6G` | Complete | Documentation and crate-map closeout |
| `EG6H` | Complete | Closeout review |

## EG6A - Search Code Map And Characterization

**Goal:**

Pin current search behavior before moving code.

**Work:**

- refresh the search code map from the current tree
- refresh the search storage-touchpoint inventory
- refresh the consumer map for executor, intelligence, root tests, and search
  unit tests
- identify which search types are already engine-owned and which still live in
  `strata-search`
- identify the current stale feature flags and optional API client surfaces
- identify tests that already cover the behavior contract
- add characterization tests where behavior is important but only indirectly
  covered

Characterization should cover at least:

- product open installs `graph -> vector -> search`
- substrate BM25 retrieval over KV, JSON, event, and graph
- substrate hybrid retrieval over vector shadow collections
- vector shadow retrieval drops orphans
- vector shadow retrieval rejects cross-space source refs
- `as_of` and `time_range` filters use hit space and intersect correctly
- deterministic ordering across repeated substrate calls
- RRF and weighted RRF tie-breaking
- expansion parser DTO behavior
- rerank blend behavior
- recovery reopens preserve search results without changing `.sidx` or manifest
  assumptions

**Acceptance:**

- search code map and storage-touchpoint inventory are current
- baseline tests or explicit gaps are recorded before module movement
- any added characterization tests pass against the pre-move implementation
- no code ownership movement is mixed into `EG6A`

**Baseline recorded:**

- Current code map, storage touchpoints, consumer map, stale feature flags, and
  optional API-client surfaces are recorded above in this document.
- `tests/engine_consolidation_search_characterization.rs` pins product-open
  runtime order, all-primitive BM25 fan-out on the product default branch,
  vector-shadow hybrid retrieval with a real source row, and disk reopen
  behavior for manifest-referenced search cache files plus result preservation.
- The pre-move fusion implementation had explicit equal-score tie-order tests
  for `RRFFuser` and `weighted_rrf_fuse`; `EG6B` moved those tests into
  `crates/engine/src/search/fuser.rs`.

**Coverage ledger before movement:**

- Product open installs `graph -> vector -> search`:
  `eg6a_product_open_installs_graph_vector_search_order` plus existing
  engine product-open tests.
- Substrate BM25 over KV, JSON, Event, and Graph:
  `eg6a_substrate_bm25_fans_out_across_kv_json_event_and_graph`.
- Hybrid retrieval over vector shadow collections:
  `eg6a_substrate_hybrid_reads_vector_shadow_sources`, which inserts the
  backing source row before the shadow vector, plus existing substrate
  temporal-hybrid tests.
- Orphan and cross-space vector-source filtering:
  existing pre-move substrate tests, later moved by `EG6B` into
  `crates/engine/src/search/substrate.rs`:
  `test_substrate_vector_filters_cross_space_sources`,
  `test_substrate_temporal_hybrid_filters_cross_space`, and
  `test_substrate_temporal_hybrid_orphan_dropped`.
- `as_of` and `time_range` intersection:
  existing substrate tests for KV, JSON, Graph, vector, and hybrid
  intersection behavior.
- Deterministic substrate ordering:
  existing `test_retrieve_deterministic` and
  `test_retrieve_parallel_deterministic`.
- RRF and weighted-RRF tie-breaking:
  `test_rrf_fuser_ties_order_by_entity_hash` and
  `test_weighted_rrf_ties_order_by_entity_hash`.
- Expansion parser DTO behavior:
  existing `crates/search/src/expand/parser.rs` tests and intelligence
  expansion tests.
- Rerank blend behavior:
  existing `crates/search/src/rerank/blend.rs` tests and intelligence rerank
  tests.
- Recovery reopen preserving search results and cache assumptions:
  `eg6a_product_reopen_preserves_search_cache_files_and_results`, which checks
  manifest headers, manifest-referenced `.sidx` headers, pre-existing segment
  bytes across reopen, and result preservation, plus existing branching
  convergence search-cache reopen tests.

No code ownership movement is part of `EG6A`; `strata-search` remains the owner
of substrate, fusion, expansion, and rerank surfaces until `EG6B`/`EG6C`.

## EG6B - Retrieval Substrate And Fusion Absorption

**Goal:**

Move the model-free retrieval substrate and fusion behavior into engine without
changing search behavior or storage formats.

**Work:**

- create or extend engine-owned modules:
  - `crates/engine/src/search/substrate.rs`
  - `crates/engine/src/search/fuser.rs`
- move `substrate.rs` into engine and update imports from `strata_engine::...`
  to `crate::...`
- keep direct storage key/namespace use inside engine-owned search code
- move `fuser.rs` into engine and update imports
- update `crates/engine/src/search/mod.rs` re-exports
- move or preserve substrate/fuser unit tests under engine
- ensure `SearchSubsystem` remains the engine lifecycle owner
- keep product runtime order unchanged

**Acceptance:**

- `cargo test -p strata-engine --lib search` covers moved substrate/fuser tests
- no production retrieval substrate logic remains in `crates/search`
- search storage touchpoints exist only inside engine
- no `strata-search` shell contains substrate or fusion logic

**Completed:**

- `crates/engine/src/search/substrate.rs` now owns the model-free retrieval
  substrate, including BM25/vector fan-out, temporal filtering, RRF helper
  behavior, and the moved substrate unit tests.
- `crates/engine/src/search/fuser.rs` now owns `Fuser`, `RRFFuser`,
  `FusedResult`, `merge_by_score`, `weighted_rrf_fuse`, deterministic tie
  ordering, and the moved fusion unit tests.
- `crates/engine/src/search/mod.rs` exposes the engine-owned substrate module
  and fusion re-exports.
- `crates/search/src/substrate.rs` and `crates/search/src/fuser.rs` were reduced
  to zero-logic compatibility re-export shells during `EG6B`, then removed when
  `crates/search` was deleted in `EG6F`.
- The temporary `crates/search` shell stopped carrying normal `strata-storage`
  or `rayon` dependencies for retrieval before it was deleted.
- `crates/search/Cargo.toml` and `crates/search/src/substrate.rs` were removed
  from `ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES`; the remaining direct
  storage bypass allowlist entries are executor-owned `EG7` work.
- Test-only substrate/fusion imports in root intelligence and common test
  helpers now use `strata_engine`.
- Production executor search still imports the compatibility shell until
  `EG6E`.

## EG6C - Expansion/Rerank Contract And Provider-Boundary Cleanup

**Goal:**

Move model-free expansion and rerank contracts into engine while keeping model
execution out of engine.

**Work:**

- move expansion DTOs and traits into engine:
  - `QueryType`
  - `ExpandedQuery`
  - `ExpandedQueries`
  - `QueryExpander`
- move expansion parser and prompt helpers into engine if retained
- move rerank DTOs and traits into engine:
  - `RerankScore`
  - `Reranker`
  - `BlendWeights`
  - `blend_scores`
- move rerank prompt helpers and response parser into engine if retained
- retire the unused embedding adapter trait unless a real caller is found
- retire optional HTTP API clients and provider-client helpers if no in-repo
  caller exists
- if an API client must survive, move it to intelligence instead of engine
- remove or replace `strata-search` feature flags in a way that does not add
  provider dependencies to engine

**Acceptance:**

- engine exposes expansion and rerank contracts needed by intelligence and
  executor
- engine does not depend on `strata-intelligence`
- engine does not depend on `strata-inference`
- engine does not gain provider HTTP/client credential ownership
- stale `expand`/`rerank` feature behavior is explicitly removed or rehomed

**Completed in EG6C:**

- `crates/engine/src/search/expand/` now owns `QueryType`,
  `ExpandedQuery`, `ExpandedQueries`, `QueryExpander`, `ExpandError`, the
  expansion parser, and the provider-neutral prompt helper.
- `crates/engine/src/search/rerank/` now owns `RerankScore`, `Reranker`,
  `RerankError`, `BlendWeights`, `blend_scores`, the rerank response parser,
  and the provider-neutral prompt helper.
- `crates/search/src/expand/*` and `crates/search/src/rerank/*` were reduced to
  compatibility re-export shells for those engine-owned contracts during
  `EG6C`, then removed when `crates/search` was deleted in `EG6F`.
- The unused embedding adapter trait was retired from `strata-search`.
- The optional provider HTTP clients (`ApiExpander`, `ApiReranker`) and
  shared provider-client helper were deleted instead of moving into engine.
- `strata-search` no longer has `expand`/`rerank` features or provider
  dependencies. `EG6E` removes executor's stale `expand`/`rerank` feature
  aliases once executor no longer depends on `strata-search`.

## EG6D - Intelligence Cutover

**Goal:**

Make intelligence consume search contracts from engine and remove its normal
dependency on `strata-search`.

**Work:**

- update `crates/intelligence/src/expand.rs` to import `ExpandedQuery`,
  `QueryType`, tokenizer helpers, and parser helpers from engine
- update `crates/intelligence/src/expand_cache.rs` to serialize engine-owned
  `ExpandedQuery` and `QueryType`
- update `crates/intelligence/src/rerank.rs` to import `BlendWeights`,
  `RerankScore`, and `blend_scores` from engine
- update intelligence tests to use engine-owned imports
- update comments that describe expansion DTOs as living in `strata-search`
- remove `strata-search` from `crates/intelligence/Cargo.toml`

**Acceptance:**

- `cargo tree -p strata-intelligence --edges normal` has no `strata-search`
- intelligence still depends on engine and inference, not storage
- expansion cache backward-compatible string DTO values remain `lex`, `vec`,
  and `hyde`
- intelligence search/expand/rerank tests pass

**Completed in EG6D:**

- `crates/intelligence/src/expand.rs` now imports `ExpandedQuery`,
  `QueryType`, tokenizer support, and expansion parsing from engine-owned
  search modules.
- `crates/intelligence/src/expand_cache.rs` serializes and deserializes
  engine-owned `ExpandedQuery` / `QueryType` while preserving the cache DTO
  strings `lex`, `vec`, and `hyde`.
- `crates/intelligence/src/rerank.rs` now imports `BlendWeights`,
  `RerankScore`, and `blend_scores` from engine-owned search modules.
- `crates/intelligence/tests/expand_cache_fork_test.rs` now uses engine-owned
  expansion DTO imports.
- `crates/intelligence/Cargo.toml` no longer depends on `strata-search`.
- The embed-gated intelligence search modules were compile-verified with
  `STRATA_INFERENCE_SKIP_LLAMA_CPP_BUILD_FOR_CHECK=1 cargo check -p strata-intelligence --all-targets --features embed`;
  the environment variable skips only the native llama.cpp build for check-only
  verification when the vendor tree is absent.

## EG6E - Executor, Tests, And Feature Cutover

**Goal:**

Make executor and root tests consume search through engine and remove executor's
normal dependency on `strata-search`.

**Work:**

- update `crates/executor/src/handlers/search.rs` to import substrate,
  expansion DTOs, and fusion helpers from engine
- keep model execution calls routed through intelligence
- remove `strata-search` from `crates/executor/Cargo.toml`
- remove stale executor `expand` and `rerank` feature forwarding, or replace it
  with documented aliases only if implementation proves in-repo compatibility
  requires them
- update root `Cargo.toml` dev-dependency/dependency entries
- update `tests/common/mod.rs` substrate helper imports
- update `tests/intelligence/*` imports that refer to `strata_search`
- keep executor public command output shapes unchanged

**Acceptance:**

- `cargo tree -p strata-executor --edges normal` has no `strata-search`
- executor still depends on intelligence for model-backed expansion, rerank,
  embedding, and RAG
- search command tests pass
- deferred intelligence integration sources have no `strata_search` imports
- no production upper crate imports `strata_search`

**Completed in EG6E:**

- `crates/executor/src/handlers/search.rs` now imports retrieval substrate,
  expansion DTOs, primitive filters, and search hits from engine-owned search
  modules.
- Model-backed expansion, rerank, embedding, and RAG call sites remain routed
  through `strata-intelligence`.
- `crates/executor/Cargo.toml` no longer depends on `strata-search`.
- The stale executor `expand` and `rerank` feature aliases were removed; the
  provider-backed clients were retired in `EG6C` and no in-repo compatibility
  caller remains.
- Root `Cargo.toml` no longer has a `strata-search` dev-dependency.
- Root search helpers and intelligence tests already consumed engine-owned
  search imports; `EG6E` verified no remaining upper-crate `strata_search`
  imports. The `tests/intelligence` harness remains intentionally deferred as
  `tests/intelligence/main.rs.deferred`, so EG6E verifies those sources by
  import scan rather than by a runnable root test target.

## EG6F - Retire Search Crate And Tighten Guards

**Goal:**

Delete `strata-search` and make the retired state enforceable.

**Work:**

- delete `crates/search`
- remove `strata-search` from workspace and root manifests
- remove `strata-search` from `Cargo.lock`
- add a retired-search guard matching the existing security, legacy executor,
  graph, and vector guards
- add or extend a guard so production upper crates do not name
  `SearchSubsystem`
- keep test-only `SearchSubsystem` usage allowed until `EG7`/`EG9` narrows or
  deletes low-level `OpenSpec::with_subsystem`
- verify direct storage guard now reports only executor `EG7` entries

**Acceptance:**

- `cargo metadata` has no `strata-search` package
- `rg "strata_search|strata-search"` has no production source or manifest
  matches outside retired-crate guard strings
- production upper crates do not name `SearchSubsystem`
- production upper crates do not import storage for search behavior
- `cargo test -p stratadb --test storage_surface_imports` passes

**Completed in EG6F:**

- `crates/search` was deleted.
- The root workspace no longer contains `strata-search`; the ignored local
  `Cargo.lock` was also verified to contain no `strata-search` package.
- `tests/storage_surface_imports.rs` now rejects retired search crate
  references and production upper-crate `SearchSubsystem` assembly.
- The engine-consolidation production scan roots no longer include the retired
  search crate.
- The direct storage bypass ledger is now executor-only for `EG7`.

## EG6G - Documentation And Crate-Map Closeout

**Goal:**

Make the documentation reflect the post-search graph before starting `EG7`.

**Work:**

- update [engine-crate-map.md](./engine-crate-map.md)
- update [engine-consolidation-plan.md](./engine-consolidation-plan.md) if
  EG6 details drifted during implementation
- update [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)
  only if engine's allowed storage use changed
- update search-related comments in engine, executor, intelligence, and tests
- record that the remaining direct storage bypass above engine is executor
  only

**Acceptance:**

- documented normal graph no longer includes `strata-search`
- inverse storage graph no longer includes `strata-search`
- EG7 starts from an executor-only direct storage bypass ledger

**Completed in EG6G:**

- [engine-crate-map.md](./engine-crate-map.md) records the post-search normal
  workspace graph and executor-only direct storage bypass.
- [engine-consolidation-plan.md](./engine-consolidation-plan.md) records `EG6`
  as complete and `strata-search` as retired.
- [../storage/storage-crate-map.md](../storage/storage-crate-map.md) and
  [../storage/v1-storage-consumption-contract.md](../storage/v1-storage-consumption-contract.md)
  no longer list search as a direct storage consumer.
- Search characterization comments describe completed engine ownership instead
  of transition-era ownership wording.

## EG6H - Closeout Review

**Goal:**

Review all of EG6 for correctness, completeness, hidden crate edges, and
behavioral drift before beginning executor storage-bypass removal.

**Work:**

- review all moved search code for import mistakes and stale peer-crate naming
- review provider boundary to confirm engine did not gain inference/provider
  ownership
- review direct storage guard output
- review product-open composition and `SearchSubsystem` naming
- review tests moved from `crates/search`
- review feature flags for stale `expand`/`rerank` wiring
- run focused and broad verification

**Acceptance:**

- no `strata-search` crate remains
- no normal dependency on `strata-search` remains
- no `strata-search` storage bypass allowlist entries remain
- no provider/model execution moved into engine
- search behavior remains characterized by passing tests
- remaining storage bypass work is scoped to `EG7`

**Completed in EG6H:**

- `cargo metadata --format-version 1 --no-deps` reports no `strata-search`
  package and no normal dependency on `strata-search`.
- `cargo tree -i strata-storage --workspace --edges normal --depth 3` shows
  `strata-engine` and the remaining `strata-executor` bypass as the only direct
  normal storage dependents.
- Source and manifest scans find no production `strata_search` imports or
  `strata-search` dependencies.
- Upper-crate `SearchSubsystem` naming was reviewed; the only remaining
  intelligence reference is inside a `#[cfg(test)]` module.
- Engine search code owns provider-neutral expansion/rerank contracts and
  deterministic parsing/blending only; provider/model execution remains above
  engine.
- Search-specific `expand`/`rerank` feature wiring is gone. The root
  `usearch-enabled` optional dependency is unrelated vector/backend plumbing,
  not a surviving search-crate feature.
- The deferred root `tests/intelligence/*.rs` harness remains unrunnable as
  standalone Cargo test targets; EG6H verified intelligence search/expand/rerank
  by import scans, package compile checks, and the registered engine/executor
  characterization tests.
