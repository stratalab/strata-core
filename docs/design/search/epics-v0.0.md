# v0.0 Epics: Search Prerequisites

Eight work items that must land before v0.1. Detailed implementation specs for issues #2139–#2146.

**Dependency graph:**
```
Start immediately (parallel):
  #2139 — _system_ space
  #2140 — Recipe types
  #2143 — Event + JSON Searchable
  #2144 — Vector Searchable
  #2146 — Inference validation

After #2139 + #2140:
  #2141 — Default recipe + API

After #2141:
  #2142 — Config migration

After #2139:
  #2145 — Shadow collection migration
```

---

## Epic 1: `_system_` Space (#2139)

### What

Hidden, internal-only space on every branch. Follows the `_graph_` pattern in `crates/graph/src/keys.rs:92-109`.

### Why existing infrastructure makes this easy

- Space validation in `crates/core/src/types.rs:489` already rejects `_system_*` names from users
- Space isolation is provided by key encoding (different space = different bytes = invisible to other-space scans)
- `_graph_` space shows the exact pattern: cached namespace via DashMap, fixed name, Key constructors
- Internal writes bypass the executor (go through `Database` directly) — no API changes needed

### Implementation

**New file: `crates/engine/src/system_space.rs` (~40 lines)**

```rust
pub const SYSTEM_SPACE: &str = "_system_";

static NS_CACHE: Lazy<DashMap<BranchId, Arc<Namespace>>> = Lazy::new(DashMap::new);

pub fn system_namespace(branch_id: BranchId) -> Arc<Namespace> {
    NS_CACHE
        .entry(branch_id)
        .or_insert_with(|| Arc::new(Namespace::for_branch_space(branch_id, SYSTEM_SPACE)))
        .clone()
}

pub fn system_kv_key(branch_id: BranchId, key: &str) -> Key {
    Key::new_kv(system_namespace(branch_id), key)
}

pub fn invalidate_cache(branch_id: &BranchId) { NS_CACHE.remove(branch_id); }
```

**Changes to existing files (~10 lines total):**

| File | Change |
|---|---|
| `engine/src/lib.rs` | `pub mod system_space;` |
| `engine/src/primitives/space.rs:62` | Add `space == SYSTEM_SPACE` to `exists()` short-circuit |
| `engine/src/primitives/space.rs:81` | Add `.filter(\|s\| s != SYSTEM_SPACE)` to `list()` |
| `executor/src/handlers/space.rs:36` | Add `space == SYSTEM_SPACE` to deletion rejection |
| `engine/src/branch_ops.rs` | Call `system_space::invalidate_cache()` on branch delete |

### Tests

- Internal write to `_system_` → succeeds
- User write to `_system_` → rejected (already works)
- `SpaceIndex.list()` → doesn't include `_system_`
- `SpaceIndex.exists("_system_")` → true
- Fork branch → `_system_` data inherited via COW
- Override on fork → parent unaffected
- Delete `_system_` → rejected
- `db.kv.list("default", None)` → doesn't show `_system_` entries (automatic from key encoding)

### Effort: ~50 lines

---

## Epic 2: Recipe Types and Three-Level Merge (#2140)

### What

Rust types for the recipe schema. All fields `Option<T>`. Deep merge function.

### Implementation

**New file: `crates/engine/src/search/recipe.rs` (~300 lines)**

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Recipe {
    pub version: Option<u32>,
    pub retrieve: Option<RetrieveConfig>,
    pub expansion: Option<ExpansionConfig>,
    pub filter: Option<FilterConfig>,
    pub fusion: Option<FusionConfig>,
    pub rerank: Option<RerankConfig>,
    pub transform: Option<TransformConfig>,
    pub prompt: Option<String>,
    pub rag_context_hits: Option<usize>,
    pub rag_max_tokens: Option<usize>,
    pub models: Option<ModelsConfig>,
    pub version_output: Option<VersionOutputConfig>,
    pub control: Option<ControlConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetrieveConfig {
    pub bm25: Option<BM25Config>,
    pub vector: Option<VectorConfig>,
    pub graph: Option<GraphConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BM25Config {
    pub sources: Option<Vec<String>>,
    pub spaces: Option<Vec<String>>,
    pub k: Option<usize>,
    pub k1: Option<f32>,
    pub b: Option<f32>,
    pub field_weights: Option<HashMap<String, f32>>,
    pub stemmer: Option<String>,
    pub stopwords: Option<String>,
    pub phrase_boost: Option<f32>,
    pub proximity_boost: Option<f32>,
}

// VectorConfig, GraphConfig, ExpansionConfig, FusionConfig, RerankConfig,
// TransformConfig, ModelsConfig, ControlConfig, VersionOutputConfig, FilterConfig
// — all follow the same pattern: every field Option<T>
```

**Merge function:**

```rust
impl Recipe {
    pub fn merge(base: &Recipe, overlay: &Recipe) -> Recipe {
        Recipe {
            version: overlay.version.or(base.version),
            retrieve: merge_option(&base.retrieve, &overlay.retrieve, RetrieveConfig::merge),
            expansion: overlay.expansion.as_ref().or(base.expansion.as_ref()).cloned(),
            // ... same pattern for all fields ...
        }
    }

    pub fn resolve(builtin: &Recipe, branch: &Recipe, per_call: Option<&Recipe>) -> Recipe {
        let merged = Recipe::merge(builtin, branch);
        match per_call {
            Some(o) => Recipe::merge(&merged, o),
            None => merged,
        }
    }
}
```

**Nested merge:** `RetrieveConfig::merge`, `BM25Config::merge` etc. — same pattern recursively. Field-by-field: overlay wins if `Some`, base fills if `None`.

**Built-in defaults (constant):**
```rust
pub fn builtin_defaults() -> Recipe {
    Recipe {
        version: Some(1),
        retrieve: Some(RetrieveConfig {
            bm25: Some(BM25Config {
                k: Some(50), k1: Some(0.9), b: Some(0.4),
                stemmer: Some("porter".into()), stopwords: Some("lucene33".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        fusion: Some(FusionConfig { method: Some("rrf".into()), k: Some(60), ..Default::default() }),
        transform: Some(TransformConfig { limit: Some(10), ..Default::default() }),
        control: Some(ControlConfig { budget_ms: Some(5000), ..Default::default() }),
        ..Default::default()
    }
}
```

### Tests

- Deserialize `{}` → all None
- Deserialize `{"retrieve": {"bm25": {}}}` → BM25 Some with all fields None
- Merge: empty overlay → base unchanged
- Merge: single nested field override → only that field changes
- Resolve: three levels stacked correctly
- `builtin_defaults()` has k1=0.9, b=0.4, limit=10
- Round-trip: serialize → deserialize → identical

### Effort: ~300 lines

---

## Epic 3: Default Recipe Storage + API (#2141)

**Depends on:** #2139 (_system_ space), #2140 (Recipe types)

### What

Store/retrieve recipes in `_system_` space. Auto-create default on database open.

### Implementation

**Engine API — add to `Database` impl (~80 lines):**

```rust
impl Database {
    pub fn set_recipe(&self, branch_id: &BranchId, name: &str, recipe: &Recipe) -> StrataResult<()> {
        let key = system_kv_key(*branch_id, &format!("recipe:{}", name));
        let json = serde_json::to_string(recipe)?;
        self.put_with_version_mode(key, Value::String(json), VersionMode::Auto)?;
        Ok(())
    }

    pub fn get_recipe(&self, branch_id: &BranchId, name: &str) -> StrataResult<Option<Recipe>> {
        let key = system_kv_key(*branch_id, &format!("recipe:{}", name));
        match self.get_versioned(&key, None)? {
            Some(v) => Ok(Some(serde_json::from_str(v.value().as_str()?)?)),
            None => Ok(None),
        }
    }

    pub fn get_default_recipe(&self, branch_id: &BranchId) -> StrataResult<Recipe> {
        match self.get_recipe(branch_id, "default")? {
            Some(r) => Ok(r),
            None => {
                let defaults = builtin_defaults();
                self.set_recipe(branch_id, "default", &defaults)?;
                Ok(defaults)
            }
        }
    }

    pub fn list_recipes(&self, branch_id: &BranchId) -> StrataResult<Vec<String>> {
        let prefix = system_kv_key(*branch_id, "recipe:");
        let results = self.scan_prefix(&prefix)?;
        Ok(results.into_iter()
            .filter_map(|(k, _)| k.user_key_string().map(|s| s.trim_start_matches("recipe:").to_string()))
            .collect())
    }
}
```

**Executor commands (~100 lines):**

| Command | Handler |
|---|---|
| `RECIPE SET <json>` | `set_recipe(branch, "default", parse(json))` |
| `RECIPE SET <name> <json>` | `set_recipe(branch, name, parse(json))` |
| `RECIPE GET` | `get_default_recipe(branch)` → JSON output |
| `RECIPE GET <name>` | `get_recipe(branch, name)` → JSON output |
| `RECIPE LIST` | `list_recipes(branch)` → list output |

### Tests

- Fresh database → `get_default_recipe()` creates and returns builtin defaults
- `set_recipe("test", recipe)` → `get_recipe("test")` round-trip
- `list_recipes()` returns all stored names
- Fork branch → recipe inherited from parent
- Override on fork → parent's recipe unchanged
- `RECIPE SET '{"retrieve":{"bm25":{"k1":1.5}}}'` → stored, retrievable

### Effort: ~180 lines

---

## Epic 4: Migrate Config from strata.toml (#2142)

**Depends on:** #2141 (Recipe storage)

### What

Move `bm25_k1`, `bm25_b`, `embed_model` from `StrataConfig` to the default recipe. One-time auto-migration.

### Implementation

**File: `crates/engine/src/database/mod.rs` — in `Database::open()` (~60 lines)**

```rust
fn migrate_search_config_to_recipe(&self, branch_id: &BranchId, config: &StrataConfig) -> StrataResult<()> {
    // Skip if recipe already exists (already migrated)
    if self.get_recipe(branch_id, "default")?.is_some() {
        return Ok(());
    }

    // Build recipe from TOML values
    let mut recipe = builtin_defaults();
    if let Some(k1) = config.bm25_k1 {
        recipe.retrieve.as_mut().unwrap().bm25.as_mut().unwrap().k1 = Some(k1);
    }
    if let Some(b) = config.bm25_b {
        recipe.retrieve.as_mut().unwrap().bm25.as_mut().unwrap().b = Some(b);
    }
    if config.embed_model != "miniLM" {
        recipe.models = Some(ModelsConfig {
            embed: Some(format!("local:{}", config.embed_model)),
            ..Default::default()
        });
    }

    self.set_recipe(branch_id, "default", &recipe)?;
    info!(target: "strata::config", "Migrated search config from strata.toml to recipe");
    Ok(())
}
```

Called during `Database::open()` after reading config, before any search operations.

**Cleanup:** Mark `bm25_k1`, `bm25_b` as deprecated in `default_toml()` template with comments pointing to recipe.

### Tests

- Database with `strata.toml` k1=1.2, b=0.7 → opens, recipe has k1=1.2, b=0.7
- Database without custom settings → opens with builtin defaults
- Second open → migration skipped (recipe already exists)
- After migration, CONFIG SET bm25_k1 has no effect on search (recipe wins)

### Effort: ~100 lines

---

## Epic 5: Wire Event + JSON Searchable to InvertedIndex (#2143)

### What

Two stubs that return empty. Fix both to query the InvertedIndex — same pattern as `kv.rs:538-608`.

### Event implementation

**File: `crates/engine/src/primitives/event.rs:993-1005`**

Replace the stub with:

```rust
fn search(&self, req: &SearchRequest) -> StrataResult<SearchResponse> {
    let start = std::time::Instant::now();
    let index = match self.db.extension::<InvertedIndex>() {
        Some(idx) => idx,
        None => return Ok(SearchResponse::empty()),
    };

    let query_terms = tokenize(&req.query);
    if query_terms.is_empty() {
        return Ok(SearchResponse::empty());
    }

    let k1 = self.db.config().bm25_k1.unwrap_or(0.9);  // TODO: read from recipe after v0.1
    let b = self.db.config().bm25_b.unwrap_or(0.4);
    let scored = index.score_top_k(&query_terms, &req.branch_id, req.k, k1, b);

    let hits: Vec<SearchHit> = scored.into_iter()
        .filter_map(|sd| {
            let entity_ref = index.resolve_doc_id(sd.doc_id)?;
            match &entity_ref {
                EntityRef::Event { .. } => Some(SearchHit {
                    doc_ref: entity_ref,
                    score: sd.score,
                    rank: 0,
                    snippet: None,  // TODO: fetch event payload for snippet
                }),
                _ => None,  // Filter to Event refs only
            }
        })
        .collect();

    Ok(SearchResponse::new(hits, false, SearchStats::new(
        start.elapsed().as_micros() as u64,
        hits.len(),
    )))
}
```

**Prerequisite verified:** Event already indexes on write at `event.rs:~371`: `index.index_document(&entity_ref, &text, None)`. The data is in the index. Just need to query it.

### JSON implementation

**File: `crates/engine/src/primitives/json/mod.rs:1352-1450`**

Current code returns empty when no `field_filter` or `sort_by`. Add BM25 fallback:

```rust
fn search(&self, req: &SearchRequest) -> StrataResult<SearchResponse> {
    // Existing: handle field_filter and sort_by if present
    if req.field_filter.is_some() || req.sort_by.is_some() {
        return self.search_by_index(req);  // existing code path
    }

    // NEW: BM25 keyword search via InvertedIndex
    let index = match self.db.extension::<InvertedIndex>() {
        Some(idx) => idx,
        None => return Ok(SearchResponse::empty()),
    };

    // Same pattern as KV and Event...
    let query_terms = tokenize(&req.query);
    // ... score_top_k, filter to EntityRef::Json, resolve snippets ...
}
```

**Prerequisite check needed:** Verify JSON documents are indexed in the InvertedIndex during `set()`. If the handler calls `index.index_document()` on JSON writes — good. If not, add it in `crates/executor/src/handlers/json.rs` alongside the auto-embed hook call.

### Tests

- Insert 3 JSON docs with text → `search("keyword")` → returns matching docs ranked by BM25
- Insert 5 events → `search("error")` → returns matching events
- Mixed: insert KV + JSON + events → search returns hits from all three primitives
- Event with event_type "error" → event_type text contributes to BM25 score
- All existing search tests still pass

### Effort: ~100 lines (30 event + 50 JSON + 20 write-path if needed)

---

## Epic 6: Wire VectorStore Searchable (#2144)

### What

`VectorStore::search()` returns empty for all modes. Wire it to `search_by_embedding()`.

### Implementation

**File: `crates/vector/src/store/mod.rs:680-733`**

Replace the stub:

```rust
fn search(&self, req: &SearchRequest) -> StrataResult<SearchResponse> {
    let start = std::time::Instant::now();

    match req.mode {
        SearchMode::Keyword => {
            // Vector doesn't do keyword search — correct to return empty
            Ok(SearchResponse::empty())
        }
        SearchMode::Vector | SearchMode::Hybrid => {
            let embedding = match &req.precomputed_embedding {
                Some(e) => e,
                None => return Ok(SearchResponse::empty()), // No embedding → can't search
            };

            // Search all _system_embed_* collections on this branch
            let collections = self.list_collections(req.branch_id, &req.space)?;
            let shadow_collections: Vec<_> = collections.iter()
                .filter(|c| c.starts_with("_system_embed_"))
                .collect();

            let mut all_hits = Vec::new();
            for collection in shadow_collections {
                let matches = self.search(
                    req.branch_id, &req.space, collection, embedding, req.k, None
                )?;

                for m in matches {
                    all_hits.push(SearchHit {
                        doc_ref: self.resolve_source_ref(req.branch_id, &req.space, collection, &m.key)
                            .unwrap_or(EntityRef::Kv {
                                branch_id: req.branch_id,
                                key: m.key.clone(),
                            }),
                        score: m.score,
                        rank: 0,
                        snippet: None,
                    });
                }
            }

            // Sort by score desc, truncate to k
            all_hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
            all_hits.truncate(req.k);

            Ok(SearchResponse::new(all_hits, false, SearchStats::new(
                start.elapsed().as_micros() as u64,
                all_hits.len(),
            )))
        }
    }
}
```

**Source ref resolution:** Shadow collections store `InlineMeta` with `source_ref: Option<EntityRef>`. Use this to map vector hits back to their source KV/JSON/Event documents. If no source_ref, fall back to the vector key as a KV EntityRef.

### Tests

- Insert data with auto_embed → search with embedding → hits returned via Searchable trait
- Keyword mode → empty (correct)
- No embedding provided → empty (correct)
- Multiple shadow collections → results from all, merged and ranked
- Source refs resolve to original KV/JSON entity refs

### Effort: ~80 lines

---

## Epic 7: Move Shadow Collections to `_system_` Space (#2145)

**Depends on:** #2139 (_system_ space)

### What

Move `_system_embed_*` collections from wherever they currently live to the `_system_` space on the user's branch. Currently they're in the "default" space.

### What changes

**Auto-embed write path** (`crates/executor/src/handlers/embed_hook.rs`):

Currently, shadow collections are created and written in the "default" space:
```rust
p.vector.create_system_collection(branch_id, SHADOW_KV, config)  // default space
```

Change to use `_system_` space:
```rust
p.vector.create_system_collection_in_space(branch_id, SYSTEM_SPACE, SHADOW_KV, config)
```

Or: use the `system_namespace()` helper to construct collection keys in `_system_` space.

**Vector search read path** (`crates/vector/src/store/search.rs`):

Currently discovers collections in "default" space. Must also look in `_system_` space. Or: the substrate (v0.1) explicitly passes the collection list from the recipe, which includes `_system_` space collections.

**Migration on database open:**

1. Check if shadow collections exist in "default" space
2. If yes, copy data to `_system_` space equivalents
3. Delete originals from "default" space (or leave as tombstones)

This is the highest-risk change — embeddings are on the critical path for hybrid search.

### Tests

- Existing database → migration copies embeddings to `_system_` space
- Hybrid search works after migration (BEIR nDCG@10 unchanged)
- New writes → embeddings go to `_system_` space
- Fork branch after migration → embeddings available on fork via COW
- Auto-embed on fork → new embeddings on fork's `_system_` space, parent unaffected

### Effort: ~300 lines. High risk. Test thoroughly.

---

## Epic 8: Validate Inference Pipeline (#2146)

### What

Test that strata-inference works across all providers before building intelligence layer features.

### Current API surface

**Generate** (`crates/inference/src/generate.rs:237`):
```rust
pub fn generate(&mut self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError>
```
- `GenerateRequest`: prompt, max_tokens, temperature, top_k, top_p, seed, stop_sequences
- `GenerateResponse`: text, stop_reason, prompt_tokens, completion_tokens

**Embed** (`crates/inference/src/embed.rs:107`):
```rust
pub fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError>
pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError>
```

**Providers** (`crates/inference/src/lib.rs`): Local (llama.cpp), Anthropic, OpenAI, Google

### Gaps found

1. **Grammar-constrained generation** — NOT in current API. `GenerateRequest` has no grammar field. Must be added before v0.2 (expansion needs lex/vec/hyde grammar).
2. **Ranking/cross-encoder API** — NOT in current API. Must be added before v0.2 (reranking).
3. **Provider:model_name parsing** — NOT in current API. Must be added before v0.8 (multi-model routing). Currently models are loaded by name, not by `provider:model` spec.

### Test plan

**Deliverable:** Benchmark script (Rust or Python) that:

1. **Local models:**
   - Load MiniLM → embed 100 texts → report throughput (target: >100 embed/s)
   - Load Qwen3 1.7B → generate 10 responses → report tok/s (target: >30) and TTFT (target: <500ms)
   - Load Qwen3-Reranker → verify ranking API exists or identify gap
   - Test context window: 2048 and 4096 tokens

2. **Cloud providers (with API keys):**
   - Anthropic Claude Sonnet → generate 5 responses → report latency
   - OpenAI GPT-4o-mini → generate 5 responses → report latency
   - Google Gemini Flash → generate 5 responses → report latency

3. **Error handling:**
   - Invalid model name → graceful error
   - Timeout → graceful error
   - Invalid API key → graceful error

4. **Report:** JSON output with all metrics for comparison.

### Gaps to file as separate issues

- Grammar-constrained generation support in strata-inference
- Ranking/cross-encoder API in strata-inference
- Provider:model_name routing in strata-inference

### Effort: ~1 day (test harness + model download + API key setup)

---

## Summary

| Epic | Issue | Depends on | Effort | Risk |
|---|---|---|---|---|
| `_system_` space | #2139 | Nothing | ~50 lines | Low |
| Recipe types | #2140 | Nothing | ~300 lines | Low |
| Recipe storage + API | #2141 | #2139, #2140 | ~180 lines | Low |
| Config migration | #2142 | #2141 | ~100 lines | Low |
| Event + JSON Searchable | #2143 | Nothing | ~100 lines | Low |
| Vector Searchable | #2144 | Nothing | ~80 lines | Medium |
| Shadow collection migration | #2145 | #2139 | ~300 lines | **High** |
| Inference validation | #2146 | Nothing | ~1 day | Low |
| **Total** | | | **~1,110 lines + 1 day** | |
