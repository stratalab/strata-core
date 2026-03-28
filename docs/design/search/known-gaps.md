# Known Gaps and Future Optimizations

None of these are blockers. The architecture handles all of them gracefully — most are "first implementation is simple, optimize later" decisions. Tracked here so they don't get lost.

---

## 1. Filter requires full document resolution — performance cliff

**Version:** v0.5
**Severity:** Performance (not correctness)

Post-retrieval filtering resolves every candidate hit to its full document to extract field values. 200 candidates = 200 document lookups. For simple predicates on small documents this is fine. At scale with complex predicates on nested JSON, this becomes the bottleneck.

**Mitigation:** Predicate pushdown. When filter predicates match indexed fields (JSON secondary indexes), short-circuit the document lookup by querying the index directly. The results are identical — it's a pure optimization.

**When to address:** When BEIR benchmarks or production profiling show filter latency > 50ms. Not v0.5.

---

## 2. Scan defined in recipe schema but not implemented

**Version:** retrieval-substrate.md
**Severity:** UX (confusion)

The recipe schema defines `retrieve.scan`, `transform.aggregate`, `transform.group_by`, and `transform.sort` (by field) in detail. Users reading the schema will expect them to work. The v0.5 doc explicitly defers these to DataFusion, but the schema doesn't say "reserved, not yet implemented."

**Mitigation:** Add a note to the schema defaults table marking these as reserved:

```
| `retrieve.scan` | Reserved — not yet implemented. Use primitive APIs (db.kv.list, db.json.list). |
| `transform.aggregate` | Reserved — not yet implemented. Use DataFusion over Arrow exports. |
| `transform.group_by` | Reserved — not yet implemented. |
| `transform.sort` (by field) | Reserved — not yet implemented. Search sorts by score. |
```

At runtime, if a recipe includes `scan` or `aggregate`, the substrate should return a clear error: `"scan operator is reserved for a future version. Use db.kv.list() for predicate-based queries."`

**When to address:** Before v0.1 ships. A clear error is better than silent no-op.

---

## 3. RAG answer quality has no automated evaluation

**Version:** v0.6, v0.7
**Severity:** Evaluation gap

`db.experiment()` in RAG mode ranks by retrieval metrics (NDCG), not answer quality. A recipe that retrieves perfectly but generates garbage answers would "win." The generated answers are returned in `result.details` for manual inspection, but there's no automated quality signal.

**Minimum viable addition:** A binary citation check — "did the model cite at least one source from the context?" This catches the most common failure mode (model ignores context, answers from training data). Not a quality metric, but a sanity check.

```json
"per_query": {
    "answer_cited": true,     // at least one [N] citation found
    "citations_valid": true,  // all cited [N] map to actual hits
    "answer_empty": false     // model produced non-empty text
}
```

**When to address:** After v0.7 (AutoResearch), before v0.8. Low effort (~30 lines).

---

## 4. Multi-model lifecycle and memory budget

**Version:** v0.8
**Severity:** Production (resource management)

The recipe says which models to use, but nothing constrains total memory:

| Model | Memory |
|---|---|
| Qwen3 1.7B (Q4_K_M) | ~1.1 GB |
| Qwen3-Reranker 0.6B (Q8_0) | ~640 MB |
| MiniLM-L6 (F16) | ~45 MB |
| **Total default local stack** | **~1.8 GB** |

On a Raspberry Pi with 4GB RAM, that's half the memory before any data is loaded.

**Questions to answer:**
- When are models loaded? (Lazy on first use? Eagerly at startup?)
- How many can be loaded simultaneously? (All? LRU eviction?)
- What's the memory budget? (Configurable? Auto-detected from available RAM?)
- What happens when a recipe requests a model that would exceed the budget?

**Current behavior:** strata-inference loads models lazily and caches them. No eviction, no budget. This works for developer machines but not for constrained environments.

**When to address:** Before targeting edge/embedded deployment. Not v0.8 (developer machines are fine).

---

## 5. Temporal RAG prompt design

**Version:** v0.3 + v0.6
**Severity:** Quality (prompt engineering)

Standard RAG: "answer using the context." Temporal RAG: "compare these two sets of results and explain the differences." Fundamentally different cognitive task for the model.

The intelligence layer doc mentions temporal RAG briefly. The v0.3 doc sketches the prompt:

```
Context (current):
[1] {snippet from T2 results}

Changes since {T1}:
NEW: [3] {snippet from new hit}
REMOVED: {snippet from removed hit}
```

This needs real prompt engineering and testing with Qwen3 1.7B. Small models struggle with comparison tasks. The prompt may need to be structured differently (e.g., side-by-side snippets, explicit "what changed" framing).

**When to address:** When temporal RAG is implemented (after v0.6 RAG works). Test different prompt structures and measure answer quality manually.

---

## 6. No snippet strategy for non-text primitives

**Version:** v0.1
**Severity:** UX (empty context in RAG)

BM25 over KV and JSON returns text snippets naturally. But:

| Primitive | What's a "snippet"? |
|---|---|
| KV | First N chars of the value (works for text values) |
| JSON | First N chars of serialized document or key fields |
| Event | `event_type: ` + first N chars of payload |
| Vector | Inline metadata text? Key? Empty? |
| Graph | Node label + top properties? |

If a vector match has no inline text and gets fed into RAG as context, the model sees `[3] (empty)`. This wastes a context slot and confuses the model.

**Fallback strategy per primitive:**

| Primitive | Primary snippet | Fallback |
|---|---|---|
| KV | Value text (truncated) | Key name |
| JSON | Extracted text from key fields (title, name, description) | Serialized JSON (truncated) |
| Event | Payload text | `{event_type} at {timestamp}` |
| Vector | Inline metadata text | Metadata JSON (truncated) |
| Graph | Node label + description | Node label + property keys |

**When to address:** v0.1 (snippet extraction is part of the basic substrate). Define the fallback chain so no hit ever has an empty snippet.

---

## 7. Expansion hallucination guard is query-level, not term-level

**Version:** v0.2
**Severity:** Quality (edge case)

The guard discards expansions sharing fewer than 2 stemmed terms with the original query. A 3-word query where the expansion hallucinates 1 of 3 terms still passes. The hallucinated term could poison BM25 results with irrelevant documents.

**Example:**
```
Original: "ssh key setup"
Expansion: "ssh key blockchain"  ← "blockchain" hallucinated, but shares 2/3 terms → passes guard
```

**Mitigations:**
1. Per-term validation — each expansion term should relate to at least one original term. Expensive (requires term-level semantic check).
2. AutoResearch workaround — experiment with expansion on vs off. If hallucinated terms hurt NDCG, AutoResearch will discover that expansion doesn't help on this dataset and disable it.
3. Grammar-constrained generation — the model can only produce lex/vec/hyde lines, limiting the space of possible hallucinations.

**Current status:** Known limitation. AutoResearch is the practical mitigation — it empirically tests whether expansion helps, regardless of the mechanism.

**When to address:** If BEIR ablation shows expansion hurting on specific datasets, investigate per-term validation. Not v0.2.

---

## 8. No recipe versioning/migration strategy

**Version:** v0.0
**Severity:** Future-proofing

Recipes are `"version": 1`. When the schema evolves (adding scan mode, new fusion methods, restructured fields), stored recipes on existing branches need migration.

The three-level merge handles adding new fields gracefully — they inherit from built-in defaults. But renamed or restructured fields (e.g., if `fusion.k` becomes `fusion.rrf.k`) would break existing recipes.

**Strategy:**
1. **Adding fields:** No migration needed. New fields default to built-in values. Existing recipes work unchanged.
2. **Renaming fields:** Version bump to 2. On load, if `version == 1`, run migration function that renames fields. Write back as version 2.
3. **Removing fields:** Version bump. Migration strips removed fields. Old values ignored.
4. **Restructuring:** Version bump. Migration maps old structure to new.

Migration runs automatically on recipe load (similar to strata.toml migration in v0.0). The user never sees it.

**When to address:** When the first breaking schema change is needed. Not now — the schema is designed to be extended without breaking changes (presence = enabled, new keys additive).
