//! Persistent FIFO cache for query expansion results.
//!
//! Cache lives in `_system_` space on the branch using the same KV-write-via-
//! transaction pattern as `recipe_store`. Per-branch storage means a branch
//! fork inherits the parent's warm cache for free via the existing COW path.
//!
//! # Storage layout
//!
//! Two key shapes in `_system_` space, both per-branch:
//!
//! | Key | Value | Purpose |
//! |---|---|---|
//! | `expand_cache:{sha256_hex}` | JSON `CacheEntry` | One entry per cached expansion |
//! | `expand_cache_fifo` | JSON `FifoIndex` | Sidecar index for FIFO eviction |
//!
//! The hash is `SHA256(query || 0x00 || model_spec)` — the null-byte separator
//! prevents collisions where `query="ab", model="c"` could collide with
//! `query="a", model="bc"`. Changing `models.expand` produces a different hash,
//! so model swaps invalidate automatically; old entries age out via FIFO.
//!
//! # Failure modes
//!
//! All read failures (missing key, wrong value type, corrupt JSON) return
//! `None` from [`get`], so the caller falls through to the model. Write
//! failures bubble up as `StrataResult::Err`; the caller in `expand_query`
//! logs and continues. The cache is **non-fatal** — it can never block
//! search.
//!
//! # Known limitations
//!
//! - **Cache key locks in filter config.** Cached entries are *post-filter*,
//!   so changing `expansion.strategy` or `expansion.min_shared_stems` does
//!   not invalidate existing entries. AutoResearch sweeps over filter
//!   parameters will return the cached result computed under whichever
//!   config wrote first. Workaround: set `cache_enabled: false` while
//!   sweeping filter parameters, or accept that filter sweeps need a fresh
//!   branch. Tracked for redesign post-v0.2.
//! - **Cache key locks in generation params.** Same applies to
//!   `temperature`, `top_k`, etc. — different gen params produce different
//!   model outputs, but the key only hashes `(query, model_spec)`.
//! - **Concurrent writes can orphan entries.** Two parallel `put()` calls on
//!   the same branch read the FIFO at the same version; one wins, the
//!   other's entry is written to KV but not registered in the FIFO. The
//!   intelligence layer issues serial expansion calls per query, so this
//!   is unreachable in practice today.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

use strata_core::BranchId;
use strata_core::Value;
use strata_engine::system_space::system_kv_key;
use strata_engine::Database;
use strata_engine::StrataResult;
use strata_search::expand::{ExpandedQuery, QueryType};

const KEY_PREFIX: &str = "expand_cache:";
const FIFO_KEY: &str = "expand_cache_fifo";

// ============================================================================
// Public API
// ============================================================================

/// SHA-256 hex of `(query || 0x00 || model_spec)`. Stable across calls.
///
/// The null-byte separator is critical: without it, `query="ab"+model="c"`
/// would hash identically to `query="a"+model="bc"`.
pub fn cache_key(query: &str, model_spec: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    hasher.update([0u8]);
    hasher.update(model_spec.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Read a cached expansion. Returns `None` on miss, wrong value type, any
/// deserialization error, or if every variant is dropped by forward-compat
/// filtering — graceful degradation, never propagates an error.
///
/// **Empty-after-filter is treated as a miss.** If a future schema adds a
/// new `QueryType` variant and an old reader sees a cache entry containing
/// only that variant, every DTO is dropped by `into_expanded()` and the
/// result would be `Vec::new()`. We return `None` instead so the caller
/// falls through to the model — otherwise the cache would silently serve
/// empty expansions until FIFO eviction.
pub fn get(db: &Database, branch_id: BranchId, key_hex: &str) -> Option<Vec<ExpandedQuery>> {
    let entry_key = system_kv_key(branch_id, &format!("{KEY_PREFIX}{key_hex}"));
    let value = db.get_value_direct(&entry_key).ok()??;
    let json = match value {
        Value::String(s) => s,
        _ => return None,
    };
    let entry: CacheEntry = serde_json::from_str(&json).ok()?;
    let variants: Vec<ExpandedQuery> = entry
        .variants
        .into_iter()
        .filter_map(|d| d.into_expanded())
        .collect();
    if variants.is_empty() {
        None
    } else {
        Some(variants)
    }
}

/// Write a new entry and apply FIFO eviction atomically.
///
/// Repeat writes for an existing key overwrite the value but do **not** bump
/// the FIFO position — FIFO is insertion order, not access order. When the
/// cache exceeds `capacity`, oldest entries are evicted in the same
/// transaction.
pub fn put(
    db: &Database,
    branch_id: BranchId,
    key_hex: &str,
    query: &str,
    model_spec: &str,
    variants: &[ExpandedQuery],
    capacity: usize,
) -> StrataResult<()> {
    // Pre-serialize the entry outside the closure (recipe_store pattern).
    let entry = CacheEntry {
        query: query.to_string(),
        model_spec: model_spec.to_string(),
        variants: variants
            .iter()
            .map(ExpandedQueryDto::from_expanded)
            .collect(),
        created_micros: now_micros(),
    };
    let entry_json = serde_json::to_string(&entry)?;
    let entry_key = system_kv_key(branch_id, &format!("{KEY_PREFIX}{key_hex}"));
    let fifo_key = system_kv_key(branch_id, FIFO_KEY);

    db.transaction(branch_id, |txn| -> StrataResult<()> {
        // 1. Read current FIFO state (absent on first call, corrupt → fresh).
        let mut fifo: FifoIndex = match txn.get(&fifo_key)? {
            Some(Value::String(s)) => serde_json::from_str(&s).unwrap_or_else(|_| FifoIndex {
                capacity,
                hashes: Vec::new(),
            }),
            _ => FifoIndex {
                capacity,
                hashes: Vec::new(),
            },
        };

        // 2. Write the new entry (or overwrite-in-place for repeat keys).
        txn.put(entry_key.clone(), Value::String(entry_json.clone()))?;

        // 3. Append to FIFO only on first insert; evict oldest while over cap.
        //    Repeat writes for an existing key do not bump position.
        if !fifo.hashes.iter().any(|h| h == key_hex) {
            fifo.hashes.push(key_hex.to_string());
            while fifo.hashes.len() > capacity {
                let evict = fifo.hashes.remove(0);
                let evict_key = system_kv_key(branch_id, &format!("{KEY_PREFIX}{evict}"));
                txn.delete(evict_key)?;
            }
        }

        // 4. Persist the updated FIFO state.
        let fifo_json = serde_json::to_string(&fifo)?;
        txn.put(fifo_key.clone(), Value::String(fifo_json))?;
        Ok(())
    })
}

// ============================================================================
// Internal types
// ============================================================================

#[derive(Serialize, Deserialize)]
struct CacheEntry {
    query: String,
    model_spec: String,
    variants: Vec<ExpandedQueryDto>,
    created_micros: u64,
}

#[derive(Serialize, Deserialize)]
struct FifoIndex {
    capacity: usize,
    hashes: Vec<String>,
}

/// Serializable bridge for `ExpandedQuery` (which lives in `strata-search`
/// and does not derive serde). The `query_type` field is a string so future
/// schema additions don't break old caches.
#[derive(Serialize, Deserialize)]
struct ExpandedQueryDto {
    query_type: String,
    text: String,
}

impl ExpandedQueryDto {
    fn from_expanded(eq: &ExpandedQuery) -> Self {
        let query_type = match eq.query_type {
            QueryType::Lex => "lex",
            QueryType::Vec => "vec",
            QueryType::Hyde => "hyde",
        }
        .to_string();
        Self {
            query_type,
            text: eq.text.clone(),
        }
    }

    /// Returns `None` for unknown `query_type` values — protects against
    /// future schema additions where an old reader sees a newer DTO.
    fn into_expanded(self) -> Option<ExpandedQuery> {
        let query_type = match self.query_type.as_str() {
            "lex" => QueryType::Lex,
            "vec" => QueryType::Vec,
            "hyde" => QueryType::Hyde,
            _ => return None,
        };
        Some(ExpandedQuery {
            query_type,
            text: self.text,
        })
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_db() -> std::sync::Arc<Database> {
        use strata_engine::database::search_only_cache_spec;
        Database::open_runtime(search_only_cache_spec()).expect("open_runtime should succeed")
    }

    fn lex(text: &str) -> ExpandedQuery {
        ExpandedQuery {
            query_type: QueryType::Lex,
            text: text.into(),
        }
    }

    #[test]
    fn test_cache_key_deterministic() {
        let k1 = cache_key("ssh setup", "qwen3:1.7b");
        let k2 = cache_key("ssh setup", "qwen3:1.7b");
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn test_cache_key_distinguishes_query() {
        let k1 = cache_key("query a", "qwen3:1.7b");
        let k2 = cache_key("query b", "qwen3:1.7b");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_cache_key_distinguishes_model() {
        let k1 = cache_key("ssh setup", "qwen3:1.7b");
        let k2 = cache_key("ssh setup", "qwen3:4b");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_cache_key_null_byte_separator() {
        // Without the null-byte separator, ("ab", "c") would hash the same as
        // ("a", "bc"). The separator must prevent that collision.
        let k1 = cache_key("ab", "c");
        let k2 = cache_key("a", "bc");
        assert_ne!(
            k1, k2,
            "null-byte separator must prevent boundary collision"
        );
    }

    #[test]
    fn test_put_get_roundtrip() {
        let db = fresh_db();
        let branch = BranchId::new();
        let key = cache_key("ssh setup", "qwen3:1.7b");
        let variants = vec![
            lex("ssh keygen"),
            ExpandedQuery {
                query_type: QueryType::Vec,
                text: "ssh key authentication".into(),
            },
            ExpandedQuery {
                query_type: QueryType::Hyde,
                text: "Generate an ed25519 key pair.".into(),
            },
        ];

        put(&db, branch, &key, "ssh setup", "qwen3:1.7b", &variants, 100).unwrap();
        let loaded = get(&db, branch, &key).expect("cache hit expected");

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].query_type, QueryType::Lex);
        assert_eq!(loaded[0].text, "ssh keygen");
        assert_eq!(loaded[1].query_type, QueryType::Vec);
        assert_eq!(loaded[2].query_type, QueryType::Hyde);
    }

    #[test]
    fn test_get_miss_returns_none() {
        let db = fresh_db();
        let branch = BranchId::new();
        let key = cache_key("never written", "qwen3:1.7b");
        assert!(get(&db, branch, &key).is_none());
    }

    #[test]
    fn test_get_corrupt_value_returns_none() {
        // Write a non-string Value at the cache key — get() must reject it.
        let db = fresh_db();
        let branch = BranchId::new();
        let key_hex = cache_key("query", "model");
        let entry_key = system_kv_key(branch, &format!("{KEY_PREFIX}{key_hex}"));

        db.transaction(branch, |txn| txn.put(entry_key.clone(), Value::Int(42)))
            .unwrap();

        assert!(get(&db, branch, &key_hex).is_none());
    }

    #[test]
    fn test_get_corrupt_json_returns_none() {
        // Write a string that isn't valid CacheEntry JSON.
        let db = fresh_db();
        let branch = BranchId::new();
        let key_hex = cache_key("query", "model");
        let entry_key = system_kv_key(branch, &format!("{KEY_PREFIX}{key_hex}"));

        db.transaction(branch, |txn| {
            txn.put(entry_key.clone(), Value::String("not json".into()))
        })
        .unwrap();

        assert!(get(&db, branch, &key_hex).is_none());
    }

    #[test]
    fn test_fifo_eviction_at_capacity() {
        let db = fresh_db();
        let branch = BranchId::new();
        let cap = 3;

        // Write 5 entries with cap=3 → first 2 should be evicted.
        let keys: Vec<String> = (0..5).map(|i| cache_key(&format!("q{i}"), "m")).collect();
        for (i, k) in keys.iter().enumerate() {
            put(
                &db,
                branch,
                k,
                &format!("q{i}"),
                "m",
                &[lex(&format!("v{i}"))],
                cap,
            )
            .unwrap();
        }

        assert!(get(&db, branch, &keys[0]).is_none(), "q0 should be evicted");
        assert!(get(&db, branch, &keys[1]).is_none(), "q1 should be evicted");
        assert!(get(&db, branch, &keys[2]).is_some(), "q2 should remain");
        assert!(get(&db, branch, &keys[3]).is_some(), "q3 should remain");
        assert!(get(&db, branch, &keys[4]).is_some(), "q4 should remain");
    }

    #[test]
    fn test_fifo_index_capacity_after_eviction() {
        // Inspect the sidecar FIFO index directly.
        let db = fresh_db();
        let branch = BranchId::new();
        let cap = 2;

        for i in 0..4 {
            let k = cache_key(&format!("q{i}"), "m");
            put(&db, branch, &k, &format!("q{i}"), "m", &[lex("v")], cap).unwrap();
        }

        let fifo_key = system_kv_key(branch, FIFO_KEY);
        let value = db.get_value_direct(&fifo_key).unwrap().unwrap();
        let json = match value {
            Value::String(s) => s,
            _ => panic!("FIFO key should hold a string"),
        };
        let fifo: FifoIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(fifo.hashes.len(), cap, "FIFO should be at capacity exactly");
    }

    #[test]
    fn test_overwrite_does_not_advance_fifo() {
        // Sequence: a, b, a (repeat), c with cap=2.
        // After: fifo=[b,c], `a` should be evicted because the repeat write
        // did NOT bump it to the back of the queue.
        let db = fresh_db();
        let branch = BranchId::new();
        let cap = 2;
        let key_a = cache_key("a", "m");
        let key_b = cache_key("b", "m");
        let key_c = cache_key("c", "m");

        put(&db, branch, &key_a, "a", "m", &[lex("v_a1")], cap).unwrap();
        put(&db, branch, &key_b, "b", "m", &[lex("v_b")], cap).unwrap();
        put(&db, branch, &key_a, "a", "m", &[lex("v_a2")], cap).unwrap(); // repeat
        put(&db, branch, &key_c, "c", "m", &[lex("v_c")], cap).unwrap();

        assert!(
            get(&db, branch, &key_a).is_none(),
            "a should be evicted even though it was written 3rd"
        );
        assert!(get(&db, branch, &key_b).is_some(), "b should remain");
        assert!(get(&db, branch, &key_c).is_some(), "c should remain");
    }

    #[test]
    fn test_overwrite_updates_value() {
        // Repeat writes update the value even though FIFO position is unchanged.
        let db = fresh_db();
        let branch = BranchId::new();
        let key = cache_key("q", "m");

        put(&db, branch, &key, "q", "m", &[lex("first")], 10).unwrap();
        put(&db, branch, &key, "q", "m", &[lex("second")], 10).unwrap();

        let loaded = get(&db, branch, &key).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].text, "second");
    }

    #[test]
    fn test_dto_unknown_query_type_dropped() {
        // A DTO with an unknown query_type is dropped on read (forward compat).
        let dto = ExpandedQueryDto {
            query_type: "future_type".into(),
            text: "x".into(),
        };
        assert!(dto.into_expanded().is_none());
    }

    #[test]
    fn test_get_returns_none_when_all_dtos_unknown() {
        // Forward-compat regression: if a future schema introduces a new
        // QueryType variant and an old reader sees a cache entry containing
        // ONLY that variant, every DTO is dropped by `into_expanded()`. The
        // result must be treated as a cache miss (None), not a successful
        // hit returning Some(vec![]) — otherwise the caller would skip the
        // model call and silently serve empty expansions.
        let db = fresh_db();
        let branch = BranchId::new();
        let key_hex = cache_key("query", "model");
        let entry_key = system_kv_key(branch, &format!("{KEY_PREFIX}{key_hex}"));

        let entry = CacheEntry {
            query: "query".into(),
            model_spec: "model".into(),
            variants: vec![ExpandedQueryDto {
                query_type: "future_type_not_in_enum".into(),
                text: "x".into(),
            }],
            created_micros: 0,
        };
        let json = serde_json::to_string(&entry).unwrap();
        db.transaction(branch, |txn| {
            txn.put(entry_key.clone(), Value::String(json))
        })
        .unwrap();

        // The entry exists, deserializes successfully, but every DTO is dropped.
        // get() must return None to force a model call on the next expand.
        assert!(get(&db, branch, &key_hex).is_none());
    }

    #[test]
    fn test_dto_roundtrip_preserves_all_query_types() {
        for qt in [QueryType::Lex, QueryType::Vec, QueryType::Hyde] {
            let original = ExpandedQuery {
                query_type: qt,
                text: "text".into(),
            };
            let dto = ExpandedQueryDto::from_expanded(&original);
            let recovered = dto.into_expanded().unwrap();
            assert_eq!(recovered.query_type, qt);
            assert_eq!(recovered.text, "text");
        }
    }

    #[test]
    fn test_branch_isolation() {
        let db = fresh_db();
        let b1 = BranchId::new();
        let b2 = BranchId::new();
        let key = cache_key("q", "m");

        put(&db, b1, &key, "q", "m", &[lex("on b1")], 10).unwrap();

        // b2 sees nothing — caches are per-branch.
        assert!(get(&db, b2, &key).is_none());
        assert!(get(&db, b1, &key).is_some());
    }
}
