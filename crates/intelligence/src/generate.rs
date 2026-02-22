//! Generation model lifecycle: lazy-loading, caching, and unloading of
//! generation engines by model name.
//!
//! Unlike [`EmbedModelState`](crate::embed::EmbedModelState) which manages a
//! single model, `GenerateModelState` supports multiple named models loaded
//! concurrently. Each engine is wrapped in a `Mutex` because
//! `GenerationEngine::generate()` requires `&mut self` (KV cache).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use strata_inference::GenerationEngine;

/// Cached generation engine entry (success or error).
///
/// This type is public so that `get_or_load` can return `Arc<Mutex<CachedEngine>>`,
/// but its fields are private. Use [`with_engine`] to access the inner engine.
pub struct CachedEngine {
    inner: Result<GenerationEngine, String>,
}

/// Multi-model generation state stored as a Database extension.
///
/// Supports concurrent generation on different models while serializing
/// same-model access (required because `GenerationEngine` uses a mutable
/// KV cache).
///
/// **Two-level locking:**
/// - Outer `Mutex<HashMap>` — held briefly during map lookup/insert
/// - Per-model `Arc<Mutex<CachedEngine>>` — held during `generate()` (potentially seconds)
pub struct GenerateModelState {
    engines: Mutex<HashMap<String, Arc<Mutex<CachedEngine>>>>,
}

impl Default for GenerateModelState {
    fn default() -> Self {
        Self {
            engines: Mutex::new(HashMap::new()),
        }
    }
}

impl GenerateModelState {
    /// Get or load a generation engine by model name.
    ///
    /// On first call for a given name, loads the model from the registry.
    /// Subsequent calls return the cached engine (or cached error).
    /// Call [`unload`] first to retry a failed load.
    pub fn get_or_load(&self, name: &str) -> Result<Arc<Mutex<CachedEngine>>, String> {
        // Fast path: check if already loaded
        {
            let map = self.engines.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = map.get(name) {
                return Ok(Arc::clone(entry));
            }
        }

        // Slow path: load model outside the outer lock to avoid blocking
        // other model lookups during a potentially slow model load.
        let result = GenerationEngine::from_registry(name)
            .map_err(|e| format!("Failed to load generation model '{}': {}", name, e));

        let entry = Arc::new(Mutex::new(CachedEngine { inner: result }));

        // Double-checked insert: another thread may have loaded the same model
        let mut map = self.engines.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(name.to_string()).or_insert(entry);
        Ok(Arc::clone(entry))
    }

    /// Unload a model from the cache, freeing its memory.
    ///
    /// Returns `true` if the model was loaded, `false` if it wasn't.
    pub fn unload(&self, name: &str) -> bool {
        let mut map = self.engines.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(name).is_some()
    }

    /// List the names of all currently loaded models.
    pub fn loaded_models(&self) -> Vec<String> {
        let map = self.engines.lock().unwrap_or_else(|e| e.into_inner());
        map.keys().cloned().collect()
    }
}

/// Execute a closure with a locked generation engine.
///
/// Acquires the per-model mutex, checks for a cached error, and passes
/// the engine to `f`. This helper avoids duplicating lock + error-check
/// boilerplate in every handler.
pub fn with_engine<T>(
    entry: &Arc<Mutex<CachedEngine>>,
    f: impl FnOnce(&mut GenerationEngine) -> T,
) -> Result<T, String> {
    let mut guard = entry.lock().unwrap_or_else(|e| e.into_inner());
    match &mut guard.inner {
        Ok(engine) => Ok(f(engine)),
        Err(e) => Err(e.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_state_is_empty() {
        let state = GenerateModelState::default();
        assert!(state.loaded_models().is_empty());
    }

    #[test]
    fn test_loaded_models_empty_initially() {
        let state = GenerateModelState::default();
        let models = state.loaded_models();
        assert_eq!(models.len(), 0);
    }

    #[test]
    fn test_unload_nonexistent_returns_false() {
        let state = GenerateModelState::default();
        assert!(!state.unload("nonexistent-model"));
    }

    #[test]
    fn test_get_or_load_caches_error() {
        let state = GenerateModelState::default();
        // First load attempt — will fail (no model file)
        let entry1 = state.get_or_load("nonexistent-model-xyz");
        assert!(entry1.is_ok(), "get_or_load should return Ok(Arc)");
        let entry1 = entry1.unwrap();
        let result1 = with_engine(&entry1, |_| ());
        assert!(result1.is_err(), "engine should have failed to load");

        // Second load attempt — returns the same cached error
        let entry2 = state.get_or_load("nonexistent-model-xyz").unwrap();
        let result2 = with_engine(&entry2, |_| ());
        assert!(result2.is_err());
        assert_eq!(
            result1.unwrap_err(),
            result2.unwrap_err(),
            "cached error should be identical"
        );
    }

    #[test]
    fn test_concurrent_load_same_model() {
        let state = Arc::new(GenerateModelState::default());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let s = Arc::clone(&state);
            handles.push(std::thread::spawn(move || {
                s.get_or_load("nonexistent-concurrent").unwrap()
            }));
        }

        let entries: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All threads should get the same Arc (same pointer)
        for entry in &entries[1..] {
            assert!(Arc::ptr_eq(&entries[0], entry));
        }
    }

    #[test]
    fn test_unload_removes_entry() {
        let state = GenerateModelState::default();
        // Load (will fail, but entry is cached)
        let _ = state.get_or_load("model-to-unload");
        assert_eq!(state.loaded_models().len(), 1);
        assert!(state.unload("model-to-unload"));
        assert!(state.loaded_models().is_empty());
    }
}
