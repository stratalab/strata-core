//! Generation model lifecycle: lazy-loading, caching, and unloading of
//! generation engines by model name.
//!
//! Unlike [`EmbedModelState`](crate::embed::EmbedModelState) which manages a
//! single model, `GenerateModelState` supports multiple named models loaded
//! concurrently. Each engine is wrapped in a `Mutex` because
//! `GenerationEngine::generate()` requires `&mut self` (KV cache).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use strata_inference::{GenerationEngine, ProviderKind};

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

    /// Create a cloud generation engine without caching.
    ///
    /// Cloud engines are stateless HTTP wrappers — trivially cheap to create.
    /// Not caching avoids stale-API-key bugs: when the user changes their key
    /// via `CONFIGURE SET`, the next `GENERATE` call picks it up immediately.
    ///
    /// The returned `Arc<Mutex<CachedEngine>>` is compatible with [`with_engine`]
    /// but is not stored in the internal map.
    pub fn create_cloud_engine(
        provider: ProviderKind,
        api_key: String,
        model: &str,
    ) -> Result<Arc<Mutex<CachedEngine>>, String> {
        let result = GenerationEngine::cloud(provider, api_key, model.to_string()).map_err(|e| {
            format!(
                "Failed to create {} engine for '{}': {}",
                provider, model, e
            )
        });

        Ok(Arc::new(Mutex::new(CachedEngine { inner: result })))
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

    // -----------------------------------------------------------------------
    // with_engine double-Result pattern tests
    // -----------------------------------------------------------------------
    // The executor handlers call with_engine(|engine| engine.generate(&req)?)
    // which produces Result<Result<T, InferenceError>, String>. These tests
    // verify the pattern works correctly with both error layers.

    #[test]
    fn with_engine_result_closure_on_error_entry() {
        // Simulate the executor's generate handler pattern: closure returns
        // Result<T, InferenceError>. When the engine failed to load, the outer
        // Result<_, String> should carry the cached load error.
        let state = GenerateModelState::default();
        let entry = state.get_or_load("nonexistent-double-result").unwrap();

        let result: Result<Result<String, strata_inference::InferenceError>, String> =
            with_engine(&entry, |engine| {
                let req = strata_inference::GenerateRequest {
                    prompt: "test".into(),
                    max_tokens: 1,
                    ..Default::default()
                };
                let resp = engine.generate(&req)?;
                Ok(resp.text)
            });

        // Outer Result should be Err (engine failed to load)
        assert!(
            result.is_err(),
            "outer Result should be Err for failed engine"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("nonexistent-double-result"),
            "error should contain model name: {err}"
        );
    }

    #[test]
    fn with_engine_error_message_contains_model_name() {
        let state = GenerateModelState::default();
        let entry = state.get_or_load("my-missing-model").unwrap();

        let result = with_engine(&entry, |_| ());
        let err = result.unwrap_err();
        assert!(
            err.contains("my-missing-model"),
            "error should contain the model name: {err}"
        );
        assert!(
            err.contains("Failed to load"),
            "error should have descriptive prefix: {err}"
        );
    }

    #[test]
    fn with_engine_double_result_flatten_pattern() {
        // This mirrors exactly how the executor handlers flatten the double-Result:
        //   with_engine(|e| e.op()?)
        //     .map_err(|e| format!("outer: {}", e))?
        //     .map_err(|e| format!("inner: {}", e))?
        let state = GenerateModelState::default();
        let entry = state.get_or_load("nonexistent-flatten").unwrap();

        let result: Result<String, String> =
            with_engine(&entry, |engine| engine.encode("hello", false))
                .map_err(|e| format!("outer: {}", e))
                .and_then(|inner| inner.map_err(|e| format!("inner: {}", e)))
                .map(|ids| format!("{} tokens", ids.len()));

        // Should fail at the outer layer (engine not loaded)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.starts_with("outer:"),
            "should fail at outer layer, got: {err}"
        );
    }

    #[test]
    fn with_engine_sequential_calls_on_same_entry() {
        // Mirrors tokenize handler: encode then decode on the same entry
        let state = GenerateModelState::default();
        let entry = state.get_or_load("nonexistent-seq").unwrap();

        let r1 = with_engine(&entry, |_| "first");
        let r2 = with_engine(&entry, |_| "second");

        // Both should fail with the same cached error
        assert!(r1.is_err());
        assert!(r2.is_err());
        assert_eq!(
            r1.unwrap_err(),
            r2.unwrap_err(),
            "sequential calls should get identical cached error"
        );
    }

    #[test]
    fn unload_then_reload_gets_fresh_entry() {
        let state = GenerateModelState::default();
        let entry1 = state.get_or_load("reload-test").unwrap();
        state.unload("reload-test");
        let entry2 = state.get_or_load("reload-test").unwrap();

        // After unload + reload, the Arc should be different (fresh entry)
        assert!(
            !Arc::ptr_eq(&entry1, &entry2),
            "reload after unload should create new entry"
        );
    }

    #[test]
    fn loaded_models_reflects_multiple_models() {
        let state = GenerateModelState::default();
        let _ = state.get_or_load("model-a");
        let _ = state.get_or_load("model-b");
        let _ = state.get_or_load("model-c");

        let mut models = state.loaded_models();
        models.sort();
        assert_eq!(models, vec!["model-a", "model-b", "model-c"]);

        state.unload("model-b");
        let mut models = state.loaded_models();
        models.sort();
        assert_eq!(models, vec!["model-a", "model-c"]);
    }

    // -----------------------------------------------------------------------
    // create_cloud_engine tests
    // -----------------------------------------------------------------------

    #[test]
    fn create_cloud_engine_returns_ok() {
        // Cloud engines are just lightweight wrappers — construction always succeeds
        // (the inner engine may hold an error, but the outer Result is Ok)
        let entry = GenerateModelState::create_cloud_engine(
            ProviderKind::Anthropic,
            "sk-test".into(),
            "claude-sonnet-4-20250514",
        );
        assert!(entry.is_ok());
    }

    #[test]
    fn create_cloud_engine_does_not_cache() {
        let state = GenerateModelState::default();

        // Create a cloud engine via the static method
        let _entry =
            GenerateModelState::create_cloud_engine(ProviderKind::OpenAI, "key".into(), "gpt-4")
                .unwrap();

        // It should NOT appear in loaded_models (not cached)
        assert!(
            state.loaded_models().is_empty(),
            "create_cloud_engine should not add to the cache"
        );
    }

    #[test]
    fn create_cloud_engine_each_call_is_independent() {
        let entry1 = GenerateModelState::create_cloud_engine(
            ProviderKind::Anthropic,
            "key-1".into(),
            "model",
        )
        .unwrap();
        let entry2 = GenerateModelState::create_cloud_engine(
            ProviderKind::Anthropic,
            "key-2".into(),
            "model",
        )
        .unwrap();

        // Each call creates a fresh, independent entry (different Arcs)
        assert!(
            !Arc::ptr_eq(&entry1, &entry2),
            "each call should create a new entry"
        );
    }

    #[test]
    fn create_cloud_engine_local_provider_wraps_error() {
        // ProviderKind::Local should fail inside GenerationEngine::cloud()
        // but the outer Result is still Ok — the error is inside CachedEngine
        let entry =
            GenerateModelState::create_cloud_engine(ProviderKind::Local, "key".into(), "model")
                .unwrap();

        let result = with_engine(&entry, |_| ());
        assert!(result.is_err(), "Local via cloud should fail");
        let err = result.unwrap_err();
        assert!(
            err.contains("Failed to create"),
            "error should be descriptive: {err}"
        );
    }

    #[test]
    fn create_cloud_engine_compatible_with_with_engine() {
        // Verify the with_engine helper works with non-cached entries
        let entry = GenerateModelState::create_cloud_engine(
            ProviderKind::Anthropic,
            "sk-test".into(),
            "claude-sonnet-4-20250514",
        )
        .unwrap();

        // Should be able to call with_engine — the engine is valid (Anthropic
        // provider constructed successfully), so the closure runs
        let result = with_engine(&entry, |engine| engine.provider());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ProviderKind::Anthropic);
    }

    #[test]
    fn create_cloud_engine_error_message_contains_provider_and_model() {
        let entry =
            GenerateModelState::create_cloud_engine(ProviderKind::Local, "key".into(), "my-model")
                .unwrap();

        let err = with_engine(&entry, |_| ()).unwrap_err();
        assert!(
            err.contains("local"),
            "error should contain provider: {err}"
        );
        assert!(
            err.contains("my-model"),
            "error should contain model: {err}"
        );
    }

    #[test]
    fn cloud_engine_and_local_entries_are_independent() {
        let state = GenerateModelState::default();

        // Load a local model (will fail, but it's cached)
        let _ = state.get_or_load("local-model");
        assert_eq!(state.loaded_models().len(), 1);

        // Create a cloud engine — should NOT affect the local cache
        let _cloud = GenerateModelState::create_cloud_engine(
            ProviderKind::Google,
            "key".into(),
            "gemini-pro",
        )
        .unwrap();

        // Local cache should still have exactly 1 entry
        assert_eq!(state.loaded_models().len(), 1);
        assert!(state.loaded_models().contains(&"local-model".to_string()));
    }
}
