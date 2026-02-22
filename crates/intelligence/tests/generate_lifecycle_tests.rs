//! Integration tests for GenerateModelState lifecycle.
//!
//! These tests require real model files and are marked `#[ignore]`.
//! Run with: `cargo test -p strata-intelligence --features embed -- --ignored`

#[cfg(feature = "embed")]
mod tests {
    use strata_intelligence::generate::{with_engine, GenerateModelState};

    #[test]
    #[ignore]
    fn test_generate_load_and_run() {
        let state = GenerateModelState::default();
        let entry = state.get_or_load("tinyllama").expect("should return entry");
        let result = with_engine(&entry, |engine| {
            let config = strata_intelligence::GenerationConfig {
                max_tokens: 10,
                ..Default::default()
            };
            engine.generate("Hello", &config)
        });
        let text = result.expect("engine access").expect("generation");
        assert!(!text.is_empty(), "generated text should not be empty");
    }

    #[test]
    #[ignore]
    fn test_generate_deterministic_with_seed() {
        let state = GenerateModelState::default();
        let entry = state.get_or_load("tinyllama").expect("should return entry");

        let config = strata_intelligence::GenerationConfig {
            max_tokens: 20,
            sampling: strata_intelligence::SamplingConfig {
                temperature: 0.8,
                seed: Some(42),
                ..Default::default()
            },
            ..Default::default()
        };

        let text1 = with_engine(&entry, |engine| {
            engine.generate("Once upon a time", &config)
        })
        .unwrap()
        .unwrap();

        // Reload to get fresh KV cache
        state.unload("tinyllama");
        let entry2 = state.get_or_load("tinyllama").expect("should return entry");
        let text2 = with_engine(&entry2, |engine| {
            engine.generate("Once upon a time", &config)
        })
        .unwrap()
        .unwrap();

        assert_eq!(text1, text2, "same seed should produce same output");
    }

    #[test]
    #[ignore]
    fn test_tokenize_roundtrip() {
        let state = GenerateModelState::default();
        let entry = state.get_or_load("tinyllama").expect("should return entry");

        let original = "Hello world";
        let ids =
            with_engine(&entry, |engine| engine.encode(original, false)).expect("engine access");
        let decoded = with_engine(&entry, |engine| engine.decode(&ids)).expect("engine access");

        // Roundtrip may not be exact (whitespace normalization), but should contain the words
        assert!(
            decoded.contains("Hello") || decoded.contains("hello"),
            "decoded text should approximately match: got '{}'",
            decoded
        );
    }

    #[test]
    #[ignore]
    fn test_unload_frees_entry() {
        let state = GenerateModelState::default();
        let _ = state.get_or_load("tinyllama").expect("should return entry");
        assert_eq!(state.loaded_models().len(), 1);
        state.unload("tinyllama");
        assert!(state.loaded_models().is_empty());
    }
}
