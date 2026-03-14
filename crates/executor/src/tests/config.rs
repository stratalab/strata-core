//! Integration tests for ConfigureSet and ConfigureGetKey commands.
//!
//! These tests exercise the full executor path: Command → dispatch → handler → database.

use crate::{Command, Error, Executor, Output};
use strata_engine::Database;
use strata_security::AccessMode;

/// Create a test executor with an in-memory database.
fn create_test_executor() -> Executor {
    let db = Database::cache().unwrap();
    Executor::new(db)
}

// =============================================================================
// ConfigureSet + ConfigureGetKey: happy-path round-trips
// =============================================================================

#[test]
fn configure_set_and_get_provider() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "anthropic".into(),
    });
    assert!(matches!(result, Ok(Output::Unit)));

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("anthropic".into())));
}

#[test]
fn configure_set_and_get_default_model() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "default_model".into(),
            value: "claude-sonnet-4-20250514".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "default_model".into(),
        })
        .unwrap();
    assert_eq!(
        result,
        Output::ConfigValue(Some("claude-sonnet-4-20250514".into()))
    );
}

#[test]
fn configure_set_and_get_api_keys() {
    let executor = create_test_executor();

    for (key, value, masked) in [
        ("anthropic_api_key", "sk-ant-test-123", "sk-a***"),
        ("openai_api_key", "sk-test-456", "sk-t***"),
        ("google_api_key", "AIza-test-789", "AIza***"),
    ] {
        executor
            .execute(Command::ConfigureSet {
                key: key.into(),
                value: value.into(),
            })
            .unwrap();

        let result = executor
            .execute(Command::ConfigureGetKey { key: key.into() })
            .unwrap();
        assert_eq!(result, Output::ConfigValue(Some(masked.into())));
    }
}

// =============================================================================
// Default values
// =============================================================================

#[test]
fn configure_get_provider_default_is_local() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("local".into())));
}

#[test]
fn configure_get_unset_optional_key_returns_none() {
    let executor = create_test_executor();

    for key in [
        "default_model",
        "anthropic_api_key",
        "openai_api_key",
        "google_api_key",
    ] {
        let result = executor
            .execute(Command::ConfigureGetKey { key: key.into() })
            .unwrap();
        assert_eq!(
            result,
            Output::ConfigValue(None),
            "Unset key {:?} should return None",
            key
        );
    }
}

// =============================================================================
// Overwrite behavior
// =============================================================================

#[test]
fn configure_set_overwrites_previous_value() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "anthropic".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "openai".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("openai".into())));
}

// =============================================================================
// Case insensitivity
// =============================================================================

#[test]
fn configure_set_key_is_case_insensitive() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "PROVIDER".into(),
            value: "google".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("google".into())));
}

#[test]
fn configure_get_key_is_case_insensitive() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "anthropic".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "Provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("anthropic".into())));
}

// =============================================================================
// Whitespace trimming
// =============================================================================

#[test]
fn configure_set_trims_key_whitespace() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "  provider  ".into(),
            value: "openai".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("openai".into())));
}

// =============================================================================
// Empty value validation
// =============================================================================

#[test]
fn configure_set_empty_provider_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("Provider cannot be empty"), "Error: {}", msg);
}

#[test]
fn configure_set_whitespace_only_provider_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "   ".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("Provider cannot be empty"), "Error: {}", msg);
}

#[test]
fn configure_set_empty_api_key_is_allowed() {
    // API keys can be set to empty string — it's equivalent to "not set"
    // (the generate handler treats empty keys the same as None)
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "anthropic_api_key".into(),
        value: "".into(),
    });
    assert!(matches!(result, Ok(Output::Unit)));
}

// =============================================================================
// Provider validation
// =============================================================================

#[test]
fn configure_set_invalid_provider_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "azure".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("Unknown provider"),
        "Error should mention unknown provider: {}",
        msg
    );
    assert!(
        msg.contains("azure"),
        "Error should mention the attempted value: {}",
        msg
    );
    assert!(
        msg.contains("local")
            && msg.contains("anthropic")
            && msg.contains("openai")
            && msg.contains("google"),
        "Error should list valid providers: {}",
        msg
    );
}

#[test]
fn configure_set_valid_providers_all_accepted() {
    let executor = create_test_executor();

    for provider in ["local", "anthropic", "openai", "google"] {
        let result = executor.execute(Command::ConfigureSet {
            key: "provider".into(),
            value: provider.into(),
        });
        assert!(
            result.is_ok(),
            "Provider {:?} should be accepted, got: {:?}",
            provider,
            result
        );
    }
}

#[test]
fn configure_set_provider_case_insensitive() {
    let executor = create_test_executor();

    // "Anthropic" (mixed case) should be accepted
    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "Anthropic".into(),
    });
    assert!(result.is_ok(), "Mixed-case provider should be accepted");
}

// =============================================================================
// Error cases
// =============================================================================

#[test]
fn configure_set_unknown_key_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "unknown_key".into(),
        value: "anything".into(),
    });
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Unknown configuration key"), "Error: {}", msg);
    assert!(
        msg.contains("unknown_key"),
        "Error should mention the key: {}",
        msg
    );
}

#[test]
fn configure_get_unknown_key_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureGetKey {
        key: "nonexistent".into(),
    });
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Unknown configuration key"), "Error: {}", msg);
}

// =============================================================================
// ConfigureSet does not affect unrelated config keys
// =============================================================================

#[test]
fn configure_set_does_not_affect_other_keys() {
    let executor = create_test_executor();

    // Set provider
    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "anthropic".into(),
        })
        .unwrap();

    // API keys should remain None
    for key in [
        "anthropic_api_key",
        "openai_api_key",
        "google_api_key",
        "default_model",
    ] {
        let result = executor
            .execute(Command::ConfigureGetKey { key: key.into() })
            .unwrap();
        assert_eq!(
            result,
            Output::ConfigValue(None),
            "Setting provider should not affect {:?}",
            key
        );
    }
}

// =============================================================================
// ConfigureSet is visible to ConfigGet (full config snapshot)
// =============================================================================

#[test]
fn configure_set_visible_in_full_config() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "openai".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "openai_api_key".into(),
            value: "sk-test".into(),
        })
        .unwrap();

    let result = executor.execute(Command::ConfigGet).unwrap();
    match result {
        Output::Config(cfg) => {
            assert_eq!(cfg.provider, "openai");
            assert_eq!(cfg.openai_api_key.as_deref(), Some("sk-test"));
        }
        _ => panic!("Expected Config output"),
    }
}

// =============================================================================
// Read-only mode
// =============================================================================

#[test]
fn configure_set_blocked_in_read_only_mode() {
    let db = Database::cache().unwrap();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let result = executor.execute(Command::ConfigureSet {
        key: "provider".into(),
        value: "openai".into(),
    });
    match result {
        Err(Error::AccessDenied { command, .. }) => {
            assert_eq!(command, "ConfigureSet");
        }
        other => panic!("Expected AccessDenied, got {:?}", other),
    }
}

#[test]
fn configure_get_key_allowed_in_read_only_mode() {
    let db = Database::cache().unwrap();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let result = executor.execute(Command::ConfigureGetKey {
        key: "provider".into(),
    });
    // Should succeed (not AccessDenied)
    assert!(
        result.is_ok(),
        "ConfigureGetKey should work in read-only mode"
    );
    assert_eq!(result.unwrap(), Output::ConfigValue(Some("local".into())));
}

// =============================================================================
// Error message quality
// =============================================================================

#[test]
fn configure_set_unknown_key_error_lists_valid_keys() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bad_key".into(),
        value: "anything".into(),
    });
    let msg = result.unwrap_err().to_string();

    // Error should list all valid keys to help the user
    for expected in [
        "provider",
        "default_model",
        "anthropic_api_key",
        "openai_api_key",
        "google_api_key",
        "embed_model",
    ] {
        assert!(
            msg.contains(expected),
            "Error should list valid key {:?}: {}",
            expected,
            msg
        );
    }
}

#[test]
fn configure_get_unknown_key_error_lists_valid_keys() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureGetKey {
        key: "bad_key".into(),
    });
    let msg = result.unwrap_err().to_string();

    for expected in [
        "provider",
        "default_model",
        "anthropic_api_key",
        "openai_api_key",
        "google_api_key",
        "embed_model",
    ] {
        assert!(
            msg.contains(expected),
            "Error should list valid key {:?}: {}",
            expected,
            msg
        );
    }
}

// =============================================================================
// Embed model configuration
// =============================================================================

#[test]
fn configure_get_embed_model_default_is_minilm() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_model".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("miniLM".into())));
}

#[test]
fn configure_set_and_get_embed_model() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "nomic-embed".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_model".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("nomic-embed".into())));
}

#[test]
fn configure_set_embed_model_all_valid_models() {
    let executor = create_test_executor();

    for model in ["miniLM", "nomic-embed", "bge-m3", "gemma-embed"] {
        let result = executor.execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: model.into(),
        });
        assert!(
            result.is_ok(),
            "Embed model {:?} should be accepted, got: {:?}",
            model,
            result
        );
    }
}

#[test]
fn configure_set_embed_model_normalizes_case() {
    let executor = create_test_executor();

    // "MINILM" → stored as canonical "miniLM"
    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "MINILM".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_model".into(),
        })
        .unwrap();
    assert_eq!(
        result,
        Output::ConfigValue(Some("miniLM".into())),
        "Should normalize to canonical casing"
    );
}

#[test]
fn configure_set_embed_model_mixed_case_accepted() {
    let executor = create_test_executor();

    // Mixed case should be accepted and normalized
    for (input, expected) in [
        ("MiniLM", "miniLM"),
        ("Nomic-Embed", "nomic-embed"),
        ("BGE-M3", "bge-m3"),
        ("Gemma-Embed", "gemma-embed"),
    ] {
        executor
            .execute(Command::ConfigureSet {
                key: "embed_model".into(),
                value: input.into(),
            })
            .unwrap();

        let result = executor
            .execute(Command::ConfigureGetKey {
                key: "embed_model".into(),
            })
            .unwrap();
        assert_eq!(
            result,
            Output::ConfigValue(Some(expected.into())),
            "Input {:?} should normalize to {:?}",
            input,
            expected
        );
    }
}

#[test]
fn configure_set_embed_model_invalid_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_model".into(),
        value: "unknown-model".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("Unknown embed_model"),
        "Error should mention unknown embed_model: {}",
        msg
    );
    assert!(
        msg.contains("unknown-model"),
        "Error should mention the attempted value: {}",
        msg
    );
}

#[test]
fn configure_set_embed_model_empty_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_model".into(),
        value: "".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("embed_model cannot be empty"),
        "Error: {}",
        msg
    );
}

#[test]
fn configure_set_embed_model_whitespace_only_returns_error() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_model".into(),
        value: "   ".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("embed_model cannot be empty"),
        "Error: {}",
        msg
    );
}

#[test]
fn configure_set_embed_model_does_not_affect_other_keys() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "bge-m3".into(),
        })
        .unwrap();

    // Provider should remain "local" (default)
    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("local".into())));
}

#[test]
fn configure_set_embed_model_visible_in_full_config() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "nomic-embed".into(),
        })
        .unwrap();

    let result = executor.execute(Command::ConfigGet).unwrap();
    match result {
        Output::Config(cfg) => {
            assert_eq!(cfg.embed_model, "nomic-embed");
        }
        _ => panic!("Expected Config output"),
    }
}

#[test]
fn configure_set_embed_model_overwrite() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "nomic-embed".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_model".into(),
            value: "bge-m3".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_model".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("bge-m3".into())));
}

// =============================================================================
// auto_embed configuration
// =============================================================================

#[test]
fn configure_set_and_get_auto_embed() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "auto_embed".into(),
            value: "true".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "auto_embed".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("true".into())));
}

#[test]
fn configure_get_auto_embed_default_is_false() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "auto_embed".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("false".into())));
}

#[test]
fn configure_set_auto_embed_case_insensitive() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "auto_embed".into(),
            value: "TRUE".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "auto_embed".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("true".into())));
}

#[test]
fn configure_set_auto_embed_invalid_value_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "auto_embed".into(),
        value: "yes".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("true") && msg.contains("false"),
        "Error: {}",
        msg
    );
}

// =============================================================================
// durability configuration
// =============================================================================

#[test]
fn configure_get_durability_default_is_standard() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "durability".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("standard".into())));
}

#[test]
fn configure_set_durability_valid_modes() {
    let executor = create_test_executor();

    for mode in ["standard", "always", "cache"] {
        let result = executor.execute(Command::ConfigureSet {
            key: "durability".into(),
            value: mode.into(),
        });
        assert!(
            result.is_ok(),
            "Durability mode {:?} should be accepted, got: {:?}",
            mode,
            result
        );

        let get_result = executor
            .execute(Command::ConfigureGetKey {
                key: "durability".into(),
            })
            .unwrap();
        assert_eq!(
            get_result,
            Output::ConfigValue(Some(mode.into())),
            "Durability should round-trip for {:?}",
            mode
        );
    }
}

#[test]
fn configure_set_durability_invalid_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "durability".into(),
        value: "turbo".into(),
    });
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("Invalid durability mode"), "Error: {}", msg);
}

#[test]
fn configure_set_durability_case_insensitive() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "durability".into(),
        value: "Always".into(),
    });
    assert!(result.is_ok());

    let get_result = executor
        .execute(Command::ConfigureGetKey {
            key: "durability".into(),
        })
        .unwrap();
    assert_eq!(get_result, Output::ConfigValue(Some("always".into())));
}

// =============================================================================
// BM25 configuration
// =============================================================================

#[test]
fn configure_set_and_get_bm25_k1() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "bm25_k1".into(),
            value: "1.5".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "bm25_k1".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("1.5".into())));
}

#[test]
fn configure_get_bm25_k1_default_is_none() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "bm25_k1".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(None));
}

#[test]
fn configure_set_bm25_k1_zero_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_k1".into(),
        value: "0".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_bm25_k1_negative_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_k1".into(),
        value: "-1.0".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_bm25_k1_infinity_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_k1".into(),
        value: "inf".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_bm25_k1_nan_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_k1".into(),
        value: "NaN".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_bm25_k1_not_a_number_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_k1".into(),
        value: "abc".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_and_get_bm25_b() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "bm25_b".into(),
            value: "0.75".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "bm25_b".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("0.75".into())));
}

#[test]
fn configure_set_bm25_b_boundary_values() {
    let executor = create_test_executor();

    // 0.0 is valid
    assert!(executor
        .execute(Command::ConfigureSet {
            key: "bm25_b".into(),
            value: "0".into(),
        })
        .is_ok());

    // 1.0 is valid
    assert!(executor
        .execute(Command::ConfigureSet {
            key: "bm25_b".into(),
            value: "1.0".into(),
        })
        .is_ok());

    // > 1.0 is rejected
    assert!(executor
        .execute(Command::ConfigureSet {
            key: "bm25_b".into(),
            value: "1.1".into(),
        })
        .is_err());

    // negative is rejected
    assert!(executor
        .execute(Command::ConfigureSet {
            key: "bm25_b".into(),
            value: "-0.1".into(),
        })
        .is_err());
}

#[test]
fn configure_set_bm25_b_infinity_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "bm25_b".into(),
        value: "inf".into(),
    });
    assert!(result.is_err());
}

// =============================================================================
// embed_batch_size configuration
// =============================================================================

#[test]
fn configure_set_and_get_embed_batch_size() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "embed_batch_size".into(),
            value: "1024".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_batch_size".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("1024".into())));
}

#[test]
fn configure_get_embed_batch_size_default_is_512() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "embed_batch_size".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("512".into())));
}

#[test]
fn configure_set_embed_batch_size_zero_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_batch_size".into(),
        value: "0".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_set_embed_batch_size_not_a_number_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_batch_size".into(),
        value: "abc".into(),
    });
    assert!(result.is_err());
}

// =============================================================================
// Model endpoint configuration (model_endpoint, model_name, etc.)
// =============================================================================

#[test]
fn configure_set_and_get_model_endpoint() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "model_endpoint".into(),
            value: "http://localhost:11434/v1".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "model_endpoint".into(),
        })
        .unwrap();
    assert_eq!(
        result,
        Output::ConfigValue(Some("http://localhost:11434/v1".into()))
    );
}

#[test]
fn configure_set_model_name_creates_model_section() {
    let executor = create_test_executor();

    // model_name should be None initially (no model section)
    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "model_name".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(None));

    // Setting model_name should create the model section
    executor
        .execute(Command::ConfigureSet {
            key: "model_name".into(),
            value: "qwen3:1.7b".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "model_name".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("qwen3:1.7b".into())));
}

#[test]
fn configure_set_model_api_key() {
    let executor = create_test_executor();

    // First create the model section
    executor
        .execute(Command::ConfigureSet {
            key: "model_endpoint".into(),
            value: "http://localhost:11434/v1".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "model_api_key".into(),
            value: "sk-test-key".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "model_api_key".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("sk-t***".into())));
}

#[test]
fn configure_set_model_timeout_ms() {
    let executor = create_test_executor();

    // Create model section first
    executor
        .execute(Command::ConfigureSet {
            key: "model_endpoint".into(),
            value: "http://localhost:11434/v1".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "model_timeout_ms".into(),
            value: "10000".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "model_timeout_ms".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("10000".into())));
}

#[test]
fn configure_set_model_timeout_ms_not_a_number_rejected() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "model_timeout_ms".into(),
        value: "abc".into(),
    });
    assert!(result.is_err());
}

#[test]
fn configure_get_model_fields_none_when_no_model() {
    let executor = create_test_executor();

    for key in [
        "model_endpoint",
        "model_name",
        "model_api_key",
        "model_timeout_ms",
    ] {
        let result = executor
            .execute(Command::ConfigureGetKey { key: key.into() })
            .unwrap();
        assert_eq!(
            result,
            Output::ConfigValue(None),
            "Model field {:?} should be None when no model section",
            key
        );
    }
}

#[test]
fn configure_set_model_fields_visible_in_full_config() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "model_endpoint".into(),
            value: "http://localhost:11434/v1".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "model_name".into(),
            value: "qwen3:1.7b".into(),
        })
        .unwrap();

    let result = executor.execute(Command::ConfigGet).unwrap();
    match result {
        Output::Config(cfg) => {
            let model = cfg.model.expect("model section should exist");
            assert_eq!(model.endpoint, "http://localhost:11434/v1");
            assert_eq!(model.model, "qwen3:1.7b");
        }
        _ => panic!("Expected Config output"),
    }
}

// =============================================================================
// CONFIG GET (full config) vs CONFIGURE GET key (masked)
// =============================================================================

#[test]
fn config_get_returns_real_values_while_configure_get_key_masks() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "anthropic_api_key".into(),
            value: "sk-ant-secret-key-12345".into(),
        })
        .unwrap();

    // CONFIG GET returns the full StrataConfig with real SensitiveString values
    let full = executor.execute(Command::ConfigGet).unwrap();
    match full {
        Output::Config(cfg) => {
            // Deref through SensitiveString gives the actual value
            assert_eq!(
                cfg.anthropic_api_key.as_deref(),
                Some("sk-ant-secret-key-12345")
            );
        }
        _ => panic!("Expected Config output"),
    }

    // CONFIGURE GET key returns a masked string
    let masked = executor
        .execute(Command::ConfigureGetKey {
            key: "anthropic_api_key".into(),
        })
        .unwrap();
    assert_eq!(masked, Output::ConfigValue(Some("sk-a***".into())));
}

// =============================================================================
// mask_api_key edge cases (via CONFIGURE GET key)
// =============================================================================

#[test]
fn configure_get_key_masks_short_api_keys() {
    let executor = create_test_executor();

    // 1-char key → masked as "***"
    executor
        .execute(Command::ConfigureSet {
            key: "openai_api_key".into(),
            value: "x".into(),
        })
        .unwrap();
    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "openai_api_key".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("***".into())));

    // Exactly 4 chars → still masked as "***" (not enough to reveal)
    executor
        .execute(Command::ConfigureSet {
            key: "openai_api_key".into(),
            value: "abcd".into(),
        })
        .unwrap();
    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "openai_api_key".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("***".into())));

    // 5 chars → shows first 4 + "***"
    executor
        .execute(Command::ConfigureSet {
            key: "openai_api_key".into(),
            value: "abcde".into(),
        })
        .unwrap();
    let result = executor
        .execute(Command::ConfigureGetKey {
            key: "openai_api_key".into(),
        })
        .unwrap();
    assert_eq!(result, Output::ConfigValue(Some("abcd***".into())));
}

// =============================================================================
// All new keys visible in unknown-key error messages
// =============================================================================

#[test]
fn unknown_key_error_lists_all_new_keys() {
    let executor = create_test_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "nonexistent".into(),
        value: "anything".into(),
    });
    let msg = result.unwrap_err().to_string();

    // The first 10 keys are shown directly; the rest are behind "and N more".
    // Verify that the visible keys include the ones within the display cap,
    // and that the truncation marker accounts for the remaining keys.
    for expected in ["durability", "auto_embed", "bm25_k1", "bm25_b"] {
        assert!(
            msg.contains(expected),
            "Error should list new key {:?}: {}",
            expected,
            msg
        );
    }
    // Keys beyond the 10-candidate display cap are summarised as "and N more"
    assert!(
        msg.contains("and 5 more"),
        "Error should indicate truncated keys: {}",
        msg
    );
}
