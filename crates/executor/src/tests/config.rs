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

    for (key, value) in [
        ("anthropic_api_key", "sk-ant-test-123"),
        ("openai_api_key", "sk-test-456"),
        ("google_api_key", "AIza-test-789"),
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
        assert_eq!(result, Output::ConfigValue(Some(value.into())));
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
            assert_eq!(cfg.openai_api_key, Some("sk-test".into()));
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
        Err(Error::AccessDenied { command }) => {
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
