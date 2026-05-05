//! Characterization tests for the security/open surface before EG2 absorption.

use strata_engine::{AccessMode, OpenOptions, SensitiveString, StrataConfig};
use strata_executor::{AccessMode as ExecutorAccessMode, OpenOptions as ExecutorOpenOptions};

#[test]
fn access_mode_defaults_to_read_write_and_keeps_serde_shape() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<AccessMode>();

    assert_eq!(AccessMode::default(), AccessMode::ReadWrite);
    assert_eq!(format!("{:?}", AccessMode::ReadWrite), "ReadWrite");
    assert_eq!(format!("{:?}", AccessMode::ReadOnly), "ReadOnly");

    let copied = AccessMode::ReadOnly;
    assert_eq!(copied, AccessMode::ReadOnly);

    assert_eq!(
        serde_json::to_value(AccessMode::ReadWrite).unwrap(),
        serde_json::json!("ReadWrite")
    );
    assert_eq!(
        serde_json::to_value(AccessMode::ReadOnly).unwrap(),
        serde_json::json!("ReadOnly")
    );

    assert_eq!(
        serde_json::from_value::<AccessMode>(serde_json::json!("ReadWrite")).unwrap(),
        AccessMode::ReadWrite
    );
    assert_eq!(
        serde_json::from_value::<AccessMode>(serde_json::json!("ReadOnly")).unwrap(),
        AccessMode::ReadOnly
    );
}

#[test]
fn open_options_builders_keep_access_mode_and_follower_independent() {
    let default_options = OpenOptions::default();
    assert_eq!(default_options.access_mode, AccessMode::ReadWrite);
    assert!(!default_options.follower);
    assert_eq!(
        format!("{:?}", default_options),
        "OpenOptions { access_mode: ReadWrite, follower: false }"
    );

    let new_options = OpenOptions::new();
    assert_eq!(new_options.access_mode, AccessMode::ReadWrite);
    assert!(!new_options.follower);

    let literal_options = OpenOptions {
        access_mode: AccessMode::ReadOnly,
        follower: true,
    };
    assert_eq!(literal_options.access_mode, AccessMode::ReadOnly);
    assert!(literal_options.follower);

    let read_only = OpenOptions::new().access_mode(AccessMode::ReadOnly);
    assert_eq!(read_only.access_mode, AccessMode::ReadOnly);
    assert!(!read_only.follower);

    let read_write_again = read_only.access_mode(AccessMode::ReadWrite);
    assert_eq!(read_write_again.access_mode, AccessMode::ReadWrite);
    assert!(!read_write_again.follower);

    let follower = OpenOptions::new().follower(true);
    assert_eq!(follower.access_mode, AccessMode::ReadWrite);
    assert!(follower.follower);

    let non_follower_again = follower.follower(false);
    assert_eq!(non_follower_again.access_mode, AccessMode::ReadWrite);
    assert!(!non_follower_again.follower);

    let read_only_then_follower = OpenOptions::new()
        .access_mode(AccessMode::ReadOnly)
        .follower(true);
    assert_eq!(read_only_then_follower.access_mode, AccessMode::ReadOnly);
    assert!(read_only_then_follower.follower);

    let follower_then_read_only = OpenOptions::new()
        .follower(true)
        .access_mode(AccessMode::ReadOnly);
    assert_eq!(follower_then_read_only.access_mode, AccessMode::ReadOnly);
    assert!(follower_then_read_only.follower);

    let cloned = follower_then_read_only.clone();
    assert_eq!(cloned.access_mode, AccessMode::ReadOnly);
    assert!(cloned.follower);
}

#[test]
fn executor_open_surface_still_reexports_open_options() {
    let options: OpenOptions = ExecutorOpenOptions::new()
        .access_mode(ExecutorAccessMode::ReadOnly)
        .follower(true);
    let read_only: AccessMode = ExecutorAccessMode::ReadOnly;

    assert_eq!(options.access_mode, read_only);
    assert!(options.follower);
}

#[test]
fn sensitive_config_strings_are_redacted_but_serde_transparent() {
    let mut config: StrataConfig = serde_json::from_value(serde_json::json!({
        "anthropic_api_key": "sk-ant-secret",
        "openai_api_key": "sk-openai-secret",
        "google_api_key": "AIza-secret",
        "model": {
            "endpoint": "http://localhost:11434/v1",
            "model": "qwen3:1.7b",
            "api_key": "sk-model-secret"
        }
    }))
    .unwrap();

    let anthropic = config.anthropic_api_key.as_ref().unwrap();
    assert_eq!(format!("{:?}", anthropic), "[REDACTED]");
    assert_eq!(format!("{}", anthropic), "[REDACTED]");
    assert_eq!(anthropic.as_str(), "sk-ant-secret");

    let deref_borrow: &str = anthropic;
    assert_eq!(deref_borrow, "sk-ant-secret");
    assert_send_sync(anthropic);

    let cloned = anthropic.clone();
    assert_eq!(Some(&cloned), config.anthropic_api_key.as_ref());
    assert_eq!(cloned.as_str(), "sk-ant-secret");
    assert_ne!(cloned.as_str(), "sk-ant-different-secret");

    config.anthropic_api_key = None;
    assert_eq!(cloned.as_str(), "sk-ant-secret");

    let serialized = serde_json::to_value(&config).unwrap();
    assert!(serialized.get("anthropic_api_key").is_none());
    assert_eq!(serialized["openai_api_key"], "sk-openai-secret");
    assert_eq!(serialized["google_api_key"], "AIza-secret");
    assert_eq!(serialized["model"]["api_key"], "sk-model-secret");

    let debug_config = format!("{:?}", config);
    assert!(debug_config.contains("[REDACTED]"));
    assert!(!debug_config.contains("sk-ant-secret"));
    assert!(!debug_config.contains("sk-openai-secret"));
    assert!(!debug_config.contains("AIza-secret"));
    assert!(!debug_config.contains("sk-model-secret"));

    assert_eq!(
        config.google_api_key.take().unwrap().into_inner(),
        "AIza-secret"
    );
}

#[test]
fn sensitive_config_strings_support_from_string_and_str_inputs() {
    let mut config = StrataConfig::default();
    config.anthropic_api_key = Some("sk-ant-from-str".into());
    config.openai_api_key = Some(String::from("sk-openai-from-string").into());
    let direct_secret = SensitiveString::from("sk-direct");

    assert_eq!(
        config.anthropic_api_key.as_ref().unwrap().as_str(),
        "sk-ant-from-str"
    );
    assert_eq!(
        config.openai_api_key.as_ref().unwrap().as_str(),
        "sk-openai-from-string"
    );
    assert_eq!(direct_secret.as_str(), "sk-direct");
    assert_send_sync(&direct_secret);
}

fn assert_send_sync<T: Send + Sync>(_: &T) {}
