//! Cross-surface checks for event and value helpers.

use std::collections::HashMap;

use strata_core::{Timestamp, Value};
use strata_engine::{
    extractable_text as engine_extractable_text, ChainVerification, Event as EngineEvent,
};

#[test]
fn event_root_surface_roundtrips_through_stable_serde_shape() {
    let payload = Value::array(vec![Value::Int(1), Value::String("a".to_string())]);
    let engine = EngineEvent {
        sequence: 9,
        event_type: "user.created".to_string(),
        payload: payload.clone(),
        timestamp: Timestamp::from(123_456),
        prev_hash: [7u8; 32],
        hash: [8u8; 32],
    };

    let prev_hash = vec![7u8; 32];
    let hash = vec![8u8; 32];
    let expected = serde_json::json!({
        "sequence": 9,
        "event_type": "user.created",
        "payload": {
            "Array": [
                { "Int": 1 },
                { "String": "a" }
            ]
        },
        "timestamp": 123_456,
        "prev_hash": prev_hash,
        "hash": hash,
    });

    assert_eq!(serde_json::to_value(&engine).unwrap(), expected);

    let roundtrip: EngineEvent = serde_json::from_value(expected).unwrap();
    assert_eq!(roundtrip, engine);
}

#[test]
fn chain_verification_root_surface_helpers_work() {
    let engine = ChainVerification::invalid(15, 11, "broken chain");

    assert!(!engine.is_valid);
    assert_eq!(engine.length, 15);
    assert_eq!(engine.first_invalid, Some(11));
    assert_eq!(engine.error.as_deref(), Some("broken chain"));
}

#[test]
fn extractable_text_matches_documented_outputs() {
    let mut object = HashMap::new();
    object.insert("name".to_string(), Value::String("strata".to_string()));
    object.insert("count".to_string(), Value::Int(2));

    assert_eq!(
        engine_extractable_text(&Value::String("hello".to_string())),
        Some("hello".to_string())
    );
    assert_eq!(engine_extractable_text(&Value::Null), None);
    assert_eq!(engine_extractable_text(&Value::Bool(true)), None);
    assert_eq!(engine_extractable_text(&Value::Bytes(vec![1, 2, 3])), None);
    assert_eq!(
        engine_extractable_text(&Value::Int(42)),
        Some("42".to_string())
    );
    assert_eq!(
        engine_extractable_text(&Value::array(vec![
            Value::Int(1),
            Value::String("a".to_string())
        ])),
        Some("[1,\"a\"]".to_string())
    );

    let object_text = engine_extractable_text(&Value::object(object)).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&object_text).unwrap();
    assert_eq!(parsed, serde_json::json!({"count": 2, "name": "strata"}));
}
