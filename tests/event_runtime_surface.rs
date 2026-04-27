//! Cross-surface checks for event and value helpers.

use std::collections::HashMap;

use strata_core::{Timestamp, Value};
use strata_engine::{
    extractable_text as engine_extractable_text, ChainVerification, Event as EngineEvent,
};

#[test]
fn event_roundtrips_between_engine_and_core_surfaces() {
    let engine = EngineEvent {
        sequence: 9,
        event_type: "user.created".to_string(),
        payload: Value::array(vec![Value::Int(1), Value::String("a".to_string())]),
        timestamp: Timestamp::from(123_456),
        prev_hash: [7u8; 32],
        hash: [8u8; 32],
    };

    let legacy: strata_core::Event = engine.clone().into();
    let roundtrip: EngineEvent = legacy.into();
    assert_eq!(roundtrip, engine);
}

#[test]
fn chain_verification_roundtrips_between_engine_and_core_surfaces() {
    let engine = ChainVerification::invalid(15, 11, "broken chain");

    let legacy: strata_core::ChainVerification = engine.clone().into();
    assert!(!legacy.is_valid);
    assert_eq!(legacy.length, 15);
    assert_eq!(legacy.first_invalid, Some(11));
    assert_eq!(legacy.error.as_deref(), Some("broken chain"));

    let roundtrip: ChainVerification = legacy.into();
    assert_eq!(roundtrip, engine);
}

#[test]
fn extractable_text_matches_core_surface() {
    let mut object = HashMap::new();
    object.insert("name".to_string(), Value::String("strata".to_string()));
    object.insert("count".to_string(), Value::Int(2));

    let values = [
        Value::String("hello".to_string()),
        Value::Null,
        Value::Bool(true),
        Value::Bytes(vec![1, 2, 3]),
        Value::Int(42),
        Value::array(vec![Value::Int(1), Value::String("a".to_string())]),
        Value::object(object),
    ];

    for value in values {
        assert_eq!(
            engine_extractable_text(&value),
            strata_core::extractable_text(&value)
        );
    }
}
