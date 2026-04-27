//! Regression tests for the engine root surface.

use strata_core::Value;
use strata_engine::database::OpenSpec;
use strata_engine::{
    delete_at_path, extractable_text, get_at_path, set_at_path, BranchEventOffsets,
    CherryPickRecord, DagEventId, EventLog, FilterCondition, FilterOp, JsonPath, JsonScalar,
    JsonValue, Limits, MetadataFilter, RevertRecord, SearchSubsystem,
};
use strata_engine::{ChainVerification, Database};
use tempfile::TempDir;

#[test]
fn branch_domain_types_preserve_behavior_through_engine_root_surface() {
    let mut offsets = BranchEventOffsets::new();
    offsets.push(7);
    offsets.push(9);
    offsets.push(12);

    assert_eq!(offsets.as_slice(), &[7, 9, 12]);
    assert_eq!(offsets.len(), 3);
    assert!(!offsets.is_empty());

    let cherry = CherryPickRecord {
        event_id: DagEventId::new_cherry_pick(),
        source: "feature-a".into(),
        target: "main".into(),
        cherry_pick_version: Some(42),
        keys_applied: 3,
        keys_deleted: 1,
        timestamp: 99,
    };
    assert!(cherry.event_id.is_cherry_pick());

    let cherry_roundtrip: CherryPickRecord =
        serde_json::from_str(&serde_json::to_string(&cherry).unwrap()).unwrap();
    assert_eq!(cherry_roundtrip, cherry);

    let revert = RevertRecord {
        event_id: DagEventId::new_revert(),
        branch: "main".into(),
        from_version: 10.into(),
        to_version: 12.into(),
        revert_version: Some(13.into()),
        keys_reverted: 4,
        timestamp: 101,
        message: Some("undo".into()),
        creator: Some("tester".into()),
    };
    assert!(revert.event_id.is_revert());

    let revert_roundtrip: RevertRecord =
        serde_json::from_str(&serde_json::to_string(&revert).unwrap()).unwrap();
    assert_eq!(revert_roundtrip, revert);
}

#[test]
fn downstream_consumer_can_use_engine_root_paths() {
    let path = JsonPath::root().key("user").key("name");
    assert_eq!(path.to_path_string(), "user.name");

    let mut doc = JsonValue::object();
    set_at_path(&mut doc, &path, JsonValue::from("hello")).unwrap();
    assert_eq!(
        get_at_path(&doc, &path).and_then(|v| v.as_str()),
        Some("hello")
    );

    let deleted = delete_at_path(&mut doc, &path).unwrap();
    assert_eq!(deleted.unwrap().as_str(), Some("hello"));
    assert!(get_at_path(&doc, &path).is_none());

    let filter = MetadataFilter::new()
        .eq("tenant", "acme")
        .gt("score", 0.9)
        .in_values(
            "tag",
            vec![
                JsonScalar::String("a".into()),
                JsonScalar::String("b".into()),
            ],
        );
    assert_eq!(
        filter.equals.get("tenant"),
        Some(&JsonScalar::String("acme".into()))
    );
    assert_eq!(filter.conditions.len(), 3);
    assert!(matches!(filter.conditions[0].op, FilterOp::Gt));
    assert!(matches!(
        filter.conditions[1],
        FilterCondition {
            ref field,
            op: FilterOp::In,
            ..
        } if field == "tag"
    ));
    assert!(matches!(
        filter.conditions[2],
        FilterCondition {
            ref field,
            op: FilterOp::In,
            ..
        } if field == "tag"
    ));

    let text = extractable_text(&Value::array(vec![
        Value::Int(1),
        Value::String("a".into()),
    ]));
    assert_eq!(text, Some("[1,\"a\"]".to_string()));

    let limits = Limits::default();
    limits.validate_key_length("ok").unwrap();
    limits.validate_value_full(&Value::Float(0.5)).unwrap();
}

#[test]
fn event_log_verification_is_available_through_engine_surface() {
    let temp_dir = TempDir::new().unwrap();
    let db =
        Database::open_runtime(OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    let branch_id = strata_core::BranchId::new();
    let log = EventLog::new(db);

    assert_eq!(
        log.verify_chain(&branch_id, "default").unwrap(),
        ChainVerification::valid(0)
    );

    log.append(
        &branch_id,
        "default",
        "task",
        Value::object(std::collections::HashMap::from([(
            "value".to_string(),
            Value::Int(1),
        )])),
    )
    .unwrap();
    log.append(
        &branch_id,
        "default",
        "task",
        Value::object(std::collections::HashMap::from([(
            "value".to_string(),
            Value::Int(2),
        )])),
    )
    .unwrap();

    assert_eq!(
        log.verify_chain(&branch_id, "default").unwrap(),
        ChainVerification::valid(2)
    );
}
