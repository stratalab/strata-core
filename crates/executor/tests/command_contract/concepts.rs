use super::support::*;

#[test]
fn commit_receipt_serializes_all_required_fields() {
    let receipt = CommitReceipt::new(7, 70, CommitDurability::Standard, 2, 1);
    let encoded = serde_json::to_value(receipt).expect("commit receipt serializes");

    assert_eq!(
        encoded,
        json!({
            "version": 7,
            "timestamp": 70,
            "committed_at": null,
            "durability": "standard",
            "put_count": 2,
            "delete_count": 1,
        })
    );
    assert_eq!(
        serde_json::from_value::<CommitReceipt>(encoded).expect("commit receipt deserializes"),
        receipt
    );

    // Every durability state has a stable wire string (#2756: the enum
    // replaced a boolean that attested unsynced commits as durable).
    for (durability, wire) in [
        (CommitDurability::NotDurable, "not_durable"),
        (CommitDurability::Standard, "standard"),
        (CommitDurability::Always, "always"),
        (CommitDurability::Uncertain, "uncertain"),
    ] {
        let receipt = CommitReceipt::new(1, 10, durability, 1, 0);
        let encoded = serde_json::to_value(receipt).expect("serializes");
        assert_eq!(encoded["durability"], json!(wire));
        assert_eq!(
            serde_json::from_value::<CommitReceipt>(encoded).expect("round-trips"),
            receipt
        );
    }

    for missing_field in [
        "version",
        "timestamp",
        "durability",
        "put_count",
        "delete_count",
    ] {
        let mut incomplete = serde_json::to_value(receipt)
            .expect("commit receipt serializes")
            .as_object()
            .expect("commit receipt is an object")
            .clone();
        incomplete.remove(missing_field);
        assert!(
            serde_json::from_value::<CommitReceipt>(Value::Object(incomplete)).is_err(),
            "commit receipt without `{missing_field}` must be rejected"
        );
    }
}

#[test]
fn mutation_effect_contract_covers_all_standard_outcomes() {
    let cases = [
        (
            MutationEffect::created(),
            (true, MutationEffectKind::Created, false, 1),
        ),
        (
            MutationEffect::updated(),
            (true, MutationEffectKind::Updated, true, 1),
        ),
        (
            MutationEffect::deleted(),
            (true, MutationEffectKind::Deleted, true, 1),
        ),
        (
            unchanged_effect(),
            (false, MutationEffectKind::Unchanged, true, 0),
        ),
        (
            MutationEffect::not_found(),
            (false, MutationEffectKind::NotFound, false, 0),
        ),
        (
            deleted_count_effect(3),
            (true, MutationEffectKind::Deleted, true, 3),
        ),
    ];

    for (effect, (applied, kind, matched, affected_count)) in cases {
        assert_eq!(effect.applied(), applied);
        assert_eq!(effect.kind(), kind);
        assert_eq!(effect.matched(), matched);
        assert_eq!(effect.affected_count(), affected_count);

        let encoded = serde_json::to_value(effect).expect("mutation effect serializes");
        assert_eq!(encoded["applied"], applied);
        assert_eq!(encoded["matched"], matched);
        assert_eq!(encoded["affected_count"], affected_count);
        assert_eq!(
            serde_json::from_value::<MutationEffect>(encoded)
                .expect("mutation effect deserializes"),
            effect
        );
    }
}

#[test]
#[should_panic(expected = "non-terminal pages must carry a continuation cursor")]
fn page_info_constructor_rejects_continued_page_without_cursor() {
    let _ = PageInfo::<String>::new(true, None);
}

#[test]
#[should_panic(expected = "terminal pages must not carry a continuation cursor")]
fn page_info_constructor_rejects_terminal_page_with_cursor() {
    let _ = PageInfo::new(false, Some("cursor".to_owned()));
}

#[test]
fn page_info_deserialization_rejects_invalid_cursor_combinations() {
    assert!(serde_json::from_value::<PageInfo<String>>(json!({
        "has_more": true,
        "cursor": null,
    }))
    .is_err());
    assert!(serde_json::from_value::<PageInfo<String>>(json!({
        "has_more": false,
        "cursor": "cursor",
    }))
    .is_err());

    let continued = PageInfo::new(true, Some("opaque-cursor".to_owned()));
    let terminal = PageInfo::<String>::terminal();
    assert_eq!(
        continued.cursor().map(String::as_str),
        Some("opaque-cursor")
    );
    assert!(continued.has_more());
    assert_eq!(terminal.cursor(), None);
    assert!(!terminal.has_more());
}

#[test]
fn batch_result_reports_mode_status_and_item_positions() {
    let commit = commit_receipt(7, 70, 1, 0);
    let empty = json_batch(Vec::new());
    assert_eq!(empty.mode(), BatchMode::Itemwise);
    assert_eq!(empty.status(), BatchStatus::Ok);
    assert!(!empty.applied());
    assert_eq!(empty.commit(), None);
    assert!(empty.is_empty());

    let ok = json_batch(vec![BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        JsonBatchItemResult::new(Some(1)),
    )]);
    assert_eq!(ok.mode(), BatchMode::Itemwise);
    assert_eq!(ok.status(), BatchStatus::Ok);
    assert!(ok.applied());
    assert_eq!(ok.commit(), Some(&commit));
    assert_eq!(ok[0].index(), 0);

    let partial = json_batch(vec![
        BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(commit),
            JsonBatchItemResult::new(Some(1)),
        ),
        BatchItem::ok(
            1,
            false,
            Some(MutationEffect::not_found()),
            None,
            JsonBatchItemResult::new(None),
        ),
    ]);
    assert_eq!(partial.status(), BatchStatus::Partial);
    assert!(partial.applied());
    assert_eq!(partial[1].index(), 1);
    assert_eq!(partial[1].status(), BatchItemStatus::Ok);
    assert!(!partial[1].applied());
    assert_eq!(partial[1].effect(), Some(&MutationEffect::not_found()));
    assert_eq!(partial[1].error(), None);

    let failed = json_batch(vec![BatchItem::failed(
        0,
        Some(JsonBatchItemResult::new(None)),
        item_error("invalid document id"),
    )]);
    assert_eq!(failed.status(), BatchStatus::Error);
    assert!(!failed.applied());
    assert!(failed[0].error_status().is_some());
    assert_eq!(failed[0].status(), BatchItemStatus::Error);
    assert_eq!(failed[0].effect(), None);
    assert_eq!(failed[0].commit(), None);

    let graph = graph_batch(vec![BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        GraphBatchItemResult::new(0, "upsert_node", Some(true), None),
    )]);
    assert_eq!(graph.mode(), BatchMode::Atomic);
    assert_eq!(graph.status(), BatchStatus::Ok);
}

#[test]
fn batch_result_shared_commit_requires_one_commit_identity() {
    let first_commit = commit_receipt(7, 70, 1, 0);
    let second_commit = commit_receipt(8, 80, 1, 0);

    let shared = json_batch(vec![
        BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(first_commit),
            JsonBatchItemResult::new(Some(1)),
        ),
        BatchItem::ok(
            1,
            true,
            Some(MutationEffect::updated()),
            Some(first_commit),
            JsonBatchItemResult::new(Some(2)),
        ),
    ]);
    assert_eq!(shared.commit(), Some(&first_commit));

    let split = json_batch(vec![
        BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(first_commit),
            JsonBatchItemResult::new(Some(1)),
        ),
        BatchItem::ok(
            1,
            true,
            Some(MutationEffect::updated()),
            Some(second_commit),
            JsonBatchItemResult::new(Some(2)),
        ),
    ]);
    assert_eq!(split.commit(), None);
    assert!(split.applied());
    assert_eq!(split.status(), BatchStatus::Ok);
}

#[test]
fn batch_get_items_report_found_without_losing_metadata() {
    let kv_found = BatchGetItemResult::new(
        bytes("present-empty"),
        Some(Bytes::new(Vec::new())),
        Some(7),
        Some(70),
    );
    let kv_missing = BatchGetItemResult::new(bytes("missing"), None, None, None);
    assert!(kv_found.found());
    assert_eq!(
        kv_found
            .value()
            .expect("empty value is still present")
            .as_slice(),
        b""
    );
    assert_eq!(kv_found.version(), Some(7));
    assert_eq!(kv_found.timestamp(), Some(70));
    assert!(!kv_missing.found());
    assert!(kv_missing.value().is_none());

    let vector_found = VectorBatchGetItemResult::new(Some(VectorVersionedData::new(
        "doc-a".to_owned(),
        VectorData::new(vec![1.0, 0.0], Some(json!({"kind": "doc"}))),
        8,
        80,
        3,
    )));
    let vector_missing = VectorBatchGetItemResult::new(None);
    assert!(vector_found.found());
    assert_eq!(
        vector_found.value().expect("vector is present").version(),
        8
    );
    assert!(!vector_missing.found());
    assert!(vector_missing.value().is_none());

    let json_null = JsonBatchGetItemResult::new(Some(Value::Null), Some(9), Some(90), Some(4));
    let json_missing = JsonBatchGetItemResult::new(None, None, None, None);
    assert!(json_null.found());
    assert_eq!(json_null.value(), Some(&Value::Null));
    assert!(!json_missing.found());
    assert_eq!(json_missing.value(), None);

    assert_eq!(
        serde_json::to_value(&kv_missing).expect("KV missing item serializes")["value"],
        Value::Null
    );
    assert_eq!(
        serde_json::to_value(&vector_missing).expect("vector missing item serializes")["value"],
        Value::Null
    );
    assert_eq!(
        serde_json::to_value(&json_missing).expect("JSON missing item serializes")["value"],
        Value::Null
    );

    for (encoded, expected_found) in [
        (
            serde_json::to_value(&kv_found).expect("KV found item serializes"),
            true,
        ),
        (
            serde_json::to_value(&kv_missing).expect("KV missing item serializes"),
            false,
        ),
        (
            serde_json::to_value(&vector_found).expect("vector found item serializes"),
            true,
        ),
        (
            serde_json::to_value(&vector_missing).expect("vector missing item serializes"),
            false,
        ),
        (
            serde_json::to_value(&json_null).expect("JSON null item serializes"),
            true,
        ),
        (
            serde_json::to_value(&json_missing).expect("JSON missing item serializes"),
            false,
        ),
    ] {
        assert_eq!(encoded["found"], expected_found);
        assert_eq!(
            encoded.get("error"),
            None,
            "batch read item payload no longer restates the wrapper's error"
        );
    }
}

#[test]
fn page_outputs_reject_non_terminal_pages_without_cursor() {
    let invalid = json!({
        "type": "json_list_result",
        "data": {
            "items": ["doc-a"],
            "has_more": true,
            "cursor": null,
        },
    });
    assert!(serde_json::from_value::<Output>(invalid).is_err());
}

#[test]
fn kv_batch_get_wrapper_marks_missing_items_as_miss() {
    let batch = kv_batch_get(vec![
        BatchItem::ok(
            0,
            false,
            None,
            None,
            BatchGetItemResult::new(bytes("a"), Some(bytes("one")), Some(1), Some(10)),
        ),
        BatchItem::miss(1, BatchGetItemResult::not_found(bytes("missing"))),
        BatchItem::ok(
            2,
            false,
            None,
            None,
            BatchGetItemResult::new(bytes("b"), Some(bytes("two")), Some(2), Some(20)),
        ),
    ]);

    assert_eq!(batch.status(), BatchStatus::Partial);
    assert!(!batch.applied());
    assert_eq!(batch[0].status(), BatchItemStatus::Ok);
    assert_eq!(batch[1].status(), BatchItemStatus::Miss);
    assert_eq!(batch[2].status(), BatchItemStatus::Ok);
    assert_eq!(batch[1].error(), None);
    assert!(!batch[1]
        .result()
        .expect("miss keeps primitive payload")
        .found());

    let encoded = serde_json::to_value(&batch).expect("batch serializes");
    assert_eq!(encoded["status"], "partial");
    assert_eq!(
        encoded["items"]
            .as_array()
            .expect("items is an array")
            .iter()
            .map(|item| item["status"].as_str().expect("status is a string"))
            .collect::<Vec<_>>(),
        vec!["ok", "miss", "ok"]
    );
    assert_eq!(encoded["items"][1]["result"]["found"], false);
    assert_eq!(encoded["items"][1]["error"], Value::Null);
}

#[test]
fn json_read_outputs_distinguish_missing_from_stored_null() {
    let versioned_null = JsonVersionedValue::new(Value::Null, 1, 10, 2);
    let missing_versioned = Output::JsonVersionedValue(MaybeJsonVersionedValue::missing());
    let stored_versioned_null =
        Output::JsonVersionedValue(MaybeJsonVersionedValue::found(versioned_null));
    let missing_versioned_json =
        serde_json::to_value(&missing_versioned).expect("missing versioned output serializes");
    let stored_versioned_null_json = serde_json::to_value(&stored_versioned_null)
        .expect("stored versioned null output serializes");

    assert_eq!(missing_versioned_json["data"]["found"], false);
    assert!(missing_versioned_json["data"]
        .as_object()
        .expect("versioned data is an object")
        .contains_key("value"));
    assert_eq!(missing_versioned_json["data"]["value"], Value::Null);
    assert_eq!(stored_versioned_null_json["data"]["found"], true);
    assert_eq!(
        stored_versioned_null_json["data"]["value"]["value"],
        Value::Null
    );
    assert_ne!(missing_versioned_json, stored_versioned_null_json);
    assert_eq!(
        serde_json::from_value::<Output>(missing_versioned_json)
            .expect("missing versioned output deserializes"),
        missing_versioned
    );
    assert_eq!(
        serde_json::from_value::<Output>(stored_versioned_null_json)
            .expect("stored versioned null output deserializes"),
        stored_versioned_null
    );

    let inconsistent_versioned = serde_json::from_value::<MaybeJsonVersionedValue>(json!({
        "found": false,
        "value": {
            "value": null,
            "version": 1,
            "timestamp": 10,
            "document_version": 2,
        },
    }))
    .expect("inconsistent versioned result deserializes");
    assert!(inconsistent_versioned.value().is_none());
    assert_eq!(inconsistent_versioned.into_option(), None);

    let batch_missing = JsonBatchGetItemResult::new(None, None, None, None);
    let batch_stored_null =
        JsonBatchGetItemResult::new(Some(Value::Null), Some(1), Some(10), Some(2));
    let batch_missing_json =
        serde_json::to_value(&batch_missing).expect("batch missing serializes");
    let batch_stored_null_json =
        serde_json::to_value(&batch_stored_null).expect("batch stored null serializes");

    assert_eq!(batch_missing_json["found"], false);
    assert_eq!(batch_missing_json["value"], Value::Null);
    assert_eq!(batch_stored_null_json["found"], true);
    assert_eq!(batch_stored_null_json["value"], Value::Null);
    assert_ne!(batch_missing_json, batch_stored_null_json);
    assert_eq!(
        serde_json::from_value::<JsonBatchGetItemResult>(batch_missing_json)
            .expect("batch missing deserializes"),
        batch_missing
    );
    assert_eq!(
        serde_json::from_value::<JsonBatchGetItemResult>(batch_stored_null_json)
            .expect("batch stored null deserializes"),
        batch_stored_null
    );
}

#[test]
fn write_outputs_use_commit_receipt_and_mutation_effect() {
    let write = Output::WriteResult {
        key: bytes("user"),
        effect: MutationEffect::created(),
        commit: commit_receipt(7, 70, 1, 0),
    };
    let write_json = serde_json::to_value(&write).expect("write output serializes");
    assert_eq!(
        write_json,
        json!({
            "type": "write_result",
            "data": {
                "key": "dXNlcg==",
                "effect": {
                    "applied": true,
                    "kind": "created",
                    "matched": false,
                    "affected_count": 1,
                },
                "commit": {
                    "version": 7,
                    "timestamp": 70,
                    "committed_at": null,
                    "durability": "standard",
                    "put_count": 1,
                    "delete_count": 0,
                },
            },
        })
    );

    let missing_delete = Output::DeleteResult {
        key: bytes("missing"),
        effect: MutationEffect::not_found(),
        commit: None,
    };
    let missing_delete_json =
        serde_json::to_value(&missing_delete).expect("delete output serializes");
    assert_eq!(
        missing_delete_json,
        json!({
            "type": "delete_result",
            "data": {
                "key": "bWlzc2luZw==",
                "effect": {
                    "applied": false,
                    "kind": "not_found",
                    "matched": false,
                    "affected_count": 0,
                },
            },
        })
    );
    assert_eq!(
        serde_json::from_value::<Output>(write_json).expect("write output deserializes"),
        write
    );
    assert_eq!(
        serde_json::from_value::<Output>(missing_delete_json).expect("delete output deserializes"),
        missing_delete
    );
}

#[test]
fn batch_item_failures_use_structured_error_status() {
    // The shared `BatchItem` wrapper owns the structured error now that the
    // inner item DTOs no longer restate it.
    let kv = BatchItem::failed(
        0,
        Some(BatchItemResult::new(Bytes::new(Vec::new()))),
        item_error("invalid key"),
    );
    let json = BatchItem::failed(
        0,
        Some(JsonBatchItemResult::new(None)),
        item_error("invalid document id"),
    );
    let vector = BatchItem::failed(
        0,
        Some(VectorBatchItemResult::new(None)),
        item_error("invalid vector"),
    );
    let event = BatchItem::failed(
        0,
        Some(EventBatchAppendItemResult::new(None, None)),
        item_error("invalid event"),
    );
    let graph = BatchItem::failed(
        0,
        Some(GraphBatchItemResult::new(2, "upsert_edge", None, None)),
        item_error("invalid graph edge"),
    );

    for encoded in [
        serde_json::to_value(&kv).expect("KV item serializes"),
        serde_json::to_value(&json).expect("JSON item serializes"),
        serde_json::to_value(&vector).expect("vector item serializes"),
        serde_json::to_value(&event).expect("event item serializes"),
        serde_json::to_value(&graph).expect("graph item serializes"),
    ] {
        let error = encoded
            .get("error")
            .and_then(Value::as_object)
            .expect("item error is a structured object");
        assert_eq!(error.get("class"), Some(&json!("invalid_argument")));
        assert_eq!(
            error.get("code"),
            Some(&json!("invalid_argument.executor.batch_item"))
        );
        assert_eq!(error.get("retry_policy"), Some(&json!("never")));
        assert_eq!(error.get("retryable"), Some(&json!(false)));
        assert_eq!(error.get("commit_outcome"), Some(&json!("not_started")));
        assert!(error.get("message").and_then(Value::as_str).is_some());
        assert!(error.get("suggested_fix").and_then(Value::as_str).is_some());
        assert!(error.get("docs_url").and_then(Value::as_str).is_some());
        assert!(error.get("reference_id").and_then(Value::as_str).is_some());
    }

    assert_eq!(kv.error(), Some("invalid key"));
    assert_eq!(json.error(), Some("invalid document id"));
    assert_eq!(vector.error(), Some("invalid vector"));
    assert_eq!(event.error(), Some("invalid event"));
    assert_eq!(graph.error(), Some("invalid graph edge"));
    assert!(kv.error_status().is_some());
    assert!(json.error_status().is_some());
    assert!(vector.error_status().is_some());
    assert!(event.error_status().is_some());
    assert!(graph.error_status().is_some());
}

#[test]
fn batch_item_successes_serialize_error_null() {
    // Every successful `BatchItem` wrapper serializes an explicit `error: null`
    // even though the slimmed inner payloads no longer carry an error field.
    let commit = commit_receipt(7, 70, 1, 0);
    let kv = BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        BatchItemResult::new(Bytes::new(b"key".to_vec())),
    );
    let json = BatchItem::ok(
        0,
        true,
        Some(MutationEffect::updated()),
        Some(commit),
        JsonBatchItemResult::new(Some(3)),
    );
    let vector = BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        VectorBatchItemResult::new(Some(4)),
    );
    let vector_get = BatchItem::miss(0, VectorBatchGetItemResult::not_found());
    let event = BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        EventBatchAppendItemResult::new(Some(1), Some("user.created".to_owned())),
    );
    let graph = BatchItem::ok(
        0,
        true,
        Some(MutationEffect::created()),
        Some(commit),
        GraphBatchItemResult::new(0, "upsert_node", Some(true), None),
    );
    let kv_get = BatchItem::miss(
        0,
        BatchGetItemResult::not_found(Bytes::new(b"key".to_vec())),
    );
    let json_get = BatchItem::miss(0, JsonBatchGetItemResult::not_found());

    for encoded in [
        serde_json::to_value(&kv).expect("KV item serializes"),
        serde_json::to_value(&kv_get).expect("KV get item serializes"),
        serde_json::to_value(&json).expect("JSON item serializes"),
        serde_json::to_value(&json_get).expect("JSON get item serializes"),
        serde_json::to_value(&vector).expect("vector item serializes"),
        serde_json::to_value(&vector_get).expect("vector get item serializes"),
        serde_json::to_value(&event).expect("event item serializes"),
        serde_json::to_value(&graph).expect("graph item serializes"),
    ] {
        assert_eq!(encoded.get("error"), Some(&Value::Null));
    }
}

#[test]
fn admin_and_space_write_classification_is_explicit() {
    for command in [
        Command::Ping {},
        Command::Info { branch: None },
        Command::Health { branch: None },
        Command::Metrics { branch: None },
        Command::Describe { branch: None },
        Command::ConfigGet {},
        Command::ConfigureGetKey {
            key: "target".to_owned(),
        },
        Command::SpaceList { branch: None },
        Command::SpaceExists {
            branch: None,
            space: "tenant_a".to_owned(),
        },
    ] {
        assert!(
            !command.is_write(),
            "{} should be classified as read-only",
            command.name()
        );
    }

    for command in [
        Command::SpaceCreate {
            branch: None,
            space: "tenant_a".to_owned(),
        },
        Command::SpaceDelete {
            branch: None,
            space: "tenant_a".to_owned(),
            force: true,
        },
    ] {
        assert!(
            command.is_write(),
            "{} should be classified as a write",
            command.name()
        );
    }
}
