use crate::support::*;

pub(super) fn kv_outputs() -> Vec<Output> {
    vec![
        Output::KvVersionedValue(Maybe::found(VersionedValue::new(bytes("one"), 1, 10))),
        Output::KvVersionedValue(Maybe::missing()),
        Output::VersionHistory(Some(HistoryResult::new(vec![HistoryItem::new(
            Some(bytes("one")),
            false,
            1,
            10,
        )]))),
        Output::VersionHistory(None),
        Output::KeysPage {
            items: vec![bytes("a"), bytes("b")],
            page: PageInfo::terminal(),
        },
        Output::KeysPage {
            items: vec![bytes("a")],
            page: PageInfo::new(true, Some(bytes("a"))),
        },
        Output::WriteResult {
            key: bytes("a"),
            effect: MutationEffect::created(),
            commit: commit_receipt(1, 10, 1, 0),
        },
        Output::DeleteResult {
            key: bytes("a"),
            effect: MutationEffect::deleted(),
            commit: Some(commit_receipt(2, 20, 0, 1)),
        },
        Output::KvScanResult {
            items: vec![ScanItem::new(bytes("a"), bytes("one"), 1, 10)],
            page: PageInfo::terminal(),
        },
        Output::BatchResults(kv_batch(vec![BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(commit_receipt(1, 10, 1, 0)),
            BatchItemResult::new(bytes("a")),
        )])),
        Output::BatchResults(kv_batch(vec![BatchItem::failed(
            0,
            Some(BatchItemResult::new(Bytes::new(Vec::new()))),
            item_error("invalid key"),
        )])),
        Output::BatchGetResults(kv_batch_get(vec![BatchItem::ok(
            0,
            false,
            None,
            None,
            BatchGetItemResult::new(bytes("a"), Some(bytes("one")), Some(1), Some(10)),
        )])),
        Output::BatchGetResults(kv_batch_get(vec![BatchItem::failed(
            0,
            Some(BatchGetItemResult::not_found(Bytes::new(Vec::new()))),
            item_error("invalid key"),
        )])),
        Output::Bool(true),
        Output::BatchExistsResults(kv_batch_exists(vec![
            BatchItem::ok(
                0,
                false,
                None,
                None,
                BatchExistsItemResult::new(bytes("a"), true),
            ),
            BatchItem::ok(
                1,
                false,
                None,
                None,
                BatchExistsItemResult::new(bytes("missing"), false),
            ),
        ])),
        Output::Uint(2),
        Output::SampleResult {
            total_count: 3,
            items: vec![SampleItem::new(bytes("a"), bytes("one"), 1, 10)],
            page: PageInfo::terminal(),
        },
    ]
}

pub(super) fn json_outputs() -> Vec<Output> {
    vec![
        Output::JsonVersionedValue(MaybeJsonVersionedValue::found(JsonVersionedValue::new(
            json!({"name": "Ada"}),
            1,
            10,
            2,
        ))),
        Output::JsonVersionedValue(MaybeJsonVersionedValue::missing()),
        Output::JsonVersionHistory(Some(vec![JsonHistoryItem::new(
            Some(json!({"name": "Ada"})),
            1,
            10,
            Some(2),
            false,
        )])),
        Output::JsonVersionHistory(Some(vec![JsonHistoryItem::new(None, 2, 20, None, true)])),
        Output::JsonVersionHistory(None),
        Output::JsonWriteResult {
            key: "doc-a".to_owned(),
            effect: MutationEffect::created(),
            commit: commit_receipt(1, 10, 1, 0),
        },
        Output::JsonDeleteResult {
            key: "doc-a".to_owned(),
            effect: MutationEffect::deleted(),
            commit: Some(commit_receipt(2, 20, 0, 1)),
        },
        Output::JsonListResult {
            items: vec!["doc-a".to_owned()],
            page: PageInfo::new(true, Some("doc-a".to_owned())),
        },
        Output::JsonScanResult {
            items: vec![JsonSampleItem::new(
                "doc-a".to_owned(),
                json!({"name": "Ada"}),
                1,
                10,
                2,
            )],
            page: PageInfo::new(true, Some("doc-b".to_owned())),
        },
        Output::JsonBatchResults(json_batch(vec![BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(commit_receipt(1, 10, 1, 0)),
            JsonBatchItemResult::new(Some(2)),
        )])),
        Output::JsonBatchResults(json_batch(vec![BatchItem::failed(
            0,
            Some(JsonBatchItemResult::new(None)),
            item_error("invalid document id"),
        )])),
        Output::JsonBatchGetResults(json_batch_get(vec![BatchItem::ok(
            0,
            false,
            None,
            None,
            JsonBatchGetItemResult::new(Some(json!("Ada")), Some(1), Some(10), Some(2)),
        )])),
        Output::JsonBatchGetResults(json_batch_get(vec![BatchItem::failed(
            0,
            Some(JsonBatchGetItemResult::not_found()),
            item_error("invalid document id"),
        )])),
        Output::JsonBatchExistsResults(presence_batch_exists(vec![
            BatchItem::ok(0, false, None, None, BatchExistsPresence::new(true)),
            BatchItem::failed(1, None, item_error("invalid document id")),
        ])),
        Output::JsonSampleResult {
            total_count: 3,
            items: vec![JsonSampleItem::new(
                "doc-a".to_owned(),
                json!({"name": "Ada"}),
                1,
                10,
                2,
            )],
            page: PageInfo::terminal(),
        },
        Output::JsonIndexDefinition(json_index_definition("by-name", JsonIndexType::Text)),
        Output::JsonIndexList {
            items: vec![
                json_index_definition("by-age", JsonIndexType::Numeric),
                json_index_definition("by-name", JsonIndexType::Tag),
                json_index_definition("by-bio", JsonIndexType::Text),
            ],
            page: PageInfo::terminal(),
        },
    ]
}
