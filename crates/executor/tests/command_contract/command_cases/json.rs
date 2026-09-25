use crate::support::*;

pub(super) fn json_commands() -> Vec<Command> {
    vec![
        Command::JsonSet {
            branch: None,
            space: None,
            key: "doc-a".to_owned(),
            path: "$.name".to_owned(),
            value: json!("Ada"),
        },
        Command::JsonGet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            key: "doc-a".to_owned(),
            path: "$.name".to_owned(),
            as_of: Some(42),
            as_of_time: None,
        },
        Command::JsonDelete {
            branch: None,
            space: None,
            key: "doc-a".to_owned(),
            path: "$.name".to_owned(),
        },
        Command::JsonHistory {
            branch: None,
            space: None,
            key: "doc-a".to_owned(),
        },
        Command::JsonExists {
            branch: None,
            space: None,
            key: "doc-a".to_owned(),
        },
        Command::JsonBatchExists {
            branch: None,
            space: None,
            keys: vec!["doc-a".to_owned(), "missing".to_owned()],
        },
        Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![BatchJsonEntry::new("doc-a", "$.name", json!("Ada"))],
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![BatchJsonGetEntry::new("doc-a", "$.name")],
            as_of: None,
            as_of_time: None,
        },
        Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![BatchJsonDeleteEntry::new("doc-a", "$.name")],
        },
        Command::JsonList {
            branch: None,
            space: None,
            prefix: Some("doc-".to_owned()),
            cursor: Some("doc-a".to_owned()),
            limit: Some(2),
            as_of: Some(99),
            as_of_time: None,
        },
        Command::JsonScan {
            branch: None,
            space: None,
            start: Some("doc-".to_owned()),
            limit: Some(10),
        },
        Command::JsonCount {
            branch: None,
            space: None,
            prefix: Some("doc-".to_owned()),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonSample {
            branch: None,
            space: None,
            prefix: Some("doc-".to_owned()),
            count: Some(3),
        },
        Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
            field_path: "$.name".to_owned(),
            index_type: JsonIndexType::Text,
        },
        Command::JsonDropIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
        },
        Command::JsonListIndexes {
            branch: None,
            space: None,
        },
    ]
}

pub(super) fn json_round_trip_edge_commands() -> Vec<Command> {
    vec![
        Command::JsonSet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            key: "doc-root".to_owned(),
            path: "$".to_owned(),
            value: json!({"name": "Ada", "tags": ["math"], "active": true}),
        },
        Command::JsonSet {
            branch: None,
            space: None,
            key: "doc-array".to_owned(),
            path: "$.tags".to_owned(),
            value: json!(["a", "b"]),
        },
        Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("", "$", json!("bad")),
                BatchJsonEntry::new("doc-a", "$[", json!({"bad": true})),
                BatchJsonEntry::new("doc-b", "$.nested", json!({"ok": true})),
            ],
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: Vec::new(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry::new("", "$"),
                BatchJsonGetEntry::new("doc-a", "$["),
            ],
            as_of: None,
            as_of_time: None,
        },
        // #3485: the two as-of clocks must round-trip on the wire the same way
        // `JsonGet`'s do — a populated `as_of` and a populated `as_of_time`,
        // each mutually exclusive with the other.
        Command::JsonBatchGet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            entries: vec![BatchJsonGetEntry::new("doc-a", "$.name")],
            as_of: Some(42),
            as_of_time: None,
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![BatchJsonGetEntry::new("doc-a", "$.name")],
            as_of: None,
            as_of_time: Some(1_700_000_000_000_000),
        },
        Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry::new("", "$"),
                BatchJsonDeleteEntry::new("doc-a", "$["),
            ],
        },
        Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-age".to_owned(),
            field_path: "$.age".to_owned(),
            index_type: JsonIndexType::Numeric,
        },
        Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-tag".to_owned(),
            field_path: "$.tag".to_owned(),
            index_type: JsonIndexType::Tag,
        },
    ]
}
