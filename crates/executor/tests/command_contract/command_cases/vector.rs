use crate::support::*;

pub(super) fn vector_commands() -> Vec<Command> {
    let mut commands = vector_collection_commands();
    commands.extend(vector_row_commands());
    commands.extend(vector_bulk_commands());
    commands
}

pub(super) fn vector_collection_commands() -> Vec<Command> {
    vec![
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        },
        Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            force: false,
        },
        Command::VectorListCollections {
            branch: None,
            space: None,
        },
        Command::VectorCollectionStats {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
        },
        Command::VectorCount {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            as_of: None,
            as_of_time: None,
        },
    ]
}

pub(super) fn vector_row_commands() -> Vec<Command> {
    vec![
        Command::VectorUpsert {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            vector: vec![1.0, 0.0],
            text: None,
            metadata: Some(json!({"kind": "doc"})),
        },
        Command::VectorGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            as_of: Some(42),
            as_of_time: None,
        },
        Command::VectorHistory {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
        },
        Command::VectorExists {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
        },
        Command::VectorBatchExists {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["doc-a".to_owned(), "missing".to_owned()],
        },
        Command::VectorListKeys {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            prefix: Some("doc-".to_owned()),
            cursor: Some("doc-a".to_owned()),
            limit: Some(2),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorScan {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            start: Some("doc-".to_owned()),
            limit: Some(10),
        },
        Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            patch: json!({"rank": 2}),
        },
        Command::VectorDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
        },
    ]
}

pub(super) fn vector_bulk_commands() -> Vec<Command> {
    vec![
        Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            filter: VectorMetadataFilter::new(vec![VectorFilterCondition::new(
                "kind",
                VectorFilterOp::Eq,
                VectorScalar::from("doc"),
            )]),
        },
        Command::VectorDeleteAll {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
        },
        Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            text: None,
            k: 10,
            filter: Some(VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "kind", "doc",
            )])),
            as_of: Some(99),
            as_of_time: None,
        },
        Command::VectorIndexQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            k: 10,
            filter: Some(VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "kind", "doc",
            )])),
            as_of: Some(99),
            as_of_time: None,
        },
        Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            entries: vec![BatchVectorEntry::new(
                "doc-a",
                vec![1.0, 0.0],
                Some(json!({"kind": "doc"})),
            )],
        },
        Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["doc-a".to_owned(), "missing".to_owned()],
        },
        Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["doc-a".to_owned(), "missing".to_owned()],
        },
    ]
}

// A flat list of command fixtures, like its event/graph siblings.
#[allow(clippy::too_many_lines)]
pub(super) fn vector_round_trip_edge_commands() -> Vec<Command> {
    vec![
        Command::VectorCreateCollection {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            dimension: 3,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        },
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "euclidean".to_owned(),
            dimension: 3,
            metric: VectorDistanceMetric::Euclidean,
            embedding_model: None,
        },
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "dot".to_owned(),
            dimension: 3,
            metric: VectorDistanceMetric::DotProduct,
            embedding_model: None,
        },
        Command::VectorUpsert {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            key: "mixed".to_owned(),
            vector: vec![0.0, 1.5, -2.0],
            text: None,
            metadata: Some(json!({})),
        },
        Command::VectorListKeys {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            prefix: Some("doc-".to_owned()),
            cursor: Some("doc-001".to_owned()),
            limit: Some(25),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorUpdateMetadata {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            key: "mixed".to_owned(),
            patch: json!({"rank": 3, "active": true, "tag": null}),
        },
        Command::VectorDeleteByFilter {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            filter: VectorMetadataFilter::new(vec![
                VectorFilterCondition::eq("tag", "doc"),
                VectorFilterCondition::new("rank", VectorFilterOp::Eq, VectorScalar::from(3)),
                VectorFilterCondition::new("active", VectorFilterOp::Eq, VectorScalar::from(true)),
                VectorFilterCondition::new("empty", VectorFilterOp::Eq, VectorScalar::Null),
            ]),
        },
        Command::VectorQuery {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            query: vec![0.0, 1.5, -2.0],
            text: None,
            k: 3,
            filter: Some(VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "tag", "doc",
            )])),
            as_of: Some(123),
            as_of_time: None,
        },
        Command::VectorIndexQuery {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            query: vec![0.0, 1.5, -2.0],
            k: 3,
            filter: Some(VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "tag", "doc",
            )])),
            as_of: Some(123),
            as_of_time: None,
        },
        Command::VectorBatchUpsert {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            entries: Vec::new(),
        },
        Command::VectorBatchGet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            keys: Vec::new(),
        },
        Command::VectorBatchDelete {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            collection: "cosine".to_owned(),
            keys: Vec::new(),
        },
    ]
}
