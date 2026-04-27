use tempfile::tempdir;

use crate::common::create_executor;
use strata_core::Value;
use strata_executor::{Command, Error, ExportFormat, ExportPrimitive, Output, SearchQuery};

#[test]
fn recipe_seed_populates_builtin_recipes() {
    let executor = create_executor();

    let output = executor
        .execute(Command::RecipeList { branch: None })
        .unwrap();
    assert_eq!(output, Output::Keys(Vec::new()));

    let output = executor.execute(Command::RecipeSeed).unwrap();
    assert_eq!(output, Output::Unit);

    let output = executor
        .execute(Command::RecipeList { branch: None })
        .unwrap();
    match output {
        Output::Keys(names) => {
            for expected in ["default", "keyword", "semantic", "hybrid", "graph", "rag"] {
                assert!(
                    names.contains(&expected.to_string()),
                    "missing recipe {expected}"
                );
            }
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn recipe_set_get_and_delete_round_trip() {
    let executor = create_executor();
    let recipe_json = r#"{"version":1,"retrieve":{"bm25":{"k1":1.5}}}"#;

    let output = executor
        .execute(Command::RecipeSet {
            branch: None,
            name: "custom".into(),
            recipe_json: recipe_json.into(),
        })
        .unwrap();
    assert_eq!(output, Output::Unit);

    let output = executor
        .execute(Command::RecipeGet {
            branch: None,
            name: "custom".into(),
        })
        .unwrap();
    match output {
        Output::Maybe(Some(Value::String(json))) => assert!(json.contains("1.5")),
        other => panic!("unexpected output: {other:?}"),
    }

    let output = executor
        .execute(Command::RecipeDelete {
            branch: None,
            name: "custom".into(),
        })
        .unwrap();
    assert_eq!(output, Output::Unit);

    let output = executor
        .execute(Command::RecipeGet {
            branch: None,
            name: "custom".into(),
        })
        .unwrap();
    assert_eq!(output, Output::Maybe(None));
}

#[test]
fn search_resolves_named_recipe_and_returns_hits() {
    let executor = create_executor();

    executor.execute(Command::RecipeSeed).unwrap();
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "greeting".into(),
            value: Value::String("hello world".into()),
        })
        .unwrap();
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "farewell".into(),
            value: Value::String("goodbye world".into()),
        })
        .unwrap();

    let output = executor
        .execute(Command::Search {
            branch: None,
            space: None,
            search: SearchQuery {
                query: "hello".into(),
                recipe: Some(serde_json::Value::String("keyword".into())),
                precomputed_embedding: None,
                k: Some(10),
                as_of: None,
                diff: None,
            },
        })
        .unwrap();

    match output {
        Output::SearchResults { hits, stats, .. } => {
            assert_eq!(stats.mode, "keyword");
            assert!(!hits.is_empty(), "search should return at least one hit");
            assert_eq!(hits[0].entity_ref.kind, "kv");
            assert_eq!(hits[0].entity_ref.key.as_deref(), Some("greeting"));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn config_commands_round_trip_and_update_nested_model_config() {
    let executor = create_executor();

    let output = executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "openai".into(),
        })
        .unwrap();
    match output {
        Output::ConfigSetResult { key, new_value } => {
            assert_eq!(key, "provider");
            assert_eq!(new_value, "openai");
        }
        other => panic!("unexpected output: {other:?}"),
    }

    let output = executor
        .execute(Command::ConfigureGetKey {
            key: "provider".into(),
        })
        .unwrap();
    assert_eq!(output, Output::ConfigValue(Some("openai".into())));

    let output = executor
        .execute(Command::ConfigureModel {
            endpoint: "http://localhost:11434/v1".into(),
            model: "qwen3:1.7b".into(),
            api_key: Some("secret-token".into()),
            timeout_ms: Some(4321),
        })
        .unwrap();
    assert_eq!(output, Output::Unit);

    let output = executor.execute(Command::ConfigGet).unwrap();
    match output {
        Output::Config(config) => {
            let model = config.model.expect("model config should exist");
            assert_eq!(model.endpoint, "http://localhost:11434/v1");
            assert_eq!(model.model, "qwen3:1.7b");
            assert_eq!(model.timeout_ms, 4321);
            assert!(model.api_key.is_some());
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn auto_embed_commands_round_trip() {
    let executor = create_executor();

    assert_eq!(
        executor.execute(Command::AutoEmbedStatus).unwrap(),
        Output::Bool(false)
    );
    assert_eq!(
        executor
            .execute(Command::ConfigSetAutoEmbed { enabled: true })
            .unwrap(),
        Output::Unit
    );
    assert_eq!(
        executor.execute(Command::AutoEmbedStatus).unwrap(),
        Output::Bool(true)
    );

    let output = executor.execute(Command::ConfigGet).unwrap();
    match output {
        Output::Config(config) => assert!(config.auto_embed),
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn time_range_and_durability_counters_are_structured() {
    let executor = create_executor();

    let output = executor
        .execute(Command::TimeRange { branch: None })
        .unwrap();
    let initial_latest = match output {
        Output::TimeRange {
            oldest_ts,
            latest_ts,
        } => {
            if oldest_ts.is_none() {
                assert!(latest_ts.is_none());
            } else {
                assert!(latest_ts.is_some());
            }
            latest_ts
        }
        other => panic!("unexpected initial time range: {other:?}"),
    };

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "ts-key".into(),
            value: Value::String("value".into()),
        })
        .unwrap();

    let output = executor
        .execute(Command::TimeRange { branch: None })
        .unwrap();
    match output {
        Output::TimeRange {
            oldest_ts: Some(oldest),
            latest_ts: Some(latest),
        } => {
            assert!(latest >= oldest);
            if let Some(initial_latest) = initial_latest {
                assert!(latest >= initial_latest);
            }
        }
        other => panic!("unexpected populated time range: {other:?}"),
    }

    let output = executor.execute(Command::DurabilityCounters).unwrap();
    assert!(matches!(output, Output::DurabilityCounters(_)));
}

#[test]
fn retention_commands_preserve_current_contract() {
    let executor = create_executor();

    let output = executor
        .execute(Command::RetentionApply { branch: None })
        .unwrap();
    assert_eq!(output, Output::Unit);

    let stats_error = executor
        .execute(Command::RetentionStats { branch: None })
        .expect_err("RetentionStats should remain unavailable");
    match stats_error {
        Error::Internal { reason, .. } => assert!(reason.contains("RetentionStats")),
        other => panic!("unexpected error: {other:?}"),
    }

    let preview_error = executor
        .execute(Command::RetentionPreview { branch: None })
        .expect_err("RetentionPreview should remain unavailable");
    match preview_error {
        Error::Internal { reason, .. } => assert!(reason.contains("RetentionPreview")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn db_export_supports_inline_json_for_kv_and_reports_vector_contract() {
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "user:1".into(),
            value: Value::String("alice".into()),
        })
        .unwrap();

    let output = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Kv,
            format: ExportFormat::Json,
            prefix: Some("user:".into()),
            limit: None,
            path: None,
            collection: None,
            graph: None,
        })
        .unwrap();
    match output {
        Output::Exported(result) => {
            assert_eq!(result.row_count, 1);
            assert_eq!(result.primitive, ExportPrimitive::Kv);
            let data = result.data.expect("inline export should include data");
            assert!(data.contains("user:1"));
            assert!(result.path.is_none());
        }
        other => panic!("unexpected output: {other:?}"),
    }

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            dimension: 3,
            metric: strata_executor::DistanceMetric::Cosine,
        })
        .unwrap();
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            key: "doc-1".into(),
            vector: vec![1.0, 0.0, 0.0],
            metadata: Some(Value::object(
                [("title".to_string(), Value::String("hello".into()))]
                    .into_iter()
                    .collect(),
            )),
        })
        .unwrap();

    let err = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Vector,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
            collection: Some("embeddings".into()),
            graph: None,
        })
        .expect_err("inline vector export should require arrow-backed file output");
    match err {
        Error::InvalidInput { reason, hint } => {
            assert!(reason.contains("Vector export requires"));
            assert!(hint.as_deref().is_some_and(|hint| hint.contains("arrow")));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn db_export_graph_and_file_output_work() {
    let executor = create_executor();
    let temp = tempdir().unwrap();
    let export_path = temp.path().join("graph.jsonl");

    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "network".into(),
            cascade_policy: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: "network".into(),
            node_id: "n1".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Graph,
            format: ExportFormat::Jsonl,
            prefix: None,
            limit: None,
            path: Some(export_path.display().to_string()),
            collection: None,
            graph: Some("network".into()),
        })
        .unwrap();

    match output {
        Output::Exported(result) => {
            assert_eq!(result.row_count, 1);
            assert_eq!(result.path.as_deref(), Some(export_path.to_str().unwrap()));
            assert!(result.size_bytes.unwrap_or(0) > 0);
            // The arrow export path splits graph output into <stem>_nodes.<ext>
            // and <stem>_edges.<ext>; the inline path writes a single file at
            // the requested path.
            #[cfg(feature = "arrow")]
            {
                let nodes_path = temp.path().join("graph_nodes.jsonl");
                let edges_path = temp.path().join("graph_edges.jsonl");
                assert!(
                    nodes_path.exists(),
                    "expected nodes file at {}",
                    nodes_path.display()
                );
                assert!(
                    edges_path.exists() || edges_path.metadata().is_err(),
                    "expected edges file at {} (or no edges)",
                    edges_path.display()
                );
            }
            #[cfg(not(feature = "arrow"))]
            assert!(export_path.exists());
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn db_export_graph_respects_selected_graph() {
    let executor = create_executor();

    for graph_name in ["network-a", "network-b"] {
        executor
            .execute(Command::GraphCreate {
                branch: None,
                space: None,
                graph: graph_name.into(),
                cascade_policy: None,
            })
            .unwrap();
    }

    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: "network-a".into(),
            node_id: "node-a".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: "network-b".into(),
            node_id: "node-b".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::DbExport {
            branch: None,
            space: None,
            primitive: ExportPrimitive::Graph,
            format: ExportFormat::Json,
            prefix: None,
            limit: None,
            path: None,
            collection: None,
            graph: Some("network-a".into()),
        })
        .unwrap();

    match output {
        Output::Exported(result) => {
            assert_eq!(result.row_count, 1);
            let data = result
                .data
                .expect("inline graph export should include data");
            assert!(data.contains("network-a/node-a"));
            assert!(!data.contains("network-b/node-b"));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[cfg(not(feature = "arrow"))]
#[test]
fn arrow_import_requires_arrow_feature() {
    let executor = create_executor();
    let error = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: "missing.jsonl".into(),
            target: "kv".into(),
            key_column: None,
            value_column: None,
            collection: None,
            format: Some("jsonl".into()),
        })
        .expect_err("ArrowImport should fail without the arrow feature");

    match error {
        Error::Internal { reason, .. } => assert!(reason.contains("arrow")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[cfg(feature = "arrow")]
#[test]
fn arrow_import_reads_jsonl_into_kv() {
    let executor = create_executor();
    let temp = tempdir().unwrap();
    let import_path = temp.path().join("kv.jsonl");
    std::fs::write(
        &import_path,
        "{\"key\":\"user:1\",\"value\":\"alice\"}\n{\"key\":\"user:2\",\"value\":\"bob\"}\n",
    )
    .unwrap();

    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: None,
            file_path: import_path.display().to_string(),
            target: "kv".into(),
            key_column: None,
            value_column: None,
            collection: None,
            format: Some("jsonl".into()),
        })
        .unwrap();

    match output {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 2),
        other => panic!("unexpected output: {other:?}"),
    }

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "user:1".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(value)) => {
            assert_eq!(value.value, Value::String("alice".into()));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[cfg(feature = "arrow")]
#[test]
fn arrow_import_registers_non_default_space() {
    let executor = create_executor();
    let temp = tempdir().unwrap();
    let import_path = temp.path().join("analytics.jsonl");
    std::fs::write(&import_path, "{\"key\":\"user:9\",\"value\":\"carol\"}\n").unwrap();

    let output = executor
        .execute(Command::ArrowImport {
            branch: None,
            space: Some("analytics".into()),
            file_path: import_path.display().to_string(),
            target: "kv".into(),
            key_column: None,
            value_column: None,
            collection: None,
            format: Some("jsonl".into()),
        })
        .unwrap();
    match output {
        Output::ArrowImported { rows_imported, .. } => assert_eq!(rows_imported, 1),
        other => panic!("unexpected output: {other:?}"),
    }

    assert_eq!(
        executor
            .execute(Command::SpaceExists {
                branch: None,
                space: "analytics".into(),
            })
            .unwrap(),
        Output::Bool(true)
    );

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: Some("analytics".into()),
            key: "user:9".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(value)) => {
            assert_eq!(value.value, Value::String("carol".into()));
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[cfg(not(feature = "embed"))]
#[test]
fn feature_gated_model_and_generation_commands_fail_cleanly_in_default_builds() {
    let executor = create_executor();

    for command in [
        Command::Embed {
            text: "hello".into(),
        },
        Command::EmbedBatch {
            texts: vec!["hello".into(), "world".into()],
        },
        Command::ModelsList,
        Command::ModelsPull {
            name: "miniLM".into(),
        },
        Command::ModelsLocal,
        Command::Generate {
            model: "qwen3:1.7b".into(),
            prompt: "hello".into(),
            max_tokens: Some(8),
            temperature: None,
            top_k: None,
            top_p: None,
            seed: None,
            stop_tokens: None,
            stop_sequences: None,
        },
        Command::Tokenize {
            model: "qwen3:1.7b".into(),
            text: "hello".into(),
            add_special_tokens: None,
        },
        Command::Detokenize {
            model: "qwen3:1.7b".into(),
            ids: vec![1, 2, 3],
        },
        Command::GenerateUnload {
            model: "qwen3:1.7b".into(),
        },
        Command::ReindexEmbeddings { branch: None },
    ] {
        let error = executor
            .execute(command)
            .expect_err("feature-gated command should fail without embed");
        match error {
            Error::Internal { reason, .. } => {
                assert!(
                    reason.contains("embed") || reason.contains("Generation"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    let output = executor.execute(Command::EmbedStatus).unwrap();
    match output {
        Output::EmbedStatus(info) => {
            assert!(!info.auto_embed);
            assert_eq!(info.pending, 0);
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[cfg(feature = "embed")]
#[test]
fn feature_gated_model_commands_have_portable_success_paths() {
    let executor = create_executor();

    match executor.execute(Command::ModelsList).unwrap() {
        Output::ModelsList(models) => assert!(!models.is_empty()),
        other => panic!("unexpected output: {other:?}"),
    }

    match executor.execute(Command::ModelsLocal).unwrap() {
        Output::ModelsList(_) => {}
        other => panic!("unexpected output: {other:?}"),
    }

    assert_eq!(
        executor
            .execute(Command::GenerateUnload {
                model: "missing-model".into(),
            })
            .unwrap(),
        Output::Bool(false)
    );

    let error = executor
        .execute(Command::Generate {
            model: "ignored".into(),
            prompt: String::new(),
            max_tokens: Some(8),
            temperature: None,
            top_k: None,
            top_p: None,
            seed: None,
            stop_tokens: None,
            stop_sequences: None,
        })
        .expect_err("empty prompt should be rejected before model loading");
    match error {
        Error::InvalidInput { reason, .. } => {
            assert!(reason.contains("Prompt must not be empty"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    match executor.execute(Command::EmbedStatus).unwrap() {
        Output::EmbedStatus(info) => {
            assert!(!info.auto_embed);
            assert!(info.total_queued >= info.pending as u64);
        }
        other => panic!("unexpected output: {other:?}"),
    }
}
