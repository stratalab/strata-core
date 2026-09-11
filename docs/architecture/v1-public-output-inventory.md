# V1 Public Output Inventory

## Status

Status: historical hand-maintained inventory for
`crates/executor/src/output.rs`, written for the response-contract completion
slices.

This document records every public `Output` variant exposed by executor at
the time and maps it to the target V1 response concept. The IDL now derives
that mapping (#3322): each command's `response_model` is resolved by
`strata-idl generate` from its family template and its generated schema, the
set of models in use is `crates/executor/idl/v1/dto-inventory.yaml`, and the
per-command result is `generated/command-index.json`. The `Target V1 model`
column below is the design-phase spelling (`MutationAck<KvWrite>`); the
published one is the derived name (`MutationAck`, `Maybe<VersionedValue>`).
Consult the generated index for the current surface.

Current scope:

1. 71 always-compiled output variants.
2. 11 `inference` feature-gated output variants.
3. 82 total output variants.

Maintenance rule: any change to `Output` must update this inventory, the
golden fixture path, and the target V1 model decision.

## Categories

The inventory uses the categories requested by the response-contract completion
plan:

1. `mutation acknowledgement`: a successful mutation, no-op mutation, or
   mutation-like lifecycle operation.
2. `optional read`: a read that can miss a logical entity.
3. `page`: a list, bounded result set, scan, or search result that should map
   to a shared V1 collection/page concept.
4. `batch`: positional multi-item response.
5. `diagnostics`: verification, planner, index, or health diagnostics.
6. `admin/status`: scalar, catalog, branch, space, and database status output.
7. `import/export`: Arrow import/export summary.
8. `inference`: inference runtime, model, generation, embedding, ranking, or
   cache response.

## V1 Shape Decisions

`wire shape changes before V1` means the serialized executor JSON should change
before the public V1 contract freezes. `SDK mapping sufficient` means the
current executor wire shape can stay stable while generated SDKs expose the
shared concept.

Golden fixture paths are target paths under:

```text
crates/executor/tests/fixtures/responses/v1/
```

Representative fixtures now exist for every public response family and every
shared V1 response concept. Some per-variant paths below remain target fixture
names for future IDL/SDK conformance expansion.

## Admin, Space, And Branch Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `Pong` | admin/status | `version` | `StatusResponse<Pong>` | No | Yes | `admin/pong.json` |
| `DatabaseInfo` | admin/status | `AdminDatabaseInfo` | `AdminDatabaseInfo` | No | Yes | `admin/database_info.json` |
| `Health` | diagnostics | `AdminHealth` | `DiagnosticsResponse<AdminHealth>` | No | Yes | `admin/health.json` |
| `Metrics` | diagnostics | `AdminMetrics` | `DiagnosticsResponse<AdminMetrics>` | No | Yes | `admin/metrics.json` |
| `Described` | admin/status | `AdminDescribe` | `AdminDescribe` | No | Yes | `admin/described.json` |
| `Config` | admin/status | `AdminConfig` | `AdminConfig` | No | Yes | `admin/config.json` |
| `ConfigValue` | optional read | `Option<String>` | `Maybe<String>` | No | Yes | `admin/config_value_found.json`, `admin/config_value_missing.json` |
| `SpaceList` | page | `items`, `has_more`, `cursor` | `Page<String, String>` | No | Yes | `spaces/list_page.json` |
| `SpaceCreateResult` | mutation acknowledgement | `space`, `created`, `effect`, `commit?`, `version?`, `timestamp?` | `MutationAck<Space>` | No | Yes | `spaces/create_applied.json`, `spaces/create_noop.json` |
| `SpaceDeleteResult` | mutation acknowledgement | `space`, `deleted`, `force`, `deleted_rows`, `effect`, `commit?`, `version?`, `timestamp?` | `MutationAck<SpaceDelete>` | No | Yes | `spaces/delete_applied.json`, `spaces/delete_missing.json` |
| `Branch` | admin/status | `BranchItem` | `StatusResponse<BranchItem>` | No | Yes | `branches/get.json`, `branches/fork.json` |
| `Branches` | page | `items`, `has_more`, `cursor` | `Page<BranchItem, String>` | No | Yes | `branches/list_page.json` |
| `BranchDeleteResult` | mutation acknowledgement | `branch`, `generation_before?`, `generation_after?`, `cleanup?` | `MutationAck<BranchDelete>` | Yes | No | `branches/delete.json` |

Notes:

1. `BranchDeleteResult` is mutation-like but lacks `MutationEffect` and
   `CommitReceipt`. V1 should either add those facts or explicitly classify
   branch deletion as control-plane status with cleanup facts.
2. Space and branch list outputs are finite today, but V1 should expose the
   same `Page` concept used by primitive lists so SDKs do not special-case
   catalog pagination.

## KV Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `KvValue` | optional read | `Option<Bytes>` | `Maybe<Bytes>` | No | Yes | `kv/get_found.json`, `kv/get_missing.json` |
| `KvVersionedValue` | optional read | `Option<VersionedValue>` | `Maybe<VersionedValue>` | No | Yes | `kv/get_versioned_found.json`, `kv/get_versioned_missing.json` |
| `VersionHistory` | optional read | `Option<Vec<HistoryItem>>` | `Maybe<Vec<HistoryItem>>` | No | Yes | `kv/history_found.json`, `kv/history_missing.json` |
| `Keys` | page | `items`, `has_more`, `cursor` | `Page<Bytes, Bytes>` | No | Yes | `kv/keys_page.json` |
| `KeysPage` | page | `items`, `has_more`, `cursor` | `Page<Bytes, Bytes>` | No | Yes | `kv/keys_paginated.json` |
| `WriteResult` | mutation acknowledgement | `key`, `effect`, `commit` | `MutationAck<KvWrite>` | No | Yes | `kv/write_applied.json` |
| `DeleteResult` | mutation acknowledgement | `key`, `effect`, `commit?` | `MutationAck<KvDelete>` | No | Yes | `kv/delete_applied.json`, `kv/delete_missing.json` |
| `KvScanResult` | page | `items`, `has_more`, `cursor` | `Page<ScanItem, Bytes>` | No | Yes | `kv/scan_page.json` |
| `BatchResults` | batch | `BatchResult<BatchItemResult>` | `BatchResult<KvMutationItem>` | No | Yes | `kv/batch_write.json`, `kv/batch_write_partial.json` |
| `BatchGetResults` | batch | `BatchResult<BatchGetItemResult { key, found, value?, version?, timestamp?, error }>` | `BatchResult<Maybe<Bytes>>` | No | Yes | `kv/batch_get.json`, `kv/batch_get_partial.json` |
| `Bool` | admin/status | `bool` | `StatusValue<bool>` | Yes | No | `kv/exists.json`, `json/drop_index.json`, `vector/exists.json`, `event/exists.json`, `spaces/exists.json` |
| `BoolList` | batch | `Vec<bool>` | `BatchResult<StatusValue<bool>>` | Yes | No | `kv/batch_exists.json` |
| `Uint` | admin/status | `u64` | `StatusValue<u64>` | Yes | No | `kv/count.json`, `json/count.json`, `vector/count.json`, `event/length.json`, `spaces/count.json` |
| `SampleResult` | page | `total_count`, `items`, `has_more`, `cursor` | `SamplePage<SampleItem>` | No | Yes | `kv/sample.json` |

Notes:

1. `Bool`, `BoolList`, and `Uint` are public transitional helpers shared by
   multiple command families. They should not remain anonymous in the V1 IDL.
2. `Keys` and `KvScanResult` now expose terminal page facts when no
   continuation cursor exists.
3. KV batch get items expose an explicit `found` field so empty byte values and
   misses remain distinguishable to plain JSON clients.

## JSON Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `JsonValue` | optional read | `MaybeJsonValue` | `Maybe<JsonValue>` | No | Yes | `json/get_found.json`, `json/get_missing.json`, `json/get_null.json` |
| `JsonVersionedValue` | optional read | `MaybeJsonVersionedValue` | `Maybe<JsonVersionedValue>` | No | Yes | `json/get_versioned_found.json`, `json/get_versioned_missing.json`, `json/get_versioned_null.json` |
| `JsonVersionHistory` | optional read | `Option<Vec<JsonHistoryItem>>` | `Maybe<Vec<JsonHistoryItem>>` | No | Yes | `json/history_found.json`, `json/history_missing.json` |
| `JsonBatchResults` | batch | `BatchResult<JsonBatchItemResult>` | `BatchResult<JsonMutationItem>` | No | Yes | `json/batch_write.json`, `json/batch_write_partial.json` |
| `JsonBatchGetResults` | batch | `BatchResult<JsonBatchGetItemResult>` | `BatchResult<Maybe<JsonValue>>` | No | Yes | `json/batch_get.json`, `json/batch_get_partial.json` |
| `JsonListResult` | page | `items`, `has_more`, `cursor` | `Page<String, String>` | No | Yes | `json/list_page.json` |
| `JsonSampleResult` | page | `total_count`, `items`, `has_more`, `cursor` | `SamplePage<JsonSampleItem>` | No | Yes | `json/sample.json` |
| `JsonIndexDefinition` | admin/status | `JsonIndexDefinition` | `StatusResponse<JsonIndexDefinition>` | No | Yes | `json/index_create.json` |
| `JsonIndexList` | page | `items`, `has_more`, `cursor` | `Page<JsonIndexDefinition, String>` | No | Yes | `json/index_list_page.json` |

Notes:

1. JSON already uses explicit `Maybe` wrappers on the wire because stored JSON
   `null` is a valid value.
2. JSON index create/drop/list are control-plane operations for the JSON
   primitive. `JsonIndexDefinition` can remain a domain status object, and
   index lists now use the shared page model.

## Vector Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `VectorWriteResult` | mutation acknowledgement | `collection`, `key`, `effect`, `commit`, `version`, `timestamp`, `vector_revision` | `MutationAck<VectorWrite>` | No | Yes | `vector/upsert_applied.json` |
| `VectorMetadataUpdateResult` | mutation acknowledgement | `collection`, `key`, `updated`, `effect`, `commit?`, `version?`, `timestamp?`, `vector_revision?` | `MutationAck<VectorMetadataUpdate>` | No | Yes | `vector/update_metadata_applied.json`, `vector/update_metadata_missing.json` |
| `VectorDeleteResult` | mutation acknowledgement | `collection`, `key`, `deleted`, `effect`, `commit?`, `version?`, `timestamp?` | `MutationAck<VectorDelete>` | No | Yes | `vector/delete_applied.json`, `vector/delete_missing.json` |
| `VectorBulkDeleteResult` | mutation acknowledgement | `collection`, `deleted_count`, `effect`, `commit?`, `version?`, `timestamp?` | `MutationAck<VectorBulkDelete>` | No | Yes | `vector/delete_by_filter_applied.json`, `vector/delete_by_filter_noop.json` |
| `VectorData` | optional read | `Option<VectorVersionedData>` | `Maybe<VectorVersionedData>` | No | Yes | `vector/get_found.json`, `vector/get_missing.json` |
| `VectorVersionHistory` | optional read | `Option<Vec<VectorHistoryItem>>` | `Maybe<Vec<VectorHistoryItem>>` | No | Yes | `vector/history_found.json`, `vector/history_missing.json` |
| `VectorMatches` | page | `Vec<VectorMatch>` | `SearchResult<VectorMatch>` | No | Yes | `vector/search.json` |
| `VectorIndexQuery` | diagnostics | `VectorIndexQueryResult` | `SearchResult<VectorMatch> + IndexDiagnostics` | No | Yes | `vector/search_with_index_diagnostics.json` |
| `VectorKeyPage` | page | `items`, `has_more`, `cursor` | `Page<String, String>` | No | Yes | `vector/keys_page.json` |
| `VectorCollectionList` | page | `items`, `has_more`, `cursor` | `Page<VectorCollectionInfo, String>` | No | Yes | `vector/collection_list_page.json` |
| `VectorBatchUpsertResults` | batch | `BatchResult<VectorBatchItemResult>` | `BatchResult<VectorMutationItem>` | No | Yes | `vector/batch_upsert.json`, `vector/batch_upsert_partial.json` |
| `VectorBatchGetResults` | batch | `BatchResult<VectorBatchGetItemResult { found, value?, error }>` | `BatchResult<Maybe<VectorVersionedData>>` | No | Yes | `vector/batch_get.json`, `vector/batch_get_partial.json` |
| `VectorBatchDeleteResults` | batch | `BatchResult<VectorBatchItemResult>` | `BatchResult<VectorMutationItem>` | No | Yes | `vector/batch_delete.json`, `vector/batch_delete_partial.json` |

Notes:

1. Plain vector search is not paginated, but it is a bounded result set and
   should still map to one SDK collection response shape.
2. `VectorIndexQuery` is the diagnostic form of vector search. Its match list
   should share the same SDK accessor as `VectorMatches`; diagnostics should be
   an optional attached diagnostic object.
3. Vector batch get items expose an explicit `found` field because itemwise
   batch reads should not require clients to infer missing entries from a
   nullable payload.

## Event Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `EventAppendResult` | mutation acknowledgement | `sequence`, `event_type`, `effect`, `commit`, `version`, `timestamp` | `MutationAck<EventAppend>` | No | Yes | `event/append_applied.json` |
| `EventRecord` | optional read | `Option<EventVersionedData>` | `Maybe<EventVersionedData>` | No | Yes | `event/get_found.json`, `event/get_missing.json` |
| `EventRecords` | page | `items`, `has_more`, `cursor` | `Page<EventVersionedData, u64>` | No | Yes | `event/list_page.json` |
| `EventLength` | admin/status | `count` | `StatusValue<u64>` | Yes | No | `event/length.json` |
| `EventTypeList` | page | `items`, `has_more`, `cursor` | `Page<String, String>` | No | Yes | `event/type_list_page.json` |
| `EventRangeResult` | page | `items`, `has_more`, `cursor` | `Page<EventVersionedData, u64>` | No | Yes | `event/range_page.json`, `event/range_terminal_page.json` |
| `EventBatchAppendResults` | batch | `BatchResult<EventBatchAppendItemResult>` | `BatchResult<EventAppendItem>` | No | Yes | `event/batch_append.json`, `event/batch_append_partial.json` |
| `EventChainVerification` | diagnostics | `EventChainVerification` | `DiagnosticsResponse<EventChainVerification>` | No | Yes | `event/chain_verification_ok.json`, `event/chain_verification_failed.json` |

Notes:

1. `EventRecords` has command-level `limit` but no continuation facts. V1
   should expose it as a terminal page or retire it in favor of
   `EventRangeResult`.
2. `EventLength` should not share the anonymous `Uint` output in the V1 IDL.

## Graph Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `GraphInfo` | mutation acknowledgement | `GraphInfoData` | `MutationAck<GraphCreate>` | Yes | No | `graph/create_applied.json` |
| `GraphInfoResult` | optional read | `Option<GraphInfoData>` | `Maybe<GraphInfoData>` | No | Yes | `graph/get_meta_found.json`, `graph/get_meta_missing.json` |
| `GraphNamePage` | page | `items`, `has_more`, `cursor` | `Page<String, String>` | No | Yes | `graph/list_page.json`, `graph/list_terminal_page.json` |
| `GraphNodeResult` | optional read | `Option<GraphNodeDataOutput>` | `Maybe<GraphNodeDataOutput>` | No | Yes | `graph/get_node_found.json`, `graph/get_node_missing.json` |
| `GraphNodePage` | page | `items`, `has_more`, `cursor` | `Page<GraphNodeDataOutput, String>` | No | Yes | `graph/node_page.json`, `graph/node_terminal_page.json` |
| `GraphEdgeResult` | optional read | `Option<GraphEdgeDataOutput>` | `Maybe<GraphEdgeDataOutput>` | No | Yes | `graph/get_edge_found.json`, `graph/get_edge_missing.json` |
| `GraphNeighborPage` | page | `items`, `has_more`, `cursor` | `Page<GraphNeighborHit, String>` | No | Yes | `graph/neighbor_page.json` |
| `GraphBindingPage` | page | `items`, `has_more`, `cursor` | `Page<GraphBindingHit, String>` | No | Yes | `graph/binding_page.json` |
| `GraphNodeWriteResult` | mutation acknowledgement | `graph`, `node_id`, `created`, `effect`, `commit`, `version`, `timestamp` | `MutationAck<GraphNodeWrite>` | No | Yes | `graph/node_write_created.json`, `graph/node_write_updated.json` |
| `GraphEdgeWriteResult` | mutation acknowledgement | `graph`, `src`, `edge_type`, `dst`, `created`, `effect`, `commit`, `version`, `timestamp` | `MutationAck<GraphEdgeWrite>` | No | Yes | `graph/edge_write_created.json`, `graph/edge_write_updated.json` |
| `GraphDeleteResult` | mutation acknowledgement | `graph`, `node_id?`, `src?`, `edge_type?`, `dst?`, `deleted`, `effect`, `commit?`, `version?`, `timestamp?` | `MutationAck<GraphDelete>` | No | Yes | `graph/delete_node_applied.json`, `graph/delete_edge_missing.json` |
| `GraphBatchWriteResult` | batch | `graph`, flattened `BatchResult<GraphBatchItemResult>` | `BatchResult<GraphMutationItem>` | No | Yes | `graph/batch_write.json`, `graph/batch_write_partial.json` |

Notes:

1. `GraphInfo` is returned by graph create and currently lacks shared
   mutation facts. This is a pre-V1 wire-shape gap.
2. Graph page variants now expose `items`, `has_more`, and `cursor` on the V1
   wire shape.

## Arrow Import/Export Outputs

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `ArrowImportResult` | import/export | `ArrowImportResult` | `ImportResult` | No | Yes | `arrow/import_kv.json`, `arrow/import_json.json`, `arrow/import_vector.json` |
| `ArrowExportResult` | import/export | `ArrowExportResult` | `ExportResult` | No | Yes | `arrow/export_kv.json`, `arrow/export_json.json`, `arrow/export_event.json`, `arrow/export_vector.json`, `arrow/export_graph.json` |

Notes:

1. Arrow import/export results are already domain-specific DTOs. V1 only needs
   stable golden snapshots and IDL models.
2. Arrow import paths that internally call batch operations must not leak old
   primitive batch wrapper assumptions.

## Inference Outputs

These variants compile only with the executor `inference` feature. They are
public V1 candidates, not internal outputs, because the executor exposes
inference commands when the feature is enabled.

| Output variant | Category | Current fields | Target V1 model | Wire shape changes before V1 | SDK mapping sufficient | Golden fixture path |
| --- | --- | --- | --- | --- | --- | --- |
| `InferenceModels` | inference | `items`, `has_more`, `cursor` | `Page<ModelInfo, String>` | No | Yes | `inference/models_page.json` |
| `InferenceModelPulled` | inference | `PullModelOutput` | `MutationAck<ModelPull>` | Yes | No | `inference/model_pulled.json` |
| `InferenceCapability` | inference | `InferenceCapability` | `StatusResponse<InferenceCapability>` | No | Yes | `inference/capability.json` |
| `InferenceGeneration` | inference | `GenerateResponse` | `InferenceGenerateResponse` | No | Yes | `inference/generation.json` |
| `InferenceTokenIds` | inference | `Vec<u32>` | `InferenceTokenizeResponse` | No | Yes | `inference/token_ids.json` |
| `InferenceText` | inference | `String` | `InferenceTextResponse` | No | Yes | `inference/text.json` |
| `InferenceEmbedding` | inference | `Vec<f32>` | `InferenceEmbeddingResponse` | No | Yes | `inference/embedding.json` |
| `InferenceEmbeddings` | inference | `EmbedResponse` | `InferenceEmbeddingsResponse` | No | Yes | `inference/embeddings.json` |
| `InferenceRanking` | inference | `RankResponse` | `InferenceRankingResponse` | No | Yes | `inference/ranking.json` |
| `InferenceUnloadResult` | inference | `unloaded` | `MutationAck<ModelUnload>` | Yes | No | `inference/unload_applied.json`, `inference/unload_missing.json` |
| `InferenceCacheStatus` | inference | `ModelCacheStatus` | `DiagnosticsResponse<ModelCacheStatus>` | No | Yes | `inference/cache_status.json` |

Notes:

1. Inference output serde coverage is not represented in the current
   command-contract fixture matrix. Golden response fixtures must add it.
2. Model pull and unload are mutation-like runtime operations and should expose
   `applied`, `effect`, and any durable/cache facts the inference runtime can
   report.
3. Model listing should use the same page/list concept as other catalogs even
   if the first V1 implementation returns a single terminal page.

## Deferred Or Internal Variants

No current `Output` variant is marked internal-only. The deferred work is shape
normalization, not hiding variants.

The following variants are public but transitional and require V1 wire or IDL
normalization before the response contract freezes:

1. `Bool`
2. `BoolList`
3. `Uint`
4. `GraphInfo`
5. `BranchDeleteResult`
6. `BatchResults`
7. `BatchGetResults`
8. `JsonBatchResults`
9. `JsonBatchGetResults`
10. `VectorBatchUpsertResults`
11. `VectorBatchGetResults`
12. `VectorBatchDeleteResults`
13. `EventLength`
14. `EventBatchAppendResults`
15. `GraphBatchWriteResult`
16. `InferenceModelPulled`
17. `InferenceUnloadResult`

## Exit Criteria Check

1. Every output variant is categorized: satisfied.
2. Every variant has a target V1 model: satisfied.
3. Deferred variants are explicitly marked non-V1 or internal: satisfied;
   there are no internal-only variants, and all transitional public variants
   are listed above.
