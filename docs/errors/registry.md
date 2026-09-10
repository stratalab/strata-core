# Error Code Registry

> **This page is an anchor index, not the registry (#3149).** It exists so a
> CI guard can prove every registered code has a documented target, and its
> anchors are checked by `every_registry_docs_url_has_target`. It is **not** the
> source of truth and must not be read as one — a second registry that can
> drift is not a registry.
>
> The shipped documentation lives at `https://stratadb.org/e/<error-code>`, and
> the authoritative metadata is in the binary: `strata agents errors --json`.

The source of truth for code metadata is:

- `crates/engine/src/diagnostics/registry.rs` for engine-owned codes.
- `crates/executor/src/error_registry.rs` for executor and inference-boundary codes.

Both are readable at runtime with `strata agents errors --json`, which is
version-matched to the binary asking and is what any consumer should use.

Each registry entry records the public class, retry policy, commit outcome,
message template, suggested fix, docs slug, and details schema. Public
command-boundary rendering must resolve through that registry before an error
is emitted.

Support can search logs by `reference_id` first and then by the stable `code`
attached to the same error status.

## Code Targets

The anchors below are intentionally machine-checkable. Keep them in sync with
the public registry when adding, renaming, or removing a public error code.

### Engine Codes

<a id="already_exists.engine.branch"></a>
- `already_exists.engine.branch`

<a id="already_exists.engine.graph"></a>
- `already_exists.engine.graph`

<a id="already_exists.engine.json_document"></a>
- `already_exists.engine.json_document`

<a id="already_exists.engine.json_index"></a>
- `already_exists.engine.json_index`

<a id="already_exists.engine.persistence"></a>
- `already_exists.engine.persistence`

<a id="already_exists.engine.vector_collection"></a>
- `already_exists.engine.vector_collection`

<a id="ambiguous_commit.engine.persistence"></a>
- `ambiguous_commit.engine.persistence`

<a id="conflict.engine.artifact_import"></a>
- `conflict.engine.artifact_import`

<a id="conflict.engine.branch_generation"></a>
- `conflict.engine.branch_generation`

<a id="conflict.engine.persistence"></a>
- `conflict.engine.persistence`

<a id="conflict.engine.promotion"></a>
- `conflict.engine.promotion`

<a id="corruption.engine.artifact_payload"></a>
- `corruption.engine.artifact_payload`

<a id="corruption.engine.persistence_recovery"></a>
- `corruption.engine.persistence_recovery`

<a id="data_loss.engine.branch_catalog"></a>
- `data_loss.engine.branch_catalog`

<a id="data_loss.engine.branch_create_pending"></a>
- `data_loss.engine.branch_create_pending`

<a id="data_loss.engine.branch_id"></a>
- `data_loss.engine.branch_id`

<a id="data_loss.engine.control_name"></a>
- `data_loss.engine.control_name`

<a id="data_loss.engine.control_plane"></a>
- `data_loss.engine.control_plane`

<a id="data_loss.engine.control_plane_missing"></a>
- `data_loss.engine.control_plane_missing`

<a id="data_loss.engine.event_index_key"></a>
- `data_loss.engine.event_index_key`

<a id="data_loss.engine.event_key"></a>
- `data_loss.engine.event_key`

<a id="data_loss.engine.event_metadata"></a>
- `data_loss.engine.event_metadata`

<a id="data_loss.engine.event_record"></a>
- `data_loss.engine.event_record`

<a id="data_loss.engine.graph_binding_key"></a>
- `data_loss.engine.graph_binding_key`

<a id="data_loss.engine.graph_binding_record"></a>
- `data_loss.engine.graph_binding_record`

<a id="data_loss.engine.graph_edge_key"></a>
- `data_loss.engine.graph_edge_key`

<a id="data_loss.engine.graph_edge_record"></a>
- `data_loss.engine.graph_edge_record`

<a id="data_loss.engine.graph_index"></a>
- `data_loss.engine.graph_index`

<a id="data_loss.engine.graph_key"></a>
- `data_loss.engine.graph_key`

<a id="data_loss.engine.graph_metadata"></a>
- `data_loss.engine.graph_metadata`

<a id="data_loss.engine.graph_node_key"></a>
- `data_loss.engine.graph_node_key`

<a id="data_loss.engine.graph_node_record"></a>
- `data_loss.engine.graph_node_record`

<a id="data_loss.engine.graph_ontology_record"></a>
- `data_loss.engine.graph_ontology_record`

<a id="data_loss.engine.graph_reverse_edge_key"></a>
- `data_loss.engine.graph_reverse_edge_key`

<a id="data_loss.engine.graph_type_index_key"></a>
- `data_loss.engine.graph_type_index_key`

<a id="data_loss.engine.graph_type_index_record"></a>
- `data_loss.engine.graph_type_index_record`

<a id="data_loss.engine.json_document"></a>
- `data_loss.engine.json_document`

<a id="data_loss.engine.json_index"></a>
- `data_loss.engine.json_index`

<a id="data_loss.engine.json_index_key"></a>
- `data_loss.engine.json_index_key`

<a id="data_loss.engine.json_key"></a>
- `data_loss.engine.json_key`

<a id="data_loss.engine.kv_key"></a>
- `data_loss.engine.kv_key`

<a id="data_loss.engine.kv_value"></a>
- `data_loss.engine.kv_value`

<a id="data_loss.engine.space_catalog"></a>
- `data_loss.engine.space_catalog`

<a id="data_loss.engine.vector_artifact"></a>
- `data_loss.engine.vector_artifact`

<a id="data_loss.engine.vector_artifacts"></a>
- `data_loss.engine.vector_artifacts`

<a id="data_loss.engine.vector_collection"></a>
- `data_loss.engine.vector_collection`

<a id="data_loss.engine.vector_collection_key"></a>
- `data_loss.engine.vector_collection_key`

<a id="data_loss.engine.vector_index_manifest"></a>
- `data_loss.engine.vector_index_manifest`

<a id="data_loss.engine.vector_index_manifest_key"></a>
- `data_loss.engine.vector_index_manifest_key`

<a id="data_loss.engine.vector_key"></a>
- `data_loss.engine.vector_key`

<a id="data_loss.engine.vector_record"></a>
- `data_loss.engine.vector_record`

<a id="failed_precondition.engine.branch_status"></a>
- `failed_precondition.engine.branch_status`

<a id="failed_precondition.engine.capability_registry"></a>
- `failed_precondition.engine.capability_registry`

<a id="failed_precondition.engine.control_payload_version"></a>
- `failed_precondition.engine.control_payload_version`

<a id="failed_precondition.engine.default_branch"></a>
- `failed_precondition.engine.default_branch`

<a id="failed_precondition.engine.graph_negative_weight"></a>
- `failed_precondition.engine.graph_negative_weight`

<a id="failed_precondition.engine.graph_ontology_edge_type"></a>
- `failed_precondition.engine.graph_ontology_edge_type`

<a id="failed_precondition.engine.graph_ontology_endpoint_type"></a>
- `failed_precondition.engine.graph_ontology_endpoint_type`

<a id="failed_precondition.engine.graph_ontology_freeze"></a>
- `failed_precondition.engine.graph_ontology_freeze`

<a id="failed_precondition.engine.graph_ontology_frozen"></a>
- `failed_precondition.engine.graph_ontology_frozen`

<a id="failed_precondition.engine.graph_ontology_node_type"></a>
- `failed_precondition.engine.graph_ontology_node_type`

<a id="failed_precondition.engine.graph_ontology_required_property"></a>
- `failed_precondition.engine.graph_ontology_required_property`

<a id="failed_precondition.engine.layout_version"></a>
- `failed_precondition.engine.layout_version`

<a id="failed_precondition.engine.migration_registry"></a>
- `failed_precondition.engine.migration_registry`

<a id="failed_precondition.engine.persistence"></a>
- `failed_precondition.engine.persistence`

<a id="failed_precondition.engine.runtime_closed"></a>
- `failed_precondition.engine.runtime_closed`

<a id="failed_precondition.engine.embedding_model_mismatch"></a>
- `failed_precondition.engine.embedding_model_mismatch`

<a id="failed_precondition.engine.embedding_model_missing"></a>
- `failed_precondition.engine.embedding_model_missing`

<a id="failed_precondition.engine.space_not_empty"></a>
- `failed_precondition.engine.space_not_empty`

<a id="failed_precondition.engine.storage_registry"></a>
- `failed_precondition.engine.storage_registry`

<a id="failed_precondition.engine.vector_artifact"></a>
- `failed_precondition.engine.vector_artifact`

<a id="failed_precondition.engine.vector_index_manifest"></a>
- `failed_precondition.engine.vector_index_manifest`

<a id="history_unavailable.engine.persistence_history"></a>
- `history_unavailable.engine.persistence_history`

<a id="internal.engine.persistence"></a>
- `internal.engine.persistence`

<a id="invalid_argument.engine.branch_catalog"></a>
- `invalid_argument.engine.branch_catalog`

<a id="invalid_argument.engine.branch_delete"></a>
- `invalid_argument.engine.branch_delete`

<a id="invalid_argument.engine.branch_name"></a>
- `invalid_argument.engine.branch_name`

<a id="invalid_argument.engine.branch_name_reserved"></a>
- `invalid_argument.engine.branch_name_reserved`

<a id="invalid_argument.engine.branch_point"></a>
- `invalid_argument.engine.branch_point`

<a id="invalid_argument.engine.config_key"></a>
- `invalid_argument.engine.config_key`

<a id="invalid_argument.engine.event_append"></a>
- `invalid_argument.engine.event_append`

<a id="invalid_argument.engine.event_batch"></a>
- `invalid_argument.engine.event_batch`

<a id="invalid_argument.engine.event_metadata"></a>
- `invalid_argument.engine.event_metadata`

<a id="invalid_argument.engine.event_payload"></a>
- `invalid_argument.engine.event_payload`

<a id="invalid_argument.engine.event_payload_too_large"></a>
- `invalid_argument.engine.event_payload_too_large`

<a id="invalid_argument.engine.event_record"></a>
- `invalid_argument.engine.event_record`

<a id="invalid_argument.engine.event_type"></a>
- `invalid_argument.engine.event_type`

<a id="invalid_argument.engine.graph_batch"></a>
- `invalid_argument.engine.graph_batch`

<a id="invalid_argument.engine.graph_binding"></a>
- `invalid_argument.engine.graph_binding`

<a id="invalid_argument.engine.graph_binding_record"></a>
- `invalid_argument.engine.graph_binding_record`

<a id="invalid_argument.engine.graph_edge_endpoint"></a>
- `invalid_argument.engine.graph_edge_endpoint`

<a id="invalid_argument.engine.graph_edge_record"></a>
- `invalid_argument.engine.graph_edge_record`

<a id="invalid_argument.engine.graph_edge_type"></a>
- `invalid_argument.engine.graph_edge_type`

<a id="invalid_argument.engine.graph_edge_type_reserved"></a>
- `invalid_argument.engine.graph_edge_type_reserved`

<a id="invalid_argument.engine.graph_edge_weight"></a>
- `invalid_argument.engine.graph_edge_weight`

<a id="invalid_argument.engine.graph_metadata"></a>
- `invalid_argument.engine.graph_metadata`

<a id="invalid_argument.engine.graph_name"></a>
- `invalid_argument.engine.graph_name`

<a id="invalid_argument.engine.graph_name_reserved"></a>
- `invalid_argument.engine.graph_name_reserved`

<a id="invalid_argument.engine.graph_node_id"></a>
- `invalid_argument.engine.graph_node_id`

<a id="invalid_argument.engine.graph_node_record"></a>
- `invalid_argument.engine.graph_node_record`

<a id="invalid_argument.engine.graph_ontology_record"></a>
- `invalid_argument.engine.graph_ontology_record`

<a id="invalid_argument.engine.graph_pagerank_options"></a>
- `invalid_argument.engine.graph_pagerank_options`

<a id="invalid_argument.engine.graph_personalization"></a>
- `invalid_argument.engine.graph_personalization`

<a id="invalid_argument.engine.graph_properties"></a>
- `invalid_argument.engine.graph_properties`

<a id="invalid_argument.engine.graph_properties_too_large"></a>
- `invalid_argument.engine.graph_properties_too_large`

<a id="invalid_argument.engine.graph_property_name"></a>
- `invalid_argument.engine.graph_property_name`

<a id="invalid_argument.engine.graph_type_hint"></a>
- `invalid_argument.engine.graph_type_hint`

<a id="invalid_argument.engine.graph_type_index_record"></a>
- `invalid_argument.engine.graph_type_index_record`

<a id="invalid_argument.engine.graph_type_name"></a>
- `invalid_argument.engine.graph_type_name`

<a id="invalid_argument.engine.graph_type_name_reserved"></a>
- `invalid_argument.engine.graph_type_name_reserved`

<a id="invalid_argument.engine.json_array_too_large"></a>
- `invalid_argument.engine.json_array_too_large`

<a id="invalid_argument.engine.json_batch"></a>
- `invalid_argument.engine.json_batch`

<a id="invalid_argument.engine.json_batch_duplicate_document"></a>
- `invalid_argument.engine.json_batch_duplicate_document`

<a id="invalid_argument.engine.json_document"></a>
- `invalid_argument.engine.json_document`

<a id="invalid_argument.engine.json_document_id"></a>
- `invalid_argument.engine.json_document_id`

<a id="invalid_argument.engine.json_document_too_deep"></a>
- `invalid_argument.engine.json_document_too_deep`

<a id="invalid_argument.engine.json_document_too_large"></a>
- `invalid_argument.engine.json_document_too_large`

<a id="invalid_argument.engine.json_index"></a>
- `invalid_argument.engine.json_index`

<a id="invalid_argument.engine.json_index_name"></a>
- `invalid_argument.engine.json_index_name`

<a id="invalid_argument.engine.json_index_name_reserved"></a>
- `invalid_argument.engine.json_index_name_reserved`

<a id="invalid_argument.engine.json_path"></a>
- `invalid_argument.engine.json_path`

<a id="invalid_argument.engine.json_path_not_found"></a>
- `invalid_argument.engine.json_path_not_found`

<a id="invalid_argument.engine.json_path_too_long"></a>
- `invalid_argument.engine.json_path_too_long`

<a id="invalid_argument.engine.json_path_type"></a>
- `invalid_argument.engine.json_path_type`

<a id="invalid_argument.engine.json_value"></a>
- `invalid_argument.engine.json_value`

<a id="invalid_argument.engine.kv_batch"></a>
- `invalid_argument.engine.kv_batch`

<a id="invalid_argument.engine.kv_batch_duplicate_key"></a>
- `invalid_argument.engine.kv_batch_duplicate_key`

<a id="invalid_argument.engine.kv_key"></a>
- `invalid_argument.engine.kv_key`

<a id="invalid_argument.engine.persistence"></a>
- `invalid_argument.engine.persistence`

<a id="invalid_argument.engine.product_space"></a>
- `invalid_argument.engine.product_space`

<a id="invalid_argument.engine.product_space_reserved"></a>
- `invalid_argument.engine.product_space_reserved`

<a id="invalid_argument.engine.space_catalog"></a>
- `invalid_argument.engine.space_catalog`

<a id="invalid_argument.engine.space_delete_default"></a>
- `invalid_argument.engine.space_delete_default`

<a id="invalid_argument.engine.space_delete_too_large"></a>
- `invalid_argument.engine.space_delete_too_large`

<a id="invalid_argument.engine.vector_artifact"></a>
- `invalid_argument.engine.vector_artifact`

<a id="invalid_argument.engine.vector_artifact_budget"></a>
- `invalid_argument.engine.vector_artifact_budget`

<a id="invalid_argument.engine.vector_batch"></a>
- `invalid_argument.engine.vector_batch`

<a id="invalid_argument.engine.vector_collection"></a>
- `invalid_argument.engine.vector_collection`

<a id="invalid_argument.engine.vector_collection_reserved"></a>
- `invalid_argument.engine.vector_collection_reserved`

<a id="invalid_argument.engine.embedding_model"></a>
- `invalid_argument.engine.embedding_model`

<a id="invalid_argument.engine.vector_dimension"></a>
- `invalid_argument.engine.vector_dimension`

<a id="invalid_argument.engine.vector_embedding"></a>
- `invalid_argument.engine.vector_embedding`

<a id="invalid_argument.engine.vector_filter"></a>
- `invalid_argument.engine.vector_filter`

<a id="invalid_argument.engine.vector_index_manifest"></a>
- `invalid_argument.engine.vector_index_manifest`

<a id="invalid_argument.engine.vector_key"></a>
- `invalid_argument.engine.vector_key`

<a id="invalid_argument.engine.vector_metadata"></a>
- `invalid_argument.engine.vector_metadata`

<a id="invalid_argument.engine.vector_metadata_field"></a>
- `invalid_argument.engine.vector_metadata_field`

<a id="invalid_argument.engine.vector_metadata_patch"></a>
- `invalid_argument.engine.vector_metadata_patch`

<a id="invalid_argument.engine.vector_metadata_too_large"></a>
- `invalid_argument.engine.vector_metadata_too_large`

<a id="invalid_argument.engine.vector_record"></a>
- `invalid_argument.engine.vector_record`

<a id="not_found.engine.branch"></a>
- `not_found.engine.branch`

<a id="not_found.engine.graph"></a>
- `not_found.engine.graph`

<a id="not_found.engine.graph_node"></a>
- `not_found.engine.graph_node`

<a id="not_found.engine.json_document"></a>
- `not_found.engine.json_document`

<a id="not_found.engine.persistence"></a>
- `not_found.engine.persistence`

<a id="not_found.engine.vector_collection"></a>
- `not_found.engine.vector_collection`

<a id="resource_exhausted.engine.graph_analytics_budget"></a>
- `resource_exhausted.engine.graph_analytics_budget`

<a id="resource_exhausted.engine.persistence_budget"></a>
- `resource_exhausted.engine.persistence_budget`

<a id="unavailable.engine.control_plane"></a>
- `unavailable.engine.control_plane`

<a id="unavailable.engine.persistence"></a>
- `unavailable.engine.persistence`

<a id="unavailable.engine.vector_artifacts"></a>
- `unavailable.engine.vector_artifacts`

<a id="unsupported.engine.graph_binding_cross_branch"></a>
- `unsupported.engine.graph_binding_cross_branch`

<a id="unsupported.engine.persistence_capability"></a>
- `unsupported.engine.persistence_capability`


### Executor Codes

<a id="failed_precondition.executor.hub_clone"></a>
- `failed_precondition.executor.hub_clone`

<a id="internal.executor.arrow"></a>
- `internal.executor.arrow`

<a id="internal.executor.unregistered_code"></a>
- `internal.executor.unregistered_code`

<a id="invalid_argument.executor.hub_branch"></a>
- `invalid_argument.executor.hub_branch`

<a id="invalid_argument.executor.hub_dataset"></a>
- `invalid_argument.executor.hub_dataset`

<a id="unsupported.executor.hub_feature_disabled"></a>
- `unsupported.executor.hub_feature_disabled`

<a id="invalid_argument.executor.hub_filter"></a>
- `invalid_argument.executor.hub_filter`

<a id="invalid_argument.executor.hub_since"></a>
- `invalid_argument.executor.hub_since`

<a id="invalid_argument.executor.hub_url"></a>
- `invalid_argument.executor.hub_url`

<a id="invalid_argument.executor.as_of_conflict"></a>
- `invalid_argument.executor.as_of_conflict`

<a id="invalid_argument.executor.arrow_base64"></a>
- `invalid_argument.executor.arrow_base64`

<a id="invalid_argument.executor.arrow_collection"></a>
- `invalid_argument.executor.arrow_collection`

<a id="invalid_argument.executor.arrow_embedding_type"></a>
- `invalid_argument.executor.arrow_embedding_type`

<a id="invalid_argument.executor.arrow_empty_export"></a>
- `invalid_argument.executor.arrow_empty_export`

<a id="invalid_argument.executor.arrow_encoding"></a>
- `invalid_argument.executor.arrow_encoding`

<a id="invalid_argument.executor.arrow_event"></a>
- `invalid_argument.executor.arrow_event`

<a id="unsupported.executor.arrow_feature_disabled"></a>
- `unsupported.executor.arrow_feature_disabled`

<a id="invalid_argument.executor.arrow_format"></a>
- `invalid_argument.executor.arrow_format`

<a id="invalid_argument.executor.arrow_graph"></a>
- `invalid_argument.executor.arrow_graph`

<a id="invalid_argument.executor.arrow_input_missing"></a>
- `invalid_argument.executor.arrow_input_missing`

<a id="invalid_argument.executor.arrow_json_key"></a>
- `invalid_argument.executor.arrow_json_key`

<a id="invalid_argument.executor.arrow_key_column"></a>
- `invalid_argument.executor.arrow_key_column`

<a id="invalid_argument.executor.arrow_non_finite_float"></a>
- `invalid_argument.executor.arrow_non_finite_float`

<a id="invalid_argument.executor.arrow_value_column"></a>
- `invalid_argument.executor.arrow_value_column`

<a id="invalid_argument.executor.arrow_vector_dimension"></a>
- `invalid_argument.executor.arrow_vector_dimension`

<a id="invalid_argument.executor.arrow_vector_key"></a>
- `invalid_argument.executor.arrow_vector_key`

<a id="invalid_argument.executor.batch_item"></a>
- `invalid_argument.executor.batch_item`

<a id="invalid_argument.executor.kv_batch_duplicate_key"></a>
- `invalid_argument.executor.kv_batch_duplicate_key`

<a id="invalid_argument.executor.json_batch_duplicate_key"></a>
- `invalid_argument.executor.json_batch_duplicate_key`

<a id="invalid_argument.executor.json_number"></a>
- `invalid_argument.executor.json_number`

<a id="invalid_argument.executor.wire_request"></a>
- `invalid_argument.executor.wire_request`

<a id="internal.executor.wire_response"></a>
- `internal.executor.wire_response`

<a id="unavailable.executor.ipc_transport"></a>
- `unavailable.executor.ipc_transport`

<a id="invalid_argument.executor.ipc_hello"></a>
- `invalid_argument.executor.ipc_hello`

<a id="resource_exhausted.executor.ipc_connections"></a>
- `resource_exhausted.executor.ipc_connections`

<a id="access_denied.executor.read_only_session"></a>
- `access_denied.executor.read_only_session`

<a id="unavailable.executor.ipc_deadline"></a>
- `unavailable.executor.ipc_deadline`

<a id="invalid_argument.executor.vector_batch_duplicate_key"></a>
- `invalid_argument.executor.vector_batch_duplicate_key`

<a id="invalid_argument.executor.limit"></a>
- `invalid_argument.executor.limit`

<a id="invalid_argument.executor.graph_analytics_budget"></a>
- `invalid_argument.executor.graph_analytics_budget`

<a id="invalid_argument.executor.vector_input"></a>
- `invalid_argument.executor.vector_input`

<a id="invalid_argument.executor.vector_dimension"></a>
- `invalid_argument.executor.vector_dimension`

<a id="invalid_argument.executor.vector_limit"></a>
- `invalid_argument.executor.vector_limit`

<a id="not_found.executor.hub_dataset"></a>
- `not_found.executor.hub_dataset`

<a id="not_found.executor.hub_resource"></a>
- `not_found.executor.hub_resource`

<a id="unavailable.executor.arrow_io"></a>
- `unavailable.executor.arrow_io`


### Inference Codes

<a id="inference.download_disabled"></a>
- `inference.download_disabled`

<a id="inference.download_failed"></a>
- `inference.download_failed`

<a id="inference.download_verification_failed"></a>
- `inference.download_verification_failed`

<a id="inference.invalid_request"></a>
- `inference.invalid_request`

<a id="inference.io_failure"></a>
- `inference.io_failure`

<a id="inference.local_runtime_failed"></a>
- `inference.local_runtime_failed`

<a id="inference.missing_api_key"></a>
- `inference.missing_api_key`

<a id="inference.missing_model"></a>
- `inference.missing_model`

<a id="inference.model_load_failed"></a>
- `inference.model_load_failed`

<a id="inference.provider_auth_failed"></a>
- `inference.provider_auth_failed`

<a id="inference.provider_malformed_response"></a>
- `inference.provider_malformed_response`

<a id="inference.provider_model_not_found"></a>
- `inference.provider_model_not_found`

<a id="inference.provider_quota_exhausted"></a>
- `inference.provider_quota_exhausted`

<a id="inference.provider_rate_limited"></a>
- `inference.provider_rate_limited`

<a id="inference.provider_timeout"></a>
- `inference.provider_timeout`

<a id="inference.provider_unavailable"></a>
- `inference.provider_unavailable`

<a id="inference.registry_corrupt"></a>
- `inference.registry_corrupt`

<a id="inference.unknown_model"></a>
- `inference.unknown_model`

<a id="inference.unsupported_operation"></a>
- `inference.unsupported_operation`

<a id="inference.unsupported_parameter"></a>
- `inference.unsupported_parameter`

<a id="inference.unsupported_provider"></a>
- `inference.unsupported_provider`

<a id="unavailable.executor.hub_transport"></a>
- `unavailable.executor.hub_transport`
