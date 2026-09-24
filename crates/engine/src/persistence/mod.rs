//! Storage persistence adapter boundary.

mod adapter;
mod fault;
mod key;
mod plan;
mod row;
mod scan_ordered;
mod space;

pub use adapter::MemoryBudgetSource;
pub(crate) use adapter::{
    close_summary_is_durable, PersistenceBranchCleanup, PersistenceBranchOutcome,
    PersistenceBranchParent, PersistenceBranchStatus, PersistenceBranchSummary,
    PersistenceImmutableSource, PersistenceOpenSummary, PersistenceOpenTarget, PersistenceReadRow,
    StoragePersistence,
};
pub(crate) use key::{
    branch_catalog_key, branch_default_key, branch_index_key, branch_pending_index_key,
    branch_pending_key, capability_registry_key, database_identity_key, decode_event_key_sequence,
    decode_graph_binding_key, decode_graph_edge_key, decode_graph_metadata_key,
    decode_graph_node_key, decode_graph_ontology_key, decode_graph_reverse_edge_key,
    decode_graph_type_index_key, decode_json_document_id, decode_json_index_name, decode_kv_key,
    decode_vector_collection_name, decode_vector_index_manifest_key, decode_vector_key,
    encode_event_key, encode_event_meta_key, encode_event_meta_space_prefix,
    encode_event_space_prefix, encode_event_type_index_key, encode_event_type_index_space_prefix,
    encode_graph_binding_key, encode_graph_binding_space_prefix,
    encode_graph_binding_target_prefix, encode_graph_edge_key, encode_graph_edge_prefix,
    encode_graph_edge_space_prefix, encode_graph_incoming_edge_prefix,
    encode_graph_incoming_edge_type_prefix, encode_graph_metadata_key,
    encode_graph_metadata_prefix, encode_graph_node_key, encode_graph_node_prefix,
    encode_graph_node_space_prefix, encode_graph_ontology_key, encode_graph_ontology_space_prefix,
    encode_graph_outgoing_edge_prefix, encode_graph_outgoing_edge_type_prefix,
    encode_graph_reverse_edge_key, encode_graph_reverse_edge_prefix,
    encode_graph_reverse_edge_space_prefix, encode_graph_type_index_graph_prefix,
    encode_graph_type_index_key, encode_graph_type_index_space_prefix,
    encode_graph_type_index_type_prefix, encode_json_index_entry_key,
    encode_json_index_entry_prefix, encode_json_index_entry_space_prefix,
    encode_json_index_meta_key, encode_json_index_meta_prefix, encode_json_key,
    encode_json_space_prefix, encode_kv_key, encode_kv_key_bytes, encode_kv_space_prefix,
    encode_vector_collection_entry_prefix, encode_vector_collection_key,
    encode_vector_collection_prefix, encode_vector_key, encode_vector_space_prefix,
    exclusive_after_key, local_instance_identity_key, migration_registry_key, next_prefix,
    remote_origin_key, reserved_space_key, space_catalog_key, space_index_key,
    storage_registry_key, vector_index_manifest_key, vector_index_manifest_prefix,
};
pub(crate) use plan::CommitPlan;
pub(crate) use row::{ReadSelector, RowAddress, RowMutation};
pub(crate) use scan_ordered::OrderedTextScan;
pub(crate) use space::RowClass;

#[cfg(any(test, feature = "testkit"))]
pub(crate) use space::row_class_storage_id_for_test;

#[cfg(any(test, feature = "testkit"))]
pub(crate) use fault::FaultOp;
#[cfg(any(test, feature = "testkit"))]
pub use fault::{RowCorruption, StorageFaultKind};
