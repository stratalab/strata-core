//! Canonical registry of engine error codes.
//!
//! Every engine error code is listed here exactly once, grouped by its stable
//! [`EngineErrorClass`]. This is the single source of truth that keeps codes
//! from drifting: every `EngineError` resolves its row here (`registry_row`),
//! a debug assertion rejects an unregistered code, and the tests below prove
//! the registry stays complete and free of dead entries.

use super::{CommitOutcomeStatus, EngineErrorClass, ErrorClass, RetryPolicy};

/// Public metadata for one stable error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorCodeRegistryEntry {
    /// Stable public code.
    pub code: &'static str,
    /// Public class exposed at command and SDK boundaries.
    pub class: ErrorClass,
    /// Reviewed retry policy for this code.
    pub retry_policy: RetryPolicy,
    /// Reviewed commit outcome for failures with this code.
    pub commit_outcome: CommitOutcomeStatus,
    /// Stable user-facing message template for generated docs and SDKs.
    pub message_template: &'static str,
    /// Actionable suggested fix shown to users.
    pub suggested_fix: &'static str,
    /// Stable docs slug. The public docs URL appends this slug as an anchor on
    /// the registry page.
    pub docs_slug: &'static str,
    /// Versioned schema identifier for details attached to this error.
    pub details_schema: &'static str,
}

/// One class and the codes that map to it.
struct CodeGroup {
    class: EngineErrorClass,
    codes: &'static [&'static str],
}

const INVALID_INPUT_CODES: &[&str] = &[
    "invalid_argument.engine.branch_catalog",
    "invalid_argument.engine.branch_delete",
    "invalid_argument.engine.branch_name",
    "invalid_argument.engine.branch_name_reserved",
    "invalid_argument.engine.branch_point",
    "invalid_argument.engine.config_key",
    "invalid_argument.engine.event_batch",
    "invalid_argument.engine.event_metadata",
    "invalid_argument.engine.event_payload",
    "invalid_argument.engine.event_payload_too_large",
    "invalid_argument.engine.event_record",
    "invalid_argument.engine.event_type",
    "invalid_argument.engine.graph_batch",
    "invalid_argument.engine.graph_binding",
    "invalid_argument.engine.graph_binding_record",
    "invalid_argument.engine.graph_edge_endpoint",
    "invalid_argument.engine.graph_edge_record",
    "invalid_argument.engine.graph_edge_type",
    "invalid_argument.engine.graph_edge_type_reserved",
    "invalid_argument.engine.graph_edge_weight",
    "invalid_argument.engine.graph_metadata",
    "invalid_argument.engine.graph_name",
    "invalid_argument.engine.graph_name_reserved",
    "invalid_argument.engine.graph_node_id",
    "invalid_argument.engine.graph_node_record",
    "invalid_argument.engine.graph_ontology_record",
    "invalid_argument.engine.graph_pagerank_options",
    "invalid_argument.engine.graph_personalization",
    "invalid_argument.engine.graph_properties",
    "invalid_argument.engine.graph_properties_too_large",
    "invalid_argument.engine.graph_property_name",
    "invalid_argument.engine.graph_type_hint",
    "invalid_argument.engine.graph_type_index_record",
    "invalid_argument.engine.graph_type_name",
    "invalid_argument.engine.graph_type_name_reserved",
    "invalid_argument.engine.json_array_too_large",
    "invalid_argument.engine.json_batch",
    "invalid_argument.engine.json_batch_duplicate_document",
    "invalid_argument.engine.json_document",
    "invalid_argument.engine.json_document_id",
    "invalid_argument.engine.json_document_too_deep",
    "invalid_argument.engine.json_document_too_large",
    "invalid_argument.engine.json_index",
    "invalid_argument.engine.json_index_name",
    "invalid_argument.engine.json_index_name_reserved",
    "invalid_argument.engine.json_path",
    "invalid_argument.engine.json_path_not_found",
    "invalid_argument.engine.json_path_too_long",
    "invalid_argument.engine.json_path_type",
    "invalid_argument.engine.json_value",
    "invalid_argument.engine.kv_batch",
    "invalid_argument.engine.kv_batch_duplicate_key",
    "invalid_argument.engine.kv_key",
    "invalid_argument.engine.persistence",
    "invalid_argument.engine.product_space",
    "invalid_argument.engine.product_space_reserved",
    "invalid_argument.engine.space_catalog",
    "invalid_argument.engine.space_delete_default",
    "invalid_argument.engine.space_delete_too_large",
    "invalid_argument.engine.vector_artifact",
    "invalid_argument.engine.vector_artifact_budget",
    "invalid_argument.engine.vector_batch",
    "invalid_argument.engine.vector_collection",
    "invalid_argument.engine.vector_collection_reserved",
    "invalid_argument.engine.embedding_model",
    "invalid_argument.engine.vector_dimension",
    "invalid_argument.engine.vector_embedding",
    "invalid_argument.engine.vector_filter",
    "invalid_argument.engine.vector_index_manifest",
    "invalid_argument.engine.vector_key",
    "invalid_argument.engine.vector_metadata",
    "invalid_argument.engine.vector_metadata_field",
    "invalid_argument.engine.vector_metadata_patch",
    "invalid_argument.engine.vector_metadata_too_large",
    "invalid_argument.engine.vector_record",
];

const NOT_FOUND_CODES: &[&str] = &[
    "not_found.engine.branch",
    "not_found.engine.graph",
    "not_found.engine.graph_node",
    "not_found.engine.json_document",
    "not_found.engine.persistence",
    "not_found.engine.vector_collection",
];

const ALREADY_EXISTS_CODES: &[&str] = &["already_exists.engine.persistence"];

const CONFLICT_CODES: &[&str] = &[
    "conflict.engine.artifact_import",
    "already_exists.engine.branch",
    "already_exists.engine.graph",
    "already_exists.engine.json_document",
    "already_exists.engine.json_index",
    "already_exists.engine.vector_collection",
    "conflict.engine.branch_generation",
    "conflict.engine.persistence",
    "conflict.engine.promotion",
    "failed_precondition.engine.graph_negative_weight",
    "failed_precondition.engine.graph_ontology_edge_type",
    "failed_precondition.engine.graph_ontology_endpoint_type",
    "failed_precondition.engine.graph_ontology_freeze",
    "failed_precondition.engine.graph_ontology_frozen",
    "failed_precondition.engine.graph_ontology_node_type",
    "failed_precondition.engine.graph_ontology_required_property",
    "failed_precondition.engine.embedding_model_mismatch",
    "failed_precondition.engine.embedding_model_missing",
    "failed_precondition.engine.space_not_empty",
];

const HISTORY_UNAVAILABLE_CODES: &[&str] = &["history_unavailable.engine.persistence_history"];

const UNSUPPORTED_CODES: &[&str] = &[
    "unsupported.engine.persistence_capability",
    "unsupported.engine.graph_binding_cross_branch",
];

const RESOURCE_EXHAUSTED_CODES: &[&str] = &[
    "resource_exhausted.engine.graph_analytics_budget",
    "resource_exhausted.engine.persistence_budget",
];

const CORRUPTION_CODES: &[&str] = &[
    "corruption.engine.artifact_payload",
    "corruption.engine.persistence_recovery",
    "data_loss.engine.branch_catalog",
    "data_loss.engine.branch_id",
    "data_loss.engine.control_name",
    "data_loss.engine.control_plane",
    "data_loss.engine.control_plane_missing",
    "data_loss.engine.event_index_key",
    "data_loss.engine.event_key",
    "data_loss.engine.event_metadata",
    "data_loss.engine.event_record",
    "data_loss.engine.graph_binding_key",
    "data_loss.engine.graph_binding_record",
    "data_loss.engine.graph_edge_key",
    "data_loss.engine.graph_edge_record",
    "data_loss.engine.graph_index",
    "data_loss.engine.graph_key",
    "data_loss.engine.graph_metadata",
    "data_loss.engine.graph_node_key",
    "data_loss.engine.graph_node_record",
    "data_loss.engine.graph_ontology_record",
    "data_loss.engine.graph_reverse_edge_key",
    "data_loss.engine.graph_type_index_key",
    "data_loss.engine.graph_type_index_record",
    "data_loss.engine.json_document",
    "data_loss.engine.json_index",
    "data_loss.engine.json_index_key",
    "data_loss.engine.json_key",
    "data_loss.engine.kv_key",
    "data_loss.engine.kv_value",
    "data_loss.engine.space_catalog",
    "data_loss.engine.vector_artifact",
    "data_loss.engine.vector_artifacts",
    "data_loss.engine.vector_collection",
    "data_loss.engine.vector_collection_key",
    "data_loss.engine.vector_index_manifest",
    "data_loss.engine.vector_index_manifest_key",
    "data_loss.engine.vector_key",
    "data_loss.engine.vector_record",
];

const AMBIGUOUS_COMMIT_CODES: &[&str] = &["ambiguous_commit.engine.persistence"];

const UNAVAILABLE_CODES: &[&str] = &[
    "failed_precondition.engine.persistence",
    "unavailable.engine.control_plane",
    "unavailable.engine.persistence",
    "unavailable.engine.vector_artifacts",
];

const INCOMPATIBLE_LAYOUT_CODES: &[&str] = &[
    "failed_precondition.engine.branch_status",
    "failed_precondition.engine.capability_registry",
    "failed_precondition.engine.control_payload_version",
    "failed_precondition.engine.default_branch",
    "failed_precondition.engine.layout_version",
    "failed_precondition.engine.migration_registry",
    "failed_precondition.engine.storage_registry",
    "failed_precondition.engine.vector_artifact",
    "failed_precondition.engine.vector_index_manifest",
];

const CLOSED_RUNTIME_CODES: &[&str] = &["failed_precondition.engine.runtime_closed"];

const INTERNAL_CODES: &[&str] = &["internal.engine.persistence"];

const GROUPS: &[CodeGroup] = &[
    CodeGroup {
        class: EngineErrorClass::InvalidInput,
        codes: INVALID_INPUT_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::NotFound,
        codes: NOT_FOUND_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Conflict,
        codes: ALREADY_EXISTS_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Conflict,
        codes: CONFLICT_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::NotFound,
        codes: HISTORY_UNAVAILABLE_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Unavailable,
        codes: UNSUPPORTED_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Unavailable,
        codes: RESOURCE_EXHAUSTED_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Corruption,
        codes: CORRUPTION_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::AmbiguousCommit,
        codes: AMBIGUOUS_COMMIT_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Unavailable,
        codes: UNAVAILABLE_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::IncompatibleLayout,
        codes: INCOMPATIBLE_LAYOUT_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::ClosedRuntime,
        codes: CLOSED_RUNTIME_CODES,
    },
    CodeGroup {
        class: EngineErrorClass::Internal,
        codes: INTERNAL_CODES,
    },
];

/// Returns the registered class for `code`, or `None` if it is not registered.
pub(crate) fn class_for_code(code: &str) -> Option<EngineErrorClass> {
    GROUPS
        .iter()
        .find(|group| group.codes.contains(&code))
        .map(|group| group.class)
}

/// Returns the public registry entry for `code`.
#[must_use]
pub fn error_code_registry_entry(code: &str) -> Option<ErrorCodeRegistryEntry> {
    GROUPS
        .iter()
        .find_map(|group| {
            group
                .codes
                .iter()
                .copied()
                .find(|registered| *registered == code)
                .map(|registered| (group.class, registered))
        })
        .map(|(legacy_class, registered)| registry_entry(legacy_class, registered))
}

/// The row a live `EngineError` for `code` carries, with the legacy class the
/// code is grouped under. The registry is the single authority for a code's
/// class, retry policy, commit outcome and suggested fix (#3280): every
/// constructor resolves its code here and has no channel to override the row.
///
/// An unregistered code is a programming error: `every_source_code_is_registered`
/// keeps one out of the engine tree (the named constructors are `pub`, but
/// their only out-of-crate caller passes a registered literal), and debug
/// builds fail here. A release build classifies it as an internal failure
/// rather than panic inside a database.
pub(crate) fn registry_row(code: &'static str) -> (EngineErrorClass, ErrorCodeRegistryEntry) {
    let class = class_for_code(code);
    debug_assert!(
        class.is_some(),
        "engine error code `{code}` is unregistered"
    );
    // Rationale: see above — unreachable for any code literal in the tree.
    let class = class.unwrap_or(EngineErrorClass::Internal);
    (class, registry_entry(class, code))
}

/// Returns every public engine error-code registry entry.
pub fn error_code_registry_entries() -> impl Iterator<Item = ErrorCodeRegistryEntry> {
    GROUPS.iter().flat_map(|group| {
        group
            .codes
            .iter()
            .copied()
            .map(move |code| registry_entry(group.class, code))
    })
}

fn registry_entry(legacy_class: EngineErrorClass, code: &'static str) -> ErrorCodeRegistryEntry {
    ErrorCodeRegistryEntry {
        code,
        class: public_class_for_code(legacy_class, code),
        retry_policy: retry_policy_for_code(code, legacy_class),
        commit_outcome: commit_outcome_for_code(code, legacy_class),
        message_template: message_template_for_code(code, legacy_class),
        suggested_fix: suggested_fix_for_code(code, legacy_class),
        docs_slug: code,
        details_schema: details_schema_for_code(code),
    }
}

fn public_class_for_code(legacy_class: EngineErrorClass, code: &str) -> ErrorClass {
    match code.split('.').next() {
        Some("not_found") => ErrorClass::NotFound,
        Some("already_exists") => ErrorClass::AlreadyExists,
        Some("invalid_argument") => ErrorClass::InvalidArgument,
        Some("failed_precondition") => ErrorClass::FailedPrecondition,
        Some("access_denied") => ErrorClass::AccessDenied,
        Some("conflict") => ErrorClass::Conflict,
        Some("ambiguous_commit") => ErrorClass::AmbiguousCommit,
        Some("history_unavailable") => ErrorClass::HistoryUnavailable,
        Some("unsupported") => ErrorClass::Unsupported,
        Some("resource_exhausted") => ErrorClass::ResourceExhausted,
        Some("unavailable") => ErrorClass::Unavailable,
        Some("io") => ErrorClass::Io,
        Some("corruption") => ErrorClass::Corruption,
        Some("data_loss") => ErrorClass::DataLoss,
        Some("serialization") => ErrorClass::Serialization,
        Some("internal") => ErrorClass::Internal,
        _ => match legacy_class {
            EngineErrorClass::InvalidInput => ErrorClass::InvalidArgument,
            EngineErrorClass::NotFound => ErrorClass::NotFound,
            EngineErrorClass::Conflict => ErrorClass::Conflict,
            EngineErrorClass::Unavailable => ErrorClass::Unavailable,
            EngineErrorClass::AmbiguousCommit => ErrorClass::AmbiguousCommit,
            EngineErrorClass::IncompatibleLayout | EngineErrorClass::ClosedRuntime => {
                ErrorClass::FailedPrecondition
            }
            EngineErrorClass::Corruption => ErrorClass::Corruption,
            EngineErrorClass::Internal => ErrorClass::Internal,
        },
    }
}

fn retry_policy_for_code(code: &str, class: EngineErrorClass) -> RetryPolicy {
    match code {
        "conflict.engine.branch_generation"
        | "conflict.engine.persistence"
        | "failed_precondition.engine.persistence"
        | "history_unavailable.engine.persistence_history"
        | "resource_exhausted.engine.persistence_budget"
        | "unsupported.engine.persistence_capability" => return RetryPolicy::AfterStateChange,
        "unavailable.engine.persistence" => return RetryPolicy::SameRequest,
        _ => {}
    }
    match class {
        EngineErrorClass::Unavailable => RetryPolicy::AfterStateChange,
        EngineErrorClass::AmbiguousCommit | EngineErrorClass::Internal => RetryPolicy::Unknown,
        _ => RetryPolicy::Never,
    }
}

fn commit_outcome_for_code(code: &str, class: EngineErrorClass) -> CommitOutcomeStatus {
    match code {
        "conflict.engine.branch_generation"
        | "conflict.engine.persistence"
        | "failed_precondition.engine.persistence"
        | "resource_exhausted.engine.persistence_budget" => {
            return CommitOutcomeStatus::DefinitelyNotCommitted;
        }
        _ => {}
    }
    match class {
        EngineErrorClass::InvalidInput | EngineErrorClass::ClosedRuntime => {
            CommitOutcomeStatus::NotStarted
        }
        EngineErrorClass::Conflict => CommitOutcomeStatus::NotStarted,
        EngineErrorClass::AmbiguousCommit => CommitOutcomeStatus::MaybeCommitted,
        _ => CommitOutcomeStatus::NotApplicable,
    }
}

/// Message text for `not_found.*` / `already_exists.*` codes.
///
/// These are matched before the capability-keyword arms so a not-found or
/// already-exists failure reports its actual class semantics instead of the
/// "…request is invalid" phrasing that only suits `invalid_argument` /
/// `corruption` codes (see finding U19).
///
/// The embedding-model codes are here for a different reason: no keyword arm
/// matches `embedding_model`, so without an entry each fell through to the
/// class-generic sentence, and a caller reading only `message_template` /
/// `suggested_fix` — the docs page, the MCP tool description — was told to
/// "reload current state and retry", which is not a thing that fixes it.
fn class_prefixed_message(code: &str) -> Option<&'static str> {
    Some(match code {
        "failed_precondition.engine.embedding_model_mismatch" => {
            "The embedding model does not match the one this vector collection records."
        }
        "failed_precondition.engine.embedding_model_missing" => {
            "This vector collection records no embedding model, so text cannot be embedded for it."
        }
        "invalid_argument.engine.embedding_model" => "The embedding model id is invalid.",
        "not_found.engine.branch" => "The requested branch was not found.",
        "not_found.engine.graph" => "The requested graph was not found.",
        "not_found.engine.graph_node" => "The requested graph node was not found.",
        "not_found.engine.json_document" => "The requested JSON document was not found.",
        "not_found.engine.vector_collection" => "The requested vector collection was not found.",
        "not_found.engine.persistence" => "The requested resource was not found.",
        "already_exists.engine.branch" => "A branch with this name already exists.",
        "already_exists.engine.graph" => "A graph with this name already exists.",
        "already_exists.engine.json_document" => "A JSON document already exists at this id.",
        "already_exists.engine.json_index" => "A JSON index with this name already exists.",
        "already_exists.engine.vector_collection" => {
            "A vector collection with this name already exists."
        }
        "already_exists.engine.persistence" => "The resource already exists.",
        _ => return None,
    })
}

/// Suggested-fix text for `not_found.*` / `already_exists.*` codes (see U19),
/// and for the embedding-model codes, whose fix is a specific command.
fn class_prefixed_suggested_fix(code: &str) -> Option<&'static str> {
    Some(match code {
        "failed_precondition.engine.embedding_model_mismatch" => {
            "Embed with the model the collection records (see `vector collection stats`), or \
             create a separate collection for the other model."
        }
        "failed_precondition.engine.embedding_model_missing" => {
            "Declare the collection's model with `vector collection set-embedding-model \
             <collection> <model>`, or pass a vector instead of text."
        }
        "invalid_argument.engine.embedding_model" => {
            "Use a non-empty model id such as `openai:text-embedding-3-small` or `miniLM`."
        }
        "not_found.engine.branch" => {
            "List branches or use the default branch, then retry with an existing branch name."
        }
        "not_found.engine.graph" => "List graphs, then retry with an existing graph name.",
        "not_found.engine.graph_node" => {
            "List the graph's nodes, then retry with an existing node id."
        }
        "not_found.engine.json_document" => {
            "List documents or create the document before reading it."
        }
        "not_found.engine.vector_collection" => {
            "List vector collections or create the collection before issuing vector operations."
        }
        "not_found.engine.persistence" => {
            "List the requested resource type, then retry with an existing resource id."
        }
        // #3275: a pre-V1 directory is rejected, never migrated (hard rule 41),
        // so this row must not share the compatible-version arm below.
        "failed_precondition.engine.layout_version" => {
            "Point Strata at an empty directory or an existing V1 database; pre-V1 databases \
             are not migrated."
        }
        "history_unavailable.engine.persistence_history" => {
            "Request history inside the retained window."
        }
        "already_exists.engine.branch" => {
            "Choose a different branch name or delete the existing branch first."
        }
        "already_exists.engine.graph" => {
            "Choose a different graph name or delete the existing graph first."
        }
        "already_exists.engine.json_document" => {
            "Update the existing document, or use a different document id."
        }
        "already_exists.engine.json_index" => {
            "Drop the existing index or choose a different index name."
        }
        "already_exists.engine.vector_collection" => {
            "Use the existing collection, or choose a different collection name."
        }
        // Fires only for a storage branch the control plane does not know
        // (`BranchAlreadyExists` is storage's sole `AlreadyExists` variant);
        // retrying cannot help under `Never`, so the remedy is the resource one.
        "already_exists.engine.persistence" => {
            "Use the existing branch or choose a different branch name."
        }
        _ => return None,
    })
}

fn message_template_for_code(code: &str, class: EngineErrorClass) -> &'static str {
    if let Some(message) = class_prefixed_message(code) {
        return message;
    }
    if code.contains("branch_name") {
        "The branch name is invalid."
    } else if code.contains("product_space") || code.contains(".space_") {
        "The requested space operation cannot be completed."
    } else if code.contains("kv_batch_duplicate_key") {
        "The KV batch contains duplicate keys."
    } else if code.contains(".kv_") {
        "The KV request is invalid."
    } else if code.contains(".json_path") {
        "The JSON path is invalid or cannot be applied to the selected value."
    } else if code.contains(".json_document") || code.contains(".json_value") {
        "The JSON document request is invalid."
    } else if code.contains(".json_index") {
        "The JSON index request is invalid."
    } else if code.contains(".vector_artifact") || code.contains(".vector_index_manifest") {
        "The vector index artifact or manifest cannot be used."
    } else if code.contains(".vector_collection") && code.starts_with("not_found.") {
        "The requested vector collection was not found."
    } else if code.contains(".vector_") {
        "The vector request is invalid."
    } else if code.contains(".event_") {
        "The event request is invalid."
    } else if code.contains("graph_binding_cross_branch") {
        "Cross-branch graph relationship bindings are not supported."
    } else if code.contains(".graph_") {
        "The graph request is invalid."
    } else if code.contains("persistence_budget") {
        "The operation exceeded a configured persistence resource budget."
    } else if code.contains("persistence_capability") {
        "The selected persistence backend does not support the requested capability."
    } else if code.contains("runtime_closed") {
        "The runtime is closed."
    } else if code.contains("layout")
        || code.contains("registry")
        || code.contains("control_payload_version")
    {
        "The database layout is incompatible with this runtime."
    } else {
        match class {
            EngineErrorClass::InvalidInput => "The request contains invalid input.",
            EngineErrorClass::NotFound => "The requested resource was not found.",
            EngineErrorClass::Conflict => "The request conflicts with current state.",
            EngineErrorClass::Unavailable => "The required engine capability is unavailable.",
            EngineErrorClass::AmbiguousCommit => "The commit outcome could not be proven.",
            EngineErrorClass::IncompatibleLayout => {
                "The stored layout is incompatible with this runtime."
            }
            EngineErrorClass::Corruption => "Stored data failed validation.",
            EngineErrorClass::ClosedRuntime => "The runtime is closed.",
            EngineErrorClass::Internal => "An internal engine error occurred.",
        }
    }
}

/// The single authority for a code's remediation hint: what `strata agents
/// errors` documents and what a live `EngineError` carries (#3237). Reached
/// only through the row (`registry_entry`); no site calls it directly (#3280).
fn suggested_fix_for_code(code: &str, class: EngineErrorClass) -> &'static str {
    if let Some(fix) = class_prefixed_suggested_fix(code) {
        return fix;
    }
    if code.contains("branch_name") {
        "Use a non-empty branch name that does not use reserved prefixes or invalid characters."
    } else if code.contains("product_space") || code.contains(".space_") {
        "Use an existing non-reserved space, or delete/move contained data before deleting the space."
    } else if code.contains("kv_batch_duplicate_key") {
        "Remove duplicate keys from the batch so each key appears once."
    } else if code.contains(".kv_") {
        "Use non-empty KV keys and keep batch structure within the documented limits."
    } else if code.contains(".json_path") {
        "Use a valid JSON path that points to an existing value of the expected type."
    } else if code.contains(".json_document") || code.contains(".json_value") {
        "Use a valid JSON document id and a JSON value within the configured size and depth limits."
    } else if code.contains(".json_index") {
        "Use a non-reserved JSON index name and definition compatible with the indexed field."
    } else if code.contains(".vector_artifact") || code.contains(".vector_index_manifest") {
        "Rebuild vector index artifacts or fall back to exact search before retrying indexed queries."
    } else if code.contains(".vector_collection") && code.starts_with("not_found.") {
        "List vector collections or create the collection before issuing vector operations."
    } else if code.contains(".vector_") {
        "Use the collection dimension, valid vector keys, and metadata/filter values supported by the collection."
    } else if code.contains(".event_") {
        "Use a valid event type, payload, and metadata within the documented event limits."
    } else if code.contains("graph_binding_cross_branch") {
        "Bind graph relationships to targets on the node's own branch; cross-branch bindings are not supported in V1."
    } else if code.contains(".graph_") {
        "Use valid graph, node, edge, and binding identifiers and keep graph properties within limits."
    } else if code.contains("persistence_budget") {
        "Reduce memory or disk pressure, lower the workload size, or raise the configured resource budget."
    } else if code.contains("persistence_capability") {
        "Open the database with a backend that supports the requested persistence capability."
    } else if code.contains("runtime_closed") {
        "Open a new database handle before issuing more commands."
    } else if code.contains("layout")
        || code.contains("registry")
        || code.contains("control_payload_version")
    {
        "Open the database with a compatible Strata version or run the required migration."
    } else {
        match class {
            EngineErrorClass::InvalidInput => {
                "Correct the invalid field named by the error message and retry the operation."
            }
            EngineErrorClass::NotFound => {
                "List the requested resource type, then retry with an existing resource id."
            }
            EngineErrorClass::Conflict => {
                "Reload current state and retry against the latest branch, space, or collection version."
            }
            EngineErrorClass::Unavailable => {
                "Retry after the required backend, control plane, or persistence capability is available."
            }
            EngineErrorClass::AmbiguousCommit => {
                "Re-open or inspect database state before assuming whether the write committed."
            }
            EngineErrorClass::IncompatibleLayout => {
                "Open the database with a compatible Strata version or run the required migration."
            }
            EngineErrorClass::Corruption => {
                "Stop writing to the database and inspect recovery diagnostics before continuing."
            }
            EngineErrorClass::ClosedRuntime => {
                "Open a new database handle before issuing more commands."
            }
            EngineErrorClass::Internal => {
                "Capture the reference id and report this as a Strata internal error."
            }
        }
    }
}

fn details_schema_for_code(code: &str) -> &'static str {
    if code.contains(".vector_") {
        "strata.error.details.vector.v1"
    } else if code.contains(".json_") {
        "strata.error.details.json.v1"
    } else if code.contains(".kv_") {
        "strata.error.details.kv.v1"
    } else if code.contains(".event_") {
        "strata.error.details.event.v1"
    } else if code.contains(".graph_") {
        "strata.error.details.graph.v1"
    } else if code.contains(".branch") {
        "strata.error.details.branch.v1"
    } else if code.contains(".space") {
        "strata.error.details.space.v1"
    } else if code.contains("persistence") {
        "strata.error.details.persistence.v1"
    } else if code.contains("control") || code.contains("registry") || code.contains("layout") {
        "strata.error.details.control_plane.v1"
    } else {
        "strata.error.details.common.v1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    const CLASS_PREFIXES: &[&str] = &[
        "invalid_argument",
        "not_found",
        "already_exists",
        "conflict",
        "failed_precondition",
        "history_unavailable",
        "unsupported",
        "resource_exhausted",
        "data_loss",
        "corruption",
        "ambiguous_commit",
        "unavailable",
        "internal",
    ];

    fn registered_codes() -> Vec<(&'static str, EngineErrorClass)> {
        GROUPS
            .iter()
            .flat_map(|group| group.codes.iter().map(move |code| (*code, group.class)))
            .collect()
    }

    fn is_code_char(byte: u8) -> bool {
        byte == b'_' || byte.is_ascii_lowercase()
    }

    /// Extracts every `<class>.engine.<detail>` string-literal code from a body.
    fn extract_codes(body: &str) -> Vec<String> {
        let bytes = body.as_bytes();
        let needle = b".engine.";
        let mut found = Vec::new();
        let mut index = 0;
        while index + needle.len() <= bytes.len() {
            if &bytes[index..index + needle.len()] != needle {
                index += 1;
                continue;
            }
            let mut prefix_start = index;
            while prefix_start > 0 && is_code_char(bytes[prefix_start - 1]) {
                prefix_start -= 1;
            }
            let mut detail_end = index + needle.len();
            while detail_end < bytes.len() && is_code_char(bytes[detail_end]) {
                detail_end += 1;
            }
            let has_prefix = prefix_start < index;
            let has_detail = detail_end > index + needle.len();
            let delimited = prefix_start > 0
                && bytes[prefix_start - 1] == b'"'
                && detail_end < bytes.len()
                && bytes[detail_end] == b'"';
            let known_prefix = has_prefix && CLASS_PREFIXES.contains(&&body[prefix_start..index]);
            if has_detail && delimited && known_prefix {
                found.push(body[prefix_start..detail_end].to_string());
            }
            index = detail_end;
        }
        found
    }

    fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("read source directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// All codes emitted as string literals anywhere under `src`, except this
    /// registry file itself (so the reverse test cannot self-justify).
    fn scanned_source_codes() -> BTreeSet<String> {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let registry = src.join("diagnostics").join("registry.rs");
        let mut files = Vec::new();
        collect_rs_files(&src, &mut files);
        let mut codes = BTreeSet::new();
        for file in files {
            if file == registry {
                continue;
            }
            let body = fs::read_to_string(&file).expect("read source file");
            for code in extract_codes(&body) {
                codes.insert(code);
            }
        }
        codes
    }

    #[test]
    fn public_class_for_code_splits_data_loss_from_corruption() {
        // #2749: the public wire class segment IS the code's own class. A
        // `data_loss.*` code must surface `DataLoss`, distinct from
        // `corruption.*`, so an SDK can tell unrecoverable loss from a detected
        // integrity violation.
        assert_eq!(
            public_class_for_code(EngineErrorClass::Corruption, "data_loss.engine.kv_value"),
            ErrorClass::DataLoss,
        );
        assert_eq!(
            public_class_for_code(
                EngineErrorClass::Corruption,
                "corruption.engine.persistence_recovery",
            ),
            ErrorClass::Corruption,
        );
        // Direction control: the prefix wins over the legacy class. Feeding a
        // non-corruption legacy class proves each prefix arm is load-bearing —
        // deleting it would fall through to the legacy fallback and change the
        // result.
        assert_eq!(
            public_class_for_code(
                EngineErrorClass::Internal,
                "corruption.engine.persistence_recovery",
            ),
            ErrorClass::Corruption,
        );
        assert_eq!(
            public_class_for_code(EngineErrorClass::Internal, "data_loss.engine.kv_value"),
            ErrorClass::DataLoss,
        );
        // The `io` arm rides into the same diff hunk as the split; cover it so
        // its deletion is caught. A `.engine.`-free string keeps it out of the
        // source-registration scanner.
        assert_eq!(
            public_class_for_code(EngineErrorClass::Internal, "io.probe"),
            ErrorClass::Io,
        );
    }

    /// Hand plants anchoring the parity sweep in `error.rs`: that sweep proves
    /// runtime == registry, these prove the registry side is per-code rather
    /// than class-generic, so the pair cannot pass by both being generic.
    #[test]
    fn suggested_fix_for_code_prefers_per_code_rows_over_class_fallback() {
        assert!(
            suggested_fix_for_code("already_exists.engine.branch", EngineErrorClass::Conflict)
                .contains("different branch name"),
            "per-code row wins over the class fallback"
        );
        assert!(
            suggested_fix_for_code(
                "invalid_argument.engine.branch_name",
                EngineErrorClass::InvalidInput,
            )
            .contains("non-empty branch name"),
            "keyword row wins over the class fallback"
        );
        // Codes without a row of their own fall back to the class hint, which
        // differs per class.
        assert_ne!(
            suggested_fix_for_code(
                "unavailable.engine.persistence",
                EngineErrorClass::Unavailable
            ),
            suggested_fix_for_code("internal.engine.persistence", EngineErrorClass::Internal),
        );
    }

    /// #3275: two rows were hidden behind adapter site text and carried the
    /// wrong remedy. `layout_version` rejects a pre-V1 directory, which is never
    /// migrated (hard rule 41), so its row must not share the "run the required
    /// migration" arm of the control-plane registry codes; `persistence_history`
    /// is a retained-window error, not the `NotFound` class fallback.
    #[test]
    fn pre_v1_layout_and_history_window_rows_carry_their_own_remedy() {
        let layout = suggested_fix_for_code(
            "failed_precondition.engine.layout_version",
            EngineErrorClass::IncompatibleLayout,
        );
        assert!(
            !layout.contains("migration"),
            "pre-V1 databases are not migrated: {layout}"
        );
        assert!(
            layout.contains("pre-V1"),
            "names the pre-V1 rejection: {layout}"
        );
        // The control-plane registry codes keep the compatible-version arm.
        assert!(suggested_fix_for_code(
            "failed_precondition.engine.migration_registry",
            EngineErrorClass::IncompatibleLayout,
        )
        .contains("compatible Strata version"));

        let history = suggested_fix_for_code(
            "history_unavailable.engine.persistence_history",
            EngineErrorClass::NotFound,
        );
        assert!(
            history.contains("retained"),
            "a history-window remedy, not the NotFound fallback: {history}"
        );
        assert_ne!(
            history,
            suggested_fix_for_code("not_found.engine.persistence", EngineErrorClass::NotFound)
        );
    }

    /// `already_exists.engine.persistence` is an `already_exists` row under
    /// `Never`: its remedy names the existing resource, not a retry (#3278).
    #[test]
    fn persistence_already_exists_row_carries_the_resource_remedy_not_a_retry() {
        let code = "already_exists.engine.persistence";
        let class = class_for_code(code).expect("registered");
        assert_eq!(retry_policy_for_code(code, class), RetryPolicy::Never);
        let fix = suggested_fix_for_code(code, class);
        assert!(!fix.contains("retry"), "a retry remedy under Never: {fix}");
        assert!(fix.contains("name"), "names the existing resource: {fix}");
    }

    #[test]
    fn registry_has_no_duplicate_codes() {
        let mut seen = BTreeSet::new();
        for (code, _class) in registered_codes() {
            assert!(seen.insert(code), "duplicate registry code: {code}");
        }
    }

    #[test]
    fn registered_classes_follow_prefix_convention() {
        for (code, class) in registered_codes() {
            let prefix = code.split('.').next().expect("code has a prefix");
            let expected = match prefix {
                "invalid_argument" => Some(EngineErrorClass::InvalidInput),
                "not_found" | "history_unavailable" => Some(EngineErrorClass::NotFound),
                "already_exists" | "conflict" => Some(EngineErrorClass::Conflict),
                "corruption" | "data_loss" => Some(EngineErrorClass::Corruption),
                "ambiguous_commit" => Some(EngineErrorClass::AmbiguousCommit),
                "unavailable" | "unsupported" | "resource_exhausted" => {
                    Some(EngineErrorClass::Unavailable)
                }
                "internal" => Some(EngineErrorClass::Internal),
                // These V1 classes are represented by the closest legacy class
                // until `EngineErrorClass` is retired as the public surface.
                // `failed_precondition` intentionally maps to several classes.
                "failed_precondition" => None,
                other => panic!("unknown class prefix: {other}"),
            };
            if let Some(expected) = expected {
                assert_eq!(class, expected, "code {code} has an unexpected class");
            } else {
                assert!(
                    matches!(
                        class,
                        EngineErrorClass::IncompatibleLayout
                            | EngineErrorClass::Conflict
                            | EngineErrorClass::Unavailable
                            | EngineErrorClass::ClosedRuntime
                    ),
                    "failed_precondition code {code} has unexpected class {class:?}"
                );
            }
        }
    }

    #[test]
    fn not_found_and_already_exists_codes_get_class_appropriate_guidance() {
        // U19: these must not render the invalid-argument "…request is invalid"
        // phrasing or its "correct the invalid field" fix.
        for code in [
            "not_found.engine.json_document",
            "already_exists.engine.json_document",
            "already_exists.engine.json_index",
            "already_exists.engine.vector_collection",
        ] {
            let entry = error_code_registry_entry(code).expect("code registered");
            assert!(
                !entry.message_template.contains("is invalid"),
                "{code} still reports an invalid-argument message: {}",
                entry.message_template
            );
            assert!(
                !entry.suggested_fix.contains("invalid field")
                    && !entry.suggested_fix.contains("within the configured size"),
                "{code} still reports invalid-argument remediation: {}",
                entry.suggested_fix
            );
        }
        assert_eq!(
            error_code_registry_entry("not_found.engine.json_document")
                .unwrap()
                .message_template,
            "The requested JSON document was not found."
        );
        assert_eq!(
            error_code_registry_entry("already_exists.engine.json_index")
                .unwrap()
                .message_template,
            "A JSON index with this name already exists."
        );
    }

    #[test]
    fn every_source_code_is_registered() {
        let registered: BTreeSet<&'static str> = registered_codes()
            .into_iter()
            .map(|(code, _)| code)
            .collect();
        let unregistered: Vec<String> = scanned_source_codes()
            .into_iter()
            .filter(|code| !registered.contains(code.as_str()))
            .collect();
        assert!(
            unregistered.is_empty(),
            "source emits unregistered engine error codes: {unregistered:?}"
        );
    }

    #[test]
    fn every_registered_code_appears_in_source() {
        let scanned = scanned_source_codes();
        let dead: Vec<&str> = registered_codes()
            .into_iter()
            .map(|(code, _)| code)
            .filter(|code| !scanned.contains(*code))
            .collect();
        assert!(
            dead.is_empty(),
            "registry has codes not emitted by source: {dead:?}"
        );
    }
}
