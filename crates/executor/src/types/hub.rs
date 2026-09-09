use std::collections::BTreeMap;

#[cfg(feature = "hub")]
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{Deserialize, Serialize};

/// Dataset-list sort key accepted by StrataHub V1.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HubDatasetSort {
    /// Most-downloaded datasets first.
    Downloads,
    /// Most-recently-updated datasets first.
    Recent,
    /// Dataset-name lexicographic order.
    Name,
    /// Largest datasets first.
    Size,
}

#[cfg(feature = "hub")]
impl From<HubDatasetSort> for strata_hub::SortKey {
    fn from(value: HubDatasetSort) -> Self {
        match value {
            HubDatasetSort::Downloads => Self::Downloads,
            HubDatasetSort::Recent => Self::Recent,
            HubDatasetSort::Name => Self::Name,
            HubDatasetSort::Size => Self::Size,
        }
    }
}

/// Hub capability advertisement returned by `hub.info`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubInfo {
    /// Protocol version advertised by the hub.
    pub protocol_version: String,
    /// Server implementation name.
    pub server_implementation: String,
    /// Server implementation version.
    pub server_version: String,
    /// Content-address hash algorithm tag.
    pub hash_algorithm: String,
    /// Maximum object size accepted by the hub.
    pub max_object_size_bytes: u64,
    /// Maximum manifest size accepted by the hub.
    pub max_manifest_size_bytes: u64,
    /// Maximum dataset size accepted by the hub.
    pub max_dataset_size_bytes: u64,
    /// Object content types accepted by the hub.
    pub supported_object_content_types: Vec<String>,
    /// True when the hub accepts telemetry posts.
    pub telemetry_endpoint_enabled: bool,
}

/// One dataset summary returned by `hub.list_datasets`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubDatasetSummary {
    /// Dataset slug.
    pub name: String,
    /// Short dataset description.
    pub description: String,
    /// Total size of the default branch bundle in bytes.
    pub size_bytes: u64,
    /// Cumulative clone/download count reported by the hub.
    pub downloads: u64,
    /// Primitive families present in the dataset.
    pub primitives: Vec<String>,
    /// Task labels attached to the dataset.
    pub tasks: Vec<String>,
    /// Free-form tags attached to the dataset.
    pub tags: Vec<String>,
    /// License identifier.
    pub license: String,
    /// Default branch name.
    pub default_branch: String,
    /// Last update timestamp as RFC 3339 UTC.
    pub last_updated: String,
    /// Optional curation badge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<String>,
}

/// Paginated dataset-list output returned by `hub.list_datasets`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubDatasetPage {
    /// Total number of datasets matching the query.
    pub total: u64,
    /// Zero-based offset of this page.
    pub offset: u64,
    /// Page-size limit applied by the hub.
    pub limit: u64,
    /// Dataset summaries in this page.
    pub items: Vec<HubDatasetSummary>,
}

/// Full dataset card returned by `hub.get_dataset`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubDatasetCard {
    /// Flattened summary fields.
    #[serde(flatten)]
    pub summary: HubDatasetSummary,
    /// Dataset owner.
    pub owner: String,
    /// Longer summary excerpt.
    pub summary_excerpt: String,
    /// Dataset creation timestamp as RFC 3339 UTC.
    pub created: String,
    /// Manifest hash for the default branch.
    pub manifest_hash: String,
    /// Engine semver range required by the bundle.
    pub engine_version_required: String,
    /// Bundle format version.
    pub format_version: String,
    /// Capability registry version required by the bundle.
    pub capability_registry_version: u32,
    /// Server-rendered clone command.
    pub clone_command: String,
    /// Dataset README in `CommonMark`.
    pub readme: String,
    /// Language identifier to quick-start snippet.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quick_start_snippets: BTreeMap<String, String>,
    /// Unknown README frontmatter keys preserved by the hub.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub frontmatter_extras: BTreeMap<String, Value>,
    /// Primitive-aware sample preview block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_preview: Option<Value>,
    /// Structural schema block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// Strata-specific feature highlights.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strata_features: Option<HubStrataFeatures>,
    /// Citation text, when published by the dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub citation: Option<String>,
    /// Dataset provenance metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<HubProvenance>,
}

/// Strata-specific feature highlights on a hub dataset card.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubStrataFeatures {
    /// Branch highlights.
    pub branches: Vec<HubBranchHighlight>,
    /// Time-travel feature examples.
    pub time_travel_highlights: Vec<String>,
    /// Multi-primitive workflow examples.
    pub multi_primitive_demos: Vec<String>,
    /// Optional notebook URL or path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example_notebook: Option<String>,
}

/// One branch highlight on a hub dataset card.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubBranchHighlight {
    /// Branch name.
    pub name: String,
    /// True when this is the default branch.
    pub is_default: bool,
}

/// Dataset provenance metadata on a hub dataset card.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubProvenance {
    /// Source dataset or publisher.
    pub source: String,
    /// Curator name.
    pub curator: String,
    /// Optional license text URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license_text_url: Option<String>,
}

/// Ref listing returned by `hub.list_refs`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubRefList {
    /// Dataset slug.
    pub dataset: String,
    /// Default branch name.
    pub default_branch: String,
    /// Live refs.
    pub refs: Vec<HubRefEntry>,
}

/// One live hub ref.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubRefEntry {
    /// Branch name.
    pub branch: String,
    /// Manifest hash the branch points at.
    pub manifest_hash: String,
    /// Last update timestamp as RFC 3339 UTC.
    pub last_updated: String,
}

/// Hub yank deny-list returned by `hub.list_yanked`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubYankedList {
    /// Snapshot generation time as RFC 3339 UTC.
    pub generated_at: String,
    /// Number of yanked entries in this snapshot.
    pub total: u64,
    /// Yanked refs.
    pub items: Vec<HubYankedEntry>,
}

/// One yanked hub ref.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubYankedEntry {
    /// Dataset slug.
    pub dataset: String,
    /// Branch name.
    pub branch: String,
    /// Yanked manifest hash.
    pub manifest_hash: String,
    /// Yank timestamp as RFC 3339 UTC.
    pub yanked_at: String,
    /// Yank reason.
    pub reason: String,
}

/// Clone progress stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HubCloneProgressStage {
    /// The ref resolved to a manifest hash.
    Resolved,
    /// The manifest was fetched and counted.
    ManifestFetched,
    /// One object was fetched.
    ObjectFetched,
    /// Local import/reconstitution started.
    Importing,
    /// Clone completed.
    Done,
    /// A future hub progress stage unknown to this executor build.
    Unknown,
}

/// Machine-readable clone progress emitted by `strata clone --progress jsonl`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct HubCloneProgress {
    /// Progress stage.
    pub stage: HubCloneProgressStage,
    /// Dataset being cloned.
    pub dataset: String,
    /// Branch being fetched when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Resolved manifest hash when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_hash: Option<String>,
    /// Object count when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_count: Option<u64>,
    /// Total object bytes when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    /// One-based object index for object fetch events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    /// Bytes fetched for the current object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

#[cfg(feature = "hub")]
impl TryFrom<strata_hub::stratahub_protocol::wire::InfoResponse> for HubInfo {
    type Error = crate::ExecutorError;

    fn try_from(
        value: strata_hub::stratahub_protocol::wire::InfoResponse,
    ) -> Result<Self, Self::Error> {
        wire_convert(value)
    }
}

#[cfg(feature = "hub")]
impl
    TryFrom<
        strata_hub::stratahub_protocol::wire::PaginationEnvelope<
            strata_hub::stratahub_protocol::wire::DatasetSummary,
        >,
    > for HubDatasetPage
{
    type Error = crate::ExecutorError;

    fn try_from(
        value: strata_hub::stratahub_protocol::wire::PaginationEnvelope<
            strata_hub::stratahub_protocol::wire::DatasetSummary,
        >,
    ) -> Result<Self, Self::Error> {
        wire_convert(value)
    }
}

#[cfg(feature = "hub")]
impl TryFrom<strata_hub::stratahub_protocol::wire::DatasetCard> for HubDatasetCard {
    type Error = crate::ExecutorError;

    fn try_from(
        value: strata_hub::stratahub_protocol::wire::DatasetCard,
    ) -> Result<Self, Self::Error> {
        wire_convert(value)
    }
}

#[cfg(feature = "hub")]
impl TryFrom<strata_hub::stratahub_protocol::wire::RefList> for HubRefList {
    type Error = crate::ExecutorError;

    fn try_from(value: strata_hub::stratahub_protocol::wire::RefList) -> Result<Self, Self::Error> {
        wire_convert(value)
    }
}

#[cfg(feature = "hub")]
impl TryFrom<strata_hub::stratahub_protocol::wire::YankedList> for HubYankedList {
    type Error = crate::ExecutorError;

    fn try_from(
        value: strata_hub::stratahub_protocol::wire::YankedList,
    ) -> Result<Self, Self::Error> {
        wire_convert(value)
    }
}

/// Converts a `stratahub_protocol` wire value into this build's executor
/// DTO. Fallible BY DESIGN: a hub running a newer protocol revision can
/// serve shapes (a new enum variant, a reworked field) that this build's
/// DTOs cannot represent — that must surface as a typed, non-retryable
/// refusal telling the user to upgrade, never as an executor panic.
#[cfg(feature = "hub")]
fn wire_convert<T, U>(value: T) -> Result<U, crate::ExecutorError>
where
    T: Serialize,
    U: DeserializeOwned,
{
    serde_json::to_value(value)
        .map_err(|error| wire_convert_error(&error))
        .and_then(|json| serde_json::from_value(json).map_err(|error| wire_convert_error(&error)))
}

#[cfg(feature = "hub")]
fn wire_convert_error(error: &serde_json::Error) -> crate::ExecutorError {
    crate::ExecutorError::new(
        crate::ExecutorErrorClass::Unavailable,
        "unavailable.executor.hub_transport",
        false,
        format!(
            "the hub response does not match this build's schema (a newer \
             hub protocol revision?) — upgrade strata: {error}"
        ),
    )
}

#[cfg(all(test, feature = "hub"))]
mod tests {
    use super::{wire_convert, HubRefList};
    use crate::public_error_code_entry;

    /// #3244: `wire_convert_error` restated the row's retry flag and had
    /// drifted from it (`after_state_change` at the site, `same_request` in the
    /// registry). The row is the authority; whether `hub_transport` is the
    /// right code for a schema mismatch is a separate question.
    #[test]
    fn a_schema_mismatch_carries_the_hub_transport_registry_row() {
        let error = wire_convert::<serde_json::Value, HubRefList>(serde_json::json!({
            "unexpected": true
        }))
        .expect_err("an unknown shape does not convert");
        let entry = public_error_code_entry("unavailable.executor.hub_transport")
            .expect("hub_transport is registered");
        assert_eq!(error.code(), entry.code);
        assert_eq!(error.public_class(), entry.class);
        assert_eq!(error.retry_policy(), entry.retry_policy);
        assert_eq!(error.commit_outcome(), entry.commit_outcome);
        assert_eq!(error.suggested_fix(), entry.suggested_fix);
    }
}
