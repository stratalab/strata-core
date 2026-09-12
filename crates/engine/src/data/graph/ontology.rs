//! Graph ontology: typed schemas for nodes and edges (GO1).
//!
//! The ontology is a per-graph, draft-then-frozen type layer. Definitions
//! are freely editable while **Draft**; `freeze_ontology` validates the set
//! (at least one type; every link endpoint references a declared object
//! type) and flips it to **Frozen**, after which type definitions refuse to
//! change. Property `value_type` and link `cardinality` are recorded
//! *hints* for consumers (AI-first schema), not enforced constraints —
//! enforcement of node/edge writes against a frozen ontology is the GO2
//! slice.
//!
//! Storage: one authored row per graph (`RowClass::GraphOntology`) holding
//! status plus all definitions, so freeze is a single atomic row update
//! and write-path validation reads one row.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strata_core::{CommitVersion, Timestamp};

use crate::commit::CommitOutcome;
use crate::diagnostics::EngineError;

use super::types::validate_text_component;
use super::{GraphEdgeType, GraphName, GraphNodeData};

const GRAPH_ONTOLOGY_FORMAT_VERSION: u8 = 1;
const MAX_TYPE_NAME_BYTES: usize = 256;
const MAX_PROPERTY_NAME_BYTES: usize = 256;
const MAX_HINT_BYTES: usize = 256;

/// The cardinality values a graph link type may declare. Unlike a free-form
/// hint, an unrecognized value is rejected so a typo cannot be stored forever.
const GRAPH_LINK_CARDINALITIES: [&str; 4] =
    ["one-to-one", "one-to-many", "many-to-one", "many-to-many"];

/// A validated ontology type name (object or link type).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphTypeName(String);

impl GraphTypeName {
    /// Creates a validated type name.
    pub fn new(name: impl Into<String>) -> Result<Self, EngineError> {
        let name = name.into();
        validate_text_component(
            &name,
            MAX_TYPE_NAME_BYTES,
            "invalid_argument.engine.graph_type_name",
            "graph type name",
        )?;
        if name.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_type_name_reserved",
                "graph type name is reserved for engine internals",
            ));
        }
        Ok(Self(name))
    }

    #[must_use]
    /// Returns the type name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for GraphTypeName {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for GraphTypeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// One declared property on an object or link type. `value_type` is a
/// recorded hint (for consumers and model prompts), not an enforced
/// constraint; only `required` participates in GO2 write validation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GraphPropertyDef {
    value_type: Option<String>,
    required: bool,
}

impl GraphPropertyDef {
    /// Creates a property definition.
    pub fn new(value_type: Option<String>, required: bool) -> Result<Self, EngineError> {
        if let Some(hint) = value_type.as_deref() {
            validate_text_component(
                hint,
                MAX_HINT_BYTES,
                "invalid_argument.engine.graph_type_hint",
                "graph property value-type hint",
            )?;
        }
        Ok(Self {
            value_type,
            required,
        })
    }

    #[must_use]
    /// Returns the recorded value-type hint.
    pub fn value_type(&self) -> Option<&str> {
        self.value_type.as_deref()
    }

    #[must_use]
    /// Returns whether the property is required when the ontology is frozen.
    pub const fn required(&self) -> bool {
        self.required
    }
}

/// A declared node (object) type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GraphObjectTypeDef {
    name: GraphTypeName,
    properties: BTreeMap<String, GraphPropertyDef>,
}

impl GraphObjectTypeDef {
    /// Creates an object type definition.
    pub fn new(
        name: GraphTypeName,
        properties: impl IntoIterator<Item = (String, GraphPropertyDef)>,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            name,
            properties: validated_properties(properties)?,
        })
    }

    #[must_use]
    /// Returns the type name.
    pub const fn name(&self) -> &GraphTypeName {
        &self.name
    }

    #[must_use]
    /// Returns the declared properties, ordered by name.
    pub const fn properties(&self) -> &BTreeMap<String, GraphPropertyDef> {
        &self.properties
    }
}

/// A declared edge (link) type between two object types.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GraphLinkTypeDef {
    name: GraphTypeName,
    source: GraphTypeName,
    target: GraphTypeName,
    cardinality: Option<String>,
    properties: BTreeMap<String, GraphPropertyDef>,
}

impl GraphLinkTypeDef {
    /// Creates a link type definition. `cardinality` is a recorded hint.
    pub fn new(
        name: GraphTypeName,
        source: GraphTypeName,
        target: GraphTypeName,
        cardinality: Option<String>,
        properties: impl IntoIterator<Item = (String, GraphPropertyDef)>,
    ) -> Result<Self, EngineError> {
        if let Some(hint) = cardinality.as_deref() {
            validate_text_component(
                hint,
                MAX_HINT_BYTES,
                "invalid_argument.engine.graph_type_hint",
                "graph link cardinality hint",
            )?;
            if !GRAPH_LINK_CARDINALITIES.contains(&hint) {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.graph_type_hint",
                    "graph link cardinality must be one of one-to-one, one-to-many, \
                     many-to-one, many-to-many",
                ));
            }
        }
        Ok(Self {
            name,
            source,
            target,
            cardinality,
            properties: validated_properties(properties)?,
        })
    }

    #[must_use]
    /// Returns the type name.
    pub const fn name(&self) -> &GraphTypeName {
        &self.name
    }

    #[must_use]
    /// Returns the declared source object type.
    pub const fn source(&self) -> &GraphTypeName {
        &self.source
    }

    #[must_use]
    /// Returns the declared target object type.
    pub const fn target(&self) -> &GraphTypeName {
        &self.target
    }

    #[must_use]
    /// Returns the recorded cardinality hint.
    pub fn cardinality(&self) -> Option<&str> {
        self.cardinality.as_deref()
    }

    #[must_use]
    /// Returns the declared properties, ordered by name.
    pub const fn properties(&self) -> &BTreeMap<String, GraphPropertyDef> {
        &self.properties
    }
}

fn validated_properties(
    properties: impl IntoIterator<Item = (String, GraphPropertyDef)>,
) -> Result<BTreeMap<String, GraphPropertyDef>, EngineError> {
    let mut validated = BTreeMap::new();
    for (name, def) in properties {
        validate_text_component(
            &name,
            MAX_PROPERTY_NAME_BYTES,
            "invalid_argument.engine.graph_property_name",
            "graph property name",
        )?;
        validated.insert(name, def);
    }
    Ok(validated)
}

/// Ontology lifecycle status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum GraphOntologyStatus {
    /// Definitions may change freely; nothing is validated on write.
    Draft,
    /// Definitions are immutable and GO2 write validation applies.
    Frozen,
}

/// The visible ontology of one graph: status plus every definition, with
/// the commit facts of the row that produced this view.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GraphOntology {
    graph: GraphName,
    status: GraphOntologyStatus,
    object_types: Vec<GraphObjectTypeDef>,
    link_types: Vec<GraphLinkTypeDef>,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphOntology {
    pub(crate) const fn new(
        graph: GraphName,
        status: GraphOntologyStatus,
        object_types: Vec<GraphObjectTypeDef>,
        link_types: Vec<GraphLinkTypeDef>,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            graph,
            status,
            object_types,
            link_types,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the graph this ontology belongs to.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the lifecycle status.
    pub const fn status(&self) -> GraphOntologyStatus {
        self.status
    }

    #[must_use]
    /// Returns the declared object types, ordered by name.
    pub fn object_types(&self) -> &[GraphObjectTypeDef] {
        &self.object_types
    }

    #[must_use]
    /// Returns the declared link types, ordered by name.
    pub fn link_types(&self) -> &[GraphLinkTypeDef] {
        &self.link_types
    }

    #[must_use]
    /// Returns the commit version of the visible ontology row.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp of the visible ontology row.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// One object type with its visible node count (GO3 summary).
#[derive(Clone, Debug, PartialEq)]
pub struct GraphObjectTypeSummary {
    def: GraphObjectTypeDef,
    node_count: u64,
}

impl GraphObjectTypeSummary {
    pub(crate) const fn new(def: GraphObjectTypeDef, node_count: u64) -> Self {
        Self { def, node_count }
    }

    #[must_use]
    /// Returns the type definition.
    pub const fn def(&self) -> &GraphObjectTypeDef {
        &self.def
    }

    #[must_use]
    /// Returns the number of visible nodes declaring this type.
    pub const fn node_count(&self) -> u64 {
        self.node_count
    }
}

/// One link type with its visible edge count (GO3 summary).
#[derive(Clone, Debug, PartialEq)]
pub struct GraphLinkTypeSummary {
    def: GraphLinkTypeDef,
    edge_count: u64,
}

impl GraphLinkTypeSummary {
    pub(crate) const fn new(def: GraphLinkTypeDef, edge_count: u64) -> Self {
        Self { def, edge_count }
    }

    #[must_use]
    /// Returns the type definition.
    pub const fn def(&self) -> &GraphLinkTypeDef {
        &self.def
    }

    #[must_use]
    /// Returns the number of visible edges carrying this type.
    pub const fn edge_count(&self) -> u64 {
        self.edge_count
    }
}

/// The ontology with per-type usage counts: every declared object type
/// with its visible node count, every declared link type with its visible
/// edge count. Counts are computed exactly at read time (type-index
/// prefix counts and one edge scan) — there are no counter rows.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphOntologySummary {
    graph: GraphName,
    status: GraphOntologyStatus,
    object_types: Vec<GraphObjectTypeSummary>,
    link_types: Vec<GraphLinkTypeSummary>,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl GraphOntologySummary {
    pub(crate) const fn new(
        graph: GraphName,
        status: GraphOntologyStatus,
        object_types: Vec<GraphObjectTypeSummary>,
        link_types: Vec<GraphLinkTypeSummary>,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            graph,
            status,
            object_types,
            link_types,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the graph.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the lifecycle status.
    pub const fn status(&self) -> GraphOntologyStatus {
        self.status
    }

    #[must_use]
    /// Returns object type summaries, ordered by name.
    pub fn object_types(&self) -> &[GraphObjectTypeSummary] {
        &self.object_types
    }

    #[must_use]
    /// Returns link type summaries, ordered by name.
    pub fn link_types(&self) -> &[GraphLinkTypeSummary] {
        &self.link_types
    }

    #[must_use]
    /// Returns the commit version of the visible ontology row.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp of the visible ontology row.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Outcome of defining an object or link type.
#[derive(Clone, Debug)]
pub struct GraphOntologyWriteOutcome {
    graph: GraphName,
    type_name: GraphTypeName,
    created: bool,
    commit: CommitOutcome,
}

impl GraphOntologyWriteOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        type_name: GraphTypeName,
        created: bool,
        commit: CommitOutcome,
    ) -> Self {
        Self {
            graph,
            type_name,
            created,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns the defined type name.
    pub const fn type_name(&self) -> &GraphTypeName {
        &self.type_name
    }

    #[must_use]
    /// Returns true when the definition was new (false: redefined in draft).
    pub const fn created(&self) -> bool {
        self.created
    }

    #[must_use]
    /// Returns the commit that persisted the definition.
    pub const fn commit(&self) -> &CommitOutcome {
        &self.commit
    }
}

/// Outcome of freezing an ontology.
#[derive(Clone, Debug)]
pub struct GraphOntologyFreezeOutcome {
    graph: GraphName,
    object_types: usize,
    link_types: usize,
    commit: CommitOutcome,
}

impl GraphOntologyFreezeOutcome {
    pub(crate) const fn new(
        graph: GraphName,
        object_types: usize,
        link_types: usize,
        commit: CommitOutcome,
    ) -> Self {
        Self {
            graph,
            object_types,
            link_types,
            commit,
        }
    }

    #[must_use]
    /// Returns the graph.
    pub const fn graph(&self) -> &GraphName {
        &self.graph
    }

    #[must_use]
    /// Returns how many object types were frozen.
    pub const fn object_types(&self) -> usize {
        self.object_types
    }

    #[must_use]
    /// Returns how many link types were frozen.
    pub const fn link_types(&self) -> usize {
        self.link_types
    }

    #[must_use]
    /// Returns the commit that persisted the freeze.
    pub const fn commit(&self) -> &CommitOutcome {
        &self.commit
    }
}

/// Stored ontology row: status plus every definition for one graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphOntologyRecord {
    graph: GraphName,
    status: GraphOntologyStatus,
    object_types: BTreeMap<GraphTypeName, GraphObjectTypeDef>,
    link_types: BTreeMap<GraphTypeName, GraphLinkTypeDef>,
}

impl GraphOntologyRecord {
    pub(crate) const fn empty_draft(graph: GraphName) -> Self {
        Self {
            graph,
            status: GraphOntologyStatus::Draft,
            object_types: BTreeMap::new(),
            link_types: BTreeMap::new(),
        }
    }

    pub(crate) const fn graph(&self) -> &GraphName {
        &self.graph
    }

    pub(crate) const fn status(&self) -> GraphOntologyStatus {
        self.status
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.status == GraphOntologyStatus::Frozen
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.object_types.is_empty() && self.link_types.is_empty()
    }

    pub(crate) const fn object_types(&self) -> &BTreeMap<GraphTypeName, GraphObjectTypeDef> {
        &self.object_types
    }

    pub(crate) const fn link_types(&self) -> &BTreeMap<GraphTypeName, GraphLinkTypeDef> {
        &self.link_types
    }

    /// Inserts or replaces an object type. Returns true when new.
    pub(crate) fn put_object_type(&mut self, def: GraphObjectTypeDef) -> bool {
        self.object_types.insert(def.name().clone(), def).is_none()
    }

    /// Inserts or replaces a link type. Returns true when new.
    pub(crate) fn put_link_type(&mut self, def: GraphLinkTypeDef) -> bool {
        self.link_types.insert(def.name().clone(), def).is_none()
    }

    /// Removes an object type. Returns true when it existed.
    pub(crate) fn remove_object_type(&mut self, name: &GraphTypeName) -> bool {
        self.object_types.remove(name).is_some()
    }

    /// Removes a link type. Returns true when it existed.
    pub(crate) fn remove_link_type(&mut self, name: &GraphTypeName) -> bool {
        self.link_types.remove(name).is_some()
    }

    /// GO2 write validation for a node under a frozen ontology: untyped
    /// nodes always pass (AI-first light enforcement); a typed node must
    /// name a declared object type and carry every required property.
    pub(crate) fn validate_node(&self, data: &GraphNodeData) -> Result<(), EngineError> {
        let Some(object_type) = data.object_type() else {
            return Ok(());
        };
        let Some(def) = self.object_types.get(object_type) else {
            return Err(EngineError::conflict(
                "failed_precondition.engine.graph_ontology_node_type",
                format!(
                    "node object type `{}` is not declared in the frozen ontology",
                    object_type.as_str()
                ),
            ));
        };
        for (name, property) in def.properties() {
            let present = data
                .properties()
                .is_some_and(|properties| properties.contains_key(name));
            if property.required() && !present {
                return Err(EngineError::conflict(
                    "failed_precondition.engine.graph_ontology_required_property",
                    format!(
                        "node is missing required property `{name}` of object type `{}`",
                        object_type.as_str()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// GO2 write validation for an edge under a frozen ontology: the edge
    /// type must be a declared link type, and each *typed* endpoint must
    /// match the link's declared source/target object type (untyped
    /// endpoints skip the check).
    pub(crate) fn validate_edge(
        &self,
        edge_type: &GraphEdgeType,
        src: &GraphNodeData,
        dst: &GraphNodeData,
    ) -> Result<(), EngineError> {
        let Some(link) = self
            .link_types
            .iter()
            .find_map(|(name, def)| (name.as_str() == edge_type.as_str()).then_some(def))
        else {
            return Err(EngineError::conflict(
                "failed_precondition.engine.graph_ontology_edge_type",
                format!(
                    "edge type `{}` is not a declared link type in the frozen ontology",
                    edge_type.as_str()
                ),
            ));
        };
        for (role, declared, endpoint) in [
            ("source", link.source(), src.object_type()),
            ("target", link.target(), dst.object_type()),
        ] {
            if let Some(actual) = endpoint {
                if actual != declared {
                    return Err(EngineError::conflict(
                        "failed_precondition.engine.graph_ontology_endpoint_type",
                        format!(
                            "edge `{}` {role} must be `{}`, node is `{}`",
                            edge_type.as_str(),
                            declared.as_str(),
                            actual.as_str()
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Freeze-time validation: at least one type, and every link endpoint
    /// references a declared object type. Returns the offending detail on
    /// failure.
    pub(crate) fn validate_for_freeze(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("ontology declares no object or link types".to_owned());
        }
        for (name, link) in &self.link_types {
            for (role, endpoint) in [("source", link.source()), ("target", link.target())] {
                if !self.object_types.contains_key(endpoint) {
                    return Err(format!(
                        "link type `{}` {role} references undeclared object type `{}`",
                        name.as_str(),
                        endpoint.as_str()
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn freeze(&mut self) {
        self.status = GraphOntologyStatus::Frozen;
    }
}

#[derive(Serialize, Deserialize)]
struct StoredGraphOntology {
    graph: String,
    status: String,
    object_types: BTreeMap<String, StoredObjectType>,
    link_types: BTreeMap<String, StoredLinkType>,
}

#[derive(Serialize, Deserialize)]
struct StoredObjectType {
    properties: BTreeMap<String, StoredPropertyDef>,
}

#[derive(Serialize, Deserialize)]
struct StoredLinkType {
    source: String,
    target: String,
    cardinality: Option<String>,
    properties: BTreeMap<String, StoredPropertyDef>,
}

#[derive(Serialize, Deserialize)]
struct StoredPropertyDef {
    value_type: Option<String>,
    required: bool,
}

pub(crate) fn encode_graph_ontology_record(
    record: &GraphOntologyRecord,
) -> Result<Vec<u8>, EngineError> {
    let stored = StoredGraphOntology {
        graph: record.graph().as_str().to_owned(),
        status: match record.status() {
            GraphOntologyStatus::Draft => "draft".to_owned(),
            GraphOntologyStatus::Frozen => "frozen".to_owned(),
        },
        object_types: record
            .object_types()
            .iter()
            .map(|(name, def)| {
                (
                    name.as_str().to_owned(),
                    StoredObjectType {
                        properties: properties_to_stored(def.properties()),
                    },
                )
            })
            .collect(),
        link_types: record
            .link_types()
            .iter()
            .map(|(name, def)| {
                (
                    name.as_str().to_owned(),
                    StoredLinkType {
                        source: def.source().as_str().to_owned(),
                        target: def.target().as_str().to_owned(),
                        cardinality: def.cardinality().map(str::to_owned),
                        properties: properties_to_stored(def.properties()),
                    },
                )
            })
            .collect(),
    };
    let mut bytes = vec![GRAPH_ONTOLOGY_FORMAT_VERSION];
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.graph_ontology_record",
            format!("graph ontology cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_graph_ontology_record(
    expected_graph: &GraphName,
    bytes: &[u8],
) -> Result<GraphOntologyRecord, EngineError> {
    if bytes.first().copied() != Some(GRAPH_ONTOLOGY_FORMAT_VERSION) {
        return Err(corruption("has an unknown format version"));
    }
    let stored = serde_json::from_slice::<StoredGraphOntology>(&bytes[1..])
        .map_err(|error| corruption(&format!("cannot be decoded: {error}")))?;
    let graph =
        GraphName::new(stored.graph).map_err(|_| corruption("contains an invalid graph name"))?;
    if &graph != expected_graph {
        return Err(corruption("identity does not match its row key"));
    }
    let status = match stored.status.as_str() {
        "draft" => GraphOntologyStatus::Draft,
        "frozen" => GraphOntologyStatus::Frozen,
        _ => return Err(corruption("contains an unknown status")),
    };
    let mut record = GraphOntologyRecord {
        graph,
        status,
        object_types: BTreeMap::new(),
        link_types: BTreeMap::new(),
    };
    for (name, object_type) in stored.object_types {
        let name =
            GraphTypeName::new(name).map_err(|_| corruption("contains an invalid type name"))?;
        let def = GraphObjectTypeDef::new(
            name.clone(),
            properties_from_stored(object_type.properties)?,
        )
        .map_err(|_| corruption("contains an invalid object type"))?;
        record.object_types.insert(name, def);
    }
    for (name, link_type) in stored.link_types {
        let name =
            GraphTypeName::new(name).map_err(|_| corruption("contains an invalid type name"))?;
        let def = GraphLinkTypeDef::new(
            name.clone(),
            GraphTypeName::new(link_type.source)
                .map_err(|_| corruption("contains an invalid link source"))?,
            GraphTypeName::new(link_type.target)
                .map_err(|_| corruption("contains an invalid link target"))?,
            link_type.cardinality,
            properties_from_stored(link_type.properties)?,
        )
        .map_err(|_| corruption("contains an invalid link type"))?;
        record.link_types.insert(name, def);
    }
    Ok(record)
}

fn properties_to_stored(
    properties: &BTreeMap<String, GraphPropertyDef>,
) -> BTreeMap<String, StoredPropertyDef> {
    properties
        .iter()
        .map(|(name, def)| {
            (
                name.clone(),
                StoredPropertyDef {
                    value_type: def.value_type().map(str::to_owned),
                    required: def.required(),
                },
            )
        })
        .collect()
}

fn properties_from_stored(
    stored: BTreeMap<String, StoredPropertyDef>,
) -> Result<Vec<(String, GraphPropertyDef)>, EngineError> {
    stored
        .into_iter()
        .map(|(name, def)| {
            Ok((
                name,
                GraphPropertyDef::new(def.value_type, def.required)
                    .map_err(|_| corruption("contains an invalid property definition"))?,
            ))
        })
        .collect()
}

fn corruption(detail: &str) -> EngineError {
    EngineError::corruption(
        "data_loss.engine.graph_ontology_record",
        format!("stored graph ontology {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        decode_graph_ontology_record, encode_graph_ontology_record, GraphLinkTypeDef,
        GraphObjectTypeDef, GraphOntologyRecord, GraphOntologyStatus, GraphPropertyDef,
        GraphTypeName,
    };
    use crate::data::graph::GraphName;
    use crate::diagnostics::EngineErrorClass;

    fn object_type(name: &str) -> GraphObjectTypeDef {
        GraphObjectTypeDef::new(
            GraphTypeName::new(name).expect("type name"),
            [(
                "title".to_owned(),
                GraphPropertyDef::new(Some("string".to_owned()), true).expect("property"),
            )],
        )
        .expect("object type")
    }

    #[test]
    fn type_names_validate_and_reserve_the_engine_prefix() {
        assert!(GraphTypeName::new("Document").is_ok());
        let error = GraphTypeName::new("_internal").expect_err("reserved");
        assert_eq!(
            error.code(),
            "invalid_argument.engine.graph_type_name_reserved"
        );
        let error = GraphTypeName::new("").expect_err("empty");
        assert_eq!(error.code(), "invalid_argument.engine.graph_type_name");
    }

    #[test]
    fn graph_link_cardinality_rejects_values_outside_the_defined_set() {
        let link = |cardinality: Option<&str>| {
            GraphLinkTypeDef::new(
                GraphTypeName::new("wrote").expect("name"),
                GraphTypeName::new("Author").expect("source"),
                GraphTypeName::new("Document").expect("target"),
                cardinality.map(str::to_owned),
                [],
            )
        };

        // Every defined cardinality is accepted, and an absent one is fine.
        for value in ["one-to-one", "one-to-many", "many-to-one", "many-to-many"] {
            link(Some(value)).unwrap_or_else(|_| panic!("`{value}` is a valid cardinality"));
        }
        link(None).expect("cardinality is optional");

        // An unrecognized cardinality is rejected with a typed error rather than
        // stored forever as a meaningless free-form hint.
        let error = link(Some("wat")).expect_err("unknown cardinality is rejected");
        assert_eq!(error.code(), "invalid_argument.engine.graph_type_hint");
    }

    #[test]
    fn ontology_record_round_trips_and_validates_identity() {
        let graph = GraphName::new("deps").expect("graph");
        let mut record = GraphOntologyRecord::empty_draft(graph.clone());
        record.put_object_type(object_type("Document"));
        record.put_object_type(object_type("Author"));
        record.put_link_type(
            GraphLinkTypeDef::new(
                GraphTypeName::new("wrote").expect("name"),
                GraphTypeName::new("Author").expect("source"),
                GraphTypeName::new("Document").expect("target"),
                Some("one-to-many".to_owned()),
                [],
            )
            .expect("link type"),
        );
        record.freeze();

        let bytes = encode_graph_ontology_record(&record).expect("encoded");
        let decoded = decode_graph_ontology_record(&graph, &bytes).expect("decoded");
        assert_eq!(decoded, record);
        assert_eq!(decoded.status(), GraphOntologyStatus::Frozen);

        let other = GraphName::new("other").expect("graph");
        let error = decode_graph_ontology_record(&other, &bytes).expect_err("identity");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_ontology_record");
    }

    #[test]
    fn ontology_record_rejects_unknown_versions_and_bad_payloads() {
        let graph = GraphName::new("deps").expect("graph");
        for bytes in [&b"\x02{}"[..], &b"\x01{"[..]] {
            let error = decode_graph_ontology_record(&graph, bytes).expect_err("rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
        }
    }

    #[test]
    fn freeze_validation_requires_types_and_declared_endpoints() {
        let graph = GraphName::new("deps").expect("graph");
        let mut record = GraphOntologyRecord::empty_draft(graph);
        assert!(record.validate_for_freeze().is_err(), "empty refuses");

        record.put_link_type(
            GraphLinkTypeDef::new(
                GraphTypeName::new("wrote").expect("name"),
                GraphTypeName::new("Author").expect("source"),
                GraphTypeName::new("Document").expect("target"),
                None,
                [],
            )
            .expect("link type"),
        );
        let detail = record.validate_for_freeze().expect_err("dangling endpoint");
        assert!(detail.contains("wrote"), "{detail}");

        record.put_object_type(object_type("Author"));
        record.put_object_type(object_type("Document"));
        assert!(record.validate_for_freeze().is_ok());
    }
}
