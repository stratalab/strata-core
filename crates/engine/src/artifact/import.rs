//! Branch artifact import — replay of SAP1 payloads into a live branch.
//!
//! The inverse of `export.rs`: decoded records replay through each data
//! service's ordinary write path with their original commit timestamps
//! (the persistence adapter's explicit-timestamp replay seam), so a
//! re-export of the imported branch is byte-identical to the source
//! artifact. Replay executes in global non-decreasing timestamp order,
//! which always satisfies the storage monotonic floor.
//!
//! Structural facts whose timestamps are not part of the payload (branch
//! and space existence, graph creation, ontology definitions) replay at
//! the global minimum content timestamp — invisible to re-export by
//! construction. Several branches import as one globally-ordered replay so
//! their shared commit stream is reconstructed without regressing the floor
//! (see [`import_branches`], #3070).

use serde_json::Value;
use strata_core::Timestamp;

use crate::api::{
    BranchName, Database, EngineError, EngineResult, EventType, GraphEdgeType, GraphName,
    GraphNodeId, JsonDocumentId, JsonPath, KvKey, KvValue, ProductSpace, VectorCollectionName,
    VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey, VectorMetadata,
};
use crate::data::event::EventPayload;
use crate::data::graph::{
    GraphEdgeData, GraphLinkTypeDef, GraphNodeData, GraphObjectTypeDef, GraphPropertyDef,
    GraphTypeName,
};
use crate::data::json::JsonValue;

use super::{decode_section, ArtifactRecord, BranchArtifact};

/// Facts about a completed branch import.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BranchImportSummary {
    sections: usize,
    records: u64,
}

impl BranchImportSummary {
    /// Number of payload sections replayed.
    #[must_use]
    pub const fn sections(&self) -> usize {
        self.sections
    }

    /// Number of records replayed.
    #[must_use]
    pub const fn records(&self) -> u64 {
        self.records
    }
}

/// Accumulates consecutive same-timestamp KV rows into one commit.
type KvRun = Option<(Timestamp, Vec<(Vec<u8>, Vec<u8>)>, i64)>;

/// One replay step. Ordered within a branch by `(timestamp, section, record)`
/// and, across branches, by `(timestamp, branch, section, record)`.
struct WorkItem {
    timestamp: Timestamp,
    section: usize,
    record: i64,
    action: Action,
}

enum Action {
    KvRows(ProductSpace, Vec<(Vec<u8>, Vec<u8>)>),
    JsonDoc(ProductSpace, String, Vec<u8>),
    Event(ProductSpace, u64, String, Vec<u8>, Timestamp),
    CreateCollection(ProductSpace, String, Vec<u8>),
    VectorEntry(ProductSpace, String, String, Vec<f32>, Option<Vec<u8>>),
    CreateGraph(ProductSpace, String, Option<Vec<u8>>),
    GraphNode(ProductSpace, String, String, Vec<u8>),
    GraphEdge(ProductSpace, String, String, String, String, Vec<u8>),
}

/// Imports the artifact's content into its branch on `db`.
///
/// The branch is created when absent and must hold no content; a
/// non-empty branch refuses with `conflict.engine.artifact_import`.
pub fn import_branch(
    db: &mut Database,
    artifact: &BranchArtifact,
) -> Result<BranchImportSummary, EngineError> {
    let mut summaries = import_branches(db, std::slice::from_ref(artifact))?;
    Ok(summaries.pop().expect("one artifact yields one summary"))
}

/// Imports several branch artifacts into `db` as one globally-ordered replay.
///
/// Branches originally share a single interleaved commit stream, and the
/// commit-timestamp floor (`CommitTimestampGuard`) is a single per-database
/// value. Importing branches one at a time would raise the floor to the first
/// branch's latest commit, then reject any later branch whose history predates
/// it (#3070). Replaying every branch's content merged into one global
/// timestamp order keeps each explicit commit ≥ the floor, so MVCC-007 holds
/// with no change to the floor's semantics.
///
/// Branch existence is created up front — it commits through a separate branch
/// path that does not touch the timestamp floor — and each branch's spaces are
/// created at the global minimum content timestamp (structural, invisible to
/// re-export, and floor-safe because it is ≤ all content). The single-branch
/// case ([`import_branch`]) delegates here and reduces to today's schedule.
pub fn import_branches(
    db: &mut Database,
    artifacts: &[BranchArtifact],
) -> Result<Vec<BranchImportSummary>, EngineError> {
    let schedules = artifacts
        .iter()
        .map(build_schedule)
        .collect::<EngineResult<Vec<_>>>()?;
    let global_min = schedules
        .iter()
        .filter_map(|items| items.first().map(|item| item.timestamp))
        .min();

    // Structural phase. Branch creation makes control-plane bookkeeping
    // commits that would otherwise be generated and advance the floor past
    // the content; hold the global minimum so they land at or below it. The
    // hold covers emptiness/creation and space registration, then clears
    // before content replays at its own per-item timestamps.
    db.set_replay_structural_timestamp(global_min);
    let structural = import_structure(db, artifacts, global_min);
    db.set_replay_structural_timestamp(None);
    structural?;

    // Merge every branch's schedule into one globally non-decreasing order.
    // Branch identity is the tiebreak below equal timestamps, so the replay
    // is a deterministic total order across targets.
    let mut merged: Vec<(usize, WorkItem)> = Vec::new();
    for (branch_index, items) in schedules.into_iter().enumerate() {
        merged.extend(items.into_iter().map(|item| (branch_index, item)));
    }
    merged.sort_by(|(a_index, a), (b_index, b)| {
        (a.timestamp, *a_index, a.section, a.record).cmp(&(
            b.timestamp,
            *b_index,
            b.section,
            b.record,
        ))
    });
    for (branch_index, item) in merged {
        replay_item(db, artifacts[branch_index].branch(), item)?;
    }

    Ok(artifacts
        .iter()
        .map(|artifact| BranchImportSummary {
            sections: artifact.sections().len(),
            records: artifact
                .sections()
                .iter()
                .map(super::ArtifactSection::record_count)
                .sum(),
        })
        .collect())
}

/// Emptiness check, branch creation, and space registration for every target,
/// run while the structural replay timestamp is held so no setup commit moves
/// the floor above `global_min` (#3070). A populated target refuses here,
/// before any content replays.
fn import_structure(
    db: &mut Database,
    artifacts: &[BranchArtifact],
    global_min: Option<Timestamp>,
) -> Result<(), EngineError> {
    for artifact in artifacts {
        ensure_empty_target_branch(db, artifact.branch())?;
    }
    for artifact in artifacts {
        create_spaces(db, artifact.branch(), artifact, global_min)?;
    }
    Ok(())
}

fn ensure_empty_target_branch(db: &mut Database, branch: &BranchName) -> Result<(), EngineError> {
    let exists = db
        .branches()?
        .list()?
        .iter()
        .any(|summary| summary.name() == branch);
    if !exists {
        db.branches()?.create(branch.clone())?;
        return Ok(());
    }
    // Reuse the exporter as the emptiness oracle: content on the target
    // would silently merge, which import never does.
    if !super::export_branch(db, branch)?.sections().is_empty() {
        return Err(EngineError::conflict(
            "conflict.engine.artifact_import",
            "import target branch already holds content",
        ));
    }
    Ok(())
}

fn create_spaces(
    db: &mut Database,
    branch: &BranchName,
    artifact: &BranchArtifact,
    min_timestamp: Option<Timestamp>,
) -> Result<(), EngineError> {
    for space in artifact.spaces() {
        if let Some(timestamp) = min_timestamp {
            db.arm_replay_commit_timestamp(timestamp);
        }
        db.spaces(branch.clone())?.create(space.clone())?;
    }
    Ok(())
}

/// Decodes every section into replay steps sorted by
/// `(timestamp, section index, record index)` — the order that keeps
/// explicit commit timestamps non-decreasing.
fn build_schedule(artifact: &BranchArtifact) -> Result<Vec<WorkItem>, EngineError> {
    let mut items = Vec::new();
    for (section_index, section) in artifact.sections().iter().enumerate() {
        schedule_section(section, section_index, &mut items)?;
    }
    items.sort_by(|a, b| {
        (a.timestamp, a.section, a.record).cmp(&(b.timestamp, b.section, b.record))
    });
    Ok(items)
}

// One arm per record variant: the length is the enum's width, not logic
// depth — splitting the match would hide the 1:1 record→action mapping.
#[allow(clippy::too_many_lines)]
fn schedule_section(
    section: &super::ArtifactSection,
    section_index: usize,
    items: &mut Vec<WorkItem>,
) -> Result<(), EngineError> {
    let space = section.space().clone();
    {
        let qualifier = section.qualifier().unwrap_or_default().to_owned();
        let mut graph_min: Option<Timestamp> = None;
        let mut pending_ontology: Option<Option<Vec<u8>>> = None;
        let mut kv_run: KvRun = None;

        for (record_index, record) in decode_section(section.model(), section.bytes()).enumerate() {
            let record_index = i64::try_from(record_index).unwrap_or(i64::MAX);
            match record? {
                ArtifactRecord::Kv {
                    key,
                    value,
                    timestamp,
                } => schedule_kv_record(
                    &mut kv_run,
                    &space,
                    section_index,
                    record_index,
                    key,
                    value,
                    timestamp,
                    items,
                ),
                ArtifactRecord::Json { id, doc, timestamp } => items.push(work_item(
                    timestamp,
                    section_index,
                    record_index,
                    Action::JsonDoc(space.clone(), id, doc),
                )),
                ArtifactRecord::Event {
                    sequence,
                    event_type,
                    payload,
                    timestamp,
                } => items.push(work_item(
                    timestamp,
                    section_index,
                    record_index,
                    Action::Event(space.clone(), sequence, event_type, payload, timestamp),
                )),
                ArtifactRecord::VectorConfig { config, timestamp } => items.push(work_item(
                    timestamp,
                    section_index,
                    record_index,
                    Action::CreateCollection(space.clone(), qualifier.clone(), config),
                )),
                ArtifactRecord::VectorEntry {
                    key,
                    embedding,
                    metadata,
                    timestamp,
                } => items.push(work_item(
                    timestamp,
                    section_index,
                    record_index,
                    Action::VectorEntry(space.clone(), qualifier.clone(), key, embedding, metadata),
                )),
                ArtifactRecord::GraphMeta { ontology } => {
                    pending_ontology = Some(ontology);
                }
                ArtifactRecord::GraphNode {
                    id,
                    data,
                    timestamp,
                } => {
                    graph_min = Some(graph_min.map_or(timestamp, |min| min.min(timestamp)));
                    items.push(work_item(
                        timestamp,
                        section_index,
                        record_index,
                        Action::GraphNode(space.clone(), qualifier.clone(), id, data),
                    ));
                }
                ArtifactRecord::GraphEdge {
                    src,
                    edge_type,
                    dst,
                    data,
                    timestamp,
                } => {
                    graph_min = Some(graph_min.map_or(timestamp, |min| min.min(timestamp)));
                    items.push(work_item(
                        timestamp,
                        section_index,
                        record_index,
                        Action::GraphEdge(
                            space.clone(),
                            qualifier.clone(),
                            src,
                            edge_type,
                            dst,
                            data,
                        ),
                    ));
                }
            }
        }
        flush_kv_run(&mut kv_run, &space, section_index, items);

        if let Some(ontology) = pending_ontology {
            // Graph creation (and ontology) precede the graph's earliest
            // content; empty graphs replay at the epoch-adjacent floor.
            items.push(WorkItem {
                timestamp: graph_min.unwrap_or(Timestamp::from_micros(0)),
                section: section_index,
                record: -1,
                action: Action::CreateGraph(space.clone(), qualifier.clone(), ontology),
            });
        }
    }
    Ok(())
}

/// Accumulates consecutive same-timestamp KV rows so they replay as one
/// commit (they were one commit at the source).
#[allow(clippy::too_many_arguments)]
fn schedule_kv_record(
    kv_run: &mut KvRun,
    space: &ProductSpace,
    section_index: usize,
    record_index: i64,
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: Timestamp,
    items: &mut Vec<WorkItem>,
) {
    match kv_run {
        Some((run_ts, rows, _)) if *run_ts == timestamp => {
            rows.push((key, value));
        }
        _ => {
            flush_kv_run(kv_run, space, section_index, items);
            *kv_run = Some((timestamp, vec![(key, value)], record_index));
        }
    }
}

const fn work_item(timestamp: Timestamp, section: usize, record: i64, action: Action) -> WorkItem {
    WorkItem {
        timestamp,
        section,
        record,
        action,
    }
}

fn flush_kv_run(run: &mut KvRun, space: &ProductSpace, section: usize, items: &mut Vec<WorkItem>) {
    if let Some((timestamp, rows, record)) = run.take() {
        items.push(WorkItem {
            timestamp,
            section,
            record,
            action: Action::KvRows(space.clone(), rows),
        });
    }
}

fn replay_item(db: &mut Database, branch: &BranchName, item: WorkItem) -> Result<(), EngineError> {
    db.arm_replay_commit_timestamp(item.timestamp);
    match item.action {
        Action::KvRows(space, rows) => {
            let entries = rows
                .into_iter()
                .map(|(key, value)| Ok((KvKey::new(key)?, KvValue::new(value))))
                .collect::<EngineResult<Vec<_>>>()?;
            db.kv(branch.clone(), space)?.put_batch(entries)?;
        }
        Action::JsonDoc(space, id, doc) => {
            let value: Value =
                serde_json::from_slice(&doc).map_err(|error| payload_error(&error))?;
            db.json(branch.clone(), space)?.set_or_create(
                JsonDocumentId::new(id)?,
                &JsonPath::root(),
                JsonValue::new(value)?,
            )?;
        }
        Action::Event(space, sequence, event_type, payload, timestamp) => {
            let value: Value =
                serde_json::from_slice(&payload).map_err(|error| payload_error(&error))?;
            let mut service = db.event(branch.clone(), space)?;
            let next = service.len()?.count();
            if next != sequence {
                return Err(EngineError::corruption(
                    "corruption.engine.artifact_payload",
                    format!("event sequence gap: expected {next}, artifact carries {sequence}"),
                ));
            }
            service.replay_append(vec![(
                EventType::new(event_type)?,
                EventPayload::new(value)?,
                timestamp,
            )])?;
        }
        Action::CreateCollection(space, name, config) => {
            replay_create_collection(db, branch, space, name, &config)?;
        }
        Action::VectorEntry(space, collection, key, embedding, metadata) => {
            let metadata = metadata
                .map(|bytes| {
                    let value: Value =
                        serde_json::from_slice(&bytes).map_err(|error| payload_error(&error))?;
                    VectorMetadata::new(value)
                })
                .transpose()?;
            db.vector(branch.clone(), space)?.upsert(
                VectorCollectionName::new(collection)?,
                VectorKey::new(key)?,
                VectorEmbedding::new(embedding)?,
                metadata,
            )?;
        }
        Action::CreateGraph(space, name, ontology) => {
            let graph = GraphName::new(name)?;
            db.graph(branch.clone(), space.clone())?
                .create_graph(graph.clone())?;
            if let Some(ontology) = ontology {
                replay_ontology(db, branch, &space, &graph, item.timestamp, &ontology)?;
            }
        }
        Action::GraphNode(space, graph, id, data) => {
            let data: GraphNodeData =
                serde_json::from_slice(&data).map_err(|error| payload_error(&error))?;
            db.graph(branch.clone(), space)?.upsert_node(
                &GraphName::new(graph)?,
                GraphNodeId::new(id)?,
                data,
            )?;
        }
        Action::GraphEdge(space, graph, src, edge_type, dst, data) => {
            replay_graph_edge(db, branch, space, &graph, src, edge_type, dst, &data)?;
        }
    }
    Ok(())
}

fn replay_create_collection(
    db: &mut Database,
    branch: &BranchName,
    space: ProductSpace,
    name: String,
    config: &[u8],
) -> Result<(), EngineError> {
    let config: Value = serde_json::from_slice(config).map_err(|error| payload_error(&error))?;
    let dimension = usize::try_from(config["dimension"].as_u64().unwrap_or(0))
        .map_err(|_| payload_corruption("vector dimension out of range"))?;
    let metric = match config["metric"].as_str() {
        Some("cosine") | None => VectorDistanceMetric::Cosine,
        Some("euclidean") => VectorDistanceMetric::Euclidean,
        Some("dot_product") => VectorDistanceMetric::DotProduct,
        Some(other) => {
            return Err(payload_corruption(&format!(
                "unknown vector metric `{other}`"
            )));
        }
    };
    db.vector(branch.clone(), space)?.create_collection(
        VectorCollectionName::new(name)?,
        VectorConfig::new(dimension, metric)?,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn replay_graph_edge(
    db: &mut Database,
    branch: &BranchName,
    space: ProductSpace,
    graph: &str,
    src: String,
    edge_type: String,
    dst: String,
    data: &[u8],
) -> Result<(), EngineError> {
    let data: Value = serde_json::from_slice(data).map_err(|error| payload_error(&error))?;
    let weight = data["weight"].as_f64().unwrap_or(1.0);
    let properties = match data.get("properties") {
        Some(value) => {
            Some(serde_json::from_value(value.clone()).map_err(|error| payload_error(&error))?)
        }
        None => None,
    };
    db.graph(branch.clone(), space)?.upsert_edge(
        &GraphName::new(graph)?,
        GraphNodeId::new(src)?,
        GraphEdgeType::new(edge_type)?,
        GraphNodeId::new(dst)?,
        GraphEdgeData::new(weight, properties)?,
    )?;
    Ok(())
}

/// Rebuilds ontology definitions (and frozen status) from the exported
/// read shape. Each write commits at the graph's replay timestamp.
fn replay_ontology(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    graph: &GraphName,
    timestamp: Timestamp,
    ontology: &[u8],
) -> Result<(), EngineError> {
    let ontology: Value =
        serde_json::from_slice(ontology).map_err(|error| payload_error(&error))?;
    for def in ontology["object_types"].as_array().into_iter().flatten() {
        let parsed = GraphObjectTypeDef::new(
            GraphTypeName::new(def["name"].as_str().unwrap_or_default())?,
            parse_property_defs(&def["properties"])?,
        )?;
        db.arm_replay_commit_timestamp(timestamp);
        db.graph(branch.clone(), space.clone())?
            .define_object_type(graph, parsed)?;
    }
    for def in ontology["link_types"].as_array().into_iter().flatten() {
        let parsed = GraphLinkTypeDef::new(
            GraphTypeName::new(def["name"].as_str().unwrap_or_default())?,
            GraphTypeName::new(def["source"].as_str().unwrap_or_default())?,
            GraphTypeName::new(def["target"].as_str().unwrap_or_default())?,
            def["cardinality"].as_str().map(str::to_owned),
            parse_property_defs(&def["properties"])?,
        )?;
        db.arm_replay_commit_timestamp(timestamp);
        db.graph(branch.clone(), space.clone())?
            .define_link_type(graph, parsed)?;
    }
    if ontology["status"].as_str() == Some("Frozen") {
        db.arm_replay_commit_timestamp(timestamp);
        db.graph(branch.clone(), space.clone())?
            .freeze_ontology(graph)?;
    }
    Ok(())
}

fn parse_property_defs(value: &Value) -> Result<Vec<(String, GraphPropertyDef)>, EngineError> {
    let mut defs = Vec::new();
    if let Some(map) = value.as_object() {
        for (name, def) in map {
            defs.push((
                name.clone(),
                GraphPropertyDef::new(
                    def["value_type"].as_str().map(str::to_owned),
                    def["required"].as_bool().unwrap_or(false),
                )?,
            ));
        }
    }
    Ok(defs)
}

fn payload_error(error: &serde_json::Error) -> EngineError {
    payload_corruption(&format!("payload JSON decode failed: {error}"))
}

fn payload_corruption(reason: &str) -> EngineError {
    EngineError::corruption(
        "corruption.engine.artifact_payload",
        format!("artifact payload section is malformed: {reason}"),
    )
}
