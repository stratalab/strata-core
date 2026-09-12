//! Branch content enumeration + SAP1 record encoding.

use strata_core::Timestamp;

use crate::api::{
    BranchName, Database, EngineError, EventRangeDirection, EventSequence, GraphDirection,
    GraphName, JsonPath, KvKey, ProductSpace,
};
use crate::data::json::JsonDocumentId;

use super::{ArtifactModel, ArtifactSection, BranchArtifact};

/// Page size for every deterministic enumeration loop.
const EXPORT_PAGE: usize = 1024;

/// Exports the branch's logical content as deterministic payload sections.
pub fn export_branch(
    db: &mut Database,
    branch: &BranchName,
) -> Result<BranchArtifact, EngineError> {
    let mut spaces = db.spaces(branch.clone())?.list()?;
    spaces.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    let mut tracker = TimestampTracker::default();
    let mut sections = Vec::new();
    for space in &spaces {
        export_kv(db, branch, space, &mut sections, &mut tracker)?;
        export_json(db, branch, space, &mut sections, &mut tracker)?;
        export_events(db, branch, space, &mut sections, &mut tracker)?;
        export_vector_collections(db, branch, space, &mut sections, &mut tracker)?;
        export_graphs(db, branch, space, &mut sections, &mut tracker)?;
    }

    Ok(BranchArtifact::new(
        branch.clone(),
        spaces,
        sections,
        tracker.max,
    ))
}

fn export_kv(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    sections: &mut Vec<ArtifactSection>,
    tracker: &mut TimestampTracker,
) -> Result<(), EngineError> {
    let mut service = db.kv(branch.clone(), space.clone())?;
    let mut encoder = SectionEncoder::default();
    let mut start: Option<KvKey> = None;
    loop {
        let rows = service.scan(start.as_ref(), Some(EXPORT_PAGE))?;
        let Some(last) = rows.last() else {
            break;
        };
        let next_start = successor_key(last.key())?;
        for row in &rows {
            tracker.observe(row.timestamp());
            encoder.record(|body| {
                put_bytes(body, row.key().as_bytes());
                put_bytes(body, row.value().as_bytes());
                put_u64(body, row.timestamp().as_micros());
            });
        }
        if rows.len() < EXPORT_PAGE {
            break;
        }
        start = Some(next_start);
    }
    encoder.finish(space, ArtifactModel::Kv, None, sections);
    Ok(())
}

fn export_json(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    sections: &mut Vec<ArtifactSection>,
    tracker: &mut TimestampTracker,
) -> Result<(), EngineError> {
    let mut service = db.json(branch.clone(), space.clone())?;
    let mut encoder = SectionEncoder::default();
    let mut cursor: Option<JsonDocumentId> = None;
    loop {
        let page = service.list(None, cursor.as_ref(), EXPORT_PAGE)?;
        for id in page.document_ids() {
            let Some(versioned) = service.get_versioned(id, &JsonPath::root())? else {
                continue;
            };
            let doc = encode_json(versioned.value());
            tracker.observe(versioned.timestamp());
            encoder.record(|body| {
                put_str(body, id.as_str());
                put_bytes(body, &doc);
                put_u64(body, versioned.timestamp().as_micros());
            });
        }
        cursor = page.cursor().cloned();
        if !page.has_more() {
            break;
        }
    }
    encoder.finish(space, ArtifactModel::Json, None, sections);
    Ok(())
}

fn export_events(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    sections: &mut Vec<ArtifactSection>,
    tracker: &mut TimestampTracker,
) -> Result<(), EngineError> {
    let mut service = db.event(branch.clone(), space.clone())?;
    let mut encoder = SectionEncoder::default();
    let mut start = EventSequence::new(0);
    loop {
        let page = service.range(
            start,
            None,
            Some(EXPORT_PAGE),
            EventRangeDirection::Forward,
            None,
        )?;
        for versioned in page.events() {
            let record = versioned.record();
            let payload = encode_json(record.payload().as_inner());
            tracker.observe(record.timestamp());
            encoder.record(|body| {
                put_u64(body, record.sequence().as_u64());
                put_str(body, record.event_type().as_str());
                put_bytes(body, &payload);
                put_u64(body, record.timestamp().as_micros());
            });
        }
        let Some(cursor) = page.cursor() else {
            break;
        };
        if !page.has_more() {
            break;
        }
        start = EventSequence::new(cursor.as_u64().saturating_add(1));
    }
    encoder.finish(space, ArtifactModel::Event, None, sections);
    Ok(())
}

fn export_vector_collections(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    sections: &mut Vec<ArtifactSection>,
    tracker: &mut TimestampTracker,
) -> Result<(), EngineError> {
    let mut service = db.vector(branch.clone(), space.clone())?;
    let mut collections = service.list_collections()?;
    collections.sort_by(|a, b| a.name().as_str().cmp(b.name().as_str()));

    for info in collections {
        let mut encoder = SectionEncoder::default();
        let config = encode_json(&info.config());
        tracker.observe(info.created_timestamp());
        encoder.record(|body| {
            put_bytes(body, &config);
            put_u64(body, info.created_timestamp().as_micros());
        });

        let mut cursor = None;
        loop {
            let page = service.list_keys(info.name(), None, cursor.as_ref(), EXPORT_PAGE)?;
            for key in page.keys() {
                let Some(versioned) = service.get_versioned(info.name(), key)? else {
                    continue;
                };
                let entry = versioned.entry();
                let metadata = entry
                    .metadata()
                    .map(|metadata| encode_json(metadata.as_inner()));
                tracker.observe(versioned.timestamp());
                encoder.record(|body| {
                    put_str(body, key.as_str());
                    put_f32s(body, entry.embedding().as_slice());
                    put_opt_bytes(body, metadata.as_deref());
                    put_u64(body, versioned.timestamp().as_micros());
                });
            }
            cursor = page.cursor().cloned();
            if !page.has_more() {
                break;
            }
        }
        encoder.finish(
            space,
            ArtifactModel::Vector,
            Some(info.name().as_str().to_owned()),
            sections,
        );
    }
    Ok(())
}

fn export_graphs(
    db: &mut Database,
    branch: &BranchName,
    space: &ProductSpace,
    sections: &mut Vec<ArtifactSection>,
    tracker: &mut TimestampTracker,
) -> Result<(), EngineError> {
    let mut service = db.graph(branch.clone(), space.clone())?;
    let mut graphs: Vec<GraphName> = Vec::new();
    let mut cursor: Option<GraphName> = None;
    loop {
        let page = service.list_graphs(cursor.as_ref(), EXPORT_PAGE)?;
        graphs.extend(page.graphs().iter().cloned());
        cursor = page.cursor().cloned();
        if !page.has_more() {
            break;
        }
    }
    graphs.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    for graph in graphs {
        let mut encoder = SectionEncoder::default();
        // The ontology read shape carries the commit facts of the row that
        // produced it (version/timestamp). Those are branch-local
        // coordinates, not logical content — strip them so artifact bytes
        // are a pure function of ontology definitions and status.
        let ontology = service.ontology(&graph)?.map(|ontology| {
            let mut value =
                serde_json::to_value(&ontology).expect("artifact payload values serialize");
            if let Some(object) = value.as_object_mut() {
                object.remove("version");
                object.remove("timestamp");
            }
            encode_json(&value)
        });
        encoder.record(|body| {
            put_opt_bytes(body, ontology.as_deref());
        });

        // Nodes in id order; each node's outgoing edges follow it, in the
        // adjacency order the graph service guarantees (edge type, then
        // destination). Every edge appears exactly once, from its source.
        let mut node_cursor = None;
        loop {
            let page = service.list_nodes(&graph, None, node_cursor.as_ref(), EXPORT_PAGE)?;
            for node in page.nodes() {
                let data = encode_json(node.data());
                tracker.observe(node.timestamp());
                encoder.record(|body| {
                    body.push(0);
                    put_str(body, node.node_id().as_str());
                    put_bytes(body, &data);
                    put_u64(body, node.timestamp().as_micros());
                });

                let mut edge_cursor: Option<String> = None;
                loop {
                    let edges = service.neighbors(
                        &graph,
                        node.node_id(),
                        GraphDirection::Outgoing,
                        None,
                        edge_cursor.as_deref(),
                        EXPORT_PAGE,
                    )?;
                    for neighbor in edges.neighbors() {
                        let edge = neighbor.edge();
                        let data = encode_json(edge.data());
                        tracker.observe(edge.timestamp());
                        encoder.record(|body| {
                            body.push(1);
                            put_str(body, edge.src().as_str());
                            put_str(body, edge.edge_type().as_str());
                            put_str(body, edge.dst().as_str());
                            put_bytes(body, &data);
                            put_u64(body, edge.timestamp().as_micros());
                        });
                    }
                    edge_cursor = edges.cursor().map(str::to_owned);
                    if !edges.has_more() {
                        break;
                    }
                }
            }
            node_cursor = page.cursor().cloned();
            if !page.has_more() {
                break;
            }
        }
        encoder.finish(
            space,
            ArtifactModel::Graph,
            Some(graph.as_str().to_owned()),
            sections,
        );
    }
    Ok(())
}

/// Byte-lexicographic successor: the smallest key strictly greater than
/// `key`, used to advance inclusive-start scans without revisiting.
fn successor_key(key: &KvKey) -> Result<KvKey, EngineError> {
    let mut bytes = key.as_bytes().to_vec();
    bytes.push(0);
    KvKey::new(bytes)
}

/// `serde_json` serialization of in-memory engine values cannot fail for
/// well-formed data (no non-string map keys reach this path), so encoding
/// panics rather than plumbing an unreachable error.
fn encode_json<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("artifact payload values serialize")
}

#[derive(Default)]
struct TimestampTracker {
    max: Option<Timestamp>,
}

impl TimestampTracker {
    fn observe(&mut self, timestamp: Timestamp) {
        self.max = Some(self.max.map_or(timestamp, |max| max.max(timestamp)));
    }
}

#[derive(Default)]
struct SectionEncoder {
    bytes: Vec<u8>,
    record_count: u64,
    scratch: Vec<u8>,
}

impl SectionEncoder {
    /// Frames one record: `u32` LE body length, then the body.
    fn record(&mut self, fill: impl FnOnce(&mut Vec<u8>)) {
        self.scratch.clear();
        fill(&mut self.scratch);
        let len = u32::try_from(self.scratch.len()).expect("record fits u32 framing");
        self.bytes.extend_from_slice(&len.to_le_bytes());
        self.bytes.extend_from_slice(&self.scratch);
        self.record_count += 1;
    }

    /// Emits the section unless it is empty (empty sections carry no
    /// information — space existence is recorded on the artifact itself).
    fn finish(
        self,
        space: &ProductSpace,
        model: ArtifactModel,
        qualifier: Option<String>,
        sections: &mut Vec<ArtifactSection>,
    ) {
        if self.record_count == 0 {
            return;
        }
        sections.push(ArtifactSection::new(
            space.clone(),
            model,
            qualifier,
            self.record_count,
            self.bytes,
        ));
    }
}

fn put_u32(body: &mut Vec<u8>, value: u32) {
    body.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(body: &mut Vec<u8>, value: u64) {
    body.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(body: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(
        body,
        u32::try_from(bytes.len()).expect("field fits u32 framing"),
    );
    body.extend_from_slice(bytes);
}

fn put_str(body: &mut Vec<u8>, value: &str) {
    put_bytes(body, value.as_bytes());
}

fn put_opt_bytes(body: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            body.push(1);
            put_bytes(body, bytes);
        }
        None => body.push(0),
    }
}

fn put_f32s(body: &mut Vec<u8>, values: &[f32]) {
    put_u32(
        body,
        u32::try_from(values.len()).expect("embedding fits u32 framing"),
    );
    for value in values {
        body.extend_from_slice(&value.to_le_bytes());
    }
}
