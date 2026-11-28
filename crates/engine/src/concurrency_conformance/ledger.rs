//! What the harness is known not to hold yet, and what it does not run.
//! Every list here only shrinks.

/// Ordered pairs `first || second` whose interleaving matches neither serial
/// order, each with the issue that tracks it. A fix deletes entries; the
/// suite fails on an entry that has become serializable.
pub(super) const KNOWN_GAPS: &[(&str, u32)] = &[
    // batch readers: 1 pairs (#3757)
    ("read kv.batch_exists(k, j) || kv.batch_delete(k, j)", 3757),
    // event readers: 3 pairs (#3783)
    ("read event.verify_chain || event.append(x)", 3783),
    ("read event.verify_chain || event.append(y)", 3783),
    ("read event.verify_chain || event.batch_append(x, z)", 3783),
    // graph readers: 8 pairs (#3774)
    ("read graph.neighbors(a) || graph.delete(g, force)", 3774),
    ("read graph.neighbors(a) || graph.node.remove(b)", 3774),
    ("read graph.node.get(b) || graph.delete(g, force)", 3774),
    (
        "read graph.nodes_by_type(T) || graph.delete(g, force)",
        3774,
    ),
    ("read graph.nodes_by_type(T) || graph.node.remove(b)", 3774),
    (
        "read graph.ontology.summary || graph.delete(g, force)",
        3774,
    ),
    (
        "read graph.ontology.summary || graph.node.add(a as V)",
        3774,
    ),
    ("read graph.ontology.summary || graph.node.remove(b)", 3774),
];

/// Ordered pairs whose interleaving matches neither serial order, but which
/// the operation's contract allows, with the contract. They must stay red:
/// a listed pair that has become serializable fails the suite, like a
/// healed gap.
pub(super) const BY_CONTRACT: &[(&str, &str)] = &[(
    "graph.bulk_insert(p, p->c) || read graph.meta",
    "bulk_insert commits per chunk (#3464), so a reader between two chunks sees the import part-done",
)];

/// Mutating commands two threads cannot run at once today, with the reason.
pub(super) const NOT_CONCURRENT: &[(&str, &str)] = &[
    ("branch.create", "Database::branches takes &mut self"),
    ("branch.delete", "Database::branches takes &mut self"),
    ("branch.fork", "Database::branches takes &mut self"),
    (
        "branch.fork_at_timestamp",
        "Database::branches takes &mut self",
    ),
    (
        "branch.fork_at_version",
        "Database::branches takes &mut self",
    ),
    ("branch.merge", "Database::branches takes &mut self"),
    ("space.create", "Database::spaces takes &mut self"),
    ("space.delete", "Database::spaces takes &mut self"),
    ("vector.batch_delete", "Database::vector takes &mut self"),
    ("vector.batch_upsert", "Database::vector takes &mut self"),
    (
        "vector.collection.create",
        "Database::vector takes &mut self",
    ),
    (
        "vector.collection.delete",
        "Database::vector takes &mut self",
    ),
    (
        "vector.collection.set_embedding_model",
        "Database::vector takes &mut self",
    ),
    ("vector.delete", "Database::vector takes &mut self"),
    ("vector.delete_all", "Database::vector takes &mut self"),
    (
        "vector.delete_by_filter",
        "Database::vector takes &mut self",
    ),
    (
        "vector.embedding.update",
        "Database::vector takes &mut self",
    ),
    ("vector.metadata.update", "Database::vector takes &mut self"),
    ("vector.upsert", "Database::vector takes &mut self"),
];

/// Concurrent mutating commands whose catalog is still to be written.
pub(super) const NOT_YET_CATALOGUED: &[(&str, &str)] = &[];
