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
    // graph: 132 pairs (#3779)
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.batch_write(m, m->a)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.bulk_insert(p, p->c)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.delete(g, force)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.edge.add(a->c)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.edge.add(c->a)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.edge.remove(a->b)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.node.add(n)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.node.remove(b)",
        3779,
    ),
    (
        "graph.apply_delete_policy(doc-1, cascade) || graph.node.remove(c)",
        3779,
    ),
    (
        "graph.batch_write(m, m->a) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    (
        "graph.batch_write(m, m->a) || graph.batch_write(m, m->a)",
        3779,
    ),
    (
        "graph.batch_write(m, m->a) || graph.bulk_insert(p, p->c)",
        3779,
    ),
    ("graph.batch_write(m, m->a) || graph.delete(g, force)", 3779),
    ("graph.batch_write(m, m->a) || graph.edge.add(a->c)", 3779),
    ("graph.batch_write(m, m->a) || graph.edge.add(c->a)", 3779),
    (
        "graph.batch_write(m, m->a) || graph.edge.remove(a->b)",
        3779,
    ),
    ("graph.batch_write(m, m->a) || graph.node.add(n)", 3779),
    ("graph.batch_write(m, m->a) || graph.node.remove(b)", 3779),
    ("graph.batch_write(m, m->a) || graph.node.remove(c)", 3779),
    (
        "graph.bulk_insert(p, p->c) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    (
        "graph.bulk_insert(p, p->c) || graph.batch_write(m, m->a)",
        3779,
    ),
    (
        "graph.bulk_insert(p, p->c) || graph.bulk_insert(p, p->c)",
        3779,
    ),
    ("graph.bulk_insert(p, p->c) || graph.delete(g, force)", 3779),
    ("graph.bulk_insert(p, p->c) || graph.edge.add(a->c)", 3779),
    ("graph.bulk_insert(p, p->c) || graph.edge.add(c->a)", 3779),
    (
        "graph.bulk_insert(p, p->c) || graph.edge.remove(a->b)",
        3779,
    ),
    ("graph.bulk_insert(p, p->c) || graph.node.add(n)", 3779),
    ("graph.bulk_insert(p, p->c) || graph.node.remove(b)", 3779),
    ("graph.bulk_insert(p, p->c) || graph.node.remove(c)", 3779),
    ("graph.delete(e) || graph.delete(e)", 3779),
    ("graph.delete(e) || graph.node.add(e: x)", 3779),
    ("graph.delete(g) || graph.delete(g, force)", 3779),
    ("graph.delete(g, force) || graph.delete(g, force)", 3779),
    (
        "graph.edge.add(a->c) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.edge.add(a->c) || graph.batch_write(m, m->a)", 3779),
    ("graph.edge.add(a->c) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.edge.add(a->c) || graph.delete(g, force)", 3779),
    ("graph.edge.add(a->c) || graph.edge.add(a->c)", 3779),
    ("graph.edge.add(a->c) || graph.edge.add(c->a)", 3779),
    ("graph.edge.add(a->c) || graph.edge.remove(a->b)", 3779),
    ("graph.edge.add(a->c) || graph.node.add(n)", 3779),
    ("graph.edge.add(a->c) || graph.node.remove(b)", 3779),
    ("graph.edge.add(a->c) || graph.node.remove(c)", 3779),
    (
        "graph.edge.add(c->a) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.edge.add(c->a) || graph.batch_write(m, m->a)", 3779),
    ("graph.edge.add(c->a) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.edge.add(c->a) || graph.delete(g, force)", 3779),
    ("graph.edge.add(c->a) || graph.edge.add(a->c)", 3779),
    ("graph.edge.add(c->a) || graph.edge.add(c->a)", 3779),
    ("graph.edge.add(c->a) || graph.edge.remove(a->b)", 3779),
    ("graph.edge.add(c->a) || graph.node.add(n)", 3779),
    ("graph.edge.add(c->a) || graph.node.remove(b)", 3779),
    ("graph.edge.add(c->a) || graph.node.remove(c)", 3779),
    (
        "graph.edge.remove(a->b) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    (
        "graph.edge.remove(a->b) || graph.batch_write(m, m->a)",
        3779,
    ),
    (
        "graph.edge.remove(a->b) || graph.bulk_insert(p, p->c)",
        3779,
    ),
    ("graph.edge.remove(a->b) || graph.delete(g, force)", 3779),
    ("graph.edge.remove(a->b) || graph.edge.add(a->c)", 3779),
    ("graph.edge.remove(a->b) || graph.edge.add(c->a)", 3779),
    ("graph.edge.remove(a->b) || graph.edge.remove(a->b)", 3779),
    ("graph.edge.remove(a->b) || graph.node.add(n)", 3779),
    ("graph.edge.remove(a->b) || graph.node.remove(b)", 3779),
    ("graph.edge.remove(a->b) || graph.node.remove(c)", 3779),
    (
        "graph.node.add(a as V) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.node.add(a as V) || graph.batch_write(m, m->a)", 3779),
    ("graph.node.add(a as V) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.node.add(a as V) || graph.delete(g, force)", 3779),
    ("graph.node.add(a as V) || graph.edge.add(a->c)", 3779),
    ("graph.node.add(a as V) || graph.edge.add(c->a)", 3779),
    ("graph.node.add(a as V) || graph.edge.remove(a->b)", 3779),
    ("graph.node.add(a as V) || graph.node.add(n)", 3779),
    ("graph.node.add(a as V) || graph.node.remove(b)", 3779),
    ("graph.node.add(a as V) || graph.node.remove(c)", 3779),
    ("graph.node.add(e: x) || graph.delete(e)", 3779),
    ("graph.node.add(e: x) || graph.node.add(e: x)", 3779),
    (
        "graph.node.add(n) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.node.add(n) || graph.batch_write(m, m->a)", 3779),
    ("graph.node.add(n) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.node.add(n) || graph.delete(g, force)", 3779),
    ("graph.node.add(n) || graph.edge.add(a->c)", 3779),
    ("graph.node.add(n) || graph.edge.add(c->a)", 3779),
    ("graph.node.add(n) || graph.edge.remove(a->b)", 3779),
    ("graph.node.add(n) || graph.node.add(n)", 3779),
    ("graph.node.add(n) || graph.node.remove(b)", 3779),
    ("graph.node.add(n) || graph.node.remove(c)", 3779),
    (
        "graph.node.remove(b) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.node.remove(b) || graph.batch_write(m, m->a)", 3779),
    ("graph.node.remove(b) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.node.remove(b) || graph.delete(g, force)", 3779),
    ("graph.node.remove(b) || graph.edge.add(a->c)", 3779),
    ("graph.node.remove(b) || graph.edge.add(c->a)", 3779),
    ("graph.node.remove(b) || graph.edge.remove(a->b)", 3779),
    ("graph.node.remove(b) || graph.node.add(n)", 3779),
    ("graph.node.remove(b) || graph.node.remove(b)", 3779),
    ("graph.node.remove(b) || graph.node.remove(c)", 3779),
    (
        "graph.node.remove(c) || graph.apply_delete_policy(doc-1, cascade)",
        3779,
    ),
    ("graph.node.remove(c) || graph.batch_write(m, m->a)", 3779),
    ("graph.node.remove(c) || graph.bulk_insert(p, p->c)", 3779),
    ("graph.node.remove(c) || graph.delete(g, force)", 3779),
    ("graph.node.remove(c) || graph.edge.add(a->c)", 3779),
    ("graph.node.remove(c) || graph.edge.add(c->a)", 3779),
    ("graph.node.remove(c) || graph.edge.remove(a->b)", 3779),
    ("graph.node.remove(c) || graph.node.add(n)", 3779),
    ("graph.node.remove(c) || graph.node.remove(b)", 3779),
    ("graph.node.remove(c) || graph.node.remove(c)", 3779),
    (
        "graph.ontology.define_link_type(M) || graph.ontology.define_object_type(U)",
        3779,
    ),
    (
        "graph.ontology.define_link_type(M) || graph.ontology.delete_link_type(L)",
        3779,
    ),
    (
        "graph.ontology.define_link_type(M) || graph.ontology.delete_object_type(V)",
        3779,
    ),
    (
        "graph.ontology.define_link_type(M) || graph.ontology.freeze",
        3779,
    ),
    (
        "graph.ontology.define_object_type(U) || graph.ontology.define_link_type(M)",
        3779,
    ),
    (
        "graph.ontology.define_object_type(U) || graph.ontology.delete_link_type(L)",
        3779,
    ),
    (
        "graph.ontology.define_object_type(U) || graph.ontology.delete_object_type(V)",
        3779,
    ),
    (
        "graph.ontology.define_object_type(U) || graph.ontology.freeze",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.delete(g, force)",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.ontology.define_link_type(M)",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.ontology.define_object_type(U)",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.ontology.delete_link_type(L)",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.ontology.delete_object_type(V)",
        3779,
    ),
    (
        "graph.ontology.delete_link_type(L) || graph.ontology.freeze",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.delete(g, force)",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.ontology.define_link_type(M)",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.ontology.define_object_type(U)",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.ontology.delete_link_type(L)",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.ontology.delete_object_type(V)",
        3779,
    ),
    (
        "graph.ontology.delete_object_type(V) || graph.ontology.freeze",
        3779,
    ),
    ("graph.ontology.freeze || graph.delete(g, force)", 3779),
    (
        "graph.ontology.freeze || graph.ontology.define_link_type(M)",
        3779,
    ),
    (
        "graph.ontology.freeze || graph.ontology.define_object_type(U)",
        3779,
    ),
    (
        "graph.ontology.freeze || graph.ontology.delete_link_type(L)",
        3779,
    ),
    (
        "graph.ontology.freeze || graph.ontology.delete_object_type(V)",
        3779,
    ),
    ("graph.ontology.freeze || graph.ontology.freeze", 3779),
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
    // json: 62 pairs (#3777)
    ("json.batch_delete(d, e) || json.batch_delete(d, e)", 3777),
    ("json.batch_delete(d, e) || json.batch_set(d, e)", 3777),
    ("json.batch_delete(d, e) || json.delete(d)", 3777),
    ("json.batch_delete(d, e) || json.set(d, $.a)", 3777),
    ("json.batch_set(d, e) || json.batch_delete(d, e)", 3777),
    ("json.batch_set(d, e) || json.batch_set(d, e)", 3777),
    ("json.batch_set(d, e) || json.create(e)", 3777),
    ("json.batch_set(d, e) || json.delete(d)", 3777),
    ("json.batch_set(d, e) || json.delete(d, $.b)", 3777),
    ("json.batch_set(d, e) || json.index.drop(by_a)", 3777),
    ("json.batch_set(d, e) || json.set(d, $.a)", 3777),
    ("json.batch_set(d, e) || json.set(d, $.x)", 3777),
    ("json.batch_set(d, e) || json.set(d, $.y)", 3777),
    ("json.batch_set(d, e) || json.set_or_create(e)", 3777),
    ("json.create(e) || json.batch_set(d, e)", 3777),
    ("json.create(e) || json.create(e)", 3777),
    ("json.create(e) || json.index.drop(by_a)", 3777),
    ("json.create(e) || json.set_or_create(e)", 3777),
    ("json.delete(d) || json.batch_delete(d, e)", 3777),
    ("json.delete(d) || json.delete(d)", 3777),
    ("json.delete(d) || json.set(d, $.a)", 3777),
    ("json.delete(d, $.b) || json.batch_delete(d, e)", 3777),
    ("json.delete(d, $.b) || json.batch_set(d, e)", 3777),
    ("json.delete(d, $.b) || json.delete(d)", 3777),
    ("json.delete(d, $.b) || json.delete(d, $.b)", 3777),
    ("json.delete(d, $.b) || json.set(d, $.a)", 3777),
    ("json.delete(d, $.b) || json.set(d, $.x)", 3777),
    ("json.delete(d, $.b) || json.set(d, $.y)", 3777),
    ("json.index.create(by_x) || json.index.create(by_x)", 3777),
    ("json.index.create(by_x) || json.set(d, $.x)", 3777),
    ("json.index.drop(by_a) || json.batch_set(d, e)", 3777),
    ("json.index.drop(by_a) || json.create(e)", 3777),
    ("json.index.drop(by_a) || json.index.drop(by_a)", 3777),
    ("json.index.drop(by_a) || json.set(d, $.a)", 3777),
    ("json.index.drop(by_a) || json.set_or_create(e)", 3777),
    ("json.set(d, $.a) || json.batch_delete(d, e)", 3777),
    ("json.set(d, $.a) || json.batch_set(d, e)", 3777),
    ("json.set(d, $.a) || json.delete(d)", 3777),
    ("json.set(d, $.a) || json.delete(d, $.b)", 3777),
    ("json.set(d, $.a) || json.index.drop(by_a)", 3777),
    ("json.set(d, $.a) || json.set(d, $.a)", 3777),
    ("json.set(d, $.a) || json.set(d, $.x)", 3777),
    ("json.set(d, $.a) || json.set(d, $.y)", 3777),
    ("json.set(d, $.x) || json.batch_delete(d, e)", 3777),
    ("json.set(d, $.x) || json.batch_set(d, e)", 3777),
    ("json.set(d, $.x) || json.delete(d)", 3777),
    ("json.set(d, $.x) || json.delete(d, $.b)", 3777),
    ("json.set(d, $.x) || json.index.create(by_x)", 3777),
    ("json.set(d, $.x) || json.set(d, $.a)", 3777),
    ("json.set(d, $.x) || json.set(d, $.x)", 3777),
    ("json.set(d, $.x) || json.set(d, $.y)", 3777),
    ("json.set(d, $.y) || json.batch_delete(d, e)", 3777),
    ("json.set(d, $.y) || json.batch_set(d, e)", 3777),
    ("json.set(d, $.y) || json.delete(d)", 3777),
    ("json.set(d, $.y) || json.delete(d, $.b)", 3777),
    ("json.set(d, $.y) || json.set(d, $.a)", 3777),
    ("json.set(d, $.y) || json.set(d, $.x)", 3777),
    ("json.set(d, $.y) || json.set(d, $.y)", 3777),
    ("json.set_or_create(e) || json.batch_set(d, e)", 3777),
    ("json.set_or_create(e) || json.create(e)", 3777),
    ("json.set_or_create(e) || json.index.drop(by_a)", 3777),
    ("json.set_or_create(e) || json.set_or_create(e)", 3777),
    // kv: 23 pairs (#3781)
    ("kv.batch_delete(k, j) || kv.batch_delete(k, j)", 3781),
    ("kv.batch_delete(k, j) || kv.batch_delete(k, n)", 3781),
    ("kv.batch_delete(k, j) || kv.delete(k)", 3781),
    ("kv.batch_delete(k, n) || kv.batch_delete(k, j)", 3781),
    ("kv.batch_delete(k, n) || kv.batch_delete(k, n)", 3781),
    ("kv.batch_delete(k, n) || kv.batch_put(k, n)", 3781),
    ("kv.batch_delete(k, n) || kv.delete(k)", 3781),
    ("kv.batch_put(k, n) || kv.batch_delete(k, j)", 3781),
    ("kv.batch_put(k, n) || kv.batch_delete(k, n)", 3781),
    ("kv.batch_put(k, n) || kv.batch_put(k, n)", 3781),
    ("kv.batch_put(k, n) || kv.delete(k)", 3781),
    ("kv.batch_put(k, n) || kv.put(n=n1)", 3781),
    ("kv.delete(k) || kv.batch_delete(k, j)", 3781),
    ("kv.delete(k) || kv.batch_delete(k, n)", 3781),
    ("kv.delete(k) || kv.delete(k)", 3781),
    ("kv.put(k=v1) || kv.batch_delete(k, j)", 3781),
    ("kv.put(k=v1) || kv.batch_delete(k, n)", 3781),
    ("kv.put(k=v1) || kv.delete(k)", 3781),
    ("kv.put(k=v2) || kv.batch_delete(k, j)", 3781),
    ("kv.put(k=v2) || kv.batch_delete(k, n)", 3781),
    ("kv.put(k=v2) || kv.delete(k)", 3781),
    ("kv.put(n=n1) || kv.batch_put(k, n)", 3781),
    ("kv.put(n=n1) || kv.put(n=n1)", 3781),
    // space registration: 21 pairs (#3778)
    (
        "event.append(new space s-event) || graph.create(new space s-graph)",
        3778,
    ),
    (
        "event.append(new space s-event) || json.set(new space s-json)",
        3778,
    ),
    (
        "event.append(new space s-event) || kv.put(new space s-kv)",
        3778,
    ),
    (
        "event.append(new space s-event) || kv.put(new space s-kv2)",
        3778,
    ),
    (
        "graph.create(new space s-graph) || event.append(new space s-event)",
        3778,
    ),
    (
        "graph.create(new space s-graph) || graph.create(new space s-graph)",
        3778,
    ),
    (
        "graph.create(new space s-graph) || json.set(new space s-json)",
        3778,
    ),
    (
        "graph.create(new space s-graph) || kv.put(new space s-kv)",
        3778,
    ),
    (
        "graph.create(new space s-graph) || kv.put(new space s-kv2)",
        3778,
    ),
    (
        "json.set(new space s-json) || event.append(new space s-event)",
        3778,
    ),
    (
        "json.set(new space s-json) || graph.create(new space s-graph)",
        3778,
    ),
    ("json.set(new space s-json) || kv.put(new space s-kv)", 3778),
    (
        "json.set(new space s-json) || kv.put(new space s-kv2)",
        3778,
    ),
    (
        "kv.put(new space s-kv) || event.append(new space s-event)",
        3778,
    ),
    (
        "kv.put(new space s-kv) || graph.create(new space s-graph)",
        3778,
    ),
    ("kv.put(new space s-kv) || json.set(new space s-json)", 3778),
    ("kv.put(new space s-kv) || kv.put(new space s-kv2)", 3778),
    (
        "kv.put(new space s-kv2) || event.append(new space s-event)",
        3778,
    ),
    (
        "kv.put(new space s-kv2) || graph.create(new space s-graph)",
        3778,
    ),
    (
        "kv.put(new space s-kv2) || json.set(new space s-json)",
        3778,
    ),
    ("kv.put(new space s-kv2) || kv.put(new space s-kv)", 3778),
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
