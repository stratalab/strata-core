# Ontology Layer

**Extends:** `strata-graph` PRD
**Status:** Draft
**Principle:** Strata stores, indexes, and retrieves. It never decides, acts, or interprets. The AI defines the ontology and creates instances. Strata makes that structure queryable.

---

## 1. What This Is

The ontology is a **user-defined type system** for the graph. It gives nodes and edges meaning — not just "a string ID with properties" but "a Patient with a date_of_birth and an mrn, linked to Lab Results via HAS_LAB_RESULT."

The graph PRD provides the mechanical layer: nodes, edges, traversal, branching. The ontology provides the semantic layer: what the nodes and edges *are*, what properties they carry, what relationships are valid, and what constraints apply.

Without the ontology, the graph is a bag of strings. With it, the graph becomes a typed knowledge structure that Strata can index, validate, and retrieve intelligently.

### What the Ontology Is Not

- **Not an agent.** Strata doesn't decide what object types exist or what relationships to create. The AI or user defines the ontology; Strata stores and enforces it.
- **Not a business logic layer.** No Actions, no Functions, no side effects, no orchestration. The AI calls graph operations directly. Strata validates them against the ontology.
- **Not a query language.** No Cypher, no SPARQL, no GraphQL. The existing graph API (neighbors, BFS, subgraph) works against typed nodes and edges. The ontology makes those queries return structured, typed results instead of raw property bags.
- **Not mandatory.** Untyped graphs (as described in the graph PRD) continue to work. The ontology is opt-in per graph.

---

## 2. Core Concepts

### Object Types

An object type is a named schema for a category of nodes. It defines what properties instances must or may have.

```json
{
  "name": "Patient",
  "properties": {
    "mrn": { "type": "string", "required": true },
    "date_of_birth": { "type": "string" },
    "department": { "type": "string" }
  }
}
```

Object types are stored in the graph's meta namespace:

```
_graph_/{graph_name}/__types__/object/{type_name} → schema (JSON)
```

When a node declares an object type, Strata validates its properties against the schema on write. Missing required properties are rejected. Unknown properties are allowed (open schema by default) or rejected (closed schema, opt-in per type).

### Link Types

A link type is a named schema for a category of edges. It defines which object types it connects, its cardinality, and what properties the edge may carry.

```json
{
  "name": "HAS_LAB_RESULT",
  "source": "Patient",
  "target": "LabResult",
  "cardinality": "one_to_many",
  "properties": {
    "ordered_by": { "type": "string" },
    "ordered_date": { "type": "string" }
  }
}
```

Stored at:

```
_graph_/{graph_name}/__types__/link/{type_name} → schema (JSON)
```

When an edge is created with a declared link type, Strata validates that the source and target nodes match the expected object types, and that edge properties conform to the schema. This prevents structural nonsense — a LabResult DIAGNOSED_WITH a Medication — without Strata needing to understand what any of it means.

### Cardinality

Link types declare cardinality constraints:

| Cardinality | Meaning | Enforcement |
|-------------|---------|-------------|
| `one_to_one` | Source has at most one target of this link type | Reject duplicate edges from same source |
| `one_to_many` | Source can have many targets | No constraint on target count |
| `many_to_one` | Many sources can link to one target | No constraint on source count |
| `many_to_many` | No constraints | Default if cardinality is omitted |

Cardinality is enforced on write. The AI decides the cardinality when defining the link type. Strata enforces it.

### Instances

A typed node is an instance of an object type. In the graph API, this means:

```python
graph.add_node("patient-4821", object_type="Patient", properties={
    "mrn": "4821",
    "date_of_birth": "1974-03-15",
    "department": "endocrinology"
}, entity_ref="json://main/patient-4821")
```

The `object_type` field in node metadata links the node to its schema. If the graph has an ontology with a `Patient` type, the properties are validated against it. If there's no ontology or no matching type, the node is stored as an untyped node (backward compatible with the graph PRD).

A typed edge is an instance of a link type:

```python
graph.add_edge("patient-4821", "lab:HbA1c", "HAS_LAB_RESULT",
    properties={"ordered_by": "Dr. Chen", "ordered_date": "2026-01-15"})
```

If `HAS_LAB_RESULT` is a declared link type, Strata validates that the source is a Patient and the target is a LabResult, and that the edge properties match the schema.

---

## 3. The Ontology Is Data

The ontology itself is stored in Strata, as KV entries within the graph's key namespace. This means:

**Versioned.** Every change to the ontology is a versioned write. You can see how the type system evolved over time via `as_of` queries on the schema entries.

**Branchable.** Fork the branch, modify the ontology on the fork (add a property, change a cardinality), test it, merge or discard. The ontology participates in branching exactly like data does. This is what makes schema evolution safe — you never modify the production ontology in place.

**Time-travelable.** Query the ontology as it was at any past timestamp. Combined with time-travel on the data itself, you can see what the type system looked like when a particular piece of data was stored.

**Diffable.** Branch diff on `_graph_/{g}/__types__/` shows exactly what changed in the ontology between two branches.

No separate ontology management system. No migration tooling. The ontology is data, and Strata already knows how to version, branch, and diff data.

---

## 4. Schema Evolution

Because the ontology is branchable, schema evolution follows the same workflow as data experimentation:

1. **Fork** the current branch
2. **Modify** the ontology on the fork — add a property, change a cardinality, add a new object type
3. **Test** — create instances against the new schema, verify queries work
4. **Merge** or **discard**

### Adding Properties

Adding a new optional property to an object type is non-breaking. Existing nodes don't have the property and that's fine — it's optional.

Adding a new required property is breaking. Existing nodes will fail validation on the next write. The AI (not Strata) is responsible for deciding how to handle this — backfill the property, make it optional, or accept that existing nodes are grandfathered.

### Removing Properties

Removing a property from the schema doesn't delete it from existing nodes. The property data persists in KV; it just isn't part of the schema anymore. This is consistent with Strata's principle that nothing is ever lost.

### Changing Types or Cardinality

These are breaking changes. The recommended workflow is: fork, modify, validate, merge. Strata doesn't automatically migrate data — the AI does that if it needs to.

---

## 5. Ontology-Grounded Retrieval

The ontology changes what search returns. Without it, `strata_search` returns `{ key, score, snippet }` — flat results. With it, search can return **typed objects with their relationships**.

### How It Works

When a search hit matches a node that has an `object_type`, the search result can be enriched:

1. The node's object type tells Strata what this result *is* (a Patient, a LabResult, a Diagnosis)
2. The link types tell Strata what's connected to it and how
3. The result includes not just the matching data but its typed context — the object type, its properties, and its immediate relationships

This is the difference between returning `{ key: "patient-4821", score: 0.92, snippet: "..." }` and returning:

```json
{
  "key": "patient-4821",
  "score": 0.92,
  "object_type": "Patient",
  "properties": { "mrn": "4821", "department": "endocrinology" },
  "links": {
    "HAS_LAB_RESULT": ["lab:HbA1c", "lab:eGFR"],
    "DIAGNOSED_WITH": ["ICD:E11.9"]
  }
}
```

The AI receives structured, typed context instead of text snippets. This is what Palantir calls Ontology-Augmented Generation — passing typed objects to the LLM rather than raw chunks. Strata provides the structure; the AI reasons over it.

### Depth Control

How much context to include is controlled by the caller:

- `depth: 0` — just the matching node with its type and properties (default)
- `depth: 1` — include immediate neighbors with their types
- `depth: 2` — include 2-hop neighborhood

Strata doesn't decide what depth is appropriate. The AI does.

---

## 6. Storage Model

The ontology extends the graph's existing KV key namespace:

```
_graph_/{graph_name}/__types__/object/{type_name}   → object type schema (JSON)
_graph_/{graph_name}/__types__/link/{type_name}      → link type schema (JSON)
```

Node metadata gains an optional `object_type` field:

```json
{
  "object_type": "Patient",
  "entity_ref": "json://main/patient-4821",
  "properties": {
    "mrn": "4821",
    "date_of_birth": "1974-03-15"
  }
}
```

Edge payloads are unchanged from the graph PRD. The edge type string (e.g., `HAS_LAB_RESULT`) is matched against declared link types for validation. If no matching link type is declared, the edge is untyped (backward compatible).

### Indexes

Object type membership is implicitly indexed by the existing node metadata. To efficiently query "all Patients," prefix scan on `_graph_/{g}/n/` and filter by `object_type`. If this becomes a bottleneck, a secondary index can be added:

```
_graph_/{graph_name}/__by_type__/{object_type}/{node_id} → ""
```

This follows the same pattern as the reverse EntityRef index in the graph PRD.

---

## 7. API Surface

### Ontology Management

```python
# Define object types
graph.define_object_type("Patient", properties={
    "mrn": {"type": "string", "required": True},
    "date_of_birth": {"type": "string"},
    "department": {"type": "string"},
})

graph.define_object_type("LabResult", properties={
    "test_name": {"type": "string", "required": True},
    "value": {"type": "string", "required": True},
    "unit": {"type": "string"},
    "reference_range": {"type": "string"},
})

# Define link types
graph.define_link_type("HAS_LAB_RESULT",
    source="Patient", target="LabResult",
    cardinality="one_to_many",
    properties={"ordered_by": {"type": "string"}})

# List types
graph.object_types()   # → ["Patient", "LabResult", ...]
graph.link_types()     # → ["HAS_LAB_RESULT", ...]

# Get type schema
graph.get_object_type("Patient")  # → schema JSON
graph.get_link_type("HAS_LAB_RESULT")  # → schema JSON
```

### Typed Node and Edge Operations

```python
# Create a typed node (validated against Patient schema)
graph.add_node("patient-4821", object_type="Patient", properties={
    "mrn": "4821",
    "date_of_birth": "1974-03-15",
    "department": "endocrinology",
}, entity_ref="json://main/patient-4821")

# Create a typed edge (validated against HAS_LAB_RESULT schema)
graph.add_edge("patient-4821", "lab:HbA1c", "HAS_LAB_RESULT",
    properties={"ordered_by": "Dr. Chen"})

# Query by object type
graph.nodes_by_type("Patient")  # → all Patient nodes
graph.nodes_by_type("LabResult", filter={"test_name": "HbA1c"})
```

### Untyped Operations (Backward Compatible)

All existing graph PRD operations work unchanged. Nodes without `object_type` are untyped. Edges with undeclared edge types are untyped. The ontology is additive — it enhances graphs that use it without breaking graphs that don't.

---

## 8. What the AI Does vs. What Strata Does

| Responsibility | Who |
|---------------|-----|
| Decide what object types exist | AI / user |
| Define properties and constraints | AI / user |
| Decide what link types exist and their cardinality | AI / user |
| Create, update, delete instances | AI / user |
| Decide when to evolve the schema | AI / user |
| Decide what to store and how to structure it | AI / user |
| Decide what depth of context to retrieve | AI / user |
| Store type definitions as versioned data | Strata |
| Validate instances against declared schemas on write | Strata |
| Enforce cardinality constraints | Strata |
| Index nodes by object type for efficient queries | Strata |
| Enrich search results with typed context and relationships | Strata |
| Branch, version, time-travel the ontology like any other data | Strata |

Strata is the filing system. A very good filing system that understands the structure of what's filed, validates it, indexes it, and retrieves it with context. But it doesn't decide what to file or why.

---

## 9. Relationship to Cognitive Architecture Layers

The ontology spans layers 2 and 3 of the cognitive architecture:

**Layer 2 contribution:** The ontology gives the graph structure meaning. Typed nodes and edges with schemas and cardinality constraints turn the raw wiring into a semantic structure. This is the mechanical foundation — without it, the graph is just strings pointing at strings.

**Layer 3 contribution:** Ontology-grounded retrieval changes what search returns. Instead of flat results, the AI receives typed objects with their relationships. This is what enables the AI to reason over structure rather than text. The ontology also provides the vocabulary for automatic association — when layer 3 infers relationships, it does so in terms of declared link types with known cardinality, not arbitrary string edges.

**Layer 4 implication:** When the cognitive interface matures, the ontology is what makes "what do we know about patient 4821?" return a structured answer traversing typed relationships, rather than a list of keyword matches. The AI defines the ontology; the ontology structures what Strata returns; the AI reasons over the structure. The loop is: cortex defines → hippocampus stores and organizes → cortex retrieves and reasons.
