# Ontology Layer

**Extends:** `strata-graph` PRD
**Status:** Draft
**Principle:** Every database paradigm starts with schema definition. Relational databases have `CREATE TABLE`. Strata has the ontology — but instead of a DBA writing DDL, an AI defines the schema conversationally, and instead of SQL, an AI queries it through recursive decomposition.

---

## 1. What This Is

The ontology is the **schema definition layer** for the graph. It's the equivalent of `CREATE TABLE`, `ALTER TABLE`, and `INFORMATION_SCHEMA` — but for a typed property graph, stored as data, branchable, and designed to be defined and queried by AI rather than written by hand.

Every database needs a data model before it can do useful work. Oracle needs table definitions. Postgres needs schemas. Palantir needs object types defined by FDEs. The data model is what turns a generic storage engine into a domain-specific knowledge structure.

Strata is no different. The graph PRD gives us the storage engine — nodes, edges, traversal, branching. The ontology gives us the data model — what the nodes and edges *are*, what properties they carry, what relationships are valid, and what constraints apply. Without the ontology, the graph is a bag of strings. With it, the graph becomes a typed knowledge structure that Strata can index, validate, and retrieve against.

The difference is *who defines the model and how*:

| | Traditional DB | Palantir | Strata |
|---|---|---|---|
| **Who models** | DBA / engineer | Domain expert + FDE | AI agent talking to Strata |
| **How** | SQL DDL, migrations | Ontology Manager UI | Conversational — define, review, refine |
| **Iteration** | Write migration, run, fix, repeat | Configure in UI, deploy pipeline | Talk, see what you're building, adjust |
| **Freeze** | Deploy to production | Publish ontology version | Explicit lifecycle: draft → frozen |
| **Query** | SQL (must know schema + query language) | OSDK / AIP (typed SDK + LLM) | Conversational — AI decomposes against the schema |

The ontology is the foundation that makes Layers 3 and 4 of the cognitive architecture possible. Without it, Layer 4 ("what do we know about patient 4821?") has nothing to decompose against. With it, the querying AI reads the schema, understands the structure, and recursively navigates to exactly the data it needs.

### What the Ontology Is Not

- **Not defined by hand.** The primary consumer is an AI agent that defines the ontology through conversational interaction with Strata. The AI describes the domain; Strata stores the resulting type definitions. A human may review and approve, but the definition workflow is AI-driven.
- **Not a business logic layer.** No Actions, no Functions, no side effects, no orchestration. The AI calls graph operations directly. Strata validates them against the ontology.
- **Not a query language.** No Cypher, no SPARQL, no GraphQL. The existing graph API (neighbors, BFS, subgraph) works against typed nodes and edges. The ontology is the map that enables intelligent query decomposition — not the query engine itself.
- **Not mandatory.** Untyped graphs (as described in the graph PRD) continue to work. The ontology is opt-in per graph.

---

## 2. The Workflow

The ontology enables a three-phase workflow that parallels traditional database development but replaces human ceremony with AI-driven conversation.

### Phase A: Define (Conversational Schema Design)

An AI agent talks to Strata to build the ontology iteratively. The agent describes the domain, and through back-and-forth, the type system takes shape:

```
AI:    "I'm modeling a healthcare domain. Patients have an MRN, date of birth,
        and belong to a department. Each patient has lab results — things like
        HbA1c, eGFR — with a test name, value, unit, and reference range."

Strata: [defines object types Patient and LabResult, link type HAS_LAB_RESULT]

AI:    "Show me what we have so far."

Strata: [returns ontology summary — 2 object types, 1 link type, properties, cardinality]

AI:    "Patients also have diagnoses. A diagnosis has an ICD code and a description.
        The link from patient to diagnosis should be many-to-many."

Strata: [adds Diagnosis object type, DIAGNOSED_WITH link type]

AI:    "Looks good. Freeze it."
```

During this phase, the ontology is in **draft** state. Type definitions can be freely created, modified, and deleted. No validation is enforced on data writes — the schema is still being shaped.

The schema introspection API is critical here. The defining AI needs to see a compact summary of the ontology at any point — what types exist, what properties they have, how they're linked — to reason about what's missing or what needs to change.

### Phase B: Freeze (Schema Becomes Authoritative)

When the defining AI (or a human reviewer) is satisfied with the ontology, it's frozen. Freezing means:

1. **Validation activates.** Writes against typed nodes and edges are validated against the schema. Missing required properties are rejected. Source/target type constraints on link types are enforced. Cardinality is enforced.
2. **Indexes are built.** Secondary indexes by object type are created for efficient type-based queries.
3. **The schema is versioned.** The frozen ontology is a versioned snapshot. Evolution happens through branching (see Schema Evolution below).

Freezing is an explicit operation, not a gradual transition. Before freeze: the ontology is a draft that the AI is shaping. After freeze: the ontology is a contract that Strata enforces.

### Phase C: Query (Recursive Decomposition Against the Schema)

Once the ontology is frozen and data is streaming in, the schema becomes the map that querying AIs use to navigate the data. This is where recursive language models (RLMs) shine.

A querying AI doesn't dump the entire graph into its context. Instead:

1. **Orient.** Read the ontology schema — a compact representation of all object types, link types, properties, and cardinality. This fits easily in context and gives the AI a complete map of the domain.
2. **Decompose.** Break the query into typed traversals. "Which patients in endocrinology have elevated HbA1c?" becomes: filter Patients by `department = "endocrinology"` → traverse HAS_LAB_RESULT → filter LabResults by `test_name = "HbA1c"` → check values.
3. **Execute.** Each step is a targeted Strata API call — `nodes_by_type`, `neighbors`, property filters. Small, efficient, typed.
4. **Synthesize.** Aggregate results across steps. The AI reasons over structured, typed objects — not raw text snippets.

The ontology makes step 2 possible. Without it, the AI has to guess at the structure. With it, decomposition is deterministic — the AI knows exactly what types exist, what properties they have, and how they connect.

---

## 3. Core Concepts

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

When the ontology is frozen and a node declares an object type, Strata validates its properties against the schema on write. Missing required properties are rejected. Unknown properties are allowed (open schema by default) or rejected (closed schema, opt-in per type).

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

When the ontology is frozen and an edge is created with a declared link type, Strata validates that the source and target nodes match the expected object types, and that edge properties conform to the schema. This prevents structural nonsense — a LabResult DIAGNOSED_WITH a Medication — without Strata needing to understand what any of it means.

### Cardinality

Link types declare cardinality constraints:

| Cardinality | Meaning | Enforcement |
|-------------|---------|-------------|
| `one_to_one` | Source has at most one target of this link type | Reject duplicate edges from same source |
| `one_to_many` | Source can have many targets | No constraint on target count |
| `many_to_one` | Many sources can link to one target | No constraint on source count |
| `many_to_many` | No constraints | Default if cardinality is omitted |

Cardinality is enforced on write after the ontology is frozen. During draft phase, cardinality is recorded but not enforced — the AI is still exploring the shape of the domain.

### Ontology Lifecycle

The ontology has an explicit lifecycle that separates schema design from schema enforcement:

```
draft  →  frozen  →  (evolve via branch)
  ↑                        |
  └────────────────────────┘
```

**Draft.** Type definitions are mutable. Validation is not enforced. The defining AI is iterating on the schema. Introspection APIs work (so the AI can see what it's building), but no constraints are applied to data writes.

**Frozen.** Type definitions are immutable on this branch. Validation is enforced on all typed writes. Indexes by object type are active. The schema is the contract.

**Evolution.** To modify a frozen ontology: fork the branch, the fork's ontology reverts to draft, modify, test, freeze again, merge. This is how schema migrations work — not through `ALTER TABLE` DDL but through branch-based experimentation.

### Instances

A typed node is an instance of an object type:

```python
graph.add_node("patient-4821", object_type="Patient", properties={
    "mrn": "4821",
    "date_of_birth": "1974-03-15",
    "department": "endocrinology"
}, entity_ref="json://main/patient-4821")
```

The `object_type` field in node metadata links the node to its schema. If the ontology is frozen and has a `Patient` type, the properties are validated. If the ontology is in draft or there's no matching type, the node is stored as-is (backward compatible with the graph PRD).

A typed edge is an instance of a link type:

```python
graph.add_edge("patient-4821", "lab:HbA1c", "HAS_LAB_RESULT",
    properties={"ordered_by": "Dr. Chen", "ordered_date": "2026-01-15"})
```

If `HAS_LAB_RESULT` is a declared link type and the ontology is frozen, Strata validates that the source is a Patient and the target is a LabResult, and that the edge properties match the schema.

---

## 4. The Ontology Is Data

The ontology itself is stored in Strata, as KV entries within the graph's key namespace. This means:

**Versioned.** Every change to the ontology is a versioned write. You can see how the type system evolved over time via `as_of` queries on the schema entries.

**Branchable.** Fork the branch, modify the ontology on the fork (add a property, change a cardinality), test it, merge or discard. The ontology participates in branching exactly like data does. This is what makes schema evolution safe — you never modify the production ontology in place.

**Time-travelable.** Query the ontology as it was at any past timestamp. Combined with time-travel on the data itself, you can see what the type system looked like when a particular piece of data was stored.

**Diffable.** Branch diff on `_graph_/{g}/__types__/` shows exactly what changed in the ontology between two branches.

No separate ontology management system. No migration tooling. The ontology is data, and Strata already knows how to version, branch, and diff data.

This is a fundamental difference from traditional databases. In Postgres, the schema and the data are separate concerns with separate tooling (DDL vs DML, migrations vs queries). In Strata, the schema is just more data — versioned, branched, and time-traveled alongside everything else. When an AI forks a branch to experiment with a schema change, it gets an isolated copy of both the ontology and the data. No migration scripts. No deployment ceremonies.

---

## 5. Schema Evolution

Because the ontology is branchable, schema evolution follows the same workflow as data experimentation:

1. **Fork** the current branch
2. The fork's ontology enters **draft** state
3. **Modify** the ontology — add a property, change a cardinality, add a new object type
4. **Test** — create instances against the new schema, verify queries work
5. **Freeze** and **merge** — or **discard**

### Adding Properties

Adding a new optional property to an object type is non-breaking. Existing nodes don't have the property and that's fine — it's optional.

Adding a new required property is breaking. Existing nodes will fail validation on the next write. The AI (not Strata) is responsible for deciding how to handle this — backfill the property, make it optional, or accept that existing nodes are grandfathered.

### Removing Properties

Removing a property from the schema doesn't delete it from existing nodes. The property data persists in KV; it just isn't part of the schema anymore. This is consistent with Strata's principle that nothing is ever lost.

### Changing Types or Cardinality

These are breaking changes. The recommended workflow is: fork, modify, validate, merge. Strata doesn't automatically migrate data — the AI does that if it needs to.

---

## 6. Schema Introspection

The ontology must be efficiently introspectable. This is not a nice-to-have — it's the foundation of the entire query workflow. When a querying AI connects to Strata, its first action is to read the ontology and orient itself. The introspection API must return a compact, complete summary that fits in a single LLM context window.

### Ontology Summary

A single call that returns the full type system:

```python
graph.ontology_summary()
```

Returns:

```json
{
  "status": "frozen",
  "object_types": {
    "Patient": {
      "properties": {
        "mrn": { "type": "string", "required": true },
        "date_of_birth": { "type": "string" },
        "department": { "type": "string" }
      },
      "node_count": 12847
    },
    "LabResult": {
      "properties": {
        "test_name": { "type": "string", "required": true },
        "value": { "type": "string", "required": true },
        "unit": { "type": "string" },
        "reference_range": { "type": "string" }
      },
      "node_count": 94523
    },
    "Diagnosis": {
      "properties": {
        "icd_code": { "type": "string", "required": true },
        "description": { "type": "string" }
      },
      "node_count": 31209
    }
  },
  "link_types": {
    "HAS_LAB_RESULT": {
      "source": "Patient",
      "target": "LabResult",
      "cardinality": "one_to_many",
      "edge_count": 94523
    },
    "DIAGNOSED_WITH": {
      "source": "Patient",
      "target": "Diagnosis",
      "cardinality": "many_to_many",
      "edge_count": 47891
    }
  }
}
```

This is the querying AI's map. With this in context, it knows:
- What types of objects exist and how many
- What properties each type has (for filtering)
- How types are connected (for traversal planning)
- The cardinality of each relationship (for understanding fan-out)

The summary includes counts because they help the AI reason about query efficiency — traversing from 12,847 patients to 94,523 lab results is a different strategy than traversing from 5 patients to 12 lab results.

### Individual Type Queries

For deeper inspection during schema design:

```python
graph.get_object_type("Patient")    # → full schema JSON
graph.get_link_type("HAS_LAB_RESULT")  # → full schema JSON
graph.object_types()                # → ["Patient", "LabResult", "Diagnosis"]
graph.link_types()                  # → ["HAS_LAB_RESULT", "DIAGNOSED_WITH"]
```

---

## 7. Ontology-Grounded Retrieval

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

The AI receives structured, typed context instead of text snippets. Strata provides the structure; the AI reasons over it.

### Depth Control

How much context to include is controlled by the caller:

- `depth: 0` — just the matching node with its type and properties (default)
- `depth: 1` — include immediate neighbors with their types
- `depth: 2` — include 2-hop neighborhood

Strata doesn't decide what depth is appropriate. The querying AI does — and it can make that decision intelligently because it has the ontology summary and knows the fan-out at each hop.

---

## 8. Storage Model

The ontology extends the graph's existing KV key namespace:

```
_graph_/{graph_name}/__types__/object/{type_name}   → object type schema (JSON)
_graph_/{graph_name}/__types__/link/{type_name}      → link type schema (JSON)
_graph_/{graph_name}/__meta__                        → includes ontology status (draft/frozen)
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

Edge payloads are unchanged from the graph PRD. The edge type string (e.g., `HAS_LAB_RESULT`) is matched against declared link types for validation when the ontology is frozen. If no matching link type is declared, the edge is untyped (backward compatible).

### Indexes

Object type membership is indexed via a secondary index:

```
_graph_/{graph_name}/__by_type__/{object_type}/{node_id} → ""
```

This follows the same pattern as the reverse EntityRef index in the graph PRD. It enables efficient `nodes_by_type("Patient")` queries without scanning all nodes.

The secondary index is maintained atomically alongside node writes — when a typed node is added, both the node entry and the type index entry are written in the same transaction. The index is built on freeze for any nodes that were added during the draft phase.

---

## 9. API Surface

### Ontology Management

```python
# Define object types (during draft phase)
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

# Define link types (during draft phase)
graph.define_link_type("HAS_LAB_RESULT",
    source="Patient", target="LabResult",
    cardinality="one_to_many",
    properties={"ordered_by": {"type": "string"}})

# Introspection (any phase)
graph.ontology_summary()           # → full schema with counts
graph.object_types()               # → ["Patient", "LabResult", ...]
graph.link_types()                  # → ["HAS_LAB_RESULT", ...]
graph.get_object_type("Patient")   # → schema JSON
graph.get_link_type("HAS_LAB_RESULT")  # → schema JSON

# Lifecycle
graph.freeze_ontology()            # draft → frozen, activates validation + indexes
graph.ontology_status()            # → "draft" | "frozen"
```

### Typed Node and Edge Operations

```python
# Create a typed node (validated if ontology is frozen)
graph.add_node("patient-4821", object_type="Patient", properties={
    "mrn": "4821",
    "date_of_birth": "1974-03-15",
    "department": "endocrinology",
}, entity_ref="json://main/patient-4821")

# Create a typed edge (validated if ontology is frozen)
graph.add_edge("patient-4821", "lab:HbA1c", "HAS_LAB_RESULT",
    properties={"ordered_by": "Dr. Chen"})

# Query by object type
graph.nodes_by_type("Patient")  # → all Patient nodes
graph.nodes_by_type("LabResult", filter={"test_name": "HbA1c"})
```

### Untyped Operations (Backward Compatible)

All existing graph PRD operations work unchanged. Nodes without `object_type` are untyped. Edges with undeclared edge types are untyped. The ontology is additive — it enhances graphs that use it without breaking graphs that don't.

---

## 10. What the AI Does vs. What Strata Does

| Responsibility | Who |
|---------------|-----|
| Describe the domain conversationally | Defining AI |
| Decide what object types and link types exist | Defining AI |
| Define properties, constraints, and cardinality | Defining AI |
| Review the ontology and decide when to freeze | Defining AI (or human reviewer) |
| Create, update, delete data instances | Producing AI / data pipeline |
| Read the ontology summary and orient | Querying AI |
| Decompose queries against the typed schema | Querying AI |
| Decide what depth of context to retrieve | Querying AI |
| Decide when to evolve the schema (fork, modify, refreeze) | Any AI |
| Store type definitions as versioned data | Strata |
| Enforce validation after freeze | Strata |
| Enforce cardinality constraints after freeze | Strata |
| Maintain secondary indexes by object type | Strata |
| Provide schema introspection (ontology summary) | Strata |
| Enrich search results with typed context and relationships | Strata |
| Branch, version, time-travel the ontology like any other data | Strata |

There are three distinct AI roles. The **defining AI** builds the ontology through conversation — it's the DBA replacement. The **producing AI** (or data pipeline) writes data against the frozen schema — it's the application. The **querying AI** reads the schema, decomposes queries, and retrieves structured results — it's the analyst. These may be the same AI or different agents in a system.

Strata is the infrastructure underneath all three. It stores, validates, indexes, and retrieves. It doesn't decide what to model, what to store, or what to query. That's the AI's job.

---

## 11. Relationship to Cognitive Architecture Layers

The ontology is the bridge from Layer 2 to Layers 3 and 4.

**Layer 2 (cross-primitive relationships):** Phase 1 gave the graph its mechanical foundation — nodes, edges, traversal, branching. Phase 2 (the ontology) gives the graph its semantic foundation — what the nodes and edges *are*. Together they complete Layer 2: a typed, traversable, branchable knowledge structure that connects data across all of Strata's primitives.

**Layer 3 (intelligent retrieval):** The ontology is what makes graph-informed scoring meaningful. Without types, "1-hop neighbor" is just topology. With types, "Patient 1-hop from LabResult via HAS_LAB_RESULT" is a semantic signal that the retrieval layer can use for ranking. The ontology also provides the vocabulary for automatic association — when Layer 3 infers relationships, it does so in terms of declared link types with known cardinality, not arbitrary string edges. And ontology-grounded retrieval (returning typed objects with relationships instead of flat results) is the bridge between the graph and the querying AI.

**Layer 4 (cognitive interface):** The ontology is what makes "what do we know about patient 4821?" a tractable question. The querying AI reads the ontology summary, sees that Patients connect to LabResults and Diagnoses, and knows exactly what traversals to execute. Without the ontology, the AI would have to guess at the structure or dump everything into context and hope. With it, query decomposition is structured, efficient, and deterministic.

The path: Phase 1 (untyped graph) → Phase 2 (ontology — typed graph) → Layer 3 (intelligent retrieval using typed structure) → Layer 4 (conversational interface powered by recursive decomposition against the ontology).

Each step earns the right to the next. You can't build Layer 4 without the ontology any more than you can write SQL without `CREATE TABLE`.
