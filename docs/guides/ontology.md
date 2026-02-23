# Ontology Guide

Define a typed schema for your graph with object types, link types, and validation.

## API Overview

### Object Types

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_define_object_type` | `(graph: &str, definition: Value) -> Result<()>` | |
| `graph_get_object_type` | `(graph: &str, name: &str) -> Result<Option<Value>>` | Definition or None |
| `graph_list_object_types` | `(graph: &str) -> Result<Vec<String>>` | Type names |
| `graph_delete_object_type` | `(graph: &str, name: &str) -> Result<()>` | |

### Link Types

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_define_link_type` | `(graph: &str, definition: Value) -> Result<()>` | |
| `graph_get_link_type` | `(graph: &str, name: &str) -> Result<Option<Value>>` | Definition or None |
| `graph_list_link_types` | `(graph: &str) -> Result<Vec<String>>` | Type names |
| `graph_delete_link_type` | `(graph: &str, name: &str) -> Result<()>` | |

### Lifecycle

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_freeze_ontology` | `(graph: &str) -> Result<()>` | |
| `graph_ontology_status` | `(graph: &str) -> Result<Option<Value>>` | Status or None |

### Queries

| Method | Signature | Returns |
|--------|-----------|---------|
| `graph_ontology_summary` | `(graph: &str) -> Result<Option<Value>>` | Full summary or None |
| `graph_nodes_by_type` | `(graph: &str, object_type: &str) -> Result<Vec<String>>` | Node IDs |

## Defining Object Types

An object type defines a category of nodes in your graph. Each type has a name and optional property definitions:

```rust
use serde_json::json;

let db = Strata::cache()?;
db.graph_create("healthcare")?;

// Define a Patient type with required and optional properties
db.graph_define_object_type("healthcare", json!({
    "name": "Patient",
    "properties": {
        "full_name": { "type": "string", "required": true },
        "date_of_birth": { "type": "string", "required": true },
        "blood_type": { "type": "string" }
    }
}))?;

// Define a LabResult type
db.graph_define_object_type("healthcare", json!({
    "name": "LabResult",
    "properties": {
        "test_name": { "type": "string", "required": true },
        "value": { "type": "number", "required": true },
        "unit": { "type": "string", "required": true },
        "collected_at": { "type": "string" }
    }
}))?;

// Define a Diagnosis type
db.graph_define_object_type("healthcare", json!({
    "name": "Diagnosis",
    "properties": {
        "code": { "type": "string", "required": true },
        "description": { "type": "string", "required": true },
        "severity": { "type": "string" }
    }
}))?;
```

### Retrieving a Type Definition

```rust
let patient_type = db.graph_get_object_type("healthcare", "Patient")?;
// Returns the full definition as a Value

let all_types = db.graph_list_object_types("healthcare")?;
// ["Diagnosis", "LabResult", "Patient"]
```

### Deleting a Type (Draft Only)

While the ontology is in draft mode, you can remove types:

```rust
db.graph_delete_object_type("healthcare", "LabResult")?;
```

## Defining Link Types

A link type defines a category of edges, including which object types it can connect and its cardinality:

```rust
// A Patient can have many LabResults
db.graph_define_link_type("healthcare", json!({
    "name": "HAS_LAB_RESULT",
    "source": "Patient",
    "target": "LabResult",
    "cardinality": "one_to_many"
}))?;

// A Patient can have many Diagnoses
db.graph_define_link_type("healthcare", json!({
    "name": "DIAGNOSED_WITH",
    "source": "Patient",
    "target": "Diagnosis",
    "cardinality": "one_to_many"
}))?;

// A LabResult can support a Diagnosis
db.graph_define_link_type("healthcare", json!({
    "name": "SUPPORTS",
    "source": "LabResult",
    "target": "Diagnosis",
    "cardinality": "many_to_many"
}))?;
```

### Retrieving a Link Type

```rust
let link = db.graph_get_link_type("healthcare", "HAS_LAB_RESULT")?;

let all_links = db.graph_list_link_types("healthcare")?;
// ["DIAGNOSED_WITH", "HAS_LAB_RESULT", "SUPPORTS"]
```

### Deleting a Link Type (Draft Only)

```rust
db.graph_delete_link_type("healthcare", "SUPPORTS")?;
```

## Draft vs Frozen

Every ontology starts in **draft** mode. In draft mode you can freely add, modify, and delete type definitions. No validation is enforced on node or edge writes.

Once the schema is ready, **freeze** it to enable validation:

```rust
// Check current status
let status = db.graph_ontology_status("healthcare")?;
// Some({"status": "draft", ...})

// Freeze the ontology
db.graph_freeze_ontology("healthcare")?;

let status = db.graph_ontology_status("healthcare")?;
// Some({"status": "frozen", ...})
```

After freezing:

- You **cannot** add, modify, or delete type definitions
- Typed node writes are **validated** against the schema
- Edge writes with a defined link type are **validated** for source/target type constraints

## Validation After Freeze

Once frozen, the following checks are enforced on writes:

**Validated:**
- Required properties must be present on typed nodes
- Edge source and target nodes must have the correct object types for the link type

**Not validated (by design):**
- Property value types are not checked at write time
- Cardinality limits are not enforced at write time

```rust
db.graph_freeze_ontology("healthcare")?;

// This succeeds — all required properties present
db.graph_add_node_typed(
    "healthcare", "patient-1", None,
    Some(json!({
        "full_name": "Jane Doe",
        "date_of_birth": "1990-05-15"
    })),
    Some("Patient"),
)?;

// This fails — missing required property "full_name"
let result = db.graph_add_node_typed(
    "healthcare", "patient-2", None,
    Some(json!({
        "date_of_birth": "1985-03-20"
    })),
    Some("Patient"),
);
assert!(result.is_err());
```

## Querying by Type

Retrieve all nodes of a given object type:

```rust
db.graph_add_node_typed("healthcare", "patient-1", None,
    Some(json!({"full_name": "Jane Doe", "date_of_birth": "1990-05-15"})),
    Some("Patient"))?;
db.graph_add_node_typed("healthcare", "patient-2", None,
    Some(json!({"full_name": "John Smith", "date_of_birth": "1985-03-20"})),
    Some("Patient"))?;

let patients = db.graph_nodes_by_type("healthcare", "Patient")?;
// ["patient-1", "patient-2"]
```

## Schema Introspection

`graph_ontology_summary` returns the full ontology with type definitions and instance counts. This is the recommended way for AI agents to orient themselves in a graph:

```rust
let summary = db.graph_ontology_summary("healthcare")?;
// Returns a Value like:
// {
//   "status": "frozen",
//   "object_types": [
//     { "name": "Patient", "properties": {...}, "node_count": 2 },
//     { "name": "LabResult", "properties": {...}, "node_count": 5 },
//     ...
//   ],
//   "link_types": [
//     { "name": "HAS_LAB_RESULT", "source": "Patient", "target": "LabResult", ... },
//     ...
//   ]
// }
```

## Schema Evolution

Ontologies cannot be unfrozen. To evolve a schema, fork the branch and redefine the ontology on the new branch:

```rust
// Original branch has a frozen ontology
db.fork_branch("schema-v2")?;
db.set_branch("schema-v2")?;

// The forked branch inherits the frozen ontology
// Create a new graph with the updated schema, or work with the inherited data
```

This approach preserves the original data and schema while letting you iterate on the new version independently.

## Next

- [Graph Store Guide](graph.md) — node/edge CRUD, traversal, bulk loading
- [Knowledge Graph Cookbook](../cookbook/knowledge-graph.md) — end-to-end recipe combining graph + ontology
- [API Quick Reference](../reference/api-quick-reference.md) — all method signatures
