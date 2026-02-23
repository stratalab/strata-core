# Knowledge Graph with Schema Validation

Build a typed knowledge graph with ontology-driven validation and bulk loading.

## Pattern

Combine the graph store with the ontology layer to create a structured knowledge graph:
- **Graph** holds entities (nodes) and relationships (edges)
- **Ontology** defines the allowed types and validates writes after freezing
- **Bulk insert** loads large datasets efficiently

## Scenario

A healthcare knowledge graph tracking patients, lab results, diagnoses, and treatments. The ontology enforces that lab results belong to patients and diagnoses link to supporting evidence.

## Implementation

### Step 1: Create the Graph

```rust
use serde_json::json;

let db = Strata::cache()?;
db.graph_create("medical")?;
```

### Step 2: Define the Ontology

Define four object types with required properties:

```rust
// Patient
db.graph_define_object_type("medical", json!({
    "name": "Patient",
    "properties": {
        "full_name": { "type": "string", "required": true },
        "date_of_birth": { "type": "string", "required": true },
        "blood_type": { "type": "string" }
    }
}))?;

// LabResult
db.graph_define_object_type("medical", json!({
    "name": "LabResult",
    "properties": {
        "test_name": { "type": "string", "required": true },
        "value": { "type": "number", "required": true },
        "unit": { "type": "string", "required": true },
        "collected_at": { "type": "string" }
    }
}))?;

// Diagnosis
db.graph_define_object_type("medical", json!({
    "name": "Diagnosis",
    "properties": {
        "code": { "type": "string", "required": true },
        "description": { "type": "string", "required": true },
        "severity": { "type": "string" }
    }
}))?;

// Treatment
db.graph_define_object_type("medical", json!({
    "name": "Treatment",
    "properties": {
        "name": { "type": "string", "required": true },
        "dosage": { "type": "string" },
        "start_date": { "type": "string", "required": true }
    }
}))?;
```

Define four link types with source/target constraints:

```rust
db.graph_define_link_type("medical", json!({
    "name": "HAS_LAB_RESULT",
    "source": "Patient",
    "target": "LabResult",
    "cardinality": "one_to_many"
}))?;

db.graph_define_link_type("medical", json!({
    "name": "DIAGNOSED_WITH",
    "source": "Patient",
    "target": "Diagnosis",
    "cardinality": "one_to_many"
}))?;

db.graph_define_link_type("medical", json!({
    "name": "SUPPORTS",
    "source": "LabResult",
    "target": "Diagnosis",
    "cardinality": "many_to_many"
}))?;

db.graph_define_link_type("medical", json!({
    "name": "TREATED_WITH",
    "source": "Diagnosis",
    "target": "Treatment",
    "cardinality": "one_to_many"
}))?;
```

### Step 3: Freeze the Ontology

Freezing locks the schema and enables validation on all subsequent writes:

```rust
db.graph_freeze_ontology("medical")?;

let status = db.graph_ontology_status("medical")?;
// Some({"status": "frozen", ...})
```

### Step 4: Bulk Load Data

Use `graph_bulk_insert` to load nodes and edges in a single call:

```rust
let nodes: Vec<(&str, Option<&str>, Option<serde_json::Value>)> = vec![
    // Patients
    ("patient-jane", None, Some(json!({"full_name": "Jane Doe", "date_of_birth": "1990-05-15", "blood_type": "A+"}))),
    ("patient-john", None, Some(json!({"full_name": "John Smith", "date_of_birth": "1985-03-20", "blood_type": "O-"}))),
    ("patient-maria", None, Some(json!({"full_name": "Maria Garcia", "date_of_birth": "1978-11-02"}))),

    // Lab results
    ("lab-001", None, Some(json!({"test_name": "HbA1c", "value": 7.2, "unit": "%", "collected_at": "2025-01-10"}))),
    ("lab-002", None, Some(json!({"test_name": "Fasting Glucose", "value": 142.0, "unit": "mg/dL", "collected_at": "2025-01-10"}))),
    ("lab-003", None, Some(json!({"test_name": "LDL Cholesterol", "value": 165.0, "unit": "mg/dL", "collected_at": "2025-01-12"}))),
    ("lab-004", None, Some(json!({"test_name": "Blood Pressure", "value": 145.0, "unit": "mmHg", "collected_at": "2025-01-15"}))),
    ("lab-005", None, Some(json!({"test_name": "HbA1c", "value": 5.4, "unit": "%", "collected_at": "2025-01-11"}))),

    // Diagnoses
    ("dx-diabetes", None, Some(json!({"code": "E11.9", "description": "Type 2 Diabetes", "severity": "moderate"}))),
    ("dx-hyperlipidemia", None, Some(json!({"code": "E78.5", "description": "Hyperlipidemia", "severity": "mild"}))),
    ("dx-hypertension", None, Some(json!({"code": "I10", "description": "Essential Hypertension", "severity": "moderate"}))),

    // Treatments
    ("tx-metformin", None, Some(json!({"name": "Metformin", "dosage": "500mg twice daily", "start_date": "2025-01-15"}))),
    ("tx-atorvastatin", None, Some(json!({"name": "Atorvastatin", "dosage": "20mg daily", "start_date": "2025-01-16"}))),
    ("tx-lisinopril", None, Some(json!({"name": "Lisinopril", "dosage": "10mg daily", "start_date": "2025-01-17"}))),
];

let edges: Vec<(&str, &str, &str, Option<f64>, Option<serde_json::Value>)> = vec![
    // Jane's lab results
    ("patient-jane", "lab-001", "HAS_LAB_RESULT", None, None),
    ("patient-jane", "lab-002", "HAS_LAB_RESULT", None, None),
    ("patient-jane", "lab-003", "HAS_LAB_RESULT", None, None),

    // John's lab results
    ("patient-john", "lab-004", "HAS_LAB_RESULT", None, None),
    ("patient-john", "lab-005", "HAS_LAB_RESULT", None, None),

    // Diagnoses
    ("patient-jane", "dx-diabetes",       "DIAGNOSED_WITH", None, None),
    ("patient-jane", "dx-hyperlipidemia", "DIAGNOSED_WITH", None, None),
    ("patient-john", "dx-hypertension",   "DIAGNOSED_WITH", None, None),

    // Lab results supporting diagnoses
    ("lab-001", "dx-diabetes",       "SUPPORTS", Some(0.95), None),
    ("lab-002", "dx-diabetes",       "SUPPORTS", Some(0.88), None),
    ("lab-003", "dx-hyperlipidemia", "SUPPORTS", Some(0.91), None),
    ("lab-004", "dx-hypertension",   "SUPPORTS", Some(0.85), None),

    // Treatments for diagnoses
    ("dx-diabetes",       "tx-metformin",    "TREATED_WITH", None, None),
    ("dx-hyperlipidemia", "tx-atorvastatin", "TREATED_WITH", None, None),
    ("dx-hypertension",   "tx-lisinopril",   "TREATED_WITH", None, None),
];

let (n, e) = db.graph_bulk_insert("medical", &nodes, &edges)?;
assert_eq!(n, 15);
assert_eq!(e, 15);
```

### Step 5: Query the Graph

Find all of Jane's lab results:

```rust
let labs = db.graph_neighbors("medical", "patient-jane", "outgoing", Some("HAS_LAB_RESULT"))?;
// 3 results: lab-001, lab-002, lab-003

for hit in &labs {
    println!("{} (weight: {})", hit.node_id, hit.weight);
}
```

Find everything reachable from Jane within 3 hops:

```rust
let result = db.graph_bfs("medical", "patient-jane", 3, None, None, Some("outgoing"))?;
// visited: ["patient-jane", "lab-001", "lab-002", "lab-003",
//           "dx-diabetes", "dx-hyperlipidemia",
//           "tx-metformin", "tx-atorvastatin"]
// depths:  patient-jane=0, lab-*=1, dx-*=2, tx-*=3

for node_id in &result.visited {
    let depth = result.depths.get(node_id).unwrap_or(&0);
    println!("  depth {}: {}", depth, node_id);
}
```

List all patients by type:

```rust
let patients = db.graph_nodes_by_type("medical", "Patient")?;
// ["patient-jane", "patient-john", "patient-maria"]
```

### Step 6: Introspect the Schema

Use `graph_ontology_summary` for a complete view of the schema and instance counts:

```rust
let summary = db.graph_ontology_summary("medical")?;
println!("{}", serde_json::to_string_pretty(&summary)?);
// {
//   "status": "frozen",
//   "object_types": [
//     { "name": "Patient", "properties": {...}, "node_count": 3 },
//     { "name": "LabResult", "properties": {...}, "node_count": 5 },
//     { "name": "Diagnosis", "properties": {...}, "node_count": 3 },
//     { "name": "Treatment", "properties": {...}, "node_count": 3 }
//   ],
//   "link_types": [
//     { "name": "HAS_LAB_RESULT", "source": "Patient", "target": "LabResult" },
//     { "name": "DIAGNOSED_WITH", "source": "Patient", "target": "Diagnosis" },
//     { "name": "SUPPORTS", "source": "LabResult", "target": "Diagnosis" },
//     { "name": "TREATED_WITH", "source": "Diagnosis", "target": "Treatment" }
//   ]
// }
```

### Step 7: Entity Reference Binding

Link a graph node to a JSON document for richer data:

```rust
// Store detailed patient record in the JSON store
db.json_set("patient:jane:record", "$", json!({
    "insurance": "BlueCross #12345",
    "emergency_contact": {"name": "Bob Doe", "phone": "555-0123"},
    "allergies": ["penicillin"]
}))?;

// Bind the graph node to the JSON document
db.graph_add_node("medical", "patient-jane", Some("json://default/patient:jane:record"), Some(json!({
    "full_name": "Jane Doe",
    "date_of_birth": "1990-05-15",
    "blood_type": "A+"
})))?;

// The node now carries both graph properties and a reference to the full record
let node = db.graph_get_node("medical", "patient-jane")?;
```

## Cleanup

```rust
db.graph_delete("medical")?;
```

## See Also

- [Graph Store Guide](../guides/graph.md) — node/edge CRUD, traversal, bulk loading
- [Ontology Guide](../guides/ontology.md) — typed schemas with validation
- [API Quick Reference](../reference/api-quick-reference.md) — all method signatures
