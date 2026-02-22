# Palantir Ontology: Architecture Study

**Purpose:** Understand Palantir's Ontology system and map its concepts to Strata's cognitive architecture. Not everything applies today — Strata is the hippocampus, not the cortex — but the full picture informs where the system can evolve.

---

## 1. What Palantir Built

Palantir's Ontology is a **typed, operational semantic layer** over data. It has four dimensions:

1. **Data Layer** — unifies disparate sources (ERPs, CRMs, sensors, document stores) into coherent objects, properties, and links. Every object type is backed by one or more datasets. The Ontology doesn't replace the data — it indexes and projects from it.

2. **Logic Layer** — computation ranging from simple business rules to ML models to LLM-driven functions. Implemented as TypeScript/Python Functions that read objects, traverse links, and return results or edits.

3. **Action Layer** — structured mutations. Every write flows through a parameterized, validated, permissioned Action Type — not raw SQL. Actions are auditable, governable, and attachable to side effects.

4. **Security Layer** — row-level, column-level, and cell-level permissions embedded in the Ontology itself, enforced through to the AI layer.

The system is both the type system AND the runtime. Type definitions drive indexing (the "Funnel" service reads backing datasets and indexes objects), querying (the Object Set Service handles all reads), code generation (the OSDK generates type-safe clients), security enforcement, and AI grounding.

---

## 2. Key Design Decisions

### Abstraction Over Storage, Not Replacement

The Ontology does not store the canonical data. It indexes data from backing datasets. Upstream data pipelines continue to work; the Ontology is an indexed, queryable, semantic view over them. A single object type can aggregate columns from up to 70 different datasets, joined by primary key.

**Strata parallel:** The ontology layer (as drafted in `ontology.md`) is a semantic view over Strata's storage primitives. Object types don't replace KV, JSON, events, or vectors — they project from them via EntityRef bindings. A Patient node bound to `json://main/patient-4821` is a typed view over a JSON document.

### Separated Edit Layer

User modifications through Actions are stored in separate writeback datasets, preserving the original backing data. This is event sourcing — the original data and the edits are maintained independently and merged at query time. Conflict resolution is configurable: "user edits always win" or "most recent value wins."

**Strata parallel:** Strata's versioning model already provides this. Every write creates a new version; nothing is overwritten. The edit history is the version history. Branch-level isolation provides even stronger separation — fork, edit, merge — without needing a separate writeback dataset.

### Entirely User-Defined Types

Object types, properties, link types, cardinality constraints — all defined by the user (or AI), not predefined by the system. No fixed categories like "People, Organizations, Projects." Whatever the domain needs.

**Strata parallel:** Directly adopted in the ontology layer. Object types and link types are user-defined schemas stored as data within the graph. The AI defines the ontology; Strata stores and enforces it.

### Schema as Runtime

Type definitions aren't documentation. They drive:
- **Indexing** — when a type definition changes, the system re-indexes affected objects
- **Querying** — the Object Set Service filters, searches, and aggregates against typed objects
- **Code generation** — the OSDK produces per-application type-safe clients
- **Security** — permissions are defined in terms of object types and properties
- **AI grounding** — structured objects (not text chunks) are passed to LLMs

**Strata parallel today:** The ontology layer enables schema-driven validation on write and typed context in search results. Schema-driven indexing (secondary indexes by object type) is planned.

**Strata parallel future:** Code generation (SDK producing typed clients from the ontology) and AI grounding (search returning typed objects with relationships instead of snippets) are natural extensions.

### Git-Like Schema Governance

Ontology changes go through a branching, proposal, review, and merge workflow. Breaking changes trigger automatic detection. Schema migrations handle user edit preservation across versions.

**Strata parallel:** This is native. The ontology is stored as versioned KV entries within the graph. Fork the branch, modify the ontology, test, merge or discard. Breaking change detection would be: compare the old schema to the new schema and report which existing nodes would fail validation.

### Interfaces (Polymorphism)

Interfaces define shared property sets and link constraints that multiple object types can implement. An interface "Location" might be implemented by Airport, Plant, and Warehouse — enabling generic operations across all of them.

**Strata today:** Not in scope for the initial ontology layer. Object types are standalone.

**Strata future:** Interfaces are a natural extension. A "Searchable" interface requiring a `title` and `description` property could enable generic search enrichment across any conforming object type. Worth considering once the base ontology is stable.

---

## 3. What Strata Adopts

These concepts map directly to Strata's architecture and are adopted in the ontology layer:

| Palantir Concept | Strata Equivalent |
|-----------------|-------------------|
| Object types with typed properties | `define_object_type()` — user-defined schemas stored as graph metadata |
| Link types with cardinality | `define_link_type()` — source/target type constraints, cardinality enforcement |
| Backing datasources | EntityRef bindings — nodes project from KV, JSON, events, vectors |
| Schema versioning | Ontology stored as KV entries — versioned, branchable, diffable automatically |
| Schema branching and governance | Fork the branch, modify the ontology, test, merge/discard |
| Ontology-grounded retrieval | Search returns typed objects with relationships instead of flat results |

---

## 4. What Strata Defers

These concepts are part of Palantir's full stack but belong to the cortex (the AI), not the hippocampus (Strata). They're documented here because the architecture should not prevent them from being built later — by Strata, by the AI, or by an application layer on top.

### Actions (Governed Mutations)

Palantir routes all mutations through parameterized, validated Action Types with permissions, submission criteria, and side effects. Every write is auditable and governable.

**Why Strata defers this:** Strata is passive infrastructure. The AI calls `graph.add_node()` and `graph.add_edge()` directly. Strata validates against the ontology (type checking, cardinality) but doesn't impose additional governance. The AI or an application layer above Strata could implement governed mutations — Strata's event log already provides the audit trail, and the ontology provides the type constraints that Actions would validate against.

**What Strata should not prevent:** An application layer that intercepts graph writes, validates them against business rules, logs them as events, and only commits if all rules pass. The ontology's type constraints are the foundation this would build on.

### Functions (Logic Layer)

Palantir embeds TypeScript/Python Functions as first-class computation within the Ontology. Functions read objects, traverse links, and return structured results or edits.

**Why Strata defers this:** Computation belongs to the cortex. Strata stores and retrieves; it doesn't compute. The AI can read objects from Strata, compute over them, and write results back.

**What Strata should not prevent:** An extension that registers named computations (e.g., "compute patient risk score from lab results and diagnoses") that run over graph snapshots. The `GraphAlgorithm` trait in the graph PRD is the extensibility point — Functions would be a higher-level version of the same pattern.

### AIP Agent Studio (Orchestration)

Palantir's Agent Studio combines LLMs, the Ontology, documents, tools, and state variables into interactive assistants.

**Why Strata defers this:** Agent orchestration is a cortex concern. Strata doesn't orchestrate agents — it provides the state layer that agents orchestrate over.

**What Strata should not prevent:** An agent framework that uses Strata as its state backend, defining its workflow state as typed objects, its decision history as events, and its knowledge as an ontology-typed graph. Strata's 8 MCP tools are already designed for this — the ontology makes the state structure explicit and queryable.

### Security Model

Palantir embeds row-level, column-level, and cell-level security into the Ontology, enforced through to the AI layer.

**Why Strata defers this:** Strata's security model (`crates/security/`) operates at the branch and space level. Object-level and property-level security is not currently in scope.

**What Strata should not prevent:** The ontology's type system is the natural place to attach permission metadata. A future extension could add `read_access` and `write_access` fields to object type and property definitions, enforced at the graph operation level.

---

## 5. What Strata Does Differently

### Embedded, Not Distributed

Palantir's Ontology is a distributed system with dedicated microservices (OMS, OSS, Funnel, Actions Service). Strata is a single embedded process. The ontology is stored in the same KV primitive as everything else. This is a deliberate architectural choice — cognitive infrastructure should be as simple to deploy as SQLite, not as complex as Kubernetes.

### Branching as a First-Class Primitive

Palantir supports ontology branching and proposals, but it's a governance workflow — review, approve, merge. In Strata, branching is a data primitive. Fork, modify, test, merge/discard — in milliseconds, programmatically, by the AI. This makes schema evolution something an agent can do mid-task, not a deployment ceremony.

### The Ontology Is the Data

In Palantir, the Ontology Manager is a separate system from the backing datasets. Type definitions live in OMS; data lives in Foundry datasets; the Funnel indexes one into the other. In Strata, the ontology is stored as KV entries in the same namespace as the graph data. There's no separate metadata service — the type system is versioned, branched, and time-traveled alongside the data it describes. When you fork a branch, you fork the ontology too.

### No Edit Layer Separation

Palantir separates backing data from user edits in writeback datasets. Strata doesn't need this — every write is a new version of the same key. The version history IS the edit history. There's nothing to reconcile because writes never overwrite.

---

## 6. The Long View

Palantir's full stack — Ontology + Functions + Actions + AIP — is a complete operational platform. Strata is not trying to be that. Strata is the persistence layer that makes such a platform possible.

But the architecture should be designed so that each Palantir capability has a natural extension point in Strata:

| Palantir Capability | Strata Extension Point |
|--------------------|----------------------|
| Object types and link types | Ontology layer (adopted) |
| Ontology-grounded retrieval | Search enrichment with typed context (adopted) |
| Schema governance | Branch + merge on ontology data (native) |
| Actions (governed mutations) | Event log + ontology validation (foundation exists, governance layer deferred) |
| Functions (computation) | `GraphAlgorithm` trait + graph snapshots (extension point exists) |
| Agent orchestration | MCP tools + ontology-typed state (interface exists) |
| Security | Branch/space-level security (exists), object-level (deferred) |

The principle is: Strata builds the hippocampus correctly. The cortex — whether it's an AI agent, an application framework, or something that doesn't exist yet — builds on top. Nothing in Strata's design should prevent the cortex from being built. Everything in Strata's design should make the cortex's job easier.
