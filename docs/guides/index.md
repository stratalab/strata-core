# Guides

Detailed walkthroughs for each StrataDB feature.

## Per-Primitive Guides

- **[KV Store](kv-store.md)** — key-value operations, prefix filtering, versioned reads
- **[Event Log](event-log.md)** — append-only event streams, reading by type
- **[State Cell](state-cell.md)** — mutable state with CAS, coordination patterns
- **[JSON Store](json-store.md)** — structured documents with path-level access
- **[Vector Store](vector-store.md)** — collections, similarity search, metadata filtering
- **[Graph Store](graph.md)** — property graph with typed nodes, edges, and traversal
- **[Ontology](ontology.md)** — typed schemas for graphs with validation
- **[Branch Management](branch-management.md)** — creating, switching, listing, and deleting branches
- **[Spaces](spaces.md)** — organizing data within branches using spaces

## Cross-Cutting Guides

- **[Sessions and Transactions](sessions-and-transactions.md)** — multi-operation atomicity
- **[Search](search.md)** — hybrid keyword + semantic search
- **[Database Configuration](database-configuration.md)** — opening methods, durability modes
- **[Branch Bundles](branch-bundles.md)** — exporting and importing branches
- **[Error Handling](error-handling.md)** — error categories and patterns
- **[Observability](observability.md)** — structured logging with `tracing` subsystem targets
