---
summary: Read graph metadata and counts.
mcp_description: Use this when the user wants a graph's node count, edge count, creation and update commit coordinates, or whether a bulk import was cut short — one row read, cheap enough to poll. Returns null if the graph does not exist.
---

Reads a graph's metadata: live node and edge counts, the create and last-update commit versions and timestamps, and `import_pending` — whether a `bulk_insert` spanning more than one commit began and has not finished (its first commit sets the flag, its last clears it, so an interrupted import leaves it set; re-running the import clears it). This is one row read, not a scan: the graph's metadata row carries its counts and is rewritten by every commit that changes a node or edge, so `updated_version` is a per-graph revision token — unchanged means no node or edge has changed since, whatever else moved on the branch. Ontology changes do not move it. Reading a graph that does not exist returns no data rather than an error. Accepts `as_of` for time travel. A graph whose rows were last written by a release before its metadata row carried counts is counted by scan until its next write.
