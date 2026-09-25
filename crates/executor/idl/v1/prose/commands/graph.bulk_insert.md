---
summary: Bulk-load nodes and edges in chunks.
mcp_description: Use this when the user wants to load many nodes and edges into a graph at once. Nodes commit before edges in chunked commits; edge endpoints must exist or arrive in the same payload. Safe to re-run after an interruption.
---

Ingests a payload of nodes and edges in chunked commits: nodes first, then edges, so edges may reference nodes from the same payload. Node objects use the key `node_id`; edges use `src`, `edge_type`, `dst`, and optional `weight` (default 1.0) and `properties`. `chunk_size` bounds items per commit (default 512, clamped at 800). The acknowledgement reports inserted counts, the number of chunk commits, and the final chunk's commit receipt.

Each chunk is its own commit, so an interruption leaves the chunks that landed. An import that spans more than one commit sets a durable watermark on the graph with its first commit and clears it with its last: `graph meta` reports `import_pending: true` in between, and afterwards if the import was cut short. The watermark says a multi-commit import is in progress, not which payload: it is cleared by whichever `graph bulk_insert`'s last chunk lands next, whatever its chunk size, and ordinary writes leave it as it is. Every row is an upsert, so finishing an interrupted import is re-running its own payload; the graph stays readable and writable throughout.
