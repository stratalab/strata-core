---
summary: Replace the embedding for one vector, keeping its metadata.
mcp_description: Use this when the user wants to re-embed a vector without restating its metadata.
---

Replaces one visible vector's embedding and leaves its metadata as it stands — the mirror of `vector update-metadata`, which patches metadata and leaves the embedding. Use this to re-embed a document after a model change, a re-chunk or a backfill; `vector upsert` writes the whole record, so metadata not restated there is removed. Missing vectors return a no-op mutation acknowledgement and are never created. With `text` instead of a vector, the text is embedded through the collection's recorded model on the same inference path as `vector upsert`, so the `inference.*` failures listed below apply only to that form.
