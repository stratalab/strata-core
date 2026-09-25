---
summary: Delete a graph and its visible data.
mcp_description: Use this when the user wants to delete an entire named graph, including all of its nodes and edges. Returns deleted=false if the graph does not exist. Works for a graph of any size.
---

Deletes a named graph and every visible node, edge, binding, and ontology row it owns. Deleting a graph that does not exist is not an error: the acknowledgement reports `deleted: false` with a `not_found` effect. Earlier states remain readable through time travel on other commands.

A graph too large for one commit is deleted in several: it disappears at the first — every read and write of it refuses `not_found.engine.graph` from then on — its rows are swept in later commits, and the acknowledged commit is the last, with row counts that cover the whole deletion. If the sweep is interrupted, the next `graph delete` (or `graph create` of the same name) finishes it before doing anything else, so no row of the old graph ever surfaces under the new one.
