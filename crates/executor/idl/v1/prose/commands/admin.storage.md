---
summary: Read the database's on-disk footprint.
mcp_description: Use this when the user asks how much disk a database uses, what of it is reclaimable, or whether reclaim is keeping up — live tables, WAL, snapshots, unreferenced and quarantined objects, and the reclaim ledger.
---

Returns the database's on-disk footprint as the storage layer computes it: the durable table objects the runtime catalogues and their bytes, the retained and active WAL bytes, and the reclaim ledger (the last pass of every reclaim family with its running totals). With `audit: true` it also lists and stats what the reclaim runners would touch — unreferenced and quarantined table objects, checkpoint snapshots and the superseded set, the WAL's reclaimable-versus-tail split — and reports a total when every part is known. The facts are database-global: every branch shares one table catalogue, one WAL and one reclaim ledger, so the branch only has to exist (it defaults to the handle branch). A cache database holds no durable objects and is refused.
