---
title: "Read storage footprint"
description: "Read the database's on-disk footprint."
source: strata-core@1.2.5
section: admin
---

Returns the database's on-disk footprint as the storage layer computes it: the durable table objects the runtime catalogues and their bytes, the retained and active WAL bytes, and the reclaim ledger (the last pass of every reclaim family with its running totals). With `audit: true` it also lists and stats what the reclaim runners would touch — unreferenced and quarantined table objects, checkpoint snapshots and the superseded set, the WAL's reclaimable-versus-tail split — and reports a total when every part is known. The facts are database-global: every branch shares one table catalogue, one WAL and one reclaim ledger, so the branch only has to exist (it defaults to the handle branch). A cache database holds no durable objects and is refused.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `audit` | `boolean` | no | Gather the audit tier too: the listing-backed facts (unreferenced and quarantined objects, snapshots, the WAL's reclaimable-versus-tail split). |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<AdminStorage>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`unsupported.engine.persistence_capability`](https://stratadb.org/e/unsupported.engine.persistence_capability)

## Invocation

- CLI: `strata admin storage`
- Wire type: `storage`
