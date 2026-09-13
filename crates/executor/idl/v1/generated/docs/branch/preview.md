---
title: "Preview branch promotion"
description: "Preview promoting one branch into another, reporting conflicts without mutating either branch."
source: strata-core@1.2.2
section: branch
---

Previews promoting the `source` branch into the `target` branch: it derives the
branch point from the recorded fork lineage, runs a three-way comparison, and
reports the conflicts a promotion would hit — entries both branches changed
differently since the branch point. Preview is read-only: it mutates neither
branch.

Each conflict reports what the selected `strategy` would do — `strict` refuses
(`refused`), `source_wins` overwrites the target with the source value. A preview
with no conflicts is clean and a promotion under `strict` would apply. Preview
covers the capabilities a promotion applies — key-value, JSON, and vectors;
events and graphs are diff-only and never appear as promotion conflicts.
Branches with no shared fork lineage are rejected with
`invalid_argument.engine.branch_point`.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Preview promoting a fork into the branch it came from.

### CLI

```console
$ strata kv put config base
created config
$ strata branch fork default experiment
forked experiment from default
$ strata command run --command-json '{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}'  # change on the fork
updated config
$ strata branch preview experiment default --strategy strict  # a clean preview — no conflicting changes on default
source                    experiment
target                    default
strategy                  strict
branch_point              3
spaces_covered            default
capabilities_covered      kv json vector
capabilities_unsupported  vector_collection event graph_metadata graph_node graph_edge graph_ontology
conflicts                 -
derived_state             -
```

### Wire

```json
{"key":"Y29uZmln","type":"kv_put","value":"YmFzZQ=="}
{"branch":"experiment","source":"default","type":"branch_fork_current"}
{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}
{"source":"experiment","strategy":"strict","target":"default","type":"branch_preview"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `source` | `string` | yes | The branch whose changes would be promoted. |
| `strategy` | `PromotionStrategy` | no | Conflict-resolution strategy to evaluate the preview under. |
| `target` | `string` | yes | The branch that would receive the promotion. |

## Returns

`StatusResponse<BranchPreviewItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`invalid_argument.engine.branch_point`](https://stratadb.org/e/invalid_argument.engine.branch_point)

## Invocation

- CLI: `strata branch preview`
- Wire type: `branch_preview`
