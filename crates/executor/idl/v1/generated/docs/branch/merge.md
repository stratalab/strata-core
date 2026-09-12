---
title: "Promote branch"
description: "Promote one branch's changes into another as a single atomic commit."
source: strata-core@1.2.1
section: branch
---

Promotes the `source` branch's changes into the `target` branch as a single
atomic commit, leaving the source unchanged. The branch point is derived from
the recorded fork lineage, and a three-way merge applies every change the source
made since that point.

Merge applies to key-value, JSON, and vector data. Event streams and graphs are
compared (see `branch.diff`) but never merged — divergent append-only and
structural data cannot be three-way merged — so a promotion leaves them
untouched.

The `strict` strategy (the default) refuses with `conflict.engine.promotion`,
mutating nothing, when the two branches changed the same entity differently since
the branch point. The `source_wins` strategy applies the source side's value or
tombstone for each such conflict and reports every overwritten or deleted target
entry. A promotion that applies nothing writes no commit and leaves the target
unchanged.

Branches with no shared fork lineage are rejected with
`invalid_argument.engine.branch_point`; a missing branch with
`not_found.engine.branch`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Promote a fork's change back into the branch it came from.

### CLI

```console
$ strata kv put config base
created config
$ strata branch fork default experiment
forked experiment from default
$ strata command run --command-json '{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}'  # change on the fork
updated config
$ strata branch merge experiment default --strategy strict  # applies the fork's change onto default
merged experiment into default: 1 applied, 0 deleted, 0 conflicts (version 9)
```

### Wire

```json
{"key":"Y29uZmln","type":"kv_put","value":"YmFzZQ=="}
{"branch":"experiment","source":"default","type":"branch_fork_current"}
{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}
{"source":"experiment","strategy":"strict","target":"default","type":"branch_merge"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `source` | `string` | yes | The branch whose changes are promoted. |
| `strategy` | `PromotionStrategy` | no | Conflict-resolution strategy (`strict` refuses on conflict). |
| `target` | `string` | yes | The branch that receives the promotion. |

## Returns

`StatusResponse<PromotionOutcomeItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`conflict.engine.promotion`](https://stratadb.org/e/conflict.engine.promotion)
- [`invalid_argument.engine.branch_point`](https://stratadb.org/e/invalid_argument.engine.branch_point)

## Invocation

- CLI: `strata branch merge`
- Wire type: `branch_merge`
