---
title: "Compare branches"
description: "Compare two branches and report the entities that differ across every primitive."
source: strata-core@1.2.2
section: branch
---

Compares two branches and reports the authored entities that differ, grouped by
capability and space: entries `added` on `branch_b`, `removed` relative to
`branch_a`, and `modified` on both sides. The comparison is directional from
`branch_a` to `branch_b`.

Every data primitive is compared — key-value, JSON documents, vectors, event
streams, and graphs. Graph changes are reported per row class: nodes, edges, and
ontology appear as separate capabilities in the result. Derived rows (search and
vector indexes, graph reverse maps) are omitted. A missing branch is rejected
with `not_found.engine.branch`.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Compare a fork against the branch it came from, across primitives.

### CLI

```console
$ strata vector collection create notes 2 --metric cosine
created collection notes (2 dimensions, cosine)
$ strata kv put config base
created config
$ strata branch fork default experiment
forked experiment from default
$ strata command run --command-json '{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}'  # diverge the key-value entry on the fork
updated config
$ strata command run --command-json '{"branch":"experiment","collection":"notes","key":"n1","type":"vector_upsert","vector":[0.1,0.2]}'  # add a vector on the fork
created n1 in notes
$ strata branch diff default experiment  # reports the key-value change and the new vector, grouped by capability
branch_a  default
branch_b  experiment

SPACE    CAPABILITY  CHANGE    IDENTITY             VERSION
default  kv          modified  config                     …
default  vector      added     base64:AAVub3Rlc24x        …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"collection":"notes","dimension":2,"metric":"cosine","type":"vector_create_collection"}
{"key":"Y29uZmln","type":"kv_put","value":"YmFzZQ=="}
{"branch":"experiment","source":"default","type":"branch_fork_current"}
{"branch":"experiment","key":"Y29uZmln","type":"kv_put","value":"dHVuZWQ="}
{"branch":"experiment","collection":"notes","key":"n1","type":"vector_upsert","vector":[0.1,0.2]}
{"branch_a":"default","branch_b":"experiment","type":"branch_diff"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `at_timestamp` | `integer` | no | Optional read-as-of commit timestamp: compare each branch as of the `timestamp` from `history` output (a commit-timeline position, not the `version`). |
| `branch_a` | `string` | yes | The first branch (the `A` side). |
| `branch_b` | `string` | yes | The second branch (the `B` side). |

## Returns

`StatusResponse<BranchComparisonItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)

## Invocation

- CLI: `strata branch diff`
- Wire type: `branch_diff`
