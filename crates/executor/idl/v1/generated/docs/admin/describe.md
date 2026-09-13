---
title: "Describe database"
description: "Read a compact description of the database."
source: strata-core@1.2.2
section: admin
---

Returns a compact description of the database for one branch: engine version, open target, the default and described branches, all active branches, the registered product spaces, per-primitive counts (KV, JSON, event, plus vector-collection and graph summaries), the sanitized config, and the available capabilities. The branch defaults to the handle branch when omitted.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Describe the database.

### CLI

```console
$ strata describe
StrataDB … · cache
branch default · branches: default · spaces: default
capabilities: arrow event graph_core inference json kv vector vector_index
kv 0 · json 0 · events 0
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"type":"describe"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<AdminDescribe>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)

## Invocation

- CLI: `strata describe`
- Wire type: `describe`
