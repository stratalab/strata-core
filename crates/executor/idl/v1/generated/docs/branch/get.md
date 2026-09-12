---
title: "Read one branch"
description: "Read one branch summary by name."
source: strata-core@1.2.1
section: branch
---

Reads the summary for one branch: name, deterministic branch id, generation, status, parent lineage, and logical creation version. A branch that does not exist is a `not_found.engine.branch` error, not an empty result.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read one branch by name.

### CLI

```console
$ strata branch create feature
created branch feature
$ strata branch get feature
name        feature
parent      -
status      active
generation  …
created_at  …
deleted_at  -
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"branch":"feature","type":"branch_create"}
{"branch":"feature","type":"branch_get"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<BranchItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)

## Invocation

- CLI: `strata branch get`
- Wire type: `branch_get`
