---
title: "Read database health"
description: "Read control-plane health facts."
source: strata-core@1.2.1
section: admin
---

Returns control-plane health facts: the worst overall status plus per-subsystem status for identity, registry, branch catalog, and the optional branch-local space catalog. Also reports the default branch and active branch count. A healthy result means every required control-plane fact is present and readable.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read control-plane health.

### CLI

```console
$ strata health
status          healthy
identity        healthy
registry        healthy
branch_catalog  healthy
space_catalog   healthy
default_branch  default
branch_count    1
```

### Wire

```json
{"type":"health"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<AdminHealth>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)

## Invocation

- CLI: `strata health`
- Wire type: `health`
