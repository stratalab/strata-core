---
title: "Read database metrics"
description: "Read lightweight database metrics."
source: strata-core@1.2.2
section: admin
---

Returns lightweight database metrics: the open target, durability, whether the handle is open, the active branch count, the registered space count for the selected branch, and the control-plane health status. The branch defaults to the handle branch when omitted.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read lightweight database metrics.

### CLI

```console
$ strata metrics
control_status  healthy
target          cache
durable         false
branch_count    1
space_count     1
```

### Wire

```json
{"type":"metrics"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<AdminMetrics>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)

## Invocation

- CLI: `strata metrics`
- Wire type: `metrics`
