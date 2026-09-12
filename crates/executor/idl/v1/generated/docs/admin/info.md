---
title: "Read database info"
description: "Read database identity and a catalog summary."
source: strata-core@1.2.1
section: admin
---

Returns database identity and a catalog summary for one branch: engine version, open target, whether this open created the database, durability, the default branch, the active branch count, the registered space count for the selected branch, and the resolved storage `memory_budget` (its `total_bytes` and its `source` — `explicit`, `derived_from_host`, or `fixed_default`). When no budget is set, the engine derives the default from host memory at open — 25% of usable memory, clamped to a ceiling — so `memory_budget` reveals what the database is actually sized to. The branch defaults to the handle branch when omitted.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Read database identity and catalog counts.

### CLI

```console
$ strata info
target          cache
version         …
durable         false
default_branch  default
branch_count    1
space_count     1
memory_budget
  source       derived_from_host
  total        … …
  usable_host  … …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"type":"info"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<AdminDatabaseInfo>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)

## Invocation

- CLI: `strata info`
- Wire type: `info`
