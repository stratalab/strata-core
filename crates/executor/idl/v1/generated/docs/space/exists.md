---
title: "Check product space existence"
description: "Check whether a product space exists on a branch."
source: strata-core@1.2.1
section: space
---

Reports whether the named product space is cataloged on the target branch. A missing space is `false`, not an error; reserved names are rejected with `invalid_argument.engine.product_space_reserved`.

Status commands return a scalar or compact status payload and do not mutate database state.

## Examples

Check whether a product space exists.

### CLI

```console
$ strata space create app
created space app
$ strata space exists app
true
$ strata space exists nope
false
```

### Wire

```json
{"space":"app","type":"space_create"}
{"space":"app","type":"space_exists"}
{"space":"nope","type":"space_exists"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusValue<boolean>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.product_space_reserved`](https://stratadb.org/e/invalid_argument.engine.product_space_reserved)

## Invocation

- CLI: `strata space exists`
- Wire type: `space_exists`
