---
title: "Create empty branch"
description: "Create a new empty root branch."
source: strata-core@1.2.1
section: branch
---

Creates an empty root branch with no parent and no data. This is not a fork: the new branch starts from nothing, and its `parent` is null. Use `branch.fork` to start from an existing branch's data. Creating a name that already exists fails with `already_exists.engine.branch`; names reserved for engine control data are rejected.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Create a new empty branch.

### CLI

```console
$ strata branch create feature
created branch feature
$ strata branch list
NAME     PARENT  STATUS  GENERATION
default  -       active           …
feature  -       active           …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"branch":"feature","type":"branch_create"}
{"type":"branch_list"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<BranchItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`already_exists.engine.branch`](https://stratadb.org/e/already_exists.engine.branch)

## Invocation

- CLI: `strata branch create`
- Wire type: `branch_create`
