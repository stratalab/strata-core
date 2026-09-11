---
title: "Delete branch"
description: "Delete an active branch and release its storage claims."
source: strata-core@1.2.1
section: branch
---

Deletes an active branch and reports the deleted branch summary, generation facts, and storage cleanup counts. The `default` branch refuses deletion with `invalid_argument.engine.branch_delete`. Deletion discards the branch's work — promote anything worth keeping onto another branch with `branch merge` before deleting, or keep working on the branch instead.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a branch.

### CLI

```console
$ strata branch create temp
$ strata branch delete temp
$ strata branch list
```

### Wire

```json
{"branch":"temp","type":"branch_create"}
{"branch":"temp","type":"branch_delete"}
{"type":"branch_list"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`invalid_argument.engine.branch_delete`](https://stratadb.org/e/invalid_argument.engine.branch_delete)

## Invocation

- CLI: `strata branch delete`
- Wire type: `branch_delete`
