---
title: "Fork branch from current head"
description: "Fork a new branch from the current head of a source branch."
source: strata-core@1.2.1
section: branch
---

Forks a new branch from the source branch's current head. The new branch sees all data visible on the source at fork time; later writes on either branch stay isolated. The returned branch summary records the parent name, fork version, and generation.

On the CLI, all three fork commands share the single verb `strata branch fork <SOURCE> <BRANCH>`: with no flags it runs this command, while `--version` routes to `branch.fork_at_version` and `--timestamp` routes to `branch.fork_at_timestamp` (both wire-surface commands).

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Fork a branch from another branch's head.

### CLI

```console
$ strata branch fork default experiment
forked experiment from default
$ strata branch list
NAME        PARENT   STATUS  GENERATION
default     -        active           …
experiment  default  active           …
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"branch":"experiment","source":"default","type":"branch_fork_current"}
{"type":"branch_list"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `source` | `string` | yes | Source branch name. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<BranchItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`already_exists.engine.branch`](https://stratadb.org/e/already_exists.engine.branch)

## Invocation

- CLI: `strata branch fork`
- Wire type: `branch_fork_current`
