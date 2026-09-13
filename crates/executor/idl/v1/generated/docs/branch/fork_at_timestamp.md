---
title: "Fork branch at timestamp"
description: "Fork a new branch from a retained source timestamp."
source: strata-core@1.2.2
section: branch
---

Forks a new branch anchored at a retained source timestamp (microseconds, on Strata's logical commit clock). The engine resolves the timestamp to the covering retained commit; the returned parent lineage records both the fork timestamp and the resolved fork version. A timestamp outside retained history fails with `history_unavailable.engine.persistence_history`.

This command has no dedicated CLI verb: the CLI expresses it as `strata branch fork <SOURCE> <BRANCH> --timestamp <TIMESTAMP>` (one shared `branch fork` verb routes to all three fork commands, so only `branch.fork` owns the CLI path). It remains fully reachable through the generic wire surface — `strata command run`, MCP, and SDKs.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Fork a branch at an earlier commit timestamp — time-travel into a branch.

### CLI

```console
$ strata kv put greeting original  # The receipt carries this commit's timestamp (microseconds).
created greeting
$ strata kv put greeting updated
updated greeting
$ strata command run --command-json '{"branch":"snapshot","source":"default","timestamp":3,"type":"branch_fork_at_timestamp"}'  # snapshot forks default's history as of that instant.
forked snapshot from default at timestamp 3
$ strata command run --command-json '{"branch":"snapshot","key":"Z3JlZXRpbmc=","type":"kv_get"}'
original
```

### Wire

```json
{"key":"Z3JlZXRpbmc=","type":"kv_put","value":"b3JpZ2luYWw="}
{"key":"Z3JlZXRpbmc=","type":"kv_put","value":"dXBkYXRlZA=="}
{"branch":"snapshot","source":"default","timestamp":3,"type":"branch_fork_at_timestamp"}
{"branch":"snapshot","key":"Z3JlZXRpbmc=","type":"kv_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `source` | `string` | yes | Source branch name. |
| `timestamp` | `integer` | yes | Source timestamp in microseconds. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<BranchItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.branch_name`](https://stratadb.org/e/invalid_argument.engine.branch_name)
- [`invalid_argument.engine.branch_name_reserved`](https://stratadb.org/e/invalid_argument.engine.branch_name_reserved)
- [`already_exists.engine.branch`](https://stratadb.org/e/already_exists.engine.branch)
- [`history_unavailable.engine.persistence_history`](https://stratadb.org/e/history_unavailable.engine.persistence_history)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `branch_fork_at_timestamp`
