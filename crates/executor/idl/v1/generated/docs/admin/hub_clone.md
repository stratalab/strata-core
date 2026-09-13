---
title: "Clone hub dataset"
description: "Clone a dataset from a hub into a new local database."
source: strata-core@1.2.2
section: admin
---

Clones a dataset from a hub into a new local database directory. Resolution, download, verification, reconstitution, and origin recording run once behind this command; the session database is not touched. The destination directory must not exist or must be empty. When the hub URL is not given, the layered resolver selects it from the flag, environment, and config layers.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `dataset` | `string` | yes | Dataset to clone. |
| `dest` | `string` | yes | Destination directory (must not exist, or be empty). |
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs (flag, `STRATA_HUB_URL`, project config, global config). |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`StatusResponse<HubCloneResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`invalid_argument.executor.hub_dataset`](https://stratadb.org/e/invalid_argument.executor.hub_dataset)
- [`invalid_argument.executor.hub_branch`](https://stratadb.org/e/invalid_argument.executor.hub_branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`failed_precondition.executor.hub_clone`](https://stratadb.org/e/failed_precondition.executor.hub_clone)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)

## Invocation

- CLI: `strata clone`
- Wire type: `hub_clone`
