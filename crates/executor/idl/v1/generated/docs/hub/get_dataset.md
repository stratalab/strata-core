---
title: "Read hub dataset"
description: "Read one StrataHub dataset card."
source: strata-core@1.2.2
section: hub
---

Reads `GET /v1/datasets/<name>` from the effective hub URL and returns the dataset card in an executor-owned JSON envelope.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs. |
| `name` | `string` | yes | Dataset slug. |

## Returns

`StatusResponse<HubDatasetCard>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)
- [`invalid_argument.executor.hub_dataset`](https://stratadb.org/e/invalid_argument.executor.hub_dataset)
- [`not_found.executor.hub_dataset`](https://stratadb.org/e/not_found.executor.hub_dataset)

## Invocation

- CLI: `strata hub get-dataset`
- Wire type: `hub_get_dataset`
