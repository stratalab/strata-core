---
title: "List hub dataset refs"
description: "List live refs for a StrataHub dataset."
source: strata-core@1.2.2
section: hub
---

Reads `GET /v1/datasets/<name>/refs` from the effective hub URL. Yanked refs are filtered by the hub.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `dataset` | `string` | yes | Dataset slug. |
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs. |

## Returns

`StatusResponse<HubRefList>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)
- [`invalid_argument.executor.hub_dataset`](https://stratadb.org/e/invalid_argument.executor.hub_dataset)
- [`not_found.executor.hub_dataset`](https://stratadb.org/e/not_found.executor.hub_dataset)

## Invocation

- CLI: `strata hub list-refs`
- Wire type: `hub_list_refs`
