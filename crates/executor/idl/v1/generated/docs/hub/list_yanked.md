---
title: "List yanked hub refs"
description: "List yanked refs from the selected StrataHub."
source: strata-core@1.2.2
section: hub
---

Reads `GET /v1/yanked` from the effective hub URL. `since`, when supplied, must be an RFC 3339 timestamp.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs. |
| `since` | `string` | no | RFC 3339 lower-bound timestamp. |

## Returns

`StatusResponse<HubYankedList>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)
- [`invalid_argument.executor.hub_since`](https://stratadb.org/e/invalid_argument.executor.hub_since)
- [`not_found.executor.hub_resource`](https://stratadb.org/e/not_found.executor.hub_resource)

## Invocation

- CLI: `strata hub list-yanked`
- Wire type: `hub_list_yanked`
