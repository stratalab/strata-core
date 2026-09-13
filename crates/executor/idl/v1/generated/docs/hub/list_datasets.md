---
title: "List hub datasets"
description: "List datasets from the selected StrataHub."
source: strata-core@1.2.2
section: hub
---

Reads `GET /v1/datasets` from the effective hub URL. Repeatable task, tag, and primitive filters preserve the StrataHub V1 query shape.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs. |
| `license` | `string` | no | License filter. |
| `limit` | `integer` | no | Page size. |
| `offset` | `integer` | no | Zero-based page offset. |
| `primitives` | `string[]` | no | Primitive filters. |
| `size_max_bytes` | `integer` | no | Maximum dataset size in bytes. |
| `size_min_bytes` | `integer` | no | Minimum dataset size in bytes. |
| `sort` | `HubDatasetSort` | no | Sort key. |
| `tags` | `string[]` | no | Tag filters. |
| `tasks` | `string[]` | no | Task filters. |

## Returns

`StatusResponse<HubDatasetPage>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)
- [`invalid_argument.executor.hub_filter`](https://stratadb.org/e/invalid_argument.executor.hub_filter)

## Invocation

- CLI: `strata hub list-datasets`
- Wire type: `hub_list_datasets`
