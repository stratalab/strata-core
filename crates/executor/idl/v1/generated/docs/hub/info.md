---
title: "Read hub info"
description: "Read the selected StrataHub's V1 capability advertisement."
source: strata-core@1.2.2
section: hub
---

Reads `GET /v1/info` from the effective hub URL resolved by the shared Strata resolver.

Status commands return a scalar or compact status payload and do not mutate database state.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `hub_url` | `string` | no | Explicit hub URL; when absent the 5-layer resolver runs (flag, `STRATA_HUB_URL`, project config, global config). |

## Returns

`StatusResponse<HubInfo>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`unsupported.executor.hub_feature_disabled`](https://stratadb.org/e/unsupported.executor.hub_feature_disabled)
- [`invalid_argument.executor.hub_url`](https://stratadb.org/e/invalid_argument.executor.hub_url)
- [`unavailable.executor.hub_transport`](https://stratadb.org/e/unavailable.executor.hub_transport)

## Invocation

- CLI: `strata hub info`
- Wire type: `hub_info`
