---
title: "Rank passages"
description: "Rank passages against a query."
source: strata-core@1.2.1
section: inference
---

Scores each candidate passage against a query with a ranking model and returns one outcome per passage. Each item carries the passage's original index and either a relevance score or a per-item error with a stable code and a redacted message, so callers can reorder passages by score while keeping them tied to their inputs. Ranking is a local-only operation: it requires a build with the local execution feature and a ranking-capable model.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | yes | Model spec. |
| `passages` | `string[]` | yes | Candidate passages. |
| `query` | `string` | yes | Query text. |

## Returns

`RankResponse`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.unknown_model`](https://stratadb.org/e/inference.unknown_model)
- [`inference.model_load_failed`](https://stratadb.org/e/inference.model_load_failed)
- [`inference.local_runtime_failed`](https://stratadb.org/e/inference.local_runtime_failed)
- [`inference.registry_corrupt`](https://stratadb.org/e/inference.registry_corrupt)

## Invocation

- CLI: `strata inference rank`
- Wire type: `inference_rank`
