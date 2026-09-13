---
title: "Download a model"
description: "Download an inference model locally."
source: strata-core@1.2.2
section: inference
---

Resolves a catalog name or model spec and downloads the model artifact into the local model directory, returning the resolved local path. A model that is already present is not downloaded again: the command returns its path in every build, with or without network access. Honors `STRATA_MODELS_DIR` for the destination and `STRATA_HF_ENDPOINT` and `STRATA_HF_TOKEN` (or `HF_TOKEN`) for gated HuggingFace repositories. The spec is resolved before anything else is checked: a malformed spec returns `inference.invalid_request`, a cloud provider spec returns `inference.unsupported_operation` (there is nothing to download), and a name that is not in the catalog returns `inference.missing_model`. A missing model is downloaded only when the runtime has network access and the build can download; otherwise the command returns `inference.download_disabled`.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | yes | Model spec or catalog name. |

## Returns

`PullModelOutput`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`inference.invalid_request`](https://stratadb.org/e/inference.invalid_request)
- [`inference.unsupported_operation`](https://stratadb.org/e/inference.unsupported_operation)
- [`inference.download_disabled`](https://stratadb.org/e/inference.download_disabled)
- [`inference.missing_model`](https://stratadb.org/e/inference.missing_model)
- [`inference.unknown_model`](https://stratadb.org/e/inference.unknown_model)
- [`inference.download_failed`](https://stratadb.org/e/inference.download_failed)
- [`inference.download_verification_failed`](https://stratadb.org/e/inference.download_verification_failed)
- [`inference.io_failure`](https://stratadb.org/e/inference.io_failure)
- [`inference.registry_corrupt`](https://stratadb.org/e/inference.registry_corrupt)

## Invocation

- CLI: `strata inference models pull`
- Wire type: `inference_models_pull`
