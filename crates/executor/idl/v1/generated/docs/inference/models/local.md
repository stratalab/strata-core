---
title: "List local models"
description: "List locally downloaded inference models."
source: strata-core@1.2.2
section: inference
---

Lists the catalog models that have at least one quantization variant present in the local model directory, as a terminal page. Entries carry the same facts as `inference models list` but are restricted to models that can run without a further download. The local model directory is resolved from `STRATA_MODELS_DIR`, falling back to `~/.strata/models`.

## Parameters

_No parameters._

## Returns

`Page<ModelInfo>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference models local`
- Wire type: `inference_models_local`
