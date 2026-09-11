---
title: "List catalog models"
description: "List catalog inference models."
source: strata-core@1.2.1
section: inference
---

Lists every model in Strata's built-in catalog as a terminal page. Each entry reports the model's task (embed, generate, or rank), architecture, default quantization, embedding dimension, HuggingFace repository, approximate artifact size, and whether the model artifact is already present in the local model directory. Use `inference models local` to see only the downloaded models, or `inference models pull` to fetch one.

## Examples

List the built-in model catalog (spans embedding, generation, ranking).

### CLI

```console
$ strata inference models list
```

### Wire

```json
{"type":"inference_models_list"}
```

## Parameters

_No parameters._

## Returns

`Page<ModelInfo>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference models list`
- Wire type: `inference_models_list`
