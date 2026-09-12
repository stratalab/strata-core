---
title: "Unload cached models"
description: "Unload cached inference models."
source: strata-core@1.2.1
section: inference
---

Removes cached model engines from the runtime to free memory. Pass a model spec to unload one entry, or omit it to unload every cached generation, embedding, and ranking model. The result reports whether any cached entry was actually removed. This affects only the in-memory runtime cache; it never deletes downloaded model files from disk.

## Examples

Evict cached models from the runtime (a no-op when nothing is loaded).

### CLI

```console
$ strata inference unload
no cached entry
```

### Wire

```json
{"type":"inference_unload"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `model` | `string` | no | Optional model spec. |

## Returns

`InferenceUnloadResult`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference unload`
- Wire type: `inference_unload`
