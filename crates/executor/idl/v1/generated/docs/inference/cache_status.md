---
title: "Report model cache"
description: "Report loaded model cache state."
source: strata-core@1.2.2
section: inference
---

Reports the runtime model cache as three lists of specs: the generation, embedding, and ranking models currently loaded in memory. Use it to check what is resident before generating, or to confirm that `inference unload` freed the models you expected. The lists reflect only in-memory engines, not models available on disk.

## Examples

Inspect which models are currently loaded in the runtime cache.

### CLI

```console
$ strata inference cache-status
generation_models  -
embedding_models   -
ranking_models     -
```

### Wire

```json
{"type":"inference_cache_status"}
```

## Parameters

_No parameters._

## Returns

`ModelCacheStatus`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)

## Invocation

- CLI: `strata inference cache-status`
- Wire type: `inference_cache_status`
