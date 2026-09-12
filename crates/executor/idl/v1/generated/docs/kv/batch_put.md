---
title: "Batch put KV values"
description: "Store multiple KV values in one itemwise batch."
source: strata-core@1.2.1
section: kv
---

Writes multiple KV entries using the executor batch contract. Valid items share commit facts where the underlying engine applies them together.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Write many entries in one commit.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
#  STATUS  EFFECT   KEY
0  ok      created  a
1  ok      created  b
$ strata command run --command-json '{"keys":["YQ==","Yg=="],"type":"kv_batch_get"}'
#  STATUS  KEY  VERSION  VALUE
0  ok      a          …  1
1  ok      b          …  2
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}
{"keys":["YQ==","Yg=="],"type":"kv_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `entries` | `BatchKvEntry[]` | yes | Entries to write. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)
- [`invalid_argument.engine.kv_batch`](https://stratadb.org/e/invalid_argument.engine.kv_batch)
- [`invalid_argument.engine.kv_batch_duplicate_key`](https://stratadb.org/e/invalid_argument.engine.kv_batch_duplicate_key)
- [`invalid_argument.executor.kv_batch_duplicate_key`](https://stratadb.org/e/invalid_argument.executor.kv_batch_duplicate_key)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `kv_batch_put`
