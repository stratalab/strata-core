---
title: "Batch delete KV values"
description: "Delete multiple KV keys in one itemwise batch."
source: strata-core@1.2.1
section: kv
---

Deletes multiple KV keys and returns one positional mutation result per key. Missing keys are represented as no-op item results.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Delete many keys in one commit.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
$ strata command run --command-json '{"keys":["YQ==","Yg=="],"type":"kv_batch_delete"}'
$ strata command run --command-json '{"keys":["YQ==","Yg=="],"type":"kv_batch_exists"}'
```

### Wire

```json
{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}
{"keys":["YQ==","Yg=="],"type":"kv_batch_delete"}
{"keys":["YQ==","Yg=="],"type":"kv_batch_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `keys` | `Bytes[]` | yes | Keys to delete. |

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
- Wire type: `kv_batch_delete`
