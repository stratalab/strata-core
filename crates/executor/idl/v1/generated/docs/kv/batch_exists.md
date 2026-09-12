---
title: "Batch check KV existence"
description: "Check existence for multiple KV keys."
source: strata-core@1.2.1
section: kv
---

Checks several KV keys and returns positional boolean status values. The response preserves the input order.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Check existence for many keys at once.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="}],"type":"kv_batch_put"}'
#  STATUS  EFFECT   KEY
0  ok      created  a
$ strata command run --command-json '{"keys":["YQ==","bWlzc2luZw=="],"type":"kv_batch_exists"}'
#  STATUS  KEY      EXISTS
0  ok      a        true
1  ok      missing  false
```

### Wire

```json
{"entries":[{"key":"YQ==","value":"MQ=="}],"type":"kv_batch_put"}
{"keys":["YQ==","bWlzc2luZw=="],"type":"kv_batch_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `keys` | `Bytes[]` | yes | Keys to check. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchExistsItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)
- [`invalid_argument.engine.kv_batch`](https://stratadb.org/e/invalid_argument.engine.kv_batch)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `kv_batch_exists`
