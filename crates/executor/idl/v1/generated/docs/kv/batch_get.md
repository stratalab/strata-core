---
title: "Batch get KV values"
description: "Read multiple KV values by key."
source: strata-core@1.2.1
section: kv
---

Reads several KV keys and returns positional item results. Each item records whether the corresponding key was found and includes value metadata when present.

Itemwise batches return one positional item result per input item. The outer batch status summarizes whether all, some, or none of the items succeeded.

## Examples

Read many keys at once; a missing key comes back as null.

### CLI

```console
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
$ strata command run --command-json '{"keys":["YQ==","Yg==","bWlzc2luZw=="],"type":"kv_batch_get"}'
```

### Wire

```json
{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}
{"keys":["YQ==","Yg==","bWlzc2luZw=="],"type":"kv_batch_get"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `keys` | `Bytes[]` | yes | Keys to read. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`BatchResult<BatchGetItemResult>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)
- [`invalid_argument.engine.kv_batch`](https://stratadb.org/e/invalid_argument.engine.kv_batch)

## Invocation

- CLI: via `strata command run` (no dedicated verb)
- Wire type: `kv_batch_get`
