---
title: "Sample KV rows"
description: "Sample visible KV rows."
source: strata-core@1.2.2
section: kv
---

Returns a deterministic bounded sample of visible KV rows plus the total matching count.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

A representative sample plus the total population size.

### CLI

```console
$ strata kv put a 1
created a
$ strata kv put b 2
created b
$ strata kv put c 3
created c
$ strata kv sample
KEY  VERSION  VALUE
a          …  1
b          …  2
c          …  3
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"key":"YQ==","type":"kv_put","value":"MQ=="}
{"key":"Yg==","type":"kv_put","value":"Mg=="}
{"key":"Yw==","type":"kv_put","value":"Mw=="}
{"type":"kv_sample"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `count` | `integer` | no | Optional sample count. Defaults to 10. |
| `prefix` | `Bytes` | no | Optional key prefix. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`SamplePage<SampleItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)

## Invocation

- CLI: `strata kv sample`
- Wire type: `kv_sample`
