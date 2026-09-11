---
title: "Delete KV value"
description: "Delete one visible KV key."
source: strata-core@1.2.1
section: kv
---

Deletes the current visible value for a KV key. Missing keys produce a no-op delete acknowledgement rather than a read-style missing value.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a key; it is no longer visible afterward.

### CLI

```console
$ strata kv put temp scratch
$ strata kv delete temp
$ strata kv exists temp
```

### Wire

```json
{"key":"dGVtcA==","type":"kv_put","value":"c2NyYXRjaA=="}
{"key":"dGVtcA==","type":"kv_delete"}
{"key":"dGVtcA==","type":"kv_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `key` | `Bytes` | yes | Key bytes. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.kv_key`](https://stratadb.org/e/invalid_argument.engine.kv_key)

## Invocation

- CLI: `strata kv delete`
- Wire type: `kv_delete`
