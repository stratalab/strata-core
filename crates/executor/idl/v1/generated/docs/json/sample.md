---
title: "Sample JSON documents"
description: "Sample visible JSON documents."
source: strata-core@1.2.2
section: json
---

Returns a bounded sample of visible JSON documents plus the total matching count. Useful for inspecting document shape before writing queries or indexes.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

A representative sample plus the total population size.

### CLI

```console
$ strata json set a '$' '{"v":1}'
created a
$ strata json set b '$' '{"v":2}'
created b
$ strata json set c '$' '{"v":3}'
created c
$ strata json sample
KEY  VERSION  VALUE
a          …  {"v":1}
b          …  {"v":2}
c          …  {"v":3}
```

`…` stands for a value that varies by run or by machine — an instant, a version, an id, a path.

### Wire

```json
{"key":"a","path":"$","type":"json_set","value":{"v":1}}
{"key":"b","path":"$","type":"json_set","value":{"v":2}}
{"key":"c","path":"$","type":"json_set","value":{"v":3}}
{"type":"json_sample"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `count` | `integer` | no | Optional sample count. Defaults to 10. |
| `prefix` | `string` | no | Optional document key prefix. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`SamplePage<JsonSampleItem>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.json_document_id`](https://stratadb.org/e/invalid_argument.engine.json_document_id)

## Invocation

- CLI: `strata json sample`
- Wire type: `json_sample`
