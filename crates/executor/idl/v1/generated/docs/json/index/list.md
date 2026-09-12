---
title: "List JSON indexes"
description: "List JSON secondary indexes."
source: strata-core@1.2.1
section: json
---

Lists JSON secondary index definitions in the selected branch and space, including each index's field path and kind.

Paginated responses use opaque cursors. Clients should pass the returned cursor back to the same command shape and must not parse cursor contents.

## Examples

List secondary indexes.

### CLI

```console
$ strata json index create by_name $.name --index-type tag
created index by_name on name (tag)
$ strata json index list
NAME     FIELD_PATH  INDEX_TYPE  SPACE    CREATED_VERSION
by_name  name        tag         default                3
```

### Wire

```json
{"field_path":"$.name","index_type":"tag","name":"by_name","type":"json_create_index"}
{"type":"json_list_indexes"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`Page<JsonIndexDefinition>`.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)

## Invocation

- CLI: `strata json index list`
- Wire type: `json_list_indexes`
