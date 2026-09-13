---
title: "Delete product space"
description: "Delete a product space from a branch."
source: strata-core@1.2.2
section: space
---

Drops the product space from the branch catalog. The `default` space refuses deletion with `invalid_argument.engine.space_delete_default`. A space that still contains visible data refuses deletion with `failed_precondition.engine.space_not_empty` unless `force: true` is set, which tombstones the visible rows first and reports the count. Deleting a space that does not exist succeeds with `deleted: false`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Delete a product space.

### CLI

```console
$ strata space create temp
created space temp
$ strata space delete temp
deleted space temp (0 rows)
$ strata space exists temp
false
```

### Wire

```json
{"space":"temp","type":"space_create"}
{"space":"temp","type":"space_delete"}
{"space":"temp","type":"space_exists"}
```

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `force` | `boolean` | no | Delete visible data in the space before dropping the catalog entry. |

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.product_space_reserved`](https://stratadb.org/e/invalid_argument.engine.product_space_reserved)
- [`invalid_argument.engine.space_delete_default`](https://stratadb.org/e/invalid_argument.engine.space_delete_default)
- [`failed_precondition.engine.space_not_empty`](https://stratadb.org/e/failed_precondition.engine.space_not_empty)
- [`invalid_argument.engine.space_delete_too_large`](https://stratadb.org/e/invalid_argument.engine.space_delete_too_large)

## Invocation

- CLI: `strata space delete`
- Wire type: `space_delete`
