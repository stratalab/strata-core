---
title: "Create product space"
description: "Create a product space on a branch."
source: strata-core@1.2.1
section: space
---

Creates a product space in the branch catalog. Creation is idempotent: creating a space that already exists succeeds with `created: false` and no mutation effect. Names reserved for engine control data are rejected with `invalid_argument.engine.product_space_reserved`.

Successful mutations return an acknowledgement of the outcome: for a state-changing write, the affected target with the mutation effect and commit facts; for mutations that produce a domain result (such as a branch or a promotion outcome), that result object.

## Examples

Create a product space.

### CLI

```console
$ strata space create app
$ strata space list
```

### Wire

```json
{"space":"app","type":"space_create"}
{"type":"space_list"}
```

## Parameters

_No parameters._

Plus the optional scope: `branch` and `space` (default to the session branch and the `"default"` space).

## Returns

`MutationAck` — an acknowledgement with no payload.

## Errors

- [`failed_precondition.engine.runtime_closed`](https://stratadb.org/e/failed_precondition.engine.runtime_closed)
- [`not_found.engine.branch`](https://stratadb.org/e/not_found.engine.branch)
- [`invalid_argument.engine.product_space`](https://stratadb.org/e/invalid_argument.engine.product_space)
- [`invalid_argument.engine.product_space_reserved`](https://stratadb.org/e/invalid_argument.engine.product_space_reserved)

## Invocation

- CLI: `strata space create`
- Wire type: `space_create`
