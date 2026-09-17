# Read historical state

Read what a value used to be, without restoring anything.

Mirrors [Read historical state](https://stratadb.org/docs/guides/branching/read-historical-state).

## Every write keeps the one before it

```console
$ strata ./shop kv put price 100
created price
$ strata ./shop kv put price 150
updated price
$ strata ./shop kv put price 200
updated price
```

Nothing was overwritten. `history` shows the versions, newest first:

```console
$ strata ./shop kv history price
…VALUE
…200
…150
…100
```

Two independent ways to name a moment sit in that table, and they are not
interchangeable.

## Read at a version

`--as-of` takes the number from the `VERSION` column: a position on the commit
timeline, not a date.

```console
$ strata ./shop kv get price --as-of 3
100
$ strata ./shop kv get price --as-of 4
150
```

The current value is untouched by having read an old one:

```console
$ strata ./shop kv get price
200
```

## A version that never existed is refused

Asking for a moment outside the retained window is an error rather than a
nearest-match guess, so a caller never mistakes a clamp for an answer:

```console
$ strata ./shop kv get price --as-of 99
history_unavailable.engine.persistence_history: requested persistence history is unavailable (…)
  hint: Request history inside the retained window.
  ref: https://stratadb.org/e/history_unavailable.engine.persistence_history
```
