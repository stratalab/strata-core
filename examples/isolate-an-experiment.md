# Isolate an experiment

Try a change without risking what already works.

Mirrors [Isolate an experiment](https://stratadb.org/docs/guides/branching/isolate-an-experiment).

## Fork, then diverge

A fork is cheap and immediate: it copies no data, it names a moment.

```console
$ strata ./shop json set portfolio '$' '{"strategy":"balanced","stocks":60,"cash":10}'
created portfolio
$ strata ./shop branch fork default risky
forked risky from default
$ strata ./shop --branch risky json set portfolio '$.strategy' '"aggressive"'
updated portfolio
$ strata ./shop --branch risky json set portfolio '$.stocks' 80
updated portfolio
```

The experiment moved and `default` did not:

```console
$ strata ./shop --branch risky json get portfolio '$.strategy'
"aggressive"
$ strata ./shop json get portfolio '$.strategy'
"balanced"
```

## See what diverged

```console
$ strata ./shop branch diff default risky
branch_a  default
branch_b  risky

SPACE    CAPABILITY  CHANGE    IDENTITY   VERSION
default  json        modified  portfolio        …
```

## Keep it, or delete it

Nothing has to be unwound. A branch that did not work out is deleted, and
`default` never saw it:

```console
$ strata ./shop branch delete risky
deleted branch risky
$ strata ./shop json get portfolio '$.stocks'
60
```
