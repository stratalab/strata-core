# mutants-cfg-filter

A post-filter for the `mutation-on-diff` CI gate (#3254). It removes
cargo-mutants survivors that land in `#[cfg(...)]` spans a lane does not
compile, where a mutant *always* survives and *vacuously* — no test in that
lane can reach the code, so the survivor says nothing about the diff.

## Why it exists

cargo-mutants (27.1.0) mutates source *text* without evaluating conditional
compilation: `cargo mutants --list -p strata-inference --features download,testkit`
and the same with `,local` added emit a byte-identical mutant set. Lane C
compiles `strata-inference` without the `local` feature, so every
`#[cfg(feature = "local")]` item is dead there and its mutants report MISSED.

These used to be suppressed by a hand-maintained list of function-name regexes
in `.cargo/mutants-inference.toml` — a list that grew one entry per incident and
could not reach a `#[cfg]` *block* inside an otherwise-compiled function without
also excluding its compiled twin. This tool derives the suppression from the
source, so a new `local` item needs no new exclusion.

## What it does

Given a cargo-mutants `missed.txt` (survivors, `path:line:col: description`), it
parses each referenced source file with `syn`, evaluates every `#[cfg(..)]`
predicate against the features it is told are inactive, and drops survivors
inside a span that resolves to `false`.

```sh
mutants-cfg-filter --inactive-feature local --in-place mutants.out/missed.txt
```

## Safety

The evaluator is a **denylist** with Kleene three-valued logic. A `feature = "X"`
atom is `False` only when `X` is explicitly named inactive; every atom it does
not recognise is `Unknown`, which is **kept**. A span is dropped only when its
predicate provably resolves to `False`. Under-marking an active span as inactive
is therefore impossible — the filter can never hide a real survivor. Its worst
case is failing to retire an exclusion, which is exactly the pre-#3254 behaviour.

## Why it is workspace-excluded

It depends on `syn`, which nothing else in the workspace does, and it is only
ever built inside the `mutation-on-diff` job. Keeping it out of the root
workspace (`[workspace] exclude`) means it adds nothing to the `--workspace`
build/clippy/check/hack/MSRV jobs, the dependency-direction guard never sees it,
and `syn` never enters the product lockfile — the reason this was chosen over
compiling lane C with `local` (which would add a vendored-llama.cpp cmake build
to every mutation shard). Its own tests run once per PR, in the
`mutation-on-diff` job on shard 0.
