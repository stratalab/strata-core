# Gap 07: Merge-Base Correctness for Repeated Merges Is Partially Externalized

## Summary

The engine's branch API does not fully own merge-base computation for all supported branch
histories. Correct merge-base selection for repeated merges and materialized branches depends
on an executor-supplied override derived from branch-DAG state.

This is an architectural gap because merge-base selection is part of merge correctness, not
just a performance optimization.

## What The Code Does Today

### Engine merge-base logic has a priority override

`compute_merge_base()` uses this order:

1. `merge_base_override` supplied by the executor
2. storage fork info from `InheritedLayer`
3. no relationship

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:1439-1485`

### Public branch APIs document that the override comes from the executor

Both `merge_branches()` and `get_merge_base()` say that `merge_base_override` is populated by
the executor from DAG data to cover repeated merges and materialized branches.

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:1845-1853`
- `crates/engine/src/branch_ops/mod.rs:2246-2252`

### The executor documents why it is needed

The executor comment is explicit: without branch-DAG hooks, repeated merges of the same
source and target cannot advance the merge base correctly.

Code evidence:

- `crates/executor/src/api/mod.rs:91-97`

## Why This Is An Architectural Gap

For a simple child-to-parent merge, storage-level fork info may be enough.

For repeated merges, the correct base is usually not the original fork point anymore. It is
the last merge-relevant common point after prior merge history is considered.

If the engine only knows "branch B was forked from branch A at version V," then repeated
merges risk being computed against an ancestry picture that is too old.

That can change:

- which keys appear changed since the base
- which conflicts are detected
- how much old history is reconsidered

## Failure Modes

### Repeated merges can reason from a stale ancestor

Inference from the code structure:

If DAG-derived override is missing, the engine can fall back to original fork info even after
earlier merges have already advanced the logical common history between the branches.

That can produce:

- larger-than-necessary diffs
- re-evaluation of already-merged history
- extra conflicts

### Materialized branches lose the storage shortcut

The public comments explicitly mention materialized branches. That means storage-level fork
metadata is not sufficient in every branch lifecycle once copy-on-write ancestry stops being
the whole story.

## Why The Branch DAG Matters Here

The branch DAG is not only an audit artifact. In this design it is also the only place that
can preserve enough merge history to answer "what is the right merge base now?" across
histories richer than a single live fork edge.

That is why treating DAG handling as optional or best-effort has correctness implications,
not just observability implications.

## Likely Remediation Direction

Inference from the current code shape:

1. Internalize authoritative merge ancestry into engine-owned branch metadata instead of
   depending on an external override parameter.
2. Keep the DAG as a query and audit surface if desired, but make merge-base computation
   resolvable inside the engine runtime itself.
3. Add repeated-merge tests that call engine APIs directly, not only executor wrappers, so
   this dependency stays visible.

## Bottom Line

Repeated-merge correctness currently depends on information supplied from outside the engine's
own branch API. That is an architectural seam in the merge model.
