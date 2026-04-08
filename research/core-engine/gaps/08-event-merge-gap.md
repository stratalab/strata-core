# Gap 08: Event Logs Still Do Not Have a Semantic Branch Merge

## Summary

Strata protects event-log correctness by refusing a class of branch merges rather than by
providing an event-aware merge algorithm. If both source and target appended to the same
event space since the fork, the merge is rejected.

This is a safe choice, but it is still a major capability gap in the primitive-aware branch
model.

## What The Code Does Today

The event-merge section in `branch_ops` explains why generic three-way merge is unsafe for
event logs:

- event rows are not just opaque KV
- they encode a hash chain and per-space metadata
- generic merge can silently corrupt that structure

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:172-186`

The concrete guard is `check_event_merge_divergence()`:

- it extracts ancestor, source, and target `EventLogMeta`
- compares `next_sequence` and `head_hash`
- rejects the merge if both sides advanced beyond the ancestor in the same space

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:245-296`

The error text is explicit that concurrent appends are "not yet supported."

Code evidence:

- `crates/engine/src/branch_ops/mod.rs:286-292`

## Why This Is An Architectural Gap

Strata's branching model is stronger for some primitives than for others:

- graph has a semantic merge path
- vector has primitive-specific precheck and rebuild behavior
- events still fall back to refusal for a common concurrent-edit case

That means the branch system is not yet uniformly primitive-aware.

## Concrete Impact

### Event-sourced workflows hit a hard stop under concurrent branch evolution

If two branches both append events to the same logical stream and later need to merge, the
engine cannot reconcile them.

### Users must handle the merge outside the engine

The current safety model effectively requires one of:

- serialized merges
- manual replay onto one side
- application-level reconciliation

### The branch model is asymmetric across primitives

The engine can say "merge unsupported" for an event space that looks structurally similar to
other branchable data from the user's perspective. That matters because the public branch
surface is global, but the true merge capability is primitive-specific.

## Why Refusal Is Still The Correct Current Behavior

The refusal is better than generic KV merge. The comments explain the corruption modes the
guard is preventing.

So the gap is not "the engine should have merged anyway."
The gap is "the engine does not yet have a safe event merge algorithm."

## Likely Remediation Direction

Inference from the current code:

1. Introduce an event-aware merge strategy that can:
   - preserve logical append order
   - rebuild `next_sequence`
   - recompute hash-chain heads
   - update any secondary event indexes coherently
2. If a true automatic merge is not acceptable, expose an explicit event rebase or replay
   workflow as a first-class branch operation instead of forcing callers into ad hoc repair.
3. Add tests for per-space behavior so independent event spaces can keep merging safely while
   same-space concurrent append semantics are upgraded.

## Bottom Line

Event logs are branch-safe only in the limited sense that the engine refuses dangerous merges.
They are not yet branch-merge capable in the same semantic sense as the richer primitive
paths.
