# Gap 05: Plain `Database::open` Does Not Recover the Full Primitive Stack

## Summary

The default engine entrypoints do not recover the same subsystem set that production uses.
`Database::open` and `Database::open_follower` hardcode only `SearchSubsystem`.

Vector recovery, graph subsystem recovery, and branch-DAG initialization are composition
choices above those default entrypoints.

## What The Code Does Today

### Default primary open hardcodes search only

`open_internal()` is the convenience path used by `Database::open` and
`open_with_config()`. It delegates to the subsystem-aware open path with
`vec![SearchSubsystem]`.

Code evidence:

- `crates/engine/src/database/open.rs:287-304`

### Default follower open does the same

`open_follower_internal()` also hardcodes only `SearchSubsystem`.

Code evidence:

- `crates/engine/src/database/open.rs:535-549`

### Vector recovery is builder-only

The vector crate explicitly documents that vector state recovery requires
`DatabaseBuilder::with_subsystem(VectorSubsystem)`.

Code evidence:

- `crates/vector/src/lib.rs:16-20`
- `crates/engine/src/database/builder.rs:59-89`

### Graph has a subsystem, but default open does not install it

`GraphSubsystem` exists and its recovery method initializes the `_system_` branch and loads
branch-status state, but default open does not include it.

Code evidence:

- `crates/graph/src/branch_dag.rs:842-862`

### Production opens use a different composition

The executor builds databases with:

- `VectorSubsystem`
- `SearchSubsystem`

and then separately calls `init_system_branch(&db)`.

Code evidence:

- `crates/executor/src/api/mod.rs:101-112`
- `crates/executor/src/api/mod.rs:216-224`
- `crates/executor/src/api/mod.rs:335-342`

## Why This Is An Architectural Gap

The same on-disk database can come up with different runtime capabilities depending on which
entrypoint the caller used:

- plain engine open
- builder with explicit subsystems
- executor wrapper

That means "open the database" is not one architectural operation. It is several distinct
composition modes with different recovery surfaces.

## Practical Consequences

### Production and engine-default behavior diverge

A caller who uses `Database::open` directly does not get the same recovered runtime that the
production wrapper expects.

### Follower behavior diverges too

Because follower open also hardcodes search only, a follower opened through the plain engine
API will not automatically recover vector state or graph-related subsystem state the way a
builder-based open can.

### There is no single authoritative "core runtime"

Subsystem recovery is real and well-factored, but the default API does not represent the
full primitive stack. That leaves the engine without a single default runtime composition.

## Why This Is Separate From The Registration Gap

Gap 04 is about semantic handlers that require global registration.

This gap is different:

- even if handlers are registered correctly
- plain `Database::open` still does not install the full subsystem list

So there are two independent composition seams:

1. which subsystems were attached to this database instance
2. which process-global handlers were registered for the process

## Likely Remediation Direction

Inference from the current code shape:

1. Decide whether `Database::open` should remain minimal or become a true standard runtime.
2. If minimal open remains, document it as such and make production composition the clearly
   named default constructor elsewhere.
3. If the goal is a single standard runtime, move more composition into a shared builder path
   and make plain open route through it.
4. Align follower open with the same composition story as primary open.

## Bottom Line

The engine has a subsystem model, but its default entrypoints do not represent the full
runtime used in production. Recovery behavior is therefore entrypoint-dependent.
