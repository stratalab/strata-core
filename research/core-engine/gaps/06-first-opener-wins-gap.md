# Gap 06: Subsystem Composition Is First-Opener-Wins for a Database Path

## Summary

Within a process, the first successful opener of a database path determines the live
`Database` instance for that path. Later opens return that same instance even if they ask for
different subsystems or a different config.

That makes runtime composition order-dependent.

## What The Code Does Today

### Opened databases are globally registered by canonical path

The engine maintains a global `OPEN_DATABASES` registry keyed by canonicalized path and
stores weak references to open instances.

Code evidence:

- `crates/engine/src/database/registry.rs:1-30`
- `crates/engine/src/database/open.rs:230-285`

### Existing instances are returned immediately

`acquire_primary_db()` checks the registry and returns the existing `Arc<Database>` if one is
already live for that path.

Code evidence:

- `crates/engine/src/database/open.rs:243-250`

### The subsystem-aware path explicitly preserves the earlier composition

`open_internal_with_subsystems()` says that if the instance is not fresh, an existing
database opened earlier, possibly with a different subsystem list, is returned unchanged.

Code evidence:

- `crates/engine/src/database/open.rs:318-329`

### Config can be written before deduplication happens

`open_with_config()` and `open_with_config_and_subsystems()` write `strata.toml` before they
route into the open logic that may return an already-live instance.

Code evidence:

- `crates/engine/src/database/open.rs:156-160`
- `crates/engine/src/database/open.rs:214-218`

## Why This Is An Architectural Gap

The registry behavior is understandable for singleton access to a database path. The gap is
that the registry is also the place where composition conflicts are silently resolved.

The result is:

- opener order controls subsystem set
- opener order controls recovery/freeze hooks
- later callers cannot tell whether their requested composition actually took effect

This is not just an optimization detail. It changes runtime behavior.

## Concrete Failure Modes

### Live runtime differs from later caller expectations

A test or embedding can open the database first with a minimal subsystem set.
Another caller later opens the same path expecting vector recovery or graph subsystem
behavior and quietly receives the earlier instance.

### Persisted config can drift from live runtime

Because config is written before the existing instance is returned, the on-disk `strata.toml`
for future restarts can change even though the current in-memory database continues to run
with the earlier open's composition and settings.

That creates a split between:

- what the current process is actually running
- what the next restart will try to run

## Why This Matters Operationally

The bug class here is nondeterminism by open order:

- tests become order-sensitive
- multi-layer applications can accidentally compose the runtime differently
- diagnosing "why doesn't this database have subsystem X?" gets harder because open itself
  does not fail

## Likely Remediation Direction

Inference from the current code shape:

1. Detect incompatible re-open requests and return an explicit error instead of silently
   reusing the earlier instance.
2. Move config writes after deduplication, or skip rewriting config when an existing live
   instance is returned.
3. Optionally support an explicit runtime-upgrade path, but only if it can safely install
   new subsystems into a live database.

## Bottom Line

The singleton-per-path registry is not the problem by itself. The problem is that it silently
turns composition conflicts into "reuse whatever the first opener created."
