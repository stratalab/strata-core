# EG2 Implementation Plan

## Purpose

`EG2` absorbs the current `strata-security` crate into `strata-engine`.

This is intentionally a small early consolidation phase. The current security
crate is not an authorization system; it is a thin owner for database open
options, access-mode policy, and a redacted secret string wrapper. Those types
belong with engine-owned database open/configuration behavior so later product
open absorption does not carry an extra peer crate.

This is the single implementation plan for the `EG2` phase. Lettered sections
such as `EG2A`, `EG2B`, `EG2C`, and `EG2D` are tracked in this file rather than
in separate letter-specific documents.

Read this with:

- [engine-consolidation-plan.md](./engine-consolidation-plan.md)
- [engine-crate-map.md](./engine-crate-map.md)
- [eg1-implementation-plan.md](./eg1-implementation-plan.md)

## Scope

`EG2` owns:

- characterizing `AccessMode`, `OpenOptions`, and `SensitiveString`
- moving those types into `strata-engine`
- keeping executor-facing re-exports stable through `strata-executor`
- cutting normal `strata-security` dependencies from engine, executor, and
  executor-legacy
- retiring the `strata-security` workspace member unless a temporary shell is
  explicitly justified
- adding a guard that prevents new production `strata-security` imports

`EG2` does not own:

- redesigning authorization, users, roles, permissions, or ACLs
- merging `AccessMode` with `DatabaseMode`
- moving product bootstrap/default subsystem selection into engine
- removing direct storage bypasses from executor
- changing storage, WAL, manifest, checkpoint, snapshot, or recovery behavior
- changing the serialized `strata.toml` shape

## Boundary Decisions

`DatabaseMode` and `AccessMode` remain separate.

`DatabaseMode` is an engine runtime mode:

- `Primary` opens a writable persisted database
- `Follower` opens a read-only view of persisted state
- `Cache` opens an ephemeral writable database

`AccessMode` is product/session policy:

- `ReadWrite` permits executor/session write commands
- `ReadOnly` rejects executor/session write commands

The two overlap but are not the same abstraction. A read-only product handle can
point at a primary database. A follower open request forces the returned product
handle to `ReadOnly`, but `OpenOptions::follower(true)` itself remains only a
request flag.

`OpenOptions` belongs beside `OpenSpec` and product open helpers, not in
storage. It should not gain storage configuration fields.

`SensitiveString` belongs in engine because `StrataConfig` is engine-owned and
contains model/provider API keys. It must keep redacted formatting and
transparent serialization semantics.

## Pre-EG2 Baseline Surface

At the start of EG2, `crates/security/src/lib.rs` owned:

| Type | Current behavior |
| --- | --- |
| `AccessMode` | `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Default`, `Serialize`, `Deserialize`; default is `ReadWrite`; variants are `ReadWrite` and `ReadOnly`. |
| `OpenOptions` | `Debug`, `Clone`; public fields `access_mode` and `follower`; default is `ReadWrite` and `false`; builders are `new()`, `access_mode(...)`, and `follower(...)`. |
| `SensitiveString` | `Clone`, transparent `Serialize`/`Deserialize`, zeroize-on-drop, redacted `Debug` and `Display`, `Deref<Target = str>`, `as_str()`, `into_inner()`, `PartialEq`, `Eq`, `From<String>`, and `From<&str>`. |

Pre-EG2 normal dependencies on `strata-security`:

| Crate | Current use | Target |
| --- | --- | --- |
| `strata-engine` | `SensitiveString` in `database/config.rs` | Own `SensitiveString` directly. |
| `strata-executor` | `AccessMode`, `OpenOptions`, `SensitiveString` | Import from `strata-engine`; continue re-exporting `AccessMode` and `OpenOptions`. |
| `strata-executor-legacy` | `AccessMode`, `OpenOptions` | Import from `strata-engine`; remove its security dependency. |

At the start of EG2, the CLI imported `AccessMode` and `OpenOptions` through
`strata-executor`. That import path can remain unchanged during `EG2`.

## Target API

Engine should own the canonical types:

- `strata_engine::AccessMode`
- `strata_engine::OpenOptions`
- `strata_engine::SensitiveString`

Preferred module placement:

- `crates/engine/src/database/open_options.rs` owns `AccessMode` and
  `OpenOptions`
- `crates/engine/src/sensitive.rs` owns `SensitiveString`
- `crates/engine/src/database/mod.rs` re-exports `AccessMode` and `OpenOptions`
  beside `OpenSpec`
- `crates/engine/src/lib.rs` re-exports all three at the crate root

Executor may continue to expose:

- `strata_executor::AccessMode`
- `strata_executor::OpenOptions`

Those re-exports should point to engine-owned types, not to a compatibility
security crate.

## Section Status

| Section | Status | Deliverable |
| --- | --- | --- |
| `EG2A` | Complete | Characterization tests and inventory for the security/open surface. |
| `EG2B` | Complete | Engine-owned `AccessMode`, `OpenOptions`, and `SensitiveString`. |
| `EG2C` | Complete | Executor and executor-legacy cut over to engine-owned types. |
| `EG2D` | Complete | `strata-security` retired, with guard coverage. |

## Shared Commands

Current security dependency inventory:

```bash
rg -n "strata_security|strata-security" crates src tests Cargo.toml \
  -g '!target/**'
```

Current type-use inventory:

```bash
rg -n "\bAccessMode\b|\bOpenOptions\b|\bSensitiveString\b" \
  crates/engine crates/executor crates/executor-legacy crates/cli src tests \
  -g '!target/**'
```

Normal dependency checks:

```bash
cargo tree -p strata-engine --edges normal --depth 1
cargo tree -p strata-executor --edges normal --depth 1
cargo tree -p strata-executor-legacy --edges normal --depth 1
cargo metadata --format-version 1 --no-deps | rg '"name":"strata-security"|strata-security'
```

Focused verification commands:

```bash
cargo test -p strata-engine sensitive
cargo test -p strata-engine access_mode
cargo test -p strata-engine open_options
cargo test -p strata-executor
cargo test -p strata-executor-legacy
cargo test -p stratadb --test storage_surface_imports
cargo fmt --check
```

The exact focused test filters may change with file/module names. The
dependency and import guards should not.

## EG2A - Security Surface Characterization

### Goal

Pin the current behavior before moving code so the absorption is mechanical and
reviewable.

### Work

- Add characterization tests in engine or a temporary local test module for
  `AccessMode`:
  - default is `ReadWrite`
  - variants are `Copy`, comparable, and debug-printable
  - serde round-trips preserve `ReadWrite` and `ReadOnly`
- Add characterization tests for `OpenOptions`:
  - `OpenOptions::default()` and `OpenOptions::new()` produce `ReadWrite` and
    `follower == false`
  - `access_mode(AccessMode::ReadOnly)` changes only access mode
  - `follower(true)` changes only the follower flag
  - builder ordering does not create hidden coupling between follower and access
    mode
- Add characterization tests for `SensitiveString`:
  - `Debug` and `Display` are exactly `[REDACTED]`
  - `as_str()` and `Deref<Target = str>` expose the raw value by borrow
  - `into_inner()` returns the raw value
  - transparent serde writes and reads the raw string
  - equality compares raw values
  - `Clone` produces an independent value
  - the type is `Send + Sync`
- Do not migrate the current allocator-memory-after-drop test verbatim. Reading
  memory through a pointer after drop is not a sound characterization test.
  Keep the zeroize-on-drop implementation, but test stable public behavior.
- Record the call-site inventory in this plan if implementation discovers new
  normal uses not listed above.

### Acceptance

`EG2A` is complete when:

- the current public behavior of all three types is covered by focused tests
- tests do not depend on `strata-security` staying as the owner
- no code movement has happened beyond test scaffolding

Status: complete.

Implemented in:

- [../../tests/engine_security_surface.rs](../../tests/engine_security_surface.rs)

The tests characterize the canonical `strata_engine::AccessMode`,
`strata_engine::OpenOptions`, and `strata_engine::SensitiveString` exports, and
keep a small executor-facing re-export compatibility check. They deliberately
avoid direct `strata_security` imports so `EG2B` can move ownership to engine
without rewriting the behavioral contract.

## EG2B - Engine-Owned Security And Open Types

### Goal

Make `strata-engine` the canonical owner of the three types without changing
runtime behavior.

### Work

- Add `AccessMode` and `OpenOptions` to engine, preferably in
  `crates/engine/src/database/open_options.rs`.
- Add `SensitiveString` to engine, preferably in
  `crates/engine/src/sensitive.rs`.
- Re-export `AccessMode` and `OpenOptions` from `crate::database` beside
  `OpenSpec`.
- Re-export `AccessMode`, `OpenOptions`, and `SensitiveString` from the
  `strata_engine` crate root.
- Update `crates/engine/src/database/config.rs` to use the engine-owned
  `SensitiveString`.
- Add `zeroize` as an engine dependency. Prefer moving it into
  `[workspace.dependencies]` if more than one crate still uses it during the
  transition; otherwise a direct engine dependency is acceptable.
- Preserve serde names and TOML shape. API keys in `strata.toml` must still
  serialize as plain strings and deserialize into redacted runtime wrappers.
- Avoid broad doc rewrites. Only update examples/import paths that mention
  `strata_security`.

### Acceptance

`EG2B` is complete when:

- engine tests for the moved types pass
- `strata-engine` no longer imports `strata_security`
- `StrataConfig` serialization/deserialization behavior is unchanged
- `cargo test -p strata-engine sensitive`, `cargo test -p strata-engine
  access_mode`, and `cargo test -p strata-engine open_options` pass, or
  equivalent focused filters pass

Status: complete.

Implemented in:

- [../../crates/engine/src/database/open_options.rs](../../crates/engine/src/database/open_options.rs)
- [../../crates/engine/src/sensitive.rs](../../crates/engine/src/sensitive.rs)
- [../../crates/engine/src/database/config.rs](../../crates/engine/src/database/config.rs)
- [../../tests/engine_security_surface.rs](../../tests/engine_security_surface.rs)

`strata-engine` now owns and exports `AccessMode`, `OpenOptions`, and
`SensitiveString`. `StrataConfig` uses the engine-owned `SensitiveString`, and
the characterization test imports the canonical engine-root exports directly.

Because `StrataConfig`'s secret fields changed type, `EG2B` also moved the
executor config handlers that assign API keys to `strata_engine::SensitiveString`.
The remaining executor and executor-legacy `AccessMode`/`OpenOptions` import
cutover stays in `EG2C`.

## EG2C - Caller Cutover

### Goal

Make executor and executor-legacy consume engine-owned types.

### Work

- Update executor imports:
  - `crates/executor/src/bridge.rs`
  - `crates/executor/src/compat.rs`
  - `crates/executor/src/executor.rs`
  - `crates/executor/src/ipc/server.rs`
  - `crates/executor/src/lib.rs`
  - `crates/executor/src/session.rs`
- `crates/executor/src/handlers/config.rs` and
  `crates/executor/src/handlers/configure_model.rs` were already cut over in
  `EG2B` because `StrataConfig`'s secret fields changed to engine-owned
  `SensitiveString`.
- Update executor-legacy imports:
  - `crates/executor-legacy/src/bootstrap.rs`
  - `crates/executor-legacy/src/lib.rs`
- Keep `strata_executor::{AccessMode, OpenOptions}` available by re-exporting
  the engine-owned types from executor.
- Preserve current CLI imports through executor unless a direct engine import is
  clearly cleaner in the touched file.
- Remove normal `strata-security` dependencies from executor and
  executor-legacy manifests.
- Re-run the current security dependency inventory command and classify every
  remaining hit.

### Acceptance

`EG2C` is complete when:

- executor and executor-legacy compile without a normal `strata-security`
  dependency
- CLI code continues to compile without import churn
- existing read-only/follower behavior is unchanged
- `cargo test -p strata-executor` and `cargo test -p strata-executor-legacy`
  pass, or any failures are documented as pre-existing

Status: complete.

Implemented in:

- [../../crates/executor/src/lib.rs](../../crates/executor/src/lib.rs)
- [../../crates/executor/src/bridge.rs](../../crates/executor/src/bridge.rs)
- [../../crates/executor/src/compat.rs](../../crates/executor/src/compat.rs)
- [../../crates/executor/src/executor.rs](../../crates/executor/src/executor.rs)
- [../../crates/executor/src/ipc/server.rs](../../crates/executor/src/ipc/server.rs)
- [../../crates/executor/src/session.rs](../../crates/executor/src/session.rs)
- [../../crates/executor-legacy/src/bootstrap.rs](../../crates/executor-legacy/src/bootstrap.rs)
- [../../crates/executor-legacy/src/lib.rs](../../crates/executor-legacy/src/lib.rs)
- [../../crates/executor/Cargo.toml](../../crates/executor/Cargo.toml)
- [../../crates/executor-legacy/Cargo.toml](../../crates/executor-legacy/Cargo.toml)

Executor and executor-legacy now import `AccessMode` and `OpenOptions` from
`strata_engine`. The public `strata_executor::{AccessMode, OpenOptions}`
re-export remains available, but now points at the engine-owned types. The CLI
continues importing through executor unchanged.

After `EG2C`, `cargo tree -i strata-security --workspace --edges normal`
reported only the `strata-security` crate itself, with no normal workspace
dependents. `EG2D` owns deleting or reducing that crate and adding guard
coverage.

## EG2D - Security Crate Retirement And Guard

### Goal

Remove the obsolete security crate edge from the workspace and make regressions
obvious.

### Work

- Prefer deleting `crates/security` and removing it from workspace members once
  all normal callers use engine-owned types.
- If deletion is not practical in the same PR, reduce `strata-security` to a
  temporary re-export shell:
  - it may re-export only engine-owned `AccessMode`, `OpenOptions`, and
    `SensitiveString`
  - it must have no normal workspace dependents
  - it must be documented as an `EG9` deletion target
- Add or extend a guard test so production code cannot reintroduce
  `strata-security` as a dependency or import.
- The guard should scan normal production manifests and Rust source, not docs or
  archived plans.
- Update [engine-crate-map.md](./engine-crate-map.md) after the graph changes.
- Update [engine-consolidation-plan.md](./engine-consolidation-plan.md) status
  once `EG2` is complete.

### Acceptance

`EG2D` is complete when:

- workspace metadata has no `strata-security` package, or a retained temporary
  shell has no normal workspace dependents
- `rg -n "strata_security|strata-security" crates src tests Cargo.toml` has no
  production hits outside an explicitly documented temporary shell
- the guard fails on new production `strata-security` imports or manifest
  dependencies
- `cargo fmt --check` passes

Status: complete.

Implemented in:

- [../../Cargo.toml](../../Cargo.toml)
- [../../tests/storage_surface_imports.rs](../../tests/storage_surface_imports.rs)
- [./engine-crate-map.md](./engine-crate-map.md)
- [./engine-consolidation-plan.md](./engine-consolidation-plan.md)

`crates/security` was deleted instead of kept as a temporary shell. The
workspace no longer contains a `strata-security` package, and production
manifests/source are guarded against reintroducing the retired crate import or
dependency path.

## EG2 Closeout

`EG2` is complete when:

- `EG2A`, `EG2B`, `EG2C`, and `EG2D` are complete
- `strata-engine` owns `AccessMode`, `OpenOptions`, and `SensitiveString`
- executor-facing re-exports point to engine-owned types
- no normal production crate depends on `strata-security`
- crate graph docs reflect the new graph
- the regression guard is in place

Status: complete.

The next implementation phase is `EG3`.
