# ST5 Branch Bundle Lift Plan

## Status

`ST5` is complete.

The branch-bundle workflow now lives entirely in `strata-engine`, and
`strata-durability` has been deleted from the workspace.

## Final Ownership

Bundle-format and archive ownership now lives under:

- `crates/engine/src/bundle/mod.rs`
- `crates/engine/src/bundle/error.rs`
- `crates/engine/src/bundle/types.rs`
- `crates/engine/src/bundle/wal_log.rs`
- `crates/engine/src/bundle/reader.rs`
- `crates/engine/src/bundle/writer.rs`

The engine-owned public surface remains:

- `strata_engine::bundle::export_branch(...)`
- `strata_engine::bundle::export_branch_with_options(...)`
- `strata_engine::bundle::import_branch(...)`
- `strata_engine::bundle::validate_bundle(...)`
- bundle DTOs and reader/writer helpers under `strata_engine::bundle::*`

## Landing Sequence

`ST5` landed in three cuts:

1. `ST5A` physically lifted the bundle-format implementation into `engine`.
2. `ST5B` cut over the remaining callers and tightened the import guard.
3. `ST5C` deleted `strata-durability` and removed the last manifest seam.

## Result

The crate graph is now simpler and more honest:

- `storage` owns the lower durability/runtime substrate
- `engine` owns the branch-domain bundle workflow
- there is no separate durability crate left to explain or maintain

## Done Criteria Closed

`ST5` closed the following items:

1. All bundle-format source files are physically defined in `engine`.
2. `strata_engine::bundle::*` is the only live bundle surface.
3. `crates/engine/Cargo.toml` no longer depends on `strata-durability`.
4. No active source file imports `strata_durability::branch_bundle::*`.
5. `tests/storage_surface_imports.rs` fully bans `strata-durability`.
6. `strata-durability` is deleted from the workspace.
