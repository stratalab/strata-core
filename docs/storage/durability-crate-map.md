# Durability Crate Map

## Status

`strata-durability` has been deleted.

The canonical durability runtime now lives entirely in
`strata_storage::durability`, including:

- WAL
- snapshots
- MANIFEST/layout/codec
- compaction
- recovery
- WAL payload and durable commit adaptation

## Historical Note

Before `ST5`, `strata-durability` survived only as the temporary host of the
branch-oriented bundle workflow.

That workflow now lives under:

- `crates/engine/src/bundle/mod.rs`
- `crates/engine/src/bundle/error.rs`
- `crates/engine/src/bundle/types.rs`
- `crates/engine/src/bundle/wal_log.rs`
- `crates/engine/src/bundle/reader.rs`
- `crates/engine/src/bundle/writer.rs`

## Architectural Meaning

The old split is gone:

- lower durability/runtime ownership is in `storage`
- branch archive/import-export ownership is in `engine`
- there is no remaining reason for a separate durability peer crate
