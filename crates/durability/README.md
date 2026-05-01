# strata-durability

`strata-durability` is now a temporary compatibility crate for the
branch-oriented `branch_bundle` workflow.

The canonical lower durability runtime lives in
`strata_storage::durability`:

- WAL
- snapshots
- MANIFEST/layout/codec
- compaction
- recovery

Only `branch_bundle` remains here until it is lifted into `strata-engine`.

## Current Surface

- `strata_durability::branch_bundle::*`

## Status

This crate is intentionally narrow and transitional. If you need storage
runtime durability APIs, import them from `strata_storage::durability::*`
instead.
