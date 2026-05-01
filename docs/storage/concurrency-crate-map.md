# Concurrency Crate Map

## Status

`strata-concurrency` was deleted in `ST4D`.

This document remains only as a historical marker in the storage
consolidation workstream. The generic transaction runtime, durability
commit adapter, and recovery coordinator all now live in `strata-storage`.

## Final Takeaway

The crate never represented a stable architectural layer. It existed as a
transitional split while:

- generic transaction state and OCC moved into `storage`
- WAL payload and commit adaptation moved into `storage`
- recovery moved into `storage`

After those moves, no honest standalone responsibility remained, so the
crate was removed.
