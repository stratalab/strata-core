# Storage Fuzz Targets

This directory contains the storage cargo-fuzz package.

The first targets exercise the current durable byte decoders through the hidden
`testkit` surface. They are fail-closed parser fuzzers: arbitrary bytes may
decode or reject, but they must not panic, allocate without decoder limits, or
accept malformed checksums as valid. **Since TCP4.6b they also carry a
round-trip fidelity oracle**: when arbitrary bytes DO decode, the decoded value
is re-encoded and decoded again inside the `format::fuzzing` seam, and any
value change (or a decode failure of freshly-encoded bytes) panics — value
loss is a finding, not just a crash (the #2688/#2689 class at the codec
layer). Structural probes (`snapshot_envelope`, the table block/artifact
family, the WAL record envelope) stay fail-closed-only with their own oracles,
documented in the seam. Service targets route generated operation scripts
through hidden L4 testkit harnesses and assert model invariants after each
step.

Useful local commands:

```bash
cargo install cargo-fuzz --locked
cargo +nightly fuzz run format_manifest
cargo +nightly fuzz run format_branch_catalog_manifest
cargo +nightly fuzz run format_pending_releases_manifest
cargo +nightly fuzz run format_quarantine
cargo +nightly fuzz run format_retained_history_extension
cargo +nightly fuzz run format_snapshot_envelope
cargo +nightly fuzz run format_snapshot_row_payload
cargo +nightly fuzz run format_storage_row
cargo +nightly fuzz run format_table_artifact
cargo +nightly fuzz run format_table_block
cargo +nightly fuzz run format_table_manifest
cargo +nightly fuzz run table_runtime_reader
cargo +nightly fuzz run table_runtime_cursor
cargo +nightly fuzz run table_runtime_compaction
cargo +nightly fuzz run format_wal_commit_payload
cargo +nightly fuzz run format_wal_record
cargo +nightly fuzz run service_quarantine
cargo +nightly fuzz run service_snapshot
cargo +nightly fuzz run layout_object_name
cargo +nightly fuzz run layout_id_roundtrip
cargo +nightly fuzz run format_key
cargo +nightly fuzz run format_segment_metadata
cargo +nightly fuzz run format_snapshot_flushed_branches_payload
cargo +nightly fuzz run format_snapshot_timeline_payload
cargo +nightly fuzz run format_wal_segment_header
cargo +nightly fuzz run format_watermark
```

The last five (TCP4.6b) close the decoder-surface gap: every `FormatDecoder`
variant now has a dedicated byte-fuzz target (previously `Key`,
`SegmentMetadata`, `SnapshotTimelinePayload`, `WalSegmentHeader`, and
`Watermark` were reachable only through the testkit grid). The nightly workflow
enumerates targets with `cargo fuzz list`, so they join automatically.

L2 object-layout classification is fuzzed by `layout_object_name` (arbitrary
name text through every `classify_*` family — a malformed name read during a
recovery `list` must never panic) and `layout_id_roundtrip` (every canonical
WAL-segment / snapshot name must classify back to its exact u64 id). The
`layer_fuzz_presence_guard` test asserts every decoder layer keeps at least one
target, so this coverage cannot silently regress.

L5 table-runtime behavior is covered by the normal `table_runtime_properties`
proptest route, the `table_runtime_reader` arbitrary-byte open target, the
`table_runtime_cursor` generated operation-script target, the
`table_runtime_compaction` generated source/policy-script target, and the
`format_table_artifact` and `format_table_block` byte fuzzers.

The fuzz package uses `default-features = false` so parser fuzzing also covers
the memory/cache-compatible build surface. Format targets should stay named for
the byte-oriented durable input they fuzz, such as `object_name_parse`,
`format_table_block`, `format_timeline_row`, and `recovery_object_inventory`.
Service targets should stay named for the service family they script, such as
`service_snapshot` and `service_quarantine`.
