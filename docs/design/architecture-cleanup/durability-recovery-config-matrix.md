# Durability & Recovery Config Truth Table

**Status:** current state as of T3-E7 (2026-04-16).
**Normative source:** `durability-recovery-scope.md` §D-DR-11.
**Regression test:** `crates/engine/tests/config_matrix.rs` (parses this file and
asserts every `StrataConfig` / `StorageConfig` field is classified exactly once).

---

## Purpose

Classifies every public field on `StrataConfig` and its nested `StorageConfig`.
The classification is load-bearing in three places:

1. **Open-time reuse.** `CompatibilitySignature` rejects a second opener whose
   open-time fields disagree with the live database. An open-time-only field
   that is not represented in the signature is a silent-drift bug.
2. **Live config mutation.** `Database::update_config` and `set_durability_mode`
   may only mutate fields classified as live-safe. The executor-side handler
   at `crates/executor/src/handlers/config.rs` rejects runtime writes to fields
   in the `OPEN_TIME_ONLY_KEYS` list.
3. **Dead-knob policy.** A field that the implementation has silently stopped
   using is deleted, not documented here. Each field listed below is reachable
   from a runtime code path or contributes to the compatibility signature.

## Classes

| Class | Meaning | Must have |
|---|---|---|
| **open-time-only** | Value is locked at open. A second opener with a different value gets `StrataError::IncompatibleReuse`. | Participation in `CompatibilitySignature` — either as an explicit field checked in `check_compatible` or hashed into `open_config_fingerprint` (typically both, for diagnosability). Runtime-mutation attempts must also be rejected via `OPEN_TIME_ONLY_KEYS` in the executor config handler. |
| **live-safe** | May change at runtime via an explicit setter or `update_config`; takes effect without reopen. | An explicit setter, or read-through from `Database::config` on every hot-path call. |
| **unsupported/deferred** | Present on the struct but not consumed. Documented with a target-state pointer. Zero such fields today. | A pointer to the target-state requirement that would implement it. |
| **non-durability** | Out of the durability/recovery domain. Classified here only so the regression test can confirm full coverage of the public struct. | Nothing — listed for completeness. |

---

## `StrataConfig` — top-level

| Field | Class | Signature? | Setter / enforcement |
|---|---|---|---|
| `durability` | live-safe | yes (`durability_mode` in signature; `Cache` discriminant treated as open-time-only) | `Database::set_durability_mode` (Standard↔Always only; Cache rejected at runtime) |
| `auto_embed` | non-durability | no | `Database::set_auto_embed` |
| `model` | non-durability | no | — |
| `embed_batch_size` | non-durability | no | — |
| `embed_model` | non-durability | no | — |
| `provider` | non-durability | no | — |
| `default_model` | non-durability | no | — |
| `anthropic_api_key` | non-durability | no | — |
| `openai_api_key` | non-durability | no | — |
| `google_api_key` | non-durability | no | — |
| `storage` | (nested — see below) | — | — |
| `allow_lossy_recovery` | open-time-only | yes (`CompatibilitySignature.allow_lossy_recovery`; also hashed into `open_config_fingerprint`) | rejected at runtime via `OPEN_TIME_ONLY_KEYS` |
| `telemetry` | non-durability | no | — |
| `default_vector_dtype` | non-durability | no | — |

## `StorageConfig` — nested under `storage`

| Field | Class | Signature? | Setter / enforcement |
|---|---|---|---|
| `memory_budget` | live-safe | no | `apply_storage_config_inner` (derives effective cache/buffer on change) |
| `max_branches` | live-safe | no | `Storage::set_max_branches` |
| `max_write_buffer_entries` | live-safe | no | `Coordinator::set_max_write_buffer_entries` |
| `max_versions_per_key` | live-safe | no | `Storage::set_max_versions_per_key` |
| `block_cache_size` | live-safe | no | `block_cache::set_global_capacity` |
| `write_buffer_size` | live-safe | no | `Storage::set_write_buffer_size` |
| `max_immutable_memtables` | live-safe | no | `Storage::set_max_immutable_memtables` |
| `l0_slowdown_writes_trigger` | live-safe | no | read per-write in `maybe_apply_write_backpressure` |
| `l0_stop_writes_trigger` | live-safe | no | read per-write in `maybe_apply_write_backpressure` |
| `background_threads` | open-time-only | yes (`CompatibilitySignature.background_threads`; also hashed into `open_config_fingerprint`) | thread pool spawned at open with no resize path; rejected at runtime via `OPEN_TIME_ONLY_KEYS` |
| `target_file_size` | live-safe | no | `Storage::set_target_file_size` |
| `level_base_bytes` | live-safe | no | `Storage::set_level_base_bytes` |
| `data_block_size` | live-safe | no | `Storage::set_data_block_size` |
| `bloom_bits_per_key` | live-safe | no | `Storage::set_bloom_bits_per_key` |
| `compaction_rate_limit` | live-safe | no | `Storage::set_compaction_rate_limit` |
| `write_stall_timeout_ms` | live-safe | no | read per-stall in `maybe_apply_write_backpressure` |
| `codec` | open-time-only | yes (`codec_id` + `codec_name` in signature) | MANIFEST validates on open (primary: `open.rs`; follower: `open.rs`); `plan_recovery` re-validates before WAL read; mismatch at reuse returns `StrataError::IncompatibleReuse` |

---

## Enforcement

- Registry reuse with a mismatched codec returns `StrataError::IncompatibleReuse`
  on the signature check (primary) or on the MANIFEST-load check (follower,
  before the signature is even published). See
  `crates/engine/src/database/tests/codec.rs`.
- Attempting to mutate an `open-time-only` field at runtime through the
  executor config handler returns `Error::InvalidInput` with a message naming
  the offending key.
- `write_stall_timeout_ms` is covered by backpressure regression tests at
  `crates/engine/src/database/tests/regressions.rs` — confirms the knob is
  read on every stalled write, not dead code.

## Durability/recovery coverage

**Open-time-only:** `codec`, `background_threads`, `allow_lossy_recovery`,
`durability = "cache"` (discriminant).

**Live-safe (durability/recovery domain):** `durability` (Standard↔Always
switch). All other `StorageConfig` entries are live-safe but LSM-tuning, not
durability/recovery per se — they are listed here because they share the
struct and the regression test iterates the whole thing.

**Unsupported/deferred:** none today.

## Target-state notes

- **Non-identity-codec persistence.** The WAL reader does not yet decode via
  the configured codec (`open.rs` blocks `codec != "identity"` combined with
  any WAL-based durability mode). Cache-durability databases may use a
  non-identity codec within a single session but cannot survive restart.
  Lifting this is tracked separately from T3-E7 and will be reflected here
  when the block is removed. Until then, a write → crash → reopen → read
  round-trip with a non-identity codec is not testable.

## Change log

- 2026-04-16 (T3-E7): initial creation. Classifies all 14 top-level
  `StrataConfig` fields and all 17 nested `StorageConfig` fields. No fields
  deleted — every knob reachable from a runtime code path.
- 2026-04-16 (T3-E7 review follow-up): `background_threads` and
  `allow_lossy_recovery` moved into `CompatibilitySignature` as explicit
  fields so registry reuse rejects drift instead of silently serving an
  instance sized/recovered differently. Rule statement tightened: an
  open-time-only knob *must* participate in the signature; `OPEN_TIME_ONLY_KEYS`
  alone is no longer considered sufficient.
