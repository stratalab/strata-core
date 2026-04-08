# Gap 03: Durable Encrypted Storage Is Blocked by WAL Codec Limitations

## Summary

The storage layer supports configurable codecs, including `aes-gcm-256`, but the durable
open path rejects non-identity codecs whenever WAL-based durability is enabled.

That means Strata currently has no durable encrypted mode in the core engine.

## What The Code Does Today

`Database::open_finish()` validates the configured codec and then explicitly rejects
non-identity codecs when the durability mode requires WAL.

The inline comment is unusually direct:

- `WalReader` parses raw bytes without codec decoding
- encrypted WAL records would be unreadable on restart
- supporting this requires WAL envelope and reader codec threading

Code evidence:

- `crates/engine/src/database/open.rs:696-715`

The test suite enforces that rejection. The engine test constructs an encrypted config with
standard durability and asserts that open fails with a WAL limitation message.

Code evidence:

- `crates/engine/src/database/tests.rs:2457-2484`

## Why This Is An Architectural Gap

The storage architecture is presenting codec selection as a persistence concern, but the WAL
path is still effectively hardwired to plaintext decode semantics.

This creates a broken capability matrix:

- encrypted snapshots/segments as a storage concept: supported
- durable restart through WAL replay: required for Standard and Always modes
- encrypted WAL replay: unsupported

So the engine exposes encryption and durability as separate knobs, but the implementation
cannot compose them.

## Practical Impact

### No durable encrypted deployment mode

Today the valid combinations are effectively:

- `identity` codec + WAL durability: allowed
- encrypted codec + `cache` durability: allowed
- encrypted codec + `standard` or `always`: rejected

That rules out the common "encrypted at rest and crash recoverable" deployment mode.

### Recovery semantics are tied to a specific serialization assumption

The WAL reader is not codec-aware, which means the recovery architecture is coupled to the
plaintext WAL format in a way the higher-level configuration does not reveal.

### Checkpoint work will eventually inherit the same concern

Inference: once checkpoint-based recovery is wired in, the system will need a coherent
policy for codec handling across:

- WAL replay
- segment reads
- checkpoint reads

Right now those responsibilities are not unified.

## Why The Current Rejection Is Correct

This guard is not overly conservative. It is preventing silent data loss.

Without the rejection:

1. the engine could write encrypted durable state
2. the WAL would be required for restart
3. the reader would fail to decode it
4. restart would either fail or recover incomplete state

Blocking the configuration is therefore the correct short-term safety behavior.

## Likely Remediation Direction

Inference from the inline comment and open path:

1. Define a WAL envelope that can identify codec and decode boundaries safely.
2. Load the codec early enough in open to decode WAL before recovery replay.
3. Ensure codec identity is anchored to `MANIFEST` and validated before replay.
4. Add migration and compatibility handling for older plaintext WAL segments.
5. Test crash recovery across:
   - encrypted Standard durability
   - encrypted Always durability
   - WAL rotation
   - partial-record truncation

## Bottom Line

Encryption exists as a storage feature, but WAL replay still assumes plaintext. Until WAL
recovery becomes codec-aware, encrypted durability remains architecturally unavailable.
