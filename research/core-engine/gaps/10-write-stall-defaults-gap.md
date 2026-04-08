# Gap 10: L0 Write Stalling Exists in Code, but Is Disabled by Default

## Summary

The engine has a real RocksDB-style backpressure path for L0 slowdown and stop-writes. But
the default configuration sets the key L0 thresholds to zero because the current interaction
with rate-limited compaction can deadlock.

So the protection mechanism exists, yet the default production posture avoids using it.

## What The Code Does Today

`maybe_apply_write_backpressure()` implements:

- synchronous flush of frozen memtables when L0 thresholds are active
- L0 stop-writes waiting on a condition variable
- L0 slowdown via small sleeps
- memtable pressure sleeps
- segment metadata pressure sleeps

Code evidence:

- `crates/engine/src/database/transaction.rs:346-442`

The default config disables the two L0 thresholds:

- `l0_slowdown_writes_trigger = 0`
- `l0_stop_writes_trigger = 0`

The inline reason is explicit: enabling them with rate-limited compaction creates a deadlock.

Code evidence:

- `crates/engine/src/database/config.rs:167-176`

## Why This Is An Architectural Gap

The config surface advertises classic LSM backpressure controls, but the default runtime does
not rely on those controls because the compaction interaction is not yet safe.

That means the system is currently balancing write pressure through a weaker set of tools:

- flush scheduling
- background compaction keeping up
- soft sleeps on memory pressure
- soft sleeps on metadata pressure

instead of the main L0 guardrail that the storage design implies.

## Practical Consequences

### The strongest read-latency protection is opt-in, not default

L0 explosion is one of the canonical failure modes in leveled LSM systems because it hurts
read amplification and overlap. The engine has code to respond to that, but operators only
get it after tuning thresholds manually.

### The config surface is more mature than the default runtime behavior

The existence of stall thresholds suggests the engine has solved the backpressure loop. The
comments show that it has not fully solved the interaction with compaction throttling.

### The deadlock concern is architectural, not cosmetic

The comment is not about a minor performance edge case. It says writes can stall waiting for
L0 drain while compaction is itself throttled too hard to drain L0.

That is a control-loop problem between:

- foreground admission
- background compaction rate limiting

## Likely Remediation Direction

Inference from the config comment and backpressure path:

1. Make the rate limiter aware of write-stall state so compaction can run at full speed while
   the system is protecting itself.
2. Re-enable safe nonzero defaults once the deadlock path is eliminated.
3. Add metrics around:
   - L0 segment count
   - stall duration
   - slowdown hits
   - compaction debt
4. Exercise sustained-ingest tests with rate limiting enabled, because that is where the
   control-loop bug lives.

## Bottom Line

The write-stall implementation is present, but the default architecture does not trust it yet.
That leaves a meaningful gap between the intended LSM control loop and the default runtime.
