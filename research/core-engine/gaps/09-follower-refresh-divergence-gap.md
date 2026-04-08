# Gap 09: Follower Refresh Can Permanently Diverge

## Summary

Follower refresh is documented as best-effort. The deeper problem is that refresh can skip a
bad WAL record and still advance its watermark past that record. Once that happens, the
record will not be retried on the next refresh.

So the current follower model does not guarantee eventual convergence after partial refresh
failures.

## What The Code Does Today

`Database::refresh()`:

- reads all WAL records after the current watermark
- iterates record by record
- logs and skips payload decode failures
- logs and skips storage-apply failures
- updates search and refresh hooks for successful records
- advances local counters
- stores the final `max_txn_id` as the new watermark

Code evidence:

- `crates/engine/src/database/lifecycle.rs:110-355`

The crucial detail is ordering:

1. `max_txn_id` is advanced before payload decode succeeds
2. decode failure does `continue`
3. storage apply failure also does `continue`
4. after the loop, watermark is set to the final `max_txn_id`

Code evidence:

- `crates/engine/src/database/lifecycle.rs:161-175`
- `crates/engine/src/database/lifecycle.rs:198-212`
- `crates/engine/src/database/lifecycle.rs:346-352`

## Why This Is An Architectural Gap

"Best-effort refresh" could mean:

- some records fail now
- later refreshes retry and catch up

That is not the current design. The current design can permanently skip a record by moving
the follower watermark beyond it.

This changes the follower contract from "eventually convergent unless the WAL itself is gone"
to "convergent only if every record in each refresh window succeeds or fails before watermark
advance logic notices."

## Concrete Failure Modes

### Corrupt payload is skipped forever

If `TransactionPayload::from_bytes()` fails for a record, the loop logs a warning and moves
on. Because `max_txn_id` already advanced, the final watermark can exclude that record from
all future refresh attempts.

### Storage apply failure is skipped forever

If `apply_recovery_atomic()` fails for one record, the same permanent skip can happen.

### Local secondary state can become inconsistent with source history

Successful later records still update:

- KV state
- search index
- refresh hooks

So the follower can continue moving forward while missing an earlier committed transaction.

## Why This Matters More Than A Temporary Lag

Temporary lag is normal for followers.
Permanent hole-skipping is different:

- it weakens read correctness on the follower
- it makes debugging difficult because refresh "succeeds" partially
- it undermines the mental model that repeated refresh calls converge to the primary

## Likely Remediation Direction

Inference from the current code shape:

1. Advance the watermark only through the highest contiguous successfully applied record.
2. Treat decode/apply failures as a degraded replication state rather than as normal skip-and-
   continue progress.
3. Optionally keep a side queue of failed transaction IDs so operators can inspect and retry.
4. Add tests for:
   - corrupt payload in the middle of a refresh window
   - storage failure in the middle of a refresh window
   - later successful records after an early failure

## Bottom Line

Follower refresh is not just lossy under error; it can become permanently non-convergent.
That is a major architectural limitation for any read-replica story built on this path.
