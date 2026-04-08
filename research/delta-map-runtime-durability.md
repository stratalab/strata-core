# Delta Map: Runtime Spine And Durability

This is the first filled delta map against the target architecture.

Scope:

- runtime composition
- open/recover lifecycle
- durability ownership
- checkpoint/recovery integration
- lifecycle closure
- follower replication semantics

The goal is not to restate the audit.
It is to translate the audit into engineering deltas:

- what the current authoritative path is
- what shadow or competing paths still exist
- what the target design should be
- what to do about the gap
- how to know the gap is actually fixed

## Target Architecture Reference

This delta map assumes the target defined in:

- [target-architecture-program.md](/home/anibjoshi/Documents/GitHub/strata-core/research/target-architecture-program.md)

The most important target assumptions for this map are:

- one engine-owned runtime spine
- one durability model
- one recovery planner
- one explicit mode matrix
- one authoritative lifecycle state machine
- no shadow durability stacks

## Gap Table

| Domain | Current authoritative path | Shadow / competing path | Target truth | Gap type | Severity | Disposition | Acceptance test |
|---|---|---|---|---|---|---|---|
| Product open authority | `Strata::open_with` in `executor` decides local primary vs follower vs IPC and adds product bootstrap behavior | raw `Database::open*` and builder opens produce materially different products | product wrappers must delegate into one engine-owned runtime constructor with explicit mode + subsystem inputs | authority_split | high | consolidate | primary/follower/cache opens produce the same engine-owned runtime semantics regardless of caller layer |
| Engine open authority | `engine::database::open` constructs the runtime after wrapper policy decisions | raw convenience opens hardcode weaker subsystem sets and can win first-opener race | engine exposes one authoritative `open_runtime(mode, config, subsystems)` path; convenience APIs are thin aliases | authority_split | high | consolidate | `Database::open`, builder open, and executor open all resolve to identical runtime composition for the same inputs |
| Same-path instance reuse | `OPEN_DATABASES` shares primary instances by path | first opener permanently determines composition for that path | instance reuse must validate semantic equivalence or reject incompatible reopen attempts | authority_split | high | rewrite_boundary | opening the same path with incompatible subsystem/mode/config surfaces a deterministic error instead of silently reusing a mismatched instance |
| Runtime mode model | primary, follower, cache are implemented as different open families | read-only primary is executor policy, not a true engine mode; IPC is another product-level shape | one explicit runtime mode matrix owned by engine: `Primary`, `Follower`, `Cache`; wrapper access policy cannot redefine core semantics | semantic_inconsistency | high | consolidate | mode behavior is defined once in engine docs/tests and wrappers only pass `DatabaseMode` |
| Subsystem composition | builder subsystem list drives recover/freeze | merge handlers, DAG hooks, refresh hooks, and some product bootstrap live outside subsystem list | one instance-local subsystem contract for init/recover/refresh/freeze/shutdown/persistence role | authority_split | high | rewrite_boundary | no semantic behavior depends on process-global registration to exist on a correctly opened database |
| Durability runtime ownership | live engine path uses `engine::database::open` + concurrency recovery + durability WAL writer | `durability::database::handle` is a second full persistence runtime | one durability runtime integrated into engine and used by the shipped product | zombie_stack | critical | delete_or_promote | either `durability::database` is deleted, or engine open is reimplemented through it and the old path removed |
| Recovery planner | live open is WAL replay plus segment reattach | `durability::recovery::coordinator` implements a richer `MANIFEST + snapshot + WAL` planner | one recovery planner, one filesystem layout, one recovery story | zombie_stack | critical | delete_or_promote | there is exactly one recovery coordinator used by open-path tests and product runtime |
| Filesystem layout | live engine uses lowercase `wal/`, `segments/`, `snapshots/` | durability runtime expects uppercase `WAL/`, `SNAPSHOTS/`, `DATA/` layout family | one canonical on-disk layout | semantic_inconsistency | high | consolidate | layout assumptions in engine, durability, tests, and docs all match one directory schema |
| Checkpoint role | checkpoints are written by engine and recorded in `MANIFEST` | open-path recovery does not consume them | checkpoints are either first-class in recovery or explicitly removed from the claimed architecture | authority_split | critical | consolidate | recovery tests demonstrate checkpoint restore followed by delta WAL replay on the shipped open path |
| WAL deletion policy | WAL truncation is guarded by flush watermark because recovery is WAL-centric | checkpoint artifacts suggest snapshot coverage should matter, but open does not trust them | WAL retention policy matches the actual recovery chain | control_plane_drift | high | consolidate | WAL compaction logic, recovery planner, and checkpoint docs describe the same source of truth |
| WAL codec support | `WalWriter` encodes through codec; live engine blocks non-identity WAL durability | `WalReader` parses raw bytes directly | codec handling is uniform across WAL write, WAL read, and recovery | authority_split | critical | consolidate | a durable encrypted database can be opened, written, restarted, and recovered with no special-case rejection |
| Snapshot codec support | snapshot header records codec ID; checkpoint serializer uses identity; section payloads remain plaintext | docs and codec presence imply broader encryption-at-rest support | snapshot payload encoding/decoding obeys the same codec contract as the rest of durability | security_drift | high | consolidate | checkpoint bytes round-trip under configured codec and plaintext exception list is reduced to explicitly approved metadata only |
| Manifest authority | `MANIFEST` exists and is consulted for codec + watermarks | durable runtime and live engine do not fully agree on what it governs | `MANIFEST` is the single physical metadata source for recovery planning | authority_split | medium | promote | open-path recovery, WAL truncation, checkpoint restore, and codec validation all read the same manifest contract |
| Lifecycle close semantics | `shutdown()` is partial graceful stop; `Drop` still performs essential cleanup | registry cleanup, fallback freeze, and final teardown still depend on destructor path | `shutdown()` is the authoritative close path; `Drop` is best-effort cleanup only | lifecycle_leak | critical | rewrite_boundary | explicit shutdown fully closes runtime, frees lock/registry ownership, and makes subsequent reopen deterministic without relying on destructor side effects |
| Background quiescence | scheduler drain, flush thread stop, tx idle wait, and freeze are split across shutdown/drop | background work can outlive early shutdown phases | engine owns a single close state machine with ordered quiescence | lifecycle_leak | high | consolidate | shutdown tests prove no background flush/compaction/freeze work can be enqueued or continue after close barrier |
| Runtime config truth | some runtime knobs are real, others drifted or are stub-backed | mode switches and maintenance surfaces overstate live behavior | every exposed runtime/durability knob maps to one tested behavior classification | control_plane_drift | high | consolidate | config truth table is complete and every key is tagged `open-time`, `live-safe`, or `unsupported` |
| Follower refresh semantics | `Database::refresh()` is engine-owned and WAL-tail based | skipped records still allow watermark advancement; strict/lossy policy differs from open | follower refresh must be contiguous, convergent, and mode-consistent | lifecycle_leak | critical | rewrite_boundary | refresh tests prove skipped/corrupt records are retried or cause hard failure, never silently disappear behind watermark |
| Follower subsystem behavior | search is refreshed directly, vector uses hook path, others rely on storage truth | refresh ownership is inconsistent across primitives | follower mode uses one declared subsystem refresh contract | authority_split | high | consolidate | each subsystem declares whether it participates in follower refresh and tests cover all installed subsystems uniformly |
| IPC fallback semantics | `Strata::open_with` may return an IPC client instead of a local runtime | product backend type changes after lock error and stale-socket retry path skips bootstrap work | IPC is a separate transport over the same canonical runtime, not a different composition story | semantic_inconsistency | medium | rewrite_boundary | IPC fallback and local open expose the same bootstrap invariants and capability surface |

## Priority Clusters

The table is long, but the work collapses into a small number of real clusters.

## Cluster A: One runtime constructor

This cluster fixes:

- product open authority
- engine open authority
- same-path reuse
- runtime mode model
- IPC fallback semantics

This is the first cluster because every other repair is harder while open-path authority is still split.

### Concrete target

Build one engine-owned constructor with inputs:

- runtime mode
- runtime config
- subsystem set
- bootstrap policy flags only if they are truly semantic

Then make:

- raw engine open
- builder open
- executor open
- follower open
- cache open

all delegate into it.

## Cluster B: One durability/recovery architecture

This cluster fixes:

- durability runtime ownership
- recovery planner duplication
- layout mismatch
- checkpoint role
- WAL deletion truth
- manifest authority
- WAL codec support
- snapshot codec support

This is the most important cluster after runtime authority.

### Concrete target

Choose one path:

- either promote the richer durability stack into the live engine runtime
- or fold its good ideas into engine/concurrency recovery and delete the old stack

But do not keep both.

## Cluster C: Real lifecycle closure

This cluster fixes:

- shutdown/drop split
- background quiescence
- reopen determinism

### Concrete target

Model engine close as a real state machine and move all essential cleanup into explicit shutdown.

## Cluster D: Real follower semantics

This cluster fixes:

- refresh convergence
- follower mode policy consistency
- subsystem refresh declaration

### Concrete target

Treat follower refresh as replication, not a best-effort helper loop.

## Recommended Execution Order

1. Cluster A: runtime constructor and mode unification
2. Cluster B: durability/recovery unification
3. Cluster C: lifecycle closure
4. Cluster D: follower semantics

That order matters.

If we try to fix checkpoints, codecs, or follower refresh before runtime authority is unified, we will probably fix the wrong path or leave a second path behind.

## Recommended Dispositions

These are the blunt calls I would make now.

### Promote or integrate

- the better parts of `durability::recovery`
- the better parts of `durability::database` if that stack is chosen as the base
- `MANIFEST` as the authoritative physical metadata anchor

### Delete

- any durability runtime path not chosen as the canonical one
- any recovery planner not chosen as the canonical one
- special-case wrapper semantics that bypass the canonical runtime once the new spine exists

### Rewrite boundary

- `OPEN_DATABASES` semantic reuse rules
- shutdown/drop ownership
- follower watermark advancement semantics
- subsystem refresh/merge registration model

## Acceptance-Test Program

These are the tests that should exist before this workstream is considered complete.

### Runtime equivalence tests

- same config through raw engine open, builder open, and executor open yields equivalent runtime composition
- incompatible same-path reopen is rejected, not silently reused
- primary, follower, and cache modes are explicit and validated

### Durability and recovery tests

- checkpoint restore is exercised on the shipped open path
- encrypted durable restart round-trips if encryption is supported
- if encryption is not supported, unsupported combinations fail at open time with one canonical message
- one on-disk layout is used across engine and durability tests

### Lifecycle tests

- explicit shutdown fully closes the instance and releases path ownership
- no essential cleanup is left solely to `Drop`
- no background task survives the close barrier

### Follower tests

- corrupt or skipped WAL record cannot be hidden behind watermark advancement
- follower strict/lossy policy matches configured mode
- installed subsystems have declared, tested refresh behavior

## Bottom Line

The runtime and durability delta is not "a lot of small bugs".
It is a small number of big architectural truths:

1. Strata still has multiple runtime authorities.
2. Strata still has multiple durability/recovery stories.
3. Close semantics are not yet engine-authoritative.
4. Follower mode is not yet replication-grade.

That is why this workstream should be treated as architecture consolidation, not cleanup.
