# Runtime Consolidation Backlog

This backlog translates Phases 0-3 of the remediation program into concrete execution work.

Scope:

- Phase 0: spec lock
- Phase 1: runtime spine consolidation
- Phase 2: durability and recovery unification
- Phase 3: lifecycle and replication repair

It is intentionally biased toward:

- deleting split authority
- choosing canonical paths
- making runtime semantics explicit
- forcing tests to define done

It is not a general bug backlog.

## Program Goal

Create one canonical Strata runtime that has:

- one open path
- one mode model
- one durability model
- one recovery planner
- one explicit shutdown model
- one convergent follower refresh model

## Freeze Policy

While this backlog is active:

- no new runtime modes
- no new persistence paths
- no new background services
- no new checkpoint or replication features
- no new wrapper-only product semantics

Allowed:

- deleting shadow stacks
- refactoring toward canonical runtime authority
- tests
- docs
- bug fixes inside chosen canonical paths

## Blocking Decisions

These must be resolved before implementation gets deep into Phase 2.

### D1. Canonical durability base

Choose one:

- `PromoteDurabilityRuntime`
- `IntegrateDurabilityIdeasIntoEngine`

Recommendation:

- `IntegrateDurabilityIdeasIntoEngine`

Reason:

- lower migration risk
- fewer product-surface surprises
- preserves current engine ownership while deleting alternate runtime

### D2. Checkpoint role

Choose one:

- `CheckpointIsAuthoritativeRecoveryInput`
- `CheckpointIsAuxiliaryArtifactOnly`

Recommendation:

- `CheckpointIsAuthoritativeRecoveryInput`

Reason:

- current surface already presents checkpoints as real durability artifacts
- continuing to write them without using them is architectural drift

### D3. Durable encryption stance

Choose one:

- `MakeDurableEncryptionRealNow`
- `DropDurableEncryptionClaimForNow`

Recommendation:

- `DropDurableEncryptionClaimForNow` unless team explicitly funds the full WAL + checkpoint + recovery codec work in this phase

Reason:

- current half-supported state is worse than an explicit temporary no

## Epic RC-0: Spec Lock

Goal:

- freeze the canonical runtime target before refactoring begins

Tasks:

- `RC-0.1` ratify [target-architecture-program.md](/home/anibjoshi/Documents/GitHub/strata-core/research/target-architecture-program.md)
- `RC-0.2` ratify [delta-map-runtime-durability.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-runtime-durability.md)
- `RC-0.3` record decisions `D1-D3`
- `RC-0.4` publish freeze policy for runtime work
- `RC-0.5` mark which existing modules are canonical, transitional, or deletion targets

Outputs:

- accepted architecture note
- accepted deletion list
- accepted runtime freeze

Exit gate:

- no disagreement on canonical runtime authority
- no disagreement on canonical durability direction

## Epic RC-1: Canonical Runtime Constructor

Goal:

- create one engine-owned runtime constructor and explicit mode model

Tasks:

- `RC-1.1` introduce explicit engine `DatabaseMode`
  Values should include at least `Primary`, `Follower`, `Cache`.
- `RC-1.2` define the canonical runtime-construction API in engine
  This should take mode, config, subsystem set, and only truly semantic bootstrap inputs.
- `RC-1.3` route raw `Database::open*` through the canonical constructor
- `RC-1.4` route `DatabaseBuilder::open*` through the canonical constructor
- `RC-1.5` route executor/product open through the canonical constructor
- `RC-1.6` route cache creation through the canonical constructor
- `RC-1.7` define which bootstrap behavior belongs in engine vs executor
- `RC-1.8` delete or demote wrapper-only semantic differences that do not belong in the product policy layer

Dependencies:

- `RC-0.*`

Acceptance:

- one code path constructs all runtime modes
- wrappers do not fork runtime semantics

## Epic RC-2: Same-Path Reopen Semantics

Goal:

- eliminate first-opener-wins semantic drift

Tasks:

- `RC-2.1` define a compatibility signature for an already-open runtime
  It should include mode, subsystem composition, and any semantics-bearing config.
- `RC-2.2` change `OPEN_DATABASES` reuse rules to validate compatibility before reuse
- `RC-2.3` reject incompatible reopen attempts with deterministic errors
- `RC-2.4` define follower-vs-primary reuse policy explicitly
- `RC-2.5` document path ownership and same-process reuse behavior

Dependencies:

- `RC-1.*`

Acceptance:

- opening the same path with incompatible runtime expectations fails loudly
- product open cannot accidentally inherit a weaker raw-engine runtime

## Epic RC-3: Canonical Subsystem Contract

Goal:

- make subsystem composition instance-local and complete

Tasks:

- `RC-3.1` define the subsystem lifecycle contract as the only instance-local semantic extension path
- `RC-3.2` classify existing behavior that currently lives outside it:
  - merge handlers
  - DAG hooks
  - refresh hooks
  - freeze hooks
- `RC-3.3` decide what moves into subsystem contracts vs engine-owned primitives
- `RC-3.4` stop relying on process-global semantic registration for a correctly opened instance
- `RC-3.5` define standard subsystem sets per mode

Dependencies:

- `RC-1.*`

Acceptance:

- a correctly opened runtime has all its semantics from engine mode plus instance-local subsystem declaration

## Epic RC-4: Canonical Durability Runtime

Goal:

- choose and enforce one durability/runtime ownership path

Tasks:

- `RC-4.1` evaluate `durability::database` against the chosen decision `D1`
- `RC-4.2` either:
  - integrate its useful pieces into engine and mark it for deletion
  - or promote it and retire the old engine-owned persistence wiring
- `RC-4.3` define the canonical ownership split between engine, concurrency, and durability after that decision
- `RC-4.4` update docs/comments to stop implying the non-canonical path is live

Dependencies:

- `RC-0.3`
- `RC-1.*`

Acceptance:

- there is one canonical persistence runtime in the codebase
- the non-canonical one is marked transitional or removed

## Epic RC-5: Canonical Recovery Planner

Goal:

- reduce recovery to one planner and one layout

Tasks:

- `RC-5.1` choose the canonical recovery planner
- `RC-5.2` unify engine open around that planner
- `RC-5.3` unify on-disk layout assumptions across engine and durability
- `RC-5.4` define the role of checkpoints according to `D2`
- `RC-5.5` unify WAL truncation logic with the actual recovery contract
- `RC-5.6` make `MANIFEST` the authoritative physical metadata anchor
- `RC-5.7` delete or retire the non-canonical recovery planner

Dependencies:

- `RC-4.*`

Acceptance:

- one recovery planner is used by the shipped open path
- one directory layout is authoritative
- checkpoint and WAL semantics match that planner

## Epic RC-6: Codec Truth

Goal:

- make codec behavior honest and singular

Tasks:

- `RC-6.1` decide whether durable encryption is in-scope this phase per `D3`
- `RC-6.2` if out of scope:
  - remove or demote product claims
  - make unsupported combinations fail with one canonical message
- `RC-6.3` if in scope:
  - thread codec decode through WAL reader
  - unify checkpoint serialization with configured codec
  - make recovery planner fully codec-aware
- `RC-6.4` publish plaintext exception list

Dependencies:

- `RC-5.*`

Acceptance:

- no half-supported durable encryption state remains

## Epic RC-7: Lifecycle State Machine

Goal:

- make shutdown explicit and authoritative

Tasks:

- `RC-7.1` define runtime lifecycle states in engine
- `RC-7.2` move essential close semantics out of `Drop`
- `RC-7.3` make shutdown own:
  - admission stop
  - task quiescence
  - WAL flush-thread teardown
  - final flush
  - subsystem freeze
  - registry/path release
- `RC-7.4` make `Drop` best-effort cleanup only
- `RC-7.5` define reopen-after-shutdown behavior explicitly

Dependencies:

- `RC-1.*`
- `RC-5.*`

Acceptance:

- explicit shutdown fully closes runtime and releases ownership
- destructor is not required for correctness

## Epic RC-8: Background Work Quiescence

Goal:

- unify close-time behavior for scheduler, flush, and follow-on work

Tasks:

- `RC-8.1` inventory every path that can enqueue background work
- `RC-8.2` prevent post-close scheduling after the close barrier begins
- `RC-8.3` unify scheduler drain and tx-idle ordering
- `RC-8.4` fix known failure modes such as stuck in-flight flags during submit failures
- `RC-8.5` document which background work is engine-owned vs external policy owned

Dependencies:

- `RC-7.*`

Acceptance:

- no background task survives the close barrier
- no scheduling path can strand the runtime in a fake “in flight” state

## Epic RC-9: Follower Replication Semantics

Goal:

- turn follower refresh from best-effort catch-up into a real replication contract

Tasks:

- `RC-9.1` define strict vs lossy follower policy explicitly
- `RC-9.2` change watermark advancement to contiguous-success-only
- `RC-9.3` ensure skipped or failed records are retried or cause hard failure
- `RC-9.4` unify follower subsystem refresh participation rules
- `RC-9.5` document follower freshness and durability semantics explicitly

Dependencies:

- `RC-5.*`
- `RC-7.*`

Acceptance:

- follower refresh cannot silently advance past missing records
- subsystem refresh behavior is declared and tested

## Epic RC-10: Runtime Truth Tests

Goal:

- encode Phase 1-3 architecture claims as tests

Tasks:

- `RC-10.1` open-path equivalence tests
- `RC-10.2` same-path incompatibility tests
- `RC-10.3` recovery planner tests
- `RC-10.4` checkpoint restore tests
- `RC-10.5` lifecycle close/reopen tests
- `RC-10.6` follower convergence tests
- `RC-10.7` codec-support matrix tests

Dependencies:

- parallel with `RC-1` through `RC-9`, but must be complete before phase close

Acceptance:

- all phase claims are tested

## First Tranche

This is the tranche I would start immediately after `RC-0`.

### Tranche 1A

- `RC-1.1` define `DatabaseMode`
- `RC-1.2` define canonical runtime constructor API
- `RC-2.1` define runtime compatibility signature
- `RC-3.1` define subsystem lifecycle contract

Why first:

- these tasks define the runtime vocabulary
- without them, refactors will keep being local and inconsistent

### Tranche 1B

- `RC-1.3` raw engine open through canonical constructor
- `RC-1.4` builder open through canonical constructor
- `RC-2.2` compatibility-checked reuse
- `RC-10.1` open-path equivalence tests

Why second:

- this is the first real authority collapse

### Tranche 1C

- `RC-1.5` executor open through canonical constructor
- `RC-1.6` cache through canonical constructor
- `RC-2.3` reject incompatible reuse
- `RC-10.2` same-path incompatibility tests

Why third:

- now the product surface is attached to the canonical runtime

## Deletion List

These items should be tracked explicitly in this backlog.

### Early deletion candidates

- non-canonical durability runtime
- non-canonical recovery planner

### Deletion once replacement exists

- wrapper-only semantic bootstrap that the engine now owns
- any same-path reuse behavior that bypasses compatibility checks

### Deferred but linked deletions

- vector WAL
- vector snapshot stack

These belong to the later primitive backlog, but should stay visible because runtime decisions will affect them.

## Phase Gates

### Gate G1: Runtime Spine Gate

Closed only when:

- all opens delegate to one constructor
- same-path incompatibility is rejected
- subsystem composition is instance-local and explicit

### Gate G2: Durability Gate

Closed only when:

- one durability runtime is canonical
- one recovery planner is canonical
- one layout is canonical
- checkpoint role is explicit and tested

### Gate G3: Lifecycle Gate

Closed only when:

- explicit shutdown is authoritative
- no essential cleanup lives only in `Drop`
- follower refresh is convergent

## Bottom Line

This backlog is the critical path for making Strata a real database runtime.

If this backlog stalls, the rest of the architecture work will keep piling better ideas on top of split authority.

If this backlog lands cleanly, the rest of the remediation program becomes much easier:

- branch truth can sit on a stable runtime
- primitive cleanup can target one lifecycle model
- product/control surfaces can be made honest against one real engine
