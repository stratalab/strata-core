# Remediation Program

This document turns the target architecture and delta maps into an execution plan.

It is not a wishlist.
It is a sequence of architectural repairs with gates, freezes, and definitions of done.

The goal is:

**move Strata from a promising but under-consolidated database product to a runtime that is architecturally honest, operationally coherent, and test-backed.**

The constraints remain:

- keep the LSM engine
- keep MVCC
- keep OCC
- keep copy-on-write branch storage

## 1. Program-Level Verdict

The audit showed three big classes of debt:

1. split authority
2. shadow stacks and zombie migrations
3. product surface outrunning runtime truth

That means the remediation program cannot be "fix the worst bugs first" in the usual sense.
If we do that, we will keep repairing local symptoms while the architecture keeps drifting.

The program has to do four things:

1. freeze architectural sprawl
2. consolidate runtime authority
3. delete shadow paths
4. encode the intended architecture into tests and product surfaces

## 2. Program Rules

These rules should hold for the duration of the remediation effort.

### Rule A: Feature Freeze Above The Kernel

No new runtime modes, persistence paths, branch semantics, or primitive lifecycle mechanisms ship until Phases 1-3 are materially complete.

Allowed during the freeze:

- bug fixes inside existing canonical paths
- tests
- docs
- deletion of shadow stacks
- internal refactors that reduce split authority

Not allowed during the freeze:

- new background services
- new persistence formats
- new branch-control-plane overlays
- new feature-gated AI/runtime behaviors
- new product promises

### Rule B: Every Parallel Path Must Get A Disposition

Each shadow path must be explicitly marked:

- `delete`
- `promote`
- `integrate`

No indefinite "keep both for now" without a dated migration gate.

### Rule C: Product Surfaces Cannot Stay Ahead Of Runtime Truth

If a control, feature, or security behavior is not real and tested:

- remove it
- demote it
- or clearly label it unsupported/experimental

### Rule D: Tests Are Architecture Gates

No phase is complete because the code "looks cleaner."
A phase is complete when its architectural claims are encoded as tests.

## 3. Hard Decisions To Make Up Front

Before implementation starts, a few decisions should be made explicitly.

### Decision 1: Canonical Durability Base

Choose one:

- promote the richer `durability` runtime into the engine path
- or fold its good ideas into the current engine/concurrency path and delete the alternate runtime

But choose.

### Decision 2: Branch DAG Role

Choose one:

- DAG is projection only
- or DAG is authoritative engine metadata

My recommendation: projection only.

### Decision 3: Secret Policy

Choose one:

- plaintext config with explicit local-permission model
- env-only for secrets
- external secret-provider integration

My recommendation: near-term honesty first.
If plaintext config remains, document it clearly and remove accidental secret leaks.

### Decision 4: Durable Encryption Support

Choose one:

- make durable encryption real across WAL + checkpoints + recovery
- or explicitly drop the claim for now

My recommendation: do not keep the current half-supported middle state.

### Decision 5: Event Branch Merge

Choose one:

- strict refusal is the product contract
- or build a first-class event rebase/merge model

My recommendation: keep strict refusal until the runtime spine is fixed.

## 4. Phase Plan

## Phase 0: Spec Lock

Goal:

- freeze the target architecture and the allowed execution plan

Inputs:

- [target-architecture-program.md](/home/anibjoshi/Documents/GitHub/strata-core/research/target-architecture-program.md)
- [delta-map-runtime-durability.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-runtime-durability.md)
- [delta-map-branches-primitives.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-branches-primitives.md)
- [delta-map-product-control-surface.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-product-control-surface.md)

Outputs:

- accepted target architecture
- accepted deletion/promote list
- accepted feature freeze policy
- accepted phase gates

Exit gate:

- no disagreement remains about the target runtime shape
- no one is still arguing that shadow paths can all remain indefinitely

## Phase 1: Runtime Spine Consolidation

Goal:

- create one canonical engine-owned runtime constructor and one explicit mode model

Primary tasks:

- define `DatabaseMode` as the authoritative engine mode model
- create one engine-owned runtime constructor
- make raw open, builder open, executor open, follower open, and cache open delegate to it
- rewrite same-path reuse to validate compatibility instead of silently reusing mismatched instances
- move subsystem composition into one explicit instance-local contract
- stop wrapper layers from adding hidden semantic bootstrap that the runtime itself does not own

Primary deletions or boundary changes:

- first-opener-wins semantic drift
- wrapper-only semantic differences between raw, builder, and product open

Exit gate:

- one canonical open path exists
- all open variants are thin front doors to it
- incompatible same-path reopen is rejected deterministically

## Phase 2: Durability And Recovery Unification

Goal:

- make durability and restart behavior singular and truthful

Primary tasks:

- choose the canonical durability runtime
- delete or absorb the alternate durability runtime
- choose the canonical recovery planner
- unify filesystem layout
- make checkpoints either part of real recovery or explicitly not part of the durability contract
- unify codec behavior across WAL, checkpoints, and recovery
- make `MANIFEST` the single physical metadata anchor

Primary deletions:

- non-canonical durability runtime
- non-canonical recovery planner

Exit gate:

- one recovery planner is used in the shipped open path
- one on-disk layout exists
- durable encryption is either fully supported or explicitly unsupported
- checkpoint role is real and tested

## Phase 3: Lifecycle And Replication Repair

Goal:

- make close, background work, and follower semantics real and deterministic

Primary tasks:

- move essential close logic out of `Drop` into explicit shutdown
- build a real lifecycle state machine
- make background task quiescence ordered and testable
- repair follower refresh so it only advances through contiguous successful replay
- unify subsystem refresh participation rules

Primary boundary changes:

- `shutdown()` becomes authoritative
- follower mode stops being best-effort catch-up and becomes a real replication contract

Exit gate:

- explicit shutdown fully closes the runtime
- no essential cleanup remains only in destructor path
- follower refresh cannot silently skip and forget records

## Phase 4: Branch Truth Consolidation

Goal:

- bring branch runtime semantics up to the level of branch storage quality

Primary tasks:

- unify branch identity
- make ancestry engine-owned
- eliminate merge-base dependence on executor DAG override
- make delete/recreate generation-safe
- unify branch lifecycle/state vocabulary
- standardize snapshot/validation semantics across merge, diff, revert, cherry-pick
- rename misleading merge policies if behavior stays the same

Primary boundary changes:

- DAG becomes projection or is internalized, but stops living in the current ambiguous state

Exit gate:

- branch identity has one canonical model
- merge base is engine-owned
- branch recreation by name is clean and test-backed

## Phase 5: Primitive Lifecycle Consolidation

Goal:

- give each primitive one lifecycle contract and one persistence story

Primary tasks:

- define primitive ownership table
- classify primary truth vs persisted derived vs ephemeral cache
- formalize search as shared derived infrastructure
- separate graph data from DAG/status control-plane projection
- collapse vector lifecycle into one subsystem/runtime contract
- delete vector WAL
- delete vector snapshot stack
- eliminate primitive-global semantic registration

Primary deletions:

- vector WAL
- vector snapshot stack
- process-global primitive merge/DAG registration if canonical instance-local replacement exists

Exit gate:

- every primitive has one recover/refresh/freeze owner
- no primitive has parallel persistence narratives
- vector correctness is clearly KV-first and cache-second

## Phase 6: Product And Control-Surface Truth

Goal:

- make the product surface honest and code-generated from runtime truth

Primary tasks:

- build a control-surface registry for all config keys and product controls
- classify each control as live, open-time only, restart required, unsupported
- remove or demote dead knobs
- centralize durability control semantics
- make secret redaction invariant
- choose and document the secret-storage policy
- document or harden IPC trust model
- publish and enforce feature/build matrix
- make provider/config validation fail fast
- stop cross-build config mutation like `auto_embed` rewrite
- remove telemetry surface if it remains unimplemented

Primary deletions:

- dead config knobs
- unsupported or misleading feature surfaces
- telemetry surface if there is still no subsystem

Exit gate:

- public docs/examples do not promise stronger semantics than the runtime
- config and feature behavior is fully classified and tested
- security policy is explicit and accurate

## Phase 7: Verification And Release Gate

Goal:

- prevent regression into architectural drift

Primary tasks:

- create architecture test matrix
- add open-path equivalence tests
- add crash/recovery matrix
- add lifecycle close tests
- add follower convergence tests
- add branch-generation and merge-base tests
- add build-matrix capability tests

Exit gate:

- critical architecture claims are enforced by tests
- release criteria reference those tests explicitly

## 5. First 90% Program Order

This is the recommended high-level order:

1. Phase 0: Spec lock
2. Phase 1: Runtime spine
3. Phase 2: Durability/recovery
4. Phase 3: Lifecycle/replication
5. Phase 4: Branch truth
6. Phase 5: Primitive lifecycle
7. Phase 6: Product/control-surface truth
8. Phase 7: Verification/release gate

This order is important.

Do not:

- fix vector lifecycle before branch/runtime truth
- fix product surfaces before runtime semantics exist
- fix follower semantics before the durability/runtime spine is singular

## 6. What Gets Deleted

The program should have an explicit deletion backlog, not just a refactor backlog.

### Delete early if possible

- alternate durability runtime not chosen as canonical
- alternate recovery planner not chosen as canonical
- vector WAL
- vector snapshot stack
- dead WAL tuning knobs
- telemetry surface if no implementation is planned

### Delete once replacements exist

- process-global semantic registration for merge/DAG behavior
- wrapper-only bootstrap semantics that bypass canonical runtime
- misleading config/control surfaces

## 7. What Gets Promoted

Some parts of the current codebase are good and should become first-class.

Promote:

- `MANIFEST` as authoritative physical metadata
- the better parts of the richer durability recovery design
- builder-style explicit subsystem declaration, but internalized into engine-owned runtime construction
- JSON/Event as the model for cleaner primitive design
- search as explicitly derived shared infrastructure

## 8. What Gets Rewritten

These are the hardest but most important rewrites.

Rewrite:

- runtime open authority
- shutdown/drop boundary
- follower watermark semantics
- branch identity and ancestry model
- primitive lifecycle ownership
- control-surface registry
- secret/redaction behavior
- feature-matrix validation

## 9. Architecture Gates

Each phase should have a gate that blocks forward movement if not satisfied.

### Gate 1: Runtime Gate

Blocked until:

- one canonical open path exists
- same-path incompatibility is rejected

### Gate 2: Durability Gate

Blocked until:

- one recovery planner exists
- one layout exists
- one checkpoint role exists

### Gate 3: Lifecycle Gate

Blocked until:

- explicit shutdown is authoritative
- follower refresh is convergent

### Gate 4: Branch Gate

Blocked until:

- one branch identity model exists
- DAG is no longer semantically ambiguous

### Gate 5: Primitive Gate

Blocked until:

- vector zombie stacks are gone
- every primitive has one lifecycle contract

### Gate 6: Product Truth Gate

Blocked until:

- every user-facing control is classified
- secret behavior is invariant
- feature/build matrix is explicit

## 10. Success Criteria

Strata should be considered architecturally remediated when these statements are all true:

1. There is one canonical runtime spine.
2. There is one canonical durability and recovery architecture.
3. Shutdown is explicit and authoritative.
4. Follower mode is convergent or fails loudly.
5. Branch identity and ancestry are engine-owned.
6. Derived state is clearly classified and rebuildable unless explicitly promoted.
7. Vector no longer carries parallel persistence stories.
8. Product and config surfaces only expose supported, tested behavior.
9. Security boundaries are modest but honest.
10. The architecture is enforced by tests, not just docs.

## 11. Immediate Next Actions

If this program is accepted, the next concrete work items should be:

1. Create `runtime-consolidation-backlog.md`
   This should break Phases 1-3 into concrete code changes.
2. Create `branch-primitive-consolidation-backlog.md`
   This should break Phases 4-5 into concrete code changes.
3. Create `product-truth-backlog.md`
   This should break Phase 6 into concrete code and doc changes.
4. Mark the deletion list with owners and prerequisites.
5. Add release-policy text that the feature freeze is active.

## 12. Bottom Line

The right way to fix Strata is not to keep making local improvements while the architecture remains split.

The right way is:

- freeze sprawl
- consolidate runtime authority
- delete shadow stacks
- tighten branch and primitive semantics
- make product surfaces honest
- lock it all down with tests

That is how Strata stops feeling like a vibecoded shell around a strong kernel and starts acting like a real database product.
