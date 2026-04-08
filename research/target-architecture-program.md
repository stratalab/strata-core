# Target Architecture Program

This document answers the next question after the audit:

**What should the architecture actually be, and how do we systematically move Strata from the current state to that state without rewriting the core kernel?**

The constraints are explicit:

- keep the LSM storage engine
- keep MVCC ordering
- keep OCC transactions
- keep the copy-on-write branching storage layer

The goal is not to reinvent Strata.
The goal is to stop the vibecoded runtime drift around a promising kernel and replace it with real database engineering.

## 1. Mission

We need to do three things in order:

1. define the target architecture precisely
2. measure the delta from current state to target state
3. execute the delta in controlled workstreams with acceptance criteria

If we skip step 1, we will keep doing opportunistic cleanup.
If we skip step 2, we will miss shadow authority and migration debt.
If we skip step 3, the architecture will keep drifting faster than it improves.

## 2. Non-Negotiable Principles

These are the architectural rules the rebuilt runtime should obey.

### A. One authoritative runtime spine

There must be one real path for:

- open
- recover
- commit
- refresh
- freeze
- shutdown

Wrappers can exist, but they must delegate into the same spine.

### B. One authoritative durability model

There must be one real answer for:

- what is durable
- what files are authoritative
- how recovery works
- when checkpoints are used
- what codecs apply to which bytes

No parallel durability stacks.

### C. Engine authority over policy layers

`executor`, CLI, and other product layers may shape the user API.
They must not redefine the runtime semantics.

### D. Derived state is not primary truth

Search, branch DAG, status caches, vector mmap caches, and similar structures are derived state unless explicitly promoted.

Derived state must be:

- rebuildable
- versioned if persisted
- clearly outside the primary durability contract unless explicitly included

### E. No process-global semantic singletons

Process-global state is acceptable for true process-level resources only.
It is not acceptable for semantic behavior like:

- merge handlers
- branch DAG hooks
- per-database capability composition
- instance-specific extension state

### F. Fail fast on unsupported combinations

Unsupported combinations must fail at:

- config validation
- open time

not later at first use.

### G. Security model must be explicit

If Strata is a same-user embedded database, say so and enforce that model cleanly.
Do not imply stronger isolation than the runtime actually provides.

### H. Feature flags may add capabilities, not change product identity invisibly

Feature flags should not silently turn one product into another.
If a build materially changes:

- semantics
- durability
- security
- config meaning
- licensing

that matrix must be explicit and validated.

### I. Delete shadow stacks aggressively

If a second path is not the chosen architecture, delete it.
Do not keep dormant parallel implementations around "just in case".

## 3. The Target Architecture

This is the design Strata should converge toward.

## 3.1 Kernel Boundary

The kernel stays:

- `storage` owns the LSM, MVCC visibility, segments, compaction mechanics, and branch inheritance
- `concurrency` owns OCC validation, version allocation, and commit ordering
- `durability` owns WAL bytes, checkpoint bytes, manifest bytes, codecs, and recovery planning

The architectural shift is not inside the kernel.
It is in how the runtime is governed around it.

## 3.2 Runtime Boundary

There should be one explicit runtime object model:

- `DatabaseRuntime` or equivalent authoritative engine-owned runtime
- one explicit `DatabaseMode`
- one explicit `RuntimeConfig`
- one explicit subsystem registry

All open paths should feed this same constructor:

- primary
- follower
- cache
- builder-based open
- product open

Different modes are acceptable.
Different semantic spines are not.

### Target rule

`Database::open`, builder open, executor open, follower open, and cache open must all be thin adapters over the same runtime-construction path.

## 3.3 Runtime Modes

Strata should expose a small, explicit runtime mode matrix:

- `Primary`
- `Follower`
- `Cache`

Each mode must define:

- lock behavior
- WAL behavior
- checkpoint behavior
- refresh behavior
- write ability
- subsystem behavior
- shutdown behavior

No hidden policy overlays.

## 3.4 Durability Design

There must be one real on-disk contract:

- one directory layout
- one manifest format
- one recovery planner
- one checkpoint policy
- one WAL codec story

### Target durability truth

- `MANIFEST` is authoritative for physical metadata
- WAL replay and checkpoint restore are integrated into the same recovery planner
- checkpoints are either first-class in open-path recovery or removed from the claimed architecture
- codec handling is uniform across WAL, checkpoint payloads, and recovery
- durable encryption is either fully supported or explicitly unsupported

No "writer encodes, reader doesn’t decode" seams.
No "snapshot records codec metadata but payload is still plaintext" seams.

## 3.5 Lifecycle Design

Lifecycle should be an engine-owned state machine, not a mix of methods plus destructor behavior.

### Required runtime states

- `Opening`
- `Open`
- `Closing`
- `Closed`
- `Failed`

### Target lifecycle rules

- `shutdown()` is the authoritative close path
- `Drop` is best-effort cleanup only, not essential finalization
- background work must quiesce behind engine-owned sequencing
- reopened instances must never inherit stale partially-closed state
- same-path instance reuse must be deliberate and validated, not accidental registry behavior

## 3.6 Background Work and Control Plane

Strata needs one engine-owned maintenance/control-plane service that governs:

- flush scheduling
- compaction scheduling
- TTL/expiry work
- retention/GC work
- follower refresh scheduling if enabled
- checkpoint scheduling if supported

### Target rules

- every live knob must map to a live behavior
- every non-live knob must be removed
- runtime reconfiguration must be explicitly classified:
  - open-time only
  - live-safe
  - restart required

The control plane should be boring, explicit, and testable.

## 3.7 Subsystem Architecture

Subsystems should be first-class runtime participants, not a mixture of:

- builder subsystems
- refresh hooks
- merge callbacks
- global registration
- executor side effects

### Target subsystem contract

Each subsystem must declare:

- open/init
- recover
- refresh
- freeze
- shutdown
- persistence contract
- whether it is primary truth or derived state

This should be instance-local, not process-global.

## 3.8 Derived State Policy

The system must clearly separate:

- primary state
- persisted derived state
- ephemeral caches

### Target classification

Primary truth:

- KV
- JSON
- Event
- branch storage metadata
- vector records if vector remains storage-native

Persisted derived state:

- only by explicit admission, versioning, and recovery contract

Ephemeral or rebuildable state:

- search indexes
- branch DAG projections
- status caches
- vector mmap accelerators unless explicitly promoted

If a structure is rebuildable, it should not pretend to be authoritative.

## 3.9 Branch Architecture

The storage-layer branching model stays.
The runtime semantics need cleanup.

### Target branch rules

- one branch identity model
- one ancestry model owned by the engine
- one merge-base algorithm owned by the engine
- DAG/status are projections, not source of truth
- branch deletion and recreation must not pollute identity or ancestry
- all branch mutation paths must obey the same snapshot and validation discipline

The engine should not depend on out-of-band executor history to decide merge correctness.

## 3.10 Replication / Follower Design

Follower mode should be treated as a real replication subsystem.

### Target follower rules

- refresh only advances through contiguous successful replay
- skipped records do not disappear behind the watermark
- strict vs lossy follower policy is explicit and consistent with open-time policy
- follower composition is the same runtime architecture as primary, minus write authority and with follower-specific durability semantics
- subsystem refresh behavior is declared, not ad hoc

If follower mode exists, it must converge or fail loudly.

## 3.11 Security Design

The target security model should be intentionally modest but honest.

### Target security rules

- engine/runtime owns true access authority
- secret redaction is invariant across surfaces
- plaintext exceptions are explicitly documented
- IPC is clearly defined as same-user local control plane unless stronger auth is added
- config/feature/security combinations fail fast when unsupported

Strata does not need to become a multi-tenant security product.
It does need to stop implying stronger boundaries than it has.

## 3.12 Feature-Matrix Design

Feature flags should produce an explicit product matrix, not hidden behavioral forks.

### Target feature rules

- every feature-gated capability has a documented impact on runtime behavior
- unsupported provider or model configs are rejected before use
- feature-disabled search/AI paths must either fail explicitly or be clearly opt-out behavior
- cross-build config mutation is not allowed
- licensing footprint changes are published as part of the feature matrix

## 4. Systematic Delta Mapping Framework

Now that the target architecture is defined, the next job is to compare every major subsystem against it using the same template.

Every domain should be mapped with this schema:

### Delta template

- `Current authoritative path`
- `Current shadow / competing paths`
- `Target design`
- `Gap type`
- `Consequence`
- `Disposition`
- `Acceptance test`

### Gap types

- `authority_split`
- `zombie_stack`
- `lifecycle_leak`
- `control_plane_drift`
- `security_drift`
- `feature_matrix_drift`
- `derived_state_confusion`
- `semantic_inconsistency`
- `test_gap`

### Disposition types

- `delete`
- `promote`
- `consolidate`
- `rewrite_boundary`
- `document_truth`
- `defer`

This keeps the work architectural instead of anecdotal.

## 5. Workstreams

These are the workstreams I would use to execute the delta systematically.

## Workstream 0: Architecture Spec Lock

Goal:

- freeze the target architecture before implementation churn starts

Deliverables:

- canonical runtime architecture document
- mode matrix
- durability contract
- derived-state classification table
- security model statement
- feature matrix statement

Acceptance:

- no new runtime path is added without reference to this spec
- no feature ships without landing in the matrix

## Workstream 1: Runtime Spine Consolidation

Goal:

- reduce Strata to one engine-owned runtime spine

Scope:

- unify raw open, builder open, executor open, follower open, cache open
- eliminate first-opener semantic drift
- move semantic authority out of wrapper layers

Primary outputs:

- one authoritative open constructor
- one runtime mode enum
- one subsystem composition path
- one instance registry policy

Acceptance:

- all opens share one semantic constructor
- wrapper layers add no hidden behavior

## Workstream 2: Durability and Recovery Unification

Goal:

- choose one durability architecture and make it real

Scope:

- integrate or delete alternate durability runtime
- integrate checkpoints into real recovery or demote/remove them
- unify filesystem layout assumptions
- unify codec application across WAL and snapshots

Primary outputs:

- one recovery planner
- one durable encryption truth
- one checkpoint policy
- one authoritative layout

Acceptance:

- no alternate recovery stack remains
- recovery tests validate the actual shipped path

## Workstream 3: Lifecycle and Control Plane Repair

Goal:

- make the runtime close, quiesce, maintain, and reconfigure predictably

Scope:

- authoritative shutdown
- background task quiescence
- maintenance subsystem
- truthful config application
- write-pressure and compaction loop stabilization

Primary outputs:

- lifecycle state machine
- control-plane truth table
- maintenance scheduler

Acceptance:

- `shutdown()` fully closes the runtime
- no essential cleanup remains only in `Drop`
- every exposed knob has a tested runtime effect or is removed

## Workstream 4: Branch Runtime Semantics

Goal:

- bring branch product semantics up to the quality of branch storage

Scope:

- unify branch identity
- internalize ancestry and merge-base truth
- harden branch deletion/recreation semantics
- normalize branch mutation snapshot discipline

Primary outputs:

- one branch identity schema
- engine-owned ancestry store
- consistent branch mutation contract

Acceptance:

- branch recreation by name is clean
- merge-base does not depend on external overlays

## Workstream 5: Primitive Consolidation

Goal:

- make every primitive choose one lifecycle and one durability model

Scope:

- vector cleanup
- search as explicit derived subsystem
- graph overlay cleanup
- event merge policy decision
- JSON conflict contract clarity

Primary outputs:

- delete dormant vector WAL/snapshot stacks
- primitive ownership table
- derived-state persistence table

Acceptance:

- no primitive owns multiple parallel persistence stories

## Workstream 6: Security and Feature-Matrix Hardening

Goal:

- make the product honest about security and build variants

Scope:

- secret redaction invariants
- config secret policy
- IPC trust model
- access authority
- feature-matrix validation
- provider fail-fast behavior

Primary outputs:

- explicit security statement
- explicit feature matrix
- fail-fast config validation

Acceptance:

- config/API surfaces do not disagree about secret visibility
- unsupported build/config combinations fail before use

## Workstream 7: Verification Program

Goal:

- prevent the architecture from drifting back into vibe-coded state

Scope:

- invariants tests
- crash/recovery matrix
- follower convergence tests
- lifecycle tests
- composition-path equivalence tests
- feature-matrix tests

Primary outputs:

- architecture test matrix
- mode equivalence tests
- crash harness

Acceptance:

- critical architecture claims are encoded as tests, not just docs

## 6. Execution Order

The sequencing matters.
Do not start by deleting random modules.

Recommended order:

1. Workstream 0: lock the target architecture
2. Workstream 1: unify runtime spine
3. Workstream 2: unify durability and recovery
4. Workstream 3: fix lifecycle and control plane
5. Workstream 4: fix branch runtime semantics
6. Workstream 5: consolidate primitives and delete zombies
7. Workstream 6: harden security and feature matrix
8. Workstream 7: encode the architecture into tests

The reason is simple:

- runtime authority first
- durability truth second
- operational correctness third
- primitive cleanup after the spine is stable

## 7. How To Measure the Delta

For each workstream, produce a domain table with one row per subsystem or boundary.

Suggested columns:

| Domain | Current Truth | Shadow Truth | Target Truth | Gap Type | Severity | Disposition | Acceptance Test |
|---|---|---|---|---|---|---|---|

This should be filled for at least:

- open/runtime composition
- recovery/durability
- lifecycle/shutdown
- background work
- branch identity and ancestry
- follower refresh
- vector lifecycle
- search lifecycle
- graph overlay behavior
- security/config secret surfaces
- feature-gated AI/provider behavior

## 8. Engineering Rules During the Program

These rules are important if we want the cleanup to be real.

### Rule 1

No new feature work that adds runtime paths until Workstreams 1-3 are materially complete.

### Rule 2

No parallel implementation should survive without an explicit owner and disposition.

### Rule 3

Every fix must choose:

- delete
- promote
- consolidate

Not "leave both for now" unless there is a documented migration checkpoint.

### Rule 4

Do not hide semantics in `executor` if they belong in the engine.

### Rule 5

If a claim appears in config, docs, or public API, it must be backed by a tested runtime path.

### Rule 6

If a mode is supported, it gets a mode-level acceptance test matrix.

## 9. Immediate Next Deliverables

The next concrete artifacts I would produce are:

1. `target-runtime-architecture.md`
   This should turn Sections 2-3 into a more formal architecture spec.
2. `delta-map-runtime-durability.md`
   First filled delta table for runtime spine and durability.
3. `delta-map-branches-primitives.md`
   Filled delta table for branches and secondary primitives.
4. `delta-map-security-features.md`
   Filled delta table for security and build variants.
5. `remediation-program.md`
   Sequenced execution backlog with owners and acceptance tests.

## 10. Bottom Line

The systematic way to fix Strata is not:

- random bug fixing
- deleting modules opportunistically
- adding another wrapper
- shipping more features while architecture stays split

The systematic way is:

- freeze the target architecture
- classify every delta against that target
- consolidate authority
- delete shadow stacks
- encode the architecture into tests

That is the difference between continuing to vibe code around a good kernel and actually engineering a database product.
