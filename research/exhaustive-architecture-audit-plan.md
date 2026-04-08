# Exhaustive Architecture Audit Plan

This plan is code-oriented.
It is designed to uncover not just obvious bugs, but structural ways Strata can
be internally inconsistent, misleading, or operationally unsafe.

The premise is simple:

- Strata is large enough that one audit lens is not enough
- different classes of breakage hide in different places
- we need an audit program, not just another code review

## Goal

Produce an evidence-backed answer to:

1. what the real runtime architecture is
2. where the codebase still contains parallel or obsolete architectures
3. where public behavior depends on embedding, order, feature flags, or
   undocumented side effects
4. which internal contracts are trustworthy, and which are only partially real

## Audit principles

### 1. Code first

Do not trust docs, comments, issue labels, or type names by themselves.
Use them only as leads.
The audit should follow active code paths, callers, and persisted formats.

### 2. One runtime truth per feature

Every important feature should have one answer to:

- where state is persisted
- who reconstructs it
- who owns the lifecycle
- what the fallback path is

If there are two answers, that is a likely architectural defect.

### 3. Defaults matter more than optional code

A feature is only "real" if it is part of the active path under default or
production composition.
Optional helpers, alternate builders, and dormant formats do not count as the
authoritative architecture.

### 4. Silent downgrade is a first-class failure mode

The audit should treat these as serious defects:

- best-effort behavior in correctness-sensitive paths
- fallback to weaker semantics without startup failure
- first-opener-wins composition
- process-global mutation
- builder-only recovery
- unused "future" subsystems that shadow active code

### 5. Every finding needs a route-to-truth

Each finding should identify:

- the claimed behavior
- the actual active path
- the shadow or conflicting path if one exists
- the concrete consequence
- the likely owner module

## Scope map

The audit should cover the full workspace, not just core storage:

- `crates/core`
- `crates/storage`
- `crates/concurrency`
- `crates/durability`
- `crates/engine`
- `crates/graph`
- `crates/search`
- `crates/vector`
- `crates/intelligence`
- `crates/inference`
- `crates/security`
- `crates/executor`
- `crates/cli`

The audit should treat the root runtime spine as:

- public entrypoint: `executor`
- orchestration/runtime: `engine`
- persistence/state: `storage`, `concurrency`, `durability`
- secondary/indexed primitives: `graph`, `vector`, `search`
- AI/runtime adjuncts: `intelligence`, `inference`
- access/config surface: `security`, `cli`, root feature flags

## Audit lenses

Each lens is meant to find a different class of failure.

## Lens 1: Runtime topology and ownership

Question:

- who actually owns each major concern at runtime

Audit for:

- duplicate ownership of recovery, persistence, or lifecycle
- ambient ownership through globals and `OnceCell`
- builder-only ownership vs default-open ownership
- executor-owned semantics that the engine pretends to own

Primary outputs:

- one runtime ownership map
- one conflict list where multiple modules appear to own the same concern

Expected hot spots:

- `engine` vs `durability`
- `engine` vs `executor`
- `engine` vs primitive subsystems

## Lens 2: Active path vs shadow path

Question:

- what production-grade modules are implemented but not authoritative

Audit for:

- alternate WAL/snapshot/recovery stacks
- unused but public persistence formats
- older code still owning the live path while newer code sits beside it
- dual control planes

Primary outputs:

- zombie migration inventory
- per-module verdict: active, shadowed, likely dormant, test-only, or live optional

Expected hot spots:

- `durability::database`
- `durability::recovery`
- `vector::wal`
- `vector::snapshot`
- scheduler and maintenance helpers

## Lens 3: Consistency model and invariant ownership

Question:

- what invariants actually hold, and where are they enforced

Audit for:

- invariants split across multiple layers
- public semantics stronger than actual enforcement
- gaps between raw layer and wrapped layer behavior
- primitives whose correctness depends on caller discipline

Primary outputs:

- invariant ledger
- list of invariants enforced in one place vs emergent across several modules

Expected hot spots:

- transaction snapshot semantics
- branch merge semantics
- JSON conflict semantics
- graph/vector/search rebuild correctness

## Lens 4: Durability, recovery, and crash boundaries

Question:

- after a crash, what state is guaranteed to come back, and by which path

Audit for:

- split restart architectures
- checkpoint/write path mismatch
- replay that is best-effort where it should be exact
- follower paths that advance state without convergence
- mutable "read-only" open paths

Primary outputs:

- restart truth table
- crash-state matrix by primitive and subsystem
- delta between claimed and actual recovery semantics

Expected hot spots:

- `engine::database::open`
- `concurrency::recovery`
- `durability::recovery`
- subsystem freeze/recover

## Lens 5: State shape and persisted format multiplicity

Question:

- how many on-disk or on-wire formats exist for the same logical thing

Audit for:

- multiple snapshot formats for one primitive
- multiple manifest/layout schemes
- config/state duplication
- incompatible identity schemes for the same entity

Primary outputs:

- persisted-format inventory
- duplicate-format matrix

Expected hot spots:

- vector persistence
- branch identity and status
- engine layout vs durability layout
- search/vector/graph local manifests

## Lens 6: Composition and embedding sensitivity

Question:

- how much behavior changes depending on who opens the database and how

Audit for:

- default open vs builder vs executor open
- order-sensitive registration
- first-opener-wins behavior
- feature-flag dependent semantic differences
- re-exported APIs that bypass required composition

Primary outputs:

- composition matrix
- semantic downgrade inventory

Expected hot spots:

- subsystem installation
- merge handlers
- DAG hooks
- refresh hooks

## Lens 7: Control plane and configuration truthfulness

Question:

- do config surfaces and knobs actually drive the behavior they claim to

Audit for:

- dead knobs
- stale comments about policies that no longer exist
- config written but not applied
- runtime mutable config that only partially takes effect
- multiple config types for the same concern

Primary outputs:

- config truth table
- "knob works / knob ignored / knob partially works" matrix

Expected hot spots:

- WAL Standard mode
- storage pressure/backpressure
- open-time-only settings
- root feature flags

## Lens 8: Lifecycle and background work

Question:

- what background work exists, who schedules it, and what happens if it fails

Audit for:

- maintenance work that is manual but presented as automatic
- background tasks with best-effort error handling in correctness-sensitive paths
- duplicated background threads
- freeze/recover asymmetry
- shutdown ordering issues

Primary outputs:

- lifecycle sequence diagrams
- background task ownership map
- error-handling severity table

Expected hot spots:

- WAL flush threads
- compaction and maintenance
- vector/search freeze paths
- follower refresh hooks

## Lens 9: Resource model and performance shape

Question:

- what memory, file, and CPU behaviors are guaranteed, and what is uncontrolled

Audit for:

- pinned metadata outside budgets
- process-global caches affecting all DB instances
- linear growth hidden behind “fast path” claims
- control loops that are present but disabled
- large rebuilds on open hidden behind "recovery"

Primary outputs:

- resource ownership map
- hard-budget vs soft-budget table
- high-risk scaling path list

Expected hot spots:

- storage metadata residency
- global block cache
- vector heap/graph rebuilds
- background scheduler saturation

## Lens 10: Isolation and boundary safety

Question:

- where can one branch, space, database, process, or feature leak into another

Audit for:

- process-global state
- branch identity fragmentation
- space leakage in recovery/search/indexes
- follower/primary interference
- extension state shared more broadly than intended

Primary outputs:

- isolation boundary map
- cross-tenant / cross-branch / cross-instance leak risks

Expected hot spots:

- branch metadata and DAG
- global singletons
- search/vector shadow collections
- follower refresh

## Lens 11: Security and sensitive-state handling

Question:

- where do secrets, embeddings, and durable bytes end up, and what is exposed

Audit for:

- plaintext assumptions in WAL or snapshots
- sensitive payloads in caches, mmap files, logs, or bundles
- access mode mismatches
- read-only paths that still mutate disk

Primary outputs:

- sensitive-data flow map
- encryption / plaintext exception list

Expected hot spots:

- WAL codec path
- checkpoint bytes
- vector mmap files
- branch bundles

## Lens 12: Feature matrix and build-mode divergence

Question:

- how much architecture changes under different cargo features or providers

Audit for:

- embed vs non-embed
- local vs cloud inference
- arrow vs non-arrow
- perf-trace vs default
- multi-process feature paths

Primary outputs:

- feature-mode matrix
- code paths compiled but never exercised in default builds

Expected hot spots:

- `intelligence`
- `inference`
- `durability` feature gates
- executor provider wiring

## Lens 13: Public API vs internal truth

Question:

- where does the public product surface imply capabilities the internals do not fully support

Audit for:

- convenience APIs that hide composition requirements
- names that imply stronger semantics than the code provides
- re-exports of dormant or alternate stacks
- operational commands that do not correspond to one runtime truth

Primary outputs:

- product-surface mismatch ledger
- naming cleanup list

Expected hot spots:

- executor re-exports
- vector crate root
- engine open APIs
- branch merge strategy names

## Lens 14: Test adequacy and invisible-risk audit

Question:

- what dangerous assumptions are untested, and what tests only prove dead paths

Audit for:

- critical paths with no regression tests
- tests only covering builder/executor paths, not raw engine paths
- modules with strong internal tests but no runtime callers
- no-failure-path testing around best-effort logic

Primary outputs:

- missing-test matrix
- “tested but not used” module list

## Workstreams

To keep this manageable, run the audit in workstreams rather than by crate only.

## Workstream A: Runtime spine

Scope:

- `executor`
- `engine`
- `concurrency`
- `storage`
- `durability`

Questions:

- what is the one real open/commit/recover/shutdown path
- where are the duplicate stacks
- what is process-global vs instance-local

## Workstream B: Secondary primitives

Scope:

- `graph`
- `vector`
- `engine::search`
- `search`

Questions:

- how secondary state is derived, frozen, rebuilt, and refreshed
- where primitive semantics depend on registration or executor composition
- where dormant parallel persistence formats exist

## Workstream C: AI/runtime adjuncts

Scope:

- `intelligence`
- `inference`
- embed-related executor paths

Questions:

- what is part of the core database contract vs optional provider integration
- where feature flags create materially different runtime semantics
- how model state, caches, and embeddings interact with persistence

## Workstream D: Public/control surfaces

Scope:

- `executor`
- `cli`
- `security`
- config and open options

Questions:

- what public APIs imply that internals cannot reliably guarantee
- where control surfaces drift from active runtime behavior

## Evidence standard

Every finding should include:

1. severity:
   `critical`, `high`, `medium`, `low`
2. lens:
   which audit lens exposed it
3. claimed behavior:
   what the codebase surface suggests
4. actual behavior:
   what the active path does
5. evidence:
   caller chain plus file references
6. consequence:
   correctness, durability, operability, performance, security, or maintainability
7. disposition:
   fix, narrow surface, delete dead path, document as optional, or defer

## Concrete audit procedures

For each workstream, use the same procedures:

### Procedure 1: Entry-point tracing

Start from the public/open path and trace inward:

- `Strata::open`
- `Database::open`
- builder-based opens
- follower open
- shutdown and drop

Goal:

- identify the authoritative path and all branches off it

### Procedure 2: Reverse caller audit

For each suspicious subsystem:

- inspect its exports
- inspect all callers outside its defining crate
- decide whether it is authoritative, optional, shadowed, or dead

This is how zombie migrations are found.

### Procedure 3: Persisted-format census

Enumerate all persisted formats and ownership:

- WAL records
- segment manifests
- MANIFEST
- snapshots/checkpoints
- branch bundles
- vector mmap files
- search manifests
- graph/system-space metadata

Goal:

- one table mapping each artifact to its writer, reader, and authoritative consumer

### Procedure 4: Feature-surface truth table

For each major capability, fill out:

- surfaced where
- actually enforced where
- default path
- alternate path
- dead path

This exposes public/internal drift.

### Procedure 5: Error-path audit

For each background or replay path, classify failures as:

- fatal and surfaced
- retried
- logged and skipped
- logged and state still advanced
- silently ignored

Goal:

- find best-effort paths in correctness-sensitive code

### Procedure 6: Global-state audit

Search for:

- `static`
- `OnceCell`
- `Lazy`
- `GLOBAL_`
- registries keyed by path

Then classify whether the state is:

- instance-local in disguise
- intentional process runtime
- accidental cross-instance coupling

### Procedure 7: Differential open-path audit

Compare behavior across:

- raw engine open
- builder open
- executor open
- cache DB
- follower DB

Goal:

- identify semantic drift by entrypoint

### Procedure 8: Compile-time matrix audit

Review feature-gated code and provider-specific code for:

- dead architecture
- alternate semantics
- untested branches

## Deliverables

The full audit should produce:

1. architecture runtime map
2. zombie migration inventory
3. persisted-format census
4. config truth table
5. composition matrix
6. failure-handling matrix
7. global-state inventory
8. gap register with severity and disposition
9. delete-or-promote list for shadow stacks
10. prioritized remediation program

## Recommended execution order

1. Runtime spine
   Map the authoritative open/commit/recover/shutdown path.
2. Shadow-stack sweep
   Find duplicate recovery, WAL, snapshot, scheduler, and subsystem stacks.
3. Composition and default-path audit
   Compare raw engine, builder, and executor behavior.
4. Primitive-specific audits
   Graph, vector, search, event, JSON.
5. Control-plane and lifecycle audit
   Config, maintenance, refresh, shutdown, background work.
6. Security and feature-matrix audit
   Sensitive bytes, codec paths, feature-gated alternate architectures.
7. Synthesis
   Convert findings into delete/promote/fix decisions.

## Success criteria

This audit is successful if, by the end, we can answer these without hand-waving:

1. what is the one true restart architecture
2. what is the one true vector durability architecture
3. what semantics are intrinsic to the engine, and which live in the executor
4. what modules are dead architecture and should be deleted
5. what claims in the public surface are overstated or false
6. what process-global state still prevents real instance isolation
7. which internal contracts are safe enough to build on

## Short version

The audit should not ask only "where are the bugs?"
It should ask:

- where are there two architectures for one concern
- where is the real behavior composition-sensitive
- where does the public surface lie about what is actually authoritative
- where does state advance even when correctness work failed

That is how we uncover the different ways Strata is broken internally.
