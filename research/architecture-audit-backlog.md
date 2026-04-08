# Architecture Audit Backlog

This document turns the audit plan into an executable program.
It is meant to answer two questions:

1. what to inspect next
2. what evidence each step must produce

The program is intentionally ordered.
We should not audit peripheral systems before the runtime spine is nailed down.

## Program order

### Stage 0: Runtime spine bootstrap

Status: completed

Goal:

- identify the one real open, commit, recover, refresh, freeze, and shutdown path

Primary scope:

- `crates/executor`
- `crates/engine`
- `crates/concurrency`
- `crates/storage`
- `crates/durability`

Outputs:

- authoritative runtime path map
- ownership matrix
- shadow-stack shortlist
- composition drift shortlist

Bootstrap findings already confirmed:

- production subsystem composition lives in `executor`, not intrinsically in `engine`
- `Database::open*` and builder/executor open are not semantically identical
- same-path open is singletonized by `OPEN_DATABASES`, so first opener wins
- follower open is a separate runtime path, not just a mode bit on the primary path
- a second durability runtime still exists in `durability::database`
- branch DAG and branch status are graph-side overlay state, while executor still
  depends on DAG history for some merge-base decisions

### Stage 1: Shadow-stack sweep

Status: completed

Goal:

- find modules that look authoritative but are not on the live path

Primary targets:

- `durability::database`
- `durability::recovery`
- `vector::wal`
- `vector::snapshot`
- alternate scheduler and maintenance helpers

Outputs:

- zombie migration inventory
- delete / promote candidates

Confirmed findings:

- `durability::database` is a real alternate database runtime, not a helper
- `durability::recovery` is a real alternate recovery architecture with a
  different filesystem layout assumption
- `vector::wal` and `vector::snapshot` are dormant parallel durability stacks
- `storage::CompactionScheduler` is a dormant alternate scheduling surface
- subsystem composition is split across builder subsystems, refresh hooks,
  merge registrations, and branch-DAG hook registration
- `GraphSubsystem` exists in canonical subsystem form but is not on the
  standard product open path

Primary deliverable:

- `research/stage-1-shadow-stack-sweep.md`

### Stage 2: Composition matrix

Status: completed

Goal:

- compare behavior across all open paths and embeddings

Compare:

- `Strata::open`
- `Strata::open_with`
- `Database::open`
- `DatabaseBuilder::open`
- `Database::open_follower`
- `DatabaseBuilder::open_follower`
- `Strata::cache`
- `Database::cache`
- IPC fallback path

Outputs:

- composition matrix
- first-opener-wins mismatch list
- executor-owned semantics inventory

Confirmed findings:

- `Strata::open_with` is a policy router that can yield local primary,
  local follower, or IPC client backends
- raw engine open, builder open, product open, follower open, and cache open
  are materially different products, not cosmetic wrappers
- primary read-only open is an executor policy over a primary runtime, not a
  follower open
- primary composition is first-opener-wins because `OPEN_DATABASES` returns the
  existing instance unchanged
- `Strata::cache` and `Database::cache` have different subsystem and branch
  bootstrap behavior
- the stale-socket retry branch inside `Strata::open_with` skips
  `init_system_branch` and built-in recipe seeding

Primary deliverable:

- `research/stage-2-composition-matrix.md`

### Stage 3: Primitive audits

Status: completed

Goal:

- determine which primitive semantics are intrinsic to the engine and which are overlay behavior

Primary targets:

- vector
- search
- graph
- JSON
- event
- branch DAG / branch status

Outputs:

- per-primitive lifecycle maps
- per-primitive persisted-format tables
- merge / refresh / freeze / rebuild gap list

Confirmed findings:

- JSON and Event are mostly engine-native primitives with storage as the real
  source of truth
- ordinary Graph data is storage-native, but graph merge semantics depend on
  registered callbacks
- Search is a global derived overlay rebuilt from other primitives, not a
  primary data primitive
- Vector is the most split primitive: storage truth plus subsystem recovery,
  refresh hooks, merge callbacks, mmap caches, and dormant WAL/snapshot stacks
- branch DAG and branch status are overlay state, not authoritative branch
  lifecycle truth
- follower refresh ownership is inconsistent across primitives: search is
  engine-hardcoded, vector is hook-based, and other primitives rely on storage
  truth plus search catch-up

Primary deliverable:

- `research/stage-3-primitive-lifecycle-map.md`

### Stage 4: Control plane and lifecycle audit

Status: completed

Goal:

- verify that knobs, maintenance, and background work behave as claimed

Primary targets:

- WAL mode knobs
- storage pressure / write stall knobs
- maintenance / GC / TTL
- background scheduler
- shutdown / drop / freeze ordering

Outputs:

- config truth table
- failure-handling matrix
- lifecycle sequence diagrams

Confirmed findings:

- memory-budget-derived sizing, live storage/coordinator config application,
  and compaction rate limiting are real control surfaces
- maintenance is mostly manual and the storage entrypoints behind
  `run_gc()` / `run_maintenance()` are mostly stubs
- L0 write-stall defaults are intentionally disabled because the current
  control loop can deadlock under rate-limited compaction
- Standard WAL sync policy has drifted from its surface: time-based syncing is
  real, count/byte thresholds are no longer authoritative
- runtime durability switching is split-authority and leaves the database's
  own durability-mode field stale
- `background_threads` only sizes the generic scheduler; it does not imply
  automatic maintenance, WAL-thread control, or parallel LSM compactions
- `schedule_flush_if_needed()` has a real failure-handling bug: if scheduler
  submit fails, `flush_in_flight` can remain stuck `true`
- shutdown and `Drop` still jointly define lifecycle truth

Primary deliverable:

- `research/stage-4-control-plane-lifecycle-audit.md`

### Stage 5: Resource and isolation audit

Status: completed

Goal:

- find all ways the runtime can leak across branch, instance, or process boundaries

Primary targets:

- process-global caches and registries
- branch identity and status
- follower refresh
- extension and hook registries
- shared mmap / local manifest state

Outputs:

- global-state inventory
- isolation boundary map
- scaling path risk list

Confirmed findings:

- `OPEN_DATABASES` makes primary same-path runtime state path-global inside a
  process
- the storage block cache is process-global, including capacity control
- vector merge callbacks, graph merge planners, and branch-DAG hooks are
  process-global first-registration-wins state
- database extensions are database-scoped in implementation but path-global in
  practice for primary same-path opens because the `Database` is shared
- branch identity is split across deterministic branch IDs, random metadata
  UUIDs, and DAG/status state keyed by branch name
- search and vector mmap/manifest files are shared local cache layers under the
  database directory and therefore part of the isolation model
- namespace caches are process-global with uneven invalidation hygiene across
  primitives

Primary deliverable:

- `research/stage-5-resource-isolation-audit.md`

### Stage 6: Security and feature-matrix audit

Status: completed

Goal:

- find architecture that changes materially under features, codecs, or providers

Primary targets:

- WAL codec path
- snapshot / checkpoint bytes
- `embed` feature
- multi-process coordination feature paths
- inference / intelligence provider wiring

Outputs:

- feature-mode matrix
- sensitive-data flow map
- plaintext exception list

Confirmed findings:

- the real security boundary is local and policy-driven: Unix file permissions,
  executor `AccessMode`, and same-user IPC socket access, not a centralized
  engine-enforced security model
- `AccessMode::ReadOnly` is a handle/session policy, not a database-global
  invariant, and `_system_` privilege differs between local and IPC backends
- secrets are stored in plaintext `strata.toml`, `SensitiveString` only redacts
  selected formatting paths, and `CONFIG GET` returns real secret values while
  `CONFIGURE GET key` masks them
- the codec/encryption story is materially divergent: durable WAL-backed opens
  reject non-identity codecs, WAL codec support is half-implemented, and the
  snapshot/checkpoint path records codec metadata while still serializing
  plaintext section data
- the `embed` feature materially changes the product: some commands fail
  loudly, while search expansion/rerank/RAG and auto-embed degrade or no-op
- non-`embed` builds can persistently rewrite `auto_embed=true` to `false`
- provider config accepts cloud provider names independent of compiled provider
  support and only fails later at generation time
- telemetry is currently a config/init surface without a corresponding runtime
  subsystem
- the feature matrix also changes the legal product: root is Apache-2.0 while
  `strata-search`, `strata-intelligence`, and `strata-inference` are BUSL-1.1

Primary deliverable:

- `research/stage-6-security-feature-matrix-audit.md`

### Stage 7: Synthesis

Status: completed

Goal:

- convert findings into decisive actions

Outputs:

- gap register with severity and disposition
- delete-or-promote list
- prioritized remediation program

Confirmed findings:

- the strongest part of the system is still the storage/OCC kernel, not the
  runtime shell around it
- the central architectural problem is split runtime authority across
  `executor`, `engine`, `concurrency`, `durability`, subsystem hooks, and
  global registrations
- the codebase repeatedly shows shadow authority and zombie migrations rather
  than one hard canonical path
- recovery, control-plane truth, lifecycle ownership, follower convergence,
  branch semantics, security, and feature-mode identity all remain weaker than
  the product surface implies
- the project is salvageable, but only if consolidation takes priority over
  feature expansion

Primary deliverable:

- `research/stage-7-final-synthesis.md`

## Workstream task list

## Workstream A: Runtime spine

Status: completed

Tasks:

- trace primary open from `Strata::open_with` through `DatabaseBuilder` into `Database::open_internal_with_subsystems`
- trace raw engine open from `Database::open` and compare it with executor open
- trace follower open and `refresh()`
- trace cache open and shutdown / drop
- trace commit path ownership from engine transaction API into concurrency manager and WAL append
- trace freeze and shutdown ownership
- reverse-audit `durability::database::handle` and `durability::recovery` for live callers
- reverse-audit higher-level direct callers of `concurrency`, `storage`, and
  `durability` APIs to distinguish real bypasses from intentional subsystem
  internals

Evidence to collect:

- caller chains
- one ownership table for open / recover / freeze / shutdown / refresh
- one path-drift table comparing entrypoints

Primary deliverable:

- `research/workstream-a-runtime-spine.md`
- `research/workstream-a-commit-path.md`
- `research/workstream-a-branch-ops-locking.md`
- `research/workstream-a-branch-dag-status.md`
- `research/workstream-a-shutdown-drop-freeze.md`
- `research/workstream-a-follower-refresh.md`
- `research/workstream-a-lower-layer-direct-callers.md`
- `research/workstream-a-ownership-matrix.md`

## Workstream B: Secondary primitives

Status: pending

Tasks:

- map vector lifecycle: write path, rebuild path, refresh hooks, checkpoint path, mmap files
- map search lifecycle: recovery, refresh, freeze, optional index state
- map graph lifecycle: branch DAG hooks, graph merge, status caches, `_system_` branch dependence
- audit event merge / replay / branch semantics
- audit JSON conflict semantics from API surface down to concurrency checks

Primary deliverables:

- primitive lifecycle notes
- persisted-format census for derived state

## Workstream C: AI and adjunct runtimes

Status: pending

Tasks:

- map `intelligence` and `inference` provider selection and feature gating
- identify what durable state, caches, or models they own
- identify whether embeddings and derived model outputs participate in core durability guarantees

Primary deliverables:

- feature-mode matrix
- adjunct-runtime ownership map

## Workstream D: Public and control surfaces

Status: pending

Tasks:

- audit `executor` and `cli` for promises stronger than engine truth
- audit `security` for access-mode and sensitive-state assumptions
- audit config surfaces against actual runtime application

Primary deliverables:

- product-surface mismatch ledger
- knob truth table

## Severity rubric

- `critical`: correctness, durability, or isolation can fail under normal use
- `high`: production behavior is composition-sensitive or misleading in important paths
- `medium`: the design is internally inconsistent or expensive, but bounded
- `low`: naming drift, dormant surface, or cleanup debt with low direct risk

## Immediate next steps

1. Finish the runtime spine map with commit-path ownership and shutdown sequencing.
2. Reverse-audit the durability stack to make the shadow-runtime boundary explicit.
3. Build the first composition matrix for executor open vs engine open vs follower vs cache.

## Exit condition for Stage 0

Stage 0 is complete only when we can answer these precisely:

- what path a normal product open uses
- what path a raw engine consumer uses
- what path a follower uses
- what path an IPC client actually uses
- who owns subsystem composition
- who owns recovery
- who owns shutdown and freeze
- what alternate runtime still exists but is not authoritative
