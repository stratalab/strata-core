# Stage 7: Final Synthesis

This is the blunt version.

After tracing the open path, commit path, recovery path, branch lifecycle, secondary primitives, control plane, resource model, security surface, and feature matrix, the conclusion is:

**Strata has a credible storage-and-transaction kernel wrapped in an under-consolidated, migration-heavy runtime that does not yet deserve strong production claims.**

That is the shortest honest summary of the current system.

## Executive Verdict

If Strata were judged only on its storage kernel, it would look promising:

- the LSM storage core is real
- the MVCC ordering model is coherent
- the OCC commit spine is defensible
- copy-on-write branch storage is technically strong

But Strata is not just a storage kernel.
It is the whole runtime that users operate:

- open
- recover
- commit
- refresh
- merge
- freeze
- shutdown
- configure
- secure
- run across products and feature builds

At that level, the architecture is not clean enough, authoritative enough, or consistent enough to be called production-hardened.

My overall rating remains roughly:

- core engine design: 7/10
- production architecture readiness: 5/10
- operational reliability across the advertised surface: 4-5/10

## What Is Actually Good

These parts are real strengths:

### 1. The storage engine is not fake

The LSM core is coherent.
The ordering primitive in internal key encoding drives:

- memtable order
- SST order
- merge iteration
- MVCC visibility

The read path is understandable and technically defensible:

- active memtable
- frozen memtables
- overlapping L0 newest-first
- sorted non-overlapping lower levels
- inherited branch layers

This is the strongest part of the database.

### 2. The commit protocol has a real spine

The actual commit path is not hand-wavy.
It does something serious:

- snapshot selection
- branch lock
- read-set validation or blind-write fast path
- version allocation
- WAL append
- storage apply
- visible-version advance

That is a respectable embedded database commit protocol.

### 3. Copy-on-write branch storage is technically strong

The storage-level branch model is a real design, not just a marketing layer:

- inherited layers
- fork version boundary
- materialization path
- COW-aware diff shortcuts

This is one of the more interesting pieces of the system.

## What Is Fundamentally Wrong

This is where the audit gets harsh.

### 4. There is no single authoritative runtime

This is the biggest architectural problem in the codebase.

Strata does not have one hard canonical runtime path.
It has overlapping authority spread across:

- `executor`
- `engine`
- `concurrency`
- `durability`
- subsystem hooks
- global registrations
- feature-gated alternate paths

As a result, "what Strata does" depends on:

- how it was opened
- which crate opened it
- whether it was opened first in-process
- what features were compiled
- what global hooks were registered beforehand

That is not runtime architecture maturity.
That is partial migration plus authority drift.

### 5. Recovery is still split across incompatible designs

The codebase contains a cleaner durability architecture:

- `MANIFEST`
- checkpoint/snapshot coordinator
- recovery coordinator
- codec-aware persistence plan

But the live engine open path still primarily uses the older recovery spine.

The result is bad in exactly the way architecture drift is bad:

- the cleaner path exists but is not authoritative
- checkpoint restore is not the normal open path
- on-disk layout assumptions differ between stacks
- codec handling differs between stacks
- the product looks more complete than the runtime actually is

This is classic zombie-migration debt.

### 6. The control plane is partially real and partially theatrical

A lot of knobs exist.
Not all of them meaningfully control the live runtime.

Examples:

- maintenance and GC surfaces exist, but some storage entrypoints are stubs
- TTL infrastructure exists, but expiry reclamation is incomplete
- Standard WAL sync knobs drifted away from implementation truth
- write-stall settings exist, but safe defaults are effectively disabled
- `background_threads` sounds broad, but much of the system still does not respect it
- telemetry exists as a surface, not as a real subsystem

This means parts of the control plane are user-facing promises without equivalent runtime authority.

### 7. Lifecycle ownership is not tight enough

Shutdown is not a full close barrier.
`Drop` still matters for finalization.
Background work can outlive parts of shutdown sequencing.
Reopen behavior can still interact badly with registry-held instances.

That is not a cosmetic flaw.
Lifecycle looseness is where durable systems leak weirdness:

- stuck state
- incomplete freeze
- stale shared instances
- shutdown/reopen races

### 8. Follower mode is not trustworthy enough

Follower mode is a separate runtime architecture, not just a read-only primary.
That is fine in principle.
The problem is that it is still best-effort in ways a replica path should not be.

The sharpest finding was this:

- `refresh()` can advance the watermark past skipped records

That means permanent divergence is possible.
For a replication path, that is a severe architectural seam.

### 9. Branching semantics are stronger at storage level than at runtime level

The storage model for branching is better than the branch product.

At the runtime/semantic layer:

- merge-base correctness is not fully engine-owned
- branch identity is split across names, deterministic IDs, metadata UUIDs, and DAG overlays
- branch DAG is not authoritative lifecycle truth
- status tracking is observational, not enforced
- some branch mutation paths are much weaker than others
- repeated merges and recreated names can contaminate ancestry logic

So the branching feature is real, but not yet governed tightly enough to justify Git-like confidence.

### 10. Secondary primitives are not clean peers

The system exposes multiple primitive families, but they do not all have the same architectural quality.

Roughly:

- JSON: healthiest
- Event: clean but semantically narrow
- Graph: decent core data path, weaker overlay semantics
- Search: derived overlay, not primary truth
- Vector: the most composition-sensitive and migration-drifted primitive

Vector in particular showed a familiar pattern:

- real KV truth
- real subsystem recovery
- extra hook-driven runtime behavior
- mmap/cache adjunct state
- dormant vector WAL
- dormant vector snapshot stack

That is not a finished primitive architecture.

## The Most Damaging Structural Theme

### 11. Strata is full of shadow authority

This theme kept recurring in every stage.

The codebase repeatedly contains:

- a live path
- a cleaner alternate path
- a global registration seam
- a builder-only seam
- a feature-gated variant
- a policy-layer wrapper

Instead of one place being authoritative.

This is why the database feels less reliable than the storage engine itself.
The kernel is stronger than the runtime shell around it.

## Security Verdict

### 12. The current security model is modest, local, and narrower than the surface suggests

Strata does not currently provide a strong general security architecture.

What it really has is:

- same-machine trust
- mostly same-user trust
- Unix permission hardening
- policy-layer access mode
- best-effort secret wrappers

What it does not really have:

- authenticated IPC
- strong multi-client authz separation
- engine-global read-only guarantees
- coherent encrypted durable storage
- universal secret redaction invariants

That is acceptable for a local embedded database if stated honestly.
It is not acceptable if implied otherwise.

## Feature-Matrix Verdict

### 13. Different builds are meaningfully different products

This is not just optional functionality.

Compile features change:

- whether AI paths exist
- whether AI paths fail loudly or degrade quietly
- whether config keys have durable meaning
- whether provider declarations are valid in practice
- whether product behavior changes after reopen
- what the license footprint of the built artifact is

This means Strata does not currently have one stable product identity across builds.

## Ruthless Product Summary

### 14. What Strata is today

Strata today is:

- a serious embedded MVCC LSM database kernel
- with ambitious branch/versioning ideas
- with multiple interesting higher-level primitives
- wrapped in a runtime that still looks like an unfinished consolidation effort

### 15. What Strata is not yet

Strata is not yet:

- a fully consolidated database runtime
- a cleanly authoritative durability architecture
- a replication system you should trust blindly
- a uniformly secure product surface
- a feature-stable product across builds
- a system whose control plane always matches its implementation

### 16. The harshest honest sentence

**Right now, Strata is closer to an impressive database core inside a vibecoded product shell than to a finished database product.**

That is not an insult.
It is the pattern the audit actually revealed:

- strong local ideas
- real technical skill in key subsystems
- weak consolidation
- weak runtime governance
- too many "almost-authoritative" layers

## The Biggest Discoveries

If I had to reduce the entire audit to the most important discoveries, they are these:

1. The kernel is stronger than the product.
2. Runtime authority is split and first-opener/feature/composition sensitive.
3. Recovery and durability are still mid-migration.
4. Follower mode is not safely convergent.
5. Branch semantics are partly real storage semantics and partly overlay behavior.
6. Vector had clear zombie durability stacks.
7. The control plane overpromises relative to live implementation.
8. Security is mostly local hardening and policy enforcement, not strong isolation.
9. The feature matrix changes both behavior and licensing in important ways.
10. The database is salvageable, but it should not keep expanding its surface before it consolidates its runtime.

## Final Bottom Line

The most accurate final statement I can make is:

**Strata is not irreparably broken, but it is internally under-consolidated enough that many of its higher-level claims currently outrun its runtime truth.**

If the team freezes surface expansion and consolidates around:

- one runtime spine
- one durability architecture
- one branch identity model
- one honest security model
- one explicit feature matrix

then the project is salvageable and could become strong.

If it keeps adding features on top of the current authority drift, it will get harder to trust, harder to debug, and harder to honestly describe.
