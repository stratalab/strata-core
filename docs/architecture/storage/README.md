# Storage Layer Documents

Status: current — describes shipped 1.2.x behaviour (#3134) index

## Purpose

This directory contains concrete design notes for each storage layer.

The top-level map lives in
[../storage-architecture.md](../storage-architecture.md). That file
defines the layer order and ownership boundaries. The files in this directory
define each layer in enough detail to guide implementation.

These documents are not required to follow a rigid template. They should stay
practical and answer the questions that matter for each layer.

## Layer Documents

Recommended order:

1. [L1. Backend IO](./l1-backend-io.md)
2. [L2. Object Layout](./l2-object-layout.md)
3. [L9. Storage API Boundary](./l9-storage-api-boundary.md) as an initial boundary sketch
4. [L3. Durable Format / Codec](./l3-durable-format-codec.md)
5. [L4. Log / Manifest / Snapshot Services](./l4-log-manifest-snapshot-services.md)
6. [Consistency And Implementation Patterns](./implementation-patterns.md)
7. [Target Crate Shape And Test Harness](./target-crate-shape-and-test-harness.md)
8. [Storage Space ID Registry](./storage-space-id-registry.md)
9. [Commit Timeline Substrate](./commit-timeline-substrate.md)
10. [L5. Table Runtime](./l5-table-runtime.md)
11. [L6. Branch-Isolated LSM Runtime](./l6-branch-isolated-lsm-runtime.md)
12. [L7. Commit Runtime](./l7-commit-runtime.md)
13. [L8. Lifecycle / Recovery / Maintenance](./l8-lifecycle-recovery-maintenance.md)
14. [L9. Storage API Boundary](./l9-storage-api-boundary.md) final alignment pass
15. [Future Object-Durable And Compute/Storage Separation Guardrails](./future-object-durable-guardrails.md)
16. [Benchmarking Plan](./benchmarking-plan.md)
17. [Test Density Roadmap](./test-density-roadmap.md)
18. [Write Amplification And Flash Endurance](./write-amplification-and-flash-endurance.md)

The ordering is deliberate. Backend IO and object layout determine whether the
rest of storage is genuinely portable. The initial storage API boundary
gives engine a target, and the final alignment pass folds in the L3-L8
layer contracts. The target crate-shape document translates those conceptual
layers into domain modules and reusable test harnesses; the implementation
should not create `l1`, `l2`, or `l3` Rust modules. The storage-space and
timeline documents pin durable byte allocations that would otherwise remain
scattered across L3, L6, L7, L8, and L9.

Runtime resource-profile requirements live in
[../runtime-resource-profile-architecture.md](../runtime-resource-profile-architecture.md).
Storage consumes resolved storage budgets from that architecture; it does
not own host probing or product resource-profile policy.

## Working Checklist

Each layer document should answer:

1. Why does this layer exist?
2. What does it own?
3. What must it not own?
4. What does it expose upward?
5. What does it require downward?
6. How does it fail?
7. How do we test it?
8. What is the V1 minimum?

The checklist is a guardrail, not a format requirement. If a layer needs state
machines, invariants, examples, or backend matrices, include them. If a section
would be filler, omit it.

## Design Rule

A layer document should not introduce an abstraction only for symmetry. Every
concept should be justified by at least one backend, failure mode, test
requirement, or upper-layer contract.

## Backend Forcing Set

The first storage implementation must prove two backend shapes:

1. Browser/cache backend for live demos and WASM-oriented development.
2. Local filesystem backend as the durable reference backend.

The design must also leave room for a later OpenDAL/object-store backend. That
backend is architecture-aware but not V1-blocking. We should not build storage
around POSIX-only assumptions, but we also should not require an OpenDAL stub or
S3 durable mode to complete the first storage rewrite.

If an OpenDAL-backed path is added before it is production-supported, it must
declare its capabilities honestly, and open must fail if the requested storage
mode needs guarantees the backend cannot provide.

The future object-durable guardrail is narrower than an implementation plan: it
exists so M4-L9 work does not leak WAL objects, manifest services, backend
handles, local paths, or publish primitives into engine-facing compute APIs.
