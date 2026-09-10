# Strata V1 Error And Diagnostics Contract

Status: current — describes shipped 1.2.x behaviour (#3134)

## Purpose

Strata V1 needs errors that are understandable to users, stable for SDKs and
automation, precise enough for tests, and still honest about low-level storage,
recovery, backend, inference, and IPC failures.

This document defines the V1 error handling contract:

1. Which layer owns each kind of error.
2. Which error concepts are stable product contracts.
3. How low-level failures become public database errors.
4. How retryability and ambiguous commit outcomes are represented.
5. How diagnostics are exposed without leaking implementation history.
6. What the testing plan must verify.

It is a bridge between the V1 architecture documents and the coming testing and
conformance plan.

## Related Documents

1. `docs/architecture/strata-v1-architecture.md`
2. `docs/architecture/core-architecture.md`
3. `docs/architecture/storage-architecture.md`
4. `docs/architecture/storage/l9-storage-api-boundary.md`
5. `docs/product/strata-v1-non-functional-requirements.md`
6. `docs/core/archive/error-research.md`
7. `docs/core/archive/core-error-review.md`
8. `docs/engine/archive/engine-error-architecture.md`

The historical error documents remain useful evidence. This document is the V1
target contract.

## Goals

1. Make every user-visible failure machine-readable.
2. Keep public error codes stable across crate rewrites.
3. Preserve source chains for debugging and support.
4. Make retry and ambiguous-commit semantics explicit.
5. Keep storage errors mechanical and product-agnostic.
6. Keep engine errors semantic and product-facing.
7. Keep command, IPC, CLI, and SDK errors serializable.
8. Support reference-grade testing without asserting on prose messages.
9. Avoid one-off error types that exist only for a single narrow call path.

## Non-Goals

1. This document does not freeze exact Rust enum variants.
2. This document does not define a wire protocol.
3. This document does not require numeric error codes for V1.
4. This document does not put a universal database error in `core`.
5. This document does not make display strings compatibility contracts.
6. This document does not require every internal helper to define its own error
   enum.

## Current Implementation Evidence

The current codebase already points in the right direction:

1. `strata-engine` owns `StrataError` and a coarse `ErrorCode` enum.
2. `strata-storage` owns `StorageError` and storage-local recovery, corruption,
   publish, and backend failures.
3. `strata-executor` owns a serializable public command error.
4. `strata-inference`, vector, search, and bundle modules own local errors that
   are translated upward.
5. Historical docs already concluded that `StrataError` should be engine-owned,
   storage should not know engine errors, and `core` should remain nearly
   error-free.

The current error surface is too uneven to freeze as V1:

1. Some codes are too coarse for automation.
2. Some errors are stringly typed.
3. Some conversion paths lose source context.
4. Retryability and ambiguous commit state are not consistently represented.
5. Tests often must assert on messages instead of stable codes.

V1 should preserve the ownership shape, but replace the ad hoc details with a
small repeatable contract.

## Layer Ownership

### Core

Core should own only type-local validation and parse errors for core-owned
types.

Examples:

1. Invalid branch ID encoding, if `BranchId` remains core-owned.
2. Invalid timestamp representation, if timestamp parsing is core-owned.
3. Invalid transparent newtype representation.

Core must not own:

1. `StrataError`.
2. `StorageError`.
3. Backend capability errors.
4. IPC errors.
5. Search, graph, vector, intelligence, or inference errors.
6. A global database error taxonomy.

Core may eventually own a tiny shared representation type only if both
storage and engine must serialize the exact same value across a lower
boundary. The default is to keep broad error vocabulary out of core.

### Storage

Storage owns mechanical persistence failures.

Storage errors may describe:

1. Invalid storage configuration.
2. Unsupported storage mode.
3. Backend capability mismatch.
4. Backend unavailable.
5. Permission denied at the backend.
6. Writer lock or lease conflict.
7. Object not found or already exists.
8. Publish precondition failed.
9. Publish failed before visibility.
10. Publish visibility unknown.
11. Publish visible but durability unconfirmed.
12. Codec initialization or codec mismatch.
13. Unsupported durable format.
14. Corrupt storage metadata.
15. Corrupt WAL.
16. WAL partial tail.
17. Corrupt table.
18. Corrupt snapshot.
19. Recovery failed or degraded.
20. Lossy recovery used.
21. Commit conflict at the storage row boundary.
22. Durable-but-not-visible commit state.
23. Retention proof incomplete.
24. Maintenance failed.
25. Internal storage invariant violation.

Storage must not decide:

1. Whether a failure is a user-facing product error.
2. How to phrase the error for CLI or SDK users.
3. Whether a graph, vector, JSON, event, search, or recipe operation is valid.
4. Whether a product command should retry.
5. Whether old pre-V1 databases should be migrated.

Storage should expose enough structured facts for engine to make those
decisions.

### Engine

Engine owns the parent database error.

Engine errors represent product and database semantics:

1. Key, document, branch, collection, stream, graph, vector, or index not found.
2. Already-exists conflicts.
3. Invalid user input.
4. Read-only write attempts.
5. Wrong type or wrong space.
6. Branch, merge, restore, and version conflicts.
7. History unavailable because retention removed it.
8. Backend unsupported for the selected runtime mode.
9. Corruption and recovery failures after interpreting storage facts.
10. Ambiguous commit outcomes.
11. Search/index/model availability failures translated into database terms.
12. Internal engine invariant violations.

Engine is the first layer allowed to turn storage mechanics into product
meaning.

### Intelligence

Intelligence owns retrieval orchestration errors:

1. Recipe validation failures.
2. Query expansion failures.
3. Reranking failures.
4. RAG stage failures.
5. Retrieval-plan capability mismatch.
6. Provenance or explanation construction failures.

It should translate engine and inference errors without hiding their code,
class, retryability, or commit outcome.

### Inference

Inference owns provider and model execution errors:

1. Provider unavailable.
2. Model not installed or not configured.
3. Network disabled by policy.
4. Tokenization failure.
5. Generation failure.
6. Embedding failure.
7. Provider response invalid.
8. Local runtime failure.

Inference should not define database semantics. Intelligence or
engine decides how provider failures affect a product command.

### Executor, IPC, CLI, SDK, And Strata AI

The command boundary owns serializable error status.

Executor and IPC should expose structured errors that include:

1. Stable error class.
2. Stable error code.
3. Retry policy.
4. Commit outcome.
5. Redacted message.
6. Optional details and hints.
7. Optional trace or request ID.

CLI and Strata AI render those facts. They should not invent a separate error
taxonomy.

## Stable Concepts

V1 should standardize only a small set of error concepts.

### Error Class

`ErrorClass` is the stable high-level category used by SDKs, CLI, IPC,
automation, and tests.

Initial V1 classes:

| Class | Meaning |
| --- | --- |
| `not_found` | A requested object, branch, key, document, model, provider, or backend object does not exist. |
| `already_exists` | A create operation targeted an object that already exists. |
| `invalid_argument` | The caller supplied malformed input or an invalid option. |
| `failed_precondition` | The request is well-formed, but the current database state or mode does not allow it. |
| `access_denied` | The caller or backend lacks permission for the requested operation. |
| `conflict` | The request conflicts with concurrent state, branch state, version state, or merge rules. |
| `ambiguous_commit` | The system cannot prove whether a write commit became durable or visible. |
| `history_unavailable` | Requested history has been pruned, compacted away, or is outside retention. |
| `unsupported` | The feature, backend, runtime mode, format, provider, or capability is not supported. |
| `resource_exhausted` | A size, capacity, quota, memory, disk, or configured limit was exceeded. |
| `unavailable` | A required local service, backend, provider, lock, IPC endpoint, or model is temporarily unavailable. |
| `io` | A storage or filesystem IO operation failed without stronger semantic classification. |
| `corruption` | Durable state, metadata, WAL, table, snapshot, or provider output violates integrity expectations. |
| `data_loss` | Durable engine state that should exist cannot be reconstructed — a stored record failed to decode or a required artifact is gone. Distinct from `corruption` (integrity violation detected) in that the data is unrecoverable, not merely inconsistent. |
| `serialization` | Encoding, decoding, schema, format, or protocol conversion failed. |
| `internal` | A Strata invariant failed or unreachable state was reached. |

Classes are intentionally few. They are broad enough for user pathways and
stable enough for automated handling.

V1 intentionally does not define `cancelled` or `deadline_exceeded` classes.
User-cancellable operations and user-supplied deadlines are not V1 product
surface yet. If they are added later, they should get explicit classes rather
than being folded into `unavailable`.

### Error Code

`ErrorCode` is the stable detailed identifier.

V1 should use stable ASCII string codes as the primary public contract. Numeric
codes may be generated later from the registry, but they are not required for
V1.

The code tables in this document are a starter set. The full public code
registry must become its own document or spec before V1 implementation freezes.

Code format:

```text
<class>.<area>.<detail>
```

Examples:

```text
not_found.engine.branch
not_found.engine.vector_collection
already_exists.engine.branch
invalid_argument.engine.kv_key
failed_precondition.engine.runtime_closed
ambiguous_commit.lifecycle.flush_publication
history_unavailable.storage_api.retained
unsupported.storage_api.capability
resource_exhausted.storage_api.memory_budget
unavailable.engine.persistence
io.lifecycle.backend
corruption.lifecycle.recovery
serialization.lifecycle.format
internal.storage_api.commit
```

Rules:

1. Error codes are stable once V1 is released.
2. Error codes are lowercase ASCII.
3. Error codes are not display messages.
4. Error codes must belong to exactly one class.
5. Error codes must be documented in a registry before implementation freezes.
6. Tests assert on codes and classes, not prose messages.
7. New codes require a product or architecture reason.
8. `internal.*` codes should be rare and treated as bugs.

### Error Surface (Stripe-Grade Fields)

Every public failure is rendered to a caller as six fields. They are not all
owned by the same layer; this is the contract for which layer produces which
field.

1. **Typed error class** — `ErrorClass`. Owned by the failing layer.
2. **Error code** — `<class>.<area>.<detail>`. Owned by the failing layer;
   registered.
3. **Human-readable message** — owned in two parts. The failing layer (e.g.
   storage) produces a *mechanical* message describing the technical failure.
   Engine and the SDK produce the *user-facing* message. Storage must
   not phrase for end users (see Layer Ownership).
4. **Suggested fix (remediation)** — owned in two parts. The failing layer
   produces a *mechanical remediation hint* (`StorageApiError::remediation()`):
   a storage/engine instruction, never product or end-user phrasing, never
   secrets. Engine/SDK translate it into user-facing guidance.
5. **Reference ID** — an opaque token that ties a user-visible error to internal
   logs. It is **assigned at the boundary/log sink, not at error construction**,
   from an injected id source (the same injectable-source discipline as the
   maintenance clock). The error value itself stays pure, `Clone`, and
   deterministic; the boundary mints one id, writes it into both the rendered
   status and the correlated internal log line. A deterministic id source must
   be available so simulation/replay (deterministic-simulation testing) stays
   reproducible. Construction-time random ids are prohibited because they break
   replay and the pure-value error shape.
6. **Doc link** — derived from the code at the boundary
   (`<docs-base>/errors/<code>`), not stored on the error value. The code is the
   stable doc anchor; codes must be URL-path-safe.

Layer responsibilities for the surface:

1. Storage owns fields 1, 2, the mechanical half of 3, and the mechanical
   half of 4. It owns no reference id, no doc URL, and no user-facing phrasing.
2. Engine composes product meaning: the user-facing message. The error-code
   registry (engine rows in `crates/engine/src/diagnostics/registry.rs`,
   executor-owned rows such as `inference.*` in
   `crates/executor/src/error_registry.rs`) is the single authority for a
   code's class, retry policy, commit outcome and suggested fix: the row
   `strata agents errors` documents is the row a live error for that code
   carries — always, not only when the site left a field at its default. A
   construction site owns the code, the message, `details` and `hints`; it
   has no channel to override the row (#3244 at the executor boundary,
   #3280 in engine: `EngineError` constructors take a code and a message,
   and `EngineErrorStatus::new` is crate-private), so site-specific
   remediation goes in `hints`, never in a per-code table of its own
   (#3237, #3241, #3243).
   A wrong row is fixed in the registry (#3275), not hidden at the site.
3. The boundary (command/IPC/SDK/CLI status renderer) resolves the code to
   its row, assigns the reference id, derives the doc link from the code, and
   emits the full status. A status arriving from a peer (IPC, a stale build)
   is re-resolved against the local registry the same way; only the peer's
   message, ids, details and hints survive the boundary.

### Retry Policy

Retryability is not a property users should infer from messages.

Initial V1 retry policy:

| Policy | Meaning |
| --- | --- |
| `never` | Retrying the same request without changing input or state should not help. |
| `after_state_change` | Retry may work after changing configuration, branch state, backend state, model setup, or permissions. |
| `same_request` | Retrying the exact same request is safe and may succeed. |
| `idempotent_only` | Retry is safe only when the operation is idempotent or carries an idempotency key. |
| `unknown` | Strata cannot safely classify retryability. This should be uncommon. |

Examples:

1. `invalid_argument.key` uses `never`.
2. `failed_precondition.read_only` uses `after_state_change`.
3. `unavailable.ipc_socket` uses `after_state_change` because the IPC server or
   socket state usually needs to change before retrying.
4. `ambiguous_commit.wal_publish` uses `unknown` by default in V1. It may use
   `idempotent_only` only if the specific command has an idempotency key or is
   otherwise proven idempotent by engine.
5. `internal.engine_invariant` uses `unknown`.

### Commit Outcome

Write-path failures need a separate commit outcome. Retryability alone is not
enough.

Initial V1 commit outcomes:

| Outcome | Meaning |
| --- | --- |
| `not_applicable` | The failed operation did not attempt a commit. |
| `not_started` | Validation failed before commit machinery began. |
| `definitely_not_committed` | Commit machinery began, but Strata can prove no commit became visible or durable. |
| `maybe_committed` | Strata cannot prove whether the commit became durable or visible. |
| `committed_post_commit_failed` | The commit succeeded, but a post-commit action failed. |

`maybe_committed` is a first-class database outcome. It must not be collapsed
into `io`, `timeout`, or `internal`.

V1 should expose `maybe_committed` for failures such as:

1. WAL or manifest publish windows where visibility is unknown.
2. Object backend conditional publish responses that time out after the backend
   may have accepted the write.
3. IPC write commands where the server may have committed before the connection
   failed.

### Diagnostic Status

Every public boundary should be able to serialize an error status with this
conceptual shape:

```text
ErrorStatus {
  class,
  code,
  retry_policy,
  commit_outcome,
  message,
  details,
  hints,
  trace_id,
}
```

The exact Rust type and wire encoding belong in the command boundary contract.
The fields above are the minimum semantic contract.

`trace_id` is optional. If no caller-provided request ID exists, the command or
IPC boundary may generate an opaque local ID for logs and support. V1 does not
require OpenTelemetry or cross-process trace propagation.

`message` is for people. It should be short, redacted, and useful. It is not a
stable API contract.

`details` are structured facts. They may include:

1. Branch ID or branch name.
2. Key or path, when safe to display.
3. Storage space ID.
4. Version or timestamp.
5. Backend kind.
6. Backend address with credentials redacted.
7. Missing backend capability.
8. Object role, such as WAL, manifest, table, snapshot, or lock.
9. Recovery health class.
10. Provider or model name.
11. IPC endpoint.
12. Limit name and observed value.

`hints` are optional and user-facing. They should explain what the user can do,
not how internal machinery is built.

## Code Families

The concrete, authoritative registry of public error codes is
**`crates/executor/idl/v1/errors.yaml`** — the generated/authored list of every
`<class>.<area>.<detail>` code surfaced to SDK docs, enforced by the IDL drift
guard (`uncovered-error-codes.yaml`: every registered code is declared in
`errors.yaml` or explicitly listed). This document owns the error *classes*
(above) and the *format*; it does not maintain a second, hand-written code list.

> Historical note: earlier drafts of this document carried a hand-authored
> "starter set" of two-part codes (`not_found.key`, `conflict.branch_merge`, …).
> That set never matched emitted code — the real codes are three-part
> (`not_found.engine.branch`) and live in `errors.yaml`. It has been removed
> (TCP3.5a, issues #2633/#2634) rather than rewritten, because a parallel
> hand-maintained registry re-drifts the moment code changes. Codes that the
> starter set listed but nothing emits (`conflict.branch_merge`,
> `conflict.branch_revert`, `conflict.branch_cherry_pick`,
> `failed_precondition.retention_window`) are not part of V1 — branch merge is
> absent in V1 (CLAUDE.md rule 20), and event retention windows are
> unimplemented.

Every emitted code's `<class>` segment is one of the error classes declared
above. This is mechanically enforced by the engine class-parity guard
(`crates/storage/tests/error_contract_class_parity_guard.rs`), which fails if a
code uses a class this document does not declare, or if this document declares a
class no code uses.

## Mapping Rules

### Storage To Engine

Storage-to-engine mapping must be explicit.

Rules:

1. Do not use blanket `From<StorageError> for StrataError` for write-path phases
   where commit outcome matters.
2. Blanket read-path conversion is acceptable only when it preserves the source
   error and cannot lose class, code, retry policy, or operation context.
   Otherwise read-path mapping should also be explicit.
3. Include operation phase when mapping storage publish, sync, manifest, WAL,
   snapshot, table, or recovery errors.
4. Preserve the storage source error.
5. Preserve object role and object name where useful.
6. Map backend capability mismatch to `unsupported.backend_capability`.
7. Map writer lock conflict to `unavailable.writer_lock` unless engine can
   classify it as a product access-mode failure.
8. Map corruption to `corruption.*`, not `io.*`.
9. Map unknown durable publish outcome to `ambiguous_commit.*`.
10. Map validation-before-mutation failures to `not_started`.
11. Map post-commit maintenance failure to `committed_post_commit_failed` when
    the user write already succeeded.

Storage errors should not contain product wording. Engine decides how to explain
them.

### Engine To Command Boundary

Engine-to-executor and engine-to-IPC mapping must be explicit.

Rules:

1. Preserve `class`, `code`, `retry_policy`, and `commit_outcome`.
2. Do not expose raw storage enum variant names as public codes.
3. Do not expose credentials, access tokens, provider secrets, or full URLs.
4. Include stable detail fields where they help automation.
5. Keep `source` chains available for logs and diagnostics, not necessarily for
   normal JSON responses.
6. Treat unknown future engine codes as `internal.command_invariant` only at the
   boundary and log enough detail to fix the mapping.

### Inference And Intelligence Mapping

Inference errors should remain provider-local until intelligence or engine turns
them into product outcomes.

Examples:

1. Missing local embedding model becomes `failed_precondition.embedding_not_available`.
2. Disabled network provider becomes `failed_precondition.network_disabled`.
3. Provider timeout may become `unavailable.model_provider`.
4. Invalid provider JSON becomes `serialization.provider_response`.

The database should not make a model-provider failure look like storage
corruption.

The global V1 registry should reserve these starter inference codes:

| Code | Class | Retry policy |
| --- | --- | --- |
| `inference.invalid_request` | `invalid_argument` | `never` |
| `inference.unknown_model` | `not_found` | `never` |
| `inference.missing_model` | `failed_precondition` | `after_state_change` |
| `inference.model_load_failed` | `failed_precondition` | `after_state_change` |
| `inference.unsupported_provider` | `unsupported` | `never` |
| `inference.unsupported_operation` | `unsupported` | `never` |
| `inference.unsupported_parameter` | `invalid_argument` | `never` |
| `inference.missing_api_key` | `failed_precondition` | `after_state_change` |
| `inference.provider_auth_failed` | `access_denied` | `after_state_change` |
| `inference.provider_rate_limited` | `unavailable` | `after_state_change` |
| `inference.provider_quota_exhausted` | `failed_precondition` | `after_state_change` |
| `inference.provider_model_not_found` | `not_found` | `never` |
| `inference.provider_timeout` | `unavailable` | `same_request` |
| `inference.provider_unavailable` | `unavailable` | `same_request` |
| `inference.provider_malformed_response` | `serialization` | `unknown` |
| `inference.download_disabled` | `failed_precondition` | `after_state_change` |
| `inference.download_failed` | `unavailable` | `same_request` |
| `inference.download_verification_failed` | `corruption` | `after_state_change` |
| `inference.local_runtime_failed` | `unavailable` | `unknown` |
| `inference.registry_corrupt` | `corruption` | `never` |
| `inference.io_failure` | `io` | `unknown` |

A refusal raised at model resolution carries the resolver's answer as data.
`strata_inference::AvailabilityDetails` is the definition of
`strata.error.details.inference.v1`: the executor flattens it into the
envelope's `details`, one entry per field keyed by the field's own name, and
adds `collection` when the spec came from a vector collection's record. An
inference error raised past resolution (a provider response, a corrupt
registry, an I/O failure) carries no such details. A client acts on
`details.availability` and `details.pull_spec`, never on the message and never
on a code that could mean two things (`inference.unknown_model` is "not a model
this binary knows"; `inference.missing_model` is "catalogued, not downloaded").
The executor's `inference_resolution_wire` test pins the pass-through for every
resolution cell.

Raw provider response bodies, raw llama.cpp messages, native pointer details,
and full prompt or document content are diagnostics context only. They must not
become public error codes or default user-facing messages. A provider's error
body is still *read*: its structured discriminators (`error.type`, `error.code`,
`error.status`, `error.details[].reason`) decide the code ahead of the HTTP
status — a 429 for an empty balance is `provider_quota_exhausted`, not
`provider_rate_limited` — and its `error.message` sentence, redacted, is the
concise user-facing message. The body as a whole is never surfaced.

### CLI And Strata AI Rendering

CLI and Strata AI should render structured errors with three layers:

1. A concise user-facing message.
2. A code and class for copying into docs, support, automation, or tests.
3. Optional details and hints.

They should avoid explaining WAL, manifests, checkpoints, memtables, table
blocks, compaction, subsystem consolidation, or crate history unless the user
explicitly asks for diagnostic detail.

## Source Chains

Every public error type should implement standard source chaining where useful.

Rules:

1. Use `thiserror` for library errors unless a later implementation plan proves
   another approach is better.
2. Use `anyhow` only at application edges, tests, tools, or CLI internals where
   typed handling is not required.
3. Prefer explicit `#[source]` fields over casual `#[from]` on parent errors.
4. Avoid rendering the same source once in `Display` and again through
   `source()`.
5. Keep error types `Send + Sync + 'static` unless an implementation plan
   records a specific reason not to.
6. Public error enums should be `#[non_exhaustive]`.

Source chains are for debugging and support. Codes, classes, retry policy, and
commit outcome are for contracts.

## Redaction

Error details must be redacted by default.

Never expose:

1. Database credentials.
2. Provider API keys.
3. Signed URLs.
4. Bearer tokens.
5. Access key IDs and secrets.
6. Full environment variable values.
7. User content values unless the API explicitly returns that content.

Allowed when useful:

1. Redacted backend address.
2. Object role.
3. Object basename or stable object ID.
4. Branch ID or name.
5. Key or path when the user supplied it and the command result normally
   exposes it.
6. Provider or model name.
7. Limit names and numeric limits.

Redaction tests are mandatory for command, IPC, CLI, SDK, and Strata AI
surfaces.

## Type Shape Guidance

V1 should prefer a small number of repeated type shapes:

1. `Error`: layer or service parent error.
2. `ErrorKind` or `ErrorClass`: stable category inside a layer when needed.
3. `ErrorCode`: stable detailed public identifier.
4. `ErrorStatus`: serializable public boundary shape.
5. `ErrorContext`: structured details carried during mapping.

Avoid creating:

1. One enum per helper function.
2. One-off `FooFailure`, `FooProblem`, `FooIssue`, and `FooFault` types for the
   same concept.
3. Error variants that differ only by display wording.
4. Variants that carry telemetry ignored by every caller.
5. Catch-all `Internal` variants for ordinary input, backend, capability, or
   conflict errors.

Layer-local helper errors are fine when they protect a real boundary. They
should collapse into the layer parent before crossing outward.

## Testing Plan Inputs

The testing and conformance plan must include these error tests.

### Registry Tests

1. Every public error code is listed in the registry.
2. Every listed code maps to exactly one class.
3. Every listed code has a retry policy default.
4. Every listed code documents whether commit outcome can vary by operation
   phase.
5. No implementation emits an unregistered public code.
6. No test asserts on display text when it can assert on code and class.

### Type Tests

1. Public error enums are `#[non_exhaustive]`.
2. Public error types preserve source chains.
3. Public error types are `Send + Sync + 'static` where required.
4. Display output is non-empty and redacted.
5. Structured details serialize and deserialize through the command boundary.

### Error Surface Contract Tests

Every public error type must have an exhaustive per-variant contract test that
asserts the layer-owned half of the Stripe-grade surface. For storage this
is `crates/storage/tests/api_error_contract.rs`:

1. A fixture samples every variant; a count backstop plus the lib's exhaustive
   `code()`/`class()`/`remediation()`/`Display` matches force a new variant to be
   handled before it can ship.
2. Each code is `<class>.<area>.<detail>`, lowercase snake_case, three segments,
   URL-path-safe (doc-linkable).
3. Each code's class prefix agrees with `class()`.
4. The mechanical message is non-empty and redaction-clean.
5. The mechanical remediation hint is non-empty, redaction-clean, and carries no
   product/end-user phrasing.
6. Source-bearing variants preserve their source chain.

The boundary layer (engine/SDK status renderer) owns the complementary test:
reference id is assigned from the injected id source, the same id appears in the
status and the log line, the deterministic id source replays identically under a
fixed seed, and the doc link is derived from the code.

### Mapping Tests

1. Storage backend errors map into the expected engine codes.
2. Storage corruption errors map to `corruption.*`.
3. Storage capability errors map to `unsupported.backend_capability`.
4. Storage publish uncertainty maps to `ambiguous_commit.*`.
5. Engine errors map into executor and IPC statuses without losing class, code,
   retry policy, or commit outcome.
6. Inference provider errors map into intelligence or engine statuses without
   looking like storage failures.

### Fault-Injection Tests

1. Failed read.
2. Failed write before visibility.
3. Failed write after possible visibility.
4. Failed durable sync.
5. Failed manifest publish.
6. Failed WAL append.
7. Partial WAL tail.
8. Corrupt WAL record.
9. Corrupt table block.
10. Corrupt snapshot.
11. Stale object metadata.
12. Writer lock conflict.
13. IPC disconnect during write.
14. Provider timeout.
15. Provider invalid response.

Each test should assert:

1. Error class.
2. Error code.
3. Retry policy.
4. Commit outcome.
5. Source chain presence when applicable.
6. Redaction.

### Fuzz And Parser Tests

Fuzz targets should verify:

1. Invalid durable bytes never panic.
2. Invalid durable bytes return `serialization.*`, `corruption.*`, or
   `unsupported.*` as appropriate.
3. Huge declared lengths do not allocate unbounded memory.
4. Trailing bytes are either rejected with a typed code or explicitly accepted
   by the format spec.
5. Command payloads reject malformed input with stable codes.

### Crash-Recovery Tests

Crash-recovery tests should verify:

1. Committed data remains committed.
2. Rejected writes remain absent.
3. Ambiguous publish windows surface `maybe_committed`.
4. Recovery health facts are available as diagnostics.
5. Lossy recovery, if enabled in developer tooling, never masquerades as normal
   success.

## Documentation Requirements

Before V1 implementation freezes, Strata should publish:

1. The public error class list.
2. The public error code registry.
3. Retry policy definitions.
4. Commit outcome definitions.
5. Redaction rules.
6. SDK handling examples.
7. CLI examples.
8. Testing requirements for new error codes.

The durable storage format spec should reference storage-specific codes only
where format readers need deterministic failure behavior.

## Acceptance Criteria

This contract is satisfied when:

1. Core contains no broad database error.
2. Storage exposes mechanical storage errors with source chains and
   structured facts.
3. Engine owns the parent product database error.
4. Executor, IPC, CLI, SDK, and Strata AI consume stable statuses instead of
   parsing messages.
5. Every public failure has a stable class and code.
6. Retryability is explicit.
7. Ambiguous commit state is explicit.
8. Sensitive details are redacted.
9. Fault-injection tests assert codes, not prose.
10. The testing plan can derive its error conformance matrix from this document.

## Open Questions

These questions should be resolved before V1 implementation freezes:

1. Does the final public wire status use exactly `ErrorStatus`, or does the
   command boundary choose a different name?
2. Should error codes remain string-only for V1, or should the registry also
   assign numeric IDs before public release?
3. Which detail keys are stable public keys versus best-effort diagnostic keys?
4. Does IPC expose source-chain summaries to trusted local clients, or only
   trace IDs?
5. Which recovery health vocabulary is public product surface and which remains
   storage diagnostic detail?

## Resolved Decisions

1. **Reference id generation** (was implied by open question 4). Resolved:
   reference ids are assigned at the boundary/log sink from an injected id
   source, never minted at error construction. See "Error Surface (Stripe-Grade
   Fields)". This keeps error values pure and deterministic and ties the
   user-visible id to internal logs. A deterministic id source must exist so
   deterministic-simulation testing replays identically.
