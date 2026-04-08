# Delta Map: Product And Control-Surface Truth

This is the third filled delta map against the target architecture.

Scope:

- public product semantics
- config/control-surface truth
- security boundary truth
- feature/build matrix truth
- executor/CLI/API promises vs engine reality

This map assumes:

- [target-architecture-program.md](/home/anibjoshi/Documents/GitHub/strata-core/research/target-architecture-program.md)
- [delta-map-runtime-durability.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-runtime-durability.md)
- [delta-map-branches-primitives.md](/home/anibjoshi/Documents/GitHub/strata-core/research/delta-map-branches-primitives.md)

## Target Architecture Reference

The relevant target assumptions are:

- product/API layers may shape UX, but not redefine runtime semantics
- every exposed control must map to one tested behavior
- unsupported combinations must fail before use
- secret/security behavior must be invariant across surfaces
- feature flags may add capabilities, but must not silently change product identity
- security model must be explicit and honest

## Gap Table

| Domain | Current authoritative path | Shadow / competing path | Target truth | Gap type | Severity | Disposition | Acceptance test |
|---|---|---|---|---|---|---|---|
| Product open contract | `Strata::open_with` is a policy router over local primary, follower, or IPC client | raw engine and builder opens expose materially different products under similar names | product open contract must describe one explicit backend/mode matrix and delegate into one canonical runtime | authority_split | high | consolidate | user-facing open APIs document and test exact backend/mode outcomes; no hidden bootstrap differences remain |
| Read-only semantics | `AccessMode::ReadOnly` is enforced in executor/session wrappers | raw `Database` handle remains writable; same database can be wrapped differently | access authority is engine/runtime truth; wrappers may narrow it but cannot be the only enforcement layer | security_drift | high | rewrite_boundary | opening a database read-only makes write denial a database/runtime invariant, not just an executor policy |
| `_system_` branch access | local privileged handle bypasses normal guard | IPC backend exposes the same surface but rejects operations because server path is different | privileged/internal branch access is one explicit capability model across all supported backends | semantic_inconsistency | high | consolidate | local and IPC products either both support the privileged path or both reject it explicitly at surface level |
| Config surface truth | `CONFIG SET`, `CONFIGURE SET`, open-time config, and runtime setters all mutate different parts of the system | some controls are live, some open-time only, some stale, some stub-backed | one control-surface classification for every key: live-safe, open-time only, restart required, unsupported | control_plane_drift | critical | consolidate | config reference is generated from code/tests and every key has one validated behavior class |
| Runtime durability control | executor coordinates config update plus runtime setter for durability | engine itself has split persisted-vs-live durability authority | one durability control path updates runtime truth, persistence truth, and observability together | authority_split | high | rewrite_boundary | switching durability through the product surface leaves no stale mode state anywhere in the runtime |
| Maintenance surface | `run_gc()` / `run_maintenance()` exist as public control APIs | storage entrypoints are mostly stubs; surface sounds stronger than implementation | maintenance APIs are either real operational controls or removed/demoted as experimental/manual helpers | control_plane_drift | high | consolidate | maintenance commands either produce measurable effect under tests or are explicitly marked unsupported/non-operative |
| Write-stall / backpressure surface | config advertises RocksDB-like slowdown/stop controls | shipped defaults disable them because live loop can deadlock under compaction throttling | public write-pressure story must match the safe shipped behavior | control_plane_drift | medium | document_or_fix | defaults, docs, and behavior all agree on whether write-stall is active and supported |
| WAL tuning surface | `Standard.batch_size` and `buffered_sync_bytes` remain exposed | live Standard policy is effectively interval-driven only | WAL tuning surface only exposes knobs that still influence the real sync policy | control_plane_drift | high | delete_or_realign | changing an exposed WAL sync knob changes runtime behavior under tests; dead knobs are removed |
| Background-work surface | `background_threads` sounds like a broad concurrency knob | in reality it only sizes the generic scheduler; other background loops ignore it | background-work configuration must describe actual ownership and scope | control_plane_drift | medium | document_or_rework | scheduler, WAL flush, maintenance, and embed work ownership are all explicit and test-backed |
| Secret redaction | `CONFIGURE GET key` masks selected secrets | `CONFIG GET` returns real values; `SensitiveString` serializes raw bytes | secret redaction is a surface invariant, not command-specific policy | security_drift | critical | rewrite_boundary | no supported config/API path returns live secret values unless explicitly documented as privileged/internal |
| Secret storage policy | config file stores plaintext API keys with best-effort file permissions | env overrides are partial and not the only path | one explicit secret policy: plaintext-with-perms, env-only, or external-secret integration | security_drift | critical | document_or_redesign | secret-handling docs, config behavior, and tests all reflect the chosen policy with no hidden exceptions |
| IPC security model | same-user Unix socket permissions are the real boundary | product framing can read like shared-access server mode rather than local trust boundary | IPC is explicitly documented as same-user local control plane unless stronger authz is implemented | security_drift | high | document_or_harden | IPC docs/tests make the trust boundary explicit and no API implies per-client auth that does not exist |
| Feature-gated command surface | some commands fail loudly when features are missing | other AI/search paths silently degrade to passthrough/no-op | unsupported feature-dependent behavior must fail explicitly or be clearly declared as optional best-effort degradation | feature_matrix_drift | high | consolidate | build-matrix tests assert whether each feature-dependent command fails hard or degrades, and docs match exactly |
| Provider config validity | config accepts `anthropic`, `openai`, `google` regardless of compiled support | actual failure appears only when generation starts | provider/model config must be validated against the compiled product at config/open time | feature_matrix_drift | high | fail_fast | unsupported provider config is rejected before first use in every supported build |
| Auto-embed config meaning | `auto_embed` exists in config and runtime surface | non-`embed` opens rewrite it to `false` and persist that downgrade | build variants must not mutate config semantics silently across products | feature_matrix_drift | critical | rewrite_boundary | opening the same data dir with different builds never rewrites semantic config unexpectedly |
| Search AI stack behavior | same search recipe may use expansion/rerank/RAG in one build | another build quietly returns lexical-only or no-answer behavior | search product surface must declare whether AI layers are required, optional, or unavailable in the build | feature_matrix_drift | high | document_or_fail_fast | search behavior matrix is explicit per build and test snapshots show expected outputs by feature set |
| Telemetry surface | telemetry exists in CLI init and config | no live runtime telemetry subsystem exists | user-facing telemetry controls only exist if they map to a real subsystem | control_plane_drift | low | delete_or_implement | either telemetry implementation exists and is tested, or the surface is removed |
| Multi-process feature | durability crate has real `multi_process` coordination primitives | root product/runtime does not expose a corresponding supported mode | feature matrix only includes runtime modes the product actually owns | feature_matrix_drift | medium | delete_or_promote | multi-process mode is either productized with docs/tests or remains internal and unadvertised |
| Licensing/product identity | root package is Apache-2.0 | `search`, `intelligence`, and `inference` are BUSL-1.1 and alter legal footprint under features | published feature matrix includes legal/product-identity consequences of build variants | feature_matrix_drift | medium | document | release/build docs clearly state license footprint changes by enabled feature set |
| CLI / executor promise strength | CLI and executor expose user-facing controls as if they are product truths | engine/runtime often only partially backs them | public surfaces must never promise stronger behavior than the canonical runtime | authority_split | critical | consolidate | each CLI/executor command has a tested mapping to one engine/runtime behavior or is demoted/removed |

## Priority Clusters

## Cluster A: Honest control surface

This cluster fixes:

- config truth classification
- durability control split
- maintenance surface drift
- WAL tuning drift
- background-work ambiguity

### Concrete target

Build one code-owned control-surface registry containing for each public knob:

- owner
- scope
- behavior class
- persistence class
- support status

Then generate docs/tests from it.

## Cluster B: Honest security surface

This cluster fixes:

- read-only being wrapper-only
- `_system_` backend divergence
- command-specific secret redaction
- plaintext secret policy ambiguity
- IPC trust-model ambiguity

### Concrete target

Choose the real security model and make every surface obey it:

- same-user embedded database
- explicit privileged/internal surfaces
- invariant secret-handling policy

Do not let API ergonomics invent stronger guarantees.

## Cluster C: Honest feature matrix

This cluster fixes:

- fail-late provider/config behavior
- silent AI/search degradation
- `auto_embed` cross-build config mutation
- dead telemetry surface
- hidden multi-process feature
- license footprint drift

### Concrete target

Publish one build matrix for:

- features
- commands
- runtime capabilities
- config validity
- security consequences
- license footprint

Then enforce it at config/open time.

## Cluster D: Product-layer promise discipline

This cluster fixes:

- executor/CLI claiming stronger semantics than engine truth
- open-path/backend ambiguity leaking through user-facing APIs
- surface behavior that depends on hidden wrapper logic

### Concrete target

Make executor and CLI thin, honest product layers over the canonical runtime.

If behavior is product-only policy, name it as such.
If behavior is engine truth, wrappers must not fork it.

## Recommended Execution Order

1. Cluster A: honest control surface
2. Cluster B: honest security surface
3. Cluster C: honest feature matrix
4. Cluster D: product-layer promise discipline

That order matters because:

- control-surface truth gives us the vocabulary for what is actually supported
- security policy must be chosen before we clean up public APIs
- feature validation depends on having an explicit product matrix
- only then can CLI/executor surfaces be reduced to honest wrappers

## Recommended Dispositions

### Promote or centralize

- one code-owned config/control registry
- one explicit build/feature matrix
- one explicit security policy statement

### Delete

- dead or non-operative knobs
- telemetry surface if no subsystem will back it
- any user-facing feature surface that only exists as drift

### Rewrite boundary

- read-only enforcement
- secret redaction behavior
- provider/config validation
- auto-embed cross-build semantics
- CLI/executor claims that outrun engine truth

## Acceptance-Test Program

These are the tests that should exist before this workstream is considered complete.

### Control-surface tests

- every public config key is classified and tested for its real runtime effect
- unsupported keys or modes fail with deterministic errors
- runtime durability switching leaves no stale state in engine observability
- maintenance commands are either effective or explicitly unsupported

### Security-surface tests

- no public config/API path leaks raw secrets unintentionally
- read-only mode is enforced at the authoritative runtime boundary
- `_system_` behavior is consistent across supported backends
- IPC trust model is documented and reflected in surface behavior

### Feature-matrix tests

- provider configs unsupported by the current build fail before first use
- `embed` / non-`embed` builds have explicit, tested command capability matrices
- `auto_embed` semantics do not mutate unexpectedly across builds
- telemetry and multi-process surfaces are either backed by real runtime support or absent

### Product-surface tests

- CLI and executor commands map to one canonical runtime behavior
- open-path/backend differences are explicit and tested
- product docs/examples do not rely on wrapper-only semantics that raw runtime does not guarantee

## Bottom Line

The product/control-surface delta is where Strata most obviously feels vibe coded.

Not because the code is chaotic everywhere, but because:

1. the public surface is stronger than the runtime truth
2. config knobs and features are not classified rigorously enough
3. security guarantees are implied more strongly than they are enforced
4. feature flags change product identity without enough explicitness

This workstream is about making Strata honest before making it bigger.
