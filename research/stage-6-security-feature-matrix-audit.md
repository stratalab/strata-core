# Stage 6: Security and Feature-Matrix Audit

This pass asked two questions:

1. what the real security boundary of Strata is
2. how much the built product changes under features, providers, and codecs

The answer to both is: more than the surface suggests.

## Verdict

Strata does not currently have a strong centralized security architecture.
It has:

- local filesystem hardening on Unix
- policy-layer access control in `executor`
- best-effort secret handling via `SensitiveString`
- partial, not coherent, encryption-at-rest plumbing

The real trust model is:

- same host
- usually same Unix user
- embedded/local-first
- no strong multi-client authz boundary

The feature matrix is similarly material.
Different builds are not just "the same database with extra capabilities".
They are meaningfully different products:

- some commands hard-fail when features are missing
- some features silently degrade
- some configs remain accepted but only fail later at use time
- some settings are persisted by one build and disabled or ignored by another
- the license footprint changes with the feature set

## Security Model

### 1. The `security` crate is not the security boundary

`crates/security` is intentionally small:

- `AccessMode`
- `OpenOptions`
- `SensitiveString`

Most real security behavior lives elsewhere:

- `engine` owns file and directory permission tightening
- `executor` owns read-only policy and `_system_` branch guards
- `durability` owns codec plumbing
- `ipc` owns cross-process exposure

That split is not automatically wrong, but it means there is no single crate that enforces the system's security invariants.

### 2. Access control is handle policy, not database authority

`AccessMode::ReadOnly` is enforced in `Executor` and `Session`.
`Database` itself does not carry an authoritative access mode.

That means:

- read-only is not a database-global invariant
- another local wrapper around the same `Arc<Database>` can still be read-write
- `Strata::from_database` and related wrapping APIs can bypass the normal open policy

Architecturally, this is capability/policy enforcement, not hard isolation.

### 3. IPC is same-user local access, not authenticated remote access

The IPC server uses a Unix domain socket with owner-only permissions.
That is useful hardening, but it is the whole boundary.

What it does not do:

- no protocol authentication
- no peer credential validation
- no per-client roles
- no per-session privilege separation

All clients inherit the server's configured `AccessMode`.
So the practical boundary is:

- "can connect to this socket as the owning user"

not:

- "is an authenticated and authorized client"

### 4. `_system_` branch privilege is backend-dependent

Local `SystemBranch` access uses `execute_internal()` and bypasses the normal `_system_` branch guard.
IPC does not have an equivalent privileged server path.

So:

- local backend: capability-style privileged handle exists
- IPC backend: same API shape exists, but `_system_` operations are rejected by the normal server path

That is a real security and product divergence, not just an implementation detail.

## Sensitive-Data Flow

### 5. `SensitiveString` is only partially protective

`SensitiveString` does three useful things:

- zeroize on drop
- redact `Debug`
- redact `Display`

But it is not a general secret-containment boundary:

- serde is transparent, so serialized output contains the real secret
- `into_inner()` deliberately returns a plain `String`
- cloned values produce more secret-bearing allocations

That is acceptable for an internal utility type, but not if other layers assume it guarantees redaction at every boundary.

### 6. Secrets are persisted in plaintext config

`StrataConfig` is serialized with `toml::to_string_pretty()`.
Because `SensitiveString` is `#[serde(transparent)]`, API keys are written to `strata.toml` as real plaintext values.

The protection story is therefore:

- on Unix: best-effort `0600` permissions
- on non-Unix: permission restriction helpers are effectively no-ops

So the actual model is plaintext-on-disk secrets with local file-permission hardening, not encrypted config or secret-store integration.

### 7. Secret redaction is command-specific, not surface-wide

`CONFIG GET` returns the full `StrataConfig`.
`CONFIGURE GET key` masks selected API-key fields.

There is even an explicit executor test asserting this behavior.

So secret exposure depends on which command path is used.
That is a product-surface divergence:

- one command is redacted
- another command returns the raw values

### 8. Environment overrides are only partial

`apply_env_overrides()` populates:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GOOGLE_API_KEY`

It does not cover `model.api_key`.

So "use env vars instead of plaintext config" is only partially true across the config surface.

## Codecs, Encryption, and Plaintext Exceptions

### 9. Encryption-at-rest is not a coherent shipped path

The codec abstraction advertises `"aes-gcm-256"` support.
The live engine open path then blocks non-identity codecs for WAL-based durability because the WAL reader does not decode codec-wrapped bytes.

The current result is:

- `Cache` mode can be opened with a non-identity codec
- `Standard` and `Always` reject it
- durable encrypted primary databases are therefore not supported

So the feature exists in the crate graph, but not in the main durable runtime.

### 10. WAL codec support is half-implemented

`WalWriter` encodes record bytes through the configured codec.
`WalReader` still parses raw bytes directly.

The engine knows this and rejects the configuration up front.
That is better than silent corruption, but it confirms the feature is not production-complete.

### 11. Snapshots/checkpoints still serialize plaintext data

The checkpoint/snapshot path has two important seams:

- `CheckpointCoordinator` hardcodes `IdentityCodec` for `SnapshotSerializer`
- `SnapshotWriter` records a codec ID in the file header, but writes section bytes directly without an outer codec encode step

So snapshot files currently carry codec metadata without actually applying the configured codec to the section payloads.

This means the plaintext exception list is larger than the surface implies:

- `strata.toml`
- `MANIFEST`
- snapshot/checkpoint sections

The WAL path is also effectively plaintext in live durable deployments because non-identity codecs are blocked there.

### 12. MANIFEST is plaintext metadata

`MANIFEST` persists:

- database UUID
- codec ID
- active WAL segment
- snapshot metadata
- flush watermark

It is not codec-protected.
That may be acceptable by design, but it needs to be documented as part of the real encryption boundary.

## Feature-Matrix Divergence

### 13. `embed` changes the product in mixed ways

Without `embed`:

- `GENERATE`, `EMBED`, and model-management commands fail loudly with compile-time feature errors
- search expansion, rerank, RAG, query embedding, and auto-embed hooks degrade to no-op or passthrough behavior

So the same search recipe can:

- use hybrid retrieval + expansion + rerank + RAG in one build
- silently become lexical-only or no-answer search in another

That is a major product divergence.

### 14. Non-`embed` builds can persistently downgrade config

On open, non-`embed` engine builds rewrite `auto_embed=true` to `false` and then persist the modified config back to `strata.toml`.

That means opening the same data directory with a different binary can permanently change runtime behavior.
This is stronger than "feature unavailable".
It is config mutation across product variants.

### 15. Provider configuration is accepted independently of compiled providers

Runtime config accepts:

- `provider = local`
- `provider = anthropic`
- `provider = openai`
- `provider = google`

That validation is string-level only.
Whether the provider is actually compiled is checked later in `strata-inference`.

So a build can accept a cloud-provider configuration and only fail at generation time with:

- "provider not enabled (compile with --features ...)"

That is another fail-late feature divergence.

### 16. Search-model features also degrade unevenly

`strata-search` gates API expansion and rerank behind its own `expand` and `rerank` features.
When disabled, the API adapters return feature-disabled errors.

At the executor level, the overall search path often treats those failures as "return the original query / hits unchanged".

That makes feature removal operationally quiet in some important paths.

### 17. Telemetry is a config surface, not a live subsystem

The codebase exposes telemetry opt-in in CLI init and stores `telemetry` in config.
I did not find a live telemetry implementation in executor, engine, search, intelligence, or inference.

So telemetry is currently:

- a user-facing config/init surface
- without a corresponding live runtime subsystem

### 18. `multi_process` exists only as a lower-level crate feature

`strata-durability` defines a `multi_process` feature and ships real WAL coordination primitives.
But that feature is not exposed at the workspace root, and I found no live engine/executor open path that composes the runtime around it.

So this is another feature-plane divergence:

- lower layer has concrete machinery
- product/runtime does not expose or own the mode

### 19. The license matrix changes with the feature set

The root package is Apache-2.0.
But these crates are BUSL-1.1:

- `strata-search`
- `strata-intelligence`
- `strata-inference`

That means some high-level features do not just change runtime behavior.
They also change the licensing footprint of the built product.

That belongs in the feature matrix because it affects what "Strata" legally is after feature selection.

## Divergence Table

| Surface | Variant A | Variant B | Divergence |
|---|---|---|---|
| Read-only access | Executor/session policy | Raw `Database` handle | Not an engine-global invariant |
| `_system_` branch | Local privileged handle works | IPC path rejects same operations | Backend-dependent privilege model |
| Secrets | `CONFIGURE GET key` masks | `CONFIG GET` returns raw config values | Redaction depends on command path |
| Config secrets | Unix file perms best-effort | Non-Unix permission helpers no-op | Protection is platform-dependent |
| Encryption at rest | Codec advertised | Durable WAL modes reject non-identity codec | Main durable path unsupported |
| Snapshots | Codec ID recorded | Payload still serialized with identity path | Metadata says "codec", bytes remain plaintext |
| Search stack | `embed` build uses AI layers | non-`embed` build falls back/no-ops | Same recipe, different product |
| Auto-embed | embed build honors config | non-embed build rewrites config to false | Cross-build config mutation |
| Providers | config accepts cloud provider names | missing feature fails at use time | Fail-late provider matrix |
| Telemetry | config/CLI surface exists | runtime implementation absent | Surface without subsystem |
| IPC access | socket-permission local trust | no auth / per-client role split | Same-user boundary only |
| Licensing | Apache root | BUSL crates in feature-expanded builds | Legal footprint changes with features |

## Severity

### Critical

- secrets are retrievable through `CONFIG GET`
- secrets are stored in plaintext config with only best-effort local file-permission protection
- the advertised encryption-at-rest story is not coherent for durable deployments

### High

- read-only access is policy-layer, not engine-authoritative
- IPC has no authentication or client-specific authorization model
- `_system_` branch privilege differs between local and IPC backends
- non-`embed` opens can rewrite and persist config changes
- provider support is fail-late rather than fail-fast at config/open time

### Medium

- telemetry exists as surface drift without a real runtime implementation
- `multi_process` coordination is a lower-layer feature without a product-owned runtime mode
- environment-variable secret override coverage is incomplete

### Low

- the `security` crate name implies more centralized authority than it actually owns
- some feature and codec comments overstate how uniform the runtime really is

## Recommended Actions

1. Define the explicit security model in product terms:
   same-user embedded database, not authenticated multi-tenant IPC.
2. Make secret redaction invariant across config surfaces.
   `CONFIG GET` should not bypass masking.
3. Decide whether config-file secrets are acceptable.
   If yes, document plaintext-on-disk plus permission hardening.
   If no, move to env-only or external secret providers.
4. Either finish codec support across WAL + snapshots + recovery, or stop presenting encryption-at-rest as a supported durable feature.
5. Move read-only enforcement and reserved-branch enforcement closer to the authoritative runtime boundary.
6. Make unsupported providers and incompatible feature combinations fail at config/open time, not first use.
7. Publish a real feature matrix covering:
   runtime behavior, silent degradation, licensing, and build requirements.

## Bottom line

The current security story is adequate for a local embedded database used by a single trusted application process or a set of same-user local processes.

It is not a strong general-purpose security architecture.
And the feature matrix is not cosmetic:
it changes durability properties, privilege behavior, AI capability, config semantics, and licensing.
