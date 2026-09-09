# V1 IDL Overlay Strategy

## Status

Accepted (2026-07-06).

This document describes how Strata structures its V1 interface definition
layer without creating a second hand-maintained product API.

Amended 2026-07-06 after the executor command-layer review
(`docs/audit/executor-review-2026-07.md`) and the first-run experience design
(`docs/design/first-run-experience.md`). The amendments settle the schema
generation mechanism, SDK generation depth, the consumer matrix and drift
guards, the versioning/deprecation lifecycle, and ecosystem publishing. The
original overlay authoring model is unchanged.

Implementation and test plans are written separately per slice.

## Context

Strata already has several layers:

```text
engine APIs
executor command DTOs
executor IDL overlay metadata
SDKs / CLI / MCP / docs
```

Adding an IDL can help generate SDKs, schemas, CLI help, MCP metadata, and docs,
but it can also become a duplicate command surface if it restates every request
and response field by hand.

The goal is to get the benefits of an IDL without turning it into another
source of truth.

## Decision

The V1 IDL should be a thin executor-owned metadata overlay on top of
executor public command DTOs.

The overlay should live inside the `executor` package because it describes
the external Strata boundary that executor already owns. A separate IDL
crate creates a misleading third product boundary between executor commands and
downstream CLI/SDK/MCP/docs generation.

Executor owns:

1. request DTO fields;
2. response DTO fields;
3. public error wire shape;
4. command serialization and deserialization;
5. golden JSON fixtures;
6. authored IDL metadata and prose;
7. generated resolved command artifacts.

The IDL overlay owns:

1. stable command IDs;
2. command family and operation classification;
3. access mode metadata;
4. commit behavior metadata;
5. pagination and batch behavior metadata;
6. docs links;
7. SDK naming hints;
8. CLI path hints;
9. MCP/tooling descriptions, agent search terms, and curated MCP tool-set
   membership;
10. error bundle associations, plus per-error `hint` and stable `ref` slugs
    (the first-run design §7.2 requires both on every public code);
11. fixture references and inline examples;
12. generation and conformance metadata;
13. lifecycle facts: `wire_status`, `since`, and deprecation metadata.

The IDL must not redefine executor DTO fields.

## Tiered Surface Model

Strata should not try to make every access surface equally handcrafted. The V1
surface should follow a tiered model:

```text
canonical executor command surface
  -> generated broad coverage and discovery
  -> curated ergonomic shortcuts for common workflows
  -> generic escape hatch for the long tail
```

This is the practical pattern used by large developer products with many
access paths. They keep one canonical API, generate broad coverage from it, and
only hand-polish the workflows that deserve a first-class experience.

For Strata, the canonical API is the executor command DTO surface. Every public
command should be reachable through a generic command runner:

```sh
strata command run --command-json '{ "type": "KvPut", ... }'
```

SDKs should expose the same complete fallback:

```text
client.execute(command)
```

Curated CLI and SDK methods can then be added where they materially improve the
experience:

```sh
strata kv put user Claude
```

```text
client.kv.put("user", "Claude")
```

The IDL's job is to make the complete command surface discoverable,
documented, validated, and generatable. It is not required to derive a perfect
human CLI syntax for every command.

## Surface Ownership

The V1 surface has three levels:

1. **Complete generic coverage.** Every public executor command can be
   serialized, documented, explained, validated against fixtures, and executed
   through a generic command path.
2. **Generated presentation metadata.** Command lists, `strata explain`, MCP
   descriptions, docs tables, error references, fixture coverage, and SDK
   method maps are generated from the resolved command index.
3. **Curated ergonomic shortcuts.** A smaller set of high-value CLI commands
   and SDK convenience methods may be handwritten or generated from simple
   shared profiles. These shortcuts are product UX, not the canonical contract.

The third level must never be the only way to access a command. If a command is
not worth a curated shortcut, it still remains accessible through the generic
executor command surface.

## Packaging And Ownership

IDL tooling is executor-owned development tooling, not a runtime dependency of
normal command execution.

Recommended layout:

```text
crates/executor/
  idl/v1/
    manifest.yaml
    defaults.yaml
    families.yaml
    kinds.yaml
    errors.yaml
    commands/
    prose/
    generated/
      command-index.json
  src/bin/strata-idl/
    main.rs
  src/idl_tooling/
    ...
```

The `idl_tooling` module should be feature-gated or otherwise isolated so
normal executor library builds do not compile YAML/prose parsing code on the
runtime path. The generated command index is the stable artifact consumed by
later CLI/SDK/MCP/docs generators.

The local developer commands should be executor package commands:

```text
cargo run -p strata-executor --features idl-tooling --bin strata-idl -- generate
cargo run -p strata-executor --features idl-tooling --bin strata-idl -- check
```

There should not be a separate `idl-next` workspace crate for V1.

## Relationship To Stainless-Style Specs

Stainless provides a useful precedent: the API schema remains the field
contract, while a separate configuration layer controls SDK structure,
resources, methods, pagination, naming, examples, and generated presentation.

Strata should follow the same broad pattern:

```text
executor DTO-derived schema = field contract
IDL overlay                 = command/product metadata
generated artifacts         = expanded machine-readable contract
```

References:

1. Stainless config reference: <https://www.stainless.com/docs/reference/config/>
2. Stainless SDK configuration: <https://www.stainless.com/docs/sdks/configure/>
3. Stainless OpenAPI extensions: <https://www.stainless.com/docs/openapi/extensions/>

The important lesson is not to copy Stainless' exact schema. The lesson is to
separate field-level API truth from SDK/docs/tooling metadata.

Stainless also avoids most of the old Spring XML failure mode because its
configuration is secondary. The configuration describes how to generate SDKs
from an API that already exists; it does not become the application. Strata
should preserve the same property: the IDL overlay describes public command
metadata for executor DTOs that already exist.

## Avoiding The Spring XML Failure Mode

The IDL should not become a 1,000-line declarative program that developers have
to mentally execute. The failure mode to avoid is not YAML, XML, or any
specific syntax. The failure mode is configuration becoming an untyped,
implicit, hard-to-debug programming language.

The IDL must also avoid becoming a handcrafted CLI mapping language. Moving
per-command argument parsing from Rust into YAML would recreate the same
maintenance problem in a different syntax. Authored command metadata should
describe command identity, behavior, docs, and presentation facts. It should
not normally spell out every argv-to-field binding.

The following constraints are part of the strategy:

1. Keep the root manifest small.
2. Split authored command files by family and, when needed, by subdomain.
3. Use shallow inheritance only.
4. Do not allow arbitrary expression languages, conditionals, scripts, or
   recursive includes.
5. Do not use YAML anchors as a public composition mechanism.
6. Make every inherited fact visible in generated resolved artifacts.
7. Provide an `explain` view for any command.
8. Fail fast on unknown fields, duplicate command IDs, duplicate CLI paths,
   duplicate MCP names, unresolved placeholders, unknown error codes, and
   missing fixtures.
9. Enforce file-size and command-count guardrails so command registries are
   split before they become hard to review.
10. Forbid per-command CLI field mappings as the default authoring pattern.
11. Require curated ergonomic shortcuts to be explicitly marked as curated, so
    generic coverage and product shortcuts do not blur together.

The authoring experience should feel like maintaining an indexed registry, not
like configuring a framework.

## Source Of Truth

### Canonical Field Shapes

Executor public DTOs are the canonical V1 wire shapes.

Examples:

```text
KvPutCommand
WriteResult
BatchResult<T>
BatchItem<T>
PageInfo
CommitReceipt
PublicErrorStatus
```

The IDL references these names but does not restate their fields.

### Canonical Shared Concepts

The response-contract work established shared concepts that SDKs should expose:

```text
MutationAck<T>
Maybe<T>
Page<T, Cursor>
BatchResult<T>
BatchItem<T>
DiagnosticsResponse<T>
StatusResponse<T>
PublicErrorStatus
```

The IDL may map executor output DTOs to these concepts:

```yaml
output: WriteResult
response_model: MutationAck<KvWrite>
```

But the executor DTO still owns the concrete serialized fields.

## Schema Layer (Decided 2026-07-06)

Argument and output JSON Schemas are generated mechanically from the executor
DTOs via `schemars`:

1. `#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]` on
   `Command`, `Output`, and every public protocol DTO. The derive compiles
   only under the `idl-tooling` feature; the runtime path gains no dependency
   and no cost.
2. `strata-idl generate` emits per-command request/response schemas into
   `generated/`, keyed by command ID, alongside the resolved command index.
3. Schemas are derived from the same serde attributes that define the wire
   format, so they cannot drift from it. A hand-rolled schema emitter is
   explicitly rejected: it would be a second description of the field
   contract, which this strategy forbids.
4. Every golden fixture must validate against its command's generated schema
   in CI.

The schema layer is the keystone artifact: MCP tool `inputSchema`, typed SDK
stubs, `strata agents commands --json`, and generic command-runner validation
are all generated from it. It resolves executor-review finding META-1.

## SDK Generation Depth (Decided 2026-07-06)

SDK generation follows the Stainless division of labor — generated core,
curated hand layer:

1. **Generated from the IDL:** protocol models, one typed method per public
   command, docstrings (from prose `summary`), error types (from the error
   registry), and pagination/batch helpers driven by `kind` metadata. This
   guarantees the full 103-command surface is in sync in every SDK without
   per-command manual work.
2. **Handwritten per SDK:** the ergonomic namespace layer (`db.kv.put(...)`
   sugar), connection/lifecycle idioms, and language-native affordances.
   Curated methods must remain lossless single-command wrappers, mirroring
   the executor facade rule.
3. Full Stainless-style generation of the ergonomic layer is rejected for V1:
   it would push per-language naming and idiom configuration into the IDL,
   which is the Spring-XML failure mode this document forbids.

## Authored Versus Resolved IDL

The hand-authored IDL should be compact. A generator should expand it into a
fully resolved machine-readable command index.

### Authored Form

```yaml
id: kv.put
kind: mutation.put
title: Put KV value
description: Store or replace a binary value by key.
input: KvPutCommand
output: WriteResult
result: KvWrite
errors+:
  - invalid_argument.kv.key
fixtures:
  response: kv/write_applied.json
```

### Resolved Form

Generated artifacts may expand the authored form to:

```yaml
id: kv.put
family: kv
op: put
kind: mutation.put
title: Put KV value
description: Store or replace a binary value by key.
docs: /docs/kv/put
mcp:
  name: strata_kv_put
  description: Store or replace a binary value by key.
cli:
  path: ["kv", "put"]
feature: core
access: write
input: KvPutCommand
output: WriteResult
response_model: MutationAck<KvWrite>
commit: commits_on_success
pagination: none
batch: none
errors:
  - invalid_argument.branch
  - invalid_argument.space
  - failed_precondition.branch_not_found
  - failed_precondition.space_not_found
  - failed_precondition.read_only
  - invalid_argument.kv.key
fixtures:
  request: kv/put.json
  response: kv/write_applied.json
```

Humans edit the authored form. Tools consume the resolved form.

Every command should have a resolved view that can be inspected locally:

```text
strata explain kv.put
```

The output should show the fully merged command contract, including inherited
family defaults, kind defaults, error bundles, fixture paths, docs paths, CLI
path, and MCP metadata. This prevents the common configuration-debugging
problem where developers cannot tell which file supplied a behavior.

## Reuse Model

One block per command is acceptable, but command blocks should reuse defaults.

Recommended reuse layers:

```text
global defaults
family defaults
operation kind defaults
command-specific overrides
```

### Global Defaults

Examples:

```yaml
defaults:
  docs: /docs/{family}/{op}
  cli_path: ["{family}", "{op}"]
  mcp_name: strata_{family}_{op}
  fixtures:
    request: "{family}/{op}.json"
```

### Family Defaults

Examples:

```yaml
families:
  kv:
    feature: core
    docs_base: /docs/kv
    common_errors:
      - invalid_argument.branch
      - invalid_argument.space
      - failed_precondition.branch_not_found
      - failed_precondition.space_not_found
```

### Kind Defaults

Examples:

```yaml
kinds:
  mutation.put:
    access: write
    commit: commits_on_success
    pagination: none
    batch: none
    response_model: MutationAck<{result}>
    errors:
      - failed_precondition.read_only

  read.get:
    access: read
    commit: none
    pagination: none
    batch: none
    response_model: Maybe<{result}>

  read.page:
    access: read
    commit: none
    pagination: cursor
    batch: none
    response_model: Page<{result}>
```

### Command Overrides

Commands only provide what is unique:

```yaml
id: vector.query
kind: read.page
title: Query vector collection
description: Search a vector collection by similarity.
input: VectorQueryCommand
output: VectorMatches
result: VectorMatch
docs: /docs/vector/query
errors+:
  - invalid_argument.vector.dimension
  - failed_precondition.vector.collection_missing
```

## Append And Override Rules

The overlay should define explicit merge rules:

1. Plain fields override inherited values.
2. `field+` appends to inherited lists.
3. `field-` removes inherited list entries when needed.
4. Generated artifacts must fail on unresolved placeholders.
5. Generated artifacts must fail when an IDL command references a missing
   executor DTO.
6. Generated artifacts must fail when two commands resolve to the same CLI path
   or MCP name unless an explicit alias is declared.

### Named Error Sets (Decided 2026-09-09, #3250)

The four reuse layers hold what is common to a family or a kind. A group of
error codes that crosses those boundaries — the model-runtime set is raised by
`inference.*` commands of one kind and `vector.*` commands of two others —
has no layer to live in, and was being copied by hand into every command that
raised it. That is the "error bundle association" the Decision section lists
(item 10) with no file behind it.

`error-sets.yaml` is that file. It declares named sets, and any error list
(`errors`, `errors+`, `errors-`, or a set declared later) references one as
`set:<id>`. The resolver expands references in place before layering, so
the resolved form and every generated artifact still see only codes. Rules:

1. A set may reference only sets declared above it (acyclic by construction).
2. A set expands to at least two distinct codes and is referenced at least
   once; every member is registered in `errors.yaml`.
3. A list that spells out every code of a defined set is rejected with the
   `set:<id>` to reference instead. Once a set exists, copying it is a
   `check` failure, not a review comment.

## Command ID Convention

Stable command IDs should follow:

```text
family.operation
family.resource.operation
```

Examples:

```text
kv.put
kv.get
json.index.create
vector.collection.create
vector.query
event.append
graph.node.get
branch.fork
space.create
admin.health
arrow.import
inference.generate
```

The ID should be stable even if CLI phrasing changes.

## CLI And SDK Coverage Strategy

The IDL should support two access styles.

### Generic Command Execution

Generic execution is the required complete surface.

The CLI should provide a command-runner path that accepts serialized executor
commands and dispatches them without any command-specific parser:

```sh
strata command run --command-json '{ "type": "KvPut", ... }'
strata command run --file ./command.json
```

SDKs should expose the same complete path:

```text
client.execute(command)
```

This path is allowed to be more verbose. Its job is coverage, stability, and
agent-friendliness. It prevents the project from blocking every new executor
command on a bespoke CLI or SDK convenience method.

The generic path should still be well-supported:

1. `strata explain <command>` shows the command's DTO name, response model,
   examples, docs link, and public errors.
2. Generated JSON schema validates command payloads.
3. Golden fixtures provide copyable request and response examples.
4. Error output uses the same public error contract as curated commands.

### Curated Ergonomic Shortcuts

Curated shortcuts are optional product UX.

Examples:

```sh
strata kv put user Claude
strata vector query docs --embedding-json '[...]' --top-k 10
```

```text
client.kv.put("user", "Claude")
client.vector.query("docs", embedding, topK: 10)
```

Curated shortcuts may be handwritten or generated from small shared profiles,
but they must be explicitly marked as curated in the resolved metadata. They
must not become the completeness mechanism.

Rules:

1. A command can ship in V1 with generic coverage and no curated shortcut.
2. Curated shortcuts should exist for common human workflows and beta-discovery
   pain points.
3. The generic command path remains the fallback for every public command.
4. A curated shortcut must round-trip to the same executor command fixture used
   by the generic path.
5. If more than a small minority of commands need explicit field mappings, the
   shortcut model is too complicated and should be simplified.

### CLI Metadata Boundary

The IDL may provide CLI path and shortcut classification:

```yaml
id: kv.put
kind: mutation.put
input: Command::KvPut
cli:
  path: ["kv", "put"]
  shortcut: curated
```

It should not normally provide a field-by-field argument map:

```yaml
# Avoid as the default pattern.
cli:
  args:
    - field: key
      position: 0
    - field: value
      position: 1
```

If a shortcut needs mapping rules, prefer shared profiles or reusable codecs
over per-command definitions. Examples: key/value write, key read, cursor page,
JSON batch file, bytes from file, bytes from stdin, vector embedding JSON.

## Generated Artifacts

The IDL pipeline should generate:

1. JSON Schema for executor request and response DTOs.
2. Resolved command index.
3. SDK model names and method map.
4. CLI command/help metadata.
5. MCP tool or docs-search metadata.
6. Public docs tables.
7. Golden fixture coverage reports.
8. Error-code coverage reports.
9. Generic command-runner metadata.
10. Curated shortcut inventory and coverage reports.

Generated artifacts should live under the executor-owned generated directory:

```text
crates/executor/idl/v1/generated/
```

Hand-authored overlays should live outside generated directories.

Recommended authored layout:

```text
crates/executor/idl/v1/
  manifest.yaml
  defaults.yaml
  families.yaml
  kinds.yaml
  errors.yaml
  commands/
    kv.yaml
    json.yaml
    vector/
      collection.yaml
      entry.yaml
      query.yaml
      index.yaml
    event.yaml
    graph.yaml
    branch.yaml
    space.yaml
    admin.yaml
    arrow.yaml
    inference.yaml
  generated/
    command-index.json
    schema.json
    docs-index.json
```

The exact split can change, but the root file must not become the full command
surface.

## Update Flow

The IDL strategy must make customer-feedback loops cheap. During beta, users may
report that a command description is confusing, Claude selects the wrong command,
SDK docs are unclear, or CLI help text is misleading.

Those updates should be made in the authored IDL/prose source, not in generated
SDKs, generated MCP metadata, generated docs, or executor code unless the wire
contract itself is wrong.

### Example: Improve Command Discovery

If customers report that an agent struggles to choose `kv.put`, update the
canonical command prose:

```text
crates/executor/idl/v1/prose/commands/kv.put.md
```

For example:

```markdown
---
summary: Store or replace a value by key.
mcp_description: Use this when the user wants to write, overwrite, or upsert a KV value.
---

Writes a binary value to the selected KV space. If the key already exists,
Strata replaces it and records a new version.
```

If the issue is command classification or searchability, update command
metadata:

```yaml
id: kv.put
kind: mutation.put
mcp:
  search_terms:
    - write key
    - update value
    - upsert
    - store binary value
```

Then regenerate the resolved index:

```text
cargo run -p strata-executor --features idl-tooling --bin strata-idl -- generate
```

The generated command index becomes the single downstream source:

```text
crates/executor/idl/v1/generated/command-index.json
```

SDK, CLI, MCP, and docs generators consume that resolved command record. They do
not walk the authored YAML/prose graph independently.

The intended flow is:

```text
customer feedback
  -> edit authored command metadata or prose
  -> regenerate command-index.json
  -> validate fixtures, DTO refs, docs refs, and error refs
  -> generate SDK/CLI/MCP/docs artifacts
  -> release beta/nightly/stable packages
```

### Update Rules

1. Change command language in `crates/executor/idl/v1/prose`.
2. Change command behavior metadata in `crates/executor/idl/v1/commands`.
3. Change executor DTOs only when the actual wire contract changes.
4. Do not edit generated SDK docs, MCP descriptions, or command-reference pages
   directly.
5. Downstream generators should read generated artifacts only.
6. CI should fail if generated artifacts are stale after an authored IDL change.

This keeps language consistent across SDKs and tools. Different SDKs may format
docstrings differently, but they should not independently rewrite command
semantics.

## Consumer Matrix And Drift Guards

The point of the IDL is to keep every surface in sync from one source. The
full consumer set, including the surfaces added by the first-run experience
design:

| Consumer | Generated from the IDL | Delivery |
|---|---|---|
| CLI help, `strata explain`, command listing | resolved index + prose | embedded catalog in the binary |
| `strata agents guide` / `commands --json` / `errors --json` | resolved index + schemas + error registry | embedded in the binary (first-run D3) |
| MCP tool schemas + descriptions + curated tool set | schemas + prose `mcp_description` + `mcp.curated` flag | `strata mcp serve` (first-run D8) |
| SDK models, typed methods, docstrings | schemas + resolved index + prose | Python/Node build pipelines (M9) |
| Website command reference + `llms-full.txt` | resolved index + prose | stratadb.org build (first-run D11) |
| Error reference pages (`/e/<code>` ref slugs) | error registry + `hint`/`ref` | stratadb.org build |
| Golden wire tests | fixtures + schemas | executor CI |

Drift guards, all CI-enforced:

1. **Exhaustiveness guard.** Every `Command` enum variant must have a resolved
   IDL entry or appear on an explicit allowlist
   (`idl/v1/uncovered-commands.yaml`). The allowlist may only shrink; adding a
   command variant without either fails CI. This resolves executor-review
   finding META-3.
2. **Consumer guard.** cli's verb tree must round-trip against the
   catalog: every catalog `cli.path` marked implemented resolves to a real
   clap verb, and every clap verb maps back to a catalog entry. The
   handwritten clap surface and the generated catalog are reconciled by test,
   not by discipline. This resolves executor-review finding META-4.
3. **Freshness guard.** Generated artifacts stale after an authored change
   fail CI (`strata-idl check`, already implemented).
4. **Fixture-behavior guard.** Fixtures must be reproducible from a real
   executor run, not merely schema-valid. The executor review found the
   `kv.scan` fixture pinning `has_more` on a command that unconditionally
   returns a terminal page (finding DSGN-2) — schema validation alone cannot
   catch a frozen lie. `strata-idl verify-fixtures` executes each request
   fixture against a scratch executor and diffs the response fixture.

## Versioning And Deprecation

The wire lifecycle vocabulary and its exposure (resolves executor-review
findings EVOL-2 and EVOL-3):

1. `wire_status` values: `stable | transitional | experimental | deprecated`.
   The current resolver accepts only the first two; the vocabulary is extended
   when the first consumer needs it.
2. Every command entry carries `since: <version>`; deprecated entries carry
   `deprecated: {since, replacement, removal_target}`. Generators render
   deprecation into SDK docstrings, CLI help, and MCP descriptions
   mechanically.
3. The IDL `schema_version` and the release version are exposed at runtime
   through `Info`/`Describe` output so a pinned SDK or third-party adapter can
   detect skew before issuing commands.
4. Additive-change playbook: new optional command fields must carry serde
   defaults; new commands enter as `experimental` or `stable`; enum growth is
   covered by `#[non_exhaustive]` (hard rule 28 — the review found three wire
   enums missing it, finding ERR-1). Removing or renaming a wire field is a
   major-version event, full stop.

## Published IDL Artifacts For The Ecosystem

Third-party adapters and plugins (LangChain/LlamaIndex integrations, CI
actions, community SDKs) must be able to build against the contract without
scraping docs — otherwise the surface "gets away" the moment outsiders start
building. The resolved artifacts are therefore published, versioned release
outputs, fanned out by the release train (first-run design §9.2):

1. **In the binary:** `strata agents commands --json` and
   `strata agents errors --json` emit the embedded resolved index and error
   registry — the local, always-version-matched mirror for coding agents.
2. **On stratadb.org:** `/idl/v1/command-index.json` and
   `/idl/v1/schemas/...`, version-stamped like `llms.txt`, giving adapter
   authors and their agents a stable fetchable contract.
3. **In SDK packages:** the resolved index ships inside the Python wheel and
   npm package so generators and downstream tooling can introspect the
   installed surface offline.

One generator, every mirror. An ecosystem adapter written against the
published index is written against the same artifact the CLI, MCP server, and
SDKs are generated from.

## Benefits

### No Duplicate Field Contract

Request and response fields live in executor DTOs. The IDL references
those DTOs and enriches them with metadata.

This avoids drift between Rust code and the public contract.

### Better SDK Ergonomics

SDKs can expose shared concepts such as `MutationAck`, `Maybe`, `Page`, and
`BatchResult` without parsing primitive-specific output names.

### Better CLI And MCP Presentation

CLI and MCP need human text, examples, docs links, command grouping, and safety
metadata. Executor code should not own that presentation layer.

### Complete Coverage Without Perfect Ergonomics

The generic command path gives every public executor command a usable CLI and
SDK route without waiting for bespoke syntax. Curated shortcuts can then focus
on the commands users actually type often.

### Lower Maintenance Cost

Defaults and command kinds avoid repeating boilerplate across hundreds of
commands.

### Explicit Behavioral Metadata

The overlay captures facts that field schemas do not express well:

1. read versus write access;
2. commit behavior;
3. pagination behavior;
4. batch behavior;
5. feature gates;
6. stable error bundles;
7. CLI path;
8. MCP name and description;
9. docs path;
10. fixture ownership.

### Conformance Hooks

The IDL can drive guard tests:

1. every public executor command has IDL metadata;
2. every IDL command references existing DTOs;
3. every command has fixtures;
4. every fixture validates against generated schema;
5. every emitted public error code exists in the registry;
6. every docs link has a target;
7. every MCP name is stable and unique.
8. every generated resolved command can be reproduced from authored inputs;
9. no authored command file exceeds the agreed review-size threshold;
10. no generated artifact is edited by hand.

## Non-Goals

1. The IDL is not a replacement for executor command DTOs.
2. The IDL is not a full OpenAPI spec.
3. The IDL is not a second hand-authored request/response schema.
4. The IDL should not expose storage or engine internals.
5. The IDL should not require SDKs to know executor implementation details.

## Risks

### Risk: Overlay Becomes A Second API

Mitigation:

1. forbid field definitions in command metadata;
2. require all input/output types to resolve to executor DTOs;
3. generate schemas from executor DTOs;
4. fail CI on drift.

### Risk: Defaults Hide Important Behavior

Mitigation:

1. generate and review resolved command artifacts;
2. require command-specific overrides for unusual commit, pagination, or batch
   behavior;
3. include resolved command metadata in docs.
4. provide `strata explain <command-id>` for local debugging.

### Risk: Overlay Becomes Spring XML In YAML

Mitigation:

1. forbid arbitrary logic in authored IDL files;
2. use only shallow global/family/kind/command inheritance;
3. split command files by family and subdomain;
4. enforce size guardrails on authored files;
5. keep the root manifest small;
6. make resolved output mandatory and easy to inspect.

### Risk: MCP Descriptions Become Stale

Mitigation:

1. default MCP descriptions from command descriptions;
2. override only when needed;
3. validate docs links and fixtures in CI.

### Risk: SDK Names Drift From Wire DTOs

Mitigation:

1. keep SDK names as overlay hints;
2. keep wire DTO names in generated schema;
3. test SDK model mapping against response fixtures.

### Risk: IDL Becomes Handwritten CLI In YAML

Mitigation:

1. require complete generic command-runner coverage;
2. treat curated shortcuts as optional UX, not the canonical surface;
3. forbid field-by-field CLI mappings as the default authoring style;
4. prefer shared shortcut profiles and reusable codecs;
5. test every curated shortcut against the same executor command fixtures as
   generic execution.

## Recommended Initial Scope

Start with IDL generation only, not CLI or SDK generation.

The first implementation slice should prove the smallest useful contract loop:

```text
executor DTOs
  -> executor-owned thin IDL overlay
  -> resolved command-index.json
```

Do not generate CLI, TypeScript, Python, MCP, or docs artifacts in the first
slice. Those should consume the same resolved command index later, after the
IDL shape is proven.

### Slice 1: KV And Vector IDL Overlay

Scope:

1. authored KV command metadata;
2. authored vector command metadata;
3. KV command prose files;
4. vector command prose files;
5. basic global, family, and kind defaults;
6. resolved `command-index.json`;
7. validation of command metadata, prose refs, executor DTO refs, error refs,
   and fixture refs.

This slice should not wire generated metadata into the CLI. It should only
produce the resolved artifact that later CLI generation will consume.

KV should cover enough command shapes to exercise:

1. mutation acknowledgements;
2. optional reads;
3. pages;
4. batches;
5. status/count outputs;
6. error references;
7. fixture references.

Vector should validate:

1. collection lifecycle commands;
2. vector entry mutations;
3. query/search commands;
4. vector-specific validation errors;
5. diagnostics/index-related metadata where exposed.

### Slice 1b: Executor-Owned IDL Packaging

Before adding CLI generation, fold the Slice 1 resolver and authored sources
into `executor`.

Scope:

1. move authored IDL from repo-root `idl/strata/v1` to
   `crates/executor/idl/v1`;
2. move resolver/generator code from any standalone IDL crate into
   executor-owned tooling;
3. expose `generate` and `check` through an executor package dev binary;
4. remove the standalone IDL crate from the workspace;
5. keep YAML/prose parsing code off the normal executor runtime path;
6. preserve the same generated `command-index.json` contents modulo source path
   changes.

This slice is intentionally packaging-only. It should not change command IDs,
response models, fixtures, prose semantics, or generated CLI behavior.

### Slice 2: CLI Discovery From IDL

After Slice 1b makes the resolved index executor-owned, generate or wire CLI
metadata from that resolved index.

Scope:

1. CLI use of the resolved command index for:
   - command help text;
   - `strata explain kv.put`;
   - command listing and grouping;
   - docs links;
   - access, commit, pagination, and batch facts.
2. Explicit shortcut inventory for commands that will eventually have curated
   human syntax.

The CLI should read generated artifacts, not authored YAML and prose.

This slice should not add command execution. Its job is discovery, help, and
explainability.

### Slice 3: Generic Command Execution

After CLI discovery works, add the generic command runner:

```sh
strata command run --command-json '{ "type": "KvPut", ... }'
strata command run --file ./command.json
```

Scope:

1. generated JSON schema or equivalent validation for serialized executor
   commands;
2. generic CLI command loading and deserialization into executor DTOs;
3. generic SDK `client.execute(command)` shape once SDK work begins;
4. fixture-backed examples for request and response payloads;
5. conformance tests proving every IDL command is reachable through generic
   execution unless explicitly marked non-executable.

This slice should not require bespoke argument parsing for every command.
Complete coverage comes from serialized executor commands.

### Slice 4: Curated Shortcuts

After generic execution exists, add curated CLI shortcuts and later SDK
resource methods for the highest-value workflows.

Scope:

1. pick shortcuts from real usage, beta feedback, and commands that humans type
   frequently;
2. prefer shared profiles and codecs over per-command field maps;
3. keep the shortcut inventory explicit in generated metadata;
4. test each shortcut against the same executor command fixture as generic
   execution;
5. keep commands without shortcuts fully reachable through generic execution.

### Later Slices

After KV/vector IDL, CLI discovery, and generic command execution work, add
curated shortcuts only for high-value workflows. Then add remaining command
families to the same IDL pipeline:

1. JSON;
2. event;
3. graph;
4. branch;
5. space;
6. admin;
7. Arrow import/export;
8. inference once its product surface is frozen.

Only after the CLI pipeline is stable should Strata generate external SDKs.
TypeScript should be the first SDK target, followed by Python. SDKs should start
with `client.execute(command)` plus generated models and docs, then add the
generated typed per-command methods and curated resource methods per the SDK
Generation Depth decision above.

## Current State And Remaining Roadmap (2026-07-06)

Where the pipeline actually stands:

| Piece | Status |
|---|---|
| Overlay authoring model (kinds/families/defaults, prose, fixtures) | ✅ complete, all 10 families (120/120 commands) |
| Resolver + validation + freshness `check` | ✅ built (`idl_tooling.rs`) |
| Resolved `command-index.json` + `cli-command-index.json` | ✅ generated |
| Generic command runner (`strata command run --command-json`) | ✅ shipped in cli |
| Runtime `CliCommandCatalog` | ✅ built, consumed only by its own tests |
| JSON Schemas (schemars) | ✅ built 2026-07-10: per-command documents in `generated/schemas/`, fixtures validate against them in `generate`/`check` |
| CLI reads the catalog (help/explain/listing) | ❌ Slice 2 unlanded; clap tree is parallel and unguarded (META-4) |
| Exhaustiveness + consumer + fixture-behavior guards | ✅ built 2026-07-10: `uncovered-commands.yaml` (shrink-only, resolver-enforced), CLI round-trip test + `uncovered-cli-verbs.yaml` in crates/cli, `strata-idl verify-fixtures` replays every pair against a scratch executor (CI-gated) |
| Lifecycle vocabulary beyond `stable\|transitional`; runtime version exposure | ❌ (EVOL-2/3) |
| Remaining 8 command families | ✅ complete 2026-07-11: all 10 families, 120/120 commands, uncovered-commands.yaml empty. Every fixture either replays against a scratch executor or carries a stated fixtures.replay_skip reason |
| Publishing (binary `agents` surface, website, SDK embedding) | ❌ depends on first-run D3/D7 |

Remaining work, in dependency order (slice codes assigned when scheduled into
M9 plans):

1. **Schemas.** schemars derives + per-command schema generation + fixture
   schema validation. Unblocks everything below.
2. **Guards.** Exhaustiveness allowlist, CLI round-trip guard,
   `verify-fixtures`. Cheap once schemas exist; stops the drift that is
   already live.
3. **Family coverage.** Author the remaining 8 families (json, event, graph,
   branch, space, admin, arrow, inference). Mechanical; the model is proven.
   The allowlist shrinks to zero here.
4. **CLI consumption (Slice 2 as written).** help/explain/listing from the
   catalog; delete the duplicated help prose from the clap tree.
5. **Agent + MCP surfaces (first-run D3/D8).** `strata agents` family and MCP
   tool schemas generated from the resolved index; curated tool-set flags
   authored in the overlay.
6. **Lifecycle + publishing.** Extended `wire_status`, `since`/`deprecated`,
   `Info`/`Describe` version exposure, release-train publishing to
   binary/website/SDK packages.
7. **SDK generation (M9).** Generated core (models, typed methods,
   docstrings) for Node then Python, curated hand layer on top.

Items 1–2 are small and should land before any new command family is authored;
every family added without guards deepens the unguarded surface.

## Acceptance Criteria For The Strategy

1. Executor DTOs remain the field source of truth.
2. The IDL overlay contains command metadata, not duplicate fields.
3. Authored command entries stay compact.
4. Resolved generated artifacts are explicit enough for generic
   SDK/CLI/MCP/docs coverage.
5. Golden fixtures validate the generated schema.
6. Public command changes require executor, IDL, and fixture updates.
7. No public command requires a bespoke curated shortcut before it is reachable
   through CLI or SDK.
