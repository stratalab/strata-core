# The V1 IDL — adding and changing commands

This directory is the **single source of truth** for Strata's command surface.
From these files the `strata-idl` generator produces the command index, the
per-command JSON Schemas, the CLI metadata, the reference docs + `llms.txt`, the
MCP tool descriptions, and the inputs the Python SDK vendors. The design and its
rationale live in
[`docs/architecture/v1-idl-overlay-strategy.md`](../../../../docs/architecture/v1-idl-overlay-strategy.md);
this README is the **runbook** for the recurring task: adding a command.

**Golden rule:** you cannot half-add a command. Every step below is enforced by
a drift guard that fails CI with an actionable message, and every coverage
allowlist may *only shrink*. If you skip a step, a guard tells you which one.

## Layout

| File / dir | What it holds |
|---|---|
| `commands/<family>.yaml` | The authored command entries (one list per family). |
| `prose/commands/<id>.md` | Per-command curated prose (frontmatter + body). |
| `prose/snippets/` | Reusable prose fragments referenced by `snippets`. |
| `families.yaml` | Per-family docs URL + family-level error set. |
| `kinds.yaml` | Operation kinds (access, commit, response-model template, snippets). |
| `defaults.yaml` | Global defaults applied to every command. |
| `dto-inventory.yaml` | The registry of allowed `response_model` shapes. |
| `errors.yaml` | The registry of public error codes surfaced to SDK docs. |
| `error-sets.yaml` | Named error sets, referenced as `set:<id>` from any error list (a group that crosses families or kinds lives here, not in copies). |
| `examples/<id>.yaml` | Optional canonical example (drives docs + SDK doctests). |
| `manifest.yaml` | Schema + generator version stamps. |
| `*uncovered*.yaml`, `missing-examples.yaml` | Shrink-only coverage allowlists. |
| `unknown-key-divergences.yaml` | Shrink-only ledger of schema-closed sites the deserializer wrongly accepts (each entry cites its issue; re-verified every generation). |
| `generated/` | **Generated — never hand-edit.** Index, schemas, docs, `llms.txt`. |

A command's final facts are resolved by layering **defaults → family → kind →
command override** (later wins; `errors` append, `errors-` remove). So author
only what differs from the layers above.

## Step 0 — Implement the command in the executor

The IDL *describes* a command; it does not create it. First land the real thing:

- Add the `Command::<Name>` and `Output::<Name>` variants
  (`crates/executor/src/command.rs`, `output.rs`) and their request/response
  DTOs, and wire the dispatch/handler.
- The JSON Schemas are **derived from these types via `schemars`** — the wire is
  never hand-described. Keep DTOs `#[serde(deny_unknown_fields)]` and follow the
  existing field conventions.

## Step 1 — Author the IDL entry

Add an entry to `commands/<family>.yaml`:

```yaml
  - id: kv.put                    # <family>.<op>, lower snake-case dot segments
    kind: mutation.put            # from kinds.yaml — supplies access/commit/response_model
    title: Put KV value
    input: Command::KvPut         # must be a real Command variant
    output: Output::WriteResult   # must be a real Output variant
    result: KvWrite               # inner type substituted into the kind's {result}
    prose: commands/kv.put.md
    fixtures:
      request: requests/v1/kv/put.json
      response: responses/v1/kv/write_applied.json
```

Notes:

- **`kind`** decides most of the shape. Reuse one from `kinds.yaml`; add a new
  kind only for a genuinely new operation category.
- **`result` + `kind.response_model`** resolve to the response model
  (`mutation.put` → `MutationAck<{result}>` → `MutationAck<KvWrite>`). When the
  template does not fit (e.g. pages), override `response_model:` directly on the
  command (see `kv.list` → `Page<Bytes, Bytes>`).
- The **resolved `response_model` must be listed in `dto-inventory.yaml`**, or
  resolution fails.
- **Errors**: family-wide codes go in `families.yaml`; command-specific ones via
  `errors+: [<code>]` (and `errors-:` to drop an inherited one). Every code must
  be registered in `errors.yaml`. A group of codes that recurs across commands,
  families, or kinds is a named set in `error-sets.yaml`, referenced as
  `set:<id>` from any error list (a set may reference sets declared above it).
  Do not copy a set's codes out by hand: `check` rejects any list that spells
  out every code of a defined set and tells you which `set:<id>` to reference.
- **CLI**: `cli_surface: verb` (default) mints a clap verb; `wire` means
  SDK/`command run`/MCP only. Override `cli_path:` if the default
  (`<family> <op>`) is wrong.
- Do **not** put field/schema definitions in command YAML — it may only
  *reference* executor DTOs (a guard rejects `fields:`/`properties:`/anchors).

## Step 2 — Write the prose

Create `prose/commands/<id>.md`:

```markdown
---
summary: Store or replace a KV value by key.
mcp_description: Use this when the user wants to write, overwrite, or upsert a value.
---

Writes a binary value to the selected KV space. If the key already exists,
Strata replaces the visible value and records a new version.
```

`summary` becomes the docs/SDK one-liner; `mcp_description` the MCP tool blurb;
the body is the reference page prose. Reuse shared fragments via `snippets:` in
the command/kind (expanded from `prose/snippets/`).

## Step 3 — Add fixtures

Under `crates/executor/tests/fixtures/`, add a `request` and `response` fixture
(plus `setup:` requests if the response needs prior state). `verify-fixtures`
validates each against the derived schema **and replays it against a scratch
cache executor**, so a fixture that pins a fact a real run never produces fails.
Regenerate/bless with `verify-fixtures --update` and review the diff.

## Step 4 — (Encouraged) canonical example

Add `examples/<id>.yaml` — a language-neutral step list that drives the CLI +
wire example tabs on the reference page **and** the SDK doctests:

```yaml
caption: Store a value, then replace it.
steps:
  - call: kv.put
    args: { key: setting, value: "v1" }
  - call: kv.put
    args: { key: setting, value: "v2" }
    note: replaces the visible value
  - call: kv.get
    args: { key: setting }
    returns: "v2"          # omit for a setup step; `returns: null` asserts a miss
```

`args` are the wire request fields (scope `branch`/`space` and the `type`
discriminator are implicit); `Bytes` fields are base64-encoded automatically at
any depth. `verify-examples` replays every step against a scratch executor.
Every step's command (including sibling calls used as setup) must itself have an
example or be in `missing-examples.yaml`.

## Step 5 — Regenerate and verify

Run from the repo root (features match CI):

```bash
F="--features idl-tooling,inference,testkit"
R="cargo run --locked -p strata-executor $F --bin strata-idl --"

$R generate         # command-index.json + generated/schemas/
$R generate-cli     # cli-command-index.json
$R generate-docs    # generated/docs/** + llms.txt
$R generate-tests   # crates/executor/tests/generated/conformance_cases.rs (TCP4.1)
$R check            # all of the above are fresh (CI gate)
$R check-cli
$R check-docs
$R check-tests
$R verify-fixtures  # every fixture validates + replays  (add --update to bless)
$R verify-examples  # every example validates, covers, and replays
```

Commit the regenerated `generated/` output alongside your source changes.

## Step 6 — Wire the Python SDK (`strata-python`)

Data-plane commands flow into the SDK automatically once the IDL is vendored;
`inference` and the two `hub` admin commands are intentionally excluded.

1. Bump the vendored IDL (`idl/v1/`, pinned by `STRATA_CORE_REV`) and run
   `python tools/generate.py` — the generated typed core gains the new method
   (or it lands in the coverage allowlist).
2. Optionally add a curated ergonomic method in
   `python/stratadb/namespaces/<family>.py` (a lossless wrapper over the core
   method).
3. If you authored an example: vendor `idl/v1/examples/<id>.yaml` +
   `missing-examples.yaml`, add a `BINDINGS` entry in
   `tools/generate_examples.py` (with a result accessor for listing shapes —
   see the `kv.list`/`kv.scan`/`kv.sample` entries), run
   `python tools/generate_examples.py --write`, then `pytest`.

## The guards (what catches a missed step)

| Guard | Enforces |
|---|---|
| `check` / `check-cli` / `check-docs` / `check-tests` | `generated/` and the generated conformance suite are fresh (regenerate + diff). |
| `generated_conformance` test target | per-command wire round-trip idempotence, nested unknown-key rejection at schema-closed sites, error-envelope replay, observed-⊆-declared output tags. |
| `unknown-key-divergences.yaml` | every entry must still be a live schema/deserializer divergence (fixed ⇒ delete the entry). |
| `verify-fixtures` | fixtures validate against the schema and replay to their response. |
| `verify-examples` | examples validate, cover the catalog, and replay. |
| `uncovered-commands.yaml` | every `Command` variant is covered by an IDL entry or listed. |
| `uncovered-cli-verbs.yaml` | every clap verb maps back to a `surface: verb` entry (`crates/cli/src/catalog_guard.rs`). |
| `uncovered-error-codes.yaml` | every registered error is declared in `errors.yaml` or listed. |
| `missing-examples.yaml` | every command has an example or is listed. |
| SDK: core-coverage guard, doctest harness, `generate_examples.py --check` | the Python surface stays in lockstep. |

Every allowlist may **only shrink**: covering a command means *deleting* its
line. The resolver rejects a listed item that is actually covered, an unknown
item, and a new item that is neither covered nor listed — so "just add it to the
allowlist" is not an escape hatch for a real command.

## Removing or renaming a command

Delete its `commands/` entry, `prose/`, `examples/`, and fixtures; remove the
executor variants; regenerate. `generate` prunes stale schema/doc files, and the
coverage guards flip to demanding the now-orphaned executor variant be covered
or listed — so a rename is: change the id + variants together, then regenerate.
