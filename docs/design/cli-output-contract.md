# CLI output: one human mode, derived from the IDL

**Status:** accepted 2026-09-11 after three review rounds; S0 complete
(S0a landed the §5 matrix, S0b the declarations and their guard, S0c the
family ⇔ schema check), #3312 and #3327 landed, S1 landed (the renderer
reads `mutation_ack`), #3326 + #3328 landed (one line reader), S2 landed
2026-09-12 (the renderer reads `columns` and `map`: one table for `page`,
`history`, `search` and `analytics`). Tracking issue **#3314** (slices
S0–S5). Every
file:line below was read on `main` at `60db96ac`. Follows the shape of `inference-model-resolution.md`: name the
mechanism, replace it, leave an executable contract behind.

Five issues describe the same gap from five sides. #3306 (the homepage hero
now streams the default output, and the default is "a mixture of four shapes,
one of which is a third JSON dialect"). #3205 (a rendering contract by
response shape, not by command). #3116 (`--raw` is a no-op for `kv get`, so
bytes written through the CLI cannot be read back through it). #3312 (the
playground accepts `--json` and renders human anyway). #3313 (the IDL's
declared response model is nominal for a fifth of the stable commands, so
"render from the declaration" is not yet a thing one can do). Each could be
fixed as an `/audit-fix`; none of the five fixes would prevent the sixth. This document
does for the CLI's output what the resolver plan did for model specs: one
principle, one contract that runs, and a slice sequence whose acceptance is
the contract.

---

## 1. The principle

> **The IDL declares the shape; the renderer reads the declaration.** Every
> command's response model is already one of ten families (`kinds.yaml`,
> `dto-inventory.yaml`). Human, raw and JSON output are three *encodings* of
> that one declared shape. No format guesses the shape from the JSON it was
> handed.

Three corollaries, each of which today's code violates:

- **One rule per family, not one arm per command.** A `Page<BranchItem>` and
  a `Page<VectorCollectionInfo>` render by the same rule with different
  columns. The rule is keyed by the IDL `kind`; the columns are declared
  beside the DTO. A command with no declaration is a test failure, not a JSON
  dump.
- **Formats differ in encoding, never in facts.** `--json` is the complete
  record (the machine contract, unchanged) — the one unmodified
  representation. Human mode shows the facts the **command** declares
  (the family fixes the layout, the command decides which facts appear —
  `hub get-dataset` shows 15 of its 27 wire fields) and *re-encodes* facts a
  person cannot read (an instant as a date, bytes as text); it never invents
  a fact `--json` lacks, and never puts a human encoding inside a machine
  structure. `--raw` is the **shell-composable** form of the same declared
  facts — bare values, TSV rows, `key<TAB>value` lines, nothing aligned,
  humanised or hinted — not the raw wire.
- **The contract is a snapshot matrix that runs.** Every command × every
  fixture × every format, rendered and pinned. Prose transcripts (README, the
  agents skill, generated docs, the site's hero and transcript gate) derive
  from the matrix or are guarded against it; none is a hand-typed copy of
  what the binary printed one afternoon.

---

## 2. The path today

An executor `Output` reaches the terminal through five steps
(`crates/cli/src/render.rs`):

| Step | Where | What it does |
|---|---|---|
| 1. serialize | `output_to_string`, `render.rs:22-36` | `serde_json::to_value(output)` — the typed `Output` becomes an untyped envelope `{type, data}` |
| 2. humanize (Human, Raw) | `humanize_kv_bytes` `:844`, `humanize_committed_at` `:1018` | rewrites schema-declared `Bytes` fields to text; rewrites **every** `committed_at` at any depth to a local date string (`wall_clock::format_instant`, `%Y-%m-%d %H:%M:%S%.6f %:z`) |
| 3. dispatch on format | `value_to_string`, `:40-55` | Json → compact; Pretty → indented; Human → `render_human`; Raw → `render_raw` |
| 4a. Human: tag arms | `render_human`, `:105-147` | 23 explicit `type` tags (`pong`, `kv_versioned_value`, `json_*`, `vector_matches`, ten `inference_*`, `described`, …); **everything else → `render_human_data`** |
| 4b. Human: structural sniff | `render_human_data`, `:149-189` | null → `(nil)`; has `items` → one `scalar_summary` per item + page tail; has `matches` → key/score lines; has `found` → `(nil)` or the scalar; has `effect` → `mutation_summary` (`<kind> <subject> applied=<bool>`); scalar → itself; **else `to_string_pretty`** |
| 4c. Raw | `render_raw`, `:191-276` | its own tag arms, then its own sniff; **`effect` → prints nothing** (`:256`); `found:false` → nothing; fallback `raw_scalar` (objects → compact JSON) |
| 5. print | `print_rendered` `:93-98`, `run` `lib.rs:81-120` | stdout; errors via `render_error` → `human_error_line` (`:976`) on stderr; exit 0 on any `Ok`, 1 on an executor error, 2 on usage |

### 2.1 Where the four shapes come from

| #3306 example | Path taken | Why it looks like that |
|---|---|---|
| `kv get` → `hi` | 4a `kv_versioned_value` → `print_optional_data` | designed arm: unwraps `{found, value: {value, version, timestamp}}` to the stored text |
| `kv put` → `updated greeting applied=true` | 4b `effect` → `mutation_summary` `:816` | sniff arm: `effect.kind`, the first of `key`/`collection`/`space`/`graph` found, and `applied` verbatim |
| `branch list` → compact NDJSON | 4b `items` → `print_items` `:679` → `scalar_summary` `:943` | `scalar_summary` of an object is `serde_json::to_string` — one internal record per line |
| `info` → pretty JSON | 4b, no key matched → `to_string_pretty` | the fallback of the fallback |
| `kv history` → JSON with a local date | step 2 rewrote `committed_at` *before* 4b chose the NDJSON path | humanization is a pre-pass on the value, so its output lands inside whatever shape step 4 later picks |

So: the *designed* arms (`kv get`, `json get`, `describe`, the inference set)
read well; every command that reaches `render_human_data` is rendered by a
structural guess, and the guess has three exits — a scalar line, a
space-separated status line, or JSON. The default mode was never designed;
it is the union of one designed path and one fallback.

The IDL already knows what the fallback is guessing. `command-index.json`
carries `kind` and `response_model` for every command:

| Family (`response_model`) | Kinds | Commands | Human rendering today |
|---|---|---|---|
| `MutationAck<T>` | `mutation.{create,put,delete,bulk_delete,metadata_update,merge}` | 36 | `mutation_summary` status line; misses `not_found <k> applied=false` on stdout, exit 0 |
| `Maybe<T>` | `read.get` | 11 | KV/JSON designed; vector/event/graph node/edge records → `render_human_data` → pretty JSON |
| `Maybe<Vec<T>>` | `read.history` | 3 | NDJSON of records (with the local date inside) |
| `Page<T, C>` | `read.page`, `inference.models_page` | 21 | scalar items one per line (fine); record items → NDJSON; `(empty)`; `-- more:` tail on stdout |
| `SamplePage<T>` | `read.sample` | 4 | as `Page` |
| `SearchResult<T>` | `read.search`, `read.diagnostics` | 2 | `key\tscore` lines (designed, `print_vector_matches` `:795`) |
| `AnalyticsResult<T>` | `read.analytics` | 6 | pretty JSON |
| `StatusResponse<T>` | `read.summary`, `action.status`, `read.status` | 20 | `pong`, `described`, inference status designed; `info`/`health`/`metrics`/`config`/`ipc_status`/hub/branch `create`+`get`/arrow → pretty JSON |
| `StatusValue<T>` | `read.status` | 10 | scalar line (fine) |
| `BatchResult<T>` | `batch.*` | 14 | pretty JSON |
| inference bespoke (`ChatResponse`, `EmbeddingsResponse`, …) | `inference.runtime_op` | 10 | designed arms |

137 commands, ten families, one rule each — that is the whole contract. The
renderer instead carries 23 tag arms (`RENDERED_TAGS`, `render.rs:1114`)
over an `Output` enum of 114 variants — 91 variants reach the sniff — and
reads neither `kind` nor `response_model`.

One caveat that shapes R2: the declared `response_model` is **nominal**. For
~14 stable commands it names a family the wire does not carry — `branch
create` is declared `MutationAck<BranchItem>` and ships a bare `BranchItem`
with no `effect`/`commit`; `vector collection delete` is declared an ack and
ships `{"type":"bool","data":true}`; `graph meta` is declared `Maybe<T>` and
ships a nullable bare object where `kv get` ships `{found, value}` — and the
families themselves have two to five wire encodings each (#3313, filed from
this inventory). The *kind* is authored intent and is right everywhere; the
*shape* is the schema's, and only the schema's.

### 2.2 `--raw`

`render_raw` is a second, smaller sniff with a different fallback order.
Its contract is what a script can rely on, and today:

- a write prints **nothing** (`:256`) — success and a silent skip are
  indistinguishable on stdout (#3306);
- `kv get` prints the same text as human mode, including base64 for
  non-UTF-8 bytes (`humanize_kv_bytes` leaves those encoded), so `--raw` is a
  no-op for exactly the case a script needs it (#3116);
- a page prints one item per line, records as compact JSON — the same NDJSON
  as human mode.

### 2.3 Channels and exit codes

`run` (`lib.rs:94-118`) exits 0 for every `Ok(Output)`, 1 for an executor
error (rendered by `render_error` to stderr), 2 for usage; `doctor`, `update
--check` and pipe mode compute their own codes. A write whose target does
not exist is an `Ok` with `effect.applied = false` — the engine is right to
say so (the operation is idempotent) — and the CLI renders it on stdout as a
status line and exits 0. Nothing in the CLI ever decides that an `Ok`
deserves stderr or a non-zero exit, so a script cannot tell `deleted` from
`not_found` without parsing the line.

Two smaller paths sit beside the main one: the REPL prints a human-only
banner (`repl.rs:67-144`) and a `{"type":"context"}` line on `use`, and the
untyped `render_value` path (agents guide, `init`/`update` reports) skips the
humanizers entirely. Both are in the matrix's binary cells; neither changes
under this plan.

### 2.4 The playground

`crates/wasm/src/lib.rs` `execute_cli` is `strata_cli::run_line`: it parses
the line with the real clap grammar via `command_from_line`, which returns
the executor `Command` *and* the `Format` the line's flags chose (`None`
when they chose nothing), executes, and renders in that format — the
binary's stdout then its stderr, as one string. (Before #3312 the format
was dropped and every result rendered human.) There is no stderr and no
exit code in the browser, so whatever the human contract says about
channels must also read correctly when both channels are one stream.

A line typed at the REPL, piped to it, or sent from the playground is the
binary's argv without the leading `strata`, so it parses with the whole
top-level grammar — the *session* arguments (the positional database path,
`--db`, `--cache`, `--durability`, `--ipc`, `--read-only`) as readily as
the global flags and the command's own. Each reader used to run that parse
itself and pick the fields it wanted off `Cli`; whatever it forgot was
parsed and dropped: the playground's format (#3312), the REPL's format
(#3326), the session arguments on both (#3327). Since #3326 there is one
reader, `crates/cli/src/line.rs` `SessionLine::parse`, and both surfaces
call it: it refuses a session argument by name through `Cli::line_refusal`
(a lone word the grammar could only read as a database path is "not a
strata command"), refuses a line of flags with no command behind them, and
carries the line's `--branch` / `--space` / format as `Option`s — `None`
is "the line chose nothing", which the surface resolves against its
session (`line.format.unwrap_or(session_format)`), never against a
default of its own. It destructures `Cli` field by field, so a flag added
there does not compile until it is carried on the line or refused. The
REPL keeps only its own verbs (`exit`, `clear`, a lone `help`, `use`) in
front of it, and one function, `handle_line`, runs and reports a line for
the interactive and the piped loop alike, in the format the line resolved
to — which is also what makes a failed pipe line report once (#3328: the
pipe loop used to prefix `error:` onto a clap error that already began
with it).

### 2.5 What the tests reach

- `rendered_tag_inventory_matches_dispatch_arms` (`render.rs` tests) pins
  the *list* of special-cased tags, not what any of them prints.
- `command_examples.rs` (#3059) replays every IDL example through
  `output_to_string(…, Human)` and commits `command-examples.json`: 389
  example lines across 124 commands, guarded against a fresh replay. This is
  already a partial pin of human output, and it already carries a
  reproducibility verdict per line: **75 lines are not reproducible, and 72
  of those are the JSON-dump fallback** exposing masked internal fields
  (`branch_id`, `generation`, `state_revision`, …). A `branch list` table with
  the columns a person needs would be reproducible; the internal record is
  not. The verdict count is the metric this plan moves.
- `render.rs` has 23 literal `assert_eq!(human(…))` unit tests;
  `cli_execution.rs` has ~10 default-mode stdout assertions (value reads,
  `(nil)`, list-contains). Every assertion is a hand-typed literal; there is
  no snapshot file.
- Nothing pins `--raw`. Nothing pins stderr/exit for an `Ok`. Nothing pins
  that the playground's string equals the binary's stdout — `execute_cli`
  has no test asserting its text at all.

### 2.6 Who consumes human output

Surveyed for this plan; the list is short because every programmatic
consumer already uses `--json`.

| Consumer | Uses | Effect of a human-mode change |
|---|---|---|
| stratadb.org `scripts/verify-transcripts.mjs` | 23 exchanges through the real binary in default mode; substring match; 11 are `… applied=true`, 8 are JSON fragments of branch records (`"name": "risky"`) | **fails** — it is the release gate, and it encodes today's shapes |
| stratadb.org `src/lib/engine/heroScript.ts` | 8 hard-coded output beats (`created portfolio.value applied=true`, the branch JSON lines, two values); header comment says "kept in sync by hand" | shows stale text until regenerated |
| stratadb.org hero / `/playground` / `TryItOut.astro` | live `executeCli` | re-render automatically off the new wasm bundle |
| stratadb.org `get-started/quickstart.mdx:22` | `created greeting applied=true` | stale |
| `README.md:194` | `strata --cache ping  # pong 1.2.1` — the only hard human string; the kv/json lines show values | unchanged unless `ping` changes |
| `command-examples.json` | 389 rendered lines | guard fails; regenerate (the intended flow) |
| `agents_skill.md`, `guidance.rs`, `agents.rs` | commands and `--json` only, no rendered output | none |
| MCP (`mcp.rs:142`), VS Code extension, Python SDK | JSON envelopes only | none |
| `docs/inference/demo-end-to-end.sh` | exit codes only | none — and it becomes a consumer of the R5 exit contract |
| `docs/` | archived plans and the frozen v1.1.0 audit only | none |

---

## 3. The defects are symptoms

### Root A — the shape is re-derived by sniffing what the IDL already declares

`render_human_data` and `render_raw` both ask "does this object have
`items`? `found`? `effect`?" — a structural guess at the response-model
family. The IDL resolves that family for every command at generation time
and ships it in `command-index.json` (which the CLI already embeds for other
purposes). The guess has to have a fallback, and the fallback is JSON, which
is how the default mode grew a third JSON dialect. #3306's four shapes,
#3205's whole inventory, and the 72 non-reproducible example lines are this
root.

### Root B — humanization is a pre-pass on the value, not a rendering of a field

`humanize_committed_at` (#3112 S5) rewrites the instant *before* the shape is
chosen, so when the shape turns out to be NDJSON the date lands inside it.
The pass was the right instinct (a person wants a date) at the wrong layer
(it should happen where a table cell or a receipt line is printed, and only
there). "Never a formatted date inside a machine structure" (#3306) is not a
rule to add; it is what falls out of rendering fields in context.

### Root C — no contract for the `Ok`-but-nothing-changed outcome

The engine reports a miss on a write as `applied: false` and is right to.
The CLI has no rule for what a person or a script should see, so the sniff
prints the wire fact (`applied=false`) as prose. `--raw` prints nothing.
Exit is 0 in every case. #3306's "`applied=` reads as engine-speak" and
"`--raw` prints nothing for writes" are both this root.

### Root D — the gates are blind here

The only executable statement of human output is `command-examples.json`,
which covers the 124 commands that have an example, human mode only, and is
by design a *description* (it records what the renderer printed; its guard
detects drift, not wrongness). There is no pin of `--raw`, no pin of the
stderr/exit contract, no pin that the playground matches the binary, and the
site's transcript gate runs in another repository at release time. Each of
the four issues was found by a person looking at a screen — the homepage
(#3306), the docs page for KV (#3116), an inventory (#3312).

### Root E — the declared response model is nominal

The fix for Root A is "read the declaration". The inventory behind this
plan found that for ~14 **stable** commands the declaration does not
describe the wire (#3313): `branch create` says `MutationAck<BranchItem>`
and ships a bare record with no `effect`; `graph meta` says `Maybe<T>` and
ships a nullable object with no `found`; three history commands share one
`Maybe<Vec<T>>` declaration across two encodings; the six transitional
commands declare `MutationAck` over a `bool`, a page, or an index record,
naming result types that exist nowhere in `crates/executor/src`. Nothing
validates `response_model` against the schema the executor's `Output`
variant derives, so the reference pages print a `Returns:` that is not
there, and the SDK/agent surfaces inherit it.

For this plan the consequence is precise: the *kind* (`MutationAck`,
`Page`, …) is authored intent and is right for every command — it says
what a person should be told. The *shape* — which fields carry the
identity, whether a miss is `found: false` or `null` — is the schema's, and
only the schema's. R2 keys the rule on the kind and the fields on the
schema; the rows where the two disagree go in a shrink-only allowlist so
the docs stop lying before the wire is normalized (which is a wire change,
and a major's).

---

## 4. Decisions this proposes

### R1 — one rule per family

The rule is keyed by the IDL `kind` (equivalently the `response_model`
family). Column and identity choices are declared per command (R2). The table is
the human contract; the last two columns are what changes for a reader.

| Family | Human (stdout unless noted) | Raw | Today → after (example) |
|---|---|---|---|
| `MutationAck<T>` | one line: `<verb> <identity>` — verb from `effect.kind` (`created` / `updated` / `deleted` / `merged` / …), identity from the command's declared identity field (`key`, `collection`, `space`, `graph`, branch `name`). Bulk: `deleted 12 vectors` (zero matches is not a miss, Q14). `applied: false` with effect kind `not_found` (or a bare `false` wire) on a named target: `no such key: greeting` to **stderr** as feedback, nothing on stdout, exit **0** — a delete of nothing is idempotent (Q2). `applied: false` with kind `unchanged` (a create that met an existing target) is a hit whose verb says so: `unchanged space staging` | the identity, one line; on a miss nothing on stdout, same stderr, exit 0 | `updated greeting applied=true` → `updated greeting`; `not_found x applied=false` → stderr `no such key: x` |
| `Maybe<T>` | found: the value as a reader wants it — KV text unchanged (#3306); `json get` the JSON leaf unchanged; a record (vector, event, graph node/edge, graph meta, ontology) as **key-value lines** of its declared `fields` (`key  n1`, `dimension  4`, …), never a JSON dump. Miss: `(nil)` on stdout, exit 0 (a read miss is an answer) | one rule from the declaration: a command that declares a **value** prints it verbatim — the stored bytes for KV (#3116), the JSON leaf for JSON, the scalar for `config <key>`; a command that declares **fields** prints `key<TAB>value` lines of the same fields, exactly as a status object does (Q16). No per-command raw projection (review round 3). Miss: nothing | `vector get` pretty JSON → key-value lines; `--raw kv get` on `00 01 ff` → three raw bytes; `--raw graph get-edge` → `src<TAB>alice` … lines |
| `Maybe<Vec<T>>` (history) | a table (R1-table), oldest last, columns declared per DTO (`VERSION`, `COMMITTED`, `VALUE`); miss `(nil)` | one row per line, tab-separated declared columns, no header | JSON-with-a-date lines → a table whose `COMMITTED` cell is the date |
| `Page<T, C>` | scalar items (`Page<Bytes>`, `Page<String>`): one per line, no header (unchanged). Record items: a table with a header row and the declared columns. Empty: `(empty)`. `has_more`: the `-- more: add --cursor …` hint moves to **stderr** (Q8) so `strata kv list \| wc -l` counts keys | one item per line, the declared columns tab-separated (R4); no header, no hint, nothing when empty | `branch list` NDJSON → `NAME  PARENT  STATUS  …` |
| `SamplePage<T>` | as `Page`; the `-- sampled N of M` stderr notice **only when N < M** — a sample that returned the whole population needs no notice (review round 3) | as `Page` | — |
| `SearchResult<T>` | a table: `KEY  SCORE  METADATA` (today's `key\tscore` lines, given a header); diagnostics as key-value lines after a blank line | the declared columns tab-separated (`key\tscore\tmetadata` — one field more than today, R4) | — |
| `AnalyticsResult<T>` | a table, columns declared per algorithm (`NODE  SCORE`, `NODE  COMPONENT`, …) | rows, tab-separated | pretty JSON → table |
| `StatusResponse<T>` | **key-value lines** of the fields the command declares (`display.fields`, in that order — the family fixes the layout, the command decides which facts appear; `--json` is the complete record): `branch_count  2`, nested objects indented one level, arrays of objects as an indented table, scalar arrays space-joined, labels = wire names unless a `header:` is declared (Q18); the designed arms (`pong`, `described`, `inference status`) stay as they are and are pinned as bespoke | the single declared identity/answer when the command has one (`ping` → version, `branch create` → name), else `key<TAB>value` lines of the same declared fields, wire names as keys, dotted for nesting, arrays as one compact-JSON field, null as an empty value (Q16) | `info` pretty JSON → key-value lines; `hub get-dataset` 27 fields → 15 |
| `StatusValue<T>` | the scalar (unchanged) | the scalar | — |
| `BatchResult<T>` | a table, one row per item: `#  STATUS  [EFFECT]  <declared columns>` (+ `ERROR` when any item failed); a stderr summary `-- itemwise: 2 ok, 1 miss` **only when an item's own `status` is not `ok`**; full success is the table alone (Q11). Never `applied` — every read batch carries `applied: false` because a read applies nothing (review round 3 caught the catalog getting this wrong) — and, as S3b found, not the envelope's `status` either: a mutation batch reports `partial` when some items applied and others were no-ops, and every one of those items is `ok`, so the envelope's word would print `-- itemwise: 2 ok` under a table whose EFFECT column already said which was which. The envelope's `status` stays the machine's signal in `--json` | the same rows as TSV | pretty JSON → table |
| inference bespoke | the ten existing arms, unchanged, pinned | unchanged | — |
| error | `code: message` + `hint:` + `ref:` lines to **stderr**, exit 1 (unchanged — this one was designed, first-run D4) | same | — |

**Tables** (R1-table): a header row in upper case, columns separated by two
spaces and left-aligned to the widest cell, no borders, no colour, numbers
right-aligned — the `kubectl` / `gh` style, which pastes into an issue and
diffs cleanly. Dates render per R3. Bytes render as text when UTF-8, else
as `base64:<…>` — labelled, so a reader knows (#3116's second half).

**What does not change:** `--json` is byte-identical before and after every
slice; the matrix pins that. (`--pretty` was the second envelope format until
Q5 deleted it at S4.)

### R2 — the declaration lives in the IDL, the renderer interprets it

*Amended 2026-09-11 by S0b (#3314) to the vocabulary as built; the original
proposal is in the history of this file. S1 (2026-09-11) made the renderer
read `mutation_ack` declarations (`Invocation` in `crates/cli/src/render.rs`);
S2 (2026-09-12) made it read `columns` under `page`, `history` and `search`
and `map` under `analytics`, laid out by one `Table` (`crates/cli/src/table.rs`);
S3a (2026-09-12) made it read `value`, `fields` and `receipt` under
`optional`, `status_value` and `status_sections`, and rendered the `search`
rule's declared `fields` block under its table; S3b (2026-09-12) made it read
`batch`, gave `branch diff` the hand-written arm it never had, and left the
renderer with exactly two paths — a declaration, or a `bespoke` arm.*

Two additions to the authored IDL, both validated when the CLI index is
generated (`strata-idl generate-cli`, gated by `check-cli`):

- `kinds.yaml`: every kind carries `render: <rule>` naming one of the R1
  rules — `mutation_ack`, `optional`, `history`, `page`, `search`,
  `analytics`, `status_value`, `status_sections`, `batch`. Twenty-two lines,
  one per kind; a kind cannot be `bespoke`, because a hand-written arm is a
  property of one command, not of an operation category.
- `commands/*.yaml`: **every** command carries `display:` — either the word
  `bespoke` (a hand-written renderer arm; thirteen today, pinned by
  `crates/executor/tests/idl_display.rs`: `admin.describe`, `admin.ping`,
  `branch.diff`, and the ten `inference.*` arms — `admin.ipc_stop` became a
  declared `fields` record at S3a, which left its conditional sentence to
  #3332) or exactly one of five shapes:

  | Shape | Keys | Allowed under |
  |---|---|---|
  | **receipt** | `receipt: "<template>"`; `identity: [<pointer>…]`; `noun: <word>` | `mutation_ack` (identity required), `status_sections` (identity forbidden) |
  | **value** | `value: <pointer>`; `as:` | `optional`, `status_value` |
  | **fields** | `fields: [{field, header?, as?, fields?}…]` | `optional`, `status_value`, `status_sections` |
  | **columns** | `columns: [{field, header?, as?}…]`; `fields:` only under `search` | `history`, `page`, `search`, `batch`, `status_value`, `status_sections` |
  | **map** | `map: <pointer>`; `header:`; `sort: key \| asc \| desc` | `analytics` |

  The family (via the kind's rule) fixes the layout; the shape says which
  facts appear, in which order, and how. A shape the rule cannot render, two
  shapes at once, or a key from another shape is a `check-cli` failure that
  names the command and the reason.

**Pointers.** Every `/…` string is a JSON pointer into the command's
generated schema document (`generated/schemas/<id>.json`): `/data/…` walks
the response payload, `/request/…` walks the request. `*` steps into array
items or map values; a decimal index steps into one array item. A pointer
that names a field the wire does not carry fails with the fields the wire
does carry (`/request has no field name (fields: branch, collection, space,
type)`); a pointer that steps into a scalar, or steps into every item where
one value is needed, fails the same way. The `/request` root exists for the
two `bool`-wire acks (`json index drop`, `vector collection delete`): the
receipt names what the request named (`deleted collection
{/request/collection}`) until the #3313 wire normalisation puts the identity
on the wire (Q15). Those are the only two `/request` pointers today.

**Receipts.** A `receipt` is a template: literal text plus `{/pointer}`
placeholders, each taking at most one filter — `|bytes` (a base64 payload
shown as text — the CLI's own reports have no declaration and take this
presentation from the value's own shape instead, #3339), `|size` (a byte count in decimal units — `135 B`, `41 kB`,
`536.9 MB`, `6.7 GB`; one decimal, trimmed when it adds nothing, and a unit
promoted when rounding fills it, #3335), `|len` (an array's
length), `|plural:<noun>` (a count with its noun) — plus `{verb}`, which
reads `/data/effect/kind` and is refused on a response without an `effect`
(those acks — branch create/fork/merge, bulk insert, ontology freeze, index
and collection create — spell their verb out). A placeholder must land on a
scalar or carry the filter that makes one; the guard types every filter
against the schema (`|len` needs an array, but `/data/key` is a base64
string). `identity` lists the pointers `--raw` prints for a write (R4), with
the same filter grammar, no `verb`, no repeats. `noun` is the word in the
idempotent miss line (`no such key: greeting`, Q2) and needs an applied
signal on the wire (`/data/effect/applied`, or a bare boolean `/data`) —
without one a miss cannot be told from a hit, so the guard refuses it. A
miss is the applied signal `false` **and** an effect kind of `not_found`
(or no kind at all, the bare-boolean wires): an `unchanged` effect that did
not apply — `space create` on an existing space, the only producer today —
is a hit, rendered through the receipt with its own verb, never a false
`no such space` (S1). A miss line renders the identity placeholders in
human form, space-joined, in every format; an absent or null placeholder
value renders as `(nil)` in human mode and as an empty cell in `--raw`.
The identity law is keyed by rule: under `mutation_ack` a receipt **must**
declare `identity`, because `--raw` for a write is its identity; under
`status_sections` (action receipts: `arrow export`, `arrow import`, `hub
clone`) it **must not**, because `--raw` there is the record's
key/value lines (Q16) and a second projection would be a second rule.

**Values and fields.** A `read.get` command declares exactly one of `value`
(the payload — `kv get`, `json get`, `config <key>`; human prints it, `--raw`
prints it verbatim) or `fields` (a record — human prints the key/value
block, `--raw` prints `key<TAB>value` lines of the same fields). One
declaration drives both modes; there is no `raw:` projection (review round 3
found five raw behaviours for one family and replaced them with this rule).
`fields` **selects as well as orders**: `hub get-dataset` declares 15 of its
27 wire fields, `admin info` drops `open` and `created`, `branch get` drops
`branch_id` and `state_revision`; `--json` is the complete record, which is
what it is for. Every status command declares its list even when the list is
"all of them", so a new wire field is a `check-cli` failure until someone
decides where it goes. *Implemented 2026-09-13 (#3358 F12): the guard had
validated only the fields a declaration selected, so it could never notice
what one omitted — a field added to a payload was silently never shown. The
record shapes now account for every field of their payload: shown, or named
in `DELIBERATELY_UNSHOWN` (`idl_tooling/display.rs`) with the reason. The
curations this paragraph describes in prose are entries there now, so they
are checked rather than remembered. Page and batch commands are out of scope:
their rule decides what shows, and their envelope — `items`, `cursor`,
`has_more`, `applied` — is not a per-command decision.* A record-valued field may carry its own `fields:`
(one level down, the same selection rule: `admin info` shows three of
`memory_budget`'s fields); a nested pointer must stay under its parent, and
a field cannot pair `as` with nested `fields`. `header:` is optional and
defaults to the wire name (Q18); it is permitted where the wire name would
misstate the humanised value — the one class today is byte counts,
`size_bytes` shown as `1 kB` under `SIZE`. A `header` is a human-mode label
only; `--raw` keys are always the wire names.

**Presentation.** `as:` on a `value`, field or column is one of `bytes`
(a base64 string), `json` (a structured or untyped value, shown compact),
`date` (an integer timestamp, R3), `size` (an integer byte count), `float`
(a number), `list` (an array of scalars, space-joined), `table` (an array of
records, an indented table inside a fields block — the `artifact_sources`
of vector diagnostics). Each is typed against the schema (`as: date needs an
integer, but /data/name is a string`).

**Columns and maps.** A column steps into exactly one row array with `/*`,
and every column of a table reads from the same array; the batch rule
additionally pins the array to `/data/items/*`, so a batch response that
grows a second array cannot silently become the table. Rows come from an
array, never from a map. `columns` pairs with `fields` only under `search`,
where the fields are the diagnostics block that follows the table. A `map`
names an object keyed by node whose values are scalars (the six analytics
payloads), with `header` for the value column and `sort` for row order.

The declaration is keyed by command, not by DTO, because one DTO carries
different identity fields under different commands (`graph_delete_result`
is `graph`, `graph`+`node_id`, or `graph`+`src`+`edge_type`+`dst` depending
on the verb) — the catalog's finding, §4 there.

**The guard** is what makes this derive-never-copy (#3250, #3244, #3226):
every kind has a `render:` and every command a `display:` (a missing one
does not parse, so it fails `check` before `check-cli` is reached); the
shape fits the rule; every pointer resolves in that command's schema
document; every placeholder, filter and `as:` is typed. There is no
fallback to JSON. A renamed DTO field is a `check-cli` failure, not an empty column, and
a phantom result type (#3313's `VectorCollectionDelete`) cannot be pointed
at because it has no schema. The guard runs at authoring time only: the CLI
embeds the resolved index and re-runs the cheap rule ⇔ shape check on it,
never the schema walk.

**`display:` is CLI-only.** The Python SDK is a sibling of the CLI, not a
wrapper around it: both derive from the IDL, and an SDK method returns the
typed wire record (`db.branch.get("x")` → a `BranchItem` with every field),
never the CLI's curated subset or its receipt strings. The SDK generator
never reads `display:` — and cannot: the declaration is resolved into
`cli-command-index.json` (the index the CLI embeds), never into
`command-index.json`, which is what the SDK vendors. `generate` and `check`
reject a `command-index.json` that carries a `display` or `render` key
anywhere in its tree, so a future generator cannot quietly start consuming
it. The same holds for MCP and the VS Code extension, which consume
`--json`. The SDK's own redesign (`db.kv.put("greeting", b"hello")`,
`db.json.set("user", {…})`, `db.branch.fork("experiment")`,
`db.vector.query("docs", vector, k=10)`) follows the #3313 wire
normalisation: the nominal-vs-wire divergences are fixed or declared first,
then the Python surface is designed on a wire that is true. Sequencing
decided 2026-09-11: CLI contract, then #3313, then the Python SDK.

**Kind selects the rule; schema supplies the shape** (Root E). The two
places the wire disagrees with the declared family today are handled
without a special case in the renderer, and both are S0c/S1 work — S0b
declares, it does not resolve encodings:

- *Encoding* — whether a `Maybe` is `{found, value}` or a nullable `data`,
  whether a history is `{items}` or a bare array, whether a page carries
  a `total_count` — is read from the schema at generation time and resolved
  into the index as a flat `encoding` (`found_value` / `nullable` for
  `optional`, `items` / `array` for `history`, `page` / `sample_page` for
  `page` — the latter the four `*.sample` commands, whose `-- sampled N of
  M` notice needs the population; `null` under every other rule; amended
  from `optional.encoding` / `history.encoding` in S0c, where the field
  landed beside `render` rather than nested under it; `page` added in S2).
  An encoding is an accepted
  spelling of the declared family, never a divergence, so it earns no row in
  the ledger below and no `transitional` mark. The renderer never sniffs it.
- *Verb* — `mutation_ack` reads `effect.kind` through `{verb}` when the
  schema has an `effect`; when it does not, the receipt spells the verb
  (`created branch {/data/name}`), which the guard enforces by refusing
  `{verb}` on an effect-less response. The `bool` wires use `/request`
  pointers as above (Q15).

The rows where declaration and schema disagree live in a shrink-only
`response-model-divergences.yaml` beside `cross-surface-divergences.yaml`
(#3313's ask); `check` fails if a row leaves the wire unchanged but the
allowlist grows. Every ledgered command carries `wire_status: transitional`
and its reference page says which shape the wire carries today; a
`transitional` command that conforms gets the generic "not yet frozen"
sentence instead. S0 lands the declarations and the allowlist as the
executable form of the inventory; nothing renders differently until S1.

The generator resolves the declaration into `cli-command-index.json` per
command; the CLI already embeds that index, and `render.rs` becomes an
interpreter: `render` picks the rule, `display` supplies the pointers. The
23 tag arms shrink to the fourteen bespoke arms; `render_human_data` and the
`_ => to_string_pretty` exit are deleted. A new command gets its human
output by declaring its shape, which it must do anyway to pass `check-cli`.

Why the IDL and not a Rust table beside the renderer: a Rust table is a copy
of field names that the schema already holds. The pointer guard is the
whole difference between a declaration and a restatement.

### R3 — a date is rendered in a cell, never in a value

`humanize_committed_at` is deleted. The `page`, `history`, `mutation_ack`
and `status_sections` rules format a field declared `as: date` at the point
of printing it. `--json` / `--pretty` keep epoch micros; `--raw` prints
epoch micros too (a script wants a number). *S2 (2026-09-12) renders `as:
date` in every declared table cell — `2026-09-10 20:19:44.123456 UTC`
human, the micros raw — and a declared command never reaches the pre-pass;
the pre-pass itself stays for the undeclared families until S4 deletes it.* Logical
clocks (`timestamp`,
`version`, `created_at` on a branch record — #3112) are never dates; a
declaration marking one `as: date` is a `check` error because the schema
type for a logical clock is not the wall-clock newtype (a guard the S0
schema pass will confirm is expressible; if it is not, the field allowlist
is the guard).

*Resolved 2026-09-13 (#3358 F11): it is **not** expressible, so the
allowlist is the guard. `schemars` flattens core's `Timestamp` newtype to a
bare `uint64`, and the executor's response DTOs carry instants as plain
`u64` besides, so nothing in a generated schema separates an instant from
any other counter — `as: date` accepted every integer. `WALL_CLOCK_SITES` in
`idl_tooling/display.rs` now names the eight (command, pointer) sites where a
date may be declared; anything else is a `check-cli` error. The list is keyed
on the command because the field names do not distinguish them: an event's
`timestamp` is an instant, a KV history row's is a position on the commit
timeline.*

Human date form: Q3.

### R4 — `--raw` is the declared columns as TSV, and bytes are bytes

**The mental model, stated so nobody reads the flag name literally:**
`--raw` is the **shell-composable** form of the same declared facts the
human mode shows — not the raw wire. The one unmodified representation of
what the engine returned is `--json`. `--raw` exists so that `cut`, `wc`,
`xargs`, `while read` and a `$(…)` substitution get one value per line and
one field per tab, with no header, hint or sentinel to strip. The flag's
help text is rewritten to say exactly this in S1 (today it says "raw
values"); no new flag is introduced — the name is fine once the docs are
honest about it.

`--raw` prints, per family, exactly the R1 "Raw" column: the identity of a
write, the stored bytes of a read verbatim (#3116), one row per item for a
page / history / search / analytics with the **declared columns
tab-separated** (the same columns the human table shows — a scan that
prints only keys is not a scan; decided 2026-09-11, replacing the earlier
"identity only" wording), the scalar for a status value, `key<TAB>value`
lines of the **declared `fields`** for a status object (Q16; the keys are
wire names, never a `header:`). Nothing else — no header, no hint, no
sentinel; a null or absent field is an empty cell, never `-` or `null`;
floats keep full precision; a multi-line string is escaped (`\n`). A write
that changed nothing prints nothing on stdout (the exit code and stderr
carry the outcome, R5). `humanize_kv_bytes` stops applying to raw KV reads;
it still applies to human mode.

### R5 — stdout is the answer, stderr is everything else, exit says whether it happened

- stdout: the data the command was asked for, in the chosen format.
- stderr: errors, and the diagnostic lines — the pagination hint, the
  `-- sample`/`-- truncated` notices, the miss line of a write, the batch
  summary when an item missed or failed. **A diagnostic line appears only
  when something needs attention**; a fully successful command writes
  nothing to stderr (Q11 — `-- itemwise: 2 ok` after a clean batch was
  noise, and is gone).
- exit 0: the command did what it was asked — including a delete of
  something that was not there, which is idempotent (Q2, decided on review
  2026-09-11). exit 1: it could not (an executor error). exit 2: usage.
  There is no exit code for "nothing to do": the shell agrees with the
  wire, which says `Ok`.
- A missed write in human and `--raw` mode: `no such key: k` on stderr as
  feedback, nothing on stdout, exit 0. In `--json` mode the `applied: false`
  envelope goes to stdout, exit 0, and **nothing** goes to stderr — in
  `--json` mode stderr carries only error envelopes, so a machine consumer
  reads one channel for the record and the other only when the exit code
  says to. If a script ever needs the assertive form, it is an opt-in flag
  (`--require-existing` → exit 1 on a miss), added when someone asks for it,
  not now.
- Exit codes are the same in every format. A script that switches `--json`
  on must not change its control flow.

The playground (one stream, no exit) shows stdout, then stderr, in that
order (#3312 fix carries the format through as well).

### R6 — no TTY sniffing

Content never depends on whether stdout is a terminal. The playground has
no terminal, agents pipe, and the site's transcript gate must see what the
docs show. `gh` and `psql` do adapt to a pipe; the cost is that
`strata kv list | head` prints something the documentation never showed.
Explicit flags only (`--json`, `--raw`). Colour, if ever, is the one thing a
TTY may add, and it is out of scope here.

### R7 — prose derives

- `command-examples.json` stays the derived transcript artifact; its
  `reproducible: false` count is a number to drive down, not a fact of life,
  and is asserted shrink-only (50 as of S5).
- `generate-docs` emits each command's human transcript (from
  `command-examples.json`) onto its reference page beside the wire example,
  so the site's per-command pages show what a person sees. The page renders
  each `$` line itself and holds it equal to the captured one, so the two
  artifacts — written by two crates, committed separately — cannot describe
  different examples.
- A step whose output exposes a run- or machine-dependent value is not
  published as captured: the masked renders differ exactly where such a value
  reached the output, so each token that differs is elided to `…` under a
  legend. Publishing the sentinel instead (`masked`, an epoch-0 date) would
  assert output the binary never printed, which is what R7 exists to stop.
- README, the agents skill (`agents_skill.md`) and `docs/` fenced
  `$ strata …` transcripts that show *output* are guarded: every shown
  output line must equal the matrix cell for that command and fixture, or
  the fence is marked `<!-- illustrative -->`. The clap-parse guard (S4a of
  the resolver plan) already proves the *inputs* parse; this is the output
  half. A version quoted in an output comment (`# pong 1.2.1`) is held to the
  version being built — the README ships inside every binary tarball, and a
  stale one shipped at v1.1.0.
- stratadb.org's hero script and `verify-transcripts.mjs` are regenerated at
  the release that ships S1–S3, in the release PR, from the same cells.

---

## 5. The contract matrix

The matrix is the acceptance instrument for every slice and the thing that
outlives them.

### 5.1 Dimensions

| Dimension | Values |
|---|---|
| command | all 137 from `generated/command-index.json` (derived, never a literal count) |
| fixture | the command's `fixtures.response` and `responses` alternates, every `error_cases[].expected_error`, and the synthetic edges derived from the primary response wherever its shape admits one — `edge:items=[]` (empty page), `edge:has_more=true` (a page with a cursor), `edge:found=false`, `edge:applied=false`, `edge:committed_at=fixed` (the Q3 instant; `=null` too once a fixture carries a real instant — today every fixture is a replay capture with `null`), `edge:value=non-utf8` (a KV value that is not text, #3116). A derived edge that does not deserialize into `Output` is a harness bug, not a skipped cell |
| format | `human`, `raw`, `json`, `pretty` |
| channel (binary cells only) | stdout, stderr, exit code |

### 5.2 Where it lives

- `crates/cli/tests/output_contract.rs` — in-process cells: every
  (command, fixture, format) rendered through `output_to_string` /
  `error_to_string` (the same functions the binary and the playground call)
  and compared to `crates/cli/tests/output-contract/<family>.txt`, one block
  per cell under a `### <command> · <fixture> · <format>` heading (one file
  per family so a slice's diff is one family's file). `human` and `raw` are
  pinned as data; `json` and `pretty` are asserted as invariants — each must
  be the unmodified wire record, compact or pretty-printed — because that is
  the whole of their contract and it never changes. Blessed once by
  `STRATA_OUTPUT_BLESS=1`; otherwise byte-for-byte. The cells run in a
  child process with `TZ=UTC` and a scrubbed environment so the local-date
  formatter is deterministic on every machine (the same harness shape as
  `resolution_matrix.rs`).
- Binary cells, same file: a fixed script of ~25 exchanges against a
  temporary durable database through `CARGO_BIN_EXE_strata` — a create, an
  update, a delete, a missed delete, a read, a missed read, a two-page list,
  an empty list, a history, `info`, a usage error, an executor error — each
  in `human`, `raw` and `json`, capturing stdout, stderr and the exit code
  separately. This is the only place the R5 contract is observable.
- Playground cell: for every exchange of the binary script, `execute_cli`'s
  string equals stdout ⧺ stderr of the binary cell (the wasm crate's path is
  `strata_cli::run_line`, compiled natively and run against a cache executor;
  `info` is skipped because it reports the target itself).
- `command-examples.json` cross-check: every reproducible example line's
  `out` equals the matrix's human render of the same replayed step (the two
  artifacts must not drift from each other; the guard fails if they do). The
  example steps are not pinned a second time — `command-examples.json` is
  already the blessed artifact for them.

### 5.3 What S0 pins, and falsification

S0 blesses **today's output, unchanged**, including every shape this
document calls wrong. The pin is a description; the contract (§4) is the
aspiration; the difference between them, cell by cell, is the work, and
each slice's PR carries the snapshot diff as its review surface and its
release note. There is no `KNOWN_RED` list because nothing in S0 is
asserted to be right — a cell is red only when a slice changed something it
did not mean to.

Before S0 merges, three plants must go red on exactly the predicted cells:
change the verb in `mutation_summary` (only the `MutationAck` human cells
whose fixture carries an `effect` — the 36 minus the #3313 rows, which
today fall through to JSON and are themselves a cell the plant must *not*
touch); remove `humanize_committed_at` (only cells whose fixture carries a
numeric `committed_at`); make `render_raw` print the identity for writes
(the same `effect`-carrying raw cells). A guard that has not been shown to
fail is not evidence.

### 5.4 Cells where contract and code disagree today

Grouped by the slice that closes them; counts from the §2.1 table.

| Slice | Cells | What changes |
|---|---|---|
| S1 (landed) | 36 `MutationAck` × human, × raw; the binary cells for a missed delete | `applied=` gone; miss → stderr line, exit stays 0, nothing on stdout; raw echoes identity |
| S2 (landed) | 21 `Page` + 4 `SamplePage` + 3 history + 6 analytics + 2 search × human, × raw | tables with declared columns; hint to stderr (sample notice only when partial); dates in cells; raw = declared columns as TSV |
| S3a (landed) | 20 `StatusResponse` (minus the 3 bespoke) + 11 record `Maybe` + the `search` diagnostics block × human, × raw | key-value lines, receipts, declared tables inside a record; raw = `key<TAB>value` under wire names |
| S3b (landed) | 14 `BatchResult` + `branch diff` + `ping` raw × human, × raw | one row per item; batch summary on stderr only when an item's own status is not `ok`; `ping --raw` is the version |
| S4 | KV `Maybe` × raw, every `committed_at` cell | raw bytes verbatim (#3116); `humanize_committed_at` deleted (R3 lands with S2's tables, so S4 is the deletion of the pre-pass and the raw-bytes change) |
| — | every `json` and `pretty` cell | **none** — pinned unchanged through every slice |

---

## 6. Slices

Each slice is one PR, ≤1,500 LOC, with the matrix as its acceptance: the
slice's cells change as §5.4 predicts, no other cell moves, and the `json` /
`pretty` cells never move. Every slice that changes a human or raw cell is
a visible CLI change and carries release notes in the PR body.

| Slice | Content | Wire change | Closes |
|---|---|---|---|
| **S0** (three PRs under the ≤1,500-LOC rule) | **S0a** — the matrix (§5.2: in-process, binary, examples cross-check; playground cells come with #3312), blessed on today's output; the three plants; both design documents committed. **S0b** — `render:` per kind in `kinds.yaml` and `display:` per command in `commands/*.yaml` (R2 as amended: `receipt`/`identity`/`noun`, `value`, `fields`, `columns`, `map`, `header`, `as`, `sort`, or `bespoke`), resolved into `cli-command-index.json` and **declared and guarded but not yet read** by the renderer (so the declaration review happens on a PR that changes no output), with the guard that `command-index.json` never carries it (the SDK boundary). **S0c** — the #3313 `check` guard (family ⇔ schema) with its shrink-only `response-model-divergences.yaml`, `wire_status: transitional` on the divergent rows, and the declarations that are simply wrong corrected (Q13) — a docs-only change to `Returns:` | none (docs `Returns:` lines change in S0c) | #3313 asks 1–2 |
| **#3312** (its own small PR, before S1) | `command_from_line` returns the format with the command; `execute_cli` renders with it; playground cells added | none | #3312 |
| **#3327** (its own small PR, before S1) | session arguments on a REPL/playground line are refused by name from one shared `Cli::line_refusal`; a typo'd lone verb is "not a strata command" instead of silently ignored | none (CLI text; a refused pipe line exits 1 as any pipe error does; release note) | #3327 |
| **#3326 + #3328** (one small PR, after S1) | one line reader, `SessionLine::parse`, for the REPL, the pipe and the playground; a line's `--json` / `--raw` / `--output-format` wins over the session's format for its answer and its error alike; a line of flags with no command is refused, not dropped; the REPL's `handle_line` is the one place a line is run and reported, so a failed pipe line reports once and in the same shape as an interactive one (code-led line or JSON envelope, not a bare `error: <message>`) | none (CLI text; exit codes unchanged; release note) | #3326, #3328 |
| **S1** (landed 2026-09-11) | R1 `mutation_ack` rule from the declaration; R5 stderr line for a `not_found` miss (exit stays 0; `unchanged` is a hit); R4 raw identity for writes and the `--raw` help text rewritten to "shell-composable"; `mutation_summary` deleted; the harness snapshots stderr as its own cell | none (CLI text; exit codes unchanged; release note) | #3306 (writes, `--raw` writes) |
| **S2** (landed 2026-09-12) | R1-table (`crates/cli/src/table.rs`); `page`, `history`, `search`, `analytics` rules from `display.columns` / `display.map` through `Invocation`; R3 dates in cells; `-- more` / `-- sampled` to stderr, human only; raw = declared columns as TSV; the `page` / `sample_page` encodings; the harness's `edge:total_count=more` cell | none (release note) | #3306 (lists, dates), #3205 §3/§5 |
| **S3a** (landed 2026-09-12) | `optional`, `status_value` and `status_sections` from `value` / `fields` / `columns` / `receipt`: label-and-value blocks, nested records one level in, an `as: table` field as an indented table whose columns `generate-cli` resolves from the row schema, and an action's one-line receipt (raw: the action's record). The `search` rule's declared `fields` block renders under its table (human only). `admin.ipc_stop` moves from bespoke to a declared record; the optional/status tag arms, their helpers and the KV/preview byte pre-pass arms are **deleted** | none (release note) | — |
| **S3b** (landed 2026-09-12) | `batch` rule from `columns` plus the cells every batch shows (`#`, `STATUS`, `EFFECT` for a write, `ERROR` when an item failed) and the stderr tally; `branch diff`'s hand-written arm (its rows sit two `*` deep, so no `columns:` pointer can describe them); `ping --raw` is the version; `humanize_kv_bytes`, `humanize_committed_at`, `format_instant` and the `items`/`found`/`value` sniffs **deleted**; an output with neither a declaration nor an arm is now reported, not guessed at | none (release note) | #3306 (admin), #3205 §1/§2/§4 |
| **S4** (landed 2026-09-12) | R4 raw bytes verbatim: `Rendered`'s stdout becomes `Answer::Text | Answer::Bytes`, and a `--raw` read of a declared `as: bytes` value answers with the stored bytes and no newline of the renderer's own, so `strata --raw kv get k > payload.bin` is the file `kv put --file` stored. A shared stream (the REPL, a pipe) adds the separator a file must not have — `print_output` takes a `Channel`. Where bytes cannot travel (a snapshot cell, the playground transcript) `Answer::text()` escapes them as `<bytes:0001ff>`. Q5: `Pretty` and the hidden `--output-format` deleted | none on the wire (CLI text; release note) | #3116 |
| **S5 core** (landed 2026-09-12) | R7 in this repo: every reference page's CLI tab is a transcript, each step's captured output under its command, checked against the page's own rendering so a stale `command-examples.json` fails `generate-docs` instead of publishing an older binary's output; a non-reproducible step is elided to `…` per varying token (the two masked renders differ exactly where a volatile value reached the output) rather than publishing the sentinel — no `masked`, no 1970 dates — under a legend; `crates/cli/tests/prose_transcripts.rs` holds README / `agents_skill.md` / `docs/**` fenced transcripts to the captures or to an explicit `<!-- illustrative -->`, and a version in an output comment to the version being built; `reproducible: false` asserted shrink-only at 50 | none | #3205 acceptance criteria |
| **S5 site** (at the release) | stratadb.org's hero script and `verify-transcripts.mjs` regenerated from the same captures, and the JSON-shaped output in its learn/get-started pages replaced | none | stratadb.org #5–#8 |

Sequencing: S0 → #3312 → S1 → S2 → S3a → S3b → S4 → S5, each depending on
the declaration the previous one made the renderer read. S1–S3 can ship in one
release or three; the site regeneration (S5) happens once, at the release
that carries the last of them, because the website documents the released
binary, not `main`.

Not in this plan: colour, a pager, TTY adaptation (R6), REPL banner and
prompt text (#2998, done), MCP tool output (JSON by contract), the VS Code
extension (reads `--json`, #3205 non-goal), the Python SDK (library, not
CLI), `--json` envelope changes of any kind.

---

## 7. Who is reading this

Two readers, and the contract is written for both at once.

A person at a terminal or on the homepage hero, who reads the default mode
and has never seen the IDL: the R1 human column is written so that a reader
who has seen `kv put` can predict `branch list`, which is #3306's test.

A coding agent, which runs `--json` and never sees human mode — except when
it reads the docs. The site's per-command pages, the agents skill and the
README show human transcripts as the illustration and the envelope as the
contract; R7 keeps the illustration honest.

---

## 8. Where we stand

Verified against `main` at `60db96ac`.

| Area | State | Gap |
|---|---|---|
| Shape declaration | IDL resolves `kind` and `response_model` for all 137 commands; `dto-inventory.yaml` registers 100+ models | nothing renders from them; no `display` declaration exists |
| Declaration ⇔ wire | `kind` is right everywhere; `response_model` is checked against nothing | nominal for ~14 stable + 6 transitional commands (#3313); three phantom result names; `Maybe`/history have two encodings each. *Since #3322: the family is checked against the schema and the payload is derived from it (84 models in use, inventory checked both ways).* |
| Human renderer | 23 designed tag arms over 114 `Output` variants; structural sniff with a JSON fallback for the other 91. *Since S1: the 36 `mutation_ack` commands render from their `display:` declaration through `Invocation` (receipt, identity, noun); the family arms remain for every other rule. Since S2: the 25 `page`, 3 `history`, 2 `search` and 6 `analytics` commands render their declared columns / map as one table; the `json_version_history` and `vector_matches` arms, the page tail, the `matches`/`items` page sniffs and the page/history byte pre-pass are deleted. Since S3a: every `optional`, `status_value` and `status_sections` command renders its declared value, record or receipt, the `search` rule shows its declared diagnostics block, and the eleven tag arms those replaced — with `point_read_record`, `print_optional_data`, `print_optional_record`, `print_maybe_json`, `json_leaf`, `print_count` and the KV/preview byte pre-pass arms — are deleted. Since S3b: `batch` renders from its columns, `branch diff` has the arm it never had, and the renderer has two paths and no third — a declaration, or one of the thirteen `bespoke` arms, with anything else reported as a renderer bug* | none. *Since #3339 the CLI's own reports — `doctor`, `init`, `update`, `uninstall`, `inference install-local`, `ipc start/stop`, `agents …`, `config …`, the REPL's context line — render through `crates/cli/src/report.rs` in the same vocabulary: an action answers with a one-line receipt and hands a script its record, a list answers with a table, and everything else is the record block with its labels taken from the payload's own keys. They are not IDL commands, so nothing declares them; the record block is the default, which is what makes the next one legible without new code* |
| `--raw` | its own sniff; base64 for non-UTF-8 reads. *Since S1: a write prints its declared identity, tab-separated; the help text says "shell-composable". Since S2: a table prints its declared columns tab-separated, no header, no hint, an empty cell for null, full float precision, bare base64 for bytes. Since S3a: a record prints `key<TAB>value` lines under the wire's own names — the pointer relative to the record's root, one dot per level, so a `header:` never reaches a script (Q18) — and an action prints its whole record the same way (Q17). Since S3b: a batch prints its rows as TSV and `ping` prints the version, and the `items`/`found`/`value` sniffs are gone* | #3116 (reads) |
| Dates | one wall-clock field (`committed_at`), humanized as a pre-pass to local time with offset. *Since S2: every declared `as: date` cell prints `2026-09-10 20:19:44.123456 UTC` (raw: the micros), and `--as-of-time` accepts that spelling back; the pre-pass no longer reaches a declared command* | the pre-pass still runs for the undeclared families (S4 deletes it) |
| Channels / exit | data + status lines on stdout; errors on stderr; exit 0/1/2. *Since S1: a missed write is `no such <noun>: <identity>` on stderr, nothing on stdout, exit 0, in human and `--raw`; `--json` keeps the envelope on stdout and stderr silent. Since S2: `-- more: add --cursor <c> to the same command` and `-- sampled N of M` (only when N < M) are stderr, human mode only. Since S3b: `-- <mode>: N ok, M miss` is stderr too, human mode only, and only when an item's own status is not `ok`* | *Since B (2026-09-13, #3358 F6/F10, closing the #3332 gap): both are declared. A column carries `tombstone: <pointer>`; a display carries `truncated:` / `total:`. See Q21* |
| Playground | real clap grammar, real renderer | format flags dropped (#3312); one stream |
| Pins | `command-examples.json` (human, 124 commands, drift only); 23 literal asserts in `render.rs`, ~10 in `cli_execution.rs`; site transcript gate (23 exchanges) at release | no raw pin, no channel/exit pin, no playground≡binary pin, no per-fixture matrix |
| Prose | inputs clap-parse-guarded (resolver S4a) | outputs hand-typed in README / skill / docs / site |

---

## 9. Out of scope

- Colour, pagers, terminal width, TTY-dependent content (R6).
- Any change to the `--json` envelope, field names, encodings or error
  registry rows.
- Binary-value *storage* or `--file` symmetric read (#3116 asks for
  `--raw` bytes; a `--output-file` is a separate feature).
- Interactive REPL chrome (#2998, shipped).
- MCP, VS Code, Python SDK surfaces. They consume `--json` / the wire and
  never read `display:` — the SDK generator gets a guard for that in S0
  (R2). The Python SDK's own redesign is sequenced after this contract and
  after #3313, as a sibling of the CLI derived from the same IDL, not a
  wrapper around it.

---

## 10. Decisions to make

| # | Question | Recommendation |
|---|---|---|
| Q1 | `kv get`: the bare value (#3306: "already right, do not change") or a receipt with version/commit lines (#3205 §2)? | **bare value.** `redis-cli GET`, `cat` and every shell idiom expect the value alone; version and commit facts are one `kv history` or `--json` away, and a receipt would break `strata kv get k > file`. Applies to `json get` too |
| Q2 | A write with `applied: false` (delete of a missing key): what does a person and a script see? Three options: **(a)** stderr `no such key: k`, exit **1**, every format — #3306's ask; `rm`, `git branch -d`, `kubectl delete` do this. **(b)** stderr `no such key: k`, exit **0** — the idempotent-delete peers: `redis DEL` → `(integer) 0`, `DELETE` in SQLite/DuckDB → 0 rows, `kubectl delete --ignore-not-found`; the shell then agrees with the wire (`Ok`, `applied: false`) and `strata kv delete k` in a retry loop stays safe. **(c)** today: stdout `not_found k applied=false`, exit 0. | **(b) — decided 2026-09-11 on review**, reversing the round-1 pick of (a). Strata's peers (SQLite/DuckDB/Redis) are idempotent, and a `--json` caller seeing `Ok` on the wire with exit 1 in the shell was the asymmetry judged worse than #3306's ask. So: human and `--raw` → stderr `no such key: k`, nothing on stdout, exit **0**; `--json` → the `applied: false` envelope on stdout, exit 0, nothing on stderr. If users ever need assertive deletion, add an opt-in `--require-existing` (exit 1 on a miss) then — not now. (c) is not on the table |
| Q3 | Human date form | **`2026-09-10 20:19:44.123456 UTC`** — UTC, microsecond precision, labelled. *Amended at S2 (2026-09-12): the original choice was second precision, which #3112 S5 forbids — a date the CLI prints must read back through `--as-of-time` to the SAME commit, and a truncated date resolves to the commit before it (`crates/cli/tests/cli_execution.rs`, `a_date_printed_by_history_reads_back_the_value_from_that_commit`). The parser reads the `UTC` label as UTC.* Deterministic across machines and the browser (where "local" is whatever the visitor's `wasm` clock says), diffs cleanly in transcripts, and matches what the docs will show. Alternatives: RFC 3339 `2026-09-10T20:19:44Z` (what AWS/kubectl/gh put in *machine* output — but `--json` keeps epoch micros and is out of scope, and the `T` reads worse in a table); local-with-offset (today's `2026-09-10 13:19:44.123456 -07:00`, what `git log` does — non-deterministic across machines and the site gate) |
| Q4 | TTY sniffing | none (R6) |
| Q5 | `Pretty` and the hidden `--output-format` flag (`options.rs:56-59`, "transitional") | **deleted at S4**, and replaced where it was load-bearing: `--human` joins `--json` and `--raw` as a named format (#3345), because a line inside a session inherits the session's format when it asks for nothing, so human needed a name to be askable at all. What went is the *hidden* second way to choose a format, not the format. Originally: delete both in S4: `--json \| jq .` is pretty; a hidden second way to choose the format is Rule 8's smell. Release note |
| Q6 | Table style | kubectl/gh: upper-case header, two-space gutters, left-aligned text, right-aligned numbers, no borders |
| Q7 | `branch list` columns — #3306 asks for `forked-at`, but `BranchItem.created_at` is a logical clock (#3112), not an instant | `NAME  PARENT  STATUS  GENERATION` now; a wall-clock `committed_at` on `BranchItem` is an additive wire change that can follow, and would then be declared `as: date` |
| Q8 | Pagination hint channel | stderr (R5); `gh` and `cargo` put notices there; `strata kv list \| wc -l` must count keys |
| Q9 | Miss sentinels for reads | keep `(nil)` and `(empty)` on stdout, exit 0 — a read miss is an answer, and both are already what the docs teach; raw prints nothing |
| Q10 | Where the display declaration lives | IDL YAML validated against the schema (R2), not a Rust table |
| Q11 | `BatchResult` human form | **a table, one row per item (`#  STATUS  [EFFECT]  <cols>`, `+ ERROR` when any failed); a stderr summary `-- itemwise: 2 ok, 1 miss` only when an item missed or failed, or an atomic batch was not applied — decided 2026-09-11 on review.** A clean batch is the table alone: a diagnostic line after full success is noise. No `(version V)` suffix; the version is in `--json` |
| Q12 | Playground channel order | stdout then stderr, no exit marker |
| Q13 | Rendering authority where declaration and wire disagree (#3313): render from the **schema** and correct/flag the declarations now, or from the **declaration** and normalize the wire? | **schema now, wire later.** The wire is what `--json` callers, fixtures and the SDK already depend on; the declaration is a docs artifact nobody executes. S0 corrects the declarations it can (`branch create` is `StatusResponse<BranchItem>` in fact) and flags the rest `transitional` in a shrink-only allowlist. Normalizing the wire (one encoding per family) is a wire change, so a major — tracked from #3313, not here |
| Q14 | A bulk delete (`vector delete-all`, `vector delete-by-filter`, `graph apply_delete_policy`) that matches nothing reports `applied: false` — a miss? | **Not a miss — decided 2026-09-11.** stdout `deleted 0 vectors from docs`, exit 0. A filter that matches nothing did what it was asked; only a *named* absent target is a miss (Q2) |
| Q15 | `bool` acks (`json index drop`, `vector collection delete`) carry no identity on the wire | receipt takes the name from the request through a `/request/…` pointer (`dropped index {/request/name}`, `identity: [/request/name]`), until #3313 puts it on the wire |
| Q16 | `--raw` for status objects (`admin info`, `inference status`, `branch get`) — today compact JSON | **`key<TAB>value` lines — decided 2026-09-11.** Dotted keys for nesting, arrays as one compact-JSON field, null as an empty value; `--raw` is TSV everywhere and `--json` is the only JSON |
| Q21 | How a rule says an answer is partial, or that a row is a deletion | **Per-rule declared pointers — decided 2026-09-13 (#3358 F6/F10, closing the #3332 gap).** A display declares `truncated:` (boolean) and `total:` (integer); the rule writes `-- truncated: this is part of the answer, not all of it` and `-- showing N of M` on stderr, human only, and a complete answer stays quiet. A *column* declares `tombstone:` (boolean, same row array) and shows `(deleted)` where its value would be — empty in `--raw`, because a script reads the flag from `--json` and a word where a value goes cannot be told from a value. A column that marks its deletions can also say a `null` in it is the stored document rather than an absence; everywhere else a null still means nothing to show, and the miss beside it says why. The renderer reads all of these by pointer: a rule sniffing for `truncated` or `tombstone` by name would be the guessing S3b deleted |
| Q16b | How a cell carries a value containing the separator, a newline, or bytes that are not text | **Escape the escape — decided 2026-09-13 (#3358 F7/F8).** A cell is injective, so a consumer can recover what was stored: `\\`, `\n`, `\t` and `\r` are escaped (the escape character *first*, or a value spelling `\n` would encode to the same six bytes as one containing a newline); bytes that are not displayable text carry the Q20 marker in **both** layouts, not only human, because bare base64 cannot be told from a value whose text is those characters; and a literal value beginning with the marker is escaped with a leading `\`, which cannot be read as an encoded backslash because that is always doubled. Displayability is *not* UTF-8 validity — a value with any control character other than the three escaped ones is bytes. A serialized JSON document escapes its own control characters and so is already an unambiguous spelling of itself; it passes through unescaped. Applies wherever values share a line: table cells, `key<TAB>value` lines, **receipts and raw identities** (which previously bypassed it entirely). The whole-value `--raw` point read (Q5/#3116) is a separate encoding and stays byte-exact |
| Q17 | `action.status` commands (`arrow export/import`, `clone`, `ipc stop`) — today pretty JSON | one-line receipt like a write (`exported 1 row of kv to /tmp/exports/kv.parquet (1.9 kB)`); `--raw` = key/value lines as any status; the record stays in `--json` |
| Q18 | Human labels and headers | **Wire field names by default; an explicit `header:` permitted where the wire name misstates the humanised value — relaxed 2026-09-11 on review** (round 1 had said "always", which yields `SIZE_BYTES  1 kB`). Headers are the wire name in UPPERCASE unless the command declares a `header:`; the one class today is byte counts (`size_bytes` → `SIZE` / `size`, `total_bytes` → `total`). `header:` is human-only, `--raw` keys stay wire names, and the declaration lives in the IDL beside the field it labels, so there is still no alias table to drift |
| Q19 | Floats in human tables | up to six decimals (`0.184417`); an integral float keeps one decimal (`1.0`); `--raw` / `--json` full precision |
| Q20 | `branch diff` / `branch preview` vector identities are the engine's internal key encoding (`AAVub3Rlc24x`) | render as `base64:…` (R1-table bytes rule) for now; exposing `collection` + `key` on the row is a wire question for #3313. *Implemented 2026-09-13 (#3357): the renderer had inferred printability from UTF-8 validity, and the identity is valid UTF-8 beginning with NUL and 0x05, so those bytes reached the terminal, the corpus and a generated reference page — which git classified as binary. A guard now asserts every generated page is text.* |

Q1–Q3, Q5 and Q13 change what users see and were decided before S0 lands
its declarations; Q14, Q16, Q18 and the R4 rewording were decided on the
catalog review (2026-09-11). A second review round the same day revised
five things — Q2 reversed to idempotent exit 0, `fields:` selection so the
command and not the family decides which facts a person sees, Q18 relaxed
to permit `header:` for byte counts, batch and other diagnostic stderr
lines only when something needs attention, and the `--raw` mental model
(shell-composable, not raw wire) written down — plus the boundary with the
Python SDK (R2). A third round (same day, with the go for S0) caught the
catalog contradicting the contract twice — read batches still carried a
success summary because the reference implementation keyed on `applied`
rather than the engine's `status`, and record-valued `Maybe<T>` had five
`--raw` behaviours — and dropped `-- sampled N of N`; all three are fixed
in R1/R2 above. The rest are recorded here so the slices implement rather
than re-decide them. The per-command rendering of every decision is
`cli-output-catalog.md`.

---

## Appendix A — prior art

Thirteen tools and guidelines surveyed from their primary documentation
(sources at the end; claims that could not be confirmed from a fetched page
are marked *unverified* and were not used to decide anything).

### A.1 Stripe and Databricks, since they were the hypothesis

They sit on opposite sides of the one contested question that matters most.

- **Stripe CLI** prints **JSON by default** — pretty-printed, and the object
  is the API object, dates as Unix epoch seconds [7][8][9]. It has no human
  mode because its product *is* the API: the CLI is a typed `curl`. That is
  the right model for a tool whose users are reading the same objects in
  the API reference, and the wrong one for a tool with a homepage REPL and
  a `kv get` that should print `world`. Strata already has the Stripe half
  (`--json` is the wire, byte-for-byte, and the fixtures pin it); this plan
  is about the other half.
- **Databricks CLI** (Go, v0.200+) defaults to **`-o text`** — tables — with
  **`-o json`** behind a flag [10][11]. That is the shape R1/R5 proposes:
  one human default, one explicit machine flag, logs to stderr. Databricks
  does not document TTY adaptation.

So the answer to "which one is best in class for Strata" is: Stripe for
the wire, Databricks for the terminal, and neither for the rules below
where they are silent (miss handling, raw mode, date form) — those come
from the tools that designed a human mode on purpose: `gh`, `git`,
`kubectl`, `redis-cli`.

### A.2 Comparison

| Tool | Default | Machine flag | Adapts to a pipe? | Write confirmation | Lists | Miss / error | Dates (machine) |
|---|---|---|---|---|---|---|---|
| **gh** | human text [2] | `--json <fields>`, `--jq`, `--template` [2] | **yes** — piped: tab-delimited, no colour, no truncation [4]; `GH_FORCE_TTY` [3] | new URL on stdout [6]; `✓ …` lines (*unverified*) | TTY aligned+colour; pipe tab-separated [4] | stderr, non-zero | ISO 8601 [5] |
| **Stripe** | **JSON** [7][8] | (is JSON); `jq` [8] | no | created object as JSON | JSON `data:[…]` | JSON / stderr (*unverified*) | epoch seconds [9] |
| **Databricks** | **text** tables [11] | `-o json` [11] | not documented | object as text/JSON | text tables | logs → stderr [11] | epoch millis (*unverified*) |
| **clig.dev** | "humans first" [1] | `--json`; `--plain` [1] | prescribes adapting when not a TTY [1] | "if you change state, tell the user … err on the side of less" [1] | ASCII tables [1] | stdout data / stderr errors; non-zero [1] | ISO 8601 implied |
| **psql** | aligned table + `(N rows)` [12] | `-A`, `--csv`, `-t` [12] | **no** — stays aligned when piped [12] (a known wart) | `DELETE 3` command tag (norm, not fetched) | aligned, header rule, footer [12] | 0 rows → `(0 rows)`, exit 0 | — |
| **redis-cli** | typed replies in a TTY [13] | `--raw` (auto when not a TTY), `--csv`, `--json` [13] | **yes** — raw when piped [13] | `OK`; `(integer) N` [13] | one per line; `1)` `2)` arrays [13] | `GET` miss `(nil)`; `DEL` miss `(integer) 0`, **exit 0** [13] | — |
| **duckdb** | duckbox table [14] | `.mode json/csv/…` [14] | not documented | DML silent (norm) | Unicode box [14] | 0 rows → empty box, exit 0 | — |
| **sqlite3** | list, `\|`-separated, **no headers** [15] | `-json`, `-csv`, `-box`, `-header` [15] | no [15] | DML silent (norm) | list, no header [15] | 0 rows → nothing, exit 0 | — |
| **git** | human ("subject to change") [16] | `--porcelain` [16] | colour only to a terminal; pages only when interactive [17] | `[main 463dc4f] msg`; `Deleted branch x (was 1a2b3c).` [17] | one per line, `* ` current [17] | `branch -d nosuch` → stderr, **exit 1** [18] | `%cI` ISO 8601 |
| **kubectl** | table, UPPERCASE headers, `AGE` [20] | `-o json/yaml/name/jsonpath`, `--no-headers` [19] | no | `deployment.apps/nginx created` [20] | aligned, UPPERCASE headers [20] | NotFound → stderr, exit 1 (*primary-unverified*) [22] | RFC 3339 UTC [21] |
| **AWS CLI v2** | **JSON** [23] | `--output json/yaml/text/table`, `--query` [23] | no (pager is TTY-gated) | created object as JSON | text = tab-delimited; table = boxed [23] | empty JSON; stderr; exit 0/1/2/… [25] | RFC 3339 [23] |
| **docker** | table [26] | `--format`, `--format json` [26][28] | flag only | **echoes identity**: `docker rm redis` → `redis` [27] | table; header via `table` directive [26] | stderr, non-zero | — |
| **cargo** | status → **stderr**, data → stdout [30] | `--message-format json` → stdout [29] | — | `Compiling` / `Finished` on stderr [30] | — | stderr | — |
| **Heroku guide** | human, "grep-parseable" tables [31] | `--json` [31] | colour/spinner gated on TTY [31] | `Enabling … done` on **stderr** [31] | aligned, `────` rule [31] | stdout data; stderr warnings/errors/out-of-band [31] | — |

### A.3 Near-universal and contested

**Near-universal** — every surveyed tool with a designed human mode does
these, and R5/R1/R4 adopt them:

1. exit 0 on success, non-zero on failure [1][18][25];
2. data on stdout, diagnostics on stderr [1][23][30][31];
3. a machine format behind an explicit flag, decoupled from the human one
   [2][11][16][23];
4. "if you changed state, tell the user", realized as an *identity echo* —
   `resource/name created` [20], the commit hash [17], the URL [6], the
   container name [27];
5. colour, pager and spinner gated on a TTY with an opt-out [1][3][17][31].

**Contested** — the plan takes a side and says which:

| Question | Side A | Side B | This plan |
|---|---|---|---|
| Does the *default* format change when piped? | gh [4], redis [13], clig [1] | psql [12], AWS [23], kubectl, Stripe, Databricks | **B** (R6): one stable default; the playground has no TTY, the docs must show what a pipe sees, and the site gate diffs output |
| Human or JSON by default? | gh, psql, duckdb, sqlite, kubectl, Databricks, git, Heroku | AWS, Stripe | **A**: human; `--json` is the wire |
| Delete of a missing target | idempotent, exit 0: redis [13], SQL `DELETE 0` [12], `kubectl --ignore-not-found` [19] | strict, stderr + exit 1: `git branch -d` [18], `kubectl delete` default [22] | **Q2, decided (b)** — the one place prior art splits Strata's peers (SQLite/DuckDB/Redis: A) from #3306's ask (B); Strata follows its peers, with `--require-existing` reserved for B |
| Write confirmation channel | stdout: kubectl [20], docker [27], git [17], gh [6] | stderr: cargo [30], Heroku [31] | **A**: the identity *is* the answer of a write (`docker rm` echoes it); `--raw` then prints it alone for `xargs`; a confirmation on stderr would vanish from the playground's single stream |
| Dates in machine output | RFC 3339: kubectl [21], AWS [23], gh [5] | epoch: Stripe [9], Databricks (*unverified*) | out of scope (`--json` unchanged, epoch micros); human form is **Q3** |
| Pager by default | AWS v2 [24] | everyone else | none (§9) |
| Table headers | on: psql, kubectl, gh, Databricks | off: sqlite3 list mode | on for record tables, off for scalar lists (R1) |

### A.4 What each rule rests on

| Rule | Supported by | Contested by |
|---|---|---|
| R1 identity-echo receipt (`created k`) | kubectl, docker, git, gh, clig | redis (`OK`), SQL shells (tag or silence) — scalar stores that never designed a human mode |
| R1 bare value for `kv get` (Q1) | redis `GET` [13], `SELECT` in psql/sqlite [12][15] | gh/kubectl show metadata for *resources* — and gate it behind `--json`/`-o` [5][19], which is what `kv history` and `--json` are |
| R1 `(nil)` / `(empty)` on stdout, exit 0 (Q9) | redis `(nil)` [13], psql `(0 rows)` [12] | — |
| R1 tables: UPPERCASE header, two-space gutter, no border (Q6) | kubectl [20], gh piped [4], Heroku [31] | psql/duckdb draw rules and boxes — heavier to diff and paste |
| R4 raw = declared columns as TSV / bytes, nothing else | redis `--raw` [13], gh piped [4], `kubectl -o name` [19] | — |
| R5 stdout/stderr split; exit codes format-independent | clig [1], AWS [23][25], Heroku [31], cargo [30] | — |
| R5 miss = exit 0 (Q2b, decided) | redis [13], SQL `DELETE 0` [12], `kubectl --ignore-not-found` [19] | git [18], kubectl default [22], `rm` (Q2a — available later as `--require-existing`) |
| R6 no TTY adaptation | psql, AWS, kubectl, Stripe, Databricks | gh [4], redis [13], clig [1] |
| Q8 pagination hint on stderr | cargo/Heroku "out-of-band on stderr" [30][31]; AWS pages silently [24] | gh prints nothing about more pages; `kubectl` uses `--chunk-size` silently |
| Q5 delete `Pretty` | gh (`--json \| jq`), Stripe (`jq`) [8] | AWS keeps `json` and `yaml` both; psql has ten formats — for a query shell |

### A.5 Sources

[1] clig.dev · [2] cli.github.com/manual/gh_help_formatting ·
[3] cli.github.com/manual/gh_help_environment ·
[4] github.blog/engineering/engineering-principles/scripting-with-github-cli ·
[5] cli.github.com/manual/gh_issue_view · [6] cli.github.com/manual/examples ·
[7] docs.stripe.com/cli · [8] stripe.com/blog/stripe-cli ·
[9] docs.stripe.com/api/billing/meter-event/object ·
[10] docs.databricks.com/aws/en/dev-tools/cli/usage ·
[11] docs.databricks.com/aws/en/dev-tools/cli/commands ·
[12] postgresql.org/docs/current/app-psql.html ·
[13] redis.io/docs/latest/develop/tools/cli ·
[14] duckdb.org/docs/current/clients/cli/output_formats ·
[15] sqlite.org/cli.html · [16] git-scm.com/docs/git-status ·
[17] git-scm.com/book/en/v2 (Customizing-Git-Git-Configuration, Git-Basics-Recording-Changes, Git-Branching-Branch-Management) ·
[18] git/git `builtin/branch.c` ·
[19] kubernetes.io/docs/reference/kubectl/generated/kubectl_get ·
[20] kubernetes.io/docs/concepts/workloads/controllers/deployment ·
[21] kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta ·
[22] github.com/kubernetes/kubernetes/issues/58048 ·
[23] docs.aws.amazon.com/cli/latest/userguide/cli-usage-output-format ·
[24] docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination ·
[25] docs.aws.amazon.com/cli/latest/topic/return-codes ·
[26] docs.docker.com/reference/cli/docker/container/ls ·
[27] docs.docker.com/reference/cli/docker/container/rm ·
[28] docs.docker.com/engine/cli/formatting ·
[29] doc.rust-lang.org/cargo/reference/external-tools ·
[30] github.com/rust-lang/cargo/issues/1473 ·
[31] devcenter.heroku.com/articles/cli-style-guide

*Unverified* (not confirmed from a fetched primary page; noted, not relied
on): gh's exact `✓ …` strings; kubectl's NotFound exit code and stream;
Stripe's TTY colourization; Databricks' JSON date encoding and TTY
behaviour; psql/SQL-shell command tags and DML silence; cargo's
present-tense status-to-stderr doc.

---

## Appendix B — restatements register

What is derived and what is written by hand on this surface. The plan's job
is to move rows from the second table to the first or delete them.

**Derived today**

| Fact | Source | Carried to |
|---|---|---|
| response-model family per command | `kinds.yaml` + `commands/*.yaml` | `command-index.json`, SDK types, docs "Returns" |
| wire schema per DTO | executor DTOs via `schemars` | `generated/schemas/`, `--json` fixtures |
| human transcript per example | `command_examples.rs` replay | `command-examples.json` (downstream: site) |
| CLI inputs in prose | clap-parse guard (resolver S4a) | README, `docs/inference`, registry hints |

**Hand-written today (targets)**

| Fact | Copies | Plan |
|---|---|---|
| which fields a person sees per shape | `render.rs` arms + sniff (`mutation_summary` subject order; `print_vector_matches` key/score; `print_described`) | `display:` on each command in `commands/*.yaml` (R2) |
| which rule a shape uses | `render_human` tag list; `render_raw` tag list; the guard test's inventory | `render:` on `kinds.yaml` (R2) |
| human output in prose | README `:194` `# pong 1.2.1`; `docs/` fences; site `quickstart.mdx:22` | matrix cells + fence guard (R7, S5) |
| human output as a release gate | stratadb.org `verify-transcripts.mjs`: 23 exchanges, 11 `applied=true` lines, 8 branch-JSON substrings | regenerated from matrix cells in the release PR (R7); becomes a *consumer* of the pin, not a second pin |
| the homepage demo | stratadb.org `heroScript.ts`: 8 output beats "kept in sync by hand" | same — generated from the cells at release |
| response model per command | `response_model:` in `commands/*.yaml`, checked against nothing (#3313) | `check` guard family ⇔ schema + shrink-only `response-model-divergences.yaml` (S0); the payload is derived from the schema and the inventory checked both ways (#3322) |
| date rendering | `wall_clock::format_instant` + the pre-pass | one `as: date` formatter in the table/receipt rules (R3) |
| what `--raw` means | `render_raw` sniff; `--raw` help text ("script-friendly raw output where possible") | R1 raw column; help text rewritten to the rule |
