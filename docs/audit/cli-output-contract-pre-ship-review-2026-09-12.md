# CLI Output Contract: Pre-Ship Review

Review date: 2026-09-12, America/Chicago. Later probes ran after midnight UTC.

Reviewed combined state: `19512cf9e92f6b97f3eceab9532f81f589757fbc` (`main`).
Baseline: `60db96ac`, immediately before S0a. All 19 requested PRs are merged
and present in the reviewed state.

**Recommendation: do not declare #3314 complete or ship this as a fully
satisfied output contract yet.** The IDL-driven renderer is a substantial
improvement, and the targeted suites pass, but there are observable correctness
gaps, missing promised guards, and unfinished release integration.

This is a release-readiness review of the combined work, not a claim that each
finding was introduced by its associated PR. Inherited defects and explicitly
deferred requirements are labelled below. No GitHub comments or issues were
created. No implementation changes were retained.

## Findings

### F1. P1: A Per-Line Branch Override Can Change Later Commands' Branch

Associated work: #3325, #3329, #3331. **Inherited execution defect**, still
present after consolidating the line grammar.

Source: [execute_parsed_command](../../crates/cli/src/lib.rs#L648),
[REPL dispatch](../../crates/cli/src/repl.rs#L269).

In a piped `strata --cache` session:

```text
branch create alternate
--branch alternate kv put key branch-only
kv get key
--branch default kv get key
```

The unqualified read returns `branch-only`; the explicit default-branch read
returns `(nil)`. The same sequence through the playground's `run_line` returns
`(nil)` for the unqualified read, as expected for a line-local override.

`execute_parsed_command` installs the override as the connection's default.
When the session context has no explicit branch, the next command falls back
to that mutated default. Subsequent writes can therefore land in an unintended
branch. This is also an actual native/playground behavior difference that the
current parity script does not cover.

Required: resolve every line against a stable session context, or restore
connection defaults after each line, including failures. Test reads and writes
after temporary overrides, with and without an explicit session branch and
through `command run`.

### F2. P1: Valid Host Commands Can Panic an Existing Session

Associated work: #3329, #3331, #3342. **Inherited dispatch defect**, not caused
by the new grammar.

Source: [host-only dispatch arms](../../crates/cli/src/lib.rs#L676),
[config_command](../../crates/cli/src/lib.rs#L837).

Sending any of `config show`, `hub info`, or `start` to a piped `strata --cache`
session exits **101** with an `unreachable!` panic. `--json config show` also
prints a Rust panic, not a structured refusal. The REPL accepts these commands
through the shared top-level parser and then reaches one-shot-only assumptions.

Required: classify/refuse host-only commands before session execution. Every
top-level command should either work in a session or return an ordinary error
while allowing the next line to execute. Include config subcommands and both
spellings of host lifecycle commands in that inventory.

### F3. P1: Published Transcripts Do Not Execute the Commands They Claim

Associated work: #3347 / R7. **Inherited quoting bug, exposed by the new
assertion that the generated output is what the shown CLI line prints.**

Source: [new transcript rendering](../../crates/executor/src/idl_tooling/examples.rs#L750),
[cli_token](../../crates/executor/src/idl_tooling/examples.rs#L861),
[JSON set reference](../../crates/executor/idl/v1/generated/docs/json/set.md#L19),
[existing clap-only guard](../../crates/cli/src/arg_spec.rs#L137).

The reference shows `strata json set user $ {"age":30,"name":"alice"}` followed
by a successful object read. Shell/shlex parsing removes the unprotected JSON
quotes. In the actual CLI the command succeeds but stores the **string**
`{age:30,name:alice}`, not the documented object. Event payloads, graph
properties, and vector metadata/filter examples instead fail JSON parsing.

A probe compared the captured requests with requests parsed from their printed
CLI lines: **36 genuine quoting failures or changed requests** across the
389-step corpus, after excluding host-only refusals, intentional temporary-path
substitutions, and equivalent default-direction differences.

The new guard compares two generated strings, and the existing input guard
only runs clap. Neither verifies that shell parsing plus command conversion
produces the captured request.

Required: shell-quote every rendered argument with a real quoting function,
including the `command run` fallback. Replay the printed CLI lines, or compare
their fully converted commands with the intended requests. Regenerate the
transcripts only after that semantic round trip passes.

### F4. P2: Human Table Padding Can Amplify a Small Response Hundreds of Times

Associated work: #3333, inherited by #3338, #3340, #3342.

Source: [Table::human](../../crates/cli/src/table.rs#L81),
[push_line](../../crates/cli/src/table.rs#L137).

Widths use the longest complete value and every row is padded to that width.
A schema-deserializable JSON scan probe with 1,000 rows, one 60,000-character
key, and otherwise short distinct keys produced:

| Representation | Bytes |
| --- | ---: |
| Input JSON envelope | 144,965 |
| Raw TSV | 81,888 |
| Human output | 60,086,017 |

The long key is below the engine's 65,535-byte document-ID limit. The renderer
builds the entire padded output in memory; more rows increase it further.
This affects both native CLI and WASM and is outside engine memory budgeting.

Required: a deterministic bounded-width policy for human cells, wrapping or an
explicit alternative layout for wide values, and an output/allocation bound.
Do not silently truncate raw or JSON data. Add a maximum-length-key and
large-cell test with many small neighboring rows.

### F5. P2: JSON Stderr Still Contains a Plain-Text Log Prefix

Associated work: #3351 / R5. **Acknowledged remaining gap**, tracked by
[#3352](https://github.com/stratalab/strata-core/issues/3352).

Source: [boundary subscriber](../../crates/cli/src/lib.rs#L106).

An actual `--json` read against a database with a nonexistent parent directory
exits 1 with a timestamped `ERROR strata_executor::error: ... source=...` line,
then the JSON error envelope, on stderr. ANSI removal works, but parsing stderr
as the promised error envelope still fails. The prefix also includes the
storage-layer source text, distinct from the curated public message.

Required: define a separate destination or structured transport for correlated
diagnostic records while preserving the reference ID. The machine error channel
must satisfy the advertised contract. Test a real boundary error with a source
chain, not only direct validation errors. Local llama.cpp stderr logging is a
separate existing concern (#3234); local inference was not exercised here.

### F6. P2: Partial BFS and Hub Listings Look Complete

Associated work: #3333, #3338. BFS is an **explicit deferral** in
[#3332](https://github.com/stratalab/strata-core/issues/3332); the hub case needs
to be included in the same completeness requirement.

Source: [render_map](../../crates/cli/src/render.rs#L902),
[hub list declaration](../../crates/executor/idl/v1/commands/hub.yaml#L31),
[RowsDecl creation for status rules](../../crates/cli/src/render.rs#L350).

With an edge from `a` to `b`, `graph bfs g a --max-nodes 1` displays only `a`
and writes no stderr. The same request in JSON reports `truncated: true`.
A hub fixture with one item and `total: 100` also displays a table with no
indication of omitted results: it is rendered under the status rule, so page
notices are never installed.

Required: declared conditional notices for bounded/truncated answers and hub
offset/limit/total pagination. Complete results should remain quiet. Add both
true/false truncation and partial/complete listing cells to the matrix.

### F7. P2: Write Identities Bypass TSV Escaping

Associated work: #3330, still present after #3344.

Source: [render_mutation_ack](../../crates/cli/src/render.rs#L1259),
[render_placeholder](../../crates/cli/src/render.rs#L1313).

A successful raw KV put with key bytes `a\nb\tc` writes those literal newline
and tab bytes followed by a newline. One identity becomes two rows and extra
columns. Human receipts and missed-write feedback likewise interpolate the
unescaped identity. This violates the one-line receipt and shell-composable
identity rules. The point-read raw-byte exception does not apply to receipts.

Required: apply a shared cell/identity encoding before joining receipt fields.
Test newline, tab, carriage return, and binary identities for successful writes
and missed writes across the relevant primitives.

### F8. P2: Raw Cell Encoding Maps Distinct Values to the Same Output

Associated work: #3333 and its consumers.

Source: [escape_cell](../../crates/cli/src/render.rs#L1231),
[bytes_text](../../crates/cli/src/render.rs#L1364).

A JSON scan key containing an actual newline and a different key containing
the two characters backslash + `n` produce identical raw rows. `escape_cell`
escapes newlines but not existing backslashes. Binary byte fields have another
collision: invalid UTF-8 byte `ff` is rendered as `/w==`, indistinguishable
from the literal UTF-8 text `/w==` in the same raw column.

Required: specify an unambiguous TSV field encoding and its inverse before
calling it shell-composable. Escape the escape character, distinguish binary
representations from literal text, and add round-trip/property tests. Keep
`--raw kv get` byte-exact as its separately defined whole-value encoding.

### F9. P2: Branch Diffs Emit Internal Control Bytes as Human Text

Associated work: #3338, #3340, propagated into docs by #3347.

Source: [branch comparison identity](../../crates/cli/src/render.rs#L1633),
[bytes_text](../../crates/cli/src/render.rs#L1364),
[generated branch diff example](../../crates/executor/idl/v1/generated/docs/branch/diff.md#L43).

Creating collection `notes`, adding vector `n1` on a fork, then diffing it
produces the identity `\u0000\u0005notesn1` with actual NUL/control bytes on
stdout. This internal binary encoding is valid UTF-8, so the generic bytes
formatter mistakes it for displayable text. The generated Markdown contains
those bytes too; Git treats the changed reference page as binary.

Q20 explicitly selected labelled base64 for these internal identities.
Required: enforce that selection, or render a defined capability-aware identity.
Display cells must not pass terminal control sequences through merely because
they are valid UTF-8. Cover both diff and preview, plus regeneration of docs.

### F10. P2: History Does Not Identify Tombstones or Preserve JSON Null in Cells

Associated work: #3333. **Explicit deferral** in #3332.

Source: [history declaration](../../crates/executor/idl/v1/commands/json.yaml#L72),
[generic null cell](../../crates/cli/src/render.rs#L1139).

In a session, set `doc` at `$` to JSON `null`, delete it at `$`, and read its
history. Both VALUE cells show `-`; the JSON envelope correctly distinguishes
`tombstone: false` from `tombstone: true`. Raw JSON null cells also collapse to
the empty cell. The catalog specifically requires `(deleted)` for a tombstone.

Required: history-aware declared cell conditions, with stored JSON null rendered
as a real value and tombstones identified explicitly. Add create-null, update,
delete, and recreate history fixtures, not only a history of present values.

### F11. P2: The Date Guard Does Not Distinguish Logical Clocks From Instants

Associated work: #3320, #3333 / R3.

Source: [check_as](../../crates/executor/src/idl_tooling/display.rs#L486).

Replacing the KV history date column's `committed_at` pointer with `timestamp`
in a scratch IDL copy still passes `resolve_cli_index`. The guard accepts any
integer for `as: date`; it neither checks a wall-clock semantic type nor uses
the fallback allowlist promised in R3. A future declaration can therefore turn
a commit counter into a plausible 1970 date and still pass the guard.

Required: semantic timestamp typing or an explicit checked allowlist, with
negative tests for `timestamp`, `version`, `generation`, and branch `created_at`.
The currently declared UTC history columns and microsecond round-trip tests
are correct; the defect is the claimed prevention of future mistakes.

### F12. P2: New Status Fields Do Not Require a Display Decision

Associated work: #3320 / R2.

Source: [check_fields](../../crates/executor/src/idl_tooling/display.rs#L518).

Adding `new_important_fact` to the `admin.info` payload schema in a scratch
copy still passes CLI-index resolution with an unchanged display declaration.
The guard validates only fields that are selected; there is no inventory of
fields intentionally omitted. R2 says a new wire field must fail `check-cli`
until someone decides where it belongs, but that requirement is not implemented.

Required: account for both displayed and deliberately hidden schema fields,
including nested records, or explicitly amend the contract to its weaker
one-directional guarantee. Curated human output is appropriate; silently
treating every future field as already reviewed is the gap.

### F13. P3: An Empty Raw Value Loses Its Transcript Separator

Associated work: #3344.

Source: [print_output](../../crates/cli/src/render.rs#L1503).

After `kv put a ""`, a piped `--raw kv get a` followed by `--raw ping` emits
only the version line. The successful empty value contributes no line break
because the transcript condition requires a nonempty answer. This disagrees
with S4's stated distinction between byte-exact one-shot output and separated
session answers.

Required: distinguish an empty `Answer::Bytes` hit from an absent answer when
adding transcript separators. One-shot empty raw reads should remain zero bytes.

### F14. P3: Table Alignment Counts Unicode Scalars, Not Terminal Columns

Associated work: #3333, shared with report/record layouts.

Source: [table widths](../../crates/cli/src/table.rs#L87),
[padding](../../crates/cli/src/table.rs#L143),
[record label widths](../../crates/cli/src/render.rs#L1008).

`chars().count()` aligns single-column Latin characters but not wide CJK
characters or combining sequences. A key such as `東京` shifts subsequent
cells relative to an ASCII row despite both being valid inputs. The current
Unicode test uses only `défaut`, which does not exercise this distinction.

Required: use display-column width for human layout and test wide characters,
combining marks, and emoji sequences. Raw output must remain unpadded.

## Remaining Release and Assurance Work

### G1. The Site Half of S5 Is Still Unfinished

This is an **acknowledged release prerequisite**, not an omission from the
deliberately core-only #3347 PR.

The locally cloned `stratadb.org` still has old receipts and branch JSON in
`scripts/verify-transcripts.mjs`, `src/lib/engine/heroScript.ts`, and
`src/content/docs/get-started/quickstart.mdx`. Running its verifier against the
reviewed binary fails **19 of 23 transcript assertions**. Regenerate the site
content and publish the matching WASM bundle with the release. The site's
existing dirty `src/data/release.json` was left untouched.

### G2. The Release Cannot Claim That Every JSON Response Is Unchanged

#3336 intentionally changes historical JSON gets from `json_value` to
`json_versioned_value`, adds an envelope level and metadata, and removes the
old Output variant. That fixes the documented inconsistency, and the targeted
JSON behavior tests pass. It is still an observable wire change, explicitly
acknowledged in the PR but contradicted by blanket statements in the contract
and frozen catalog.

Record the exception and release/versioning decision, update the shipped
changelog, and coordinate SDK/IPC consumers of the old historical-read shape.
Test the selected compatibility/refusal policy rather than assuming a docs fix
is wire-compatible. Removal of `--output-format`/`pretty`, the new `--human`
flag, and newline-free raw point reads also need user-facing release notes.

### G3. The Multi-Output Guard Follow-Up Lost Its Open Tracker

#3336 notes that display pointers should be checked against every non-progress
output a command may emit. It points to #3334, but #3334 is now closed. The
guard still walks the primary response only. The current offending JSON
producer was removed, so this is an **assurance gap**, not another current
JSON rendering failure.

Give this follow-up an open owner and an acceptance test. A second output must
not automatically be treated as a progress event just because its tag differs
from the primary fixture. The matrix currently makes that approximation.

### G4. Green Snapshots Do Not Establish Every Claimed Invariant

- The JSON matrix compares rendered output with the current typed serializer,
  not an independent byte-for-byte historical baseline. It proves that the
  renderer does not humanize JSON, not that no wire field/tag was changed.
- The matrix needs truncation, tombstones, control characters, escape collisions,
  very wide cells, empty raw hits, and multi-line scope restoration cases.
  These are observable missing dimensions behind F1 and F4-F13.
- CLI-owned reports have unit tests and some binary integration coverage, but
  do not participate in the exhaustive IDL-command matrix. Their host-only
  nature is not a reason to omit independent binary stdout/stderr/exit cells.
- [#3317](https://github.com/stratalab/strata-core/issues/3317) remains open:
  `idl_bin_dispatch`, `inference_hermetic_behavior`, and `vector_text_provenance`
  are not named in the unconditional full-feature per-PR test lane. Mutation
  baselines are not a replacement for guaranteed execution of those tests.

### G5. Reconcile the Contract With the Explicit Deferrals

#3332 remains open for tombstone cells, truncation notices, and the IPC-stop
conditional receipt. A named issue is useful tracking, but it does not satisfy
the accepted behavior. Either complete these requirements or explicitly approve
a smaller release contract. Update stale status prose and removed-format
references so the acceptance checklist describes one current state.

## PR Coverage

| PR | Reviewed Area | Outcome / Relevant Findings |
| --- | --- | --- |
| #3315 | Matrix, fixture edges, binary/script parity | G4; existing cells pass but miss adversarial dimensions |
| #3320 | IDL display types, schema walk, runtime index | F11, F12, G3 |
| #3321 | Family/schema guard, divergence ledger, encodings | Targeted tests pass; no additional current-wire defect identified |
| #3323 | Named response DTOs | No additional defect identified; changes preserve serialized fields |
| #3324 | Payload derivation and bidirectional inventory | Targeted tests pass; no additional defect identified |
| #3325 | Playground format propagation | Format behavior passes; native/parity scope gap F1 remains |
| #3329 | Session argument refusals | Refusals pass; broader inherited host-dispatch gap F2 |
| #3330 | Declared mutation receipts/raw identities | F7, F8 |
| #3331 | Shared line grammar and error reporting | Format/error tests pass; inherited F1 and F2 remain |
| #3333 | Tables, pages, history, search, analytics | F4, F6, F8, F10, F14; F11 assurance |
| #3336 | Versioned historical JSON reads | Behavior tests pass; intentional wire exception G2 and guard G3 |
| #3338 | Record/status/value rendering and nested tables | F4, F6, F9 |
| #3340 | Batch rendering and bespoke branch diff | F4, F9; mixed item-status tests pass |
| #3341 | Shared size vocabulary | Existing boundary tests pass; no additional issue assigned |
| #3342 | CLI-owned reports | Shared table concerns; inherited host-session gap F2; report coverage G4 |
| #3344 | Exact raw point-read bytes and removed format | Binary round trips pass; empty transcript case F13 |
| #3347 | Captured reference transcripts and prose guard | F3, propagation of F9, and site prerequisite G1 |
| #3346 | Explicit --human and flag conflicts | Targeted native tests pass; no additional issue identified |
| #3351 | Piped stderr ANSI | ANSI fix passes; remaining machine-channel issue F5 |

Unrelated mutation-CI and Result-alias cleanup commits in the same range were
not treated as part of the 19-PR implementation review.

## Verification

All of the following targeted tests passed on the combined state:

| Suite | Passed | Ignored |
| --- | ---: | ---: |
| CLI library, `inference,testkit` | 231 | 2 regeneration entry points |
| CLI `cli_execution` | 34 | 0 |
| CLI `output_contract` | 4 | 1 child entry point, executed by its parent |
| CLI `prose_transcripts` | 8 | 0 |
| Executor `idl_display`, `idl-tooling,inference,testkit` | 52 | 0 |
| Executor `idl_response_model`, same features | 23 | 0 |
| Executor `json_behavior`, same features | 21 | 0 |
| WASM `session`, actual wasm32 target with wasm-bindgen-test-runner | 13 | 0 |

Total: **386 passing tests**, excluding the temporary diagnostic probes.
The native build of the WASM session target ran zero tests because of its
target cfg and is not counted. `cargo check -p strata-wasm --target
wasm32-unknown-unknown` also passed, with existing feature-specific warnings.
All six IDL gates passed: `check`, `check-cli`, `check-docs`, `check-tests`,
`verify-examples`, and `verify-fixtures`.

Additional probes exercised the real binary and public renderer, and made
isolated changes in temporary IDL copies. They confirmed the behaviors and
measurements stated above. The temporary integration-test source was removed
after review. The site verifier failed as described in G1.

No full-workspace, mutation-testing, release-artifact, live cloud-provider,
local-model, or graphical browser run is claimed by this review. Passing these
focused suites is useful regression evidence, not proof that all possible
outputs or data sizes are correct.

## Suggested Order

1. Fix F1-F3: branch targeting, session panics, and executable documentation.
2. Bound human output and complete the data/channel contracts: F4-F10.
3. Implement or explicitly revise the promised guards: F11-F12 and G3-G4.
4. Resolve the smaller presentation cases F13-F14.
5. Publish an accurate release contract/changelog and matching site/WASM assets;
   rerun the expanded matrix and site verifier before closing #3314.
