# CLI Output Contract: Fix Review

Review date: 2026-09-12, America/Chicago; verification continued on September 13 UTC.

Final reviewed commit: `824bcb753cdb40d8bf3f9420db90756fb69fcc29`, on
`fix/3352-boundary-log-opt-in`. Comparison baseline: `19512cf9` from the
[original review](cli-output-contract-pre-ship-review-2026-09-12.md).

Scope: merged fixes #3360, #3361, #3362, #3363, #3364, #3366, #3367, plus
the local logging fix committed as `824bcb75` during this review.

**The original P1 reproductions are fixed. This is not yet a fully satisfied
output contract: three P2 implementation gaps remain, along with three P3
presentation/test issues and outstanding release integration.** The remaining
encoding and nested-field issues are incomplete coverage of the earlier
findings, not claims that the fixes introduced every underlying defect.

## Findings

### R1. P2: Text Cells and Receipts Still Emit Terminal Control Sequences

Source: [escape_cell](../../crates/cli/src/render.rs#L1367).
Related fix: #3360, original F7-F9.

The new `displayable_text` check correctly protects byte-valued fields,
including the internal vector identities in branch comparisons. Ordinary
string fields bypass that check. `escape_cell` handles backslash, newline,
tab, and carriage return, but leaves ESC and other control characters intact.

A real binary probe passed the JSON document ID `a\u001b[2Jb`, with an actual
ESC byte, to `json set`. The command succeeded. Its human receipt, the human
`json scan` row, and `--raw json list` all contained the literal escape
sequence. A JSON string value containing `\u001b[31mred` also reached scan
and history cells unchanged. These are display cells and receipts, not the
byte-exact raw point-read exception.

A terminal interprets these bytes instead of displaying the stored identity
or value literally. The generated-page text guard protects the current
example corpus, not arbitrary user data.

Required: define reversible escaping for all control characters in ordinary
text cells and receipt placeholders, and apply it consistently. Cover both
string and byte fields, successful and missed receipts, and human/raw tables.
Keep whole-value raw KV reads byte-exact.

### R2. P2: JSON Cells Are Still Not Reversible

Source: [JSON cell presentation](../../crates/cli/src/render.rs#L1308),
[raw_scalar](../../crates/cli/src/render.rs#L2274).
Related fixes: #3360 and #3366, original F8/F10.

Arrays and objects are serialized as JSON, but JSON strings lose their quotes
and scalar type information. Consequently a consumer cannot invert the new
Q16b cell encoding even when it knows the column is declared `as: json`.

Real binary probes, using separate fresh cache sessions with identical key
and commit version, produced these collisions in `--raw json scan`:

| Stored JSON values | Identical VALUE cell |
| --- | --- |
| Array `[1]` and string `"[1]"` | `[1]` |
| Number `1` and string `"1"` | `1` |
| Boolean `true` and string `"true"` | `true` |

The history fix correctly distinguishes a tombstone from a stored JSON null.
However, that stored null and the JSON string `"null"` both display `null` in
the history VALUE cell. In scans, a stored null still becomes an empty raw
cell, which also represents the empty JSON string.

Required: use a type-preserving encoding for JSON-valued cells, such as compact
JSON for every present JSON value, including strings and null, with a separate
absence/deletion convention. Add round-trip tests across all JSON types, not
only distinct string escape spellings. This finding concerns cells, not the
separately specified bare-leaf behavior of `--raw json get`.

### R3. P2: The New Field-Decision Guard Does Not Check Nested Records

Source: [top-level guard invocation](../../crates/executor/src/idl_tooling/display.rs#L97),
[nested recursion](../../crates/executor/src/idl_tooling/display.rs#L676),
[field inventory](../../crates/executor/src/idl_tooling/display.rs#L715).
Related fix: #3364, original F12.

The new guard checks the immediate record fields once. Selecting
`/data/memory_budget` counts the entire record as decided; recursion into its
`fields` validates pointers but never runs the omission inventory.

Two temporary tests against isolated copies of the actual IDL both failed
their expected-rejection assertions:

1. Add `new_important_fact` to `AdminMemoryBudget.properties` in the
   `admin.info` schema. `resolve_cli_index` still succeeds.
2. Remove the existing `/data/memory_budget/usable_host_bytes` display entry,
   leaving the schema unchanged. Resolution still succeeds without an omission
   decision.

The original top-level `new_important_fact` probe is now correctly rejected.
The remaining gap is the same selection rule that R2 explicitly requires for
nested records.

Required: inventory each selectively displayed record recursively. Key omission
decisions by the full field location so an omission at one depth does not
accidentally excuse an unrelated field with the same name elsewhere.

### R4. P3: The Empty-Answer Regression Test Pins the Product Version

Source: [an_empty_value_is_still_an_answer_in_a_session](../../crates/cli/tests/cli_execution.rs#L1349).
Related fix: #3367, original F13.

The separator behavior is fixed, but its regression test asserts that the next
`ping` prints the literal `1.2.1`. A normal version bump will fail this test
even when the separator behavior remains correct. Nearby changelog tests
already obtain the version from `env!("CARGO_PKG_VERSION")`.

Required: use the build's version in this assertion. This is a test defect;
the actual empty-hit/miss/one-shot behavior passed.

### R5. P3: Unicode Width Is Fixed for Tables, Not Record Blocks

Source: [record labels](../../crates/cli/src/render.rs#L1082),
[push_field](../../crates/cli/src/render.rs#L1224),
[report labels](../../crates/cli/src/report.rs#L193).
Related fix: #3363, original F14.

`Table` now uses terminal-column width correctly. Both record-block paths
still measure labels with `chars().count()`, and `push_field` uses Rust's
scalar-count string padding.

A temporary test through the public `value_to_string` report renderer, with
labels `aa` and two CJK characters, measured the value starts at terminal
columns 4 and 6 respectively. The expected-alignment assertion failed.

Current authored record labels are predominantly ASCII, so this is a smaller
residual presentation issue than the original user-key table problem. It is
not evidence that the newly corrected table path remains broken.

Required: share terminal-width measurement and padding across table and
record/report layouts. Include wide and combining labels in tests.

### R6. P3: The Logging Default Test Inherits the Logging Opt-In

Source: [binary test helpers](../../crates/cli/tests/cli_execution.rs#L12),
[logging test](../../crates/cli/tests/cli_execution.rs#L1172).
Related fix: `824bcb75`, original F5.

The new default-off logging behavior works. The test claiming to exercise
that default does not clear `STRATA_LOG` from child processes, so it can
accidentally test the opposite configuration.

This command fails the JSON-envelope assertion:

```sh
STRATA_LOG=error cargo test -p strata-cli --features inference,testkit \
  --test cli_execution the_boundary_log_is_asked_for_rather_than_assumed -- --exact
```

Required: clear `STRATA_LOG` in the baseline subprocess helpers, then apply
explicit overrides in `strata_env`. The default and opt-in cases should not
depend on the environment used to run the tests.

The same test's correlation assertion also needs strengthening: it extracts
`reference` from `logged`, then checks `logged.contains(reference)`, which is
necessarily true. Compare the independently parsed public error reference with
the log reference instead.

## Original Findings: Disposition

| Original | Result |
| --- | --- |
| F1: branch override leak | Fixed; regular and raw-command overrides, plus failure restoration, checked |
| F2: session panics | Fixed; host-only commands refuse and the session survives |
| F3: executable transcripts | Fixed for the tested corpus; semantic command round-trip guard passes |
| F4: padding amplification | Fixed by the column padding cap; wide-value regression passes |
| F5: unsolicited stderr logs | Fixed at `824bcb75`; default JSON error is parseable, opt-in logging retained; R6 covers test weaknesses |
| F6: partial-result notices | Fixed for declared BFS and hub cases; complete results stay quiet |
| F7: receipt delimiters | Original newline/tab reproduction fixed; remaining control-byte coverage is R1 |
| F8: cell collisions | Original text-escape and byte-marker collisions fixed; JSON type collisions remain in R2 |
| F9: internal vector control bytes | Original branch-comparison and generated-document cases fixed; ordinary text controls remain in R1 |
| F10: tombstone versus null | Original distinction fixed; broader JSON cell typing is R2 |
| F11: dates on logical clocks | Fixed by the command/pointer allowlist; negative guard tests pass |
| F12: omitted field decisions | Top-level case fixed; nested records remain unchecked, R3 |
| F13: empty raw hit separator | Behavior fixed; version-dependent regression test is R4 |
| F14: Unicode layout | Tables fixed; record blocks remain, R5 |

An intermediate version of the logging edit lost native feature gates and
failed the WASM build. The gates were restored before `824bcb75`; the final
WASM session run passes. That intermediate failure is not an outstanding finding.

## Still Required Before Release

- **G1, site integration:** the local `stratadb.org` verifier still fails
  **19 of 23** assertions against the current binary. The expected old write
  receipts and branch JSON remain in its transcript definitions. Update the
  site content and coordinate the corresponding released WASM bundle. The
  existing site `src/data/release.json` modification was not changed.
- **G2, release contract:** `CHANGELOG.md` is unchanged from the previous
  review and still starts with the September 6 release. Record the historical
  `json get` wire-shape exception, removed output-format/pretty interface,
  human/raw changes, and consumer/versioning decision. The frozen catalog
  still claims every JSON response is byte-identical across this work.
- **G3, alternate outputs:** the display resolver still checks the primary
  response schema. The matrix still decides whether an alternate output uses
  the declaration by comparing its tag to the primary fixture's tag
  (`output_contract.rs:227`). A different tag is not itself proof that an
  output is only a progress event. This remains an assurance gap rather than
  a newly observed current-wire rendering failure.
- **G4, CI coverage:** `.github/workflows/ci.yml` remains unchanged, and
  #3317 remains open. The three named testkit targets still need guaranteed
  per-PR execution. The new adversarial cases above should join the retained
  test suite. The JSON matrix still compares against the current serializer,
  not an independent historical wire baseline.
- **G5, documentation agreement:** #3332 is now closed and its BFS/tombstone
  requirements are implemented. Its closure records IPC stop as intentionally
  resolved by the declared record output. The frozen catalog still promises
  `stopped ipc host` / `no ipc host was running` rather than the actual
  `stopped  false` record; reconcile that documentation rather than treating
  the intentionally selected record as another unfixed implementation defect.

## Verification

Final targeted suites passed:

| Suite | Passed |
| --- | ---: |
| CLI library, `inference,testkit` | 241 |
| CLI `cli_execution` | 41 |
| CLI `output_contract` | 4 |
| CLI `prose_transcripts` | 8 |
| Executor `idl_display`, `idl-tooling,inference,testkit` | 56 |
| Executor `idl_response_model`, same features | 24 |
| Executor `json_behavior`, same features | 21 |
| WASM `session`, actual wasm32 target with wasm-bindgen-test-runner | 13 |

Total: **408 passing tests**, excluding repeated runs and diagnostic probes.
The CLI library has two ignored regeneration entry points; the matrix has one
ignored child entry point that its parent invokes.

All six IDL gates passed: `check`, `check-cli`, `check-docs`, `check-tests`,
`verify-examples`, and `verify-fixtures`. `git diff --check` passed.

Separately, the two nested-record rejection probes and the record-width probe
failed as described above; the logging test failed under an inherited opt-in.
Real binary probes demonstrated JSON cell collisions and literal terminal
control bytes. Temporary test sources were removed after verification.

No full-workspace, mutation, live-provider, local-model, graphical-browser,
or release-artifact validation is claimed. No implementation changes were
made, and no GitHub issues or comments were posted. Only this report was retained.
