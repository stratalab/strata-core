# CLI output catalog — every command, every format

**Status:** frozen design artifact after three review rounds (2026-09-11), companion to `cli-output-contract.md` (tracker #3314). Generated on 2026-09-11 from `main` at `60db96ac`: every command's `command-examples` steps and IDL fixture response, rendered through today's real renderer, next to what the contract's rules would print. The "proposed" text comes from a reference implementation of §4 R1–R7 written for the review; nothing here is shipped code, and nothing changes until S1. This file is not regenerated as slices land — the executable form of the "today" column is the §5 matrix (`crates/cli/tests/output_contract.rs` and its `output-contract/` snapshots), and each of S1–S5 moves the snapshots from the "today" column to the "proposed" one for the rows it owns.

137 commands · 169 example steps · 109 commands whose human or raw output changes · `--json` byte-identical throughout.

## 1. How to read it

- `$ strata …` is the real corpus command; commands without a CLI verb show their `command run --command-json` form; `(fixture …)` marks a command the corpus has no step for (hub, inference one-shots); a few steps were captured from the 1.2.1 binary for miss / paging cases the corpus lacks.
- Each step shows `human today` → `human proposed`, `raw today` → `raw proposed`, and the `--json` line, which does not change. `= unchanged` means the proposed output is byte-identical to today's.
- `stderr›` marks a line that goes to stderr; `[exit 1]` a non-zero exit; `∅` an empty stdout; `⇥` is a real TAB character (invisible otherwise) — `--raw` is tab-separated, and a few of today's designed arms use tabs too.
- Dates come from the dump run (2026-09-11 04:57:45 UTC); replay `committed_at` values in fixtures are `null` and render `-` (human) or an empty field (`--raw`).
- The one-line `display:` under each command is the declaration S0 adds to the IDL (`commands/*.yaml`), in the placeholder syntax of the contract's R2: `{field}` is a JSON pointer into the response, `|bytes` / `|size` / `|plural:noun` / `|len` are the four formatters; `fields …` lists the facts a status command shows (the rest is `--json`); `(header x)` marks an explicit human label where the wire name would misstate a humanised value.
- The three formats are three encodings of one declared shape. **`--json` is the unmodified record** — every wire field, byte-identical before and after this work. **Human mode** is what a person reads: the command's declared facts, aligned, humanised (dates, byte counts), with diagnostics on stderr. **`--raw` is the shell-composable form** of the same declared facts — bare values, TSV rows, `key<TAB>value` lines, no header, no hint, no humanising — for `cut`, `xargs`, `while read`, `wc -l`. It is not the raw wire; a script that wants the whole record uses `--json`.
- This is the third review round (2026-09-11): the five revisions from the second — idempotent delete-of-missing, curated status fields, relaxed labels, batch summaries only on attention, the `--raw` mental model — plus the third round's two consistency fixes (batch summaries on read batches; one `--raw` rule for record reads) and the `-- sampled` polish are applied throughout and listed in §3.

## 2. Cross-cutting behaviour

| Situation | Today | Proposed |
|---|---|---|
| Write applied | `created setting applied=true` on stdout | `created setting` on stdout, exit 0 |
| Delete of a missing target | `not_found missing applied=false` on **stdout**, exit **0**; `--raw` prints nothing | **idempotent**: stderr `no such key: missing`, stdout empty, exit **0** (human and `--raw`); `--json` prints the `applied: false` envelope on stdout, exit 0, nothing on stderr (stderr in `--json` mode carries only error envelopes). Q2 revised on review 2026-09-11; `--require-existing` can add the assertive form later |
| Bulk delete matching nothing | `not_found docs applied=false`, exit 0 | `deleted 0 vectors from docs`, exit 0 (Q14: zero matches is not a miss) |
| Read miss | `(nil)`, exit 0; `--raw` prints nothing | unchanged |
| Empty page | `(empty)` on stdout; `--raw` prints `(empty)` too | `(empty)` on stdout; `--raw` prints **nothing** |
| More pages | `-- more: add --cursor <c> to the same command` on **stdout** | same line on **stderr** |
| Sample | no hint | stderr `-- sampled N of <total_count>` only when the sample is smaller than the population |
| Batch summary | none (one JSON line per item) | stderr `-- itemwise: 2 ok, 1 miss` **only when an item missed or failed**; a fully successful batch is the table alone |
| Status records | every wire field, pretty JSON | the command's declared `fields` only (`hub get-dataset` shows 15 of 27); the rest is `--json` |
| Diagnostic lines (stderr) | mixed into stdout | only when something needs attention: more pages, a partial sample, a truncated traversal or listing, a miss, a batch whose engine status is not `ok`. Full success prints no stderr |
| Record reads, `--raw` | `vector get` compact JSON of `data`; `event get` the payload; `graph get-node` the properties; `graph get-edge` and `graph meta` one TSV line; `ontology get` compact JSON — five behaviours for one family | one rule from the declaration: a declared **value** prints verbatim; declared **fields** print as `key<TAB>value` lines (Q16), the same as a status object |
| Dates | `committed_at` rewritten to local `2026-09-10 21:57:45.412845 -07:00` **inside** the JSON line | `2026-09-11 04:57:45 UTC` in a table cell only; never inside a JSON value; `--json` keeps the integer |
| Bytes (kv) | key/value decoded to UTF-8 inside the JSON line; binary shows as base64 with no marker | UTF-8 verbatim in cells; non-UTF-8 as `base64:<b64>`; `--raw` writes the bytes verbatim |
| Errors | stderr `code: message (err_ref)` + `  hint:` + `  ref:` lines, exit 1 | unchanged, identical in every format (`--json` prints the error envelope on stderr, exit 1) |
| Usage errors | clap on stderr, exit 2 | unchanged |
| `--json` | one line, wire envelope | **byte-identical, every command, every slice** — the one unmodified representation |
| `--raw` | a per-shape sniff; nothing for writes; compact JSON for statuses | the **shell-composable** form of the human answer: the same declared fields and columns, as bare values / TSV / `key<TAB>value`, with no header, alignment, hint or humanising. It is not the raw wire — that is `--json`. The `--raw` help text says so (S1) |
| `--pretty` / `--output-format` | hidden flag | deleted in S4 (Q5) |
| TTY | no sniffing | no sniffing (R6) |
| Human labels | n/a | the wire field name; an explicit `header:` only where the wire name misstates the humanised value (`size_bytes` → `size`, `SIZE`); never in `--raw`, whose keys are always wire names (Q18 as relaxed on review) |
| Null / absent field | `null` inside JSON | human: `-`; `--raw`: an **empty** field (never `-`, never `null`); `--json`: `null` |
| Floats | JSON literal | human: up to six decimals, integral floats keep `.0` (`1.0`, `0.184417`); `--raw` / `--json`: full precision (Q19) |
| Multi-line strings in `--raw` | n/a (JSON) | escaped `\n` so `--raw` stays one record per line |

Exit codes: 0 = the command did what it was asked (including `(nil)` / `(empty)` and an idempotent delete of nothing), 1 = the engine refused (an executor error), 2 = usage. Exit codes are the same in every format.

## 3. Decisions this catalog adds to §10 of the contract

Q14, Q16, Q18 and the R4 rewording were decided 2026-09-11 (marked ✅); the second review round the same day **revised five things** (marked 🔁) — each is applied throughout the catalog below. The third round (same day, the go for S0) found **two places where this catalog contradicted the contract** and one polish item, all fixed (marked 🛠). Q15, Q17, Q19, Q20 stand as proposed unless the review says otherwise.

- 🛠 **Successful read batches still showed `-- itemwise: 2 ok`.** A generator bug, not a design change: the reference implementation tested `applied == false` as its "atomic batch not applied" signal, and every *read* batch (`batch_get`, `batch_exists`) carries `applied: false` on the wire because a read applies nothing. The signal is the engine's own top-level `status` (`ok` / `partial` / `failed`) — one field, declared, and it already encodes "an item missed or failed, or the batch did not apply". The renderer in S2 reads that field and nothing else.
- 🛠 **Record-valued `Maybe<T>` had five `--raw` behaviours.** `vector get` printed compact JSON of `data`, `event get` the payload, `graph get-node` the properties, `graph get-edge` and `graph meta` a one-line TSV, `ontology get` compact JSON — sensible one by one, underivable as a family. Now one rule, and it is the same rule status objects already follow: a `read.get` declares either a **value** (`kv get`, `json get`, `config <key>` — human prints it, `--raw` prints it verbatim) or **fields** (a record — human prints the key/value block, `--raw` prints `key<TAB>value` lines of the same fields, Q16). No command-level `raw:` projection exists; the declaration that drives human mode drives raw mode. `graph get-edge --raw` is therefore `src<TAB>alice` / `edge_type<TAB>knows` / … rather than `alice<TAB>knows<TAB>bob<TAB>1.0`; a script that wants the row uses `cut -f2 | paste -s`, and one that wants the record uses `--json`.
- 🛠 **`-- sampled 3 of 3` is gone.** A sample that returned the whole population needs no one's attention; the notice appears only when the sample is smaller than the population. This is the "diagnostics only on attention" principle applied to the last place it was not.

- 🔁 **Q2 — delete of a missing target is idempotent, exit 0.** Reverses the first-round decision (stderr + exit 1). `strata kv delete missing` prints `no such key: missing` on stderr as feedback, nothing on stdout, and exits 0 — redis `DEL` → `0`, SQL `DELETE 0`, `kubectl delete --ignore-not-found`. The shell now agrees with the wire (`Ok`, `applied: false`), and `--json` no longer returns a success envelope alongside a failing exit. Same in `--raw`. In `--json` mode the envelope is the feedback: stdout carries it, stderr stays empty (stderr in `--json` mode carries only error envelopes). If assertive deletion is ever needed it is a flag (`--require-existing`), not the default.
- 🔁 **Status records show declared facts, not every field.** The family fixes the layout (`field  value` lines); the **command** declares which facts appear and in what order (`display.fields`). `hub get-dataset` shows 15 of its 27 wire fields (no `readme`, `schema`, `sample_preview`, `provenance`, `manifest_hash`, version stamps or the frontmatter blob); `admin info` drops `open` and `created`; `branch get` drops `branch_id` and `state_revision`. `--json` is the complete record — that is what it is for. Every status command declares its list, even when the list is "all of them", so a new wire field is a `check` failure until someone decides where it goes. `--raw` shows the same declared fields.
- 🔁 **Q18 relaxed — wire name by default, an explicit `header:` where it materially helps.** The rule stays "label = wire field name" (`vector_revision`, `event_type`, `branch_count`) and headers are the same names in UPPERCASE. The one class where the wire name misstates the humanised value — byte counts — declares a header: `size_bytes  1 kB` becomes `size  1 kB`, `SIZE_BYTES` becomes `SIZE`, `memory_budget.total_bytes` becomes `total  6.7 GB`. A `header:` is a human-mode label only; `--raw` keys and `--json` names are always the wire names, so nothing is learned twice. Any other `header:` needs a reason in the declaration.
- 🔁 **Batch summaries only when something needs attention.** A fully successful batch is the table alone — its STATUS column already reads `ok` on every row, and `-- itemwise: 2 ok (version 3)` after it was noise. The stderr summary appears only for misses, failures or an atomic batch that did not apply; the commit version is in `--json`. The same rule governs every diagnostic line: more pages, a sample, a truncated traversal or a truncated hub listing get one; a complete listing gets none (`hub list-datasets` drops its `-- showing 1 of 1` line; `hub list-yanked` drops its footer).
- 🔁 **`--raw` is shell-composable output, not raw wire output.** It keeps the same declared fields and columns as human mode and drops the alignment, headers, hints and humanising — bare values, TSV rows, `key<TAB>value` lines — so `cut`, `xargs`, `while read` and `wc -l` work. The unmodified representation is `--json`. No new flag; the mental model is written into the contract (R4) and the `--raw` help text (S1).
- ✅ **Q14 — bulk deletes with zero matches.** `vector delete-all`, `vector delete-by-filter` and `graph apply_delete_policy` report `applied: false` when nothing matched. Not a miss — stdout `deleted 0 vectors from docs`, exit 0. The command did what it was asked; only a *named* target that is absent is a miss.
- **Q15 — `bool` acks.** `json index drop` and `vector collection delete` return a bare `true`/`false` with no identity on the wire. Proposed: the receipt takes the name from the request (`dropped index by_name`), declared as `identity: request.name`, until the #3313 wire normalisation puts it on the wire. The alternative is a verb-only line (`dropped`), which is worse for the user and no more honest.
- ✅ **Q16 — `--raw` for status objects.** Today: compact JSON. `key<TAB>value` lines with dotted keys (`memory_budget.total_bytes<TAB>6661720064`), arrays as one compact-JSON field (`tags<TAB>["tabular"]`), null as an empty value — so `--raw` is TSV everywhere and `--json` is the only JSON. Machine consumers that want the object use `--json`.
- **Q17 — receipts for actions.** `arrow export`, `arrow import`, `clone` and `ipc stop` are `action.status` and today print pretty JSON. Proposed: a one-line receipt (`exported 1 row of kv to /tmp/exports/kv.parquet (1.9 kB)`), same as writes; the full record stays in `--json`, and `--raw` prints the record as key/value lines like any status.
- **Q19 — floats in tables.** Scores, ranks, weights print with up to six decimals in human tables (`0.184417`), and an integral float keeps one decimal (`1.0`) so it still reads as a float; `--raw` and `--json` keep full precision.
- **Q20 — `branch diff` / `branch preview` identities.** kv identities are keys and decode to UTF-8; vector identities are the engine's internal key encoding (`AAVub3Rlc24x`) and do not. The catalog shows them as `base64:…`; the engine should expose `collection` + `key` for vector rows — a wire question for the #3313 normalisation, not the renderer.

Everything else follows the decided Q1–Q13.

**Boundary with the Python SDK.** `display:` declarations (`identity`, `columns`, `fields`, `header`) are CLI-only. The Python SDK is a sibling of the CLI, not a wrapper around it: both derive from the IDL, and an SDK method returns the typed wire record (`db.branch.get("x")` → a `BranchItem` with every field), never the CLI's curated subset or its receipt strings. S0 adds a guard that the SDK generator ignores `display:`. The SDK's own redesign (`db.kv.put("greeting", b"hello")`, `db.branch.fork("experiment")`, `db.vector.query("docs", vector, k=10)`) follows the #3313 wire normalisation — the nominal-vs-wire divergences are fixed or declared first, then the Python surface is designed on a wire that is true.

## 4. What the survey found on the way (today's renderer)

Evidence for Roots A–C of the contract, all reproduced from the replay corpus:

- `mutation_summary` picks the wrong subject or none: `graph add-node social alice` → `created social applied=true`; `graph create social` → `created applied=true`; `event append user.created …` → `created applied=true`; `graph ontology define-object-type g person` → `created g applied=true`; `graph remove-node g zz` → `not_found g applied=false` (the graph, not the node, is named).
- Every `MutationAck` prints **nothing** under `--raw`.
- Acks without an `effect` fall through to pretty JSON: `branch create`, `branch fork`, `json index create`, `graph bulk-insert`, `graph ontology freeze`; `vector collection create` prints a one-item page as a JSON line; `json index drop` prints `true`.
- Batch commands print one compact JSON line per item with the humanised local date **inside** it (Root B) — while the kv keys on the same line stay base64 (`"key":"YQ=="`), so one line mixes a humanised field with an un-humanised one.
- `has_more` and `-- more:` go to stdout, so `strata kv list | wc -l` is off by one on a paged result.
- `--raw` on an empty page prints the literal `(empty)`.
- Fixture error responses carry no `message`, so every error in the corpus renders as `<code>: command failed`; the binary prints the real message, hint and ref. The errors shown per command below take message and hint from the registry, which are class-generic (`The inference request is invalid.`); the binary's message is usually more specific (`invalid_argument.engine.kv_key: KV key must not be empty`). The shape — `code: message (err_ref)`, `hint:`, `ref:` on stderr, exit 1 — is what the catalog reviews; it does not change.
- `vector query --raw` is a designed arm today (`key<TAB>score`); under R4 it becomes TSV of the declared columns, which adds a METADATA field.
- The same `Output` variant carries different identity fields across commands (`graph_delete_result`: `graph` / `graph`+`node_id` / `graph`+`src`+`edge_type`+`dst`), so the `display` declaration belongs to the **command**, not the DTO — a correction to the contract's R2 wording.
- `--raw` for tables: the contract's R4 said "identity only"; this catalog proposed TSV of the declared columns (a scan that prints only keys is not a scan). ✅ DECIDED 2026-09-11 — R4 reworded in the contract.

## 5. Rules by family

| Family | Kinds | Commands | Rule |
|---|---|---|---|
| Writes | `mutation.put`, `mutation.create`, `mutation.delete`, `mutation.bulk_delete`, `mutation.metadata_update`, `mutation.merge` | 36 | One receipt line on stdout: `<verb> <identity>`, verb from `effect.kind` (`created` / `updated` / `deleted` / `unchanged`), identity from the declared fields. Nothing else. A delete of something that is not there is **idempotent**: `no such <noun>: <identity>` on **stderr** as feedback, empty stdout, exit **0** (Q2, revised on review 2026-09-11 — redis `DEL` / SQL `DELETE` semantics; `--require-existing` can add the assertive form later). Bulk deletes report the count (Q14). `--raw` prints the identity only (nothing on a miss, same stderr). Acks without an `effect` (branch create/fork/merge, bulk insert, ontology freeze, index create, collection create) use a fixed receipt template. `bool` acks (`json index drop`, `vector collection delete`) take the identity from the request because the wire carries none (Q15). |
| Reads: single value or record | `read.get` | 11 | `kv get` prints the bytes (UTF-8 verbatim, else `base64:…`); `json get` prints the JSON value (pretty for objects and arrays, the literal for scalars). Records print `field  value` lines, labels are the wire field names, nested objects are one indented block, JSON-valued fields are compact JSON. A miss prints `(nil)` on stdout and exits 0 (Q9). One rule for `--raw`, derived from the declaration: a command that declares a **value** (`kv get`, `json get`, `config <key>`) prints that value verbatim (bytes / compact JSON / the scalar); a command that declares **fields** (a record — vector, event, graph node/edge, graph meta, ontology) prints `key<TAB>value` lines of the same fields, exactly as a status object does (Q16). No per-command raw projection; nothing for a miss. |
| Reads: history | `read.history` | 3 | A table, newest first as the wire orders it: `VERSION  COMMITTED_AT  VALUE` (+ `DOCUMENT_VERSION` / `VECTOR_REVISION`). A tombstone row shows `(deleted)` in the value column. `committed_at` is the only date and it lives in a cell: `2026-09-11 04:57:45 UTC`, `-` when unknown (R3). `--raw` is the same columns as TSV, no header, epoch microseconds; a tombstone row has an empty VALUE cell (the `tombstone` flag itself is in `--json`). |
| Reads: pages, samples, search | `read.page`, `read.sample`, `read.search`, `read.diagnostics` | 25 | Pages of scalars print one per line (unchanged). Pages of records print a kubectl-style table: UPPERCASE headers named after the wire fields, two-space gutter, no borders, numbers right-aligned; logical `timestamp` is omitted (it duplicates `version`). `has_more` → stderr `-- more: add --cursor <c> to the same command`; a sample adds stderr `-- sampled N of <total_count>` only when N < total_count (a sample that returned everything needs no notice); an empty page prints `(empty)`. `--raw` is TSV of the same columns, no header. |
| Reads: graph analytics | `read.analytics` | 6 | `NODE` plus the declared value column, sorted so the answer reads top-down (rank descending, distance / depth ascending, otherwise by node). Input echoes (`graph`, `source`, `iterations`, `personalized`, `direction`) are not repeated — they are in `--json`. `bfs` adds stderr `-- truncated` when the traversal was cut. `--raw` is `node<TAB>value`. |
| Status, summaries, actions | `read.status`, `read.summary`, `action.status` | 30 | `field  value` lines — the family fixes the layout, the **command declares which facts appear** (`display.fields`, in that order); `--json` is the complete record. Labels are the wire field names, except an explicit `header:` where the wire name misstates the humanised value (`size_bytes` → `size  1 kB`; Q18 as relaxed on review). Nested objects indented, arrays of objects as an indented table, byte counts humanised (`6.7 GB`). Scalar statuses (`count`, `exists`) print the scalar. Actions (`arrow export/import`, `clone`, `ipc stop`) get a one-line receipt like writes. `describe` and `ping` keep their designed lines. `--raw` prints `key<TAB>value` lines of the same declared fields, wire names as keys, dotted for nesting (Q16). A diagnostic line on stderr only when a listing is truncated. |
| Batches | `batch.itemwise_mutation`, `batch.atomic_mutation`, `batch.itemwise_read`, `batch.itemwise_status` | 14 | A table with one row per item: `#  STATUS  [EFFECT]  <declared columns>` (+ `ERROR` when any item failed). A stderr summary `-- <mode>: N ok, M miss/failed` **only when an item missed or failed** — full success is the table alone (revised on review 2026-09-11). `--raw` is the same rows as TSV. Batch commands have no CLI verb today (`command run --command-json` only). |
| Inference | `inference.runtime_op`, `inference.models_page` | 12 | The designed arms (`models list`, `embed`, `generate`, `status`, `pull`, `rank`, `tokenize`, `detokenize`, `unload`) are unchanged. `capability` and `cache-status`, which fall through to pretty JSON today, become `field  value` lines of their declared fields like every other status. |

## 6. The catalog

### 6.1 Writes (36 commands)

One receipt line on stdout: `<verb> <identity>`, verb from `effect.kind` (`created` / `updated` / `deleted` / `unchanged`), identity from the declared fields. Nothing else. A delete of something that is not there is **idempotent**: `no such <noun>: <identity>` on **stderr** as feedback, empty stdout, exit **0** (Q2, revised on review 2026-09-11 — redis `DEL` / SQL `DELETE` semantics; `--require-existing` can add the assertive form later). Bulk deletes report the count (Q14). `--raw` prints the identity only (nothing on a miss, same stderr). Acks without an `effect` (branch create/fork/merge, bulk insert, ontology freeze, index create, collection create) use a fixed receipt template. `bool` acks (`json index drop`, `vector collection delete`) take the identity from the request because the wire carries none (Q15).

#### `graph.bulk_insert` — `strata graph bulk-insert`

*mutation.put · MutationAck<GraphBulkInsert> · stable*  
display: receipt `inserted {nodes_inserted|plural:node}, {edges_inserted|plural:edge} into {graph}` · raw `{graph}`

```text
(fixture responses/v1/graph/bulk_insert_applied.json)
human today         {
                      "commit": {
                        "committed_at": null,
                        "delete_count": 0,
                        "durability": "not_durable",
                        "put_count": 2,
                        "timestamp": 5,
                        "version": 5
                      },
                      "commits": 2,
                      "edges_inserted": 2,
                      "graph": "social",
                      "nodes_inserted": 3
                    }
human proposed      inserted 3 nodes, 2 edges into social
raw today           {"commit":{"committed_at":null,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":5,"version":5},"commits":2,"edges_inserted":2,"graph":"social","nodes_inserted":3}
raw proposed        social
--json (unchanged)  {"data":{"commit":{"committed_at":null,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":5,"version":5},"commits":2,"edges_inserted":2,"graph":"social","nodes_inserted":3},"type":"graph_bulk_insert_result"}
```

#### `graph.edge.add` — `strata graph add-edge`

*mutation.put · MutationAck<GraphEdgeWrite> · stable*  
display: receipt `{verb} edge {src} -[{edge_type}]-> {dst} in {graph}` · raw `{src}\t{edge_type}\t{dst}`

```text
$ strata graph add-edge social alice knows bob
human today         created social applied=true
human proposed      created edge alice -[knows]-> bob in social
raw today           ∅ (prints nothing)
raw proposed        alice⇥knows⇥bob
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665431212,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":6,"version":6},"dst":"bob","edge_type":"knows","effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"graph":"social","src":"alice"},"type":"graph_edge_write_result"}
```

#### `graph.node.add` — `strata graph add-node`

*mutation.put · MutationAck<GraphNodeWrite> · stable*  
display: receipt `{verb} node {node_id} in {graph}` · raw `{node_id}`

```text
$ strata graph add-node social alice --type person --properties {"age":30}
human today         created social applied=true
human proposed      created node alice in social
raw today           ∅ (prints nothing)
raw proposed        alice
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665436116,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"graph":"social","node_id":"alice"},"type":"graph_node_write_result"}
```

#### `graph.ontology.define_link_type` — `strata graph ontology define-link-type`

*mutation.put · MutationAck<GraphOntologyWrite> · stable*  
display: receipt `{verb} link type {type_name} in {graph}` · raw `{type_name}`

```text
$ strata graph ontology define-link-type g knows person person
human today         created g applied=true
human proposed      created link type knows in g
raw today           ∅ (prints nothing)
raw proposed        knows
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665439844,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"graph":"g","kind":"link","type_name":"knows"},"type":"graph_ontology_write_result"}
```

#### `graph.ontology.define_object_type` — `strata graph ontology define-object-type`

*mutation.put · MutationAck<GraphOntologyWrite> · stable*  
display: receipt `{verb} object type {type_name} in {graph}` · raw `{type_name}`

```text
$ strata graph ontology define-object-type g person
human today         created g applied=true
human proposed      created object type person in g
raw today           ∅ (prints nothing)
raw proposed        person
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665440553,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"graph":"g","kind":"object","type_name":"person"},"type":"graph_ontology_write_result"}
```

#### `graph.ontology.freeze` — `strata graph ontology freeze`

*mutation.put · MutationAck<GraphOntologyFreeze> · stable*  
display: receipt `froze ontology of {graph} ({object_types|plural:object type}, {link_types|plural:link type})` · raw `{graph}`

```text
$ strata graph ontology freeze g
human today         {
                      "commit": {
                        "committed_at": "2026-09-11 04:57:45.443166 +00:00",
                        "delete_count": 0,
                        "durability": "not_durable",
                        "put_count": 1,
                        "timestamp": 5,
                        "version": 5
                      },
                      "graph": "g",
                      "link_types": 0,
                      "object_types": 1
                    }
human proposed      froze ontology of g (1 object type, 0 link types)
raw today           {"commit":{"committed_at":"2026-09-11 04:57:45.443166 +00:00","delete_count":0,"durability":"not_durable","put_count":1,"timestamp":5,"version":5},"graph":"g","link_types":0,"object_types":1}
raw proposed        g
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665443166,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":5,"version":5},"graph":"g","link_types":0,"object_types":1},"type":"graph_ontology_freeze_result"}
```

#### `json.set` — `strata json set`

*mutation.put · MutationAck<JsonWrite> · stable*  
display: receipt `{verb} {key}` · raw `{key}`

```text
$ strata json set user $ {"age":30,"name":"alice"}
human today         created user applied=true
human proposed      created user
raw today           ∅ (prints nothing)
raw proposed        user
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665456598,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"key":"user"},"type":"json_write_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.json_document_id: The JSON document request is invalid. (err_…)
  hint: Use a valid JSON document id and a JSON value within the configured size and depth limits.
  ref: https://stratadb.org/e/invalid_argument.engine.json_document_id
```

#### `kv.put` — `strata kv put`

*mutation.put · MutationAck<KvWrite> · stable*  
display: receipt `{verb} {key|bytes}` · raw `{key|bytes}`

```text
$ strata kv put setting v1
human today         created setting applied=true
human proposed      created setting
raw today           ∅ (prints nothing)
raw proposed        setting
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665462582,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"key":"c2V0dGluZw=="},"type":"write_result"}
```
```text
$ strata kv put setting v2
human today         updated setting applied=true
human proposed      updated setting
raw today           ∅ (prints nothing)
raw proposed        setting
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665462692,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"updated","matched":true},"key":"c2V0dGluZw=="},"type":"write_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.kv_key: The KV request is invalid. (err_…)
  hint: Use non-empty KV keys and keep batch structure within the documented limits.
  ref: https://stratadb.org/e/invalid_argument.engine.kv_key
```

#### `vector.collection.set_embedding_model` — `strata vector collection set-embedding-model`

*mutation.put · MutationAck<VectorCollectionInfo> · transitional*  
display: receipt `updated collection {items.0.name}: embedding model {items.0.embedding_model}` · raw `{items.0.name}`

```text
$ strata vector collection set-embedding-model docs openai:text-embedding-3-small
human today         {"count":0,"dimension":3,"embedding_model":"openai:text-embedding-3-small","metric":"cosine","name":"docs"}
human proposed      updated collection docs: embedding model openai:text-embedding-3-small
raw today           {"count":0,"dimension":3,"embedding_model":"openai:text-embedding-3-small","metric":"cosine","name":"docs"}
raw proposed        docs
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"count":0,"dimension":3,"embedding_model":"openai:text-embedding-3-small","metric":"cosine","name":"docs"}]},"type":"vector_collection_list"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.embedding_model: The embedding model id is invalid. (err_…)
  hint: Use a non-empty model id such as `openai:text-embedding-3-small` or `miniLM`.
  ref: https://stratadb.org/e/invalid_argument.engine.embedding_model
failed_precondition.engine.embedding_model_mismatch: The embedding model does not match the one this vector collection records. (err_…)
  hint: Embed with the model the collection records (see `vector collection stats`), or create a separate collection for the other model.
  ref: https://stratadb.org/e/failed_precondition.engine.embedding_model_mismatch
not_found.engine.vector_collection: The requested vector collection was not found. (err_…)
  hint: List vector collections or create the collection before issuing vector operations.
  ref: https://stratadb.org/e/not_found.engine.vector_collection
```

#### `vector.upsert` — `strata vector upsert`

*mutation.put · MutationAck<VectorWrite> · stable*  
display: receipt `{verb} {key} in {collection}` · raw `{key}`

```text
$ strata vector upsert docs a [1.0,0.0,0.0] --metadata {"tag":"x"}
human today         created a applied=true
human proposed      created a in docs
raw today           ∅ (prints nothing)
raw proposed        a
--json (unchanged)  {"data":{"collection":"docs","commit":{"committed_at":1789102665482665,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"key":"a","vector_revision":1},"type":"vector_write_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
failed_precondition.engine.embedding_model_missing: This vector collection records no embedding model, so text cannot be embedded for it. (err_…)
  hint: Declare the collection's model with `vector collection set-embedding-model <collection> <model>`, or pass a vector instead of text.
  ref: https://stratadb.org/e/failed_precondition.engine.embedding_model_missing
invalid_argument.executor.vector_input: A vector write or query supplied neither a vector nor a text, or both. (err_…)
  hint: Pass exactly one: an explicit vector, or a text to embed with the collection's recorded model.
  ref: https://stratadb.org/e/invalid_argument.executor.vector_input
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
```

#### `branch.create` — `strata branch create`

*mutation.create · MutationAck<BranchItem> · stable*  
display: receipt `created branch {name}` · raw `{name}`

```text
$ strata branch create feature
human today         {
                      "branch_id": "dc42122c-83b7-5436-89bc-9ffa4299697c",
                      "created_at": 3,
                      "deleted_at": null,
                      "generation": 1,
                      "name": "feature",
                      "parent": null,
                      "state_revision": 0,
                      "status": "active"
                    }
human proposed      created branch feature
raw today           {"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"}
raw proposed        feature
--json (unchanged)  {"data":{"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"},"type":"branch"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
already_exists.engine.branch: A branch with this name already exists. (err_…)
  hint: Choose a different branch name or delete the existing branch first.
  ref: https://stratadb.org/e/already_exists.engine.branch
```

#### `branch.fork` — `strata branch fork`

*mutation.create · MutationAck<BranchItem> · stable*  
display: receipt `forked {name} from {parent.name}` · raw `{name}`

```text
$ strata branch fork default experiment
human today         {
                      "branch_id": "1a29fdd4-745b-5b66-ad18-75b3cf51cef6",
                      "created_at": 3,
                      "deleted_at": null,
                      "generation": 1,
                      "name": "experiment",
                      "parent": {
                        "branch_id": "00000000-0000-0000-0000-000000000000",
                        "fork_timestamp": null,
                        "fork_version": 1,
                        "generation": 1,
                        "name": "default"
                      },
                      "state_revision": 0,
                      "status": "active"
                    }
human proposed      forked experiment from default
raw today           {"branch_id":"1a29fdd4-745b-5b66-ad18-75b3cf51cef6","created_at":3,"deleted_at":null,"generation":1,"name":"experiment","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":null,"fork_version":1,"generation":1,"name":"default"},"state_revision":0,"status":"active"}
raw proposed        experiment
--json (unchanged)  {"data":{"branch_id":"1a29fdd4-745b-5b66-ad18-75b3cf51cef6","created_at":3,"deleted_at":null,"generation":1,"name":"experiment","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":null,"fork_version":1,"generation":1,"name":"default"},"state_revision":0,"status":"active"},"type":"branch"}
```

#### `branch.fork_at_timestamp` — `command run --command-json`

*mutation.create · MutationAck<BranchItem> · stable*  
display: receipt `forked {name} from {parent.name} at timestamp {parent.fork_timestamp}` · raw `{name}`

```text
$ strata command run --command-json '{"branch":"snapshot","source":"default","timestamp":3,"type":"branch_fork_at_timestamp"}'
human today         {
                      "branch_id": "39de3743-01cb-53e3-9cb2-371a4599ccdf",
                      "created_at": 5,
                      "deleted_at": null,
                      "generation": 1,
                      "name": "snapshot",
                      "parent": {
                        "branch_id": "00000000-0000-0000-0000-000000000000",
                        "fork_timestamp": 3,
                        "fork_version": 3,
                        "generation": 1,
                        "name": "default"
                      },
                      "state_revision": 0,
                      "status": "active"
                    }
human proposed      forked snapshot from default at timestamp 3
raw today           {"branch_id":"39de3743-01cb-53e3-9cb2-371a4599ccdf","created_at":5,"deleted_at":null,"generation":1,"name":"snapshot","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":3,"fork_version":3,"generation":1,"name":"default"},"state_revision":0,"status":"active"}
raw proposed        snapshot
--json (unchanged)  {"data":{"branch_id":"39de3743-01cb-53e3-9cb2-371a4599ccdf","created_at":5,"deleted_at":null,"generation":1,"name":"snapshot","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":3,"fork_version":3,"generation":1,"name":"default"},"state_revision":0,"status":"active"},"type":"branch"}
```

#### `branch.fork_at_version` — `command run --command-json`

*mutation.create · MutationAck<BranchItem> · stable*  
display: receipt `forked {name} from {parent.name} at version {parent.fork_version}` · raw `{name}`

```text
$ strata command run --command-json '{"branch":"snapshot","source":"default","type":"branch_fork_at_version","version":3}'
human today         {
                      "branch_id": "39de3743-01cb-53e3-9cb2-371a4599ccdf",
                      "created_at": 5,
                      "deleted_at": null,
                      "generation": 1,
                      "name": "snapshot",
                      "parent": {
                        "branch_id": "00000000-0000-0000-0000-000000000000",
                        "fork_timestamp": null,
                        "fork_version": 3,
                        "generation": 1,
                        "name": "default"
                      },
                      "state_revision": 0,
                      "status": "active"
                    }
human proposed      forked snapshot from default at version 3
raw today           {"branch_id":"39de3743-01cb-53e3-9cb2-371a4599ccdf","created_at":5,"deleted_at":null,"generation":1,"name":"snapshot","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":null,"fork_version":3,"generation":1,"name":"default"},"state_revision":0,"status":"active"}
raw proposed        snapshot
--json (unchanged)  {"data":{"branch_id":"39de3743-01cb-53e3-9cb2-371a4599ccdf","created_at":5,"deleted_at":null,"generation":1,"name":"snapshot","parent":{"branch_id":"00000000-0000-0000-0000-000000000000","fork_timestamp":null,"fork_version":3,"generation":1,"name":"default"},"state_revision":0,"status":"active"},"type":"branch"}
```

#### `event.append` — `strata event append`

*mutation.create · MutationAck<EventAppend> · stable*  
display: receipt `appended {event_type} #{sequence}` · raw `{sequence}`

```text
$ strata event append user.created {"id":1}
human today         created applied=true
human proposed      appended user.created #0
raw today           ∅ (prints nothing)
raw proposed        0
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665412263,"delete_count":0,"durability":"not_durable","put_count":3,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"event_type":"user.created","sequence":0},"type":"event_append_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.event_type: The event request is invalid. (err_…)
  hint: Use a valid event type, payload, and metadata within the documented event limits.
  ref: https://stratadb.org/e/invalid_argument.engine.event_type
```

#### `graph.create` — `strata graph create`

*mutation.create · MutationAck<GraphInfoData> · stable*  
display: receipt `{verb} graph {info.graph}` · raw `{info.graph}`

```text
$ strata graph create social
human today         created applied=true
human proposed      created graph social
raw today           ∅ (prints nothing)
raw proposed        social
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665429781,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"info":{"created_timestamp":3,"created_version":3,"edge_count":0,"graph":"social","node_count":0,"updated_timestamp":3,"updated_version":3}},"type":"graph_create_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.graph_name: The graph request is invalid. (err_…)
  hint: Use valid graph, node, edge, and binding identifiers and keep graph properties within limits.
  ref: https://stratadb.org/e/invalid_argument.engine.graph_name
already_exists.engine.graph: A graph with this name already exists. (err_…)
  hint: Choose a different graph name or delete the existing graph first.
  ref: https://stratadb.org/e/already_exists.engine.graph
```

#### `json.index.create` — `strata json index create`

*mutation.create · MutationAck<JsonIndexCreate> · transitional*  
display: receipt `created index {name} on {field_path} ({index_type})` · raw `{name}`

```text
$ strata json index create by_name $.name --index-type tag
human today         {
                      "created_timestamp": 3,
                      "created_version": 3,
                      "field_path": "name",
                      "index_type": "tag",
                      "name": "by_name",
                      "space": "default"
                    }
human proposed      created index by_name on name (tag)
raw today           {"created_timestamp":3,"created_version":3,"field_path":"name","index_type":"tag","name":"by_name","space":"default"}
raw proposed        by_name
--json (unchanged)  {"data":{"created_timestamp":3,"created_version":3,"field_path":"name","index_type":"tag","name":"by_name","space":"default"},"type":"json_index_definition"}
```

#### `space.create` — `strata space create`

*mutation.create · MutationAck<SpaceCreate> · stable*  
display: receipt `{verb} space {space}` · raw `{space}`

```text
$ strata space create app
human today         created app applied=true
human proposed      created space app
raw today           ∅ (prints nothing)
raw proposed        app
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665464382,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"space":"app"},"type":"space_create_result"}
```

#### `vector.collection.create` — `strata vector collection create`

*mutation.create · MutationAck<VectorCollectionCreate> · transitional*  
display: receipt `created collection {items.0.name} ({items.0.dimension} dimensions, {items.0.metric})` · raw `{items.0.name}`

```text
$ strata vector collection create docs 3 --metric cosine
human today         {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
human proposed      created collection docs (3 dimensions, cosine)
raw today           {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
raw proposed        docs
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"count":0,"dimension":3,"metric":"cosine","name":"docs"}]},"type":"vector_collection_list"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.vector_dimension: The vector request is invalid. (err_…)
  hint: Use the collection dimension, valid vector keys, and metadata/filter values supported by the collection.
  ref: https://stratadb.org/e/invalid_argument.engine.vector_dimension
invalid_argument.engine.embedding_model: The embedding model id is invalid. (err_…)
  hint: Use a non-empty model id such as `openai:text-embedding-3-small` or `miniLM`.
  ref: https://stratadb.org/e/invalid_argument.engine.embedding_model
```

#### `branch.delete` — `strata branch delete`

*mutation.delete · MutationAck<BranchDelete> · stable*  
display: receipt `{verb} branch {branch.name}` · miss → stderr `no such branch: {branch.name}`, exit 0 (idempotent) · raw `{branch.name}`

```text
$ strata branch delete temp
human today         deleted applied=true
human proposed      deleted branch temp
raw today           ∅ (prints nothing)
raw proposed        temp
--json (unchanged)  {"data":{"branch":{"branch_id":"39d446db-cbca-54ec-b793-509e5325483b","created_at":3,"deleted_at":6,"generation":1,"name":"temp","parent":null,"state_revision":2,"status":"deleted"},"cleanup":{"protected_tables":0,"releasable_tables":0,"removed_refs":0},"deleted":true,"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"generation_after":1,"generation_before":1},"type":"branch_delete_result"}
```

#### `graph.delete` — `strata graph delete`

*mutation.delete · MutationAck<GraphDelete> · stable*  
display: receipt `{verb} graph {graph}` · miss → stderr `no such graph: {graph}`, exit 0 (idempotent) · raw `{graph}`

```text
$ strata graph delete temp
human today         deleted temp applied=true
human proposed      deleted graph temp
raw today           ∅ (prints nothing)
raw proposed        temp
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665430366,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"graph":"temp"},"type":"graph_delete_result"}
```
```text
$ strata graph delete nope
human today         not_found nope applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such graph: nope
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such graph: nope
--json (unchanged)  {"data":{"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"graph":"nope"},"type":"graph_delete_result"}
```

#### `graph.edge.remove` — `strata graph remove-edge`

*mutation.delete · MutationAck<GraphDelete> · stable*  
display: receipt `{verb} edge {src} -[{edge_type}]-> {dst} in {graph}` · miss → stderr `no such edge: {src} -[{edge_type}]-> {dst} in {graph}`, exit 0 (idempotent) · raw `{src}\t{edge_type}\t{dst}`

```text
$ strata graph remove-edge social alice knows bob
human today         deleted social applied=true
human proposed      deleted edge alice -[knows]-> bob in social
raw today           ∅ (prints nothing)
raw proposed        alice⇥knows⇥bob
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665433185,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":7,"version":7},"dst":"bob","edge_type":"knows","effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"graph":"social","src":"alice"},"type":"graph_delete_result"}
```
```text
$ strata graph remove-edge g a knows zz
human today         not_found g applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such edge: a -[knows]-> zz in g
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such edge: a -[knows]-> zz in g
--json (unchanged)  {"data":{"dst":"zz","edge_type":"knows","effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"graph":"g","src":"a"},"type":"graph_delete_result"}
```

#### `graph.node.remove` — `strata graph remove-node`

*mutation.delete · MutationAck<GraphDelete> · stable*  
display: receipt `{verb} node {node_id} in {graph}` · miss → stderr `no such node: {node_id} in {graph}`, exit 0 (idempotent) · raw `{node_id}`

```text
$ strata graph remove-node social alice
human today         deleted social applied=true
human proposed      deleted node alice in social
raw today           ∅ (prints nothing)
raw proposed        alice
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665438286,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"graph":"social","node_id":"alice"},"type":"graph_delete_result"}
```
```text
$ strata graph remove-node g zz
human today         not_found g applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such node: zz in g
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such node: zz in g
--json (unchanged)  {"data":{"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"graph":"g","node_id":"zz"},"type":"graph_delete_result"}
```

#### `graph.ontology.delete_link_type` — `strata graph ontology delete-link-type`

*mutation.delete · MutationAck<GraphOntologyDelete> · stable*  
display: receipt `{verb} link type {type_name} in {graph}` · miss → stderr `no such link type: {type_name} in {graph}`, exit 0 (idempotent) · raw `{type_name}`

```text
$ strata graph ontology delete-link-type g knows
human today         deleted g applied=true
human proposed      deleted link type knows in g
raw today           ∅ (prints nothing)
raw proposed        knows
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665441498,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":6,"version":6},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"graph":"g","kind":"link","type_name":"knows"},"type":"graph_ontology_delete_result"}
```

#### `graph.ontology.delete_object_type` — `strata graph ontology delete-object-type`

*mutation.delete · MutationAck<GraphOntologyDelete> · stable*  
display: receipt `{verb} object type {type_name} in {graph}` · miss → stderr `no such object type: {type_name} in {graph}`, exit 0 (idempotent) · raw `{type_name}`

```text
$ strata graph ontology delete-object-type g company
human today         deleted g applied=true
human proposed      deleted object type company in g
raw today           ∅ (prints nothing)
raw proposed        company
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665442335,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":6,"version":6},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"graph":"g","kind":"object","type_name":"company"},"type":"graph_ontology_delete_result"}
```
```text
$ strata graph ontology delete-object-type g nope
human today         not_found g applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such object type: nope in g
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such object type: nope in g
--json (unchanged)  {"data":{"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"graph":"g","kind":"object","type_name":"nope"},"type":"graph_ontology_delete_result"}
```

#### `json.delete` — `strata json delete`

*mutation.delete · MutationAck<JsonDelete> · stable*  
display: receipt `{verb} {key}` · miss → stderr `no such key: {key}`, exit 0 (idempotent) · raw `{key}`

```text
$ strata json delete temp $
human today         deleted temp applied=true
human proposed      deleted temp
raw today           ∅ (prints nothing)
raw proposed        temp
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665450896,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"key":"temp"},"type":"json_delete_result"}
```
```text
$ strata json delete nope $
human today         not_found nope applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such key: nope
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such key: nope
--json (unchanged)  {"data":{"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"key":"nope"},"type":"json_delete_result"}
```

#### `json.index.drop` — `strata json index drop`

*mutation.delete · MutationAck<JsonIndexDrop> · transitional*  
display: receipt `dropped index {request.name}` · miss → stderr `no such index: {request.name}`, exit 0 (idempotent) · raw `{request.name}`

```text
$ strata json index drop by_name
human today         true
human proposed      dropped index by_name
raw today           true
raw proposed        by_name
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata json index drop nope
human today         false
human proposed      ∅ (prints nothing)
                    stderr› no such index: nope
raw today           false
raw proposed        ∅ (prints nothing)
                    stderr› no such index: nope
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `kv.delete` — `strata kv delete`

*mutation.delete · MutationAck<KvDelete> · stable*  
display: receipt `{verb} {key|bytes}` · miss → stderr `no such key: {key|bytes}`, exit 0 (idempotent) · raw `{key|bytes}`

```text
$ strata kv delete temp
human today         deleted temp applied=true
human proposed      deleted temp
raw today           ∅ (prints nothing)
raw proposed        temp
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665460077,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"key":"dGVtcA=="},"type":"delete_result"}
```
```text
$ strata kv delete missing
human today         not_found missing applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such key: missing
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such key: missing
--json (unchanged)  {"data":{"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"key":"bWlzc2luZw=="},"type":"delete_result"}
```

#### `space.delete` — `strata space delete`

*mutation.delete · MutationAck<SpaceDelete> · stable*  
display: receipt `{verb} space {space} ({deleted_rows|plural:row})` · miss → stderr `no such space: {space}`, exit 0 (idempotent) · raw `{space}`

```text
$ strata space delete temp
human today         deleted temp applied=true
human proposed      deleted space temp (0 rows)
raw today           ∅ (prints nothing)
raw proposed        temp
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665464977,"delete_count":0,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"deleted_rows":0,"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"force":false,"space":"temp"},"type":"space_delete_result"}
```
```text
$ strata space delete nope
human today         not_found nope applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such space: nope
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such space: nope
--json (unchanged)  {"data":{"deleted_rows":0,"effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"force":false,"space":"nope"},"type":"space_delete_result"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.space_delete_default: The requested space operation cannot be completed. (err_…)
  hint: Use an existing non-reserved space, or delete/move contained data before deleting the space.
  ref: https://stratadb.org/e/invalid_argument.engine.space_delete_default
```

#### `vector.collection.delete` — `strata vector collection delete`

*mutation.delete · MutationAck<VectorCollectionDelete> · transitional*  
display: receipt `deleted collection {request.name}` · miss → stderr `no such collection: {request.name}`, exit 0 (idempotent) · raw `{request.name}`

```text
$ strata vector collection delete temp
human today         true
human proposed      deleted collection temp
raw today           true
raw proposed        temp
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata vector collection delete nope
human today         false
human proposed      ∅ (prints nothing)
                    stderr› no such collection: nope
raw today           false
raw proposed        ∅ (prints nothing)
                    stderr› no such collection: nope
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `vector.delete` — `strata vector delete`

*mutation.delete · MutationAck<VectorDelete> · stable*  
display: receipt `{verb} {key} in {collection}` · miss → stderr `no such vector: {key} in {collection}`, exit 0 (idempotent) · raw `{key}`

```text
$ strata vector delete docs a
human today         deleted a applied=true
human proposed      deleted a in docs
raw today           ∅ (prints nothing)
raw proposed        a
--json (unchanged)  {"data":{"collection":"docs","commit":{"committed_at":1789102665473344,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"key":"a"},"type":"vector_delete_result"}
```
```text
$ strata vector delete docs nope
human today         not_found nope applied=false
human proposed      ∅ (prints nothing)
                    stderr› no such vector: nope in docs
raw today           ∅ (prints nothing)
raw proposed        ∅ (prints nothing)
                    stderr› no such vector: nope in docs
--json (unchanged)  {"data":{"collection":"docs","effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false},"key":"nope"},"type":"vector_delete_result"}
```

#### `graph.apply_delete_policy` — `command run --command-json`

*mutation.bulk_delete · MutationAck<GraphDeletePolicyApply> · stable*  
display: receipt `applied delete policy {policy} ({effect.affected_count} affected)` · zero matches → receipt with count, exit 0 · raw `{effect.affected_count}`

```text
$ strata command run --command-json '{"policy":"cascade","target":{"key":"user:1","primitive":"kv","space":"default"},"type":"graph_apply_delete_policy"}'
human today         deleted applied=true
human proposed      applied delete policy cascade (1 affected)
raw today           ∅ (prints nothing)
raw proposed        1
--json (unchanged)  {"data":{"commit":{"committed_at":1789102665426995,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"policy":"cascade"},"type":"graph_delete_policy_result"}
```

#### `vector.delete_all` — `strata vector delete-all`

*mutation.bulk_delete · MutationAck<VectorBulkDelete> · stable*  
display: receipt `deleted {effect.affected_count|plural:vector} from {collection}` · zero matches → receipt with count, exit 0 · raw `{effect.affected_count}`

```text
$ strata vector delete-all docs
human today         deleted docs applied=true
human proposed      deleted 1 vector from docs
raw today           ∅ (prints nothing)
raw proposed        1
--json (unchanged)  {"data":{"collection":"docs","commit":{"committed_at":1789102665474103,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true}},"type":"vector_bulk_delete_result"}
```
```text
$ strata vector delete-all docs   (empty collection)
human today         not_found docs applied=false
human proposed      deleted 0 vectors from docs
raw today           ∅ (prints nothing)
raw proposed        0
--json (unchanged)  {"data":{"collection":"docs","effect":{"affected_count":0,"applied":false,"kind":"not_found","matched":false}},"type":"vector_bulk_delete_result"}
```

#### `vector.delete_by_filter` — `strata vector delete-by-filter`

*mutation.bulk_delete · MutationAck<VectorBulkDelete> · stable*  
display: receipt `deleted {effect.affected_count|plural:vector} from {collection}` · zero matches → receipt with count, exit 0 · raw `{effect.affected_count}`

```text
$ strata vector delete-by-filter docs --filter {"conditions":[{"field":"tag","op":"eq","value":{"type":"string","value":"drop"}}]}
human today         deleted docs applied=true
human proposed      deleted 1 vector from docs
raw today           ∅ (prints nothing)
raw proposed        1
--json (unchanged)  {"data":{"collection":"docs","commit":{"committed_at":1789102665475141,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":6,"version":6},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true}},"type":"vector_bulk_delete_result"}
```

#### `vector.metadata.update` — `strata vector update-metadata`

*mutation.metadata_update · MutationAck<VectorMetadataUpdate> · stable*  
display: receipt `{verb} {key} in {collection}` · miss → stderr `no such vector: {key} in {collection}`, exit 0 (idempotent) · raw `{key}`

```text
$ strata vector update-metadata docs a {"tag":"z"}
human today         updated a applied=true
human proposed      updated a in docs
raw today           ∅ (prints nothing)
raw proposed        a
--json (unchanged)  {"data":{"collection":"docs","commit":{"committed_at":1789102665479486,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"updated","matched":true},"key":"a","vector_revision":2},"type":"vector_metadata_update_result"}
```

#### `branch.merge` — `strata branch merge`

*mutation.merge · MutationAck<PromotionOutcomeItem> · stable*  
display: receipt `merged {source} into {target}: {applied|len} applied, {deleted|len} deleted, {conflicts|len} conflicts (version {target_version})` · raw `{target_version}`

```text
$ strata branch merge experiment default --strategy strict
human today         {
                      "applied": [
                        {
                          "capability": "kv",
                          "identity": "config",
                          "space": "default",
                          "value": "tuned"
                        }
                      ],
                      "branch_point": 3,
                      "capabilities_covered": [
                        "kv",
                        "json",
                        "vector"
                      ],
                      "capabilities_unsupported": [
                        "vector_collection",
                        "event",
                        "graph_metadata",
                        "graph_node",
                        "graph_edge",
                        "graph_ontology"
                      ],
                      "conflicts": [],
                      "deleted": [],
                      "derived_state": [],
                      "source": "experiment",
                      "spaces_covered": [
                        "default"
                      ],
                      "strategy": "strict",
                      "target": "default",
                      "target_timestamp": 9,
                      "target_version": 9
                    }
human proposed      merged experiment into default: 1 applied, 0 deleted, 0 conflicts (version 9)
raw today           {"applied":[{"capability":"kv","identity":"config","space":"default","value":"tuned"}],"branch_point":3,"capabilities_covered":["kv","json","vector"],"capabilities_unsupported":["vector_collection","event","graph_metadata","graph_node","graph_edge","graph_ontology"],"conflicts":[],"deleted":[],"derived_state":[],"source":"experiment","spaces_covered":["default"],"strategy":"strict","target":"default","target_timestamp":9,"target_version":9}
raw proposed        9
--json (unchanged)  {"data":{"applied":[{"capability":"kv","identity":"Y29uZmln","space":"default","value":"dHVuZWQ="}],"branch_point":3,"capabilities_covered":["kv","json","vector"],"capabilities_unsupported":["vector_collection","event","graph_metadata","graph_node","graph_edge","graph_ontology"],"conflicts":[],"deleted":[],"derived_state":[],"source":"experiment","spaces_covered":["default"],"strategy":"strict","target":"default","target_timestamp":9,"target_version":9},"type":"branch_merge"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
conflict.engine.promotion: The request conflicts with current state. (err_…)
  hint: Reload current state and retry against the latest branch, space, or collection version.
  ref: https://stratadb.org/e/conflict.engine.promotion
invalid_argument.engine.branch_point: The request contains invalid input. (err_…)
  hint: Correct the invalid field named by the error message and retry the operation.
  ref: https://stratadb.org/e/invalid_argument.engine.branch_point
```

### 6.2 Reads: single value or record (11 commands)

`kv get` prints the bytes (UTF-8 verbatim, else `base64:…`); `json get` prints the JSON value (pretty for objects and arrays, the literal for scalars). Records print `field  value` lines, labels are the wire field names, nested objects are one indented block, JSON-valued fields are compact JSON. A miss prints `(nil)` on stdout and exits 0 (Q9). One rule for `--raw`, derived from the declaration: a command that declares a **value** (`kv get`, `json get`, `config <key>`) prints that value verbatim (bytes / compact JSON / the scalar); a command that declares **fields** (a record — vector, event, graph node/edge, graph meta, ontology) prints `key<TAB>value` lines of the same fields, exactly as a status object does (Q16). No per-command raw projection; nothing for a miss.

#### `admin.config_key` — `strata config get-key`

*read.get · Maybe<String> · stable*  
display: value `data` as scalar · miss `(nil)` · raw = the value

```text
(fixture responses/v1/admin/config_value_default_branch.json)
human today         default
human proposed      = unchanged
raw today           default
raw proposed        = unchanged
--json (unchanged)  {"data":"default","type":"config_value"}
```
```text
$ strata config get-key missing
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":null,"type":"config_value"}
```

#### `admin.remote` — `strata remote`

*read.get · Maybe<RemoteOriginInfo> · stable*  
display: record: every RemoteOriginInfo field · origin null → `(nil)` · raw = key/value lines

```text
$ strata remote
human today         {
                      "origin": null
                    }
human proposed      (nil)
raw today           {"origin":null}
raw proposed        ∅ (prints nothing)
--json (unchanged)  {"data":{"origin":null},"type":"remote_origin_result"}
```

#### `event.get` — `strata event get`

*read.get · Maybe<EventVersionedData> · stable*  
display: fields sequence, event_type, timestamp, payload, hash, previous_hash, version · miss `(nil)` · raw = key/value lines of the same fields

```text
$ strata event get 0
human today         {
                      "event": {
                        "event_type": "user.created",
                        "hash": "568c2ccd449f9ddf56f8c00d917af4fe762380ffaa43b7df10a2305b3704ead9",
                        "payload": {
                          "id": 1
                        },
                        "previous_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                        "sequence": 0,
                        "timestamp": 1789102665414786
                      },
                      "timestamp": 3,
                      "version": 3
                    }
human proposed      sequence       0
                    event_type     user.created
                    timestamp      2026-09-11 04:57:45 UTC
                    payload        {"id":1}
                    hash           568c2ccd449f9ddf56f8c00d917af4fe762380ffaa43b7df10a2305b3704ead9
                    previous_hash  0000000000000000000000000000000000000000000000000000000000000000
                    version        3
raw today           {"event":{"event_type":"user.created","hash":"568c2ccd449f9ddf56f8c00d917af4fe762380ffaa43b7df10a2305b3704ead9","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665414786},"timestamp":3,"version":3}
raw proposed        sequence⇥0
                    event_type⇥user.created
                    timestamp⇥1789102665414786
                    payload⇥{"id":1}
                    hash⇥568c2ccd449f9ddf56f8c00d917af4fe762380ffaa43b7df10a2305b3704ead9
                    previous_hash⇥0000000000000000000000000000000000000000000000000000000000000000
                    version⇥3
--json (unchanged)  {"data":{"found":true,"value":{"event":{"event_type":"user.created","hash":"568c2ccd449f9ddf56f8c00d917af4fe762380ffaa43b7df10a2305b3704ead9","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665414786},"timestamp":3,"version":3}},"type":"event_record"}
```
```text
$ strata event get 999
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"event_record"}
```

#### `graph.edge.get` — `strata graph get-edge`

*read.get · Maybe<GraphEdgeDataOutput> · stable*  
display: fields src, edge_type, dst, graph, weight, version · miss `(nil)` · raw = key/value lines of the same fields

```text
$ strata graph get-edge social alice knows bob
human today         {
                      "dst": "bob",
                      "edge_type": "knows",
                      "graph": "social",
                      "src": "alice",
                      "timestamp": 6,
                      "version": 6,
                      "weight": 1.0
                    }
human proposed      src        alice
                    edge_type  knows
                    dst        bob
                    graph      social
                    weight     1.0
                    version    6
raw today           {"dst":"bob","edge_type":"knows","graph":"social","src":"alice","timestamp":6,"version":6,"weight":1.0}
raw proposed        src⇥alice
                    edge_type⇥knows
                    dst⇥bob
                    graph⇥social
                    weight⇥1.0
                    version⇥6
--json (unchanged)  {"data":{"found":true,"value":{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","timestamp":6,"version":6,"weight":1.0}},"type":"graph_edge_result"}
```
```text
$ strata graph get-edge social alice knows absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"graph_edge_result"}
```

#### `graph.meta` — `strata graph meta`

*read.get · Maybe<GraphInfoData> · stable*  
display: fields graph, node_count, edge_count, created_version, updated_version · miss `(nil)` · raw = key/value lines of the same fields

```text
$ strata graph meta social
human today         {
                      "created_timestamp": 3,
                      "created_version": 3,
                      "edge_count": 0,
                      "graph": "social",
                      "node_count": 2,
                      "updated_timestamp": 5,
                      "updated_version": 5
                    }
human proposed      graph            social
                    node_count       2
                    edge_count       0
                    created_version  3
                    updated_version  5
raw today           {"created_timestamp":3,"created_version":3,"edge_count":0,"graph":"social","node_count":2,"updated_timestamp":5,"updated_version":5}
raw proposed        graph⇥social
                    node_count⇥2
                    edge_count⇥0
                    created_version⇥3
                    updated_version⇥5
--json (unchanged)  {"data":{"created_timestamp":3,"created_version":3,"edge_count":0,"graph":"social","node_count":2,"updated_timestamp":5,"updated_version":5},"type":"graph_info_result"}
```

#### `graph.node.get` — `strata graph get-node`

*read.get · Maybe<GraphNodeDataOutput> · stable*  
display: fields node_id, graph, object_type, properties, version · miss `(nil)` · raw = key/value lines of the same fields

```text
$ strata graph get-node social alice
human today         {
                      "graph": "social",
                      "node_id": "alice",
                      "object_type": "person",
                      "properties": {
                        "age": 30
                      },
                      "timestamp": 4,
                      "version": 4
                    }
human proposed      node_id      alice
                    graph        social
                    object_type  person
                    properties   {"age":30}
                    version      4
raw today           {"graph":"social","node_id":"alice","object_type":"person","properties":{"age":30},"timestamp":4,"version":4}
raw proposed        node_id⇥alice
                    graph⇥social
                    object_type⇥person
                    properties⇥{"age":30}
                    version⇥4
--json (unchanged)  {"data":{"found":true,"value":{"graph":"social","node_id":"alice","object_type":"person","properties":{"age":30},"timestamp":4,"version":4}},"type":"graph_node_result"}
```
```text
$ strata graph get-node social absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"graph_node_result"}
```

#### `graph.ontology.get` — `strata graph ontology get`

*read.get · Maybe<GraphOntologyData> · stable*  
display: record: graph, status, version, object_types table, link_types table · raw = key/value lines of the same fields

```text
$ strata graph ontology get g
human today         {
                      "graph": "g",
                      "object_types": [
                        {
                          "name": "person"
                        }
                      ],
                      "status": "draft",
                      "timestamp": 4,
                      "version": 4
                    }
human proposed      graph         g
                    status        draft
                    version       4
                    object_types
                      NAME
                      person
raw today           {"graph":"g","object_types":[{"name":"person"}],"status":"draft","timestamp":4,"version":4}
raw proposed        graph⇥g
                    status⇥draft
                    version⇥4
                    object_types⇥[{"name":"person"}]
--json (unchanged)  {"data":{"graph":"g","object_types":[{"name":"person"}],"status":"draft","timestamp":4,"version":4},"type":"graph_ontology_result"}
```

#### `graph.ontology.summary` — `strata graph ontology summary`

*read.get · Maybe<GraphOntologySummaryData> · stable*  
display: record: graph, status, version, object_types table, link_types table · raw = key/value lines of the same fields

```text
$ strata graph ontology summary g
human today         {
                      "graph": "g",
                      "object_types": [
                        {
                          "name": "person",
                          "node_count": 0
                        }
                      ],
                      "status": "draft",
                      "timestamp": 4,
                      "version": 4
                    }
human proposed      graph         g
                    status        draft
                    version       4
                    object_types
                      NAME    NODE_COUNT
                      person           0
raw today           {"graph":"g","object_types":[{"name":"person","node_count":0}],"status":"draft","timestamp":4,"version":4}
raw proposed        graph⇥g
                    status⇥draft
                    version⇥4
                    object_types⇥[{"name":"person","node_count":0}]
--json (unchanged)  {"data":{"graph":"g","object_types":[{"name":"person","node_count":0}],"status":"draft","timestamp":4,"version":4},"type":"graph_ontology_summary_result"}
```

#### `json.get` — `strata json get`

*read.get · Maybe<JsonVersionedValue> · stable*  
display: value `value.value` as json · miss `(nil)` · raw = the value

```text
$ strata json get user $
human today         {"age":30,"name":"alice"}
human proposed      {
                      "age": 30,
                      "name": "alice"
                    }
raw today           {"age":30,"name":"alice"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":true,"value":{"document_version":1,"timestamp":3,"value":{"age":30,"name":"alice"},"version":3}},"type":"json_versioned_value"}
```
```text
$ strata json get user $.name
human today         "alice"
human proposed      = unchanged
raw today           alice
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":true,"value":{"document_version":1,"timestamp":3,"value":"alice","version":3}},"type":"json_versioned_value"}
```
```text
$ strata json get absent $
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"json_versioned_value"}
```

#### `kv.get` — `strata kv get`

*read.get · Maybe<VersionedValue> · stable*  
display: value `value.value` as bytes · miss `(nil)` · raw = the value

```text
$ strata kv get greeting
human today         hello
human proposed      = unchanged
raw today           hello
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":true,"value":{"timestamp":3,"value":"aGVsbG8=","version":3}},"type":"kv_versioned_value"}
```
```text
$ strata kv get absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"kv_versioned_value"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
history_unavailable.engine.persistence_history: The requested resource was not found. (err_…)
  hint: Request history inside the retained window.
  ref: https://stratadb.org/e/history_unavailable.engine.persistence_history
```

#### `vector.get` — `strata vector get`

*read.get · Maybe<VectorVersionedData> · stable*  
display: fields key, vector_revision, version, embedding, metadata · miss `(nil)` · raw = key/value lines of the same fields

```text
$ strata vector get docs a
human today         {
                      "data": {
                        "embedding": [
                          1.0,
                          0.0,
                          0.0
                        ]
                      },
                      "key": "a",
                      "timestamp": 4,
                      "vector_revision": 1,
                      "version": 4
                    }
human proposed      key              a
                    vector_revision  1
                    version          4
                    embedding        [1.0,0.0,0.0]
                    metadata         -
raw today           {"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}
raw proposed        key⇥a
                    vector_revision⇥1
                    version⇥4
                    embedding⇥[1.0,0.0,0.0]
                    metadata⇥
--json (unchanged)  {"data":{"found":true,"value":{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}},"type":"vector_data"}
```
```text
$ strata vector get docs absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":{"found":false,"value":null},"type":"vector_data"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
not_found.engine.vector_collection: The requested vector collection was not found. (err_…)
  hint: List vector collections or create the collection before issuing vector operations.
  ref: https://stratadb.org/e/not_found.engine.vector_collection
```

### 6.3 Reads: history (3 commands)

A table, newest first as the wire orders it: `VERSION  COMMITTED_AT  VALUE` (+ `DOCUMENT_VERSION` / `VECTOR_REVISION`). A tombstone row shows `(deleted)` in the value column. `committed_at` is the only date and it lives in a cell: `2026-09-11 04:57:45 UTC`, `-` when unknown (R3). `--raw` is the same columns as TSV, no header, epoch microseconds; a tombstone row has an empty VALUE cell (the `tombstone` flag itself is in `--json`).

#### `json.history` — `strata json history`

*read.history · Maybe<Vec<JsonHistoryItem>> · stable*  
display: columns VERSION  DOCUMENT_VERSION  COMMITTED_AT (date)  VALUE (json) · raw TSV

```text
(fixture responses/v1/json/history_found.json)
human today         {"document_version":2,"timestamp":4,"tombstone":false,"value":{"age":36,"name":"Ada"},"version":4}
                    {"document_version":1,"timestamp":3,"tombstone":false,"value":{"name":"Ada"},"version":3}
human proposed      VERSION  DOCUMENT_VERSION  COMMITTED_AT  VALUE
                          4                 2  -             {"age":36,"name":"Ada"}
                          3                 1  -             {"name":"Ada"}
raw today           {"document_version":2,"timestamp":4,"tombstone":false,"value":{"age":36,"name":"Ada"},"version":4}
                    {"document_version":1,"timestamp":3,"tombstone":false,"value":{"name":"Ada"},"version":3}
raw proposed        4⇥2⇥⇥{"age":36,"name":"Ada"}
                    3⇥1⇥⇥{"name":"Ada"}
--json (unchanged)  {"data":[{"document_version":2,"timestamp":4,"tombstone":false,"value":{"age":36,"name":"Ada"},"version":4},{"document_version":1,"timestamp":3,"tombstone":false,"value":{"name":"Ada"},"version":3}],"type":"json_version_history"}
```
```text
$ strata json history absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":null,"type":"json_version_history"}
```

#### `kv.history` — `strata kv history`

*read.history · Maybe<Vec<HistoryItem>> · stable*  
display: columns VERSION  COMMITTED_AT (date)  VALUE (bytes) · raw TSV

```text
(fixture responses/v1/kv/history_found.json)
human today         {"timestamp":4,"tombstone":false,"value":"two","version":4}
                    {"timestamp":3,"tombstone":false,"value":"one","version":3}
human proposed      VERSION  COMMITTED_AT  VALUE
                          4  -             two
                          3  -             one
raw today           {"timestamp":4,"tombstone":false,"value":"two","version":4}
                    {"timestamp":3,"tombstone":false,"value":"one","version":3}
raw proposed        4⇥⇥two
                    3⇥⇥one
--json (unchanged)  {"data":{"items":[{"timestamp":4,"tombstone":false,"value":"dHdv","version":4},{"timestamp":3,"tombstone":false,"value":"b25l","version":3}]},"type":"version_history"}
```
```text
$ strata kv history absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":null,"type":"version_history"}
```

#### `vector.history` — `strata vector history`

*read.history · Maybe<Vec<VectorHistoryItem>> · stable*  
display: columns VERSION  VECTOR_REVISION  COMMITTED_AT (date)  EMBEDDING (json)  METADATA (json) · raw TSV

```text
(fixture responses/v1/vector/history_found.json)
human today         {"data":{"embedding":[1.0,0.0],"metadata":{"kind":"doc"}},"key":"doc-a","timestamp":4,"tombstone":false,"vector_revision":1,"version":4}
human proposed      VERSION  VECTOR_REVISION  COMMITTED_AT  EMBEDDING  METADATA
                          4                1  -             [1.0,0.0]  {"kind":"doc"}
raw today           {"data":{"embedding":[1.0,0.0],"metadata":{"kind":"doc"}},"key":"doc-a","timestamp":4,"tombstone":false,"vector_revision":1,"version":4}
raw proposed        4⇥1⇥⇥[1.0,0.0]⇥{"kind":"doc"}
--json (unchanged)  {"data":{"items":[{"data":{"embedding":[1.0,0.0],"metadata":{"kind":"doc"}},"key":"doc-a","timestamp":4,"tombstone":false,"vector_revision":1,"version":4}]},"type":"vector_version_history"}
```
```text
$ strata vector history docs absent
human today         (nil)
human proposed      = unchanged
raw today           ∅ (prints nothing)
raw proposed        = unchanged
--json (unchanged)  {"data":null,"type":"vector_version_history"}
```

### 6.4 Reads: pages, samples, search (25 commands)

Pages of scalars print one per line (unchanged). Pages of records print a kubectl-style table: UPPERCASE headers named after the wire fields, two-space gutter, no borders, numbers right-aligned; logical `timestamp` is omitted (it duplicates `version`). `has_more` → stderr `-- more: add --cursor <c> to the same command`; a sample adds stderr `-- sampled N of <total_count>` only when N < total_count (a sample that returned everything needs no notice); an empty page prints `(empty)`. `--raw` is TSV of the same columns, no header.

#### `branch.list` — `strata branch list`

*read.page · Page<BranchItem, String> · stable*  
display: columns NAME  PARENT  STATUS  GENERATION · raw TSV

```text
$ strata branch list
human today         {"branch_id":"00000000-0000-0000-0000-000000000000","created_at":null,"deleted_at":null,"generation":1,"name":"default","parent":null,"state_revision":0,"status":"active"}
                    {"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"}
human proposed      NAME     PARENT  STATUS  GENERATION
                    default  -       active           1
                    feature  -       active           1
raw today           {"branch_id":"00000000-0000-0000-0000-000000000000","created_at":null,"deleted_at":null,"generation":1,"name":"default","parent":null,"state_revision":0,"status":"active"}
                    {"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"}
raw proposed        default⇥⇥active⇥1
                    feature⇥⇥active⇥1
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"branch_id":"00000000-0000-0000-0000-000000000000","created_at":null,"deleted_at":null,"generation":1,"name":"default","parent":null,"state_revision":0,"status":"active"},{"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"}]},"type":"branches"}
```

#### `event.list` — `strata event list`

*read.page · Page<EventVersionedData, u64> · stable*  
display: columns SEQUENCE  EVENT_TYPE  TIMESTAMP (date)  PAYLOAD (json) · raw TSV

```text
$ strata event list
human today         {"event":{"event_type":"user.created","hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665415378},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"2fcc37561b7fb5a4d7ebe2c368cd6329f642dd54bf01ca5a2abc924e4108e8f2","payload":{"id":2},"previous_hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","sequence":1,"timestamp":1789102665415572},"timestamp":4,"version":4}
human proposed      SEQUENCE  EVENT_TYPE    TIMESTAMP                PAYLOAD
                           0  user.created  2026-09-11 04:57:45 UTC  {"id":1}
                           1  user.updated  2026-09-11 04:57:45 UTC  {"id":2}
raw today           {"event":{"event_type":"user.created","hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665415378},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"2fcc37561b7fb5a4d7ebe2c368cd6329f642dd54bf01ca5a2abc924e4108e8f2","payload":{"id":2},"previous_hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","sequence":1,"timestamp":1789102665415572},"timestamp":4,"version":4}
raw proposed        0⇥user.created⇥1789102665415378⇥{"id":1}
                    1⇥user.updated⇥1789102665415572⇥{"id":2}
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"event":{"event_type":"user.created","hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665415378},"timestamp":3,"version":3},{"event":{"event_type":"user.updated","hash":"2fcc37561b7fb5a4d7ebe2c368cd6329f642dd54bf01ca5a2abc924e4108e8f2","payload":{"id":2},"previous_hash":"7351d9cdb673dcd824b9bbcb7e38e8fc92654cccfa0585045dfac74b6b416479","sequence":1,"timestamp":1789102665415572},"timestamp":4,"version":4}]},"type":"event_records"}
```

#### `event.range` — `strata event range`

*read.page · Page<EventVersionedData, u64> · stable*  
display: columns SEQUENCE  EVENT_TYPE  TIMESTAMP (date)  PAYLOAD (json) · raw TSV

```text
$ strata event range 0 --direction forward
human today         {"event":{"event_type":"user.created","hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665416164},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"f6104053e5d249a62bd2d7a5d99a7c1a02b999734d4badd2a5a820013cfc947d","payload":{"id":2},"previous_hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","sequence":1,"timestamp":1789102665416366},"timestamp":4,"version":4}
human proposed      SEQUENCE  EVENT_TYPE    TIMESTAMP                PAYLOAD
                           0  user.created  2026-09-11 04:57:45 UTC  {"id":1}
                           1  user.updated  2026-09-11 04:57:45 UTC  {"id":2}
raw today           {"event":{"event_type":"user.created","hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665416164},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"f6104053e5d249a62bd2d7a5d99a7c1a02b999734d4badd2a5a820013cfc947d","payload":{"id":2},"previous_hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","sequence":1,"timestamp":1789102665416366},"timestamp":4,"version":4}
raw proposed        0⇥user.created⇥1789102665416164⇥{"id":1}
                    1⇥user.updated⇥1789102665416366⇥{"id":2}
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"event":{"event_type":"user.created","hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665416164},"timestamp":3,"version":3},{"event":{"event_type":"user.updated","hash":"f6104053e5d249a62bd2d7a5d99a7c1a02b999734d4badd2a5a820013cfc947d","payload":{"id":2},"previous_hash":"3679f765ed10f2215ba12f279984d5bf052375f4c23dbb5de6cea0ec887b5904","sequence":1,"timestamp":1789102665416366},"timestamp":4,"version":4}]},"type":"event_range_result"}
```

#### `event.range_time` — `strata event range-time`

*read.page · Page<EventVersionedData, u64> · stable*  
display: columns SEQUENCE  EVENT_TYPE  TIMESTAMP (date)  PAYLOAD (json) · raw TSV

```text
$ strata event range-time 0 --direction forward
human today         {"event":{"event_type":"user.created","hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665417000},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"4ee58d36cdbacaaccfbb7f0f5f7149c6ddce1f40ae92528df5c316ec9d8908ec","payload":{"id":2},"previous_hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","sequence":1,"timestamp":1789102665417193},"timestamp":4,"version":4}
human proposed      SEQUENCE  EVENT_TYPE    TIMESTAMP                PAYLOAD
                           0  user.created  2026-09-11 04:57:45 UTC  {"id":1}
                           1  user.updated  2026-09-11 04:57:45 UTC  {"id":2}
raw today           {"event":{"event_type":"user.created","hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665417000},"timestamp":3,"version":3}
                    {"event":{"event_type":"user.updated","hash":"4ee58d36cdbacaaccfbb7f0f5f7149c6ddce1f40ae92528df5c316ec9d8908ec","payload":{"id":2},"previous_hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","sequence":1,"timestamp":1789102665417193},"timestamp":4,"version":4}
raw proposed        0⇥user.created⇥1789102665417000⇥{"id":1}
                    1⇥user.updated⇥1789102665417193⇥{"id":2}
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"event":{"event_type":"user.created","hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","payload":{"id":1},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","sequence":0,"timestamp":1789102665417000},"timestamp":3,"version":3},{"event":{"event_type":"user.updated","hash":"4ee58d36cdbacaaccfbb7f0f5f7149c6ddce1f40ae92528df5c316ec9d8908ec","payload":{"id":2},"previous_hash":"7152efbe03ba42b047bbb9fc5524093127f0f3674cdd2a33dac986d891d45deb","sequence":1,"timestamp":1789102665417193},"timestamp":4,"version":4}]},"type":"event_range_result"}
```

#### `event.types` — `strata event types`

*read.page · Page<String, String> · stable*  

```text
$ strata event types
human today         user.created
                    user.updated
human proposed      = unchanged
raw today           user.created
                    user.updated
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["user.created","user.updated"]},"type":"event_type_list"}
```

#### `graph.bindings` — `command run --command-json`

*read.page · Page<GraphBindingHit, String> · stable*  
display: columns GRAPH  NODE_ID  PRIMITIVE  SPACE  KEY  VERSION · raw TSV

```text
$ strata command run --command-json '{"target":{"key":"user:1","primitive":"kv","space":"default"},"type":"graph_bindings_for_entity"}'
human today         {"binding":{"target":{"key":"user:1","primitive":"kv","space":"default"}},"graph":"kb","node_id":"ada","timestamp":4,"version":4}
human proposed      GRAPH  NODE_ID  PRIMITIVE  SPACE    KEY     VERSION
                    kb     ada      kv         default  user:1        4
raw today           {"binding":{"target":{"key":"user:1","primitive":"kv","space":"default"}},"graph":"kb","node_id":"ada","timestamp":4,"version":4}
raw proposed        kb⇥ada⇥kv⇥default⇥user:1⇥4
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"binding":{"target":{"key":"user:1","primitive":"kv","space":"default"}},"graph":"kb","node_id":"ada","timestamp":4,"version":4}]},"type":"graph_binding_page"}
```

#### `graph.list` — `strata graph list`

*read.page · Page<String, String> · stable*  

```text
$ strata graph list
human today         social
human proposed      = unchanged
raw today           social
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["social"]},"type":"graph_name_page"}
```

#### `graph.neighbors` — `strata graph neighbors`

*read.page · Page<GraphNeighborHit, String> · stable*  
display: columns NODE_ID  DIRECTION  EDGE_TYPE  SRC  DST  WEIGHT (float) · raw TSV

```text
$ strata graph neighbors social alice --direction outgoing
human today         {"direction":"outgoing","dst":"bob","edge":{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","timestamp":6,"version":6,"weight":1.0},"edge_type":"knows","graph":"social","node":{"graph":"social","node_id":"bob","timestamp":5,"version":5},"node_id":"bob","src":"alice"}
human proposed      NODE_ID  DIRECTION  EDGE_TYPE  SRC    DST  WEIGHT
                    bob      outgoing   knows      alice  bob     1.0
raw today           {"direction":"outgoing","dst":"bob","edge":{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","timestamp":6,"version":6,"weight":1.0},"edge_type":"knows","graph":"social","node":{"graph":"social","node_id":"bob","timestamp":5,"version":5},"node_id":"bob","src":"alice"}
raw proposed        bob⇥outgoing⇥knows⇥alice⇥bob⇥1.0
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"direction":"outgoing","dst":"bob","edge":{"dst":"bob","edge_type":"knows","graph":"social","src":"alice","timestamp":6,"version":6,"weight":1.0},"edge_type":"knows","graph":"social","node":{"graph":"social","node_id":"bob","timestamp":5,"version":5},"node_id":"bob","src":"alice"}]},"type":"graph_neighbor_page"}
```

#### `graph.node.list` — `strata graph list-nodes`

*read.page · Page<GraphNodeDataOutput, String> · stable*  
display: columns NODE_ID  OBJECT_TYPE  VERSION · raw TSV

```text
$ strata graph list-nodes social
human today         {"graph":"social","node_id":"alice","timestamp":4,"version":4}
                    {"graph":"social","node_id":"bob","timestamp":5,"version":5}
human proposed      NODE_ID  OBJECT_TYPE  VERSION
                    alice    -                  4
                    bob      -                  5
raw today           {"graph":"social","node_id":"alice","timestamp":4,"version":4}
                    {"graph":"social","node_id":"bob","timestamp":5,"version":5}
raw proposed        alice⇥⇥4
                    bob⇥⇥5
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"graph":"social","node_id":"alice","timestamp":4,"version":4},{"graph":"social","node_id":"bob","timestamp":5,"version":5}]},"type":"graph_node_page"}
```

#### `graph.nodes_by_type` — `strata graph nodes-by-type`

*read.page · Page<GraphNodeDataOutput, String> · stable*  
display: columns NODE_ID  OBJECT_TYPE  VERSION · raw TSV

```text
$ strata graph nodes-by-type g person
human today         {"graph":"g","node_id":"a","object_type":"person","timestamp":4,"version":4}
                    {"graph":"g","node_id":"b","object_type":"person","timestamp":5,"version":5}
human proposed      NODE_ID  OBJECT_TYPE  VERSION
                    a        person             4
                    b        person             5
raw today           {"graph":"g","node_id":"a","object_type":"person","timestamp":4,"version":4}
                    {"graph":"g","node_id":"b","object_type":"person","timestamp":5,"version":5}
raw proposed        a⇥person⇥4
                    b⇥person⇥5
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"graph":"g","node_id":"a","object_type":"person","timestamp":4,"version":4},{"graph":"g","node_id":"b","object_type":"person","timestamp":5,"version":5}]},"type":"graph_node_page"}
```

#### `json.index.list` — `strata json index list`

*read.page · Page<JsonIndexDefinition, String> · stable*  
display: columns NAME  FIELD_PATH  INDEX_TYPE  SPACE  CREATED_VERSION · raw TSV

```text
$ strata json index list
human today         {"created_timestamp":3,"created_version":3,"field_path":"name","index_type":"tag","name":"by_name","space":"default"}
human proposed      NAME     FIELD_PATH  INDEX_TYPE  SPACE    CREATED_VERSION
                    by_name  name        tag         default                3
raw today           {"created_timestamp":3,"created_version":3,"field_path":"name","index_type":"tag","name":"by_name","space":"default"}
raw proposed        by_name⇥name⇥tag⇥default⇥3
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"created_timestamp":3,"created_version":3,"field_path":"name","index_type":"tag","name":"by_name","space":"default"}]},"type":"json_index_list"}
```

#### `json.list` — `strata json list`

*read.page · Page<String, String> · stable*  

```text
$ strata json list --prefix user:
human today         user:1
                    user:2
human proposed      = unchanged
raw today           user:1
                    user:2
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["user:1","user:2"]},"type":"json_list_result"}
```

#### `json.scan` — `strata json scan`

*read.page · Page<JsonSampleItem, String> · stable*  
display: columns KEY  VERSION  VALUE (json) · raw TSV

```text
$ strata json scan
human today         {"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3}
                    {"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4}
human proposed      KEY  VERSION  VALUE
                    a          3  {"v":1}
                    b          4  {"v":2}
raw today           {"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3}
                    {"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4}
raw proposed        a⇥3⇥{"v":1}
                    b⇥4⇥{"v":2}
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3},{"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4}]},"type":"json_scan_result"}
```

#### `kv.list` — `strata kv list`

*read.page · Page<Bytes, Bytes> · stable*  

```text
$ strata kv list --prefix user:
human today         user:1
                    user:2
human proposed      = unchanged
raw today           user:1
                    user:2
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["dXNlcjox","dXNlcjoy"]},"type":"keys_page"}
```
```text
$ strata kv list --limit 2
human today         user:1
                    user:2
                    -- more: add --cursor dXNlcjoy to the same command
human proposed      user:1
                    user:2
                    stderr› -- more: add --cursor dXNlcjoy to the same command
raw today           user:1
                    user:2
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":"dXNlcjoy","has_more":true,"items":["dXNlcjox","dXNlcjoy"]},"type":"keys_page"}
```
```text
$ strata kv list --prefix zzz
human today         (empty)
human proposed      = unchanged
raw today           (empty)
raw proposed        ∅ (prints nothing)
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[]},"type":"keys_page"}
```

#### `kv.scan` — `strata kv scan`

*read.page · Page<ScanItem, Bytes> · stable*  
display: columns KEY (bytes)  VERSION  VALUE (bytes) · raw TSV

```text
$ strata kv scan
human today         {"key":"a","timestamp":3,"value":"1","version":3}
                    {"key":"b","timestamp":4,"value":"2","version":4}
human proposed      KEY  VERSION  VALUE
                    a          3  1
                    b          4  2
raw today           {"key":"a","timestamp":3,"value":"1","version":3}
                    {"key":"b","timestamp":4,"value":"2","version":4}
raw proposed        a⇥3⇥1
                    b⇥4⇥2
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"key":"YQ==","timestamp":3,"value":"MQ==","version":3},{"key":"Yg==","timestamp":4,"value":"Mg==","version":4}]},"type":"kv_scan_result"}
```

#### `space.list` — `strata space list`

*read.page · Page<String, String> · stable*  

```text
$ strata space list
human today         app
                    default
human proposed      = unchanged
raw today           app
                    default
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["app","default"]},"type":"space_list"}
```

#### `vector.collection.list` — `strata vector collection list`

*read.page · Page<VectorCollectionInfo, String> · stable*  
display: columns NAME  DIMENSION  METRIC  COUNT  EMBEDDING_MODEL · raw TSV

```text
$ strata vector collection list
human today         {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
human proposed      NAME  DIMENSION  METRIC  COUNT  EMBEDDING_MODEL
                    docs          3  cosine      0  -
raw today           {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
raw proposed        docs⇥3⇥cosine⇥0⇥
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"count":0,"dimension":3,"metric":"cosine","name":"docs"}]},"type":"vector_collection_list"}
```

#### `vector.keys` — `strata vector keys`

*read.page · Page<String, String> · stable*  

```text
$ strata vector keys docs
human today         a
                    b
human proposed      = unchanged
raw today           a
                    b
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":["a","b"]},"type":"vector_key_page"}
```

#### `vector.scan` — `strata vector scan`

*read.page · Page<VectorVersionedData, String> · stable*  
display: columns KEY  VERSION  VECTOR_REVISION  EMBEDDING (json)  METADATA (json) · raw TSV

```text
$ strata vector scan docs
human today         {"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}
                    {"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}
human proposed      KEY  VERSION  VECTOR_REVISION  EMBEDDING      METADATA
                    a          4                1  [1.0,0.0,0.0]  -
                    b          5                1  [0.0,1.0,0.0]  -
raw today           {"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}
                    {"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}
raw proposed        a⇥4⇥1⇥[1.0,0.0,0.0]⇥
                    b⇥5⇥1⇥[0.0,1.0,0.0]⇥
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4},{"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}]},"type":"vector_scan_result"}
```

#### `graph.sample` — `strata graph sample`

*read.sample · SamplePage<GraphNodeDataOutput> · stable*  
display: columns NODE_ID  OBJECT_TYPE  VERSION · raw TSV

```text
$ strata graph sample g
human today         {"graph":"g","node_id":"a","timestamp":4,"version":4}
                    {"graph":"g","node_id":"b","timestamp":5,"version":5}
human proposed      NODE_ID  OBJECT_TYPE  VERSION
                    a        -                  4
                    b        -                  5
raw today           {"graph":"g","node_id":"a","timestamp":4,"version":4}
                    {"graph":"g","node_id":"b","timestamp":5,"version":5}
raw proposed        a⇥⇥4
                    b⇥⇥5
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"graph":"g","node_id":"a","timestamp":4,"version":4},{"graph":"g","node_id":"b","timestamp":5,"version":5}],"total_count":2},"type":"graph_sample_result"}
```

#### `json.sample` — `strata json sample`

*read.sample · SamplePage<JsonSampleItem> · stable*  
display: columns KEY  VERSION  VALUE (json) · raw TSV

```text
$ strata json sample
human today         {"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3}
                    {"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4}
                    {"document_version":1,"key":"c","timestamp":5,"value":{"v":3},"version":5}
human proposed      KEY  VERSION  VALUE
                    a          3  {"v":1}
                    b          4  {"v":2}
                    c          5  {"v":3}
raw today           {"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3}
                    {"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4}
                    {"document_version":1,"key":"c","timestamp":5,"value":{"v":3},"version":5}
raw proposed        a⇥3⇥{"v":1}
                    b⇥4⇥{"v":2}
                    c⇥5⇥{"v":3}
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"document_version":1,"key":"a","timestamp":3,"value":{"v":1},"version":3},{"document_version":1,"key":"b","timestamp":4,"value":{"v":2},"version":4},{"document_version":1,"key":"c","timestamp":5,"value":{"v":3},"version":5}],"total_count":3},"type":"json_sample_result"}
```

#### `kv.sample` — `strata kv sample`

*read.sample · SamplePage<SampleItem> · stable*  
display: columns KEY (bytes)  VERSION  VALUE (bytes) · raw TSV

```text
$ strata kv sample
human today         {"key":"a","timestamp":3,"value":"1","version":3}
                    {"key":"b","timestamp":4,"value":"2","version":4}
                    {"key":"c","timestamp":5,"value":"3","version":5}
human proposed      KEY  VERSION  VALUE
                    a          3  1
                    b          4  2
                    c          5  3
raw today           {"key":"a","timestamp":3,"value":"1","version":3}
                    {"key":"b","timestamp":4,"value":"2","version":4}
                    {"key":"c","timestamp":5,"value":"3","version":5}
raw proposed        a⇥3⇥1
                    b⇥4⇥2
                    c⇥5⇥3
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"key":"YQ==","timestamp":3,"value":"MQ==","version":3},{"key":"Yg==","timestamp":4,"value":"Mg==","version":4},{"key":"Yw==","timestamp":5,"value":"Mw==","version":5}],"total_count":3},"type":"sample_result"}
```

#### `vector.sample` — `strata vector sample`

*read.sample · SamplePage<VectorVersionedData> · stable*  
display: columns KEY  VERSION  VECTOR_REVISION  EMBEDDING (json)  METADATA (json) · raw TSV

```text
$ strata vector sample docs
human today         {"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}
                    {"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}
human proposed      KEY  VERSION  VECTOR_REVISION  EMBEDDING      METADATA
                    a          4                1  [1.0,0.0,0.0]  -
                    b          5                1  [0.0,1.0,0.0]  -
raw today           {"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}
                    {"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}
raw proposed        a⇥4⇥1⇥[1.0,0.0,0.0]⇥
                    b⇥5⇥1⇥[0.0,1.0,0.0]⇥
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4},{"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":5,"vector_revision":1,"version":5}],"total_count":2},"type":"vector_sample_result"}
```

#### `vector.query` — `strata vector query`

*read.search · SearchResult<VectorMatch> · stable*  
display: columns KEY  SCORE (float)  METADATA (json) · raw TSV (today's designed raw arm prints `key<TAB>score` only; METADATA is new in raw)

```text
$ strata vector query docs [1.0,0.0,0.0] --k 2
human today         a⇥1.0
                    b⇥0.0
human proposed      KEY  SCORE  METADATA
                    a      1.0  -
                    b      0.0  -
raw today           a⇥1.0
                    b⇥0.0
raw proposed        a⇥1.0⇥
                    b⇥0.0⇥
--json (unchanged)  {"data":[{"key":"a","score":1.0},{"key":"b","score":0.0}],"type":"vector_matches"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
failed_precondition.engine.embedding_model_missing: This vector collection records no embedding model, so text cannot be embedded for it. (err_…)
  hint: Declare the collection's model with `vector collection set-embedding-model <collection> <model>`, or pass a vector instead of text.
  ref: https://stratadb.org/e/failed_precondition.engine.embedding_model_missing
invalid_argument.executor.vector_input: A vector write or query supplied neither a vector nor a text, or both. (err_…)
  hint: Pass exactly one: an explicit vector, or a text to embed with the collection's recorded model.
  ref: https://stratadb.org/e/invalid_argument.executor.vector_input
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
```

#### `vector.index.query` — `command run --command-json`

*read.diagnostics · SearchResult<VectorMatch> + IndexDiagnostics · stable*  
display: matches table KEY SCORE METADATA, then `diagnostics` block · raw TSV of matches

```text
$ strata command run --command-json '{"collection":"docs","k":2,"query":[1.0,0.0,0.0],"type":"vector_index_query"}'
human today         a⇥1.0
                    b⇥0.0
human proposed      KEY  SCORE  METADATA
                    a      1.0  -
                    b      0.0  -
                    
                    diagnostics
                      active_delta_count            0
                      active_delta_seal_threshold   16
                      active_delta_source_count     0
                      artifact_sources              -
                      collection                    docs
                      collection_exact_threshold    64
                      derived                       0 B
                      exact_fallback_count          0
                      exact_source_count            1
                      filtered_underfill_fallback   true
                      flat_source_count             0
                      hnsw_graph_builds             0
                      hnsw_memory_budget            67.1 MB
                      hnsw_source_count             0
                      indexed_source_count          0
                      indexed_vector_count          0
                      last_query_fallback_reason    collection_below_exact_threshold
                      last_query_used_index         false
                      manifest_inherited_ref_count  0
                      manifest_owned_ref_count      0
                      manifest_ref_count            0
                      manifest_status               missing
                      overfetch_factor              4
                      policy_mode                   auto
                      resolved_index_kind_summary   exact
                      source_candidate_limit        18446744073709551615
                      source_flat_threshold         64
                      source_hnsw_threshold         18446744073709551615
raw today           a⇥1.0
                    b⇥0.0
raw proposed        a⇥1.0⇥
                    b⇥0.0⇥
--json (unchanged)  {"data":{"diagnostics":{"active_delta_count":0,"active_delta_seal_threshold":16,"active_delta_source_count":0,"artifact_sources":[],"collection":"docs","collection_exact_threshold":64,"derived_bytes":0,"exact_fallback_count":0,"exact_source_count":1,"filtered_underfill_fallback":true,"flat_source_count":0,"hnsw_graph_builds":0,"hnsw_memory_budget_bytes":67108864,"hnsw_source_count":0,"indexed_source_count":0,"indexed_vector_count":0,"last_query_fallback_reason":"collection_below_exact_threshold","last_query_used_index":false,"manifest_inherited_ref_count":0,"manifest_owned_ref_count":0,"manifest_ref_count":0,"manifest_status":"missing","overfetch_factor":4,"policy_mode":"auto","resolved_index_kind_summary":"exact","source_candidate_limit":18446744073709551615,"source_flat_threshold":64,"source_hnsw_threshold":18446744073709551615},"matches":[{"key":"a","score":1.0},{"key":"b","score":0.0}]},"type":"vector_index_query"}
```

### 6.5 Reads: graph analytics (6 commands)

`NODE` plus the declared value column, sorted so the answer reads top-down (rank descending, distance / depth ascending, otherwise by node). Input echoes (`graph`, `source`, `iterations`, `personalized`, `direction`) are not repeated — they are in `--json`. `bfs` adds stderr `-- truncated` when the traversal was cut. `--raw` is `node<TAB>value`.

#### `graph.analytics.bfs` — `strata graph bfs`

*read.analytics · AnalyticsResult<GraphBfsData> · stable*  
display: table NODE, DEPTH from `depths` sorted asc · raw TSV

```text
$ strata graph bfs g a
human today         {
                      "depths": {
                        "a": 0,
                        "b": 1,
                        "c": 2
                      },
                      "edges": [
                        {
                          "dst": "b",
                          "edge_type": "knows",
                          "src": "a",
                          "weight": 1.0
                        },
                        {
                          "dst": "c",
                          "edge_type": "knows",
                          "src": "b",
                          "weight": 1.0
                        }
                      ],
                      "graph": "g",
                      "start": "a",
                      "truncated": false,
                      "visited": [
                        "a",
                        "b",
                        "c"
                      ]
                    }
human proposed      NODE  DEPTH
                    a         0
                    b         1
                    c         2
raw today           {"depths":{"a":0,"b":1,"c":2},"edges":[{"dst":"b","edge_type":"knows","src":"a","weight":1.0},{"dst":"c","edge_type":"knows","src":"b","weight":1.0}],"graph":"g","start":"a","truncated":false,"visited":["a","b","c"]}
raw proposed        a⇥0
                    b⇥1
                    c⇥2
--json (unchanged)  {"data":{"depths":{"a":0,"b":1,"c":2},"edges":[{"dst":"b","edge_type":"knows","src":"a","weight":1.0},{"dst":"c","edge_type":"knows","src":"b","weight":1.0}],"graph":"g","start":"a","truncated":false,"visited":["a","b","c"]},"type":"graph_bfs_result"}
```

#### `graph.analytics.cdlp` — `strata graph cdlp`

*read.analytics · AnalyticsResult<GraphCdlpData> · stable*  
display: table NODE, LABEL from `labels` sorted node · raw TSV

```text
$ strata graph cdlp g
human today         {
                      "graph": "g",
                      "labels": {
                        "a": "a",
                        "b": "b",
                        "c": "a"
                      }
                    }
human proposed      NODE  LABEL
                    a     a
                    b     b
                    c     a
raw today           {"graph":"g","labels":{"a":"a","b":"b","c":"a"}}
raw proposed        a⇥a
                    b⇥b
                    c⇥a
--json (unchanged)  {"data":{"graph":"g","labels":{"a":"a","b":"b","c":"a"}},"type":"graph_cdlp_result"}
```

#### `graph.analytics.lcc` — `strata graph lcc`

*read.analytics · AnalyticsResult<GraphLccData> · stable*  
display: table NODE, COEFFICIENT from `coefficients` sorted node · raw TSV

```text
$ strata graph lcc g
human today         {
                      "coefficients": {
                        "a": 0.0,
                        "b": 0.0,
                        "c": 0.0
                      },
                      "graph": "g"
                    }
human proposed      NODE  COEFFICIENT
                    a             0.0
                    b             0.0
                    c             0.0
raw today           {"coefficients":{"a":0.0,"b":0.0,"c":0.0},"graph":"g"}
raw proposed        a⇥0.0
                    b⇥0.0
                    c⇥0.0
--json (unchanged)  {"data":{"coefficients":{"a":0.0,"b":0.0,"c":0.0},"graph":"g"},"type":"graph_lcc_result"}
```

#### `graph.analytics.pagerank` — `strata graph pagerank`

*read.analytics · AnalyticsResult<GraphPagerankData> · stable*  
display: table NODE, RANK from `ranks` sorted desc · raw TSV

```text
$ strata graph pagerank g
human today         {
                      "graph": "g",
                      "iterations": 20,
                      "personalized": false,
                      "ranks": {
                        "a": 0.18441687554671377,
                        "b": 0.3411710064820743,
                        "c": 0.4744121179712114
                      }
                    }
human proposed      NODE  RANK
                    c     0.474412
                    b     0.341171
                    a     0.184417
raw today           {"graph":"g","iterations":20,"personalized":false,"ranks":{"a":0.18441687554671377,"b":0.3411710064820743,"c":0.4744121179712114}}
raw proposed        c⇥0.4744121179712114
                    b⇥0.3411710064820743
                    a⇥0.18441687554671377
--json (unchanged)  {"data":{"graph":"g","iterations":20,"personalized":false,"ranks":{"a":0.18441687554671377,"b":0.3411710064820743,"c":0.4744121179712114}},"type":"graph_pagerank_result"}
```

#### `graph.analytics.sssp` — `strata graph sssp`

*read.analytics · AnalyticsResult<GraphSsspData> · stable*  
display: table NODE, DISTANCE from `distances` sorted asc · raw TSV

```text
$ strata graph sssp g a
human today         {
                      "direction": "outgoing",
                      "distances": {
                        "a": 0.0,
                        "b": 1.0,
                        "c": 2.0
                      },
                      "graph": "g",
                      "source": "a"
                    }
human proposed      NODE  DISTANCE
                    a          0.0
                    b          1.0
                    c          2.0
raw today           {"direction":"outgoing","distances":{"a":0.0,"b":1.0,"c":2.0},"graph":"g","source":"a"}
raw proposed        a⇥0.0
                    b⇥1.0
                    c⇥2.0
--json (unchanged)  {"data":{"direction":"outgoing","distances":{"a":0.0,"b":1.0,"c":2.0},"graph":"g","source":"a"},"type":"graph_sssp_result"}
```

#### `graph.analytics.wcc` — `strata graph wcc`

*read.analytics · AnalyticsResult<GraphWccData> · stable*  
display: table NODE, COMPONENT from `components` sorted node · raw TSV

```text
$ strata graph wcc g
human today         {
                      "component_count": 1,
                      "components": {
                        "a": "a",
                        "b": "a",
                        "c": "a"
                      },
                      "graph": "g"
                    }
human proposed      NODE  COMPONENT
                    a     a
                    b     a
                    c     a
raw today           {"component_count":1,"components":{"a":"a","b":"a","c":"a"},"graph":"g"}
raw proposed        a⇥a
                    b⇥a
                    c⇥a
--json (unchanged)  {"data":{"component_count":1,"components":{"a":"a","b":"a","c":"a"},"graph":"g"},"type":"graph_wcc_result"}
```

### 6.6 Status, summaries, actions (30 commands)

`field  value` lines — the family fixes the layout, the **command declares which facts appear** (`display.fields`, in that order); `--json` is the complete record. Labels are the wire field names, except an explicit `header:` where the wire name misstates the humanised value (`size_bytes` → `size  1 kB`; Q18 as relaxed on review). Nested objects indented, arrays of objects as an indented table, byte counts humanised (`6.7 GB`). Scalar statuses (`count`, `exists`) print the scalar. Actions (`arrow export/import`, `clone`, `ipc stop`) get a one-line receipt like writes. `describe` and `ping` keep their designed lines. `--raw` prints `key<TAB>value` lines of the same declared fields, wire names as keys, dotted for nesting (Q16). A diagnostic line on stderr only when a listing is truncated.

#### `branch.diff` — `strata branch diff`

*read.status · StatusResponse<BranchComparisonItem> · stable*  
display: branch_a/branch_b lines, then table SPACE CAPABILITY CHANGE IDENTITY VERSION · raw TSV

```text
$ strata branch diff default experiment
human today         {
                      "branch_a": "default",
                      "branch_b": "experiment",
                      "spaces": [
                        {
                          "added": [],
                          "capability": "kv",
                          "modified": [
                            {
                              "identity": "config",
                              "version": 8
                            }
                          ],
                          "removed": [],
                          "space": "default"
                        },
                        {
                          "added": [
                            {
                              "identity": "\u0000\u0005notesn1",
                              "version": 9
                            }
                          ],
                          "capability": "vector",
                          "modified": [],
                          "removed": [],
                          "space": "default"
                        }
                      ]
                    }
human proposed      branch_a  default
                    branch_b  experiment
                    
                    SPACE    CAPABILITY  CHANGE    IDENTITY             VERSION
                    default  kv          modified  config                     8
                    default  vector      added     base64:AAVub3Rlc24x        9
raw today           {"branch_a":"default","branch_b":"experiment","spaces":[{"added":[],"capability":"kv","modified":[{"identity":"config","version":8}],"removed":[],"space":"default"},{"added":[{"identity":"\u0000\u0005notesn1","version":9}],"capability":"vector","modified":[],"removed":[],"space":"default"}]}
raw proposed        default⇥kv⇥modified⇥config⇥8
                    default⇥vector⇥added⇥base64:AAVub3Rlc24x⇥9
--json (unchanged)  {"data":{"branch_a":"default","branch_b":"experiment","spaces":[{"added":[],"capability":"kv","modified":[{"identity":"Y29uZmln","version":8}],"removed":[],"space":"default"},{"added":[{"identity":"AAVub3Rlc24x","version":9}],"capability":"vector","modified":[],"removed":[],"space":"default"}]},"type":"branch_comparison"}
```

#### `branch.get` — `strata branch get`

*read.status · StatusResponse<BranchItem> · stable*  
display: fields name, parent, status, generation, created_at, deleted_at · raw key/value of the same fields

```text
$ strata branch get feature
human today         {
                      "branch_id": "dc42122c-83b7-5436-89bc-9ffa4299697c",
                      "created_at": 3,
                      "deleted_at": null,
                      "generation": 1,
                      "name": "feature",
                      "parent": null,
                      "state_revision": 0,
                      "status": "active"
                    }
human proposed      name        feature
                    parent      -
                    status      active
                    generation  1
                    created_at  3
                    deleted_at  -
raw today           {"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"}
raw proposed        name⇥feature
                    parent⇥
                    status⇥active
                    generation⇥1
                    created_at⇥3
                    deleted_at⇥
--json (unchanged)  {"data":{"branch_id":"dc42122c-83b7-5436-89bc-9ffa4299697c","created_at":3,"deleted_at":null,"generation":1,"name":"feature","parent":null,"state_revision":0,"status":"active"},"type":"branch"}
```

#### `branch.preview` — `strata branch preview`

*read.status · StatusResponse<BranchPreviewItem> · stable*  
display: key/value lines; conflicts as an indented table · raw key/value

```text
$ strata branch preview experiment default --strategy strict
human today         {
                      "branch_point": 3,
                      "capabilities_covered": [
                        "kv",
                        "json",
                        "vector"
                      ],
                      "capabilities_unsupported": [
                        "vector_collection",
                        "event",
                        "graph_metadata",
                        "graph_node",
                        "graph_edge",
                        "graph_ontology"
                      ],
                      "conflicts": [],
                      "derived_state": [],
                      "source": "experiment",
                      "spaces_covered": [
                        "default"
                      ],
                      "strategy": "strict",
                      "target": "default"
                    }
human proposed      source                    experiment
                    target                    default
                    strategy                  strict
                    branch_point              3
                    spaces_covered            default
                    capabilities_covered      kv json vector
                    capabilities_unsupported  vector_collection event graph_metadata graph_node graph_edge graph_ontology
                    conflicts                 -
                    derived_state             -
raw today           {"branch_point":3,"capabilities_covered":["kv","json","vector"],"capabilities_unsupported":["vector_collection","event","graph_metadata","graph_node","graph_edge","graph_ontology"],"conflicts":[],"derived_state":[],"source":"experiment","spaces_covered":["default"],"strategy":"strict","target":"default"}
raw proposed        branch_point⇥3
                    capabilities_covered⇥["kv","json","vector"]
                    capabilities_unsupported⇥["vector_collection","event","graph_metadata","graph_node","graph_edge","graph_ontology"]
                    conflicts⇥[]
                    derived_state⇥[]
                    source⇥experiment
                    spaces_covered⇥["default"]
                    strategy⇥strict
                    target⇥default
--json (unchanged)  {"data":{"branch_point":3,"capabilities_covered":["kv","json","vector"],"capabilities_unsupported":["vector_collection","event","graph_metadata","graph_node","graph_edge","graph_ontology"],"conflicts":[],"derived_state":[],"source":"experiment","spaces_covered":["default"],"strategy":"strict","target":"default"},"type":"branch_preview"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.engine.branch_point: The request contains invalid input. (err_…)
  hint: Correct the invalid field named by the error message and retry the operation.
  ref: https://stratadb.org/e/invalid_argument.engine.branch_point
```

#### `event.count` — `strata event count`

*read.status · StatusValue<u64> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata event count
human today         2
human proposed      = unchanged
raw today           2
raw proposed        = unchanged
--json (unchanged)  {"data":{"count":2},"type":"event_count"}
```

#### `event.exists` — `strata event exists`

*read.status · StatusValue<bool> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata event exists 0
human today         true
human proposed      = unchanged
raw today           true
raw proposed        = unchanged
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata event exists 999
human today         false
human proposed      = unchanged
raw today           false
raw proposed        = unchanged
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `event.verify_chain` — `strata event verify-chain`

*read.status · StatusValue<EventChainVerification> · stable*  
display: fields valid, length, first_invalid, error · raw key/value of the same fields

```text
$ strata event verify-chain
human today         {
                      "error": null,
                      "first_invalid": null,
                      "length": 1,
                      "valid": true
                    }
human proposed      valid          true
                    length         1
                    first_invalid  -
                    error          -
raw today           {"error":null,"first_invalid":null,"length":1,"valid":true}
raw proposed        valid⇥true
                    length⇥1
                    first_invalid⇥
                    error⇥
--json (unchanged)  {"data":{"error":null,"first_invalid":null,"length":1,"valid":true},"type":"event_chain_verification"}
```

#### `json.count` — `strata json count`

*read.status · StatusValue<u64> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata json count
human today         2
human proposed      = unchanged
raw today           2
raw proposed        = unchanged
--json (unchanged)  {"data":2,"type":"uint"}
```

#### `json.exists` — `strata json exists`

*read.status · StatusValue<bool> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata json exists user
human today         true
human proposed      = unchanged
raw today           true
raw proposed        = unchanged
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata json exists absent
human today         false
human proposed      = unchanged
raw today           false
raw proposed        = unchanged
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `kv.count` — `strata kv count`

*read.status · StatusValue<u64> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata kv count
human today         2
human proposed      = unchanged
raw today           2
raw proposed        = unchanged
--json (unchanged)  {"data":2,"type":"uint"}
```

#### `kv.exists` — `strata kv exists`

*read.status · StatusValue<bool> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata kv exists k
human today         true
human proposed      = unchanged
raw today           true
raw proposed        = unchanged
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata kv exists absent
human today         false
human proposed      = unchanged
raw today           false
raw proposed        = unchanged
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `space.exists` — `strata space exists`

*read.status · StatusValue<bool> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata space exists app
human today         true
human proposed      = unchanged
raw today           true
raw proposed        = unchanged
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata space exists nope
human today         false
human proposed      = unchanged
raw today           false
raw proposed        = unchanged
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `vector.collection.stats` — `strata vector collection stats`

*read.status · StatusResponse<VectorCollectionInfo> · transitional*  
display: columns NAME  DIMENSION  METRIC  COUNT  EMBEDDING_MODEL · raw TSV

```text
$ strata vector collection stats docs
human today         {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
human proposed      NAME  DIMENSION  METRIC  COUNT  EMBEDDING_MODEL
                    docs          3  cosine      0  -
raw today           {"count":0,"dimension":3,"metric":"cosine","name":"docs"}
raw proposed        docs⇥3⇥cosine⇥0⇥
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"count":0,"dimension":3,"metric":"cosine","name":"docs"}]},"type":"vector_collection_list"}
```

#### `vector.count` — `strata vector count`

*read.status · StatusValue<u64> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata vector count docs
human today         2
human proposed      = unchanged
raw today           2
raw proposed        = unchanged
--json (unchanged)  {"data":2,"type":"uint"}
```

#### `vector.exists` — `strata vector exists`

*read.status · StatusValue<bool> · stable*  
display: key/value lines (scalar → the scalar) · raw key/value lines

```text
$ strata vector exists docs a
human today         true
human proposed      = unchanged
raw today           true
raw proposed        = unchanged
--json (unchanged)  {"data":true,"type":"bool"}
```
```text
$ strata vector exists docs absent
human today         false
human proposed      = unchanged
raw today           false
raw proposed        = unchanged
--json (unchanged)  {"data":false,"type":"bool"}
```

#### `admin.config` — `strata config get`

*read.summary · StatusResponse<AdminConfig> · stable*  
display: fields target, durable, default_branch, created · raw key/value of the same fields

```text
$ strata config get
human today         {
                      "created": true,
                      "default_branch": "default",
                      "durable": false,
                      "target": "cache"
                    }
human proposed      target          cache
                    durable         false
                    default_branch  default
                    created         true
raw today           {"created":true,"default_branch":"default","durable":false,"target":"cache"}
raw proposed        target⇥cache
                    durable⇥false
                    default_branch⇥default
                    created⇥true
--json (unchanged)  {"data":{"created":true,"default_branch":"default","durable":false,"target":"cache"},"type":"config"}
```

#### `admin.describe` — `strata describe`

*read.summary · StatusResponse<AdminDescribe> · stable*  
display: designed arm — unchanged

```text
$ strata describe
human today         StrataDB 1.2.1 · cache
                    branch default · branches: default · spaces: default
                    capabilities: arrow event graph_core inference json kv vector vector_index
                    kv 0 · json 0 · events 0
human proposed      = unchanged
raw today           {"branch":"default","branches":["default"],"capabilities":{"arrow":true,"event":true,"graph_core":true,"inference":true,"json":true,"kv":true,"vector":true,"vector_index":true},"config":{"created":true,"default_branch":"default","durable":false,"target":"cache"},"default_branch":"default","primitives":{"event_count":0,"graphs":[],"json_count":0,"kv_count":0,"vector_collections":[]},"spaces":["default"],"target":"cache","version":"1.2.1"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"branch":"default","branches":["default"],"capabilities":{"arrow":true,"event":true,"graph_core":true,"inference":true,"json":true,"kv":true,"vector":true,"vector_index":true},"config":{"created":true,"default_branch":"default","durable":false,"target":"cache"},"default_branch":"default","primitives":{"event_count":0,"graphs":[],"json_count":0,"kv_count":0,"vector_collections":[]},"spaces":["default"],"target":"cache","version":"1.2.1"},"type":"described"}
```

#### `admin.health` — `strata health`

*read.summary · StatusResponse<AdminHealth> · stable*  
display: fields status, identity, registry, branch_catalog, space_catalog, default_branch, branch_count · raw key/value of the same fields

```text
$ strata health
human today         {
                      "branch_catalog": "healthy",
                      "branch_count": 1,
                      "default_branch": "default",
                      "identity": "healthy",
                      "registry": "healthy",
                      "space_catalog": "healthy",
                      "status": "healthy"
                    }
human proposed      status          healthy
                    identity        healthy
                    registry        healthy
                    branch_catalog  healthy
                    space_catalog   healthy
                    default_branch  default
                    branch_count    1
raw today           {"branch_catalog":"healthy","branch_count":1,"default_branch":"default","identity":"healthy","registry":"healthy","space_catalog":"healthy","status":"healthy"}
raw proposed        status⇥healthy
                    identity⇥healthy
                    registry⇥healthy
                    branch_catalog⇥healthy
                    space_catalog⇥healthy
                    default_branch⇥default
                    branch_count⇥1
--json (unchanged)  {"data":{"branch_catalog":"healthy","branch_count":1,"default_branch":"default","identity":"healthy","registry":"healthy","space_catalog":"healthy","status":"healthy"},"type":"health"}
```

#### `admin.info` — `strata info`

*read.summary · StatusResponse<AdminDatabaseInfo> · stable*  
display: fields target, version, durable, default_branch, branch_count, space_count, memory_budget · raw key/value of the same fields

```text
$ strata info
human today         {
                      "branch_count": 1,
                      "created": true,
                      "default_branch": "default",
                      "durable": false,
                      "memory_budget": {
                        "source": "derived_from_host",
                        "total_bytes": 6661720064,
                        "usable_host_bytes": 26646880256
                      },
                      "open": true,
                      "space_count": 1,
                      "target": "cache",
                      "version": "1.2.1"
                    }
human proposed      target          cache
                    version         1.2.1
                    durable         false
                    default_branch  default
                    branch_count    1
                    space_count     1
                    memory_budget
                      source       derived_from_host
                      total        6.7 GB
                      usable_host  26.6 GB
raw today           {"branch_count":1,"created":true,"default_branch":"default","durable":false,"memory_budget":{"source":"derived_from_host","total_bytes":6661720064,"usable_host_bytes":26646880256},"open":true,"space_count":1,"target":"cache","version":"1.2.1"}
raw proposed        target⇥cache
                    version⇥1.2.1
                    durable⇥false
                    default_branch⇥default
                    branch_count⇥1
                    space_count⇥1
                    memory_budget.source⇥derived_from_host
                    memory_budget.total_bytes⇥6661720064
                    memory_budget.usable_host_bytes⇥26646880256
--json (unchanged)  {"data":{"branch_count":1,"created":true,"default_branch":"default","durable":false,"memory_budget":{"source":"derived_from_host","total_bytes":6661720064,"usable_host_bytes":26646880256},"open":true,"space_count":1,"target":"cache","version":"1.2.1"},"type":"database_info"}
```

#### `admin.ipc_status` — `strata ipc status`

*read.summary · StatusResponse<AdminIpcStatus> · stable*  
display: fields hosting, is_owner, client_count · raw key/value of the same fields

```text
$ strata ipc status
human today         {
                      "client_count": 0,
                      "hosting": false,
                      "is_owner": true
                    }
human proposed      hosting       false
                    is_owner      true
                    client_count  0
raw today           {"client_count":0,"hosting":false,"is_owner":true}
raw proposed        hosting⇥false
                    is_owner⇥true
                    client_count⇥0
--json (unchanged)  {"data":{"client_count":0,"hosting":false,"is_owner":true},"type":"ipc_status"}
```

#### `admin.metrics` — `strata metrics`

*read.summary · StatusResponse<AdminMetrics> · stable*  
display: fields control_status, target, durable, branch_count, space_count · raw key/value of the same fields

```text
$ strata metrics
human today         {
                      "branch_count": 1,
                      "control_status": "healthy",
                      "durable": false,
                      "open": true,
                      "space_count": 1,
                      "target": "cache"
                    }
human proposed      control_status  healthy
                    target          cache
                    durable         false
                    branch_count    1
                    space_count     1
raw today           {"branch_count":1,"control_status":"healthy","durable":false,"open":true,"space_count":1,"target":"cache"}
raw proposed        control_status⇥healthy
                    target⇥cache
                    durable⇥false
                    branch_count⇥1
                    space_count⇥1
--json (unchanged)  {"data":{"branch_count":1,"control_status":"healthy","durable":false,"open":true,"space_count":1,"target":"cache"},"type":"metrics"}
```

#### `admin.ping` — `strata ping`

*read.summary · StatusResponse<PingInfo> · stable*  
display: designed arm — unchanged

```text
$ strata ping
human today         pong 1.2.1
human proposed      = unchanged
raw today           {"version":"1.2.1"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"version":"1.2.1"},"type":"pong"}
```

#### `hub.get_dataset` — `strata hub get-dataset`

*read.summary · StatusResponse<HubDatasetCard> · stable*  
display: fields name, owner, badge, description, license, primitives, tags, tasks, size_bytes (header size), downloads, default_branch, created, last_updated, engine_version_required, clone_command · raw key/value of the same fields

```text
(fixture responses/v1/hub/dataset.json)
human today         {
                      "badge": "official",
                      "capability_registry_version": 1,
                      "citation": "Fixture citation.",
                      "clone_command": "strata clone titanic",
                      "created": "2026-09-01T00:00:00Z",
                      "default_branch": "main",
                      "description": "Classic passenger-survival dataset.",
                      "downloads": 7,
                      "engine_version_required": ">=1.1.0",
                      "format_version": "v1",
                      "frontmatter_extras": {
                        "source": "fixture"
                      },
                      "last_updated": "2026-09-02T00:00:00Z",
                      "license": "CC0",
                      "manifest_hash": "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                      "name": "titanic",
                      "owner": "stratahub",
                      "primitives": [
                        "kv"
                      ],
                      "provenance": {
                        "curator": "stratahub",
                        "license_text_url": "https://example.test/license",
                        "source": "fixture"
                      },
                      "quick_start_snippets": {
                        "python": "print('titanic')"
                      },
                      "readme": "# Titanic\n",
                      "sample_preview": {
                        "kv": [
                          {
                            "key": "passenger:1",
                            "value_summary": "survived=true"
                          }
                        ]
                      },
                      "schema": {
                        "kv": {
                          "namespaces": [
                            {
                              "entry_count": 1,
                              "prefix": "passenger:",
                              "value_type": "json"
                            }
                          ]
                        }
                      },
                      "size_bytes": 1024,
                      "strata_features": {
                        "branches": [
                          {
                            "is_default": true,
                            "name": "main"
                          }
                        ],
                        "example_notebook": "examples/titanic.ipynb",
                        "multi_primitive_demos": [],
                        "time_travel_highlights": []
                      },
                      "summary_excerpt": "Classic passenger-survival dataset.",
                      "tags": [
                        "tabular"
                      ],
                      "tasks": [
                        "classification"
                      ]
                    }
human proposed      name                     titanic
                    owner                    stratahub
                    badge                    official
                    description              Classic passenger-survival dataset.
                    license                  CC0
                    primitives               kv
                    tags                     tabular
                    tasks                    classification
                    size                     1 kB
                    downloads                7
                    default_branch           main
                    created                  2026-09-01T00:00:00Z
                    last_updated             2026-09-02T00:00:00Z
                    engine_version_required  >=1.1.0
                    clone_command            strata clone titanic
raw today           {"badge":"official","capability_registry_version":1,"citation":"Fixture citation.","clone_command":"strata clone titanic","created":"2026-09-01T00:00:00Z","default_branch":"main","description":"Classic passenger-survival dataset.","downloads":7,"engine_version_required":">=1.1.0","format_version":"v1","frontmatter_extras":{"source":"fixture"},"last_updated":"2026-09-02T00:00:00Z","license":"CC0","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","name":"titanic","owner":"stratahub","primitives":["kv"],"provenance":{"curator":"stratahub","license_text_url":"https://example.test/license","source":"fixture"},"quick_start_snippets":{"python":"print('titanic')"},"readme":"# Titanic\n","sample_preview":{"kv":[{"key":"passenger:1","value_summary":"survived=true"}]},"schema":{"kv":{"namespaces":[{"entry_count":1,"prefix":"passenger:","value_type":"json"}]}},"size_bytes":1024,"strata_features":{"branches":[{"is_default":true,"name":"main"}],"example_notebook":"examples/titanic.ipynb","multi_primitive_demos":[],"time_travel_highlights":[]},"summary_excerpt":"Classic passenger-survival dataset.","tags":["tabular"],"tasks":["classification"]}
raw proposed        name⇥titanic
                    owner⇥stratahub
                    badge⇥official
                    description⇥Classic passenger-survival dataset.
                    license⇥CC0
                    primitives⇥["kv"]
                    tags⇥["tabular"]
                    tasks⇥["classification"]
                    size_bytes⇥1024
                    downloads⇥7
                    default_branch⇥main
                    created⇥2026-09-01T00:00:00Z
                    last_updated⇥2026-09-02T00:00:00Z
                    engine_version_required⇥>=1.1.0
                    clone_command⇥strata clone titanic
--json (unchanged)  {"data":{"badge":"official","capability_registry_version":1,"citation":"Fixture citation.","clone_command":"strata clone titanic","created":"2026-09-01T00:00:00Z","default_branch":"main","description":"Classic passenger-survival dataset.","downloads":7,"engine_version_required":">=1.1.0","format_version":"v1","frontmatter_extras":{"source":"fixture"},"last_updated":"2026-09-02T00:00:00Z","license":"CC0","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","name":"titanic","owner":"stratahub","primitives":["kv"],"provenance":{"curator":"stratahub","license_text_url":"https://example.test/license","source":"fixture"},"quick_start_snippets":{"python":"print('titanic')"},"readme":"# Titanic\n","sample_preview":{"kv":[{"key":"passenger:1","value_summary":"survived=true"}]},"schema":{"kv":{"namespaces":[{"entry_count":1,"prefix":"passenger:","value_type":"json"}]}},"size_bytes":1024,"strata_features":{"branches":[{"is_default":true,"name":"main"}],"example_notebook":"examples/titanic.ipynb","multi_primitive_demos":[],"time_travel_highlights":[]},"summary_excerpt":"Classic passenger-survival dataset.","tags":["tabular"],"tasks":["classification"]},"type":"hub_dataset"}
```

#### `hub.info` — `strata hub info`

*read.summary · StatusResponse<HubInfo> · stable*  
display: fields server_implementation, server_version, protocol_version, hash_algorithm, max_dataset_size_bytes (header max_dataset_size), max_object_size_bytes (header max_object_size), max_manifest_size_bytes (header max_manifest_size), supported_object_content_types, telemetry_endpoint_enabled · raw key/value of the same fields

```text
(fixture responses/v1/hub/info.json)
human today         {
                      "hash_algorithm": "blake3",
                      "max_dataset_size_bytes": 5368709120,
                      "max_manifest_size_bytes": 1048576,
                      "max_object_size_bytes": 536870912,
                      "protocol_version": "v1",
                      "server_implementation": "stratahub",
                      "server_version": "0.1.0",
                      "supported_object_content_types": [
                        "application/octet-stream"
                      ],
                      "telemetry_endpoint_enabled": false
                    }
human proposed      server_implementation           stratahub
                    server_version                  0.1.0
                    protocol_version                v1
                    hash_algorithm                  blake3
                    max_dataset_size                5.4 GB
                    max_object_size                 536.9 MB
                    max_manifest_size               1 MB
                    supported_object_content_types  application/octet-stream
                    telemetry_endpoint_enabled      false
raw today           {"hash_algorithm":"blake3","max_dataset_size_bytes":5368709120,"max_manifest_size_bytes":1048576,"max_object_size_bytes":536870912,"protocol_version":"v1","server_implementation":"stratahub","server_version":"0.1.0","supported_object_content_types":["application/octet-stream"],"telemetry_endpoint_enabled":false}
raw proposed        server_implementation⇥stratahub
                    server_version⇥0.1.0
                    protocol_version⇥v1
                    hash_algorithm⇥blake3
                    max_dataset_size_bytes⇥5368709120
                    max_object_size_bytes⇥536870912
                    max_manifest_size_bytes⇥1048576
                    supported_object_content_types⇥["application/octet-stream"]
                    telemetry_endpoint_enabled⇥false
--json (unchanged)  {"data":{"hash_algorithm":"blake3","max_dataset_size_bytes":5368709120,"max_manifest_size_bytes":1048576,"max_object_size_bytes":536870912,"protocol_version":"v1","server_implementation":"stratahub","server_version":"0.1.0","supported_object_content_types":["application/octet-stream"],"telemetry_endpoint_enabled":false},"type":"hub_info"}
```

#### `hub.list_datasets` — `strata hub list-datasets`

*read.summary · StatusResponse<HubDatasetPage> · stable*  
display: columns NAME  BADGE  LICENSE  PRIMITIVES (list)  SIZE (size)  DOWNLOADS  LAST_UPDATED · raw TSV

```text
(fixture responses/v1/hub/datasets.json)
human today         {"badge":"official","default_branch":"main","description":"Classic passenger-survival dataset.","downloads":7,"last_updated":"2026-09-02T00:00:00Z","license":"CC0","name":"titanic","primitives":["kv"],"size_bytes":1024,"tags":["tabular"],"tasks":["classification"]}
human proposed      NAME     BADGE     LICENSE  PRIMITIVES  SIZE  DOWNLOADS  LAST_UPDATED
                    titanic  official  CC0      kv          1 kB          7  2026-09-02T00:00:00Z
raw today           {"badge":"official","default_branch":"main","description":"Classic passenger-survival dataset.","downloads":7,"last_updated":"2026-09-02T00:00:00Z","license":"CC0","name":"titanic","primitives":["kv"],"size_bytes":1024,"tags":["tabular"],"tasks":["classification"]}
raw proposed        titanic⇥official⇥CC0⇥["kv"]⇥1024⇥7⇥2026-09-02T00:00:00Z
--json (unchanged)  {"data":{"items":[{"badge":"official","default_branch":"main","description":"Classic passenger-survival dataset.","downloads":7,"last_updated":"2026-09-02T00:00:00Z","license":"CC0","name":"titanic","primitives":["kv"],"size_bytes":1024,"tags":["tabular"],"tasks":["classification"]}],"limit":20,"offset":0,"total":1},"type":"hub_datasets"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.executor.hub_filter: The hub browse filter or pagination request is invalid. (err_…)
  hint: Use StrataHub V1 filter names and keep list limits in the range 1..=200.
  ref: https://stratadb.org/e/invalid_argument.executor.hub_filter
```

#### `hub.list_refs` — `strata hub list-refs`

*read.summary · StatusResponse<HubRefList> · stable*  
display: columns BRANCH  LAST_UPDATED  MANIFEST_HASH · raw TSV

```text
(fixture responses/v1/hub/refs.json)
human today         {
                      "dataset": "titanic",
                      "default_branch": "main",
                      "refs": [
                        {
                          "branch": "main",
                          "last_updated": "2026-09-02T00:00:00Z",
                          "manifest_hash": "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        }
                      ]
                    }
human proposed      dataset         titanic
                    default_branch  main
                    
                    BRANCH  LAST_UPDATED          MANIFEST_HASH
                    main    2026-09-02T00:00:00Z  blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
raw today           {"dataset":"titanic","default_branch":"main","refs":[{"branch":"main","last_updated":"2026-09-02T00:00:00Z","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]}
raw proposed        main⇥2026-09-02T00:00:00Z⇥blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
--json (unchanged)  {"data":{"dataset":"titanic","default_branch":"main","refs":[{"branch":"main","last_updated":"2026-09-02T00:00:00Z","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]},"type":"hub_refs"}
```

#### `hub.list_yanked` — `strata hub list-yanked`

*read.summary · StatusResponse<HubYankedList> · stable*  
display: columns DATASET  BRANCH  REASON  YANKED_AT  MANIFEST_HASH · raw TSV

```text
(fixture responses/v1/hub/yanked.json)
human today         {"branch":"main","dataset":"bad-dataset","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","reason":"policy_violation","yanked_at":"2026-09-02T00:00:00Z"}
human proposed      DATASET      BRANCH  REASON            YANKED_AT             MANIFEST_HASH
                    bad-dataset  main    policy_violation  2026-09-02T00:00:00Z  blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
raw today           {"branch":"main","dataset":"bad-dataset","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","reason":"policy_violation","yanked_at":"2026-09-02T00:00:00Z"}
raw proposed        bad-dataset⇥main⇥policy_violation⇥2026-09-02T00:00:00Z⇥blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
--json (unchanged)  {"data":{"generated_at":"2026-09-02T00:00:00Z","items":[{"branch":"main","dataset":"bad-dataset","manifest_hash":"blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","reason":"policy_violation","yanked_at":"2026-09-02T00:00:00Z"}],"total":1},"type":"hub_yanked"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.executor.hub_since: The hub yanked-list timestamp is invalid. (err_…)
  hint: Pass --since as an RFC 3339 timestamp such as 2026-09-02T00:00:00Z.
  ref: https://stratadb.org/e/invalid_argument.executor.hub_since
```

#### `admin.hub_clone` — `strata clone`

*action.status · StatusResponse<HubClone> · stable*  
display: receipt `cloned {dataset}@{branch} into {dest} ({object_count|plural:object}, {total_bytes|size})` · raw key/value lines

```text
(fixture responses/v1/admin/hub_clone.json)
human today         {
                      "branch": "default",
                      "dataset": "strata/titanic",
                      "dest": "./titanic.strata",
                      "manifest_hash": "blake3:8ac589d2e4965c755758414a8f952e1b0f2a01cefea8d6841d1ce8246fc41175",
                      "object_count": 12,
                      "total_bytes": 40960
                    }
human proposed      cloned strata/titanic@default into ./titanic.strata (12 objects, 41 kB)
raw today           {"branch":"default","dataset":"strata/titanic","dest":"./titanic.strata","manifest_hash":"blake3:8ac589d2e4965c755758414a8f952e1b0f2a01cefea8d6841d1ce8246fc41175","object_count":12,"total_bytes":40960}
raw proposed        branch⇥default
                    dataset⇥strata/titanic
                    dest⇥./titanic.strata
                    manifest_hash⇥blake3:8ac589d2e4965c755758414a8f952e1b0f2a01cefea8d6841d1ce8246fc41175
                    object_count⇥12
                    total_bytes⇥40960
--json (unchanged)  {"data":{"branch":"default","dataset":"strata/titanic","dest":"./titanic.strata","manifest_hash":"blake3:8ac589d2e4965c755758414a8f952e1b0f2a01cefea8d6841d1ce8246fc41175","object_count":12,"total_bytes":40960},"type":"hub_clone_result"}
```

#### `admin.ipc_stop` — `strata ipc stop`

*action.status · StatusResponse<AdminIpcStop> · stable*  
display: receipt `stopped ipc host` / `no ipc host was running` · raw key/value lines

```text
$ strata ipc stop
human today         {
                      "stopped": false
                    }
human proposed      no ipc host was running
raw today           {"stopped":false}
raw proposed        stopped⇥false
--json (unchanged)  {"data":{"stopped":false},"type":"ipc_stop"}
```

#### `arrow.export` — `strata arrow export`

*action.status · StatusResponse<ArrowExport> · stable*  
display: receipt `exported {row_count|plural:row} of {primitive} to {paths.0} ({size_bytes|size})` · raw key/value lines

```text
(fixture responses/v1/arrow/export_kv_csv.json)
human today         {
                      "format": "csv",
                      "paths": [
                        "kv_out.csv"
                      ],
                      "primitive": "kv",
                      "row_count": 3,
                      "size_bytes": 135
                    }
human proposed      exported 3 rows of kv to kv_out.csv (135 B)
raw today           {"format":"csv","paths":["kv_out.csv"],"primitive":"kv","row_count":3,"size_bytes":135}
raw proposed        format⇥csv
                    paths⇥["kv_out.csv"]
                    primitive⇥kv
                    row_count⇥3
                    size_bytes⇥135
--json (unchanged)  {"data":{"format":"csv","paths":["kv_out.csv"],"primitive":"kv","row_count":3,"size_bytes":135},"type":"arrow_export_result"}
```

#### `arrow.import` — `strata arrow import`

*action.status · StatusResponse<ArrowImport> · stable*  
display: receipt `imported {rows_imported|plural:row} into {target} from {file_path} ({rows_skipped} skipped)` · raw key/value lines

```text
$ strata arrow import /tmp/exports/kv.parquet --target kv
human today         {
                      "batches_processed": 1,
                      "file_path": "/tmp/.tmpaH350d/kv.parquet",
                      "rows_imported": 1,
                      "rows_skipped": 0,
                      "target": "kv"
                    }
human proposed      imported 1 row into kv from /tmp/.tmpaH350d/kv.parquet (0 skipped)
raw today           {"batches_processed":1,"file_path":"/tmp/.tmpaH350d/kv.parquet","rows_imported":1,"rows_skipped":0,"target":"kv"}
raw proposed        batches_processed⇥1
                    file_path⇥/tmp/.tmpaH350d/kv.parquet
                    rows_imported⇥1
                    rows_skipped⇥0
                    target⇥kv
--json (unchanged)  {"data":{"batches_processed":1,"file_path":"/tmp/.tmpaH350d/kv.parquet","rows_imported":1,"rows_skipped":0,"target":"kv"},"type":"arrow_import_result"}
```

### 6.7 Batches (14 commands)

A table with one row per item: `#  STATUS  [EFFECT]  <declared columns>` (+ `ERROR` when any item failed). A stderr summary `-- <mode>: N ok, M miss/failed` **only when an item missed or failed** — full success is the table alone (revised on review 2026-09-11). `--raw` is the same rows as TSV. Batch commands have no CLI verb today (`command run --command-json` only).

#### `event.batch_append` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<EventBatchAppendItem> · stable*  
display: table # STATUS EFFECT EVENT_TYPE SEQUENCE · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"event_type":"user.created","payload":{"id":1}},{"event_type":"user.updated","payload":{"id":2}}],"type":"event_batch_append"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.412845 +00:00","delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"event_type":"user.created","sequence":0},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.412845 +00:00","delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"event_type":"user.updated","sequence":1},"status":"ok"}
human proposed      #  STATUS  EFFECT   EVENT_TYPE    SEQUENCE
                    0  ok      created  user.created         0
                    1  ok      created  user.updated         1
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.412845 +00:00","delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"event_type":"user.created","sequence":0},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.412845 +00:00","delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"event_type":"user.updated","sequence":1},"status":"ok"}
raw proposed        0⇥ok⇥created⇥user.created⇥0
                    1⇥ok⇥created⇥user.updated⇥1
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665412845,"delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"items":[{"applied":true,"commit":{"committed_at":1789102665412845,"delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"event_type":"user.created","sequence":0},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665412845,"delete_count":0,"durability":"not_durable","put_count":5,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"event_type":"user.updated","sequence":1},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"event_batch_append_results"}
```

#### `json.batch_delete` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<JsonMutationItem> · stable*  
display: table # STATUS EFFECT DOCUMENT_VERSION · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}}],"type":"json_batch_set"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.447598 +00:00","delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"}
human proposed      #  STATUS  EFFECT   DOCUMENT_VERSION
                    0  ok      created                 1
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.447598 +00:00","delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"}
raw proposed        0⇥ok⇥created⇥1
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665447598,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"items":[{"applied":true,"commit":{"committed_at":1789102665447598,"delete_count":0,"durability":"not_durable","put_count":1,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"json_batch_results"}
```
```text
$ strata command run --command-json '{"entries":[{"key":"a","path":"$"}],"type":"json_batch_delete"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.447782 +00:00","delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"document_version":null},"status":"ok"}
human proposed      #  STATUS  EFFECT   DOCUMENT_VERSION
                    0  ok      deleted                 -
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.447782 +00:00","delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"document_version":null},"status":"ok"}
raw proposed        0⇥ok⇥deleted⇥
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665447782,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"items":[{"applied":true,"commit":{"committed_at":1789102665447782,"delete_count":1,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"document_version":null},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"json_batch_results"}
```

#### `json.batch_set` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<JsonMutationItem> · stable*  
display: table # STATUS EFFECT DOCUMENT_VERSION · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"key":"a","path":"$","value":{"v":1}},{"key":"b","path":"$","value":{"v":2}}],"type":"json_batch_set"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.449563 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.449563 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"document_version":1},"status":"ok"}
human proposed      #  STATUS  EFFECT   DOCUMENT_VERSION
                    0  ok      created                 1
                    1  ok      created                 1
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.449563 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.449563 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"document_version":1},"status":"ok"}
raw proposed        0⇥ok⇥created⇥1
                    1⇥ok⇥created⇥1
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665449563,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"items":[{"applied":true,"commit":{"committed_at":1789102665449563,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"document_version":1},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665449563,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"document_version":1},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"json_batch_results"}
```

#### `kv.batch_delete` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<KvMutationItem> · stable*  
display: table # STATUS EFFECT KEY · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457113 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457113 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
human proposed      #  STATUS  EFFECT   KEY
                    0  ok      created  a
                    1  ok      created  b
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457113 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457113 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
raw proposed        0⇥ok⇥created⇥a
                    1⇥ok⇥created⇥b
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665457113,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"items":[{"applied":true,"commit":{"committed_at":1789102665457113,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665457113,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"batch_results"}
```
```text
$ strata command run --command-json '{"keys":["YQ==","Yg=="],"type":"kv_batch_delete"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457278 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457278 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
human proposed      #  STATUS  EFFECT   KEY
                    0  ok      deleted  a
                    1  ok      deleted  b
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457278 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.457278 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
raw proposed        0⇥ok⇥deleted⇥a
                    1⇥ok⇥deleted⇥b
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665457278,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"items":[{"applied":true,"commit":{"committed_at":1789102665457278,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665457278,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"batch_results"}
```

#### `kv.batch_put` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<KvMutationItem> · stable*  
display: table # STATUS EFFECT KEY · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"key":"YQ==","value":"MQ=="},{"key":"Yg==","value":"Mg=="}],"type":"kv_batch_put"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.458877 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.458877 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
human proposed      #  STATUS  EFFECT   KEY
                    0  ok      created  a
                    1  ok      created  b
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.458877 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.458877 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}
raw proposed        0⇥ok⇥created⇥a
                    1⇥ok⇥created⇥b
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665458877,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"items":[{"applied":true,"commit":{"committed_at":1789102665458877,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"key":"YQ=="},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665458877,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":3,"version":3},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"key":"Yg=="},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"batch_results"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
invalid_argument.executor.kv_batch_duplicate_key: The KV batch contains duplicate keys. (err_…)
  hint: Remove duplicate keys so each KV batch item targets a unique key.
  ref: https://stratadb.org/e/invalid_argument.executor.kv_batch_duplicate_key
```

#### `vector.batch_delete` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<VectorMutationItem> · stable*  
display: table # STATUS EFFECT VECTOR_REVISION · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"collection":"docs","keys":["a","b"],"type":"vector_batch_delete"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.466851 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"vector_revision":null},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.466851 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"vector_revision":null},"status":"ok"}
human proposed      #  STATUS  EFFECT   VECTOR_REVISION
                    0  ok      deleted                -
                    1  ok      deleted                -
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.466851 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"vector_revision":null},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.466851 +00:00","delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"vector_revision":null},"status":"ok"}
raw proposed        0⇥ok⇥deleted⇥
                    1⇥ok⇥deleted⇥
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665466851,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"items":[{"applied":true,"commit":{"committed_at":1789102665466851,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":0,"result":{"vector_revision":null},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665466851,"delete_count":2,"durability":"not_durable","put_count":0,"timestamp":5,"version":5},"effect":{"affected_count":1,"applied":true,"kind":"deleted","matched":true},"error":null,"index":1,"result":{"vector_revision":null},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"vector_batch_delete_results"}
```

#### `vector.batch_upsert` — `command run --command-json`

*batch.itemwise_mutation · BatchResult<VectorMutationItem> · stable*  
display: table # STATUS EFFECT VECTOR_REVISION · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"collection":"docs","entries":[{"key":"a","vector":[1.0,0.0,0.0]},{"key":"b","vector":[0.0,1.0,0.0]}],"type":"vector_batch_upsert"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.469195 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"vector_revision":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.469195 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"vector_revision":1},"status":"ok"}
human proposed      #  STATUS  EFFECT   VECTOR_REVISION
                    0  ok      created                1
                    1  ok      created                1
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.469195 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"vector_revision":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.469195 +00:00","delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"vector_revision":1},"status":"ok"}
raw proposed        0⇥ok⇥created⇥1
                    1⇥ok⇥created⇥1
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665469195,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"items":[{"applied":true,"commit":{"committed_at":1789102665469195,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"vector_revision":1},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665469195,"delete_count":0,"durability":"not_durable","put_count":2,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"vector_revision":1},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"vector_batch_upsert_results"}
```

#### `graph.batch_write` — `command run --command-json`

*batch.atomic_mutation · BatchResult<GraphBatchItemResult> · stable*  
display: table # STATUS EFFECT OPERATION CREATED · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"graph":"g","operations":[{"data":{"object_type":"person"},"node_id":"a","type":"upsert_node"},{"data":{"object_type":"person"},"node_id":"b","type":"upsert_node"},{"data":{},"dst":"b","edge_type":"knows","src":"a","type":"upsert_edge"}],"type":"graph_batch_write"}'
human today         {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"created":true,"operation":"upsert_node","operation_index":0},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"created":true,"operation":"upsert_node","operation_index":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":2,"result":{"created":true,"operation":"upsert_edge","operation_index":2},"status":"ok"}
human proposed      #  STATUS  EFFECT   OPERATION    CREATED
                    0  ok      created  upsert_node  true
                    1  ok      created  upsert_node  true
                    2  ok      created  upsert_edge  true
raw today           {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"created":true,"operation":"upsert_node","operation_index":0},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"created":true,"operation":"upsert_node","operation_index":1},"status":"ok"}
                    {"applied":true,"commit":{"committed_at":"2026-09-11 04:57:45.427712 +00:00","delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":2,"result":{"created":true,"operation":"upsert_edge","operation_index":2},"status":"ok"}
raw proposed        0⇥ok⇥created⇥upsert_node⇥true
                    1⇥ok⇥created⇥upsert_node⇥true
                    2⇥ok⇥created⇥upsert_edge⇥true
--json (unchanged)  {"data":{"applied":true,"commit":{"committed_at":1789102665427712,"delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"graph":"g","items":[{"applied":true,"commit":{"committed_at":1789102665427712,"delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":0,"result":{"created":true,"operation":"upsert_node","operation_index":0},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665427712,"delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":1,"result":{"created":true,"operation":"upsert_node","operation_index":1},"status":"ok"},{"applied":true,"commit":{"committed_at":1789102665427712,"delete_count":0,"durability":"not_durable","put_count":3,"timestamp":4,"version":4},"effect":{"affected_count":1,"applied":true,"kind":"created","matched":false},"error":null,"index":2,"result":{"created":true,"operation":"upsert_edge","operation_index":2},"status":"ok"}],"mode":"atomic","status":"ok"},"type":"graph_batch_write_result"}
```

#### `json.batch_get` — `command run --command-json`

*batch.itemwise_read · BatchResult<Maybe<JsonValue>> · stable*  
display: table # STATUS DOCUMENT_VERSION VERSION VALUE · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"entries":[{"key":"a","path":"$"},{"key":"b","path":"$"}],"type":"json_batch_get"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":1},"version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":2},"version":3},"status":"ok"}
human proposed      #  STATUS  DOCUMENT_VERSION  VERSION  VALUE
                    0  ok                     1        3  {"v":1}
                    1  ok                     1        3  {"v":2}
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":1},"version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":2},"version":3},"status":"ok"}
raw proposed        0⇥ok⇥1⇥3⇥{"v":1}
                    1⇥ok⇥1⇥3⇥{"v":2}
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":1},"version":3},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"document_version":1,"found":true,"timestamp":3,"value":{"v":2},"version":3},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"json_batch_get_results"}
```

#### `kv.batch_get` — `command run --command-json`

*batch.itemwise_read · BatchResult<Maybe<Bytes>> · stable*  
display: table # STATUS KEY VERSION VALUE · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"keys":["YQ==","Yg==","bWlzc2luZw=="],"type":"kv_batch_get"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"key":"YQ==","timestamp":3,"value":"MQ==","version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"key":"Yg==","timestamp":3,"value":"Mg==","version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":2,"result":{"found":false,"key":"bWlzc2luZw==","timestamp":null,"value":null,"version":null},"status":"miss"}
human proposed      #  STATUS  KEY      VERSION  VALUE
                    0  ok      a              3  1
                    1  ok      b              3  2
                    2  miss    missing        -  -
                    stderr› -- itemwise: 2 ok, 1 miss
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"key":"YQ==","timestamp":3,"value":"MQ==","version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"key":"Yg==","timestamp":3,"value":"Mg==","version":3},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":2,"result":{"found":false,"key":"bWlzc2luZw==","timestamp":null,"value":null,"version":null},"status":"miss"}
raw proposed        0⇥ok⇥a⇥3⇥1
                    1⇥ok⇥b⇥3⇥2
                    2⇥miss⇥missing⇥⇥
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"key":"YQ==","timestamp":3,"value":"MQ==","version":3},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"key":"Yg==","timestamp":3,"value":"Mg==","version":3},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":2,"result":{"found":false,"key":"bWlzc2luZw==","timestamp":null,"value":null,"version":null},"status":"miss"}],"mode":"itemwise","status":"partial"},"type":"batch_get_results"}
```

#### `vector.batch_get` — `command run --command-json`

*batch.itemwise_read · BatchResult<Maybe<VectorVersionedData>> · stable*  
display: table # STATUS KEY VERSION VECTOR_REVISION EMBEDDING METADATA · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"collection":"docs","keys":["a","b"],"type":"vector_batch_get"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"value":{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"value":{"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"}
human proposed      #  STATUS  KEY  VERSION  VECTOR_REVISION  EMBEDDING      METADATA
                    0  ok      a          4                1  [1.0,0.0,0.0]  -
                    1  ok      b          4                1  [0.0,1.0,0.0]  -
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"value":{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"value":{"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"}
raw proposed        0⇥ok⇥a⇥4⇥1⇥[1.0,0.0,0.0]⇥
                    1⇥ok⇥b⇥4⇥1⇥[0.0,1.0,0.0]⇥
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"found":true,"value":{"data":{"embedding":[1.0,0.0,0.0]},"key":"a","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"found":true,"value":{"data":{"embedding":[0.0,1.0,0.0]},"key":"b","timestamp":4,"vector_revision":1,"version":4}},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"vector_batch_get_results"}
```

#### `json.batch_exists` — `command run --command-json`

*batch.itemwise_status · BatchResult<BatchExistsPresence> · stable*  
display: table # STATUS EXISTS · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"keys":["a","missing"],"type":"json_batch_exists"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}
human proposed      #  STATUS  EXISTS
                    0  ok      true
                    1  ok      false
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}
raw proposed        0⇥ok⇥true
                    1⇥ok⇥false
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"json_batch_exists_results"}
```

#### `kv.batch_exists` — `command run --command-json`

*batch.itemwise_status · BatchResult<BatchExistsItemResult> · stable*  
display: table # STATUS KEY EXISTS · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"keys":["YQ==","bWlzc2luZw=="],"type":"kv_batch_exists"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true,"key":"YQ=="},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false,"key":"bWlzc2luZw=="},"status":"ok"}
human proposed      #  STATUS  KEY      EXISTS
                    0  ok      a        true
                    1  ok      missing  false
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true,"key":"YQ=="},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false,"key":"bWlzc2luZw=="},"status":"ok"}
raw proposed        0⇥ok⇥a⇥true
                    1⇥ok⇥missing⇥false
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true,"key":"YQ=="},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false,"key":"bWlzc2luZw=="},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"batch_exists_results"}
```

#### `vector.batch_exists` — `command run --command-json`

*batch.itemwise_status · BatchResult<BatchExistsPresence> · stable*  
display: table # STATUS EXISTS · stderr summary only on miss/failure · raw TSV

```text
$ strata command run --command-json '{"collection":"docs","keys":["a","missing"],"type":"vector_batch_exists"}'
human today         {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}
human proposed      #  STATUS  EXISTS
                    0  ok      true
                    1  ok      false
raw today           {"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"}
                    {"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}
raw proposed        0⇥ok⇥true
                    1⇥ok⇥false
--json (unchanged)  {"data":{"applied":false,"commit":null,"items":[{"applied":false,"commit":null,"effect":null,"error":null,"index":0,"result":{"exists":true},"status":"ok"},{"applied":false,"commit":null,"effect":null,"error":null,"index":1,"result":{"exists":false},"status":"ok"}],"mode":"itemwise","status":"ok"},"type":"vector_batch_exists_results"}
```

### 6.8 Inference (12 commands)

The designed arms (`models list`, `embed`, `generate`, `status`, `pull`, `rank`, `tokenize`, `detokenize`, `unload`) are unchanged. `capability` and `cache-status`, which fall through to pretty JSON today, become `field  value` lines of their declared fields like every other status.

#### `inference.cache_status` — `strata inference cache-status`

*inference.runtime_op · ModelCacheStatus · stable*  
display: fields generation_models, embedding_models, ranking_models · raw key/value of the same fields

```text
$ strata inference cache-status
human today         {
                      "embedding_models": [],
                      "generation_models": [],
                      "ranking_models": []
                    }
human proposed      generation_models  -
                    embedding_models   -
                    ranking_models     -
raw today           {"embedding_models":[],"generation_models":[],"ranking_models":[]}
raw proposed        generation_models⇥[]
                    embedding_models⇥[]
                    ranking_models⇥[]
--json (unchanged)  {"data":{"embedding_models":[],"generation_models":[],"ranking_models":[]},"type":"inference_cache_status"}
```

#### `inference.capability` — `strata inference capability`

*inference.runtime_op · InferenceCapability · stable*  
display: fields provider, model, availability, can_generate, can_embed, can_rank, can_tokenize, embedding_dim, requires_api_key, requires_network, network_enabled, provider_feature_enabled, supports_tools, supports_json_object, supports_json_schema, supports_logprobs · raw key/value of the same fields

```text
$ strata inference capability openai:gpt-4o-mini
human today         {
                      "availability": "ready",
                      "can_embed": true,
                      "can_generate": true,
                      "can_rank": false,
                      "can_tokenize": false,
                      "embedding_dim": 0,
                      "model": "gpt-4o-mini",
                      "network_enabled": true,
                      "provider": "openai",
                      "provider_feature_enabled": true,
                      "requires_api_key": true,
                      "requires_network": true,
                      "supports_json_object": true,
                      "supports_json_schema": true,
                      "supports_logprobs": true,
                      "supports_tools": true
                    }
human proposed      provider                  openai
                    model                     gpt-4o-mini
                    availability              ready
                    can_generate              true
                    can_embed                 true
                    can_rank                  false
                    can_tokenize              false
                    embedding_dim             0
                    requires_api_key          true
                    requires_network          true
                    network_enabled           true
                    provider_feature_enabled  true
                    supports_tools            true
                    supports_json_object      true
                    supports_json_schema      true
                    supports_logprobs         true
raw today           {"availability":"ready","can_embed":true,"can_generate":true,"can_rank":false,"can_tokenize":false,"embedding_dim":0,"model":"gpt-4o-mini","network_enabled":true,"provider":"openai","provider_feature_enabled":true,"requires_api_key":true,"requires_network":true,"supports_json_object":true,"supports_json_schema":true,"supports_logprobs":true,"supports_tools":true}
raw proposed        provider⇥openai
                    model⇥gpt-4o-mini
                    availability⇥ready
                    can_generate⇥true
                    can_embed⇥true
                    can_rank⇥false
                    can_tokenize⇥false
                    embedding_dim⇥0
                    requires_api_key⇥true
                    requires_network⇥true
                    network_enabled⇥true
                    provider_feature_enabled⇥true
                    supports_tools⇥true
                    supports_json_object⇥true
                    supports_json_schema⇥true
                    supports_logprobs⇥true
--json (unchanged)  {"data":{"availability":"ready","can_embed":true,"can_generate":true,"can_rank":false,"can_tokenize":false,"embedding_dim":0,"model":"gpt-4o-mini","network_enabled":true,"provider":"openai","provider_feature_enabled":true,"requires_api_key":true,"requires_network":true,"supports_json_object":true,"supports_json_schema":true,"supports_logprobs":true,"supports_tools":true},"type":"inference_capability"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.invalid_request: The inference request is invalid. (err_…)
  hint: Correct the inference request options for the selected provider or model.
  ref: https://stratadb.org/e/inference.invalid_request
```

#### `inference.detokenize` — `strata inference detokenize`

*inference.runtime_op · DetokenizedText · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/text.json)
human today         hello
human proposed      = unchanged
raw today           hello
raw proposed        = unchanged
--json (unchanged)  {"data":"hello","type":"inference_text"}
```

#### `inference.embed` — `strata inference embed`

*inference.runtime_op · EmbeddingsResponse · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/embed.json)
human today         1 embeddings · dim 8
                      [0] [0.0681, 0.3584, -0.2061, -0.7119, -0.1242, -0.5920, …]
human proposed      = unchanged
raw today           0.06806039810180664 0.3584129810333252 -0.20612019300460815 -0.71190345287323 -0.12423133850097656 -0.5919586420059204 -0.23881715536117554 0.2620750665664673
raw proposed        = unchanged
--json (unchanged)  {"data":{"data":[{"embedding":[0.06806039810180664,0.3584129810333252,-0.20612019300460815,-0.71190345287323,-0.12423133850097656,-0.5919586420059204,-0.23881715536117554,0.2620750665664673],"index":0}],"dimension":8,"model":"miniLM","usage":{"completion_tokens":0,"prompt_tokens":2,"total_tokens":2}},"type":"inference_embeddings"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
inference.invalid_request: The inference request is invalid. (err_…)
  hint: Correct the inference request options for the selected provider or model.
  ref: https://stratadb.org/e/inference.invalid_request
inference.unsupported_operation: The inference operation is unsupported. (err_…)
  hint: Use an inference operation supported by the selected provider and model.
  ref: https://stratadb.org/e/inference.unsupported_operation
```

#### `inference.generate` — `strata inference generate`

*inference.runtime_op · ChatResponse · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/generate.json)
human today         fake:Write a haiku about databases.
                    -- stop: stop · prompt 5 tok · completion 5 tok
human proposed      = unchanged
raw today           fake:Write a haiku about databases.
raw proposed        = unchanged
--json (unchanged)  {"data":{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"fake:Write a haiku about databases.","role":"assistant"}}],"model":"anthropic:claude-3-5-haiku-latest","usage":{"completion_tokens":5,"prompt_tokens":5,"total_tokens":10}},"type":"inference_generation"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
inference.unsupported_operation: The inference operation is unsupported. (err_…)
  hint: Use an inference operation supported by the selected provider and model.
  ref: https://stratadb.org/e/inference.unsupported_operation
```

#### `inference.models.pull` — `strata inference models pull`

*inference.runtime_op · PullModelOutput · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/models_pull.json)
human today         pulled miniLM -> /fake/models/all-MiniLM-L6-v2.F16.gguf
human proposed      = unchanged
raw today           {"model":"miniLM","path":"/fake/models/all-MiniLM-L6-v2.F16.gguf"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"model":"miniLM","path":"/fake/models/all-MiniLM-L6-v2.F16.gguf"},"type":"inference_model_pulled"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
inference.unsupported_operation: The inference operation is unsupported. (err_…)
  hint: Use an inference operation supported by the selected provider and model.
  ref: https://stratadb.org/e/inference.unsupported_operation
```

#### `inference.rank` — `strata inference rank`

*inference.runtime_op · RankResponse · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/rank.json)
human today         0⇥0.000000
                    1⇥0.000000
human proposed      = unchanged
raw today           {"index":0,"score":0.0,"status":"ok"}
                    {"index":1,"score":0.0,"status":"ok"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"items":[{"index":0,"score":0.0,"status":"ok"},{"index":1,"score":0.0,"status":"ok"}]},"type":"inference_ranking"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.unsupported_operation: The inference operation is unsupported. (err_…)
  hint: Use an inference operation supported by the selected provider and model.
  ref: https://stratadb.org/e/inference.unsupported_operation
```

#### `inference.status` — `strata inference status`

*inference.runtime_op · InferenceStatus · stable*  
display: designed arm — unchanged

```text
$ strata inference status
human today         build⇥cloud providers only
                    ⇥local models: this build runs cloud models only, and a bare model name means a local model. Either name a cloud model instead — `openai:<model>`, `google:<model>` or `anthropic:<model>` — or run `strata inference install-local` to add local execution.
                    
                    providers
                      openai⇥no key -- `strata config set openai.api_key <key>`, or export OPENAI_API_KEY
                      anthropic⇥no key -- `strata config set anthropic.api_key <key>`, or export ANTHROPIC_API_KEY
                      google⇥no key -- `strata config set google.api_key <key>`, or export GOOGLE_API_KEY
                      local⇥not in this build
                    
                    models⇥/home/anibjoshi/.strata/models (shared by every database)
                      downloaded⇥5 of 11 catalogued
                    ⇥this build cannot download models -- `strata inference install-local` adds downloading along with local execution
human proposed      = unchanged
raw today           {"local_execution":false,"local_remedy":"this build runs cloud models only, and a bare model name means a local model. Either name a cloud model instead — `openai:<model>`, `google:<model>` or `anthropic:<model>` — or run `strata inference install-local` to add local execution.","model_download":false,"models_catalogued":11,"models_dir":"/home/anibjoshi/.strata/models","models_downloaded":5,"providers":[{"base_url":"https://api.openai.com/v1","base_url_env_var":"OPENAI_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"OPENAI_API_KEY","key_present":false,"key_source":null,"model_prefix":"openai:","provider":"openai","ready":false,"requires_api_key":true},{"base_url":"https://api.anthropic.com","base_url_env_var":"ANTHROPIC_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"ANTHROPIC_API_KEY","key_present":false,"key_source":null,"model_prefix":"anthropic:","provider":"anthropic","ready":false,"requires_api_key":true},{"base_url":"https://generativelanguage.googleapis.com","base_url_env_var":"GOOGLE_GEMINI_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"GOOGLE_API_KEY","key_present":false,"key_source":null,"model_prefix":"google:","provider":"google","ready":false,"requires_api_key":true},{"base_url":null,"base_url_env_var":null,"base_url_source":null,"feature_enabled":false,"key_env_var":null,"key_present":false,"key_source":null,"model_prefix":"local:","provider":"local","ready":false,"requires_api_key":false}]}
raw proposed        = unchanged
--json (unchanged)  {"data":{"local_execution":false,"local_remedy":"this build runs cloud models only, and a bare model name means a local model. Either name a cloud model instead — `openai:<model>`, `google:<model>` or `anthropic:<model>` — or run `strata inference install-local` to add local execution.","model_download":false,"models_catalogued":11,"models_dir":"/home/anibjoshi/.strata/models","models_downloaded":5,"providers":[{"base_url":"https://api.openai.com/v1","base_url_env_var":"OPENAI_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"OPENAI_API_KEY","key_present":false,"key_source":null,"model_prefix":"openai:","provider":"openai","ready":false,"requires_api_key":true},{"base_url":"https://api.anthropic.com","base_url_env_var":"ANTHROPIC_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"ANTHROPIC_API_KEY","key_present":false,"key_source":null,"model_prefix":"anthropic:","provider":"anthropic","ready":false,"requires_api_key":true},{"base_url":"https://generativelanguage.googleapis.com","base_url_env_var":"GOOGLE_GEMINI_BASE_URL","base_url_source":null,"feature_enabled":true,"key_env_var":"GOOGLE_API_KEY","key_present":false,"key_source":null,"model_prefix":"google:","provider":"google","ready":false,"requires_api_key":true},{"base_url":null,"base_url_env_var":null,"base_url_source":null,"feature_enabled":false,"key_env_var":null,"key_present":false,"key_source":null,"model_prefix":"local:","provider":"local","ready":false,"requires_api_key":false}]},"type":"inference_status"}
```

#### `inference.tokenize` — `strata inference tokenize`

*inference.runtime_op · TokenIds · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/tokenize.json)
human today         104 101 108 108 111 32 119 111 114 108 100
human proposed      = unchanged
raw today           104 101 108 108 111 32 119 111 114 108 100
raw proposed        = unchanged
--json (unchanged)  {"data":[104,101,108,108,111,32,119,111,114,108,100],"type":"inference_token_ids"}
```

Errors (stderr, exit 1, unchanged; message/hint from the registry):

```text
inference.unknown_model: The requested model is not in the catalog. (err_…)
  hint: Check the model name against `strata inference models list`, or pass the path of a GGUF file.
  ref: https://stratadb.org/e/inference.unknown_model
```

#### `inference.unload` — `strata inference unload`

*inference.runtime_op · UnloadResult · stable*  
display: designed arm — unchanged

```text
$ strata inference unload
human today         no cached entry
human proposed      = unchanged
raw today           {"unloaded":false}
raw proposed        = unchanged
--json (unchanged)  {"data":{"unloaded":false},"type":"inference_unload_result"}
```

#### `inference.models.list` — `strata inference models list`

*inference.models_page · Page<ModelInfo, String> · stable*  
display: designed arm — unchanged

```text
$ strata inference models list
human today         miniLM⇥embed⇥bert⇥f16⇥unavailable⇥45 MB
                    nomic-embed⇥embed⇥nomic-bert⇥q8_0⇥unavailable⇥260 MB
                    bge-m3⇥embed⇥xlm-roberta⇥q8_0⇥unavailable⇥1.2 GB
                    gemma-embed⇥embed⇥gemma3⇥q8_0⇥unavailable⇥320 MB
                    gpt2⇥generate⇥gpt2⇥q8_0⇥unavailable⇥178 MB
                    tinyllama⇥generate⇥llama⇥q4_k_m⇥unavailable⇥670 MB
                    qwen3:1.7b⇥generate⇥qwen3⇥q8_0⇥unavailable⇥2.0 GB
                    gemma3:1b⇥generate⇥gemma3⇥q4_k_m⇥unavailable⇥780 MB
                    phi3.5⇥generate⇥phi3⇥q4_k_m⇥unavailable⇥2.4 GB
                    llama3.1:8b⇥generate⇥llama⇥q4_k_m⇥unavailable⇥4.6 GB
                    qwen3:8b⇥generate⇥qwen3⇥q4_k_m⇥unavailable⇥4.7 GB
                    
                    11 model(s) unavailable: this build cannot run local models -- a bare name like these means a local model. Add local execution with `strata inference install-local`, or name a cloud model instead (`openai:<model>`, `google:<model>`, `anthropic:<model>`).
human proposed      = unchanged
raw today           {"architecture":"bert","default_quant":"f16","embedding_dim":384,"hf_repo":"stratalab-org/all-MiniLM-L6-v2-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/all-MiniLM-L6-v2.F16.gguf","name":"miniLM","runnable":false,"size_bytes":45000000,"task":"embed"}
                    {"architecture":"nomic-bert","default_quant":"q8_0","embedding_dim":768,"hf_repo":"stratalab-org/nomic-embed-text-v1.5-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/nomic-embed-text-v1.5.Q8_0.gguf","name":"nomic-embed","runnable":false,"size_bytes":260000000,"task":"embed"}
                    {"architecture":"xlm-roberta","default_quant":"q8_0","embedding_dim":1024,"hf_repo":"stratalab-org/BGE-M3-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/bge-m3-Q8_0.gguf","name":"bge-m3","runnable":false,"size_bytes":1200000000,"task":"embed"}
                    {"architecture":"gemma3","default_quant":"q8_0","embedding_dim":768,"hf_repo":"stratalab-org/embedding-gemma-300M-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/embedding-gemma-300M-Q8_0.gguf","name":"gemma-embed","runnable":false,"size_bytes":320000000,"task":"embed"}
                    {"architecture":"gpt2","default_quant":"q8_0","embedding_dim":0,"hf_repo":"stratalab-org/gpt2-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/gpt2.Q8_0.gguf","name":"gpt2","runnable":false,"size_bytes":178000000,"task":"generate"}
                    {"architecture":"llama","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/TinyLlama-1.1B-Chat-v1.0-GGUF","is_local":false,"local_path":null,"name":"tinyllama","runnable":false,"size_bytes":670000000,"task":"generate"}
                    {"architecture":"qwen3","default_quant":"q8_0","embedding_dim":0,"hf_repo":"stratalab-org/Qwen3-1.7B-GGUF","is_local":false,"local_path":null,"name":"qwen3:1.7b","runnable":false,"size_bytes":2000000000,"task":"generate"}
                    {"architecture":"gemma3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/gemma-3-1b-it-GGUF","is_local":false,"local_path":null,"name":"gemma3:1b","runnable":false,"size_bytes":780000000,"task":"generate"}
                    {"architecture":"phi3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Phi-3.5-mini-instruct-GGUF","is_local":false,"local_path":null,"name":"phi3.5","runnable":false,"size_bytes":2400000000,"task":"generate"}
                    {"architecture":"llama","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Meta-Llama-3.1-8B-Instruct-GGUF","is_local":false,"local_path":null,"name":"llama3.1:8b","runnable":false,"size_bytes":4600000000,"task":"generate"}
                    {"architecture":"qwen3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Qwen3-8B-GGUF","is_local":false,"local_path":null,"name":"qwen3:8b","runnable":false,"size_bytes":4700000000,"task":"generate"}
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[{"architecture":"bert","default_quant":"f16","embedding_dim":384,"hf_repo":"stratalab-org/all-MiniLM-L6-v2-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/all-MiniLM-L6-v2.F16.gguf","name":"miniLM","runnable":false,"size_bytes":45000000,"task":"embed"},{"architecture":"nomic-bert","default_quant":"q8_0","embedding_dim":768,"hf_repo":"stratalab-org/nomic-embed-text-v1.5-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/nomic-embed-text-v1.5.Q8_0.gguf","name":"nomic-embed","runnable":false,"size_bytes":260000000,"task":"embed"},{"architecture":"xlm-roberta","default_quant":"q8_0","embedding_dim":1024,"hf_repo":"stratalab-org/BGE-M3-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/bge-m3-Q8_0.gguf","name":"bge-m3","runnable":false,"size_bytes":1200000000,"task":"embed"},{"architecture":"gemma3","default_quant":"q8_0","embedding_dim":768,"hf_repo":"stratalab-org/embedding-gemma-300M-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/embedding-gemma-300M-Q8_0.gguf","name":"gemma-embed","runnable":false,"size_bytes":320000000,"task":"embed"},{"architecture":"gpt2","default_quant":"q8_0","embedding_dim":0,"hf_repo":"stratalab-org/gpt2-GGUF","is_local":true,"local_path":"/home/anibjoshi/.strata/models/gpt2.Q8_0.gguf","name":"gpt2","runnable":false,"size_bytes":178000000,"task":"generate"},{"architecture":"llama","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/TinyLlama-1.1B-Chat-v1.0-GGUF","is_local":false,"local_path":null,"name":"tinyllama","runnable":false,"size_bytes":670000000,"task":"generate"},{"architecture":"qwen3","default_quant":"q8_0","embedding_dim":0,"hf_repo":"stratalab-org/Qwen3-1.7B-GGUF","is_local":false,"local_path":null,"name":"qwen3:1.7b","runnable":false,"size_bytes":2000000000,"task":"generate"},{"architecture":"gemma3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/gemma-3-1b-it-GGUF","is_local":false,"local_path":null,"name":"gemma3:1b","runnable":false,"size_bytes":780000000,"task":"generate"},{"architecture":"phi3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Phi-3.5-mini-instruct-GGUF","is_local":false,"local_path":null,"name":"phi3.5","runnable":false,"size_bytes":2400000000,"task":"generate"},{"architecture":"llama","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Meta-Llama-3.1-8B-Instruct-GGUF","is_local":false,"local_path":null,"name":"llama3.1:8b","runnable":false,"size_bytes":4600000000,"task":"generate"},{"architecture":"qwen3","default_quant":"q4_k_m","embedding_dim":0,"hf_repo":"stratalab-org/Qwen3-8B-GGUF","is_local":false,"local_path":null,"name":"qwen3:8b","runnable":false,"size_bytes":4700000000,"task":"generate"}]},"type":"inference_models"}
```

#### `inference.models.local` — `strata inference models local`

*inference.models_page · Page<ModelInfo, String> · stable*  
display: designed arm — unchanged

```text
(fixture responses/v1/inference/models_local.json)
human today         (none)
human proposed      = unchanged
raw today           (empty)
raw proposed        = unchanged
--json (unchanged)  {"data":{"cursor":null,"has_more":false,"items":[]},"type":"inference_models"}
```

---

Generated by the catalog script in the #3314 review session from `output-catalog-dump.json`; regenerate rather than hand-edit.
