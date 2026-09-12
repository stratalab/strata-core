//! The CLI's own reports (#3339).
//!
//! `doctor`, `init`, `update`, `uninstall`, `inference install-local`,
//! `ipc start/stop`, `agents …`, `config …` and the REPL's context line are
//! not executor commands: they run in the CLI, they are not in the IDL, and
//! no `display:` declaration describes them. Until the output contract's S3b
//! they shared the renderer's JSON fallback with command output, which is why
//! they went on printing JSON at a reader long after the commands stopped.
//!
//! They follow the same vocabulary by hand, and only three shapes of it:
//!
//! - an **action** answers with a one-line receipt, and hands a script its
//!   record as `key<TAB>value` lines (output contract Q17);
//! - a **list** answers with a table, and hands a script the same rows as TSV
//!   (R1-table);
//! - everything else is a **record** — the block `strata info` already prints,
//!   with its labels taken from the payload's own keys, since a report has no
//!   declaration to choose them.
//!
//! A report that adds no shape of its own still gets the record block, so the
//! next one someone writes is legible without touching this file.
//!
//! Wasm-safe: `command print` and the config family reach here from the
//! playground too.

use serde_json::Value;

use crate::options::Format;
use strata_executor::cli_metadata::CliDisplayAs;

use crate::render::{cell, escape_cell, push_field, push_indented, push_label, Rendered, INDENT};
use crate::table::{Cell, Table};

// Writing to a String is infallible; the macro keeps the call sites terse.
macro_rules! line {
    ($out:expr, $($arg:tt)*) => {{
        use std::fmt::Write as _;
        let _ = writeln!($out, $($arg)*);
    }};
}

/// Renders a report for a reader or a script. `--json` and `--pretty` never
/// come here: the envelope is the report.
pub(crate) fn render_report(value: &Value, format: Format) -> Rendered {
    let (kind, data) = split(value);
    let mut stdout = String::new();
    let mut stderr = String::new();
    match kind {
        // `command print` composes a command for `command run` to read back;
        // its JSON is the answer, not a description of one.
        Some("command") => line!(stdout, "{}", compact(data)),
        Some(kind) if receipt(kind, data).is_some() => {
            if format == Format::Human {
                // The receipt is the whole answer for a reader; a script gets
                // the facts it was built from (Q17).
                line!(stdout, "{}", receipt(kind, data).unwrap_or_default());
            } else {
                render_record(data, format, &mut stdout);
            }
        }
        Some("agents_commands") => render_list(
            data.get("commands").unwrap_or(&Value::Null),
            &[("path_display", "COMMAND"), ("summary", "SUMMARY")],
            format,
            &mut stdout,
        ),
        Some("agents_errors") => render_list(
            data.get("errors").unwrap_or(&Value::Null),
            &[("code", "CODE"), ("class", "CLASS"), ("hint", "HINT")],
            format,
            &mut stdout,
        ),
        Some("doctor") => {
            render_record(data, format, &mut stdout);
            // The same rule every listing follows: a diagnostic only when
            // something deserves attention (R5).
            if format == Format::Human {
                let issues = data
                    .get("issues")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                if issues > 0 {
                    let plural = if issues == 1 { "issue" } else { "issues" };
                    line!(stderr, "-- {issues} {plural}");
                }
            }
        }
        Some(_) | None => render_record(data, format, &mut stdout),
    }
    Rendered::new(stdout, stderr)
}

/// A report's name and payload. A report names itself the way an output does
/// (`{"type": …, "data": …}`); the config family answers with a bare record,
/// which is its own payload.
fn split(value: &Value) -> (Option<&str>, &Value) {
    let Some(object) = value.as_object() else {
        return (None, value);
    };
    match (
        object.get("type").and_then(Value::as_str),
        object.get("data"),
    ) {
        (Some(kind), Some(data)) => (Some(kind), data),
        // `command print` emits a command, whose `type` is its wire name and
        // whose fields sit beside it.
        (Some(_), None) => (Some("command"), value),
        (None, _) => (None, value),
    }
}

/// The one-line answer an action-shaped report gives a reader, or `None` for
/// the reports that are records.
fn receipt(kind: &str, data: &Value) -> Option<String> {
    let text = |key: &str| {
        data.get(key)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    let flag = |key: &str| data.get(key).and_then(Value::as_bool) == Some(true);
    let count = |key: &str| data.get(key).and_then(Value::as_array).map_or(0, Vec::len);
    Some(match kind {
        "init" if flag("created") => format!("initialized {}", text("home")),
        "init" => format!("{} is already initialized", text("home")),
        "update" if flag("changed") => {
            format!("updated strata {} -> {}", text("current"), text("latest"))
        }
        "update" if flag("update_available") => format!(
            "an update is available: {} -> {} (run `strata update`)",
            text("current"),
            text("latest")
        ),
        "update" => format!("strata is up to date ({})", text("current")),
        "inference_install_local" if flag("changed") => format!(
            "strata {} now runs local models; `strata inference status` confirms it",
            text("version")
        ),
        "inference_install_local" => {
            format!("this build already runs local models ({})", text("version"))
        }
        "uninstall" if flag("aborted") => "uninstall cancelled".to_owned(),
        "uninstall" => plural(count("removed"), "path", "removed"),
        "agents_init" => {
            let written = plural(count("written"), "file", "wrote");
            match data.get("next").and_then(Value::as_str) {
                Some(next) => format!("{written}; {next}"),
                None => written,
            }
        }
        "agents_skill" => plural(count("written"), "skill", "wrote"),
        _ => return None,
    })
}

/// `wrote 2 files`, `removed 1 path` — the verb, the count, and its noun.
fn plural(count: usize, noun: &str, verb: &str) -> String {
    if count == 1 {
        format!("{verb} {count} {noun}")
    } else {
        format!("{verb} {count} {noun}s")
    }
}

/// A record report: `label  value` lines from the payload's own keys for a
/// reader, `key<TAB>value` for a script. Nested records are indented under
/// their key, a list of records is a table, and a scalar payload is itself.
fn render_record(data: &Value, format: Format, out: &mut String) {
    let Some(fields) = data.as_object() else {
        match (data, format) {
            (Value::Null, Format::Human) => line!(out, "(nil)"),
            (Value::Null, _) => {}
            _ => line!(out, "{}", cell(Some(data), None, format).text),
        }
        return;
    };
    if fields.is_empty() {
        if format == Format::Human {
            line!(out, "(empty)");
        }
        return;
    }
    if format == Format::Human {
        render_block(fields, 0, out);
    } else {
        render_flat(fields, "", out);
    }
}

/// One block of `label  value` lines, labels padded to the widest in this
/// block — the layout a declared record already uses, with the payload's keys
/// standing in for declared labels.
fn render_block(fields: &serde_json::Map<String, Value>, indent: usize, out: &mut String) {
    let width = fields
        .keys()
        .map(|key| key.chars().count())
        .max()
        .unwrap_or(0);
    for (key, value) in fields {
        match value {
            Value::Object(nested) if !nested.is_empty() => {
                push_label(out, indent, key);
                render_block(nested, indent + INDENT, out);
            }
            Value::Array(items) if !items.is_empty() && items.iter().all(Value::is_object) => {
                push_label(out, indent, key);
                push_indented(
                    out,
                    &records_table(items, Format::Human).human(),
                    indent + INDENT,
                );
            }
            _ => push_field(
                out,
                indent,
                key,
                width,
                &cell(Some(value), as_(value), Format::Human).text,
            ),
        }
    }
}

/// The same record for a script: one `key<TAB>value` line per leaf, nested
/// keys dotted, lists and tables as compact JSON — the rule a declared
/// record's `--raw` follows (Q16).
fn render_flat(fields: &serde_json::Map<String, Value>, prefix: &str, out: &mut String) {
    for (key, value) in fields {
        let key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            Value::Object(nested) if !nested.is_empty() => render_flat(nested, &key, out),
            _ => line!(
                out,
                "{key}\t{}",
                cell(Some(value), as_(value), Format::Raw).text
            ),
        }
    }
}

/// A list report: the named columns of every record, as a table for a reader
/// and TSV for a script.
fn render_list(items: &Value, columns: &[(&str, &str)], format: Format, out: &mut String) {
    let Some(items) = items.as_array() else {
        if format == Format::Human {
            line!(out, "(nil)");
        }
        return;
    };
    if items.is_empty() {
        if format == Format::Human {
            line!(out, "(empty)");
        }
        return;
    }
    let mut table = Table::new(
        columns
            .iter()
            .map(|(_, header)| (*header).to_owned())
            .collect(),
    );
    for item in items {
        table.push(
            columns
                .iter()
                .map(|(key, _)| cell(item.get(*key), None, format))
                .collect(),
        );
    }
    out.push_str(&match format {
        Format::Human => table.human(),
        Format::Raw | Format::Json => table.raw(),
    });
}

/// What a report's value looks like without a declaration to say so: a list
/// of scalars reads as one (space-joined for a reader, `-` when empty, compact
/// JSON for a script), and everything else is itself.
fn as_(value: &Value) -> Option<CliDisplayAs> {
    value.as_array().map(|_| CliDisplayAs::List)
}

/// A table over records whose columns are whichever keys they carry, in the
/// order the first record lists them — a report declares no columns, so the
/// data does.
fn records_table(items: &[Value], format: Format) -> Table {
    let mut columns: Vec<&String> = Vec::new();
    for item in items {
        for key in item.as_object().into_iter().flatten().map(|(key, _)| key) {
            if !columns.contains(&key) {
                columns.push(key);
            }
        }
    }
    let mut table = Table::new(columns.iter().map(|key| key.to_ascii_uppercase()).collect());
    for item in items {
        table.push(
            columns
                .iter()
                .map(|key| cell(item.get(key.as_str()), None, format))
                .collect(),
        );
    }
    table
}

/// One line of compact JSON, escaped so a report stays one line per fact.
fn compact(value: &Value) -> String {
    Cell::text(escape_cell(
        &serde_json::to_string(value).unwrap_or_default(),
    ))
    .text
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{receipt, render_report};
    use crate::options::Format;

    fn human(value: &serde_json::Value) -> String {
        render_report(value, Format::Human).stdout.text()
    }

    fn raw(value: &serde_json::Value) -> String {
        render_report(value, Format::Raw).stdout.text()
    }

    /// Every report this CLI composes, and the shape it answers in. A report
    /// that is not an action or a list is a record, which is the default — so
    /// a new one is legible the day it is written, and this list says which
    /// ones earned a sentence instead.
    const REPORTS: &[(&str, &str)] = &[
        ("agents_commands", "list"),
        ("agents_errors", "list"),
        ("agents_init", "receipt"),
        ("agents_skill", "receipt"),
        ("context", "record"),
        ("doctor", "record"),
        ("inference_install_local", "receipt"),
        ("init", "receipt"),
        ("ipc_started", "record"),
        ("ipc_stop", "record"),
        ("uninstall", "receipt"),
        ("update", "receipt"),
    ];

    #[test]
    fn only_the_action_shaped_reports_answer_with_a_sentence() {
        // The payload carries every key the receipts read, so a tag that
        // claims a sentence has to produce one from it.
        let sample = json!({
            "home": "/h", "created": true, "current": "1.2.1", "latest": "1.3.0",
            "changed": false, "update_available": true, "version": "1.2.1",
            "removed": ["/a"], "written": [{"agent": "claude", "path": "/b"}],
            "next": serde_json::Value::Null, "aborted": false
        });
        for (kind, shape) in REPORTS {
            let answer = receipt(kind, &sample);
            assert_eq!(
                answer.is_some(),
                *shape == "receipt",
                "{kind} is declared {shape} but {} a receipt",
                if answer.is_some() { "has" } else { "has no" }
            );
        }
    }

    #[test]
    fn an_action_tells_a_reader_what_happened_and_a_script_what_it_did() {
        let init = json!({"type": "init", "data": {"home": "/h", "created": true}});
        assert_eq!(human(&init), "initialized /h\n");
        // Q17: the script gets the record the sentence was built from.
        assert_eq!(raw(&init), "created\ttrue\nhome\t/h\n");
        assert_eq!(
            human(&json!({"type": "init", "data": {"home": "/h", "created": false}})),
            "/h is already initialized\n"
        );
    }

    #[test]
    fn update_says_which_of_its_three_answers_this_is() {
        let update = |available: bool, changed: bool| {
            human(&json!({"type": "update", "data": {
                "current": "1.2.1", "latest": "1.3.0",
                "update_available": available, "changed": changed
            }}))
        };
        assert_eq!(update(false, false), "strata is up to date (1.2.1)\n");
        assert_eq!(
            update(true, false),
            "an update is available: 1.2.1 -> 1.3.0 (run `strata update`)\n"
        );
        assert_eq!(update(true, true), "updated strata 1.2.1 -> 1.3.0\n");
    }

    #[test]
    fn a_counted_action_names_its_noun_once_or_many() {
        let uninstall = |removed: serde_json::Value| {
            human(&json!({"type": "uninstall", "data": {"aborted": false, "removed": removed}}))
        };
        assert_eq!(uninstall(json!(["/a"])), "removed 1 path\n");
        assert_eq!(uninstall(json!(["/a", "/b"])), "removed 2 paths\n");
        assert_eq!(
            human(&json!({"type": "uninstall", "data": {"aborted": true}})),
            "uninstall cancelled\n"
        );
        // A follow-up step is part of the sentence, not a second line.
        assert_eq!(
            human(&json!({"type": "agents_init", "data": {
                "written": ["/a"], "next": "run `strata agents init --apply`"
            }})),
            "wrote 1 file; run `strata agents init --apply`\n"
        );
    }

    #[test]
    fn the_agent_files_report_counts_what_it_wrote() {
        // `agents init` and `agents skill --write` both answer with the files
        // they planted, under the key each one uses.
        assert_eq!(
            human(&json!({"type": "agents_skill", "data": {"written": [
                {"agent": "claude", "path": ".claude/skills/strata/SKILL.md", "state": "written"},
                {"agent": "cursor", "path": ".cursor/rules/strata.mdc", "state": "written"}
            ]}})),
            "wrote 2 skills\n"
        );
        assert_eq!(
            human(&json!({"type": "agents_skill", "data": {"written": []}})),
            "wrote 0 skills\n"
        );
        // A script gets the paths and their states, not the sentence.
        assert!(
            raw(&json!({"type": "agents_init", "data": {"written": [".strata/AGENTS.md"]}}))
                .contains(".strata/AGENTS.md"),
        );
    }

    #[test]
    fn install_local_says_whether_this_build_changed() {
        let install = |changed: bool| {
            human(&json!({"type": "inference_install_local", "data": {
                "version": "1.2.1", "local_execution": true, "changed": changed
            }}))
        };
        assert_eq!(
            install(true),
            "strata 1.2.1 now runs local models; `strata inference status` confirms it\n"
        );
        assert_eq!(
            install(false),
            "this build already runs local models (1.2.1)\n"
        );
    }

    #[test]
    fn a_record_with_nothing_in_it_says_so_rather_than_printing_braces() {
        // An empty payload, an empty nested record, and an absent one: none of
        // them should reach a reader as JSON punctuation.
        assert_eq!(human(&json!({"type": "doctor", "data": {}})), "(empty)\n");
        assert_eq!(raw(&json!({"type": "doctor", "data": {}})), "");
        assert_eq!(human(&json!({"type": "doctor", "data": null})), "(nil)\n");
        assert_eq!(raw(&json!({"type": "doctor", "data": null})), "");
        let nested = json!({"type": "doctor", "data": {"binary": "1.2.1", "database": {}}});
        assert_eq!(
            human(&nested),
            "binary    1.2.1\ndatabase  {}\n",
            "an empty nested record is a value, not a block with nothing under it"
        );
        assert_eq!(raw(&nested), "binary\t1.2.1\ndatabase\t{}\n");
    }

    #[test]
    fn a_record_report_is_the_block_a_reader_knows_from_a_command() {
        let doctor = json!({"type": "doctor", "data": {
            "binary": "1.2.1",
            "inference": {"local_execution": false, "ready_providers": []},
            "issues": [{"code": "failed_precondition.cli.binary_not_on_path", "hint": "add it"}],
            "path_ok": false
        }});
        assert_eq!(
            human(&doctor),
            concat!(
                "binary     1.2.1\n",
                "inference\n",
                "  local_execution  false\n",
                "  ready_providers  -\n",
                "issues\n",
                "  CODE                                        HINT\n",
                "  failed_precondition.cli.binary_not_on_path  add it\n",
                "path_ok    false\n",
            ),
            "nested records indent, record lists are tables, an empty list is `-`"
        );
        // Q16: wire names, dotted by nesting, lists as compact JSON.
        assert_eq!(
            raw(&doctor),
            concat!(
                "binary\t1.2.1\n",
                "inference.local_execution\tfalse\n",
                "inference.ready_providers\t[]\n",
                "issues\t[{\"code\":\"failed_precondition.cli.binary_not_on_path\",\"hint\":\"add it\"}]\n",
                "path_ok\tfalse\n",
            )
        );
    }

    #[test]
    fn doctor_says_how_many_issues_only_when_it_found_some() {
        let with = |issues: serde_json::Value| {
            render_report(
                &json!({"type": "doctor", "data": {"issues": issues}}),
                Format::Human,
            )
            .stderr
        };
        assert_eq!(with(json!([])), "", "a clean run says nothing (R5)");
        assert_eq!(with(json!([{"code": "a"}])), "-- 1 issue\n");
        assert_eq!(with(json!([{"code": "a"}, {"code": "b"}])), "-- 2 issues\n");
        // A script reads the count from the rows, or from `--json`.
        assert_eq!(
            render_report(
                &json!({"type": "doctor", "data": {"issues": [{"code": "a"}]}}),
                Format::Raw
            )
            .stderr,
            ""
        );
    }

    #[test]
    fn a_list_report_is_a_table_of_the_columns_it_names() {
        let errors = json!({"type": "agents_errors", "data": {"count": 1, "errors": [
            {"code": "invalid_argument.engine.branch_name", "class": "invalid_argument", "hint": "use a name"}
        ]}});
        assert_eq!(
            human(&errors),
            concat!(
                "CODE                                 CLASS             HINT\n",
                "invalid_argument.engine.branch_name  invalid_argument  use a name\n",
            )
        );
        assert_eq!(
            raw(&errors),
            "invalid_argument.engine.branch_name\tinvalid_argument\tuse a name\n"
        );
        assert_eq!(
            human(&json!({"type": "agents_errors", "data": {"errors": []}})),
            "(empty)\n"
        );
        assert_eq!(raw(&json!({"type": "agents_errors", "data": {}})), "");
    }

    #[test]
    fn a_report_with_no_name_is_still_a_record() {
        // The config family answers with a bare record — no envelope, no tag.
        assert_eq!(
            human(&json!({"hub.url": "https://hub.example/", "source": "built-in default"})),
            "hub.url  https://hub.example/\nsource   built-in default\n"
        );
    }

    #[test]
    fn command_print_stays_the_command_it_composed() {
        // Its JSON is the answer: `strata command print … | strata command run`.
        let command = json!({"type": "kv_put", "key": "a", "value": "MQ=="});
        assert_eq!(
            human(&command),
            "{\"key\":\"a\",\"type\":\"kv_put\",\"value\":\"MQ==\"}\n"
        );
        assert_eq!(raw(&command), human(&command));
    }
}
