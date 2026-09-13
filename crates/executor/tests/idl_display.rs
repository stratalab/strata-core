//! The CLI display layer's authoring guard (#3314, S0b).
//!
//! `render:` on a kind and `display:` on a command are CLI-only facts: they
//! are joined into `cli-command-index.json` by `generate-cli`, never into
//! `command-index.json` (the SDKs derive from the wire model, not from the
//! CLI). Every pointer a declaration makes is resolved against the command's
//! generated schema document when the CLI index is resolved, so a stale or
//! mistyped declaration fails `check-cli` rather than rendering `-` forever.
//! These tests drive the resolver over a scratch copy of the real tree with
//! one authored change at a time, so each rule is observed through the call
//! site the gate uses, not through a helper in isolation.
//!
//! Requires `inference` as well as `idl-tooling` for the same reason as the
//! sibling full-catalog IDL tests: the base resolver validates every request
//! fixture against the runtime `Command` enum, and the `inference.*` fixtures
//! only deserialize when those variants are compiled in (#2982).

#![cfg(all(feature = "idl-tooling", feature = "inference"))]

use std::path::{Path, PathBuf};

use strata_executor::cli_metadata::{CliDisplayAs, CliDisplayDecl, CliRenderRule};
use strata_executor::idl_tooling::{
    resolve_cli_index, resolve_default_cli_index, resolve_default_index, resolve_index,
    CliCommandIndex, IdlError,
};

/// The commands the contract hands to a hand-written renderer arm. A
/// declaration is the default; this list only grows by a contract amendment.
const BESPOKE: &[&str] = &[
    "admin.describe",
    "admin.ping",
    "branch.diff",
    "inference.detokenize",
    "inference.embed",
    "inference.generate",
    "inference.models.list",
    "inference.models.local",
    "inference.models.pull",
    "inference.rank",
    "inference.status",
    "inference.tokenize",
    "inference.unload",
];

struct Scratch {
    root: tempfile::TempDir,
}

impl Scratch {
    fn new() -> Self {
        let real = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("executor lives under crates/")
            .to_path_buf();
        let root = tempfile::tempdir().expect("scratch root");
        for relative in ["crates/executor/idl/v1", "crates/executor/tests/fixtures"] {
            copy_tree(&real.join(relative), &root.path().join(relative));
        }
        // The base resolver also scans the enum sources for variant coverage.
        let src = root.path().join("crates/executor/src");
        std::fs::create_dir_all(&src).expect("scratch src dir");
        for file in ["command.rs", "output.rs"] {
            std::fs::copy(real.join("crates/executor/src").join(file), src.join(file))
                .expect("copy enum source");
        }
        Self { root }
    }

    fn idl(&self) -> PathBuf {
        self.root.path().join("crates/executor/idl/v1")
    }

    /// Replaces one command's whole `display:` block (everything between the
    /// `display:` line and the `fixtures:` line) with `block`, which is the
    /// raw YAML at the command's indent and ends with a newline.
    fn set_display(&self, file: &str, id: &str, block: &str) {
        let path = self.idl().join("commands").join(file);
        let text = std::fs::read_to_string(&path).expect("read commands file");
        let entry = text
            .find(&format!("  - id: {id}\n"))
            .expect("command entry exists");
        let start = entry
            + text[entry..]
                .find("    display:")
                .expect("command declares display");
        let end = start
            + text[start..]
                .find("    fixtures:")
                .expect("fixtures line follows display");
        let rewritten = format!("{}{block}{}", &text[..start], &text[end..]);
        std::fs::write(&path, rewritten).expect("write commands file");
    }

    fn replace_in(&self, relative: &str, from: &str, to: &str) {
        let path = self.idl().join(relative);
        let text = std::fs::read_to_string(&path).expect("read authored file");
        assert_eq!(
            text.matches(from).count(),
            1,
            "{relative}: the edit anchor must occur exactly once"
        );
        std::fs::write(&path, text.replacen(from, to, 1)).expect("write authored file");
    }

    fn resolve(&self) -> Result<CliCommandIndex, IdlError> {
        resolve_cli_index(self.root.path())
    }

    /// Resolves the CLI index and returns the authored-IDL rejection.
    fn rejection(&self) -> String {
        match self.resolve() {
            Err(IdlError::Invalid(message)) => message,
            Err(other) => panic!("expected an authored-IDL rejection, got {other}"),
            Ok(_) => panic!("expected the scratch tree to be rejected"),
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).expect("scratch mkdir");
    for entry in std::fs::read_dir(source).expect("scratch read_dir") {
        let entry = entry.expect("scratch entry");
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("scratch file_type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("scratch copy");
        }
    }
}

/// Asserts a rejection names the command and gives the reason, so a guard
/// that fires on the wrong command (or for the wrong reason) is visible.
fn assert_rejects(scratch: &Scratch, command: &str, reason: &str) {
    let message = scratch.rejection();
    assert!(
        message.contains(&format!("`{command}`")) && message.contains(reason),
        "expected a rejection of `{command}` mentioning {reason:?}, got: {message}"
    );
}

fn display_of<'a>(index: &'a CliCommandIndex, id: &str) -> &'a CliDisplayDecl {
    &index
        .commands
        .iter()
        .find(|command| command.id == id)
        .unwrap_or_else(|| panic!("`{id}` is in the CLI index"))
        .display
}

fn render_of(index: &CliCommandIndex, id: &str) -> CliRenderRule {
    index
        .commands
        .iter()
        .find(|command| command.id == id)
        .unwrap_or_else(|| panic!("`{id}` is in the CLI index"))
        .render
}

// ------------------------------------------------------------- the real tree

#[test]
fn every_command_resolves_with_its_kinds_rule_and_only_the_contracted_bespoke_arms() {
    let index = resolve_default_cli_index().expect("the real tree resolves");
    let base = resolve_default_index().expect("the base index resolves");
    assert_eq!(
        index.commands.len(),
        base.commands.len(),
        "the CLI index carries exactly the commands the base index does"
    );
    // The CLI index orders commands by CLI path; the contract lists ids.
    let mut bespoke: Vec<&str> = index
        .commands
        .iter()
        .filter(|command| command.display == CliDisplayDecl::Bespoke)
        .map(|command| command.id.as_str())
        .collect();
    bespoke.sort_unstable();
    assert_eq!(
        bespoke, BESPOKE,
        "the bespoke set is the contract's, no more"
    );
}

#[test]
fn render_follows_the_kind_not_the_command() {
    let index = resolve_default_cli_index().expect("the real tree resolves");
    // Two commands of one kind share the rule; the rule changes with the kind.
    assert_eq!(render_of(&index, "kv.put"), CliRenderRule::MutationAck);
    assert_eq!(render_of(&index, "kv.delete"), CliRenderRule::MutationAck);
    assert_eq!(render_of(&index, "kv.get"), CliRenderRule::Optional);
    assert_eq!(render_of(&index, "json.history"), CliRenderRule::History);
    assert_eq!(render_of(&index, "branch.list"), CliRenderRule::Page);
    assert_eq!(
        render_of(&index, "vector.index.query"),
        CliRenderRule::Search
    );
    assert_eq!(
        render_of(&index, "graph.analytics.wcc"),
        CliRenderRule::Analytics
    );
    assert_eq!(render_of(&index, "event.count"), CliRenderRule::StatusValue);
    assert_eq!(
        render_of(&index, "admin.info"),
        CliRenderRule::StatusSections
    );
    assert_eq!(render_of(&index, "kv.batch_get"), CliRenderRule::Batch);
}

#[test]
fn a_declaration_reaches_the_cli_index_as_authored() {
    let index = resolve_default_cli_index().expect("the real tree resolves");
    let CliDisplayDecl::Declared(put) = display_of(&index, "kv.put") else {
        panic!("kv.put is declared");
    };
    assert_eq!(put.receipt.as_deref(), Some("{verb} {/data/key|bytes}"));
    assert_eq!(put.noun.as_deref(), Some("key"));
    assert_eq!(put.identity, ["/data/key|bytes"]);
    let CliDisplayDecl::Declared(get) = display_of(&index, "kv.get") else {
        panic!("kv.get is declared");
    };
    assert_eq!(get.value.as_deref(), Some("/data/value/value"));
}

#[test]
fn the_sdk_index_carries_no_display_or_render_key() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("idl/v1/generated/command-index.json");
    let text = std::fs::read_to_string(&path).expect("command index reads");
    let index: serde_json::Value = serde_json::from_str(&text).expect("command index parses");
    assert_eq!(
        first_key(&index, ""),
        None,
        "the display layer is CLI-only and must not reach the SDK-facing index"
    );
}

fn first_key(value: &serde_json::Value, at: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => map.iter().find_map(|(key, child)| {
            let here = format!("{at}/{key}");
            if key == "display" || key == "render" {
                Some(here)
            } else {
                first_key(child, &here)
            }
        }),
        serde_json::Value::Array(items) => items
            .iter()
            .enumerate()
            .find_map(|(index, child)| first_key(child, &format!("{at}/{index}"))),
        _ => None,
    }
}

// ------------------------------------------------------------ the two layers

#[test]
fn a_command_the_authored_layer_no_longer_declares_is_rejected() {
    let scratch = Scratch::new();
    // The base index still lists `kv.exists`; the authored file does not.
    let path = scratch.idl().join("commands/kv.yaml");
    let text = std::fs::read_to_string(&path).expect("read kv.yaml");
    let start = text.find("  - id: kv.exists\n").expect("kv.exists entry");
    let end = start + text[start + 1..].find("\n  - id: ").expect("a later entry") + 1;
    std::fs::write(&path, format!("{}{}", &text[..start], &text[end..])).expect("write kv.yaml");

    assert_rejects(&scratch, "kv.exists", "declares no `display:`");
}

#[test]
fn a_command_without_a_display_key_does_not_parse() {
    let scratch = Scratch::new();
    scratch.set_display("event.yaml", "event.count", "");

    match scratch.resolve() {
        Err(IdlError::Yaml { path, source }) => {
            assert!(path.ends_with("commands/event.yaml"), "{}", path.display());
            assert!(source.to_string().contains("display"), "{source}");
        }
        other => panic!("expected a YAML rejection, got {other:?}"),
    }
}

#[test]
fn a_kind_the_authored_layer_no_longer_names_is_rejected() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "kinds.yaml",
        "  - id: read.get\n",
        "  - id: read.get_renamed\n",
    );

    let message = scratch.rejection();
    assert!(
        message.contains("kind `read.get`") && message.contains("declares no `render:`"),
        "{message}"
    );
}

#[test]
fn a_kind_without_a_render_key_does_not_parse() {
    let scratch = Scratch::new();
    scratch.replace_in("kinds.yaml", "    render: optional\n", "");

    match scratch.resolve() {
        Err(IdlError::Yaml { path, source }) => {
            assert!(path.ends_with("kinds.yaml"), "{}", path.display());
            assert!(source.to_string().contains("render"), "{source}");
        }
        other => panic!("expected a YAML rejection, got {other:?}"),
    }
}

#[test]
fn fields_is_only_a_display_word() {
    let scratch = Scratch::new();
    // Inside `display:` it selects wire fields — the real tree passes. At the
    // command level it would define a DTO, which command YAML may not do.
    scratch.replace_in(
        "commands/kv.yaml",
        "  - id: kv.exists\n",
        "  - id: kv.exists\n    fields:\n      - key\n",
    );

    match resolve_index(scratch.root.path()) {
        Err(IdlError::Invalid(message)) => {
            assert!(
                message.contains("defines DTO fields via `fields:`"),
                "{message}"
            );
        }
        other => panic!("expected the DTO guard to fire, got {other:?}"),
    }
}

// ---------------------------------------------------------- shape versus rule

#[test]
fn a_shape_the_rule_does_not_render_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display("kv.yaml", "kv.get", "    display:\n      receipt: got\n");

    assert_rejects(
        &scratch,
        "kv.get",
        "declares a `receipt` display but its kind renders `optional`",
    );
}

#[test]
fn two_shapes_at_once_are_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.get",
        "    display:\n      value: /data/value/value\n      fields:\n        - field: /data/value/version\n",
    );

    assert_rejects(&scratch, "kv.get", "declares no single shape");
}

#[test]
fn a_key_from_another_shape_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.get",
        "    display:\n      value: /data/value/value\n      noun: key\n",
    );

    assert_rejects(
        &scratch,
        "kv.get",
        "`noun` is not part of the `value` shape",
    );
}

#[test]
fn columns_with_fields_is_a_search_only_pairing() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.list",
        "    display:\n      columns:\n        - field: /data/items/*/name\n      fields:\n        - field: /data/next_cursor\n",
    );

    assert_rejects(
        &scratch,
        "branch.list",
        "pairs `columns` with `fields`, which only the `search` rule renders",
    );
}

#[test]
fn a_write_receipt_needs_an_identity() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.put",
        "    display:\n      receipt: \"{verb} {/data/key|bytes}\"\n      noun: key\n",
    );

    assert_rejects(
        &scratch,
        "kv.put",
        "declares a `receipt` without `identity`",
    );
}

#[test]
fn an_action_receipt_has_no_identity() {
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"exported {/data/primitive}\"\n      identity:\n        - /data/primitive\n",
    );

    assert_rejects(
        &scratch,
        "arrow.export",
        "declares `identity` under `status_sections`",
    );
}

#[test]
fn a_map_needs_header_and_sort() {
    let scratch = Scratch::new();
    scratch.set_display(
        "graph.yaml",
        "graph.analytics.wcc",
        "    display:\n      map: /data/components\n      header: COMPONENT\n",
    );

    assert_rejects(
        &scratch,
        "graph.analytics.wcc",
        "`map` requires `header` and `sort`",
    );
}

// --------------------------------------------------------------- the pointers

#[test]
fn a_pointer_to_a_field_the_wire_does_not_carry_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.get",
        "    display:\n      value: /data/value/nope\n",
    );

    let message = scratch.rejection();
    assert!(
        message.contains("`kv.get`") && message.contains("has no field `nope`"),
        "{message}"
    );
    assert!(
        message.contains("version"),
        "the rejection lists the fields the wire does carry: {message}"
    );
}

#[test]
fn a_pointer_outside_the_data_and_request_roots_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "event.yaml",
        "event.count",
        "    display:\n      value: /meta/count\n",
    );

    assert_rejects(
        &scratch,
        "event.count",
        "must start with `/data` or `/request`",
    );
}

#[test]
fn a_request_pointer_resolves_against_the_request_and_names_its_fields() {
    let scratch = Scratch::new();
    scratch.set_display(
        "vector.yaml",
        "vector.collection.delete",
        "    display:\n      receipt: \"deleted collection {/request/name}\"\n      noun: collection\n      identity:\n        - /request/collection\n",
    );

    let message = scratch.rejection();
    assert!(
        message.contains("`vector.collection.delete`")
            && message.contains("`/request` has no field `name`")
            && message.contains("collection"),
        "{message}"
    );
}

#[test]
fn a_value_that_steps_into_every_item_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.get",
        "    display:\n      value: /data/value/*\n",
    );

    assert_rejects(&scratch, "kv.get", "steps into every item");
}

#[test]
fn stepping_into_a_scalar_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "event.yaml",
        "event.count",
        "    display:\n      value: /data/count/low\n",
    );

    assert_rejects(
        &scratch,
        "event.count",
        "is an integer; cannot step into `low`",
    );
}

#[test]
fn a_map_is_stepped_into_with_a_star_not_a_key() {
    // `hub.get_dataset` is the one wire with a map on the data side.
    let scratch = Scratch::new();
    scratch.set_display(
        "hub.yaml",
        "hub.get_dataset",
        "    display:\n      fields:\n        - field: /data/quick_start_snippets/python\n",
    );

    assert_rejects(
        &scratch,
        "hub.get_dataset",
        "`/data/quick_start_snippets` is a map; step into its values with `*`",
    );
}

#[test]
fn an_array_is_stepped_into_with_a_star_or_an_index() {
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"exported to {/data/paths/first}\"\n",
    );
    assert_rejects(
        &scratch,
        "arrow.export",
        "`/data/paths` is an array; step into its items with `*` or an index",
    );

    // Both sides of the boundary: an index other than the real declaration's
    // `0` steps into the same item schema.
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"exported to {/data/paths/1}\"\n",
    );
    scratch
        .resolve()
        .expect("an index steps into the array's items");
}

#[test]
fn a_union_the_layer_cannot_render_is_named_at_its_pointer() {
    // The rank items are a tagged union (`ok` | `error`), the one shape
    // besides a `null` pairing the walk refuses; the command is `bespoke`
    // for exactly that reason, and a declaration must not get past the
    // pointer.
    let scratch = Scratch::new();
    scratch.set_display(
        "inference.yaml",
        "inference.rank",
        "    display:\n      fields:\n        - field: /data/items/0\n",
    );

    assert_rejects(
        &scratch,
        "inference.rank",
        "`/data/items/0` is a union the display layer cannot render; declare `bespoke`",
    );
}

// ------------------------------------------------------------------- receipts

#[test]
fn a_receipt_with_unbalanced_braces_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.put",
        "    display:\n      receipt: \"{verb {/data/key|bytes}\"\n      identity:\n        - /data/key|bytes\n",
    );

    assert_rejects(&scratch, "kv.put", "unbalanced braces");
}

#[test]
fn a_verb_placeholder_needs_an_effect_kind() {
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"{verb} {/data/primitive}\"\n",
    );

    assert_rejects(
        &scratch,
        "arrow.export",
        "`{verb}` needs `/data/effect/kind`",
    );
}

#[test]
fn a_placeholder_needs_a_scalar_or_a_filter() {
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"exported to {/data/paths}\"\n",
    );

    assert_rejects(
        &scratch,
        "arrow.export",
        "`/data/paths` is an array, not a scalar",
    );
}

#[test]
fn an_unknown_filter_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.put",
        "    display:\n      receipt: \"{verb} {/data/key|hex}\"\n      identity:\n        - /data/key|bytes\n",
    );

    assert_rejects(&scratch, "kv.put", "unknown filter `|hex`");
}

#[test]
fn a_filter_needs_its_type() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.put",
        "    display:\n      receipt: \"{verb} {/data/key|len}\"\n      identity:\n        - /data/key|bytes\n",
    );

    assert_rejects(
        &scratch,
        "kv.put",
        "`|len` needs an array, but `/data/key` is a base64 string",
    );
}

#[test]
fn a_plural_filter_without_a_noun_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "space.yaml",
        "space.delete",
        "    display:\n      receipt: \"{verb} space {/data/space} ({/data/deleted_rows|plural})\"\n      noun: space\n      identity:\n        - /data/space\n",
    );

    assert_rejects(&scratch, "space.delete", "`|plural` needs a noun");
}

#[test]
fn a_plural_filter_with_an_empty_noun_is_rejected() {
    // The colon alone is not a noun, even on the integer the filter needs.
    let scratch = Scratch::new();
    scratch.set_display(
        "space.yaml",
        "space.delete",
        "    display:\n      receipt: \"{verb} space {/data/space} ({/data/deleted_rows|plural:})\"\n      noun: space\n      identity:\n        - /data/space\n",
    );

    assert_rejects(&scratch, "space.delete", "`|plural` needs a noun");
}

#[test]
fn a_noun_needs_an_applied_signal() {
    let scratch = Scratch::new();
    scratch.set_display(
        "arrow.yaml",
        "arrow.export",
        "    display:\n      receipt: \"exported {/data/primitive}\"\n      noun: export\n",
    );

    assert_rejects(&scratch, "arrow.export", "`noun` needs an applied signal");
}

#[test]
fn identity_names_values_not_the_verb() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.put",
        "    display:\n      receipt: \"{verb} {/data/key|bytes}\"\n      identity:\n        - verb\n",
    );

    assert_rejects(&scratch, "kv.put", "`identity` names values, not `verb`");
}

// ------------------------------------------------------------------ as / fields

#[test]
fn as_date_needs_an_integer() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.get",
        "    display:\n      fields:\n        - field: /data/name\n          as: date\n",
    );

    assert_rejects(
        &scratch,
        "branch.get",
        "`as: date` needs an integer, but `/data/name` is a string",
    );
}

#[test]
fn as_bytes_needs_a_base64_string() {
    let scratch = Scratch::new();
    scratch.set_display(
        "kv.yaml",
        "kv.count",
        "    display:\n      value: /data\n      as: bytes\n",
    );

    assert_rejects(
        &scratch,
        "kv.count",
        "`as: bytes` needs a base64 string, but `/data` is an integer",
    );
}

#[test]
fn as_table_needs_an_array_of_records() {
    let scratch = Scratch::new();
    scratch.set_display(
        "admin.yaml",
        "admin.info",
        "    display:\n      fields:\n        - field: /data/version\n          as: table\n",
    );

    assert_rejects(
        &scratch,
        "admin.info",
        "`as: table` needs an array of records",
    );
}

#[test]
fn a_tables_columns_are_resolved_from_its_row_schema() {
    let index = resolve_default_cli_index().expect("the real tree resolves");
    let CliDisplayDecl::Declared(preview) = display_of(&index, "branch.preview") else {
        panic!("branch.preview is declared");
    };
    let conflicts = preview
        .fields
        .iter()
        .find(|field| field.field == "/data/conflicts")
        .expect("branch.preview declares its conflicts table");
    assert_eq!(conflicts.as_, Some(CliDisplayAs::Table));
    // One column per row property, named by the schema, none authored.
    let schema: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("idl/v1/generated/schemas/branch.preview.json"),
        )
        .expect("branch.preview schema reads"),
    )
    .expect("branch.preview schema parses");
    let row_properties: Vec<String> = schema["$defs"]["PreviewConflictItem"]["properties"]
        .as_object()
        .expect("the conflict row is a record")
        .keys()
        .cloned()
        .collect();
    let columns: Vec<&str> = conflicts
        .columns
        .iter()
        .map(|column| column.field.as_str())
        .collect();
    let expected: Vec<String> = row_properties
        .iter()
        .map(|name| format!("/data/conflicts/*/{name}"))
        .collect();
    assert_eq!(columns, expected);
    assert!(
        conflicts
            .columns
            .iter()
            .all(|column| column.header.is_none()
                && column.fields.is_empty()
                && column.columns.is_empty()),
        "a resolved column carries only its pointer and presentation"
    );
    // The presentation follows the property's type: a base64 identity (and
    // the nullable base64 values) decode as bytes, an enum is bare text.
    let as_of = |name: &str| {
        conflicts
            .columns
            .iter()
            .find(|column| column.field == format!("/data/conflicts/*/{name}"))
            .expect("column resolved")
            .as_
    };
    assert_eq!(as_of("identity"), Some(CliDisplayAs::Bytes));
    assert_eq!(as_of("source_value"), Some(CliDisplayAs::Bytes));
    assert_eq!(as_of("target_value"), Some(CliDisplayAs::Bytes));
    assert_eq!(as_of("capability"), None);
    assert_eq!(as_of("kind"), None);
}

#[test]
fn a_table_nested_under_a_record_field_is_resolved_too() {
    let index = resolve_default_cli_index().expect("the real tree resolves");
    let CliDisplayDecl::Declared(query) = display_of(&index, "vector.index.query") else {
        panic!("vector.index.query is declared");
    };
    let sources = query
        .fields
        .iter()
        .find(|field| field.field == "/data/diagnostics")
        .expect("the diagnostics block")
        .fields
        .iter()
        .find(|field| field.field == "/data/diagnostics/artifact_sources")
        .expect("the artifact sources table");
    assert_eq!(sources.as_, Some(CliDisplayAs::Table));
    assert!(
        !sources.columns.is_empty(),
        "a table one level down resolves its columns as a top-level one does"
    );
    assert!(sources.columns.iter().all(|column| column
        .field
        .starts_with("/data/diagnostics/artifact_sources/*/")));
}

#[test]
fn authored_table_columns_are_refused() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.preview",
        "    display:\n      fields:\n        - field: /data/conflicts\n          as: table\n          columns:\n            - field: /data/conflicts/*/identity\n",
    );

    assert_rejects(
        &scratch,
        "branch.preview",
        "field `/data/conflicts` carries `columns`; a table's columns are resolved from the schema",
    );
}

#[test]
fn a_table_whose_rows_have_no_fields_is_refused() {
    let scratch = Scratch::new();
    // A row record the schema gives no properties: nothing to make a column of.
    scratch.replace_in(
        "generated/schemas/branch.preview.json",
        "\"$ref\": \"#/$defs/PreviewConflictItem\"",
        "\"type\": \"object\"",
    );

    assert_rejects(
        &scratch,
        "branch.preview",
        "`as: table` on `/data/conflicts` has a row with no fields",
    );
}

#[test]
fn as_list_needs_an_array_of_scalars() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.preview",
        "    display:\n      fields:\n        - field: /data/conflicts\n          as: list\n",
    );

    assert_rejects(
        &scratch,
        "branch.preview",
        "`as: list` needs an array of scalars",
    );
}

#[test]
fn an_empty_header_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.get",
        "    display:\n      fields:\n        - field: /data/name\n          header: \"  \"\n",
    );

    assert_rejects(&scratch, "branch.get", "has an empty `header`");
}

#[test]
fn a_repeated_field_is_rejected() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.get",
        "    display:\n      fields:\n        - field: /data/name\n        - field: /data/name\n",
    );

    assert_rejects(&scratch, "branch.get", "`fields` repeats `/data/name`");
}

#[test]
fn nested_fields_need_a_record() {
    let scratch = Scratch::new();
    scratch.set_display(
        "admin.yaml",
        "admin.info",
        "    display:\n      fields:\n        - field: /data/version\n          fields:\n            - field: /data/version/major\n",
    );

    assert_rejects(
        &scratch,
        "admin.info",
        "carries `fields` but is a string, not a record",
    );
}

#[test]
fn nested_fields_stay_under_their_parent() {
    let scratch = Scratch::new();
    scratch.set_display(
        "admin.yaml",
        "admin.info",
        "    display:\n      fields:\n        - field: /data/memory_budget\n          fields:\n            - field: /data/version\n",
    );

    assert_rejects(
        &scratch,
        "admin.info",
        "nested field `/data/version` is not under `/data/memory_budget`",
    );
}

#[test]
fn nested_fields_do_not_pair_with_as() {
    let scratch = Scratch::new();
    scratch.set_display(
        "admin.yaml",
        "admin.info",
        "    display:\n      fields:\n        - field: /data/memory_budget\n          as: json\n          fields:\n            - field: /data/memory_budget/source\n",
    );

    assert_rejects(&scratch, "admin.info", "pairs `as` with a nested `fields`");
}

// -------------------------------------------------------------------- columns

#[test]
fn a_column_must_step_into_a_row_array() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.list",
        "    display:\n      columns:\n        - field: /data/items\n",
    );

    assert_rejects(
        &scratch,
        "branch.list",
        "does not step into a row array with `/*`",
    );
}

#[test]
fn one_table_has_one_row_source() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.list",
        "    display:\n      columns:\n        - field: /data/items/*/name\n        - field: /data/*/name\n",
    );

    assert_rejects(
        &scratch,
        "branch.list",
        "columns read rows from `/data/items/*` and `/data/*`",
    );
}

#[test]
fn rows_come_from_an_array_not_a_map() {
    let scratch = Scratch::new();
    scratch.set_display(
        "hub.yaml",
        "hub.get_dataset",
        "    display:\n      columns:\n        - field: /data/quick_start_snippets/*\n",
    );

    assert_rejects(
        &scratch,
        "hub.get_dataset",
        "rows must come from an array, but `/data/quick_start_snippets` is a map",
    );
}

#[test]
fn a_column_does_not_carry_nested_fields() {
    let scratch = Scratch::new();
    scratch.set_display(
        "branch.yaml",
        "branch.list",
        "    display:\n      columns:\n        - field: /data/items/*/parent\n          fields:\n            - field: /data/items/*/parent/name\n",
    );

    assert_rejects(
        &scratch,
        "branch.list",
        "carries `fields`; only a record field may",
    );
}

#[test]
fn batch_rows_are_the_items_even_when_the_wire_grows_another_array() {
    let scratch = Scratch::new();
    // Plant a second array on the batch response so the rule, not the
    // walker, is what refuses the declaration.
    let path = scratch.idl().join("generated/schemas/kv.batch_get.json");
    let text = std::fs::read_to_string(&path).expect("schema reads");
    let mut document: serde_json::Value = serde_json::from_str(&text).expect("schema parses");
    let properties = document
        .pointer_mut("/$defs/BatchResult2/properties")
        .and_then(serde_json::Value::as_object_mut)
        .expect("batch result properties");
    properties.insert(
        "extra".to_owned(),
        serde_json::json!({
            "type": "array",
            "items": { "type": "object", "properties": { "x": { "type": "string" } } }
        }),
    );
    std::fs::write(&path, document.to_string()).expect("schema writes");
    scratch.set_display(
        "kv.yaml",
        "kv.batch_get",
        "    display:\n      columns:\n        - field: /data/extra/*/x\n",
    );

    assert_rejects(
        &scratch,
        "kv.batch_get",
        "batch columns read rows from `/data/items/*`, not `/data/extra/*`",
    );
}

// ------------------------------------------------------------------------ map

#[test]
fn a_map_needs_an_object_keyed_by_node() {
    let scratch = Scratch::new();
    scratch.set_display(
        "graph.yaml",
        "graph.analytics.wcc",
        "    display:\n      map: /data\n      header: COMPONENT\n      sort: key\n",
    );

    assert_rejects(
        &scratch,
        "graph.analytics.wcc",
        "`map` needs an object keyed by node, but `/data` is a record",
    );
}

/// #3358 F11: `as: date` used to accept any integer, so a counter could be
/// declared a date and pass. Every one of these is an integer in the schema
/// and none is microseconds since the epoch; a date column over one of them
/// renders 1970 forever (#3112).
#[test]
fn a_counter_cannot_be_declared_a_date() {
    // The review's own probe: swap the KV history date column's pointer for the
    // logical clock beside it, whose description says it "is never a calendar
    // date".
    let scratch = Scratch::new();
    scratch.replace_in(
        "commands/kv.yaml",
        "        - field: /data/items/*/committed_at\n          as: date\n",
        "        - field: /data/items/*/timestamp\n          as: date\n",
    );
    assert_rejects(&scratch, "kv.history", "not a registered wall-clock field");

    // A branch's `created_at` and `generation` are counters too (R3, #3112),
    // and neither is anywhere near an instant.
    for counter in ["created_at", "generation"] {
        let scratch = Scratch::new();
        scratch.replace_in(
            "commands/branch.yaml",
            &format!("        - field: /data/{counter}\n"),
            &format!("        - field: /data/{counter}\n          as: date\n"),
        );
        assert_rejects(&scratch, "branch.get", "not a registered wall-clock field");
    }
}

/// Direction control: the sites that really are instants keep working, and the
/// allowlist is keyed on the command, not on the field name — an event's
/// `timestamp` is a wall-clock instant even though a KV history row's is not.
#[test]
fn a_registered_wall_clock_field_is_still_a_date() {
    let index = Scratch::new().resolve().expect("the real tree resolves");
    for (command, pointer) in [
        ("kv.history", "/data/items/*/committed_at"),
        ("event.list", "/data/items/*/event/timestamp"),
    ] {
        let declared = format!("{:?}", display_of(&index, command));
        assert!(
            declared.contains(pointer),
            "{command} no longer declares {pointer}: {declared}"
        );
    }
}

/// #3358 F12: R2 says a new wire field must fail `check-cli` until someone
/// decides where it belongs. The guard used to validate only the fields a
/// declaration selected, so a field added to a record payload was silently
/// never shown — the declaration stayed valid because nothing asked what it
/// left out.
#[test]
fn a_new_field_on_a_record_payload_needs_a_decision() {
    let scratch = Scratch::new();
    scratch.replace_in(
        "generated/schemas/admin.info.json",
        "\"branch_count\":",
        "\"new_important_fact\": {\"type\": \"string\"}, \"branch_count\":",
    );
    assert_rejects(&scratch, "admin.info", "does not say what happens to");
    assert!(
        scratch.rejection().contains("new_important_fact"),
        "the rejection should name the field: {}",
        scratch.rejection()
    );
}

/// Direction control: the check is the record shapes' business. A page or a
/// batch decides what to show by its rule, and its envelope — `items`,
/// `cursor`, `has_more`, `applied` — is not a decision an author makes per
/// command.
#[test]
fn a_page_envelope_is_not_an_undecided_field() {
    // Each of these carries a `fields` block under a page or batch rule, and
    // each has envelope fields (`items`, `cursor`, `has_more`, `applied`) that
    // no declaration names. The real tree resolving at all is the assertion:
    // an inventory applied to them would reject it.
    let index = Scratch::new().resolve().expect("the real tree resolves");
    for command in ["kv.list", "kv.batch_put", "event.list"] {
        // Panics if the command is missing, so this pins that the commands
        // this test speaks for still exist.
        let _ = display_of(&index, command);
    }
}
