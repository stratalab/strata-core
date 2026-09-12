//! Runtime CLI metadata conformance tests.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde_json::Value;
use strata_executor::cli_metadata::{
    CliCommandCatalog, CliMetadataError, EMBEDDED_CLI_COMMAND_INDEX_JSON,
};

/// The IDL command index the embedded CLI index was generated from — the file
/// the index's own `source` names, read from the repo. Tests that need "every
/// command" or "every command in a family" take the set from here rather than
/// restating a count: a literal count was bumped by every command-adding PR,
/// and when two such PRs bumped it identically git merged them without a
/// conflict and the count came out one short.
fn idl_command_index(catalog: &CliCommandCatalog) -> Value {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives two levels below the repo root");
    let path = repo_root.join(&catalog.index().source.path);
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("IDL command index {} reads: {error}", path.display()));
    serde_json::from_str(&json).expect("IDL command index is JSON")
}

/// Ids of the IDL commands `keep` accepts, by the command's IDL record.
fn idl_command_ids(index: &Value, keep: impl Fn(&Value) -> bool) -> BTreeSet<&str> {
    index["commands"]
        .as_array()
        .expect("IDL command index lists commands")
        .iter()
        .filter(|command| keep(command))
        .map(|command| command["id"].as_str().expect("IDL command has an id"))
        .collect()
}

#[test]
fn embedded_cli_metadata_loads_without_generator_feature() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");

    assert_eq!(catalog.index().schema_version, "strata.cli.v1");
    assert_eq!(
        catalog.index().generator_version,
        "strata-executor-cli-idl.3"
    );

    // Every command the IDL declares, and only those, in the embedded index
    // and in its count.
    let source = idl_command_index(&catalog);
    let expected_ids = idl_command_ids(&source, |_| true);
    assert!(!expected_ids.is_empty(), "the IDL declares commands");
    let embedded_ids = catalog
        .commands()
        .iter()
        .map(|command| command.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(embedded_ids, expected_ids);
    assert_eq!(catalog.index().command_count, expected_ids.len());

    // One family group per family the IDL uses.
    let expected_families = source["commands"]
        .as_array()
        .expect("IDL command index lists commands")
        .iter()
        .map(|command| {
            command["family"]
                .as_str()
                .expect("IDL command has a family")
        })
        .collect::<BTreeSet<_>>();
    let embedded_families = catalog
        .families()
        .iter()
        .map(|family| family.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(embedded_families, expected_families);
}

#[test]
fn command_lookup_supports_ids_and_cli_paths() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");

    let kv_put = catalog.command("kv.put").expect("kv.put resolves by id");
    assert_eq!(kv_put.path_display, "kv put");
    assert_eq!(kv_put.access, "write");
    assert_eq!(kv_put.commit, "commits_on_success");

    assert_eq!(
        catalog
            .command("kv put")
            .expect("kv put resolves by path")
            .id,
        "kv.put"
    );
    assert_eq!(
        catalog
            .command("kv batch-get")
            .expect("hyphenated batch path resolves")
            .id,
        "kv.batch_get"
    );
    assert_eq!(
        catalog
            .command_by_path(["vector", "collection", "create"])
            .expect("nested vector path resolves")
            .id,
        "vector.collection.create"
    );
    assert_eq!(
        catalog
            .command_by_path(["vector", "delete-by-filter"])
            .expect("hyphenated vector path resolves")
            .id,
        "vector.delete_by_filter"
    );
    assert_eq!(
        catalog
            .command_by_path_display("vector query")
            .expect("vector query resolves")
            .id,
        "vector.query"
    );
    assert!(catalog.command("kv missing").is_none());
}

#[test]
fn catalog_entries_publish_the_executable_wire_name() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");

    // The dotted id (`kv.list`) and the CLI path (`kv list`) are neither the
    // executable literal a tool serializes into `{"type": ...}`. The entry must
    // carry that wire name so a catalog reader can construct a call.
    let list = catalog.command("kv.list").expect("kv.list resolves");
    assert_eq!(list.wire, "kv_list");
    assert_ne!(list.wire, list.id);
    assert_ne!(list.wire, list.path_display);

    let scan = catalog.command("json.scan").expect("json.scan resolves");
    assert_eq!(scan.wire, "json_scan");

    // Every entry's wire name is the snake_case of its `Command::` variant, is
    // never dotted, and never carries the `Command::` prefix.
    for command in catalog.commands() {
        let variant = command
            .input
            .strip_prefix("Command::")
            .unwrap_or_else(|| panic!("{} input is not a Command variant", command.id));
        let expected = pascal_to_snake(variant);
        assert_eq!(
            command.wire, expected,
            "{} publishes the wrong wire name",
            command.id
        );
        assert!(
            command
                .wire
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "{} wire name `{}` is not snake_case",
            command.id,
            command.wire
        );
    }
}

#[test]
fn command_by_wire_resolves_every_entry_and_only_by_its_wire() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");
    for command in catalog.commands() {
        let found = catalog
            .command_by_wire(&command.wire)
            .unwrap_or_else(|| panic!("{} resolves by wire `{}`", command.id, command.wire));
        assert_eq!(found.id, command.id);
    }
    // The wire is the only key this lookup answers to: not the dotted id, not
    // the CLI path, and not a wire with stray whitespace.
    assert!(catalog.command_by_wire("kv.put").is_none());
    assert!(catalog.command_by_wire("kv put").is_none());
    assert!(catalog.command_by_wire(" kv_put").is_none());
    assert_eq!(
        catalog
            .command_by_wire("kv_put")
            .map(|command| command.id.as_str()),
        Some("kv.put")
    );
}

#[test]
fn runtime_validation_rejects_duplicate_wire_names() {
    // Two entries on one wire would make the wire lookup answer for the wrong
    // command. The clone keeps `input` in step so the per-command wire/input
    // check passes and only the duplicate is left to catch.
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    let first_wire = value["commands"][0]["wire"].clone();
    let first_input = value["commands"][0]["input"].clone();
    value["commands"][1]["wire"] = first_wire;
    value["commands"][1]["input"] = first_input;
    assert_parse_invalid(&value, "duplicate wire name");
}

fn pascal_to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[test]
fn command_listing_is_grouped_and_sorted() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");

    let families = catalog.families();
    assert_eq!(families[0].id, "admin");
    assert_eq!(families[1].id, "arrow");
    assert_eq!(families[2].id, "branch");
    assert_eq!(families[3].id, "event");
    assert_eq!(families[4].id, "graph");
    assert_eq!(families[5].id, "hub");
    assert_eq!(families[6].id, "inference");
    assert_eq!(families[7].id, "json");
    assert_eq!(families[8].id, "kv");
    assert_eq!(families[9].id, "space");
    assert_eq!(families[10].id, "vector");

    // The family listing carries exactly the IDL's KV commands, and the
    // family group agrees with it.
    let kv_commands = catalog
        .commands_for_family("kv")
        .expect("KV family commands exist");
    let source = idl_command_index(&catalog);
    let expected_kv = idl_command_ids(&source, |command| command["family"] == "kv");
    assert_eq!(
        kv_commands
            .iter()
            .map(|command| command.id.as_str())
            .collect::<BTreeSet<_>>(),
        expected_kv
    );
    assert_eq!(
        kv_commands
            .iter()
            .map(|command| command.id.as_str())
            .collect::<Vec<_>>(),
        families[8]
            .commands
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );

    let mut sorted_paths = catalog
        .commands()
        .iter()
        .map(|command| command.path.clone())
        .collect::<Vec<_>>();
    let actual_paths = sorted_paths.clone();
    sorted_paths.sort();
    assert_eq!(actual_paths, sorted_paths);
    assert!(catalog.commands_for_family("__absent__").is_none());
}

#[test]
fn unknown_command_suggestions_are_deterministic() {
    let catalog = CliCommandCatalog::embedded().expect("embedded CLI metadata loads");

    let suggestions = catalog.suggestions("kv ptu", 3);
    assert!(suggestions.command_ids.contains(&"kv.put".to_owned()));
    assert!(suggestions.paths.contains(&"kv put".to_owned()));
    assert_eq!(suggestions.command_ids.len(), 3);
    assert_eq!(suggestions.paths.len(), 3);
    assert_eq!(suggestions, catalog.suggestions("kv ptu", 3));

    let empty = catalog.suggestions("kv ptu", 0);
    assert!(empty.command_ids.is_empty());
    assert!(empty.paths.is_empty());
}

#[test]
fn runtime_validation_rejects_malformed_metadata() {
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["command_count"] = Value::from(999);
    assert_parse_invalid(&value, "command_count");

    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["schema_version"] = Value::from("wrong.schema");
    assert_parse_invalid(&value, "unsupported CLI command index schema");
}

#[test]
fn runtime_validation_rejects_bad_source_versions() {
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["source"]["schema_version"] = Value::from("wrong.source.schema");
    assert_parse_invalid(&value, "unsupported source command index schema");

    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["source"]["generator_version"] = Value::from("wrong.source.generator");
    assert_parse_invalid(&value, "unsupported source command index generator");
}

#[test]
fn runtime_validation_rejects_command_identity_mismatch() {
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["commands"][0]["family"] = Value::from("vector");
    assert_parse_invalid(&value, "identity does not match family/op");
}

#[test]
fn runtime_validation_rejects_wire_name_that_disagrees_with_input() {
    // A wire name that is not the snake_case of the command's `input` variant
    // would send a catalog reader to the wrong (or a nonexistent) command.
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["commands"][0]["wire"] = Value::from("not_the_wire_name");
    assert_parse_invalid(&value, "wire name");

    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["commands"][0]["wire"] = Value::from("");
    assert_parse_invalid(&value, "wire");
}

#[test]
fn runtime_validation_rejects_bad_family_group_membership() {
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["families"][0]["commands"][0] = Value::from("vector.query");
    assert_parse_invalid(&value, "owned by family");

    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    value["families"][0]["commands"]
        .as_array_mut()
        .expect("family commands is array")
        .pop()
        .expect("family has command to remove");
    value["families"][0]["command_count"] = Value::from(
        value["families"][0]["commands"]
            .as_array()
            .expect("family commands is array")
            .len(),
    );
    assert_parse_invalid(&value, "family groups do not cover every command");
}

/// Mutates the embedded index entry for `id` and hands the whole index back.
fn embedded_index_with(id: &str, edit: impl FnOnce(&mut Value)) -> Value {
    let mut value: Value =
        serde_json::from_str(EMBEDDED_CLI_COMMAND_INDEX_JSON).expect("metadata parses as JSON");
    let entry = value["commands"]
        .as_array_mut()
        .expect("commands is array")
        .iter_mut()
        .find(|command| command["id"] == id)
        .unwrap_or_else(|| panic!("embedded index carries `{id}`"));
    edit(entry);
    value
}

#[test]
fn runtime_validation_rejects_an_encoding_that_disagrees_with_the_render_rule() {
    // An `optional` reader needs to know whether a miss is `{found: false}`
    // or a null `data`; a stable command that resolved neither is a generator
    // fault the runtime must not paper over.
    let value = embedded_index_with("kv.get", |entry| entry["encoding"] = Value::Null);
    assert_parse_invalid(&value, "resolved to no encoding");

    // A history encoding on an optional reader would misread every hit.
    let value = embedded_index_with("kv.get", |entry| entry["encoding"] = Value::from("items"));
    assert_parse_invalid(&value, "wire encoding is `items`");

    // A rule that reads no encoding must not carry one.
    let value = embedded_index_with("kv.put", |entry| {
        entry["encoding"] = Value::from("found_value");
    });
    assert_parse_invalid(&value, "which reads none");

    // Direction control: a transitional optional wire may carry no encoding
    // (its shape is ledgered, not resolved), and the checked-in index parses.
    let value = embedded_index_with("kv.get", |entry| {
        entry["encoding"] = Value::Null;
        entry["wire_status"] = Value::from("transitional");
    });
    let json = serde_json::to_string(&value).expect("metadata serializes");
    let catalog =
        CliCommandCatalog::parse(&json).expect("a transitional wire may lack an encoding");
    assert_eq!(
        catalog.command("kv.get").expect("kv.get resolves").encoding,
        None
    );
}

#[test]
fn runtime_metadata_source_does_not_use_authoring_inputs() {
    let crate_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    // The runtime module and its display submodule together; the display
    // vocabulary is authored in YAML but the runtime only ever sees the
    // resolved JSON index.
    let mut source =
        fs::read_to_string(crate_root.join("src/cli_metadata.rs")).expect("source reads");
    source.push_str(
        &fs::read_to_string(crate_root.join("src/cli_metadata/display.rs"))
            .expect("display source reads"),
    );
    for forbidden in [
        "serde_yaml",
        "frontmatter",
        "commands/*.yaml",
        "prose/**/*.md",
        "idl_tooling",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime metadata source must not reference `{forbidden}`"
        );
    }

    let manifest = fs::read_to_string(crate_root.join("Cargo.toml")).expect("manifest reads");
    let default_feature_section = manifest
        .split("[features]")
        .nth(1)
        .expect("features section exists")
        .split("[dependencies]")
        .next()
        .expect("dependencies section follows features");
    assert!(!default_feature_section.contains("idl-tooling = [\"default\""));
}

fn assert_invalid(error: CliMetadataError, expected: &str) {
    match error {
        CliMetadataError::Invalid(message) => assert!(
            message.contains(expected),
            "expected `{message}` to contain `{expected}`"
        ),
        CliMetadataError::Json(error) => {
            panic!("expected validation error, got JSON error {error}")
        }
    }
}

fn assert_parse_invalid(value: &Value, expected: &str) {
    let json = serde_json::to_string(&value).expect("metadata serializes");
    let error = CliCommandCatalog::parse(&json).expect_err("invalid metadata fails");
    assert_invalid(error, expected);
}
