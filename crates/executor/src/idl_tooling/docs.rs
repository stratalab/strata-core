//! Reference documentation emission.
//!
//! Renders one Markdown reference page per command from the resolved IDL —
//! the curated prose body, a parameters table derived from the request JSON
//! Schema, the response model, and the registered error set — plus a
//! per-family index page and a root `llms.txt`. Every input already exists in
//! the IDL (`prose/commands`, `generated/schemas`, `errors.yaml`); this module
//! only composes it, so the reference cannot drift from the wire. `check-docs`
//! is the drift guard, mirroring `check` / `check-cli`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use super::examples::{self, Example};
use super::response_model::{self, ResponseFamily};
use super::{invalid, relative_to, resolve_index, schemas, IdlError, ResolvedCommand, Result};

/// Canonical site origin for absolute links in the machine layer, matching the
/// schema `$id` base and the `/e/<code>` error routes.
const SITE_BASE: &str = "https://stratadb.org";

/// Rationale for `.expect()` on `write!` into a `String`: the `fmt::Write`
/// impl for `String` never returns `Err`, so a failure here is impossible.
const INFALLIBLE: &str = "writing to a String is infallible";

/// Builds the `source:` provenance stamp. Tracks the workspace release version
/// (stable across commits, so the drift guard only re-blesses on a version
/// bump) rather than a git rev, matching the site's `source` frontmatter.
fn source_stamp() -> String {
    format!("strata-core@{}", env!("CARGO_PKG_VERSION"))
}

fn docs_dir_path(repo_root: &Path) -> PathBuf {
    repo_root
        .join(super::IDL_DIR)
        .join("generated")
        .join("docs")
}

/// Generates the full reference tree under `generated/docs/`, pruning any file
/// that no longer corresponds to a resolved command or family.
pub(super) fn generate_docs(repo_root: &Path) -> Result<()> {
    let files = render_all(repo_root)?;
    let docs_dir = docs_dir_path(repo_root);
    fs::create_dir_all(&docs_dir).map_err(|source| IdlError::Write {
        path: docs_dir.clone(),
        source,
    })?;

    for (path, content) in &files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| IdlError::Write {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(path, content).map_err(|source| IdlError::Write {
            path: path.clone(),
            source,
        })?;
    }

    let expected: BTreeSet<&PathBuf> = files.keys().collect();
    for path in walk_files(&docs_dir)? {
        if !expected.contains(&path) {
            fs::remove_file(&path).map_err(|source| IdlError::Write {
                path: path.clone(),
                source,
            })?;
        }
    }
    Ok(())
}

/// Checks whether the generated reference tree is fresh: every rendered page
/// matches on disk and no stale file remains.
pub(super) fn check_docs(repo_root: &Path) -> Result<()> {
    let files = render_all(repo_root)?;
    for (path, expected) in &files {
        let actual = match fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(IdlError::DocsStale { path: path.clone() })
            }
            Err(source) => {
                return Err(IdlError::Read {
                    path: path.clone(),
                    source,
                })
            }
        };
        if &actual != expected {
            return Err(IdlError::DocsStale { path: path.clone() });
        }
    }

    let expected: BTreeSet<&PathBuf> = files.keys().collect();
    let docs_dir = docs_dir_path(repo_root);
    for path in walk_files(&docs_dir)? {
        if !expected.contains(&path) {
            return Err(invalid(format!(
                "unexpected file in generated docs dir: {}",
                relative_to(&path, repo_root)
            )));
        }
    }
    Ok(())
}

/// Renders every output file, keyed by absolute path (deterministic order).
fn render_all(repo_root: &Path) -> Result<BTreeMap<PathBuf, String>> {
    let index = resolve_index(repo_root)?;
    let documents = schemas::schema_documents(&index)?;
    let example_specs = examples::load_examples(repo_root)?;
    examples::validate_examples(repo_root, &index, &documents, &example_specs)?;
    let arg_spec = examples::load_arg_spec(repo_root)?;
    let docs_dir = docs_dir_path(repo_root);
    let stamp = source_stamp();

    let by_id: BTreeMap<&str, &ResolvedCommand> = index
        .commands
        .iter()
        .map(|command| (command.id.as_str(), command))
        .collect();
    let mut families: BTreeMap<&str, Vec<&ResolvedCommand>> = BTreeMap::new();
    let mut files = BTreeMap::new();
    for entry in &index.commands {
        families.entry(&entry.family).or_default().push(entry);
        let schema = documents
            .get(&entry.id)
            .ok_or_else(|| invalid(format!("no schema document for `{}`", entry.id)))?;
        files.insert(
            docs_dir.join(page_rel_path(entry)?),
            render_page(
                entry,
                schema,
                &stamp,
                &by_id,
                &documents,
                example_specs.get(&entry.id),
                &arg_spec,
            )?,
        );
    }

    for (family, commands) in &families {
        files.insert(
            docs_dir.join(family).join("index.md"),
            render_family_index(family, commands, &stamp),
        );
    }
    files.insert(docs_dir.join("llms.txt"), render_llms_txt(&families));
    Ok(files)
}

/// Maps a command's `/docs/<family>/<op_path>` URL to a file under the docs dir.
fn page_rel_path(entry: &ResolvedCommand) -> Result<PathBuf> {
    let rel = entry.docs.strip_prefix("/docs/").ok_or_else(|| {
        invalid(format!(
            "command `{}` has docs path `{}` outside /docs/",
            entry.id, entry.docs
        ))
    })?;
    Ok(PathBuf::from(format!("{rel}.md")))
}

#[allow(clippy::too_many_arguments)]
fn render_page(
    entry: &ResolvedCommand,
    schema: &Value,
    stamp: &str,
    by_id: &BTreeMap<&str, &ResolvedCommand>,
    schemas: &BTreeMap<String, Value>,
    example: Option<&Example>,
    arg_spec: &examples::CliArgSpec,
) -> Result<String> {
    let mut out = String::new();
    out.push_str("---\n");
    writeln!(out, "title: {}", yaml_quote(&entry.title)).expect(INFALLIBLE);
    writeln!(out, "description: {}", yaml_quote(&entry.summary)).expect(INFALLIBLE);
    writeln!(out, "source: {stamp}").expect(INFALLIBLE);
    writeln!(out, "section: {}", entry.family).expect(INFALLIBLE);
    out.push_str("---\n\n");

    out.push_str(entry.description.trim());
    out.push_str("\n\n");

    if let Some(example) = example {
        out.push_str(&examples::render_section(by_id, schemas, example, arg_spec));
    }

    out.push_str(&render_parameters(schema));
    out.push_str(&render_returns(entry, schema)?);
    out.push_str(&render_errors(entry));
    out.push_str(&render_invocation(entry, schema));

    let mut result = out.trim_end().to_owned();
    result.push('\n');
    Ok(result)
}

/// One parameter row for the request table.
struct ParamRow {
    name: String,
    type_label: String,
    required: bool,
    description: String,
}

impl ParamRow {
    fn from_property(name: &str, prop: &Value, required: bool) -> Self {
        Self {
            name: name.to_owned(),
            type_label: type_label(prop),
            required,
            description: prop
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }
}

fn render_parameters(schema: &Value) -> String {
    let defs = schema
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let request = schema.get("request");
    let required = string_set(request.and_then(|r| r.get("required")));

    let mut rows = Vec::new();
    let mut has_scope = false;
    if let Some(props) = request
        .and_then(|r| r.get("properties"))
        .and_then(Value::as_object)
    {
        for (name, prop) in props {
            match name.as_str() {
                // The wire discriminator is not a user-facing parameter.
                "type" => continue,
                // Branch/space are the shared scope, surfaced as a note.
                "branch" | "space" => {
                    has_scope = true;
                    continue;
                }
                // The inference `request` wrapper carries the real knobs one
                // level down; inline the referenced request DTO so callers see
                // `temperature`, `tools`, ... instead of one opaque `request`.
                "request" => {
                    if let Some(object) = wrapper_object(prop, &defs) {
                        expand_object(object, &mut rows);
                        continue;
                    }
                }
                _ => {}
            }
            rows.push(ParamRow::from_property(
                name,
                prop,
                required.contains(name.as_str()),
            ));
        }
    }

    let mut out = String::from("## Parameters\n\n");
    if rows.is_empty() {
        out.push_str("_No parameters._\n\n");
    } else {
        out.push_str("| Name | Type | Required | Description |\n");
        out.push_str("|---|---|---|---|\n");
        for row in &rows {
            writeln!(
                out,
                "| `{}` | `{}` | {} | {} |",
                row.name,
                row.type_label,
                if row.required { "yes" } else { "no" },
                table_cell(&row.description),
            )
            .expect(INFALLIBLE);
        }
        out.push('\n');
    }
    if has_scope {
        out.push_str(
            "Plus the optional scope: `branch` and `space` (default to the session branch \
             and the `\"default\"` space).\n\n",
        );
    }
    out
}

/// Returns the referenced object schema for an inference-style wrapper property
/// (`{ "$ref": "#/$defs/<Object>" }`), or `None` when the property is a scalar
/// newtype (e.g. `Bytes`) that must stay a single typed row.
fn wrapper_object<'a>(prop: &Value, defs: &'a Map<String, Value>) -> Option<&'a Value> {
    let name = prop
        .get("$ref")
        .and_then(Value::as_str)?
        .strip_prefix("#/$defs/")?;
    let target = defs.get(name)?;
    let is_object = target.get("type").and_then(Value::as_str) == Some("object");
    (is_object && target.get("properties").is_some()).then_some(target)
}

/// Inlines a wrapper object's properties as parameter rows (one level deep).
fn expand_object(object: &Value, rows: &mut Vec<ParamRow>) {
    let required = string_set(object.get("required"));
    if let Some(props) = object.get("properties").and_then(Value::as_object) {
        for (name, prop) in props {
            rows.push(ParamRow::from_property(
                name,
                prop,
                required.contains(name.as_str()),
            ));
        }
    }
}

/// The declared response model, and — for a `transitional` command — what the
/// wire carries today, derived from the schema by the same classifier the
/// response-model guard runs, so the page and the ledger cannot disagree.
fn render_returns(entry: &ResolvedCommand, schema: &Value) -> Result<String> {
    let family = ResponseFamily::from_declaration(&entry.id, &entry.response_model)?;
    let mut out = String::from("## Returns\n\n");
    write!(out, "`{}`", entry.response_model).expect(INFALLIBLE);
    // `Maybe<T>` models a miss as absence, never an error — call it out.
    if matches!(family, ResponseFamily::Maybe | ResponseFamily::MaybeVec) {
        out.push_str(" — a miss returns nothing rather than raising.");
    } else {
        out.push('.');
    }
    out.push_str("\n\n");
    if entry.wire_status == "transitional" {
        let shape = response_model::classify(&entry.id, schema)?;
        if response_model::accepts(family, &shape) {
            out.push_str(
                "**Transitional wire:** this command's response shape is not yet frozen and may \
                 change.\n\n",
            );
        } else {
            writeln!(
                out,
                "**Transitional wire:** the response currently carries {} rather than {}; the \
                 declaration is the target shape and the wire is scheduled to be normalised.\n",
                shape.describe(),
                family.describe()
            )
            .expect(INFALLIBLE);
        }
    }
    Ok(out)
}

fn render_errors(entry: &ResolvedCommand) -> String {
    let mut out = String::from("## Errors\n\n");
    if entry.errors.is_empty() {
        out.push_str("_None specific to this command._\n\n");
    } else {
        for error in &entry.errors {
            writeln!(out, "- [`{}`]({})", error.code, error.docs).expect(INFALLIBLE);
        }
        out.push('\n');
    }
    out
}

fn render_invocation(entry: &ResolvedCommand, schema: &Value) -> String {
    let wire = schema
        .pointer("/request/properties/type/const")
        .and_then(Value::as_str)
        .unwrap_or(&entry.id);
    let mut out = String::from("## Invocation\n\n");
    if entry.cli.surface == "verb" {
        writeln!(out, "- CLI: `strata {}`", entry.cli.path.join(" ")).expect(INFALLIBLE);
    } else {
        out.push_str("- CLI: via `strata command run` (no dedicated verb)\n");
    }
    writeln!(out, "- Wire type: `{wire}`").expect(INFALLIBLE);
    out.push('\n');
    out
}

fn render_family_index(family: &str, commands: &[&ResolvedCommand], stamp: &str) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    writeln!(out, "title: {}", yaml_quote(&format!("{family} commands"))).expect(INFALLIBLE);
    writeln!(
        out,
        "description: {}",
        yaml_quote(&format!("Command reference for the {family} family."))
    )
    .expect(INFALLIBLE);
    writeln!(out, "source: {stamp}").expect(INFALLIBLE);
    writeln!(out, "section: {family}").expect(INFALLIBLE);
    out.push_str("---\n\n");
    writeln!(out, "# `{family}` — command reference").expect(INFALLIBLE);
    out.push('\n');
    out.push_str("| Command | Summary |\n|---|---|\n");
    for entry in commands {
        writeln!(
            out,
            "| [{}]({}) | {} |",
            table_cell(&entry.title),
            entry.docs,
            table_cell(&entry.summary),
        )
        .expect(INFALLIBLE);
    }
    out
}

fn render_llms_txt(families: &BTreeMap<&str, Vec<&ResolvedCommand>>) -> String {
    let mut out = String::from("# StrataDB — command reference\n\n");
    out.push_str(
        "> Generated from the Strata IDL. Every reference page below is also available as \
         Markdown by appending `.md` to its URL.\n\n",
    );
    for (family, commands) in families {
        writeln!(out, "## {family}").expect(INFALLIBLE);
        out.push('\n');
        for entry in commands {
            writeln!(
                out,
                "- [{}]({SITE_BASE}{}.md): {}",
                entry.title, entry.docs, entry.summary
            )
            .expect(INFALLIBLE);
        }
        out.push('\n');
    }
    let mut result = out.trim_end().to_owned();
    result.push('\n');
    result
}

/// Renders a JSON-Schema property as a short human type label:
/// `$ref` → def name, arrays → `T[]`, nullable unions → the non-null arm.
fn type_label(prop: &Value) -> String {
    if let Some(reference) = prop.get("$ref").and_then(Value::as_str) {
        return ref_name(reference);
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(arm) = prop.get(key).and_then(Value::as_array) {
            let parts: Vec<String> = arm
                .iter()
                .filter(|value| value.get("type").and_then(Value::as_str) != Some("null"))
                .map(type_label)
                .collect();
            if !parts.is_empty() {
                return parts.join(" | ");
            }
        }
    }
    if type_includes(prop, "array") {
        return match prop.get("items") {
            Some(items) => format!("{}[]", type_label(items)),
            None => "array".to_owned(),
        };
    }
    match prop.get("type") {
        Some(Value::String(name)) => name.clone(),
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(Value::as_str)
            .find(|name| *name != "null")
            .unwrap_or("any")
            .to_owned(),
        _ => "any".to_owned(),
    }
}

fn type_includes(prop: &Value, wanted: &str) -> bool {
    match prop.get("type") {
        Some(Value::String(name)) => name == wanted,
        Some(Value::Array(names)) => names.iter().any(|value| value.as_str() == Some(wanted)),
        _ => false,
    }
}

fn ref_name(reference: &str) -> String {
    reference.rsplit('/').next().unwrap_or(reference).to_owned()
}

fn string_set(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Collapses a description to a single table-cell-safe line.
fn table_cell(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.replace('|', "\\|")
}

/// Emits a double-quoted YAML scalar so frontmatter parses regardless of the
/// characters a title or summary contains (colons, quotes, ...).
fn yaml_quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Recursively lists every file under `dir` (sorted), or nothing if absent.
fn walk_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current).map_err(|source| IdlError::Read {
            path: current.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| IdlError::Read {
                path: current.clone(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| IdlError::Read {
                path: path.clone(),
                source,
            })?;
            if file_type.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn type_label_renders_scalars_refs_arrays_and_nullable_unions() {
        assert_eq!(type_label(&json!({"type": "string"})), "string");
        // A nullable scalar shows the non-null arm (optionality is a column).
        assert_eq!(type_label(&json!({"type": ["integer", "null"]})), "integer");
        assert_eq!(type_label(&json!({"$ref": "#/$defs/Bytes"})), "Bytes");
        assert_eq!(
            type_label(
                &json!({"type": ["array", "null"], "items": {"$ref": "#/$defs/ChatMessage"}})
            ),
            "ChatMessage[]"
        );
        assert_eq!(
            type_label(&json!({"type": "array", "items": {"type": "string"}})),
            "string[]"
        );
        assert_eq!(
            type_label(&json!({"anyOf": [{"$ref": "#/$defs/ResponseFormat"}, {"type": "null"}]})),
            "ResponseFormat"
        );
    }

    #[test]
    fn wrapper_object_expands_request_dtos_but_not_scalar_newtypes() {
        let mut defs = Map::new();
        defs.insert(
            "ChatRequest".into(),
            json!({"type": "object", "properties": {"temperature": {"type": "number"}}}),
        );
        // Bytes is a base64 string newtype — an object-with-properties it is not.
        defs.insert(
            "Bytes".into(),
            json!({"type": "string", "contentEncoding": "base64"}),
        );

        assert!(wrapper_object(&json!({"$ref": "#/$defs/ChatRequest"}), &defs).is_some());
        assert!(wrapper_object(&json!({"$ref": "#/$defs/Bytes"}), &defs).is_none());
        assert!(wrapper_object(&json!({"type": "string"}), &defs).is_none());
    }

    #[test]
    fn table_cell_flattens_newlines_and_escapes_pipes() {
        assert_eq!(table_cell("one\n  two   three"), "one two three");
        assert_eq!(table_cell("a | b"), "a \\| b");
    }

    #[test]
    fn yaml_quote_escapes_quotes_and_backslashes() {
        assert_eq!(yaml_quote("Put KV value"), "\"Put KV value\"");
        assert_eq!(yaml_quote("say \"hi\""), "\"say \\\"hi\\\"\"");
    }
}
