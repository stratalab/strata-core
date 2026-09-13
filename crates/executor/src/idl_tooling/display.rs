//! Authoring-time guard for the CLI display layer (#3314, S0b).
//!
//! `render:` lives on each kind in `kinds.yaml`; `display:` on each command
//! in `commands/*.yaml`. `generate-cli` joins both into
//! `cli-command-index.json` and resolves every pointer a declaration makes
//! against the command's generated schema document, so a declaration that
//! names a field the wire does not carry fails `check-cli` instead of
//! rendering `-` forever. Nothing here runs at render time, and nothing here
//! reaches `command-index.json` — `assert_no_display_keys` holds that line,
//! because the Python SDK derives from the wire model, never from the CLI.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde_json::{Map, Value};

use super::{
    invalid, read_yaml, schemas_dir_path, CommandsFileSource, IdlError, KindsSource, Result,
    COMMAND_FILES,
};
use crate::cli_metadata::{
    validate_display_shape, CliDisplay, CliDisplayAs, CliDisplayDecl, CliDisplayField,
    CliDisplayShape, CliRenderRule, ReceiptFilter, ReceiptPlaceholder, ReceiptTemplate,
};

/// The display layer as authored: one render rule per kind, one declaration
/// per command.
pub(super) struct DisplayLayer {
    pub(super) render_by_kind: BTreeMap<String, CliRenderRule>,
    pub(super) display_by_command: BTreeMap<String, CliDisplayDecl>,
}

/// Re-reads the authored layer. This is the one place `generate-cli` reads
/// authored YAML: the layer is deliberately absent from `command-index.json`.
pub(super) fn load_display_layer(idl_root: &Path) -> Result<DisplayLayer> {
    let kinds: KindsSource = read_yaml(&idl_root.join("kinds.yaml"))?;
    let render_by_kind = kinds
        .kinds
        .into_iter()
        .map(|kind| (kind.id, kind.render))
        .collect();
    let mut display_by_command = BTreeMap::new();
    for file_name in COMMAND_FILES {
        let path = idl_root.join("commands").join(file_name);
        let file: CommandsFileSource = read_yaml(&path)?;
        for command in file.commands {
            if display_by_command
                .insert(command.id.clone(), command.display)
                .is_some()
            {
                return Err(invalid(format!("duplicate command id `{}`", command.id)));
            }
        }
    }
    Ok(DisplayLayer {
        render_by_kind,
        display_by_command,
    })
}

/// Reads `generated/schemas/<id>.json`, the document every pointer resolves
/// against.
pub(super) fn read_schema_document(repo_root: &Path, command_id: &str) -> Result<Value> {
    let path = schemas_dir_path(repo_root).join(format!("{command_id}.json"));
    let text = fs::read_to_string(&path).map_err(|source| IdlError::Read {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|source| IdlError::Json { path, source })
}

/// Checks one command's declaration — shape against the kind's rule, then
/// every pointer, placeholder, filter and `as` against the schema document —
/// and returns it resolved: every `as: table` field carries the columns its
/// row schema implies. The resolved form is what `generate-cli` writes, so
/// the renderer reads a table's columns from the index and never guesses
/// them from a row.
pub(super) fn resolve_display(
    command_id: &str,
    rule: CliRenderRule,
    decl: &CliDisplayDecl,
    document: &Value,
) -> Result<CliDisplayDecl> {
    validate_display_shape(command_id, rule, decl).map_err(invalid)?;
    let CliDisplayDecl::Declared(display) = decl else {
        return Ok(decl.clone());
    };
    let schema = SchemaDoc {
        document,
        command_id,
    };
    let outcome = match display.shape() {
        Ok(CliDisplayShape::Receipt) => check_receipt(&schema, display),
        Ok(CliDisplayShape::Value) => check_value(&schema, display),
        Ok(CliDisplayShape::Fields) => check_fields(&schema, &display.fields, None)
            .and_then(|()| check_every_payload_field_was_decided(&schema, &display.fields)),
        Ok(CliDisplayShape::Columns) => check_columns(&schema, display, rule),
        Ok(CliDisplayShape::Map) => check_map(&schema, display),
        Err(reason) => Err(reason),
    };
    // Only a fields block renders a nested table; a column's `as: table`
    // stays a compact-JSON cell, so its columns are never needed.
    let resolved = outcome.and_then(|()| {
        let mut resolved = display.clone();
        resolve_tables(&schema, &mut resolved.fields)?;
        Ok(CliDisplayDecl::Declared(resolved))
    });
    resolved.map_err(|reason| invalid(format!("command `{command_id}` display {reason}")))
}

/// Fails when a serialized command index carries a `display` or `render`
/// key anywhere: those are CLI-only facts and the SDKs read this file.
pub(super) fn assert_no_display_keys(text: &str, path: &Path) -> Result<()> {
    let value: Value = serde_json::from_str(text).map_err(|source| IdlError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    match find_key(&value, &["display", "render"], "") {
        Some(at) => Err(invalid(format!(
            "{} carries `{at}`; the display layer is CLI-only and must not reach the SDKs",
            path.display()
        ))),
        None => Ok(()),
    }
}

fn find_key(value: &Value, keys: &[&str], at: &str) -> Option<String> {
    match value {
        Value::Object(map) => map.iter().find_map(|(key, child)| {
            let here = format!("{at}/{key}");
            if keys.contains(&key.as_str()) {
                Some(here)
            } else {
                find_key(child, keys, &here)
            }
        }),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .find_map(|(index, child)| find_key(child, keys, &format!("{at}/{index}"))),
        _ => None,
    }
}

// ------------------------------------------------------------------ schema walk

/// What a resolved pointer lands on, after `$ref`, nullability and enum
/// encodings are stripped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaType {
    Object,
    Map,
    Array,
    Any,
    Text,
    Base64,
    Integer,
    Number,
    Boolean,
    Enum,
}

impl SchemaType {
    const fn is_scalar(self) -> bool {
        matches!(
            self,
            Self::Text | Self::Base64 | Self::Integer | Self::Number | Self::Boolean | Self::Enum
        )
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Object => "record",
            Self::Map => "map",
            Self::Array => "array",
            Self::Any => "untyped value",
            Self::Text => "string",
            Self::Base64 => "base64 string",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Enum => "enum",
        }
    }
}

struct Node<'a> {
    schema: &'a Value,
    ty: SchemaType,
}

struct SchemaDoc<'a> {
    document: &'a Value,
    /// The command being checked. A pointer alone cannot say whether an
    /// integer is an instant: `timestamp` is a wall-clock time on an event and
    /// a position on the commit timeline in a KV history row.
    command_id: &'a str,
}

impl<'a> SchemaDoc<'a> {
    /// Resolves `/data/...` against the response payload and `/request/...`
    /// against the request. `*` steps into array items or map values; a
    /// decimal index steps into array items.
    fn resolve(&self, pointer: &str) -> std::result::Result<Node<'a>, String> {
        let rest = pointer
            .strip_prefix('/')
            .ok_or_else(|| format!("pointer `{pointer}` must start with `/data` or `/request`"))?;
        let mut segments = rest.split('/');
        // `split` always yields a first piece (possibly empty); the default
        // is unreachable and an empty root falls to the `_` arm below.
        let root = segments.next().unwrap_or_default();
        let start = match root {
            "data" => self.document.pointer("/response/properties/data"),
            "request" => self.document.pointer("/request"),
            _ => {
                return Err(format!(
                    "pointer `{pointer}` must start with `/data` or `/request`"
                ))
            }
        }
        .ok_or_else(|| format!("schema document has no `{root}` root"))?;
        let mut walked = format!("/{root}");
        let mut node = self
            .normalize(start)
            .map_err(|reason| format!("`{walked}` {reason}"))?;
        for segment in segments {
            if segment.is_empty() {
                return Err(format!("pointer `{pointer}` has an empty segment"));
            }
            let child =
                Self::step(&node, segment).map_err(|reason| format!("`{walked}` {reason}"))?;
            walked.push('/');
            walked.push_str(segment);
            node = self
                .normalize(child)
                .map_err(|reason| format!("`{walked}` {reason}"))?;
        }
        Ok(node)
    }

    /// The type of an array's items or a map's values.
    fn element(&self, at: &str, node: &Node<'a>) -> std::result::Result<Node<'a>, String> {
        let child = Self::step(node, "*").map_err(|reason| format!("`{at}` {reason}"))?;
        self.normalize(child)
            .map_err(|reason| format!("`{at}/*` {reason}"))
    }

    fn normalize(&self, mut schema: &'a Value) -> std::result::Result<Node<'a>, String> {
        loop {
            match schema {
                Value::Bool(true) => {
                    return Ok(Node {
                        schema,
                        ty: SchemaType::Any,
                    })
                }
                Value::Object(map) => {
                    if let Some(reference) = map.get("$ref") {
                        schema = self.deref(reference)?;
                    } else if let Some(Value::Array(variants)) = map.get("anyOf") {
                        schema = nullable_variant(variants).ok_or_else(|| {
                            "is a union the display layer cannot render; declare `bespoke`"
                                .to_owned()
                        })?;
                    } else {
                        return classify(map).map(|ty| Node { schema, ty });
                    }
                }
                _ => return Err("is not a schema object".to_owned()),
            }
        }
    }

    fn deref(&self, reference: &Value) -> std::result::Result<&'a Value, String> {
        let name = reference
            .as_str()
            .and_then(|reference| reference.strip_prefix("#/$defs/"))
            .ok_or_else(|| format!("has a non-local `$ref` {reference}"))?;
        self.document
            .pointer(&format!("/$defs/{name}"))
            .ok_or_else(|| format!("refers to a missing definition `{name}`"))
    }

    fn step(node: &Node<'a>, segment: &str) -> std::result::Result<&'a Value, String> {
        match node.ty {
            SchemaType::Object => {
                let properties = node.schema.get("properties").and_then(Value::as_object);
                properties
                    .and_then(|properties| properties.get(segment))
                    .ok_or_else(|| {
                        let names = properties.map_or_else(String::new, |properties| {
                            properties.keys().cloned().collect::<Vec<_>>().join(", ")
                        });
                        format!("has no field `{segment}` (fields: {names})")
                    })
            }
            SchemaType::Map if segment == "*" => node
                .schema
                .get("additionalProperties")
                .ok_or_else(|| "has untyped values".to_owned()),
            SchemaType::Map => Err("is a map; step into its values with `*`".to_owned()),
            SchemaType::Array if segment == "*" || segment.bytes().all(|b| b.is_ascii_digit()) => {
                node.schema
                    .get("items")
                    .ok_or_else(|| "has untyped items".to_owned())
            }
            SchemaType::Array => {
                Err("is an array; step into its items with `*` or an index".to_owned())
            }
            ty => Err(format!("is {}; cannot step into `{segment}`", article(ty))),
        }
    }
}

/// `anyOf: [T, {type: null}]` — the one union the layer understands.
fn nullable_variant(variants: &[Value]) -> Option<&Value> {
    let is_null = |variant: &Value| variant.get("type").and_then(Value::as_str) == Some("null");
    match variants {
        [inner, null] if is_null(null) && !is_null(inner) => Some(inner),
        [null, inner] if is_null(null) && !is_null(inner) => Some(inner),
        _ => None,
    }
}

fn classify(map: &Map<String, Value>) -> std::result::Result<SchemaType, String> {
    let ty = match map.get("type") {
        Some(Value::String(ty)) => Some(ty.as_str()),
        Some(Value::Array(types)) => {
            let non_null: Vec<&str> = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|ty| *ty != "null")
                .collect();
            match non_null.as_slice() {
                [one] => Some(*one),
                _ => return Err("has a multi-typed schema; declare `bespoke`".to_owned()),
            }
        }
        Some(_) => return Err("has a malformed `type`".to_owned()),
        None => None,
    };
    match ty {
        Some("object") => {
            let is_map = !map.contains_key("properties")
                && map
                    .get("additionalProperties")
                    .is_some_and(|values| *values != Value::Bool(false));
            Ok(if is_map {
                SchemaType::Map
            } else {
                SchemaType::Object
            })
        }
        Some("array") => Ok(SchemaType::Array),
        Some("string") => {
            let base64 = map.get("contentEncoding").and_then(Value::as_str) == Some("base64");
            Ok(if base64 {
                SchemaType::Base64
            } else {
                SchemaType::Text
            })
        }
        Some("integer") => Ok(SchemaType::Integer),
        Some("number") => Ok(SchemaType::Number),
        Some("boolean") => Ok(SchemaType::Boolean),
        Some(other) => Err(format!("has unsupported type `{other}`")),
        None => match map.get("oneOf") {
            Some(Value::Array(variants))
                if variants
                    .iter()
                    .all(|variant| variant.get("const").is_some()) =>
            {
                Ok(SchemaType::Enum)
            }
            Some(_) => {
                Err("is a union the display layer cannot render; declare `bespoke`".to_owned())
            }
            None if map.contains_key("allOf") => Err(
                "is a composition the display layer cannot render; declare `bespoke`".to_owned(),
            ),
            None => Ok(SchemaType::Any),
        },
    }
}

fn article(ty: SchemaType) -> String {
    let name = ty.name();
    let an = matches!(
        name.as_bytes().first(),
        Some(b'a' | b'e' | b'i' | b'o' | b'u')
    );
    format!("{} {name}", if an { "an" } else { "a" })
}

// ------------------------------------------------------------------ checks

type Check = std::result::Result<(), String>;

fn check_receipt(schema: &SchemaDoc<'_>, display: &CliDisplay) -> Check {
    let Some(receipt) = display.receipt.as_deref() else {
        return Err("`receipt` is missing".to_owned());
    };
    for placeholder in ReceiptTemplate::parse(receipt)?.placeholders() {
        check_placeholder(schema, placeholder)?;
    }
    let mut seen = BTreeSet::new();
    for entry in &display.identity {
        if entry == "verb" {
            return Err("`identity` names values, not `verb`".to_owned());
        }
        if !seen.insert(entry.as_str()) {
            return Err(format!("`identity` repeats `{entry}`"));
        }
        check_placeholder(schema, &ReceiptPlaceholder::parse(entry)?)?;
    }
    if let Some(noun) = display.noun.as_deref() {
        if noun.trim().is_empty() {
            return Err("`noun` is empty".to_owned());
        }
        let applied = schema
            .resolve("/data/effect/applied")
            .or_else(|_| schema.resolve("/data"))
            .map(|node| node.ty == SchemaType::Boolean);
        if applied != Ok(true) {
            return Err("`noun` needs an applied signal (`/data/effect/applied`, or a bare boolean `/data`); without one a miss cannot be told from a hit".to_owned());
        }
    }
    Ok(())
}

/// Resolves one parsed placeholder against the schema: `{verb}` needs the
/// effect kind, a pointer must reach a value its filter (or no filter) fits.
fn check_placeholder(schema: &SchemaDoc<'_>, placeholder: &ReceiptPlaceholder) -> Check {
    let value = match placeholder {
        ReceiptPlaceholder::Verb => {
            let kind = schema.resolve("/data/effect/kind").map_err(|_| {
                "`{verb}` needs `/data/effect/kind`, which this response does not carry".to_owned()
            })?;
            if !matches!(kind.ty, SchemaType::Enum | SchemaType::Text) {
                return Err("`{verb}` needs `/data/effect/kind` to be an enum".to_owned());
            }
            return Ok(());
        }
        ReceiptPlaceholder::Value(value) => value,
    };
    let pointer = value.pointer.as_str();
    let node = schema.resolve(pointer)?;
    match &value.filter {
        None if node.ty.is_scalar() => Ok(()),
        None => Err(format!(
            "`{pointer}` is {}, not a scalar; use `|len` for an array or point at a field",
            article(node.ty)
        )),
        Some(filter) => check_filter(pointer, &node, filter),
    }
}

fn check_filter(pointer: &str, node: &Node<'_>, filter: &ReceiptFilter) -> Check {
    let needs = match filter {
        ReceiptFilter::Plural(_) | ReceiptFilter::Size => SchemaType::Integer,
        ReceiptFilter::Bytes => SchemaType::Base64,
        ReceiptFilter::Len => SchemaType::Array,
    };
    if node.ty == needs {
        Ok(())
    } else {
        Err(format!(
            "`|{}` needs {}, but `{pointer}` is {}",
            filter.name(),
            article(needs),
            article(node.ty)
        ))
    }
}

fn check_value(schema: &SchemaDoc<'_>, display: &CliDisplay) -> Check {
    let Some(pointer) = display.value.as_deref() else {
        return Err("`value` is missing".to_owned());
    };
    if pointer.contains('*') {
        return Err(format!(
            "`value` names one value; `{pointer}` steps into every item — declare `columns` for rows"
        ));
    }
    let node = schema.resolve(pointer)?;
    match display.as_ {
        Some(as_) => check_as(schema, pointer, &node, as_),
        None => Ok(()),
    }
}

/// `as` fit: what each presentation needs the schema to say.
/// Payload fields a record command deliberately does not show, as
/// (command, field). Each is a decision someone made once; a field that is
/// not here and not in `fields` fails `check-cli` (#3358 F12).
const DELIBERATELY_UNSHOWN: &[(&str, &str)] = &[
    // True whenever a reader can see the answer at all: neither command can be
    // reported on a handle that is not open.
    ("admin.info", "open"),
    ("admin.metrics", "open"),
    // A fact about the open that produced this handle, not about the database
    // it describes. `config get` reports it, where it is the subject.
    ("admin.info", "created"),
    // Machine specifics of the current host, not facts about the database:
    // `ipc status` reports whether it is hosting, whether this process owns
    // the socket, and how many clients are attached. The per-client list, the
    // owning pid and the socket path vary by machine and by run.
    ("admin.ipc_status", "clients"),
    ("admin.ipc_status", "owner_pid"),
    ("admin.ipc_status", "socket_path"),
    // The graph records carry a logical clock, not an instant (R3), so it has
    // no reading a person can use; the version is what identifies the write.
    ("graph.edge.get", "timestamp"),
    ("graph.node.get", "timestamp"),
    ("graph.ontology.get", "timestamp"),
    ("graph.ontology.summary", "timestamp"),
    ("graph.meta", "created_timestamp"),
    ("graph.meta", "updated_timestamp"),
    // A node's binding names the row the node projects from, which `graph
    // bindings` reports as its subject.
    ("graph.node.get", "binding"),
    // See the note in the PR for #3358 F12: an edge's properties are shown by
    // no command today, unlike a node's. Recorded as it stands rather than
    // changed inside a guard slice.
    ("graph.edge.get", "properties"),
    // The catalogue record `hub get-dataset` curates down to the fifteen facts
    // a reader acts on. The rest are the manifest's own bookkeeping, long-form
    // prose, or embedded documents that do not belong in a terminal record;
    // `--json` carries all of them.
    ("hub.get_dataset", "capability_registry_version"),
    ("hub.get_dataset", "citation"),
    ("hub.get_dataset", "format_version"),
    ("hub.get_dataset", "frontmatter_extras"),
    ("hub.get_dataset", "manifest_hash"),
    ("hub.get_dataset", "provenance"),
    ("hub.get_dataset", "quick_start_snippets"),
    ("hub.get_dataset", "readme"),
    ("hub.get_dataset", "sample_preview"),
    ("hub.get_dataset", "schema"),
    ("hub.get_dataset", "strata_features"),
    ("hub.get_dataset", "summary_excerpt"),
    // The engine's own identifiers for a branch and its lineage: a reader
    // works with the branch by name, and `branch diff`/`preview` report the
    // lineage where it is the subject. The contract names this curation
    // ("`branch get` drops `branch_id` and `state_revision`"); until now it
    // was recorded only in prose.
    ("branch.get", "branch_id"),
    ("branch.get", "merge_parent"),
    ("branch.get", "state_revision"),
    // The versioned wrapper's own logical clock, beside the commit version
    // these commands do show. It is a position on the commit timeline, not an
    // instant (R3); for an event the instant a reader wants is the event's own
    // `timestamp` inside the record, which `event get` shows as a date.
    ("event.get", "timestamp"),
    ("vector.get", "timestamp"),
    // Facts about fetching a model, not about the capability being reported:
    // `inference capability` answers what this build can run.
    ("inference.capability", "pull_spec"),
    ("inference.capability", "size_bytes"),
];

/// Where a wall-clock date may be declared, as (command, pointer).
///
/// R3 wanted this to be a type — "a declaration marking one `as: date` is a
/// `check` error because the schema type for a logical clock is not the
/// wall-clock newtype" — with an explicit fallback: "if it is not [expressible],
/// the field allowlist is the guard". It is not expressible. `schemars`
/// flattens core's `Timestamp` newtype to a bare `uint64`, and the executor's
/// own response DTOs carry instants as plain `u64`, so nothing in a generated
/// schema separates an instant from any other counter (#3358 F11).
///
/// Naming the sites is what tells them apart, because the field names do not:
/// an event's `timestamp` is a real instant, while a KV history row's
/// `timestamp` is a position on the commit timeline whose own description says
/// it "is never a calendar date".
const WALL_CLOCK_SITES: &[(&str, &str)] = &[
    ("admin.remote", "/data/origin/fetched_at_micros"),
    ("event.get", "/data/value/event/timestamp"),
    ("event.list", "/data/items/*/event/timestamp"),
    ("event.range", "/data/items/*/event/timestamp"),
    ("event.range_time", "/data/items/*/event/timestamp"),
    ("json.history", "/data/*/committed_at"),
    ("kv.history", "/data/items/*/committed_at"),
    ("vector.history", "/data/items/*/committed_at"),
];

fn is_wall_clock(command_id: &str, pointer: &str) -> bool {
    WALL_CLOCK_SITES
        .iter()
        .any(|(command, field)| *command == command_id && *field == pointer)
}

fn check_as(schema: &SchemaDoc<'_>, pointer: &str, node: &Node<'_>, as_: CliDisplayAs) -> Check {
    let (fits, needs) = match as_ {
        CliDisplayAs::Bytes => (node.ty == SchemaType::Base64, "a base64 string"),
        CliDisplayAs::Json => (
            matches!(
                node.ty,
                SchemaType::Any | SchemaType::Object | SchemaType::Map | SchemaType::Array
            ),
            "a structured or untyped value",
        ),
        CliDisplayAs::Date => {
            if node.ty == SchemaType::Integer && !is_wall_clock(schema.command_id, pointer) {
                return Err(format!(
                    "`as: date` on `{pointer}` of `{}` is not a registered wall-clock field. \
                     Every integer is a candidate date and most are not one — a commit \
                     version or a position on the commit timeline rendered as a date reads \
                     as 1970 (#3112). If this field really is microseconds since the epoch, \
                     add it to `WALL_CLOCK_SITES`.",
                    schema.command_id
                ));
            }
            (node.ty == SchemaType::Integer, "an integer")
        }
        CliDisplayAs::Size => (node.ty == SchemaType::Integer, "an integer"),
        CliDisplayAs::Float => (node.ty == SchemaType::Number, "a number"),
        CliDisplayAs::List => (
            node.ty == SchemaType::Array && schema.element(pointer, node)?.ty.is_scalar(),
            "an array of scalars",
        ),
        CliDisplayAs::Table => (
            node.ty == SchemaType::Array && schema.element(pointer, node)?.ty == SchemaType::Object,
            "an array of records",
        ),
    };
    if fits {
        Ok(())
    } else {
        Err(format!(
            "`as: {}` needs {needs}, but `{pointer}` is {}",
            as_.as_str(),
            article(node.ty)
        ))
    }
}

fn check_fields(schema: &SchemaDoc<'_>, fields: &[CliDisplayField], parent: Option<&str>) -> Check {
    let mut seen = BTreeSet::new();
    for field in fields {
        let pointer = field.field.as_str();
        if pointer.contains('*') {
            return Err(format!(
                "field `{pointer}` steps into every item with `*`; declare `columns` for rows or `as: table` on the array"
            ));
        }
        if let Some(parent) = parent {
            if !pointer.starts_with(&format!("{parent}/")) {
                return Err(format!("nested field `{pointer}` is not under `{parent}`"));
            }
        }
        if !seen.insert(pointer) {
            return Err(format!("`fields` repeats `{pointer}`"));
        }
        check_authored_columns(pointer, field)?;
        check_header(pointer, field.header.as_deref())?;
        let node = schema.resolve(pointer)?;
        if let Some(as_) = field.as_ {
            check_as(schema, pointer, &node, as_)?;
        }
        if !field.fields.is_empty() {
            if field.as_.is_some() {
                return Err(format!(
                    "field `{pointer}` pairs `as` with a nested `fields`"
                ));
            }
            if node.ty != SchemaType::Object {
                return Err(format!(
                    "field `{pointer}` carries `fields` but is {}, not a record",
                    article(node.ty)
                ));
            }
            check_fields(schema, &field.fields, Some(pointer))?;
        }
    }
    Ok(())
}

/// The record every declared field belongs to: their common parent, when they
/// share one. A declaration whose fields sit at different depths describes no
/// single record, and is left to the other checks.
fn declared_record_root(fields: &[CliDisplayField]) -> Option<String> {
    fields
        .iter()
        .filter_map(|field| field.field.rsplit_once('/').map(|(parent, _)| parent))
        .min_by_key(|parent| parent.matches('/').count())
        .map(ToOwned::to_owned)
}

/// The record field a declared pointer decides about: the segment just below
/// the record root. Reaching into `/data/parent/name` is a decision about
/// `parent`.
fn decided_field<'a>(pointer: &'a str, root: &str) -> Option<&'a str> {
    pointer
        .strip_prefix(root)?
        .strip_prefix('/')?
        .split('/')
        .next()
}

/// Every field of a record payload is either shown or deliberately not shown.
///
/// R2 says a new wire field must fail `check-cli` until someone decides where
/// it belongs. The guard validated only the fields a declaration *selected*,
/// so a field added to a payload was silently never shown — the declaration
/// stayed valid because nothing asked about what it omitted (#3358 F12).
///
/// Scoped to the record shapes, where a hidden fact actually misleads: these
/// are the commands a person reads as a description of something (`info`,
/// `describe`, `branch get`). A page or a batch decides what to show by its
/// rule, not by this list.
fn check_every_payload_field_was_decided(
    schema: &SchemaDoc<'_>,
    fields: &[CliDisplayField],
) -> Check {
    // The record is wherever the declaration points, not `/data`: an
    // `optional` command's payload sits under the `Maybe` envelope at
    // `/data/value`, and `found` beside it is the envelope's business.
    let Some(root) = declared_record_root(fields) else {
        return Ok(());
    };
    let shown: BTreeSet<&str> = fields
        .iter()
        .filter_map(|field| decided_field(&field.field, &root))
        .collect();
    let Ok(record) = schema.resolve(&root) else {
        return Ok(());
    };
    let Some(properties) = record.schema.get("properties").and_then(Value::as_object) else {
        return Ok(());
    };
    let undecided: Vec<&String> = properties
        .keys()
        .filter(|name| {
            !shown.contains(name.as_str())
                && !DELIBERATELY_UNSHOWN
                    .iter()
                    .any(|(command, field)| *command == schema.command_id && field == name)
        })
        .collect();
    if undecided.is_empty() {
        return Ok(());
    }
    Err(format!(
        "`{}` does not say what happens to {undecided:?}. A record command shows every \
         field of its payload or records the omission: add it to `fields`, or to \
         `DELIBERATELY_UNSHOWN` with the reason it is not worth a reader's attention.",
        schema.command_id
    ))
}

/// A table's columns are resolved, never authored: a declaration that
/// spells them out would restate the schema, and drift from it.
fn check_authored_columns(pointer: &str, field: &CliDisplayField) -> Check {
    if field.columns.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "field `{pointer}` carries `columns`; a table's columns are resolved from the schema"
        ))
    }
}

fn check_header(pointer: &str, header: Option<&str>) -> Check {
    match header {
        Some(header) if header.trim().is_empty() => {
            Err(format!("field `{pointer}` has an empty `header`"))
        }
        _ => Ok(()),
    }
}

fn check_columns(schema: &SchemaDoc<'_>, display: &CliDisplay, rule: CliRenderRule) -> Check {
    let mut rows: Option<&str> = None;
    let mut seen = BTreeSet::new();
    for column in &display.columns {
        let pointer = column.field.as_str();
        let Some((prefix, _)) = pointer.split_once("/*") else {
            return Err(format!(
                "column `{pointer}` does not step into a row array with `/*`"
            ));
        };
        if pointer.matches('*').count() != 1 {
            return Err(format!("column `{pointer}` steps into more than one array"));
        }
        let source = &pointer[..prefix.len() + 2];
        match rows {
            None => rows = Some(source),
            Some(existing) if existing != source => {
                return Err(format!(
                "columns read rows from `{existing}` and `{source}`; one table has one row source"
            ))
            }
            Some(_) => {}
        }
        if !seen.insert(pointer) {
            return Err(format!("`columns` repeats `{pointer}`"));
        }
        if !column.fields.is_empty() {
            return Err(format!(
                "column `{pointer}` carries `fields`; only a record field may"
            ));
        }
        check_authored_columns(pointer, column)?;
        check_header(pointer, column.header.as_deref())?;
        let node = schema.resolve(pointer)?;
        if let Some(as_) = column.as_ {
            check_as(schema, pointer, &node, as_)?;
        }
    }
    let Some(source) = rows else {
        return Err("`columns` is empty".to_owned());
    };
    let array = &source[..source.len() - 2];
    if schema.resolve(array)?.ty != SchemaType::Array {
        return Err(format!(
            "rows must come from an array, but `{array}` is a map"
        ));
    }
    if rule == CliRenderRule::Batch && source != "/data/items/*" {
        return Err(format!(
            "batch columns read rows from `/data/items/*`, not `{source}`"
        ));
    }
    check_fields(schema, &display.fields, None)
}

/// Fills the `columns` of every `as: table` field from its row schema, at
/// every depth a nested `fields` reaches.
fn resolve_tables(schema: &SchemaDoc<'_>, fields: &mut [CliDisplayField]) -> Check {
    for field in fields {
        if field.as_ == Some(CliDisplayAs::Table) {
            field.columns = table_columns(schema, &field.field)?;
        }
        resolve_tables(schema, &mut field.fields)?;
    }
    Ok(())
}

/// One column per property of the row record, in the order `--json` prints
/// them (alphabetical), each with the presentation its type implies: base64
/// as `bytes` (a preview conflict's identity decodes to text, Q20),
/// structured values as compact `json`, scalars as they are.
fn table_columns(
    schema: &SchemaDoc<'_>,
    pointer: &str,
) -> std::result::Result<Vec<CliDisplayField>, String> {
    let rows = schema.resolve(pointer)?;
    let row = schema.element(pointer, &rows)?;
    let names: BTreeSet<&str> = row
        .schema
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(Map::keys)
        .map(String::as_str)
        .collect();
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let field = format!("{pointer}/*/{name}");
        let node = schema.resolve(&field)?;
        columns.push(CliDisplayField {
            field,
            header: None,
            as_: implied_as(node.ty),
            fields: Vec::new(),
            columns: Vec::new(),
        });
    }
    if columns.is_empty() {
        return Err(format!(
            "`as: table` on `{pointer}` has a row with no fields"
        ));
    }
    Ok(columns)
}

/// The presentation a resolved column takes from its type, when the scalar
/// text would not do.
const fn implied_as(ty: SchemaType) -> Option<CliDisplayAs> {
    match ty {
        SchemaType::Base64 => Some(CliDisplayAs::Bytes),
        SchemaType::Object | SchemaType::Map | SchemaType::Array | SchemaType::Any => {
            Some(CliDisplayAs::Json)
        }
        SchemaType::Text
        | SchemaType::Integer
        | SchemaType::Number
        | SchemaType::Boolean
        | SchemaType::Enum => None,
    }
}

fn check_map(schema: &SchemaDoc<'_>, display: &CliDisplay) -> Check {
    let Some(pointer) = display.map.as_deref() else {
        return Err("`map` is missing".to_owned());
    };
    if pointer.contains('*') {
        return Err(format!(
            "`map` names the object itself, not its values: `{pointer}`"
        ));
    }
    let node = schema.resolve(pointer)?;
    if node.ty != SchemaType::Map {
        return Err(format!(
            "`map` needs an object keyed by node, but `{pointer}` is {}",
            article(node.ty)
        ));
    }
    let values = schema.element(pointer, &node)?;
    if !values.ty.is_scalar() {
        return Err(format!(
            "`map` values must be scalars, but `{pointer}/*` is {}",
            article(values.ty)
        ));
    }
    if display
        .header
        .as_deref()
        .is_some_and(|header| header.trim().is_empty())
    {
        return Err("`header` is empty".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::{json, Value};

    use super::{assert_no_display_keys, classify, nullable_variant, SchemaType};
    use crate::idl_tooling::IdlError;

    /// The schema walk's two pure decisions, truth-tabled: the generated
    /// documents exercise only the branches the wire happens to take (no
    /// `allOf`, no `properties` beside `additionalProperties`, null always
    /// the second `anyOf` variant), so the other rows live here.
    #[test]
    fn nullable_variant_accepts_exactly_one_null_beside_one_inner() {
        let inner = json!({"type": "string"});
        let other = json!({"type": "integer"});
        let null = json!({"type": "null"});
        assert_eq!(
            nullable_variant(&[inner.clone(), null.clone()]),
            Some(&inner)
        );
        assert_eq!(
            nullable_variant(&[null.clone(), inner.clone()]),
            Some(&inner)
        );
        assert_eq!(nullable_variant(&[inner.clone(), other]), None);
        assert_eq!(nullable_variant(&[null.clone(), null.clone()]), None);
        assert_eq!(nullable_variant(std::slice::from_ref(&inner)), None);
        assert_eq!(nullable_variant(&[]), None);
        assert_eq!(nullable_variant(&[inner, null.clone(), null]), None);
    }

    fn classified(schema: Value) -> std::result::Result<SchemaType, String> {
        let Value::Object(map) = schema else {
            panic!("truth table rows are schema objects")
        };
        classify(&map)
    }

    #[test]
    fn classify_tells_records_from_maps_by_properties_and_additional_properties() {
        let string = json!({"type": "string"});
        assert_eq!(
            classified(json!({"type": "object", "properties": {"a": string}})),
            Ok(SchemaType::Object)
        );
        assert_eq!(
            classified(json!({"type": "object"})),
            Ok(SchemaType::Object)
        );
        assert_eq!(
            classified(json!({"type": "object", "additionalProperties": false})),
            Ok(SchemaType::Object)
        );
        // Declared fields win even when the schema also admits extras.
        assert_eq!(
            classified(json!({
                "type": "object",
                "properties": {"a": string},
                "additionalProperties": string
            })),
            Ok(SchemaType::Object)
        );
        assert_eq!(
            classified(json!({"type": "object", "additionalProperties": string})),
            Ok(SchemaType::Map)
        );
        assert_eq!(
            classified(json!({"type": "object", "additionalProperties": true})),
            Ok(SchemaType::Map)
        );
    }

    #[test]
    fn classify_reads_scalars_encodings_and_nullable_type_lists() {
        assert_eq!(classified(json!({"type": "array"})), Ok(SchemaType::Array));
        assert_eq!(classified(json!({"type": "string"})), Ok(SchemaType::Text));
        assert_eq!(
            classified(json!({"type": "string", "contentEncoding": "base64"})),
            Ok(SchemaType::Base64)
        );
        assert_eq!(
            classified(json!({"type": "integer"})),
            Ok(SchemaType::Integer)
        );
        assert_eq!(
            classified(json!({"type": "number"})),
            Ok(SchemaType::Number)
        );
        assert_eq!(
            classified(json!({"type": "boolean"})),
            Ok(SchemaType::Boolean)
        );
        assert_eq!(
            classified(json!({"type": ["integer", "null"]})),
            Ok(SchemaType::Integer)
        );
        assert_eq!(
            classified(json!({"type": ["null", "string"]})),
            Ok(SchemaType::Text)
        );
        let multi =
            classified(json!({"type": ["string", "integer"]})).expect_err("two non-null types");
        assert!(multi.contains("multi-typed"), "{multi}");
        let malformed = classified(json!({"type": 42})).expect_err("a number is not a type");
        assert!(malformed.contains("malformed `type`"), "{malformed}");
        let unsupported = classified(json!({"type": "date"})).expect_err("no such JSON type");
        assert!(
            unsupported.contains("unsupported type `date`"),
            "{unsupported}"
        );
    }

    #[test]
    fn classify_admits_const_enums_and_refuses_other_unions_and_compositions() {
        let ok = json!({"const": "ok", "type": "string"});
        let record = json!({"type": "object", "properties": {"code": {"type": "string"}}});
        assert_eq!(
            classified(json!({"oneOf": [ok.clone(), {"const": "err"}]})),
            Ok(SchemaType::Enum)
        );
        let mixed = classified(json!({"oneOf": [ok, record.clone()]}))
            .expect_err("a tagged union is not an enum");
        assert!(mixed.contains("is a union"), "{mixed}");
        let composed = classified(json!({"allOf": [record]})).expect_err("no compositions");
        assert!(composed.contains("is a composition"), "{composed}");
        assert_eq!(classified(json!({})), Ok(SchemaType::Any));
        assert_eq!(
            classified(json!({"description": "anything"})),
            Ok(SchemaType::Any)
        );
    }

    fn rejection(text: &str) -> String {
        match assert_no_display_keys(text, Path::new("command-index.json")) {
            Err(IdlError::Invalid(message)) => message,
            Err(other) => panic!("expected an authored-IDL rejection, got {other}"),
            Ok(()) => panic!("expected the index to be rejected"),
        }
    }

    #[test]
    fn a_display_key_on_a_command_is_named_by_path() {
        let message = rejection(
            r#"{"commands": [{"id": "kv.put"}, {"id": "kv.get", "display": "bespoke"}]}"#,
        );
        assert!(message.contains("`/commands/1/display`"), "{message}");
        assert!(message.contains("command-index.json"), "{message}");
    }

    #[test]
    fn a_render_key_anywhere_in_the_tree_is_rejected() {
        let message = rejection(r#"{"kinds": {"read.get": {"render": "optional"}}}"#);
        assert!(message.contains("`/kinds/read.get/render`"), "{message}");
    }

    #[test]
    fn display_as_a_value_is_not_a_key() {
        // Only keys carry the layer; a wire field may hold the word.
        assert_no_display_keys(
            r#"{"commands": [{"id": "x", "summary": "display", "fields": ["render"]}]}"#,
            Path::new("command-index.json"),
        )
        .expect("values are not keys");
    }
}
