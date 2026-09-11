//! Response-model derivation and guard (#3313, #3322; S0c of the CLI output
//! contract, #3314).
//!
//! A command's `response_model` names a *family* (`MutationAck`, `Maybe<T>`,
//! `Page<T>`, …) and every family implies a wire shape. Authored IDL declares
//! only the family, as a template with a `{payload}` slot
//! (`Maybe<{payload}>`); the payload is *derived* from the generated schema —
//! the `$def` (or wire-spelled primitive) at the slot the family's shape
//! reserves for it — so the published name can never disagree with the wire
//! the schema describes. `MutationAck` carries no payload: an acknowledgement
//! is the same record for every mutation.
//!
//! This module classifies the schema's `response.data` into a [`WireShape`],
//! accepts it against the declared [`ResponseFamily`], fills the template, and
//! requires every disagreement to be listed in the shrink-only
//! `response-model-divergences.yaml` ledger with `wire_status: transitional`.
//! A ledgered command's published `response_model` is the row's `declared`
//! target — spelled within the declared family and naming a `$def` of the
//! command's own schema — while the ledger names the wire that has not yet
//! caught up. Normalising a listed wire is a separate, tracked change — this
//! guard only stops the set from growing silently.
//!
//! `dto-inventory.yaml` is the reviewed set of published response models: a
//! derived name that is not listed fails `generate`, and so does a listed
//! name no command resolves to, so the inventory can only ever carry the
//! models in use.
//!
//! *Encoding* is not divergence. Whether a `Maybe<T>` is `{found, value}` or a
//! nullable `data`, and whether a history is `{items}` or a bare array, are
//! both accepted spellings of the declared family (contract §Root E); the
//! spelling is resolved into the CLI index as `encoding` so the renderer never
//! sniffs it.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::path::Path;

use serde::Deserialize;
use serde_json::{Map, Value};

use super::{enforce_debt_budget, invalid, read_yaml, CommandIndex, Result};
use crate::cli_metadata::CliWireEncoding;

/// The shrink-only ledger of commands whose wire diverges from their declared
/// response model.
pub(super) const RESPONSE_MODEL_DIVERGENCES_FILE: &str = "response-model-divergences.yaml";

/// The reviewed set of published response models.
pub(super) const DTO_INVENTORY_FILE: &str = "dto-inventory.yaml";

/// The slot a family template leaves for the schema-derived payload name.
const PAYLOAD_SLOT: &str = "{payload}";

/// JSON Schema type names a payload may be spelled as when it is not a `$def`.
const PRIMITIVES: [&str; 4] = ["boolean", "integer", "number", "string"];

/// `response-model-divergences.yaml`: one row per command whose generated
/// schema does not carry the shape its `response_model` family implies.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DivergencesSource {
    /// Debt-count budget: MUST equal `divergences.len()` (see
    /// `enforce_debt_budget`). Growth needs a reviewed raise; a drained row
    /// forces the budget down.
    budget: usize,
    divergences: Vec<DivergenceRow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DivergenceRow {
    command: String,
    /// The complete `response_model` the command publishes until its wire is
    /// normalised: the target, spelled within the family the command's
    /// template declares.
    declared: String,
    /// The [`WireShape::name`] the schema carries today.
    wire: String,
    /// The issue tracking the wire's normalisation.
    issue: NonZeroU64,
}

/// `dto-inventory.yaml`: every response model a command may resolve to.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DtoInventorySource {
    response_models: Vec<String>,
}

/// Family prefixes, longest first: `Maybe<Vec<` shadows `Maybe<`.
const FAMILY_PREFIXES: [(&str, ResponseFamily); 9] = [
    ("Maybe<Vec<", ResponseFamily::MaybeVec),
    ("Maybe<", ResponseFamily::Maybe),
    ("SamplePage<", ResponseFamily::SamplePage),
    ("Page<", ResponseFamily::Page),
    ("SearchResult<", ResponseFamily::SearchResult),
    ("StatusValue<", ResponseFamily::StatusValue),
    ("StatusResponse<", ResponseFamily::StatusResponse),
    ("AnalyticsResult<", ResponseFamily::AnalyticsResult),
    ("BatchResult<", ResponseFamily::BatchResult),
];

/// The family a `response_model` declaration names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponseFamily {
    MutationAck,
    Maybe,
    MaybeVec,
    Page,
    SamplePage,
    SearchResult,
    SearchResultDiagnostics,
    StatusValue,
    StatusResponse,
    AnalyticsResult,
    BatchResult,
    /// The payload alone: the command promises a specific record, not a family.
    Bare,
}

impl ResponseFamily {
    /// Parses the family of a `response_model`, template or complete: the
    /// kind layer declares `Maybe<{payload}>`, the resolved index and the
    /// ledger's `declared` column carry `Maybe<VersionedValue>`.
    pub(super) fn from_declaration(command_id: &str, declared: &str) -> Result<Self> {
        if declared == Self::MutationAck.template() {
            return Ok(Self::MutationAck);
        }
        if declared.starts_with("SearchResult<") && declared.ends_with(" + IndexDiagnostics") {
            return Ok(Self::SearchResultDiagnostics);
        }
        if let Some((_, family)) = FAMILY_PREFIXES
            .iter()
            .find(|(prefix, _)| declared.starts_with(prefix))
        {
            return Ok(*family);
        }
        if declared.contains('<') {
            return Err(invalid(format!(
                "command `{command_id}` declares response_model `{declared}` of an unknown family"
            )));
        }
        Ok(Self::Bare)
    }

    /// Parses an authored `response_model` and requires it to be exactly the
    /// family's template: the payload is derived, never declared.
    pub(super) fn from_template(command_id: &str, template: &str) -> Result<Self> {
        let family = Self::from_declaration(command_id, template)?;
        if template != family.template() {
            return Err(invalid(format!(
                "command `{command_id}` declares response_model `{template}`; the payload is \
                 derived from the schema, so declare the family template `{}`",
                family.template()
            )));
        }
        Ok(family)
    }

    /// The authored spelling of the family: the family around a `{payload}`
    /// slot, or `MutationAck` alone.
    pub(super) const fn template(self) -> &'static str {
        match self {
            Self::MutationAck => "MutationAck",
            Self::Maybe => "Maybe<{payload}>",
            Self::MaybeVec => "Maybe<Vec<{payload}>>",
            Self::Page => "Page<{payload}>",
            Self::SamplePage => "SamplePage<{payload}>",
            Self::SearchResult => "SearchResult<{payload}>",
            Self::SearchResultDiagnostics => "SearchResult<{payload}> + IndexDiagnostics",
            Self::StatusValue => "StatusValue<{payload}>",
            Self::StatusResponse => "StatusResponse<{payload}>",
            Self::AnalyticsResult => "AnalyticsResult<{payload}>",
            Self::BatchResult => "BatchResult<{payload}>",
            Self::Bare => "{payload}",
        }
    }

    /// The payload a complete declaration of this family carries, or `None`
    /// for a family without a slot; a declaration that does not fit the
    /// template is rejected.
    fn payload_of(self, command_id: &str, declared: &str) -> Result<Option<String>> {
        let template = self.template();
        let Some((prefix, suffix)) = template.split_once(PAYLOAD_SLOT) else {
            return Ok(None);
        };
        declared
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .filter(|payload| !payload.is_empty())
            .map(|payload| Some(payload.to_owned()))
            .ok_or_else(|| {
                invalid(format!(
                    "command `{command_id}` declaration `{declared}` does not fit `{template}`"
                ))
            })
    }

    /// Prose for the shape the family promises (docs `Returns:` section).
    pub(super) const fn describe(self) -> &'static str {
        match self {
            Self::MutationAck => "a mutation acknowledgement",
            Self::Maybe => "an optional value",
            Self::MaybeVec => "an optional history",
            Self::Page => "a page",
            Self::SamplePage => "a sample page",
            Self::SearchResult => "an array of matches",
            Self::SearchResultDiagnostics => "matches with diagnostics",
            Self::StatusValue => "a bare scalar",
            Self::StatusResponse | Self::AnalyticsResult => "a bare record",
            Self::BatchResult => "a batch result",
            Self::Bare => "the named record",
        }
    }
}

/// The shape a schema's `response.data` describes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WireShape {
    /// A JSON scalar; carries the schema type name.
    Scalar(String),
    Array,
    Nullable(Box<WireShape>),
    /// An object with a required `effect`.
    MutationAck,
    /// `{items, has_more, cursor?}`.
    Page,
    /// `{items, has_more, total_count, cursor?}`.
    SamplePage,
    /// The itemwise batch envelope.
    Batch,
    /// `{found, value}`.
    FoundValue,
    /// `{matches, diagnostics}`.
    Diagnostics,
    /// `{items}` alone.
    Items,
    /// Any other object.
    Record,
}

impl WireShape {
    /// Machine name, as written in the ledger's `wire:` column.
    pub(super) fn name(&self) -> String {
        match self {
            Self::Scalar(ty) => format!("scalar:{ty}"),
            Self::Array => "array".to_owned(),
            Self::Nullable(inner) => format!("nullable:{}", inner.name()),
            Self::MutationAck => "mutation_ack".to_owned(),
            Self::Page => "page".to_owned(),
            Self::SamplePage => "sample_page".to_owned(),
            Self::Batch => "batch".to_owned(),
            Self::FoundValue => "found_value".to_owned(),
            Self::Diagnostics => "diagnostics".to_owned(),
            Self::Items => "items".to_owned(),
            Self::Record => "record".to_owned(),
        }
    }

    /// Prose for the docs `Returns:` section.
    pub(super) fn describe(&self) -> String {
        match self {
            Self::Scalar(ty) => format!("a bare `{ty}`"),
            Self::Array => "a bare array".to_owned(),
            Self::Nullable(inner) => format!("{} or `null`", inner.describe()),
            Self::MutationAck => "a mutation acknowledgement".to_owned(),
            Self::Page => "a page".to_owned(),
            Self::SamplePage => "a sample page".to_owned(),
            Self::Batch => "a batch result".to_owned(),
            Self::FoundValue => "a `{found, value}` record".to_owned(),
            Self::Diagnostics => "matches with diagnostics".to_owned(),
            Self::Items => "an `{items}` record".to_owned(),
            Self::Record => "a bare record".to_owned(),
        }
    }
}

/// Classifies the `response.data` of one generated schema document.
pub(super) fn classify(command_id: &str, document: &Value) -> Result<WireShape> {
    classify_node(
        command_id,
        defs_of(document),
        response_data(command_id, document)?,
    )
}

fn defs_of(document: &Value) -> Option<&Map<String, Value>> {
    document.get("$defs").and_then(Value::as_object)
}

fn response_data<'a>(command_id: &str, document: &'a Value) -> Result<&'a Value> {
    document
        .pointer("/response/properties/data")
        .ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` schema has no response data"
            ))
        })
}

fn classify_node(
    command_id: &str,
    defs: Option<&Map<String, Value>>,
    node: &Value,
) -> Result<WireShape> {
    let node = deref(command_id, defs, node)?;
    let map = node
        .as_object()
        .ok_or_else(|| unclassifiable(command_id, "a non-object schema node"))?;
    if let Some(variants) = map.get("anyOf") {
        return classify_any_of(command_id, defs, variants);
    }
    match map.get("type") {
        Some(Value::String(ty)) => classify_typed(command_id, map, ty),
        Some(Value::Array(types)) => {
            let names: Vec<&str> = types.iter().filter_map(Value::as_str).collect();
            let (nulls, others): (Vec<&str>, Vec<&str>) =
                names.iter().copied().partition(|ty| *ty == "null");
            match (
                names.len() == types.len(),
                nulls.as_slice(),
                others.as_slice(),
            ) {
                (true, [_], [ty]) => Ok(WireShape::Nullable(Box::new(classify_typed(
                    command_id, map, ty,
                )?))),
                _ => Err(unclassifiable(
                    command_id,
                    "a type list that is not `[T, \"null\"]`",
                )),
            }
        }
        _ => Err(unclassifiable(
            command_id,
            "a node with neither `type` nor `anyOf`",
        )),
    }
}

/// `anyOf: [T, {type: null}]` in either order is a nullable `T`; every other
/// union is unclassifiable rather than guessed at.
fn classify_any_of(
    command_id: &str,
    defs: Option<&Map<String, Value>>,
    variants: &Value,
) -> Result<WireShape> {
    let variants = variants
        .as_array()
        .ok_or_else(|| unclassifiable(command_id, "a non-list `anyOf`"))?;
    let mut nulls = 0;
    let mut inner = None;
    for variant in variants {
        let variant = deref(command_id, defs, variant)?;
        if variant.get("type") == Some(&Value::from("null")) {
            nulls += 1;
        } else if inner.replace(variant).is_some() {
            return Err(unclassifiable(command_id, "an `anyOf` of several values"));
        }
    }
    match (nulls, inner) {
        (1, Some(inner)) => Ok(WireShape::Nullable(Box::new(classify_node(
            command_id, defs, inner,
        )?))),
        _ => Err(unclassifiable(
            command_id,
            "an `anyOf` that is not `[T, null]`",
        )),
    }
}

fn classify_typed(command_id: &str, map: &Map<String, Value>, ty: &str) -> Result<WireShape> {
    match ty {
        "boolean" | "integer" | "number" | "string" => Ok(WireShape::Scalar(ty.to_owned())),
        "array" => Ok(WireShape::Array),
        "object" => Ok(classify_object(map)),
        other => Err(unclassifiable(command_id, &format!("type `{other}`"))),
    }
}

/// Every object classifies: the envelope shapes by their key sets, anything
/// else as a record. Key sets are the schema's `properties`; a map-valued
/// object (no `properties`, only `additionalProperties`) is a record.
fn classify_object(map: &Map<String, Value>) -> WireShape {
    let keys: BTreeSet<&str> = map
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().map(String::as_str).collect())
        // No `properties` means an open map, which has no envelope keys.
        .unwrap_or_default();
    let required: BTreeSet<&str> = map
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        // A schema with no `required` requires nothing.
        .unwrap_or_default();
    let subset_of = |allowed: &[&str]| keys.iter().all(|key| allowed.contains(key));
    let demands = |needed: &[&str]| needed.iter().all(|key| required.contains(key));
    let exactly = |wanted: &[&str]| keys.len() == wanted.len() && subset_of(wanted);
    if required.contains("effect") {
        WireShape::MutationAck
    } else if subset_of(&["items", "has_more", "cursor"]) && demands(&["items", "has_more"]) {
        WireShape::Page
    } else if subset_of(&["items", "has_more", "cursor", "total_count"])
        && demands(&["items", "has_more", "total_count"])
    {
        WireShape::SamplePage
    } else if demands(&["items", "mode", "status", "applied"]) {
        WireShape::Batch
    } else if exactly(&["found", "value"]) {
        WireShape::FoundValue
    } else if exactly(&["matches", "diagnostics"]) {
        WireShape::Diagnostics
    } else if exactly(&["items"]) {
        WireShape::Items
    } else {
        WireShape::Record
    }
}

/// Follows `$ref: "#/$defs/<name>"` chains; anything else is returned as is.
fn deref<'a>(
    command_id: &str,
    defs: Option<&'a Map<String, Value>>,
    mut node: &'a Value,
) -> Result<&'a Value> {
    while let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        let name = def_name(command_id, reference)?;
        node = defs.and_then(|defs| defs.get(name)).ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` schema references undefined `{name}`"
            ))
        })?;
    }
    Ok(node)
}

/// The `$defs` entry a `$ref` names.
fn def_name<'a>(command_id: &str, reference: &'a str) -> Result<&'a str> {
    reference.strip_prefix("#/$defs/").ok_or_else(|| {
        invalid(format!(
            "command `{command_id}` schema references `{reference}` outside `$defs`"
        ))
    })
}

fn unclassifiable(command_id: &str, what: &str) -> super::IdlError {
    invalid(format!(
        "command `{command_id}` response data is unclassifiable: {what}"
    ))
}

/// Whether `shape` is an accepted spelling of `family`.
pub(super) fn accepts(family: ResponseFamily, shape: &WireShape) -> bool {
    match family {
        ResponseFamily::MutationAck => *shape == WireShape::MutationAck,
        ResponseFamily::Maybe => {
            matches!(shape, WireShape::FoundValue | WireShape::Nullable(_))
        }
        ResponseFamily::MaybeVec => matches!(
            shape,
            WireShape::Nullable(inner) if matches!(**inner, WireShape::Items | WireShape::Array)
        ),
        ResponseFamily::Page => *shape == WireShape::Page,
        ResponseFamily::SamplePage => *shape == WireShape::SamplePage,
        ResponseFamily::SearchResult => *shape == WireShape::Array,
        ResponseFamily::SearchResultDiagnostics => *shape == WireShape::Diagnostics,
        ResponseFamily::StatusValue => matches!(shape, WireShape::Scalar(_)),
        ResponseFamily::StatusResponse | ResponseFamily::AnalyticsResult => {
            *shape == WireShape::Record
        }
        ResponseFamily::BatchResult => *shape == WireShape::Batch,
        ResponseFamily::Bare => true,
    }
}

/// The schema node at the slot an accepted `shape` of `family` reserves for
/// the payload; `None` for a family that carries none.
///
/// Every slot is read through `$ref`s except the payload itself, whose `$ref`
/// is the name being derived.
fn payload_slot<'a>(
    command_id: &str,
    family: ResponseFamily,
    shape: &WireShape,
    document: &'a Value,
) -> Result<Option<&'a Value>> {
    use ResponseFamily as F;
    let defs = defs_of(document);
    let data = response_data(command_id, document)?;
    let property = |node: &'a Value, key: &str| -> Result<&'a Value> {
        deref(command_id, defs, node)?
            .pointer(&format!("/properties/{key}"))
            .ok_or_else(|| {
                invalid(format!(
                    "command `{command_id}` response data carries no `{key}` property"
                ))
            })
    };
    let element = |node: &'a Value| -> Result<&'a Value> {
        deref(command_id, defs, node)?.get("items").ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` response data carries an array without `items`"
            ))
        })
    };
    let no_slot = || {
        invalid(format!(
            "command `{command_id}` response data carries {} which has no payload slot for {}",
            shape.describe(),
            family.describe()
        ))
    };
    let slot = match (family, shape) {
        (F::MutationAck, _) => return Ok(None),
        (F::Maybe, WireShape::FoundValue) => property(non_null(data), "value")?,
        (F::Maybe, WireShape::Nullable(_))
        | (F::StatusValue | F::StatusResponse | F::AnalyticsResult | F::Bare, _) => data,
        (F::MaybeVec, WireShape::Nullable(inner)) => {
            let list = non_null(data);
            match inner.as_ref() {
                WireShape::Items => element(property(list, "items")?)?,
                WireShape::Array => element(list)?,
                _ => return Err(no_slot()),
            }
        }
        (F::Page | F::SamplePage, _) => element(property(data, "items")?)?,
        (F::SearchResult, _) => element(data)?,
        (F::SearchResultDiagnostics, _) => element(property(data, "matches")?)?,
        (F::BatchResult, _) => property(element(property(data, "items")?)?, "result")?,
        (F::Maybe | F::MaybeVec, _) => return Err(no_slot()),
    };
    Ok(Some(slot))
}

/// The non-null half of `anyOf: [T, {type: null}]`; any other node as is. A
/// `type: [T, "null"]` node keeps its own `properties`/`items`, so it needs
/// no unwrapping.
fn non_null(node: &Value) -> &Value {
    node.get("anyOf")
        .and_then(Value::as_array)
        .and_then(|variants| {
            variants
                .iter()
                .find(|variant| variant.get("type") != Some(&Value::from("null")))
        })
        .unwrap_or(node)
}

/// The published name of a payload node: its `$def`, a wire primitive, or a
/// `[]`-suffixed element name for an array of either.
fn payload_name(
    command_id: &str,
    defs: Option<&Map<String, Value>>,
    node: &Value,
) -> Result<String> {
    let node = non_null(node);
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        let name = def_name(command_id, reference)?;
        // Resolving proves the `$def` exists; the name itself is the payload.
        deref(command_id, defs, node)?;
        return Ok(name.to_owned());
    }
    let map = node
        .as_object()
        .ok_or_else(|| unnameable(command_id, "a non-object schema node"))?;
    match single_type(map) {
        Some(ty) if PRIMITIVES.contains(&ty) => Ok(ty.to_owned()),
        Some("array") => {
            let items = map
                .get("items")
                .ok_or_else(|| unnameable(command_id, "an array without `items`"))?;
            Ok(format!("{}[]", payload_name(command_id, defs, items)?))
        }
        Some("object") => Err(unnameable(
            command_id,
            "an anonymous record; give the DTO a name so the schema can `$ref` it",
        )),
        Some(other) => Err(unnameable(command_id, &format!("type `{other}`"))),
        None => Err(unnameable(command_id, "a node with no single type")),
    }
}

/// The one non-null JSON type a node names, if it names exactly one.
fn single_type(map: &Map<String, Value>) -> Option<&str> {
    match map.get("type") {
        Some(Value::String(ty)) => Some(ty),
        Some(Value::Array(types)) => {
            let mut others = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|ty| *ty != "null");
            let first = others.next()?;
            others.next().is_none().then_some(first)
        }
        _ => None,
    }
}

fn unnameable(command_id: &str, what: &str) -> super::IdlError {
    invalid(format!(
        "command `{command_id}` response payload has no name: {what}"
    ))
}

/// Whether a ledgered declaration's payload names a `$def` of the command's
/// schema or a wire primitive, either possibly `[]`-suffixed.
fn names_a_def(payload: &str, defs: Option<&Map<String, Value>>) -> bool {
    let base = payload.trim_end_matches("[]");
    PRIMITIVES.contains(&base) || defs.is_some_and(|defs| defs.contains_key(base))
}

/// The spelling the CLI renderer needs for an optional or history value;
/// `None` for every other shape.
pub(super) fn encoding(shape: &WireShape) -> Option<CliWireEncoding> {
    match shape {
        WireShape::FoundValue => Some(CliWireEncoding::FoundValue),
        WireShape::Nullable(inner) => Some(match **inner {
            WireShape::Items => CliWireEncoding::Items,
            WireShape::Array => CliWireEncoding::Array,
            _ => CliWireEncoding::Nullable,
        }),
        _ => None,
    }
}

/// Resolves the CLI-index `encoding` of one command from its schema document.
pub(super) fn encoding_for(command_id: &str, document: &Value) -> Result<Option<CliWireEncoding>> {
    Ok(encoding(&classify(command_id, document)?))
}

/// Resolves every command's published `response_model` from its family
/// template and generated schema — the guard `generate` and `check` run.
///
/// A command whose wire matches its declared family publishes the filled
/// template; a ledgered, transitional divergence publishes its row's target;
/// anything else fails. The ledger names nothing else, and the inventory
/// lists exactly the models the commands resolve to.
pub(super) fn derive_response_models(
    idl_root: &Path,
    index: &mut CommandIndex,
    documents: &BTreeMap<String, Value>,
) -> Result<()> {
    let inventory = read_inventory(idl_root)?;
    let ledger: DivergencesSource = read_yaml(&idl_root.join(RESPONSE_MODEL_DIVERGENCES_FILE))?;
    enforce_debt_budget(
        RESPONSE_MODEL_DIVERGENCES_FILE,
        ledger.divergences.len(),
        ledger.budget,
    )?;
    let mut rows: BTreeMap<&str, &DivergenceRow> = BTreeMap::new();
    for row in &ledger.divergences {
        if rows.insert(row.command.as_str(), row).is_some() {
            return Err(invalid(format!(
                "duplicate `{}` in {RESPONSE_MODEL_DIVERGENCES_FILE}",
                row.command
            )));
        }
    }
    let mut referenced = BTreeSet::new();
    for command in &mut index.commands {
        let document = documents
            .get(&command.id)
            .ok_or_else(|| invalid(format!("no schema document for `{}`", command.id)))?;
        let family = ResponseFamily::from_template(&command.id, &command.response_model)?;
        let shape = classify(&command.id, document)?;
        let row = rows.remove(command.id.as_str());
        let resolved = resolve_declaration(command, family, &shape, row, document)?;
        if !inventory.contains(&resolved) {
            return Err(invalid(format!(
                "command `{}` resolves to response model `{resolved}` which {DTO_INVENTORY_FILE} \
                 does not list; add it (every published model is reviewed once)",
                command.id
            )));
        }
        referenced.insert(resolved.clone());
        command.response_model = resolved;
    }
    if let Some(unknown) = rows.into_keys().next() {
        return Err(invalid(format!(
            "{RESPONSE_MODEL_DIVERGENCES_FILE} lists `{unknown}` which is not a command id; remove it"
        )));
    }
    if let Some(unreferenced) = inventory.difference(&referenced).next() {
        return Err(invalid(format!(
            "{DTO_INVENTORY_FILE} lists `{unreferenced}` which no command resolves to; remove it \
             (the inventory carries only the models in use)"
        )));
    }
    Ok(())
}

fn read_inventory(idl_root: &Path) -> Result<BTreeSet<String>> {
    let source: DtoInventorySource = read_yaml(&idl_root.join(DTO_INVENTORY_FILE))?;
    let mut inventory = BTreeSet::new();
    for model in source.response_models {
        if !inventory.insert(model.clone()) {
            return Err(invalid(format!(
                "duplicate `{model}` in {DTO_INVENTORY_FILE}"
            )));
        }
    }
    Ok(inventory)
}

/// The complete `response_model` one command publishes.
fn resolve_declaration(
    command: &super::ResolvedCommand,
    family: ResponseFamily,
    shape: &WireShape,
    row: Option<&DivergenceRow>,
    document: &Value,
) -> Result<String> {
    let id = &command.id;
    let template = &command.response_model;
    if accepts(family, shape) {
        if let Some(row) = row {
            return Err(invalid(format!(
                "`{id}` now carries {} as its `{template}` declares; remove its row (#{}) from \
                 {RESPONSE_MODEL_DIVERGENCES_FILE} (the ledger may only shrink)",
                shape.describe(),
                row.issue
            )));
        }
        return Ok(match payload_slot(id, family, shape, document)? {
            None => template.clone(),
            Some(slot) => {
                template.replace(PAYLOAD_SLOT, &payload_name(id, defs_of(document), slot)?)
            }
        });
    }
    let Some(row) = row else {
        return Err(invalid(format!(
            "command `{id}` declares `{template}` ({}) but its schema carries {}; correct the \
             declaration, or if the wire is what is wrong, list the command in \
             {RESPONSE_MODEL_DIVERGENCES_FILE} with its target declaration and mark it \
             `wire_status: transitional`",
            family.describe(),
            shape.describe()
        )));
    };
    let declared_family = ResponseFamily::from_declaration(id, &row.declared)?;
    if declared_family != family {
        return Err(invalid(format!(
            "{RESPONSE_MODEL_DIVERGENCES_FILE} row for `{id}` declares `{}` ({}) but the command \
             declares `{template}` ({}); a row states the target within the declared family",
            row.declared,
            declared_family.describe(),
            family.describe()
        )));
    }
    if let Some(payload) = family.payload_of(id, &row.declared)? {
        if !names_a_def(&payload, defs_of(document)) {
            return Err(invalid(format!(
                "{RESPONSE_MODEL_DIVERGENCES_FILE} row for `{id}` declares `{}` but `{payload}` is \
                 neither a `$def` of the command's schema nor a wire primitive",
                row.declared
            )));
        }
    }
    let wire = shape.name();
    if row.wire != wire {
        return Err(invalid(format!(
            "{RESPONSE_MODEL_DIVERGENCES_FILE} row for `{id}` records wire `{}` but the schema \
             carries `{wire}`",
            row.wire
        )));
    }
    if command.wire_status != "transitional" {
        return Err(invalid(format!(
            "command `{id}` is listed in {RESPONSE_MODEL_DIVERGENCES_FILE} but its wire_status \
             is `{}`; a ledgered divergence must be `transitional`",
            command.wire_status
        )));
    }
    Ok(row.declared.clone())
}

#[cfg(test)]
mod tests {
    use super::{
        accepts, classify, defs_of, encoding, names_a_def, payload_name, payload_slot,
        ResponseFamily, WireShape,
    };
    use crate::cli_metadata::CliWireEncoding;
    use serde_json::{json, Value};

    fn document(data: &Value, defs: &Value) -> Value {
        json!({ "$defs": defs, "response": { "properties": { "data": data } } })
    }

    fn shape_of(data: &Value) -> WireShape {
        classify("t.c", &document(data, &json!({}))).expect("classifies")
    }

    #[test]
    fn families_parse_by_prefix_longest_first() {
        let cases = [
            ("MutationAck", ResponseFamily::MutationAck),
            ("Maybe<VersionedValue>", ResponseFamily::Maybe),
            ("Maybe<Vec<VersionedValue>>", ResponseFamily::MaybeVec),
            ("Page<BranchItem>", ResponseFamily::Page),
            ("SamplePage<KvItem>", ResponseFamily::SamplePage),
            ("SearchResult<VectorMatch>", ResponseFamily::SearchResult),
            (
                "SearchResult<VectorMatch> + IndexDiagnostics",
                ResponseFamily::SearchResultDiagnostics,
            ),
            ("StatusValue<boolean>", ResponseFamily::StatusValue),
            ("StatusResponse<HealthInfo>", ResponseFamily::StatusResponse),
            ("AnalyticsResult<number>", ResponseFamily::AnalyticsResult),
            ("BatchResult<KvItem>", ResponseFamily::BatchResult),
            ("EmbedResponse", ResponseFamily::Bare),
            ("{payload}", ResponseFamily::Bare),
        ];
        for (declared, expected) in cases {
            let family = ResponseFamily::from_declaration("t.c", declared).expect("known family");
            assert_eq!(family, expected, "{declared}");
        }
        for declared in ["Option<KvItem>", "MutationAck<BranchItem>"] {
            let rejected = ResponseFamily::from_declaration("t.c", declared)
                .expect_err("an unknown generic family is rejected");
            assert!(
                rejected.to_string().contains("unknown family"),
                "{declared}: {rejected}"
            );
        }
    }

    #[test]
    fn templates_round_trip_and_complete_declarations_are_not_templates() {
        use ResponseFamily as F;
        let families = [
            F::MutationAck,
            F::Maybe,
            F::MaybeVec,
            F::Page,
            F::SamplePage,
            F::SearchResult,
            F::SearchResultDiagnostics,
            F::StatusValue,
            F::StatusResponse,
            F::AnalyticsResult,
            F::BatchResult,
            F::Bare,
        ];
        for family in families {
            let parsed = ResponseFamily::from_template("t.c", family.template()).expect("template");
            assert_eq!(parsed, family, "{}", family.template());
        }
        for declared in [
            "Maybe<VersionedValue>",
            "EmbedResponse",
            "Page<{payload}, String>",
        ] {
            let rejected = ResponseFamily::from_template("t.c", declared)
                .expect_err("a complete declaration is not a template");
            assert!(
                rejected.to_string().contains("declare the family template"),
                "{declared}: {rejected}"
            );
        }
    }

    #[test]
    fn payloads_are_read_back_out_of_complete_declarations() {
        use ResponseFamily as F;
        let payload = |family: F, declared: &str| family.payload_of("t.c", declared);
        assert_eq!(payload(F::MutationAck, "MutationAck").expect("fits"), None);
        assert_eq!(
            payload(F::Maybe, "Maybe<VersionedValue>").expect("fits"),
            Some("VersionedValue".to_owned())
        );
        assert_eq!(
            payload(F::MaybeVec, "Maybe<Vec<HistoryItem>>").expect("fits"),
            Some("HistoryItem".to_owned())
        );
        assert_eq!(
            payload(
                F::SearchResultDiagnostics,
                "SearchResult<VectorMatch> + IndexDiagnostics"
            )
            .expect("fits"),
            Some("VectorMatch".to_owned())
        );
        assert_eq!(
            payload(F::Bare, "integer[]").expect("fits"),
            Some("integer[]".to_owned())
        );
        for (family, declared) in [
            (F::Maybe, "Page<VersionedValue>"),
            (F::Maybe, "Maybe<>"),
            (F::Maybe, "Maybe<VersionedValue"),
            (F::StatusValue, "StatusResponse<Info>"),
        ] {
            let rejected = payload(family, declared).expect_err("does not fit");
            assert!(
                rejected.to_string().contains("does not fit"),
                "{declared}: {rejected}"
            );
        }
    }

    #[test]
    fn scalars_arrays_and_nullables_classify() {
        assert_eq!(
            shape_of(&json!({"type": "boolean"})),
            WireShape::Scalar("boolean".into())
        );
        assert_eq!(
            shape_of(&json!({"type": "integer"})),
            WireShape::Scalar("integer".into())
        );
        assert_eq!(
            shape_of(&json!({"type": "array", "items": {}})),
            WireShape::Array
        );
        assert_eq!(
            shape_of(&json!({"type": ["string", "null"]})),
            WireShape::Nullable(Box::new(WireShape::Scalar("string".into())))
        );
        assert_eq!(
            shape_of(&json!({"type": ["array", "null"], "items": {}})),
            WireShape::Nullable(Box::new(WireShape::Array))
        );
        assert_eq!(
            shape_of(
                &json!({"anyOf": [{"type": "null"}, {"type": "object", "properties": {"items": {}}, "required": ["items"]}]})
            ),
            WireShape::Nullable(Box::new(WireShape::Items))
        );
    }

    #[test]
    fn objects_classify_by_key_set() {
        let object = |properties: &[&str], required: &[&str]| {
            let properties: serde_json::Map<String, Value> = properties
                .iter()
                .map(|key| ((*key).to_owned(), json!({})))
                .collect();
            shape_of(&json!({"type": "object", "properties": properties, "required": required}))
        };
        assert_eq!(
            object(&["effect", "commit"], &["effect", "commit"]),
            WireShape::MutationAck
        );
        assert_eq!(
            object(&["items", "has_more", "cursor"], &["items", "has_more"]),
            WireShape::Page
        );
        assert_eq!(
            object(
                &["items", "has_more", "cursor", "total_count"],
                &["items", "has_more", "total_count"]
            ),
            WireShape::SamplePage
        );
        assert_eq!(
            object(
                &["items", "mode", "status", "applied", "count"],
                &["items", "mode", "status", "applied"]
            ),
            WireShape::Batch
        );
        assert_eq!(
            object(&["found", "value"], &["found"]),
            WireShape::FoundValue
        );
        assert_eq!(
            object(&["matches", "diagnostics"], &["matches", "diagnostics"]),
            WireShape::Diagnostics
        );
        assert_eq!(object(&["items"], &["items"]), WireShape::Items);
        assert_eq!(
            object(&["items", "limit", "offset", "total"], &["items"]),
            WireShape::Record
        );
        assert_eq!(object(&["origin"], &[]), WireShape::Record);
        // A map-valued object has no `properties` at all.
        assert_eq!(
            shape_of(&json!({"type": "object", "additionalProperties": {"type": "number"}})),
            WireShape::Record
        );
    }

    #[test]
    fn refs_resolve_through_defs_and_dangling_refs_are_rejected() {
        let defs = json!({"Inner": {"type": "boolean"}, "Outer": {"$ref": "#/$defs/Inner"}});
        let shape = classify("t.c", &document(&json!({"$ref": "#/$defs/Outer"}), &defs))
            .expect("chained refs resolve");
        assert_eq!(shape, WireShape::Scalar("boolean".into()));
        let dangling = classify("t.c", &document(&json!({"$ref": "#/$defs/Missing"}), &defs))
            .expect_err("a dangling ref is rejected");
        assert!(
            dangling.to_string().contains("undefined `Missing`"),
            "{dangling}"
        );
    }

    #[test]
    fn unions_that_are_not_nullable_are_unclassifiable() {
        for data in [
            json!({"anyOf": [{"type": "string"}, {"type": "integer"}]}),
            json!({"anyOf": [{"type": "null"}, {"type": "null"}]}),
            json!({"type": ["string", "integer"]}),
            json!({"type": "date"}),
            json!({"description": "no type at all"}),
        ] {
            let rejected = classify("t.c", &document(&data, &json!({}))).expect_err("rejected");
            assert!(
                rejected.to_string().contains("unclassifiable"),
                "{data}: {rejected}"
            );
        }
    }

    #[test]
    fn acceptance_truth_table() {
        use ResponseFamily as F;
        let nullable = |inner: WireShape| WireShape::Nullable(Box::new(inner));
        let shapes = [
            WireShape::Scalar("boolean".into()),
            WireShape::Array,
            nullable(WireShape::Record),
            nullable(WireShape::Items),
            nullable(WireShape::Array),
            WireShape::MutationAck,
            WireShape::Page,
            WireShape::SamplePage,
            WireShape::Batch,
            WireShape::FoundValue,
            WireShape::Diagnostics,
            WireShape::Items,
            WireShape::Record,
        ];
        // For each family, the names of the shapes it accepts.
        let expected: [(F, &[&str]); 12] = [
            (F::MutationAck, &["mutation_ack"]),
            (
                F::Maybe,
                &[
                    "nullable:record",
                    "nullable:items",
                    "nullable:array",
                    "found_value",
                ],
            ),
            (F::MaybeVec, &["nullable:items", "nullable:array"]),
            (F::Page, &["page"]),
            (F::SamplePage, &["sample_page"]),
            (F::SearchResult, &["array"]),
            (F::SearchResultDiagnostics, &["diagnostics"]),
            (F::StatusValue, &["scalar:boolean"]),
            (F::StatusResponse, &["record"]),
            (F::AnalyticsResult, &["record"]),
            (F::BatchResult, &["batch"]),
            (
                F::Bare,
                &[
                    "scalar:boolean",
                    "array",
                    "nullable:record",
                    "nullable:items",
                    "nullable:array",
                    "mutation_ack",
                    "page",
                    "sample_page",
                    "batch",
                    "found_value",
                    "diagnostics",
                    "items",
                    "record",
                ],
            ),
        ];
        for (family, accepted) in expected {
            for shape in &shapes {
                assert_eq!(
                    accepts(family, shape),
                    accepted.contains(&shape.name().as_str()),
                    "{family:?} vs {}",
                    shape.name()
                );
            }
        }
    }

    /// The name derived for `data` under `family`, through the real
    /// classify → slot → name chain.
    fn derive(family: ResponseFamily, data: &Value, defs: &Value) -> super::Result<Option<String>> {
        let document = document(data, defs);
        let shape = classify("t.c", &document)?;
        assert!(
            accepts(family, &shape),
            "{family:?} accepts {}",
            shape.name()
        );
        payload_slot("t.c", family, &shape, &document)?
            .map(|slot| payload_name("t.c", defs_of(&document), slot))
            .transpose()
    }

    #[test]
    fn payload_slots_follow_the_family_shape() {
        use ResponseFamily as F;
        let item = json!({"$ref": "#/$defs/Item"});
        let defs = json!({
            "Item": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Found": {"type": "object", "properties": {"found": {"type": "boolean"}, "value": item}, "required": ["found"]},
            "History": {"type": "object", "properties": {"items": {"type": "array", "items": item}}, "required": ["items"]},
            "Page": {"type": "object", "properties": {"items": {"type": "array", "items": item}, "has_more": {"type": "boolean"}}, "required": ["items", "has_more"]},
            "Sample": {"type": "object", "properties": {"items": {"type": "array", "items": item}, "has_more": {"type": "boolean"}, "total_count": {"type": "integer"}}, "required": ["items", "has_more", "total_count"]},
            "Outcome": {"type": "object", "properties": {"result": item}},
            "Batch": {"type": "object", "properties": {"items": {"type": "array", "items": {"$ref": "#/$defs/Outcome"}}, "mode": {}, "status": {}, "applied": {}}, "required": ["items", "mode", "status", "applied"]},
            "Ack": {"type": "object", "properties": {"effect": {"type": "string"}}, "required": ["effect"]},
        });
        let cases: [(F, Value, Option<&str>); 13] = [
            (F::MutationAck, json!({"$ref": "#/$defs/Ack"}), None),
            (F::Maybe, json!({"$ref": "#/$defs/Found"}), Some("Item")),
            (
                F::Maybe,
                json!({"anyOf": [item, {"type": "null"}]}),
                Some("Item"),
            ),
            (
                F::Maybe,
                json!({"type": ["string", "null"]}),
                Some("string"),
            ),
            (
                F::MaybeVec,
                json!({"anyOf": [{"$ref": "#/$defs/History"}, {"type": "null"}]}),
                Some("Item"),
            ),
            (
                F::MaybeVec,
                json!({"type": ["array", "null"], "items": item}),
                Some("Item"),
            ),
            (F::Page, json!({"$ref": "#/$defs/Page"}), Some("Item")),
            (
                F::SamplePage,
                json!({"$ref": "#/$defs/Sample"}),
                Some("Item"),
            ),
            (
                F::SearchResult,
                json!({"type": "array", "items": item}),
                Some("Item"),
            ),
            (
                F::SearchResultDiagnostics,
                json!({"type": "object", "properties": {"matches": {"type": "array", "items": item}, "diagnostics": {}}}),
                Some("Item"),
            ),
            (
                F::BatchResult,
                json!({"$ref": "#/$defs/Batch"}),
                Some("Item"),
            ),
            (
                F::StatusValue,
                json!({"type": "integer", "format": "uint64"}),
                Some("integer"),
            ),
            (
                F::Bare,
                json!({"type": "array", "items": {"type": "integer"}}),
                Some("integer[]"),
            ),
        ];
        for (family, data, expected) in cases {
            let name = derive(family, &data, &defs).expect("derives");
            assert_eq!(name.as_deref(), expected, "{family:?} over {data}");
        }
        assert_eq!(
            derive(F::StatusResponse, &json!({"$ref": "#/$defs/Item"}), &defs).expect("derives"),
            Some("Item".to_owned())
        );
        assert_eq!(
            derive(F::AnalyticsResult, &json!({"$ref": "#/$defs/Item"}), &defs).expect("derives"),
            Some("Item".to_owned())
        );
    }

    #[test]
    fn payloads_without_a_name_are_rejected() {
        use ResponseFamily as F;
        let anonymous = json!({"type": "object", "properties": {"id": {"type": "string"}}});
        let cases: [(F, Value, &str); 7] = [
            (F::StatusResponse, anonymous.clone(), "anonymous record"),
            (
                F::Page,
                json!({"type": "object", "properties": {"items": {"type": "array", "items": anonymous}, "has_more": {}}, "required": ["items", "has_more"]}),
                "anonymous record",
            ),
            (
                F::Bare,
                json!({"type": "array", "items": {"$ref": "#/$defs/Missing"}}),
                "undefined `Missing`",
            ),
            (F::Bare, json!({"type": "array"}), "without `items`"),
            (
                F::Page,
                json!({"type": "object", "properties": {"items": {"type": "array"}, "has_more": {}}, "required": ["items", "has_more"]}),
                "without `items`",
            ),
            (
                F::Maybe,
                json!({"type": "object", "properties": {"found": {}, "value": {"type": "date"}}}),
                "type `date`",
            ),
            (
                F::Bare,
                json!({"type": "array", "items": {"description": "untyped"}}),
                "no single type",
            ),
        ];
        for (family, data, reason) in cases {
            let rejected = derive(family, &data, &json!({})).expect_err("rejected");
            assert!(
                rejected.to_string().contains(reason),
                "{family:?} over {data}: {rejected}"
            );
        }
        // A shape the family accepts but that carries no payload slot.
        let rejected = payload_slot(
            "t.c",
            F::Maybe,
            &WireShape::Scalar("boolean".into()),
            &document(&json!({"type": "boolean"}), &json!({})),
        )
        .expect_err("no slot");
        assert!(
            rejected.to_string().contains("no payload slot"),
            "{rejected}"
        );
    }

    #[test]
    fn ledgered_payloads_must_name_a_def_or_a_primitive() {
        let document = json!({"$defs": {"Info": {"type": "object"}}});
        let defs = defs_of(&document);
        for payload in ["Info", "Info[]", "integer", "string[]"] {
            assert!(names_a_def(payload, defs), "{payload}");
        }
        for payload in ["Infoo", "Vec<Info>", "{payload}", "u64", ""] {
            assert!(!names_a_def(payload, defs), "{payload}");
        }
        assert!(!names_a_def("Info", None));
        assert!(names_a_def("boolean", None));
    }

    #[test]
    fn encodings_follow_the_shape() {
        let nullable = |inner: WireShape| WireShape::Nullable(Box::new(inner));
        assert_eq!(
            encoding(&WireShape::FoundValue),
            Some(CliWireEncoding::FoundValue)
        );
        assert_eq!(
            encoding(&nullable(WireShape::Record)),
            Some(CliWireEncoding::Nullable)
        );
        assert_eq!(
            encoding(&nullable(WireShape::Scalar("string".into()))),
            Some(CliWireEncoding::Nullable)
        );
        assert_eq!(
            encoding(&nullable(WireShape::Items)),
            Some(CliWireEncoding::Items)
        );
        assert_eq!(
            encoding(&nullable(WireShape::Array)),
            Some(CliWireEncoding::Array)
        );
        for shape in [
            WireShape::Scalar("boolean".into()),
            WireShape::Array,
            WireShape::MutationAck,
            WireShape::Page,
            WireShape::Items,
            WireShape::Record,
        ] {
            assert_eq!(encoding(&shape), None, "{}", shape.name());
        }
    }
}
