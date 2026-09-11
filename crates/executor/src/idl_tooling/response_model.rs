//! Response-model guard (#3313; S0c of the CLI output contract, #3314).
//!
//! A command's `response_model` names a *family* (`MutationAck<T>`,
//! `Maybe<T>`, `Page<T>`, …) and every family implies a wire shape. Until now
//! nothing checked that the shape the generated schema actually describes is
//! the one the declaration promises, so a `MutationAck<…>` could ship a bare
//! `boolean` and the docs, SDKs and renderer would all believe the declaration.
//!
//! This module classifies the schema's `response.data` into a [`WireShape`],
//! accepts it against the declared [`ResponseFamily`], and requires every
//! disagreement to be listed in the shrink-only
//! `response-model-divergences.yaml` ledger with `wire_status: transitional`.
//! The declaration is the target; the ledger names the wire that has not yet
//! caught up. Normalising a listed wire is a separate, tracked change — this
//! guard only stops the set from growing silently.
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
    /// The resolved `response_model` the command declares (the target shape).
    declared: String,
    /// The [`WireShape::name`] the schema carries today.
    wire: String,
    /// The issue tracking the wire's normalisation.
    issue: NonZeroU64,
}

/// Family prefixes, longest first: `Maybe<Vec<` shadows `Maybe<`.
const FAMILY_PREFIXES: [(&str, ResponseFamily); 10] = [
    ("Maybe<Vec<", ResponseFamily::MaybeVec),
    ("Maybe<", ResponseFamily::Maybe),
    ("MutationAck<", ResponseFamily::MutationAck),
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
    /// A bare DTO name: the command promises a specific record, not a family.
    Bare,
}

impl ResponseFamily {
    /// Parses the family prefix of a resolved `response_model`.
    pub(super) fn from_declaration(command_id: &str, declared: &str) -> Result<Self> {
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
    let defs = document.get("$defs").and_then(Value::as_object);
    let data = document
        .pointer("/response/properties/data")
        .ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` schema has no response data"
            ))
        })?;
    classify_node(command_id, defs, data)
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
        let name = reference.strip_prefix("#/$defs/").ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` schema references `{reference}` outside `$defs`"
            ))
        })?;
        node = defs.and_then(|defs| defs.get(name)).ok_or_else(|| {
            invalid(format!(
                "command `{command_id}` schema references undefined `{name}`"
            ))
        })?;
    }
    Ok(node)
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

/// The guard `generate` and `check` run: every command's wire either matches
/// its declared family or is a ledgered, transitional divergence — and the
/// ledger names nothing else.
pub(super) fn enforce_response_models(
    idl_root: &Path,
    index: &CommandIndex,
    documents: &BTreeMap<String, Value>,
) -> Result<()> {
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
    for command in &index.commands {
        let document = documents
            .get(&command.id)
            .ok_or_else(|| invalid(format!("no schema document for `{}`", command.id)))?;
        let family = ResponseFamily::from_declaration(&command.id, &command.response_model)?;
        let shape = classify(&command.id, document)?;
        let row = rows.remove(command.id.as_str());
        check_command(command, family, &shape, row)?;
    }
    if let Some(unknown) = rows.into_keys().next() {
        return Err(invalid(format!(
            "{RESPONSE_MODEL_DIVERGENCES_FILE} lists `{unknown}` which is not a command id; remove it"
        )));
    }
    Ok(())
}

fn check_command(
    command: &super::ResolvedCommand,
    family: ResponseFamily,
    shape: &WireShape,
    row: Option<&DivergenceRow>,
) -> Result<()> {
    let id = &command.id;
    let declared = &command.response_model;
    if accepts(family, shape) {
        return match row {
            None => Ok(()),
            Some(row) => Err(invalid(format!(
                "`{id}` now carries {} as its `{declared}` declares; remove its row (#{}) from \
                 {RESPONSE_MODEL_DIVERGENCES_FILE} (the ledger may only shrink)",
                shape.describe(),
                row.issue
            ))),
        };
    }
    let Some(row) = row else {
        return Err(invalid(format!(
            "command `{id}` declares `{declared}` ({}) but its schema carries {}; correct the \
             declaration, or if the wire is what is wrong, list the command in \
             {RESPONSE_MODEL_DIVERGENCES_FILE} and mark it `wire_status: transitional`",
            family.describe(),
            shape.describe()
        )));
    };
    if row.declared != *declared {
        return Err(invalid(format!(
            "{RESPONSE_MODEL_DIVERGENCES_FILE} row for `{id}` records declared `{}` but the \
             command declares `{declared}`",
            row.declared
        )));
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{accepts, classify, encoding, ResponseFamily, WireShape};
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
            ("MutationAck<BranchItem>", ResponseFamily::MutationAck),
            ("Maybe<VersionedValue>", ResponseFamily::Maybe),
            ("Maybe<Vec<VersionedValue>>", ResponseFamily::MaybeVec),
            ("Page<BranchItem, String>", ResponseFamily::Page),
            ("SamplePage<KvItem>", ResponseFamily::SamplePage),
            ("SearchResult<VectorMatch>", ResponseFamily::SearchResult),
            (
                "SearchResult<VectorMatch> + IndexDiagnostics",
                ResponseFamily::SearchResultDiagnostics,
            ),
            ("StatusValue<bool>", ResponseFamily::StatusValue),
            ("StatusResponse<HealthInfo>", ResponseFamily::StatusResponse),
            ("AnalyticsResult<f64>", ResponseFamily::AnalyticsResult),
            ("BatchResult<KvItem>", ResponseFamily::BatchResult),
            ("EmbedResponse", ResponseFamily::Bare),
        ];
        for (declared, expected) in cases {
            let family = ResponseFamily::from_declaration("t.c", declared).expect("known family");
            assert_eq!(family, expected, "{declared}");
        }
        let rejected = ResponseFamily::from_declaration("t.c", "Option<KvItem>")
            .expect_err("an unknown generic family is rejected");
        assert!(
            rejected.to_string().contains("unknown family"),
            "{rejected}"
        );
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
