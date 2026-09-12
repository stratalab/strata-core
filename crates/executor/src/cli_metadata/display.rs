//! CLI display declarations (`docs/design/cli-output-contract.md` §4 R2).
//!
//! A kind declares a *render rule* (`render:`) — the layout its family gets.
//! A command declares a *display shape* (`display:`) — which facts appear, in
//! which order, and how. Both are authored in the IDL; the resolver joins them
//! into `cli-command-index.json`, and this module holds the wire types and
//! the rule ⇔ shape compatibility check the runtime re-runs on the embedded
//! index. Pointer resolution against the generated schemas is an
//! authoring-time guard and lives in the IDL tooling.
//!
//! The Python SDK is a sibling of the CLI, never a wrapper: nothing here is
//! written to `command-index.json`, and the tooling guards that boundary.

use serde::de::{Error as DeError, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Layout rule a kind declares; every kind carries exactly one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliRenderRule {
    /// Writes: one receipt line, idempotent miss on stderr.
    MutationAck,
    /// `Maybe<T>` reads: the value, or a not-found line.
    Optional,
    /// Version history tables.
    History,
    /// Cursor pages and samples.
    Page,
    /// Ranked search results, optionally with a diagnostics block.
    Search,
    /// Map-valued analytics keyed by node.
    Analytics,
    /// Scalar or record status reads.
    StatusValue,
    /// Multi-field summaries and action receipts.
    StatusSections,
    /// Itemwise batch tables.
    Batch,
}

impl CliRenderRule {
    /// Every rule, in declaration order.
    pub const ALL: [Self; 9] = [
        Self::MutationAck,
        Self::Optional,
        Self::History,
        Self::Page,
        Self::Search,
        Self::Analytics,
        Self::StatusValue,
        Self::StatusSections,
        Self::Batch,
    ];

    /// The authored spelling (a kind's `render:`), identical to the
    /// serde name — pinned by `render_rule_names_match_serde`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MutationAck => "mutation_ack",
            Self::Optional => "optional",
            Self::History => "history",
            Self::Page => "page",
            Self::Search => "search",
            Self::Analytics => "analytics",
            Self::StatusValue => "status_value",
            Self::StatusSections => "status_sections",
            Self::Batch => "batch",
        }
    }
}

/// How an `optional`, `history` or `page` command spells its value on the
/// wire (contract §Root E). Resolved from the generated schema at authoring
/// time and carried in the CLI index as `encoding`, so the renderer reads it
/// rather than sniffing the response — a sample page announces itself here,
/// never through the presence of `total_count`. Every other rule carries none.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliWireEncoding {
    /// `Maybe<T>` as `{found, value}`.
    FoundValue,
    /// `Maybe<T>` as a nullable `data`.
    Nullable,
    /// `Maybe<Vec<T>>` as a nullable `{items}`.
    Items,
    /// `Maybe<Vec<T>>` as a nullable bare array.
    Array,
    /// `{items, has_more, cursor?}`.
    Page,
    /// `{items, has_more, total_count, cursor?}` — a page drawn from a
    /// population the renderer reports when the page is smaller than it.
    SamplePage,
}

impl CliWireEncoding {
    /// Every encoding, in declaration order.
    pub const ALL: [Self; 6] = [
        Self::FoundValue,
        Self::Nullable,
        Self::Items,
        Self::Array,
        Self::Page,
        Self::SamplePage,
    ];

    /// The index spelling, identical to the serde name — pinned by
    /// `wire_encoding_names_match_serde`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FoundValue => "found_value",
            Self::Nullable => "nullable",
            Self::Items => "items",
            Self::Array => "array",
            Self::Page => "page",
            Self::SamplePage => "sample_page",
        }
    }
}

/// A command's `display:` declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliDisplayDecl {
    /// A hand-written renderer arm; no declaration to validate.
    Bespoke,
    /// A declared shape rendered by the family rule.
    Declared(CliDisplay),
}

const BESPOKE: &str = "bespoke";

impl Serialize for CliDisplayDecl {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Bespoke => serializer.serialize_str(BESPOKE),
            Self::Declared(display) => display.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CliDisplayDecl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DeclVisitor;

        impl<'de> Visitor<'de> for DeclVisitor {
            type Value = CliDisplayDecl;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("`bespoke` or a display shape map")
            }

            fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
                if value == BESPOKE {
                    Ok(CliDisplayDecl::Bespoke)
                } else {
                    Err(E::unknown_variant(value, &[BESPOKE]))
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                CliDisplay::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(CliDisplayDecl::Declared)
            }
        }

        deserializer.deserialize_any(DeclVisitor)
    }
}

/// Declared display shape. Every `/…` string is a JSON pointer into the
/// command's generated schema (`/data/…` into the response, `/request/…`
/// into the request); the authoring guard resolves each one.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliDisplay {
    /// Human receipt template for writes and action receipts. `{/ptr}`
    /// placeholders take one optional filter (`|plural:<noun>`, `|size`,
    /// `|bytes`, `|len`); `{verb}` reads `/data/effect/kind`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<String>,
    /// Noun for the idempotent miss line (`no such <noun>: <identity>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noun: Option<String>,
    /// What `--raw` prints for a receipt: one pointer per column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity: Vec<String>,
    /// Bare payload pointer for value reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Presentation of `value`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<CliDisplayAs>,
    /// Table columns; pointers step into the row array with `*`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<CliDisplayField>,
    /// Record fields: selects and orders the facts a human sees.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<CliDisplayField>,
    /// Map-valued analytics payload (object keyed by node).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub map: Option<String>,
    /// Column header for the map's values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// Row order for the map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<CliDisplaySort>,
}

/// One declared column or field.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliDisplayField {
    /// Schema pointer.
    pub field: String,
    /// Header override; defaults to the upper-cased wire name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// Presentation override.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<CliDisplayAs>,
    /// A record-valued field's own selection, rendered as an indented block.
    /// Same rule one level down: the declaration decides which facts appear.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<CliDisplayField>,
}

/// Presentation of a value beyond its scalar text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliDisplayAs {
    /// Base64 payload shown as UTF-8 text when it decodes cleanly.
    Bytes,
    /// Arbitrary JSON shown compact.
    Json,
    /// Microsecond timestamp shown as UTC.
    Date,
    /// Byte count shown with a unit.
    Size,
    /// Floating point shown with fixed precision.
    Float,
    /// Array of scalars shown space-joined.
    List,
    /// Array of records shown as a nested table (columns from the schema).
    Table,
}

impl CliDisplayAs {
    /// Every presentation, for exhaustive checks.
    pub const ALL: [Self; 7] = [
        Self::Bytes,
        Self::Json,
        Self::Date,
        Self::Size,
        Self::Float,
        Self::List,
        Self::Table,
    ];

    /// The authored spelling (`as:` in a display declaration), identical to
    /// the serde name — pinned by `display_as_names_match_serde`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Json => "json",
            Self::Date => "date",
            Self::Size => "size",
            Self::Float => "float",
            Self::List => "list",
            Self::Table => "table",
        }
    }
}

/// Row order for a map-valued analytics payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CliDisplaySort {
    /// Value descending.
    Desc,
    /// Value ascending.
    Asc,
    /// Key ascending.
    Key,
}

/// A parsed `receipt:` template (contract §4 R2): literal text and
/// placeholders in authored order. The grammar lives here, once — the
/// authoring guard resolves each placeholder against the generated schema
/// and the CLI renderer resolves it against the response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptTemplate {
    segments: Vec<ReceiptSegment>,
}

/// One piece of a receipt template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiptSegment {
    /// Text between placeholders, copied through verbatim.
    Literal(String),
    /// A `{…}` placeholder.
    Placeholder(ReceiptPlaceholder),
}

/// The body of a `{…}` placeholder, or one `identity` entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiptPlaceholder {
    /// `{verb}`: the effect kind (`/data/effect/kind`) as a past-tense word.
    Verb,
    /// `{/pointer}` or `{/pointer|filter}`: one value from the response
    /// (`/data/…`) or the request (`/request/…`).
    Value(ReceiptValue),
}

/// A pointer placeholder: where the value lives and how it is shown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptValue {
    /// JSON pointer into the envelope (`/data/…`) or the request (`/request/…`).
    pub pointer: String,
    /// The one optional filter after `|`.
    pub filter: Option<ReceiptFilter>,
}

/// A placeholder filter; each one names the schema type it needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiptFilter {
    /// A base64 string shown as text when it decodes cleanly.
    Bytes,
    /// An integer byte count shown with a unit.
    Size,
    /// An array shown as its length.
    Len,
    /// An integer count shown with its noun, pluralised.
    Plural(String),
}

impl ReceiptTemplate {
    /// Parses a receipt template. Grammar only: placeholders must be
    /// non-empty and balanced, a pointer names one value (no `*`), and a
    /// filter is one of the four the contract defines.
    pub fn parse(template: &str) -> Result<Self, String> {
        let mut segments = Vec::new();
        let mut literal = String::new();
        let mut open: Option<usize> = None;
        for (index, ch) in template.char_indices() {
            match (ch, open) {
                ('{', None) => open = Some(index + 1),
                ('}', Some(start)) => {
                    let body = &template[start..index];
                    if body.is_empty() {
                        return Err(format!("receipt `{template}` has an empty placeholder"));
                    }
                    if !literal.is_empty() {
                        segments.push(ReceiptSegment::Literal(std::mem::take(&mut literal)));
                    }
                    segments.push(ReceiptSegment::Placeholder(ReceiptPlaceholder::parse(
                        body,
                    )?));
                    open = None;
                }
                ('{' | '}', _) => {
                    return Err(format!("receipt `{template}` has unbalanced braces"));
                }
                (_, None) => literal.push(ch),
                (_, Some(_)) => {}
            }
        }
        if open.is_some() {
            return Err(format!("receipt `{template}` has unbalanced braces"));
        }
        if !literal.is_empty() {
            segments.push(ReceiptSegment::Literal(literal));
        }
        Ok(Self { segments })
    }

    /// The template's pieces in authored order.
    pub fn segments(&self) -> &[ReceiptSegment] {
        &self.segments
    }

    /// The template's placeholders in authored order.
    pub fn placeholders(&self) -> impl Iterator<Item = &ReceiptPlaceholder> {
        self.segments.iter().filter_map(|segment| match segment {
            ReceiptSegment::Placeholder(placeholder) => Some(placeholder),
            ReceiptSegment::Literal(_) => None,
        })
    }
}

impl ReceiptPlaceholder {
    /// Parses one placeholder body: `verb`, or a pointer with at most one
    /// `|filter`.
    pub fn parse(body: &str) -> Result<Self, String> {
        if body == "verb" {
            return Ok(Self::Verb);
        }
        let (pointer, filter) = body
            .split_once('|')
            .map_or((body, None), |(pointer, filter)| (pointer, Some(filter)));
        if pointer.contains('*') {
            return Err(format!(
                "placeholder `{{{body}}}` steps into every item with `*`; a receipt names one value (use an index)"
            ));
        }
        Ok(Self::Value(ReceiptValue {
            pointer: pointer.to_owned(),
            filter: filter.map(ReceiptFilter::parse).transpose()?,
        }))
    }
}

impl ReceiptFilter {
    /// Parses the text after a placeholder's `|`.
    pub fn parse(filter: &str) -> Result<Self, String> {
        let (name, argument) = filter
            .split_once(':')
            .map_or((filter, None), |(name, argument)| (name, Some(argument)));
        match (name, argument) {
            ("plural", Some(noun)) if !noun.trim().is_empty() => Ok(Self::Plural(noun.to_owned())),
            ("plural", _) => Err("`|plural` needs a noun: `|plural:row`".to_owned()),
            ("size", None) => Ok(Self::Size),
            ("bytes", None) => Ok(Self::Bytes),
            ("len", None) => Ok(Self::Len),
            _ => Err(format!(
                "unknown filter `|{filter}`; filters are `|plural:<noun>`, `|size`, `|bytes`, `|len`"
            )),
        }
    }

    /// The filter's name as authored (`plural`, `size`, `bytes`, `len`).
    pub fn name(&self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Size => "size",
            Self::Len => "len",
            Self::Plural(_) => "plural",
        }
    }
}

/// The shape a `CliDisplay` takes, derived from which keys it sets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CliDisplayShape {
    /// `receipt` (+ `identity` for writes, + `noun`).
    Receipt,
    /// `value` (+ `as`).
    Value,
    /// `fields`.
    Fields,
    /// `columns` (+ `fields` under the search rule).
    Columns,
    /// `map` + `header` + `sort`.
    Map,
}

impl CliDisplayShape {
    const fn name(self) -> &'static str {
        match self {
            Self::Receipt => "receipt",
            Self::Value => "value",
            Self::Fields => "fields",
            Self::Columns => "columns",
            Self::Map => "map",
        }
    }
}

impl CliDisplay {
    /// Classifies the declaration by its primary key and rejects keys that
    /// do not belong to that shape.
    pub fn shape(&self) -> Result<CliDisplayShape, String> {
        let shape = match (
            &self.receipt,
            &self.value,
            &self.map,
            !self.columns.is_empty(),
            !self.fields.is_empty(),
        ) {
            (Some(_), None, None, false, false) => CliDisplayShape::Receipt,
            (None, Some(_), None, false, false) => CliDisplayShape::Value,
            (None, None, Some(_), false, false) => CliDisplayShape::Map,
            (None, None, None, true, _) => CliDisplayShape::Columns,
            (None, None, None, false, true) => CliDisplayShape::Fields,
            _ => {
                return Err(
                    "declares no single shape; set exactly one of `receipt`, `value`, `map`, `columns`, `fields`"
                        .to_owned(),
                )
            }
        };
        let stray = [
            (
                shape != CliDisplayShape::Receipt && self.noun.is_some(),
                "noun",
            ),
            (
                shape != CliDisplayShape::Receipt && !self.identity.is_empty(),
                "identity",
            ),
            (shape != CliDisplayShape::Value && self.as_.is_some(), "as"),
            (
                shape != CliDisplayShape::Map && self.header.is_some(),
                "header",
            ),
            (shape != CliDisplayShape::Map && self.sort.is_some(), "sort"),
        ]
        .into_iter()
        .find_map(|(is_stray, key)| is_stray.then_some(key));
        if let Some(key) = stray {
            return Err(format!(
                "`{key}` is not part of the `{}` shape",
                shape.name()
            ));
        }
        match shape {
            CliDisplayShape::Map if self.header.is_none() || self.sort.is_none() => {
                Err("`map` requires `header` and `sort`".to_owned())
            }
            _ => Ok(shape),
        }
    }
}

/// Which shapes a render rule accepts; the rule table of R2.
fn allowed_shapes(rule: CliRenderRule) -> &'static [CliDisplayShape] {
    use CliDisplayShape::{Columns, Fields, Map, Receipt, Value};
    match rule {
        CliRenderRule::MutationAck => &[Receipt],
        CliRenderRule::Optional => &[Value, Fields],
        // `search` alone may pair `columns` with a `fields` block; that is
        // checked below, the row shape is the same.
        CliRenderRule::History
        | CliRenderRule::Page
        | CliRenderRule::Batch
        | CliRenderRule::Search => &[Columns],
        CliRenderRule::Analytics => &[Map],
        CliRenderRule::StatusValue => &[Value, Fields, Columns],
        CliRenderRule::StatusSections => &[Fields, Columns, Receipt],
    }
}

/// Checks that a command's declaration fits its kind's render rule. Shape
/// only — pointers are resolved by the authoring guard, not at runtime.
pub fn validate_display_shape(
    command_id: &str,
    rule: CliRenderRule,
    decl: &CliDisplayDecl,
) -> Result<(), String> {
    let CliDisplayDecl::Declared(display) = decl else {
        return Ok(());
    };
    let shape = display
        .shape()
        .map_err(|reason| format!("command `{command_id}` display {reason}"))?;
    if !allowed_shapes(rule).contains(&shape) {
        return Err(format!(
            "command `{command_id}` declares a `{}` display but its kind renders `{}`",
            shape.name(),
            rule.as_str()
        ));
    }
    if shape == CliDisplayShape::Columns
        && !display.fields.is_empty()
        && rule != CliRenderRule::Search
    {
        return Err(format!(
            "command `{command_id}` pairs `columns` with `fields`, which only the `search` rule renders"
        ));
    }
    // `--raw` for a write receipt is its identity; for an action receipt it is
    // the record's key/value lines, so `identity` is required by the one rule
    // and has no meaning under the other.
    if shape == CliDisplayShape::Receipt {
        let has_identity = !display.identity.is_empty();
        if rule == CliRenderRule::MutationAck && !has_identity {
            return Err(format!(
                "command `{command_id}` declares a `receipt` without `identity`; `--raw` for a write is its identity"
            ));
        }
        if rule == CliRenderRule::StatusSections && has_identity {
            return Err(format!(
                "command `{command_id}` declares `identity` under `status_sections`, where `--raw` is the record's key/value lines"
            ));
        }
        // The renderer reads the template straight off the embedded index, so
        // the index it accepts must already parse; the authoring guard adds
        // schema resolution on top of this grammar.
        let grammar = |reason: String| format!("command `{command_id}` display {reason}");
        if let Some(receipt) = display.receipt.as_deref() {
            ReceiptTemplate::parse(receipt).map_err(grammar)?;
        }
        for entry in &display.identity {
            ReceiptPlaceholder::parse(entry).map_err(grammar)?;
        }
    }
    Ok(())
}

/// Checks that a command's wire encoding is the one its render rule needs:
/// `optional` reads `found_value` or `nullable`, `history` reads `items` or
/// `array`, `page` reads `page` or `sample_page`, and no other rule carries
/// one. A stable `optional`/`history`/`page` command must carry an encoding;
/// only a `transitional` one — whose wire is a ledgered divergence from its
/// declared family — may lack it.
pub fn validate_encoding_shape(
    command_id: &str,
    rule: CliRenderRule,
    encoding: Option<CliWireEncoding>,
    wire_status: &str,
) -> Result<(), String> {
    use CliRenderRule::{History, Optional, Page};
    use CliWireEncoding::{Array, FoundValue, Items, Nullable};
    match (rule, encoding) {
        (Optional, Some(FoundValue | Nullable))
        | (History, Some(Items | Array))
        | (Page, Some(CliWireEncoding::Page | CliWireEncoding::SamplePage)) => Ok(()),
        (Optional | History | Page, None) => {
            if wire_status == "transitional" {
                Ok(())
            } else {
                Err(format!(
                    "command `{command_id}` renders `{}` but its stable wire resolved to no encoding",
                    rule.as_str()
                ))
            }
        }
        (Optional | History | Page, Some(other)) => Err(format!(
            "command `{command_id}` renders `{}` but its wire encoding is `{}`",
            rule.as_str(),
            other.as_str()
        )),
        (_, Some(other)) => Err(format!(
            "command `{command_id}` carries wire encoding `{}` but its kind renders `{}`, which reads none",
            other.as_str(),
            rule.as_str()
        )),
        (_, None) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_display_shape, validate_encoding_shape, CliDisplay, CliDisplayAs, CliDisplayDecl,
        CliRenderRule, CliWireEncoding, ReceiptFilter, ReceiptPlaceholder, ReceiptSegment,
        ReceiptTemplate, ReceiptValue,
    };

    fn value(pointer: &str, filter: Option<ReceiptFilter>) -> ReceiptSegment {
        ReceiptSegment::Placeholder(ReceiptPlaceholder::Value(ReceiptValue {
            pointer: pointer.to_owned(),
            filter,
        }))
    }

    fn literal(text: &str) -> ReceiptSegment {
        ReceiptSegment::Literal(text.to_owned())
    }

    #[test]
    fn receipt_template_splits_literals_and_placeholders_in_order() {
        let template = ReceiptTemplate::parse(
            "{verb} {/data/key|bytes} ({/data/rows|plural:row}, {/data/n|len} of {/data/b|size})",
        )
        .expect("parses");
        assert_eq!(
            template.segments(),
            [
                ReceiptSegment::Placeholder(ReceiptPlaceholder::Verb),
                literal(" "),
                value("/data/key", Some(ReceiptFilter::Bytes)),
                literal(" ("),
                value("/data/rows", Some(ReceiptFilter::Plural("row".to_owned()))),
                literal(", "),
                value("/data/n", Some(ReceiptFilter::Len)),
                literal(" of "),
                value("/data/b", Some(ReceiptFilter::Size)),
                literal(")"),
            ]
        );
        assert_eq!(template.placeholders().count(), 5);
        // No placeholders at all is a legal (if pointless) receipt; an
        // adjacent pair yields no empty literal between them.
        assert_eq!(
            ReceiptTemplate::parse("done").expect("parses").segments(),
            [literal("done")]
        );
        assert_eq!(
            ReceiptTemplate::parse("{/data/a}{/data/b}")
                .expect("parses")
                .segments(),
            [value("/data/a", None), value("/data/b", None)]
        );
        assert_eq!(ReceiptTemplate::parse("").expect("parses").segments(), []);
    }

    #[test]
    fn receipt_template_rejects_bad_braces_and_filters() {
        let reject = |template: &str| ReceiptTemplate::parse(template).expect_err("rejected");
        assert_eq!(reject("{}"), "receipt `{}` has an empty placeholder");
        assert_eq!(
            reject("{/data/a"),
            "receipt `{/data/a` has unbalanced braces"
        );
        assert_eq!(
            reject("/data/a}"),
            "receipt `/data/a}` has unbalanced braces"
        );
        assert_eq!(
            reject("{{/data/a}}"),
            "receipt `{{/data/a}}` has unbalanced braces"
        );
        assert_eq!(
            reject("{/data/items/*/name}"),
            "placeholder `{/data/items/*/name}` steps into every item with `*`; a receipt names one value (use an index)"
        );
        assert_eq!(
            reject("{/data/a|hex}"),
            "unknown filter `|hex`; filters are `|plural:<noun>`, `|size`, `|bytes`, `|len`"
        );
        assert_eq!(
            reject("{/data/a|plural}"),
            "`|plural` needs a noun: `|plural:row`"
        );
        assert_eq!(
            reject("{/data/a|plural: }"),
            "`|plural` needs a noun: `|plural:row`"
        );
        assert_eq!(
            reject("{/data/a|size:kb}"),
            "unknown filter `|size:kb`; filters are `|plural:<noun>`, `|size`, `|bytes`, `|len`"
        );
        // Only the first `|` splits; the rest is the filter's text.
        assert_eq!(
            reject("{/data/a|bytes|len}"),
            "unknown filter `|bytes|len`; filters are `|plural:<noun>`, `|size`, `|bytes`, `|len`"
        );
    }

    #[test]
    fn placeholder_and_filter_names_round_trip() {
        assert_eq!(
            ReceiptPlaceholder::parse("verb").expect("parses"),
            ReceiptPlaceholder::Verb
        );
        // `verb` with a filter is a pointer named `verb`, left for the guard
        // to resolve (and refuse).
        assert_eq!(
            ReceiptPlaceholder::parse("verb|len").expect("parses"),
            ReceiptPlaceholder::Value(ReceiptValue {
                pointer: "verb".to_owned(),
                filter: Some(ReceiptFilter::Len),
            })
        );
        for (text, filter) in [
            ("bytes", ReceiptFilter::Bytes),
            ("size", ReceiptFilter::Size),
            ("len", ReceiptFilter::Len),
            ("plural:vector", ReceiptFilter::Plural("vector".to_owned())),
        ] {
            let parsed = ReceiptFilter::parse(text).expect("parses");
            assert_eq!(parsed, filter);
            let name = text.split(':').next().expect("a filter has a name");
            assert_eq!(parsed.name(), name);
        }
    }

    #[test]
    fn a_receipt_shape_must_parse_to_be_accepted() {
        let display = |receipt: &str, identity: &[&str]| {
            CliDisplayDecl::Declared(CliDisplay {
                receipt: Some(receipt.to_owned()),
                identity: identity.iter().map(|entry| (*entry).to_owned()).collect(),
                ..CliDisplay::default()
            })
        };
        let rule = CliRenderRule::MutationAck;
        validate_display_shape("t.c", rule, &display("{verb} {/data/key}", &["/data/key"]))
            .expect("a well-formed receipt is accepted");
        let error =
            validate_display_shape("t.c", rule, &display("{verb} {/data/key", &["/data/key"]))
                .expect_err("an unbalanced receipt is refused");
        assert_eq!(
            error,
            "command `t.c` display receipt `{verb} {/data/key` has unbalanced braces"
        );
        let error = validate_display_shape("t.c", rule, &display("{verb}", &["/data/key|hex"]))
            .expect_err("an identity entry with an unknown filter is refused");
        assert_eq!(
            error,
            "command `t.c` display unknown filter `|hex`; filters are `|plural:<noun>`, `|size`, `|bytes`, `|len`"
        );
    }

    #[test]
    fn wire_encoding_names_match_serde() {
        for encoding in CliWireEncoding::ALL {
            let json = serde_json::to_value(encoding).expect("encoding serializes");
            assert_eq!(json, serde_json::Value::from(encoding.as_str()));
            let back: CliWireEncoding = serde_json::from_value(json).expect("encoding round-trips");
            assert_eq!(back, encoding);
        }
    }

    #[test]
    fn encoding_shape_truth_table() {
        use CliWireEncoding::{Array, FoundValue, Items, Nullable, Page, SamplePage};
        for rule in CliRenderRule::ALL {
            for encoding in CliWireEncoding::ALL.map(Some).into_iter().chain([None]) {
                let stable = validate_encoding_shape("t.c", rule, encoding, "stable").is_ok();
                let transitional =
                    validate_encoding_shape("t.c", rule, encoding, "transitional").is_ok();
                let reads_one = matches!(
                    rule,
                    CliRenderRule::Optional | CliRenderRule::History | CliRenderRule::Page
                );
                let expected_stable = matches!(
                    (rule, encoding),
                    (CliRenderRule::Optional, Some(FoundValue | Nullable))
                        | (CliRenderRule::History, Some(Items | Array))
                        | (CliRenderRule::Page, Some(Page | SamplePage))
                ) || (!reads_one && encoding.is_none());
                assert_eq!(stable, expected_stable, "stable {rule:?} {encoding:?}");
                // Transitional relaxes exactly one cell: a missing encoding
                // under a rule that reads one.
                let relaxed = encoding.is_none() && reads_one;
                assert_eq!(
                    transitional,
                    expected_stable || relaxed,
                    "transitional {rule:?} {encoding:?}"
                );
            }
        }
    }

    #[test]
    fn render_rule_names_match_serde() {
        for rule in CliRenderRule::ALL {
            let json = serde_json::to_value(rule).expect("rule serializes");
            assert_eq!(json, serde_json::Value::from(rule.as_str()));
            let back: CliRenderRule = serde_json::from_value(json).expect("rule round-trips");
            assert_eq!(back, rule);
        }
    }

    #[test]
    fn display_as_names_match_serde() {
        for as_ in CliDisplayAs::ALL {
            let json = serde_json::to_value(as_).expect("presentation serializes");
            assert_eq!(json, serde_json::Value::from(as_.as_str()));
            let back: CliDisplayAs =
                serde_json::from_value(json).expect("presentation round-trips");
            assert_eq!(back, as_);
        }
    }

    #[test]
    fn display_decl_round_trips_bespoke_and_declared() {
        let bespoke: CliDisplayDecl = serde_json::from_str("\"bespoke\"").expect("parses");
        assert_eq!(bespoke, CliDisplayDecl::Bespoke);
        assert_eq!(
            serde_json::to_string(&bespoke).expect("serializes"),
            "\"bespoke\""
        );
        let declared: CliDisplayDecl =
            serde_json::from_str(r#"{"value":"/data","as":"json"}"#).expect("parses");
        assert!(matches!(declared, CliDisplayDecl::Declared(_)));
        assert_eq!(
            serde_json::to_string(&declared).expect("serializes"),
            r#"{"value":"/data","as":"json"}"#
        );
        let error = serde_json::from_str::<CliDisplayDecl>("\"custom\"")
            .expect_err("only `bespoke` is a bare word");
        assert!(error.to_string().contains("unknown variant `custom`"));
        let error = serde_json::from_str::<CliDisplayDecl>(r#"{"colums":[]}"#)
            .expect_err("unknown keys are rejected");
        assert!(error.to_string().contains("unknown field `colums`"));
        // Neither a word nor a map: the visitor says what it expected.
        let error = serde_json::from_str::<CliDisplayDecl>("42")
            .expect_err("a number is neither `bespoke` nor a shape");
        assert!(
            error
                .to_string()
                .contains("expected `bespoke` or a display shape map"),
            "{error}"
        );
    }
}
