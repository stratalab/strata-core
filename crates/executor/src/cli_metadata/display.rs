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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CliDisplayAs, CliDisplayDecl, CliRenderRule};

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
