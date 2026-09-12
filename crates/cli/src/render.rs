//! CLI response rendering.

use base64::Engine as _;
use serde::Serialize;
use serde_json::Value;
use strata_executor::cli_metadata::{
    CliDisplay, CliDisplayAs, CliDisplayDecl, CliDisplayField, CliDisplayShape, CliDisplaySort,
    CliRenderRule, CliWireEncoding, ReceiptFilter, ReceiptPlaceholder, ReceiptSegment,
    ReceiptTemplate, ReceiptValue,
};
use strata_executor::{Command, Output};

use crate::catalog;
use crate::options::Format;
use crate::table::{Cell, Table};
use crate::CliError;

// Writing to a String is infallible; the macro keeps the call sites terse.
macro_rules! line {
    ($out:expr, $($arg:tt)*) => {{
        use std::fmt::Write as _;
        let _ = writeln!($out, $($arg)*);
    }};
}

/// What one command prints, by channel (output contract R5): `stdout` carries
/// the answer in the chosen format, `stderr` the feedback that needs a
/// reader's attention — today, the miss line of a write (`no such key: k`).
/// Wasm-safe: the binary prints the two streams (`print_output`), the
/// playground joins them (`run_line`), the contract harness snapshots them as
/// separate cells.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Rendered {
    /// The answer, exactly as the binary writes it: JSON/pretty envelopes and
    /// human/raw lines are newline-terminated; a missed write prints nothing.
    pub stdout: String,
    /// Feedback, newline-terminated when present. Always empty in `--json`
    /// and `--pretty`, whose envelope already carries the same fact.
    pub stderr: String,
}

impl Rendered {
    fn stdout(text: String) -> Self {
        Self {
            stdout: text,
            stderr: String::new(),
        }
    }

    /// Both channels in the order a terminal shows them for one command —
    /// the playground's single-string transcript.
    pub fn stdout_then_stderr(self) -> String {
        let mut text = self.stdout;
        text.push_str(&self.stderr);
        text
    }
}

/// What a rendering knows about the command that produced its output: the
/// command's `display:` declaration from the embedded catalog, parsed into
/// the shape its render rule reads, and, when a declared receipt quotes the
/// request (`{/request/name}`), the request itself. `--json` and `--pretty`
/// read no declaration — the envelope is the record — so they carry none.
/// Wasm-safe.
#[derive(Clone, Debug)]
pub struct Invocation {
    declared: Option<Declared>,
}

/// A `display:` declaration parsed for the rule it renders under (output
/// contract R2). Only `batch` — the rule S3b owns — still renders through
/// the family path and parses to nothing.
#[derive(Clone, Debug)]
enum Declared {
    /// `mutation_ack`: a receipt and an identity.
    Ack(MutationAck),
    /// `page`, `history`, `search`: the rows at one pointer, as a table.
    Rows(RowsDecl),
    /// `analytics`: a node-keyed map, as a two-column table.
    Map(MapDecl),
    /// `optional`, `status_value`, `status_sections`: one record — a declared
    /// value or a block of declared fields — and what absence looks like.
    Record(RecordDecl),
    /// `status_sections` for an action: one declared receipt.
    Receipt(ReceiptDecl),
    /// `batch`: one row per item, around the declared cells.
    Batch(BatchDecl),
}

/// A parsed `mutation_ack` declaration (output contract R1/R2).
#[derive(Clone, Debug)]
struct MutationAck {
    receipt: ReceiptTemplate,
    identity: Vec<ReceiptPlaceholder>,
    noun: Option<String>,
    request: Option<Value>,
}

/// A parsed `columns:` declaration (output contract R1-table): the one array
/// the rows come from and the cell each column reads out of a row.
#[derive(Clone, Debug)]
struct RowsDecl {
    /// Pointer to the row array on the envelope: `/data/items`, or `/data`
    /// when the wire is the bare array.
    rows: String,
    columns: Vec<Column>,
    /// Present under the `page` rule: the continuation and sample facts
    /// beside the rows decide what stderr says.
    page: Option<PageDecl>,
    /// The `search` rule's block beside its rows — the index diagnostics —
    /// shown to a reader under the table. A script reads the rows alone.
    fields: Vec<Field>,
}

/// One declared column.
#[derive(Clone, Debug)]
struct Column {
    /// Pointer within one row (`/version`, `/data/embedding`); empty when the
    /// row is the cell — a scalar list.
    path: String,
    header: String,
    as_: Option<CliDisplayAs>,
}

/// What a page's stderr may report.
#[derive(Clone, Copy, Debug)]
struct PageDecl {
    /// The wire carries `total_count`: the rows are a sample of a population.
    sampled: bool,
}

/// A parsed `map:` declaration (output contract R1-table for analytics).
#[derive(Clone, Debug)]
struct MapDecl {
    /// Pointer to the node-keyed object.
    map: String,
    /// Header over the value column; the key column is always `NODE`.
    header: String,
    sort: CliDisplaySort,
}

/// A parsed record declaration: the commands whose answer is one record,
/// whether they describe it as a single `value:` or a block of `fields:`.
#[derive(Clone, Debug)]
struct RecordDecl {
    miss: Miss,
    body: RecordBody,
}

/// How a command says there is nothing to show (output contract R1, Q9).
#[derive(Clone, Debug)]
enum Miss {
    /// A status read always has an answer.
    Never,
    /// A `{found, value}` wire carries its own presence flag.
    FoundFalse,
    /// The record itself is null at this pointer.
    NullAt(String),
}

/// What a record shows.
#[derive(Clone, Debug)]
enum RecordBody {
    /// One value, and nothing around it: `kv get` prints the bytes.
    Value {
        pointer: String,
        as_: Option<CliDisplayAs>,
    },
    /// A block of declared fields.
    Fields(Vec<Field>),
}

/// One declared field of a record block.
#[derive(Clone, Debug)]
struct Field {
    /// Pointer to the value on the envelope.
    pointer: String,
    /// The `--raw` key: the pointer relative to the record's root, one dot
    /// per level (`memory_budget.total_bytes`). Wire names throughout — a
    /// `header:` is a reader's label and never reaches a script (Q18).
    key: String,
    /// The reader's label: `header:` when authored, else the last pointer
    /// segment as the wire spells it.
    label: String,
    as_: Option<CliDisplayAs>,
    /// A nested record: its own block, under this field's label.
    fields: Vec<Field>,
    /// An `as: table` field's columns, resolved from the row schema by
    /// `generate-cli` — never authored.
    columns: Vec<Column>,
}

/// A parsed `batch` declaration (output contract R1-batch): the item rows
/// and their declared cells, plus whether this command's items report an
/// effect — a write says what each item did, a read has nothing to report.
#[derive(Clone, Debug)]
struct BatchDecl {
    rows: RowsDecl,
    effect: bool,
}

/// A parsed `receipt:` declaration under a status rule: an action's answer
/// (output contract Q17).
#[derive(Clone, Debug)]
struct ReceiptDecl {
    receipt: ReceiptTemplate,
}

impl Invocation {
    /// A rendering with no declaration: JSON/pretty output, progress events,
    /// and every output the catalog does not describe.
    pub const fn none() -> Self {
        Self { declared: None }
    }

    /// The declaration `command` renders under in `format`. Human and raw
    /// look the command up in the embedded catalog; JSON and pretty never
    /// consult it.
    pub fn of(command: &Command, format: Format) -> Result<Self, CliError> {
        Self::for_wire(command.name(), format, || serde_json::to_value(command))
    }

    /// The declaration the command with wire name `wire` renders under in
    /// `format`. `request` is asked for only when the declaration quotes the
    /// request, so a caller that never needs it never serializes it.
    pub fn for_wire(
        wire: &str,
        format: Format,
        request: impl FnOnce() -> Result<Value, serde_json::Error>,
    ) -> Result<Self, CliError> {
        if matches!(format, Format::Json | Format::Pretty) {
            return Ok(Self::none());
        }
        let Some(entry) = catalog::embedded()?.command_by_wire(wire) else {
            return Ok(Self::none());
        };
        let CliDisplayDecl::Declared(display) = &entry.display else {
            return Ok(Self::none());
        };
        // The IDL guard has already accepted every shipped declaration; a
        // parse error here means the embedded catalog and the guard disagree.
        let invalid = |reason: String| {
            CliError::usage(format!(
                "display declaration for `{wire}` is invalid: {reason}"
            ))
        };
        let declared = match entry.render {
            CliRenderRule::MutationAck => {
                let mut ack = MutationAck::parse(display).map_err(invalid)?;
                if ack.quotes_request() {
                    ack.request = Some(request()?);
                }
                Declared::Ack(ack)
            }
            CliRenderRule::Page => {
                let page = PageDecl {
                    sampled: entry.encoding == Some(CliWireEncoding::SamplePage),
                };
                Declared::Rows(RowsDecl::parse(display, Some(page)).map_err(invalid)?)
            }
            CliRenderRule::History | CliRenderRule::Search => {
                Declared::Rows(RowsDecl::parse(display, None).map_err(invalid)?)
            }
            CliRenderRule::Analytics => Declared::Map(MapDecl::parse(display).map_err(invalid)?),
            // The record rules read whichever shape the command declared:
            // one value, a block of fields, a table, or an action's receipt.
            CliRenderRule::Optional
            | CliRenderRule::StatusValue
            | CliRenderRule::StatusSections => match display.shape().map_err(invalid)? {
                CliDisplayShape::Columns => {
                    Declared::Rows(RowsDecl::parse(display, None).map_err(invalid)?)
                }
                CliDisplayShape::Receipt => {
                    Declared::Receipt(ReceiptDecl::parse(display).map_err(invalid)?)
                }
                CliDisplayShape::Value | CliDisplayShape::Fields => Declared::Record(
                    RecordDecl::parse(display, entry.render, entry.encoding).map_err(invalid)?,
                ),
                CliDisplayShape::Map => {
                    return Err(invalid("a record rule declares no map".to_owned()))
                }
            },
            CliRenderRule::Batch => Declared::Batch(BatchDecl {
                rows: RowsDecl::parse(display, None).map_err(invalid)?,
                // A write batch reports what each item did; a read batch
                // carries `effect: null` on every item and says nothing.
                effect: entry.access == "write",
            }),
        };
        Ok(Self {
            declared: Some(declared),
        })
    }

    /// Whether a declaration was parsed at all.
    #[cfg(test)]
    const fn is_declared(&self) -> bool {
        self.declared.is_some()
    }

    /// The parsed `mutation_ack` declaration, when that is what was parsed.
    #[cfg(test)]
    fn ack(self) -> Option<MutationAck> {
        match self.declared {
            Some(Declared::Ack(ack)) => Some(ack),
            _ => None,
        }
    }
}

impl MutationAck {
    /// Parses a `mutation_ack` display.
    fn parse(display: &CliDisplay) -> Result<Self, String> {
        let receipt = display
            .receipt
            .as_deref()
            .ok_or_else(|| "a mutation_ack declares a receipt".to_owned())?;
        Ok(Self {
            receipt: ReceiptTemplate::parse(receipt)?,
            identity: display
                .identity
                .iter()
                .map(|entry| ReceiptPlaceholder::parse(entry))
                .collect::<Result<_, _>>()?,
            noun: display.noun.clone(),
            request: None,
        })
    }

    /// Whether any placeholder reads the request (`/request/…`) rather than
    /// the response envelope.
    fn quotes_request(&self) -> bool {
        quotes_request(self.receipt.placeholders().chain(&self.identity))
    }
}

/// Whether any of these placeholders reads the request (`/request/…`)
/// rather than the response envelope.
fn quotes_request<'a>(mut placeholders: impl Iterator<Item = &'a ReceiptPlaceholder>) -> bool {
    placeholders.any(|placeholder| match placeholder {
        ReceiptPlaceholder::Value(value) => value.pointer.starts_with("/request/"),
        ReceiptPlaceholder::Verb => false,
    })
}

impl RowsDecl {
    /// Parses a `columns:` display, plus the `search` rule's block beside
    /// the rows.
    fn parse(display: &CliDisplay, page: Option<PageDecl>) -> Result<Self, String> {
        let (rows, columns) = parse_columns(&display.columns)?;
        Ok(Self {
            rows,
            columns,
            page,
            fields: Field::parse_all(&display.fields, DATA)?,
        })
    }

    /// A single column that is the row itself: `kv list` prints keys, one
    /// per line, with no header.
    fn is_scalar_list(&self) -> bool {
        matches!(self.columns.as_slice(), [column] if column.path.is_empty())
    }
}

impl RecordDecl {
    /// Parses a `value:` or `fields:` display for a record rule. The rule
    /// and the wire encoding decide what a miss looks like: `optional` reads
    /// a `{found, value}` flag when the wire carries one and a null record
    /// otherwise, while a status read always has an answer.
    fn parse(
        display: &CliDisplay,
        rule: CliRenderRule,
        encoding: Option<CliWireEncoding>,
    ) -> Result<Self, String> {
        let optional = matches!(rule, CliRenderRule::Optional);
        let found_value = encoding == Some(CliWireEncoding::FoundValue);
        let miss = |root: &str| match (optional, found_value) {
            (false, _) => Miss::Never,
            (true, true) => Miss::FoundFalse,
            (true, false) => Miss::NullAt(root.to_owned()),
        };
        if let Some(pointer) = &display.value {
            return Ok(Self {
                miss: miss(pointer),
                body: RecordBody::Value {
                    pointer: pointer.clone(),
                    as_: display.as_,
                },
            });
        }
        if display.fields.is_empty() {
            return Err("a record declares a value or at least one field".to_owned());
        }
        // A `{found, value}` wire wraps the record; anything else carries it
        // wherever the declared fields agree it sits.
        let root = if found_value {
            FOUND_VALUE.to_owned()
        } else {
            common_parent(&display.fields)
        };
        Ok(Self {
            miss: miss(&root),
            body: RecordBody::Fields(Field::parse_all(&display.fields, &root)?),
        })
    }

    /// Whether this response has no record to show.
    fn is_miss(&self, envelope: &Value) -> bool {
        match &self.miss {
            Miss::Never => false,
            Miss::FoundFalse => {
                envelope.pointer("/data/found").and_then(Value::as_bool) != Some(true)
            }
            Miss::NullAt(pointer) => envelope.pointer(pointer).is_none_or(Value::is_null),
        }
    }
}

impl Field {
    fn parse_all(fields: &[CliDisplayField], root: &str) -> Result<Vec<Self>, String> {
        fields
            .iter()
            .map(|field| Self::parse(field, root))
            .collect()
    }

    fn parse(field: &CliDisplayField, root: &str) -> Result<Self, String> {
        let columns = if field.columns.is_empty() {
            Vec::new()
        } else {
            let (rows, columns) = parse_columns(&field.columns)?;
            if rows != field.field {
                return Err(format!(
                    "table `{}` reads its rows at `{rows}`",
                    field.field
                ));
            }
            columns
        };
        Ok(Self {
            key: raw_key(&field.field, root),
            label: field
                .header
                .clone()
                .unwrap_or_else(|| last_segment(&field.field).to_owned()),
            pointer: field.field.clone(),
            as_: field.as_,
            fields: Self::parse_all(&field.fields, root)?,
            columns,
        })
    }
}

impl ReceiptDecl {
    /// Parses a `receipt:` display for an action. An action reports what it
    /// did, so its receipt reads the response; the `/request` root exists for
    /// the two write acks whose wire carries no identity (Q15), and a status
    /// rule has no such gap to fill.
    fn parse(display: &CliDisplay) -> Result<Self, String> {
        let receipt = display
            .receipt
            .as_deref()
            .ok_or_else(|| "an action declares a receipt".to_owned())?;
        let receipt = ReceiptTemplate::parse(receipt)?;
        if quotes_request(receipt.placeholders()) {
            return Err("an action receipt reads the response, not the request".to_owned());
        }
        Ok(Self { receipt })
    }
}

impl MapDecl {
    /// Parses a `map:` display.
    fn parse(display: &CliDisplay) -> Result<Self, String> {
        Ok(Self {
            map: display
                .map
                .clone()
                .ok_or_else(|| "an analytics display declares a map".to_owned())?,
            header: display
                .header
                .clone()
                .ok_or_else(|| "a map declares the header over its values".to_owned())?,
            sort: display.sort.unwrap_or(CliDisplaySort::Key),
        })
    }
}

/// Parses a `columns:` list. Every column steps into the same row array
/// with one `/*` (`/data/items/*/version`); the text before it is the row
/// source and the text after it the cell's path within a row.
fn parse_columns(columns: &[CliDisplayField]) -> Result<(String, Vec<Column>), String> {
    let mut rows: Option<String> = None;
    let mut cells = Vec::with_capacity(columns.len());
    for field in columns {
        let (source, path) = field
            .field
            .split_once("/*")
            .ok_or_else(|| format!("column `{}` does not step into a row array", field.field))?;
        match &rows {
            None => rows = Some(source.to_owned()),
            Some(rows) if rows == source => {}
            Some(rows) => {
                return Err(format!(
                    "column `{}` reads rows at `{source}`, not `{rows}`",
                    field.field
                ))
            }
        }
        cells.push(Column {
            path: path.to_owned(),
            header: column_header(field),
            as_: field.as_,
        });
    }
    Ok((
        rows.ok_or_else(|| "a table declares at least one column".to_owned())?,
        cells,
    ))
}

/// The envelope's payload, and the record inside a `{found, value}` payload.
const DATA: &str = "/data";
const FOUND_VALUE: &str = "/data/value";

/// The last segment of a pointer (`/data/parent/name` → `name`).
fn last_segment(pointer: &str) -> &str {
    pointer.rsplit_once('/').map_or(pointer, |(_, last)| last)
}

/// A field's `--raw` key: its pointer relative to the record's root, one dot
/// per level. Only a segment boundary is stripped, so a root that is a
/// prefix of a longer name leaves the pointer alone.
fn raw_key(pointer: &str, root: &str) -> String {
    pointer
        .strip_prefix(root)
        .filter(|rest| rest.starts_with('/'))
        .unwrap_or(pointer)
        .trim_start_matches('/')
        .replace('/', ".")
}

/// The deepest pointer every declared field sits under — the record's root
/// on a wire that does not wrap it: `/data/origin` when every field reads
/// `/data/origin/…`, `/data` when they spread out.
fn common_parent(fields: &[CliDisplayField]) -> String {
    fn parent(pointer: &str) -> Vec<&str> {
        pointer
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent)
            .split('/')
            .skip(1)
            .collect()
    }
    let Some(first) = fields.first() else {
        return DATA.to_owned();
    };
    let mut root = parent(&first.field);
    for field in &fields[1..] {
        let shared = root
            .iter()
            .zip(parent(&field.field))
            .take_while(|(ours, theirs)| *ours == theirs)
            .count();
        root.truncate(shared);
    }
    let mut pointer = String::new();
    for segment in root {
        pointer.push('/');
        pointer.push_str(segment);
    }
    if pointer.is_empty() {
        return DATA.to_owned();
    }
    pointer
}

/// The header over a column: `header:` when authored, else the last pointer
/// segment upper-cased (`/data/items/*/parent/name` → `NAME`).
fn column_header(field: &CliDisplayField) -> String {
    field.header.clone().unwrap_or_else(|| {
        field
            .field
            .rsplit_once('/')
            .map_or(field.field.as_str(), |(_, last)| last)
            .to_ascii_uppercase()
    })
}

/// Renders an executor `Output` for `format`, without touching stdio. JSON
/// and pretty print the envelope; human and raw render a declared command
/// through its `display:` declaration (`invocation`) and everything else
/// through the family renderers. Wasm-safe: the binary prints the result
/// (`print_output`), the playground returns it (`run_line`).
pub fn render_output(
    output: &Output,
    invocation: &Invocation,
    format: Format,
) -> Result<Rendered, CliError> {
    let value = serde_json::to_value(output)?;
    if matches!(format, Format::Json | Format::Pretty) {
        return Ok(Rendered::stdout(terminated(
            value_to_string(&value, format)?,
            format,
        )));
    }
    // A declaration reads the wire as it is: each column names its own
    // presentation, so nothing below rewrites the envelope first.
    match &invocation.declared {
        Some(Declared::Ack(ack)) => return Ok(render_mutation_ack(&value, ack, format)),
        Some(Declared::Rows(rows)) => return Ok(render_rows(&value, rows, format)),
        Some(Declared::Map(map)) => return Ok(render_map(&value, map, format)),
        Some(Declared::Record(record)) => return Ok(render_record(&value, record, format)),
        Some(Declared::Receipt(receipt)) => return Ok(render_receipt(&value, receipt, format)),
        Some(Declared::Batch(batch)) => return Ok(render_batch(&value, batch, format)),
        None => {}
    }
    // Everything else is a `display: bespoke` command — a hand-written arm.
    // Every other command in the catalog reaches a reader through its
    // declaration, so an output with no arm here is a renderer bug rather
    // than a shape to guess at.
    let (kind, data) = tagged_output(&value)
        .ok_or_else(|| CliError::usage("an executor output carries no type tag"))?;
    let mut stdout = String::new();
    render_bespoke(kind, data, format, &mut stdout)?;
    Ok(Rendered::stdout(stdout))
}

/// Output contract R1-table for a declared row list. Human prints the
/// declared columns under an UPPERCASE header (`Table`), or one cell per
/// line with no header when the row is the cell; `(nil)` when the rows are
/// absent (a history of a key that never existed), `(empty)` when there are
/// none. Raw prints the same cells tab-separated and nothing else. A page's
/// continuation and sample facts go to stderr (R5), human only: a script
/// reads `has_more` from `--json`.
fn render_rows(envelope: &Value, decl: &RowsDecl, format: Format) -> Rendered {
    let mut stdout = String::new();
    let rows = envelope.pointer(&decl.rows).and_then(Value::as_array);
    match rows {
        None => {
            if format == Format::Human {
                line!(stdout, "(nil)");
            }
        }
        Some(rows) if rows.is_empty() => {
            if format == Format::Human {
                line!(stdout, "(empty)");
            }
        }
        Some(rows) if decl.is_scalar_list() => {
            for row in rows {
                line!(
                    stdout,
                    "{}",
                    cell(Some(row), decl.columns[0].as_, format).text
                );
            }
        }
        Some(rows) => {
            let table = rows_table(rows, &decl.columns, format);
            stdout = match format {
                Format::Human => table.human(),
                Format::Raw | Format::Json | Format::Pretty => table.raw(),
            };
        }
    }
    // What the search found, and how: the declared block under the rows,
    // for a reader only — a script's rows stay one record per line.
    if format == Format::Human && !decl.fields.is_empty() {
        stdout.push('\n');
        render_fields_human(envelope, &decl.fields, 0, &mut stdout);
    }
    let mut stderr = String::new();
    if let (Some(page), Format::Human) = (decl.page, format) {
        // The page facts sit beside the rows, one level up.
        let facts = decl
            .rows
            .rsplit_once('/')
            .and_then(|(parent, _)| envelope.pointer(parent))
            .unwrap_or(&Value::Null);
        page_notices(facts, rows.map_or(0, Vec::len), page, &mut stderr);
    }
    Rendered { stdout, stderr }
}

/// What a reader is told about a page beyond its rows: that it is a sample
/// (`-- sampled N of M`, only when the sample is smaller than the
/// population) and how to fetch the next page (`-- more: add --cursor …`).
fn page_notices(facts: &Value, shown: usize, page: PageDecl, out: &mut String) {
    if page.sampled {
        if let Some(total) = facts.get("total_count").and_then(Value::as_u64) {
            if u64::try_from(shown).is_ok_and(|shown| shown < total) {
                line!(out, "-- sampled {shown} of {total}");
            }
        }
    }
    if facts.get("has_more").and_then(Value::as_bool) == Some(true) {
        if let Some(cursor) = facts.get("cursor").filter(|cursor| !cursor.is_null()) {
            // Actionable, not just a token (#2998): tell the reader how to
            // fetch the next page. The cursor stays base64: `--cursor`
            // accepts it verbatim.
            line!(
                out,
                "-- more: add --cursor {} to the same command",
                scalar_summary(cursor)
            );
        }
    }
}

/// Output contract R1-batch: one row per item — its position, how it
/// landed, what it did (writes only), the command's declared cells, and the
/// error of any item that carries one. A batch that did not come back wholly
/// `ok` says how its items landed on stderr (Q11); a clean batch is the table
/// alone, because the STATUS column already reads `ok` on every row.
fn render_batch(envelope: &Value, decl: &BatchDecl, format: Format) -> Rendered {
    // A batch always answers with a list, even an empty one: the wire type
    // carries `items` unconditionally, so there is no absent case to report.
    let items = envelope
        .pointer(&decl.rows.rows)
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut stdout = String::new();
    match items {
        [] => {
            if format == Format::Human {
                line!(stdout, "(empty)");
            }
        }
        items => {
            // The error column appears for the whole batch or not at all: a
            // reader should not have to notice a column that comes and goes.
            let failed = items.iter().any(|item| item_error(item).is_some());
            let mut headers = vec!["#".to_owned(), "STATUS".to_owned()];
            if decl.effect {
                headers.push("EFFECT".to_owned());
            }
            headers.extend(decl.rows.columns.iter().map(|column| column.header.clone()));
            if failed {
                headers.push("ERROR".to_owned());
            }
            let mut table = Table::new(headers);
            for item in items {
                let mut row = vec![
                    cell(item.pointer("/index"), None, format),
                    cell(item.pointer("/status"), None, format),
                ];
                if decl.effect {
                    row.push(cell(item.pointer("/effect/kind"), None, format));
                }
                row.extend(
                    decl.rows
                        .columns
                        .iter()
                        .map(|column| cell(item.pointer(&column.path), column.as_, format)),
                );
                if failed {
                    // The code, not the message: it is the stable name of
                    // what went wrong, and the whole status is in `--json`.
                    row.push(cell(item_error(item), None, format));
                }
                table.push(row);
            }
            stdout = match format {
                Format::Human => table.human(),
                Format::Raw | Format::Json | Format::Pretty => table.raw(),
            };
        }
    }
    let mut stderr = String::new();
    if format == Format::Human {
        batch_summary(envelope, items, &mut stderr);
    }
    Rendered { stdout, stderr }
}

/// The error code of an item that failed, when it carries one.
fn item_error(item: &Value) -> Option<&Value> {
    item.pointer("/error/code").filter(|code| !code.is_null())
}

/// How a batch's items landed, when some of them did not land well (Q11):
/// the mode the engine ran, then a count per status in the wire's own words —
/// `-- itemwise: 2 ok, 1 miss`.
///
/// The trigger is the rows themselves, not the envelope's `status`: a
/// mutation batch reports `partial` when some items applied and others were
/// no-ops, and every one of those items is `ok` — the EFFECT column has
/// already said which was which, and `-- itemwise: 2 ok` after it is the
/// noise this rule exists to prevent. A machine reads `status` from `--json`.
fn batch_summary(envelope: &Value, items: &[Value], out: &mut String) {
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for item in items {
        let status = item
            .pointer("/status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        *counts.entry(status).or_default() += 1;
    }
    // `ok` leads; the rest follow in the wire's own order, whatever statuses
    // a future batch grows.
    let ok = counts.remove("ok").unwrap_or(0);
    if counts.is_empty() {
        return;
    }
    let mut tally = vec![format!("{ok} ok")];
    tally.extend(
        counts
            .iter()
            .map(|(status, count)| format!("{count} {status}")),
    );
    let mode = envelope
        .pointer("/data/mode")
        .and_then(Value::as_str)
        .unwrap_or("batch");
    line!(out, "-- {mode}: {}", tally.join(", "));
}

/// Output contract R1-table for an analytics map: one row per node under
/// `NODE` and the declared header, ordered as declared — by value (ties by
/// node) or by node. `(nil)` when the map is absent, `(empty)` when it has
/// no entries, human only.
fn render_map(envelope: &Value, decl: &MapDecl, format: Format) -> Rendered {
    let mut stdout = String::new();
    match envelope.pointer(&decl.map).and_then(Value::as_object) {
        None => {
            if format == Format::Human {
                line!(stdout, "(nil)");
            }
        }
        Some(entries) if entries.is_empty() => {
            if format == Format::Human {
                line!(stdout, "(empty)");
            }
        }
        Some(entries) => {
            let mut rows: Vec<(&String, &Value)> = entries.iter().collect();
            rows.sort_by(|(node_a, value_a), (node_b, value_b)| match decl.sort {
                CliDisplaySort::Key => node_a.cmp(node_b),
                CliDisplaySort::Asc => {
                    value_order(value_a, value_b).then_with(|| node_a.cmp(node_b))
                }
                CliDisplaySort::Desc => {
                    value_order(value_b, value_a).then_with(|| node_a.cmp(node_b))
                }
            });
            let mut table = Table::new(vec!["NODE".to_owned(), decl.header.clone()]);
            for (node, value) in rows {
                table.push(vec![
                    Cell::text(escape_cell(node)),
                    cell(Some(value), None, format),
                ]);
            }
            stdout = match format {
                Format::Human => table.human(),
                Format::Raw | Format::Json | Format::Pretty => table.raw(),
            };
        }
    }
    Rendered::stdout(stdout)
}

/// Output contract R1 for a declared record: a reader gets `label  value`
/// lines — a nested record as its own indented block, an `as: table` field
/// as an indented table — and a script gets `key<TAB>value` lines under the
/// wire's own names (Q16). A command that declares one `value:` prints that
/// value alone. A miss prints `(nil)` for a reader and nothing for a script,
/// exit 0 either way (Q9).
fn render_record(envelope: &Value, decl: &RecordDecl, format: Format) -> Rendered {
    if decl.is_miss(envelope) {
        return Rendered::stdout(miss_line(format));
    }
    let mut stdout = String::new();
    match &decl.body {
        RecordBody::Value { pointer, as_ } => match envelope.pointer(pointer) {
            Some(value) => stdout.push_str(&value_line(value, *as_, format)),
            None => return Rendered::stdout(miss_line(format)),
        },
        RecordBody::Fields(fields) => match format {
            Format::Human => render_fields_human(envelope, fields, 0, &mut stdout),
            Format::Raw => render_fields_raw(envelope, fields, &mut stdout),
            Format::Json | Format::Pretty => {}
        },
    }
    Rendered::stdout(stdout)
}

/// What a command with nothing to show prints (Q9).
fn miss_line(format: Format) -> String {
    match format {
        Format::Human => "(nil)\n".to_owned(),
        Format::Raw | Format::Json | Format::Pretty => String::new(),
    }
}

/// A declared `value:` as the whole answer. `json get` pretty-prints the
/// document for a reader and hands a script the leaf, where a present JSON
/// null stays `null` and a miss still prints nothing (#3064). Everything
/// else prints under its declared presentation, unescaped: the value is the
/// line, not a cell in one.
fn value_line(value: &Value, as_: Option<CliDisplayAs>, format: Format) -> String {
    let text = match (as_, format) {
        (Some(CliDisplayAs::Json), Format::Human) => {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| raw_scalar(value))
        }
        (Some(CliDisplayAs::Json), _) => raw_json_leaf(value),
        _ if value.is_null() => return miss_line(format),
        _ => presented(value, as_, format).0,
    };
    let mut line = text;
    line.push('\n');
    line
}

/// Two spaces per level, so a nested block reads as inside its label.
const INDENT: usize = 2;

/// One `fields:` block for a reader: labels padded to the widest in this
/// block, nested records and tables indented under their label. A field with
/// nothing to show reads `-` rather than vanishing, so the block always says
/// what the command declares.
fn render_fields_human(envelope: &Value, fields: &[Field], indent: usize, out: &mut String) {
    let width = fields
        .iter()
        .map(|field| field.label.chars().count())
        .max()
        .unwrap_or(0);
    for field in fields {
        let value = envelope.pointer(&field.pointer);
        if !field.fields.is_empty() {
            if value.is_some_and(Value::is_object) {
                push_label(out, indent, &field.label);
                render_fields_human(envelope, &field.fields, indent + INDENT, out);
            } else {
                push_field(out, indent, &field.label, width, "-");
            }
            continue;
        }
        if !field.columns.is_empty() {
            match value
                .and_then(Value::as_array)
                .filter(|rows| !rows.is_empty())
            {
                Some(rows) => {
                    push_label(out, indent, &field.label);
                    push_indented(
                        out,
                        &rows_table(rows, &field.columns, Format::Human).human(),
                        indent + INDENT,
                    );
                }
                None => push_field(out, indent, &field.label, width, "-"),
            }
            continue;
        }
        push_field(
            out,
            indent,
            &field.label,
            width,
            &cell(value, field.as_, Format::Human).text,
        );
    }
}

/// The same block for a script: one `key<TAB>value` line per leaf, in
/// declared order. A nested record contributes its leaves under dotted keys
/// and no line of its own; a table or list is compact JSON, a hole is empty.
fn render_fields_raw(envelope: &Value, fields: &[Field], out: &mut String) {
    for field in fields {
        if field.fields.is_empty() {
            line!(
                out,
                "{}\t{}",
                field.key,
                cell(envelope.pointer(&field.pointer), field.as_, Format::Raw).text
            );
        } else {
            render_fields_raw(envelope, &field.fields, out);
        }
    }
}

/// Output contract R1 for an action (Q17): a reader gets the declared
/// one-line receipt, a script the action's whole record as `key<TAB>value`
/// lines in the wire's own order — an action declares no fields to choose
/// between, and its facts are what a script came for.
fn render_receipt(envelope: &Value, decl: &ReceiptDecl, format: Format) -> Rendered {
    let mut stdout = String::new();
    match format {
        Format::Human => {
            stdout.push_str(&receipt_text(&decl.receipt, envelope, None, format));
            stdout.push('\n');
        }
        // An action's payload is a record — the guard proved the receipt's
        // pointers resolve inside it — so there is nothing else to print.
        Format::Raw => {
            if let Some(Value::Object(record)) = envelope.pointer(DATA) {
                for (key, value) in record {
                    line!(stdout, "{key}\t{}", cell(Some(value), None, format).text);
                }
            }
        }
        Format::Json | Format::Pretty => {}
    }
    Rendered::stdout(stdout)
}

/// A declared table: one row per element, one cell per declared column.
fn rows_table(rows: &[Value], columns: &[Column], format: Format) -> Table {
    let mut table = Table::new(columns.iter().map(|column| column.header.clone()).collect());
    for row in rows {
        table.push(
            columns
                .iter()
                .map(|column| cell(row.pointer(&column.path), column.as_, format))
                .collect(),
        );
    }
    table
}

/// A label alone on its line: what follows is indented under it.
fn push_label(out: &mut String, indent: usize, label: &str) {
    line!(out, "{:indent$}{label}", "");
}

/// One `label  value` line, padded and never trailing whitespace.
fn push_field(out: &mut String, indent: usize, label: &str, width: usize, text: &str) {
    let line = format!("{:indent$}{label:<width$}  {text}", "");
    line!(out, "{}", line.trim_end());
}

/// A rendered block, one level in.
fn push_indented(out: &mut String, text: &str, indent: usize) {
    for line in text.lines() {
        line!(out, "{:indent$}{line}", "");
    }
}

/// How two map values order: numerically when both are numbers, else by
/// their scalar text.
fn value_order(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        _ => raw_scalar(a).cmp(&raw_scalar(b)),
    }
}

/// One cell of a declared table (output contract R1-table, R4): the value
/// under its declared presentation, or the format's null cell (`-` human,
/// empty raw) when it is null or absent.
fn cell(value: Option<&Value>, as_: Option<CliDisplayAs>, format: Format) -> Cell {
    let value = match value {
        None | Some(Value::Null) => {
            return Cell::null(if format == Format::Human { "-" } else { "" });
        }
        Some(value) => value,
    };
    // An empty list or table has nothing to show a reader: the null cell says
    // so, where `[]` would read as a value. A script still gets `[]`.
    if format == Format::Human
        && matches!(as_, Some(CliDisplayAs::List | CliDisplayAs::Table))
        && value.as_array().is_some_and(Vec::is_empty)
    {
        return Cell::null("-");
    }
    let (text, numeric) = presented(value, as_, format);
    let text = escape_cell(&text);
    if numeric {
        Cell::number(text)
    } else {
        Cell::text(text)
    }
}

/// The text a declared value shows under its presentation (output contract
/// R4), and whether it is a number for alignment. Unescaped: a table cell
/// escapes it, a value that is the whole line prints as it is.
fn presented(value: &Value, as_: Option<CliDisplayAs>, format: Format) -> (String, bool) {
    let text = |text: String| (text, false);
    let number = |text: String| (text, true);
    match as_ {
        None | Some(CliDisplayAs::Float) => match value {
            Value::Number(n) => number(number_text(n, format)),
            other => text(scalar_text(other, format)),
        },
        Some(CliDisplayAs::Bytes) => text(bytes_text(value, format)),
        Some(CliDisplayAs::Json | CliDisplayAs::Table) => text(raw_scalar(value)),
        Some(CliDisplayAs::Date) => match (value.as_u64(), format) {
            (Some(micros), Format::Human) => text(crate::wall_clock::format_utc_instant(micros)),
            (Some(micros), _) => number(micros.to_string()),
            (None, _) => text(scalar_text(value, format)),
        },
        Some(CliDisplayAs::Size) => match (value.as_u64(), format) {
            (Some(bytes), Format::Human) => text(size_text(bytes)),
            (Some(bytes), _) => number(bytes.to_string()),
            (None, _) => text(scalar_text(value, format)),
        },
        Some(CliDisplayAs::List) => match (value.as_array(), format) {
            (Some(items), Format::Human) => text(
                items
                    .iter()
                    .map(|item| scalar_text(item, format))
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            _ => text(raw_scalar(value)),
        },
    }
}

/// A scalar's cell text: strings as they are, numbers under the format's
/// precision rule, everything else as compact JSON.
fn scalar_text(value: &Value, format: Format) -> String {
    match value {
        Value::Number(n) => number_text(n, format),
        other => raw_scalar(other),
    }
}

/// A number's cell text (output contract Q19): a reader sees a float to at
/// most six decimals, trailing zeros trimmed but never the point (`1.0`,
/// `0.31746`); a script sees the wire's full precision. Integers are as
/// they are.
fn number_text(n: &serde_json::Number, format: Format) -> String {
    match (n.as_f64(), format) {
        (Some(float), Format::Human) if n.is_f64() => float_text(float),
        _ => n.to_string(),
    }
}

fn float_text(float: f64) -> String {
    let mut text = format!("{float:.6}");
    let trimmed = text.trim_end_matches('0').len();
    text.truncate(trimmed);
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

/// A cell holds one line: a newline or tab inside a value is spelled out
/// (`\n`, `\t`) so the row stays one row in both layouts (R4).
fn escape_cell(text: &str) -> String {
    text.replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
}

/// Output contract R1 for a `MutationAck`: a hit prints the declared receipt
/// (human) or the declared identity, tab-separated (raw), on stdout; a miss
/// prints `no such <noun>: <identity>` on stderr and nothing on stdout, in
/// both formats. A write with no `noun` names no target and cannot miss:
/// `deleted 0 vectors` is an answer, not feedback (Q14).
fn render_mutation_ack(envelope: &Value, ack: &MutationAck, format: Format) -> Rendered {
    let request = ack.request.as_ref();
    if let Some(noun) = ack.noun.as_deref().filter(|_| is_miss(envelope)) {
        let identity = ack
            .identity
            .iter()
            .map(|placeholder| render_placeholder(placeholder, envelope, request, Format::Human))
            .collect::<Vec<_>>()
            .join(" ");
        return Rendered {
            stdout: String::new(),
            stderr: format!("no such {noun}: {identity}\n"),
        };
    }
    let mut out = String::new();
    match format {
        Format::Human => out.push_str(&receipt_text(&ack.receipt, envelope, request, format)),
        Format::Raw => {
            let identity = ack
                .identity
                .iter()
                .map(|placeholder| render_placeholder(placeholder, envelope, request, format))
                .collect::<Vec<_>>();
            out.push_str(&identity.join("\t"));
        }
        // A declaration is never consulted for the envelope formats.
        Format::Json | Format::Pretty => {}
    }
    out.push('\n');
    Rendered::stdout(out)
}

/// One receipt, filled in: its literals as authored, its placeholders read
/// off the response (or the request, when the template quotes it).
fn receipt_text(
    template: &ReceiptTemplate,
    envelope: &Value,
    request: Option<&Value>,
    format: Format,
) -> String {
    let mut out = String::new();
    for segment in template.segments() {
        match segment {
            ReceiptSegment::Literal(text) => out.push_str(text),
            ReceiptSegment::Placeholder(placeholder) => {
                out.push_str(&render_placeholder(placeholder, envelope, request, format));
            }
        }
    }
    out
}

/// A write that named a target and found nothing there: the applied signal
/// is `false` and the effect kind, when the envelope carries one, is
/// `not_found`. An `unchanged` write that did not apply (a create that met
/// an existing target) is a hit whose verb says so, never a false miss.
fn is_miss(envelope: &Value) -> bool {
    let data = &envelope["data"];
    let applied = match data {
        Value::Bool(applied) => *applied,
        // A write with no applied signal is a hit; the guard requires the
        // signal wherever a `noun` is declared.
        _ => data["effect"]["applied"].as_bool().unwrap_or(true),
    };
    !applied && matches!(data["effect"]["kind"].as_str(), None | Some("not_found"))
}

/// One placeholder's text. Missing or null values render as the format's
/// empty value (`(nil)` for human, nothing for raw) rather than failing:
/// the guard has already proven every pointer against the schema, so a hole
/// here is an optional field that is absent on this response.
fn render_placeholder(
    placeholder: &ReceiptPlaceholder,
    envelope: &Value,
    request: Option<&Value>,
    format: Format,
) -> String {
    let absent = || match format {
        Format::Human => "(nil)".to_owned(),
        Format::Raw | Format::Json | Format::Pretty => String::new(),
    };
    let ReceiptPlaceholder::Value(ReceiptValue { pointer, filter }) = placeholder else {
        return envelope
            .pointer("/data/effect/kind")
            .and_then(Value::as_str)
            .map_or_else(absent, |kind| kind.replace('_', " "));
    };
    let value = pointer.strip_prefix("/request").map_or_else(
        || envelope.pointer(pointer),
        |path| request.and_then(|request| request.pointer(path)),
    );
    let value = match value {
        Some(Value::Null) | None => return absent(),
        Some(value) => value,
    };
    match filter {
        None => match format {
            Format::Human => scalar_summary(value),
            Format::Raw | Format::Json | Format::Pretty => raw_scalar(value),
        },
        Some(ReceiptFilter::Bytes) => bytes_text(value, format),
        Some(ReceiptFilter::Size) => value.as_u64().map_or_else(absent, size_text),
        Some(ReceiptFilter::Len) => value
            .as_array()
            .map_or_else(absent, |items| items.len().to_string()),
        Some(ReceiptFilter::Plural(noun)) => value
            .as_u64()
            .map_or_else(absent, |count| plural_text(count, noun)),
    }
}

/// `|bytes`: a base64 wire string as the text it encodes. Non-UTF-8 bytes
/// stay base64, labelled `base64:` for a reader (R1-table) and bare for a
/// script, which S4 changes to the bytes themselves.
fn bytes_text(value: &Value, format: Format) -> String {
    let Some(encoded) = value.as_str() else {
        return scalar_summary(value);
    };
    match decode_base64_text(encoded) {
        Some(text) => text,
        None if format == Format::Human => format!("base64:{encoded}"),
        None => encoded.to_owned(),
    }
}

/// `|plural:<noun>`: a count with its noun — `1 row`, `12 rows`.
fn plural_text(count: u64, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// `|size`: a byte count with a decimal unit — the same text the inference
/// registry prints for a model (`format_model_size`), restated here because
/// inference imports nothing from the workspace (Rule 3) and the CLI's
/// non-inference builds cannot import it back. The two are kept in step by
/// `size_text_matches_the_inference_registry`.
fn size_text(bytes: u64) -> String {
    // Decimal units, because a byte count a reader is shown is about scale,
    // not about memory pages: 1 kB is 1000 bytes, as every disk and download
    // says. One decimal, trimmed when it adds nothing (`1 kB`, not `1.0 kB`),
    // and whole bytes below a kilobyte.
    const UNITS: [(u64, &str); 3] = [(1_000_000_000, "GB"), (1_000_000, "MB"), (1_000, "kB")];
    for (scale, unit) in UNITS {
        #[allow(clippy::cast_precision_loss)] // A byte count shown to one decimal.
        let scaled = bytes as f64 / scale as f64;
        // Rounding decides the unit, so a count that reads as `1000 MB` after
        // rounding is shown as `1 GB` instead.
        let rounded = (scaled * 10.0).round() / 10.0;
        if rounded >= 1.0 {
            let text = format!("{rounded:.1}");
            return format!("{} {unit}", text.strip_suffix(".0").unwrap_or(&text));
        }
    }
    format!("{bytes} B")
}

/// Renders one of the CLI's own reports — `doctor`, `init`, `update`,
/// `uninstall`, `ipc start/stop`, `agents …`, the REPL's context line: JSON
/// the CLI composes for itself, which no `display:` declaration describes
/// because none of them is an executor command. A reader gets the payload
/// laid out, a script gets it on one line. Executor command output never
/// comes here — it renders through `render_output` (output contract R1).
/// Wasm-safe.
pub fn value_to_string(value: &Value, format: Format) -> Result<String, CliError> {
    Ok(match format {
        Format::Json => serde_json::to_string(value)?,
        Format::Pretty => serde_json::to_string_pretty(value)?,
        Format::Human | Format::Raw => {
            // A report names itself the way an output does; the name is for a
            // machine reading `--json`, so the lines show the payload.
            let data = tagged_output(value).map_or(value, |(_, data)| data);
            let mut out = match (data, format) {
                (Value::Null, Format::Human) => "(nil)".to_owned(),
                (Value::Null, _) => return Ok(String::new()),
                (Value::Bool(_) | Value::Number(_) | Value::String(_), Format::Human) => {
                    scalar_summary(data)
                }
                (Value::Bool(_) | Value::Number(_) | Value::String(_), _) => raw_scalar(data),
                (_, Format::Human) => serde_json::to_string_pretty(data)?,
                (_, _) => serde_json::to_string(data)?,
            };
            out.push('\n');
            out
        }
    })
}

/// Renders an executor error status to its display string for `format`, without
/// touching stdio. Wasm-safe.
pub fn error_to_string(status: &impl Serialize, format: Format) -> String {
    #[derive(Serialize)]
    struct ErrorEnvelope<'a, T: Serialize + ?Sized> {
        error: &'a T,
    }

    let envelope = ErrorEnvelope { error: status };
    let serialize_failed =
        |error: serde_json::Error| format!("error: failed to render executor error: {error}");
    match format {
        Format::Json => serde_json::to_string(&envelope).unwrap_or_else(serialize_failed),
        Format::Pretty => serde_json::to_string_pretty(&envelope).unwrap_or_else(serialize_failed),
        Format::Human | Format::Raw => match serde_json::to_value(status) {
            Ok(value) => human_error_line(&value),
            Err(error) => serialize_failed(error),
        },
    }
}

/// The stderr text of an executor error exactly as the binary writes it: the
/// error line, newline-terminated in every format.
pub(crate) fn error_line(status: &impl Serialize, format: Format) -> String {
    let mut line = error_to_string(status, format);
    line.push('\n');
    line
}

fn terminated(mut rendered: String, format: Format) -> String {
    if matches!(format, Format::Json | Format::Pretty) {
        rendered.push('\n');
    }
    rendered
}

/// Prints an `Output` the way the binary does: the answer on stdout, the
/// feedback on stderr.
#[cfg(feature = "native")]
pub(crate) fn print_output(
    output: &Output,
    invocation: &Invocation,
    format: Format,
) -> Result<(), CliError> {
    let rendered = render_output(output, invocation, format)?;
    print!("{}", rendered.stdout);
    eprint!("{}", rendered.stderr);
    Ok(())
}

#[cfg(feature = "native")]
pub(crate) fn render_value(value: &Value, format: Format) -> Result<(), CliError> {
    print!("{}", terminated(value_to_string(value, format)?, format));
    Ok(())
}

#[cfg(feature = "native")]
pub(crate) fn render_error(status: &impl Serialize, format: Format) {
    eprint!("{}", error_line(status, format));
}

/// Output contract R1 for the commands the catalog marks `display: bespoke`:
/// `describe`, `ping`, `branch diff` and the ten inference arms, each with
/// the shape its own surface earned. Both formats live side by side, so what
/// a reader sees and what a script gets are decided in one place.
///
/// A tag with no arm is unreachable: every other command declares its display
/// and renders from it, and `check-cli` refuses a command that declares
/// neither. It is reported rather than guessed at, because guessing is what
/// this contract exists to end.
fn render_bespoke(
    kind: &str,
    data: &Value,
    format: Format,
    out: &mut String,
) -> Result<(), CliError> {
    let human = format == Format::Human;
    match kind {
        "pong" => {
            let version = data.get("version").and_then(Value::as_str).unwrap_or("");
            if human {
                line!(out, "pong {version}");
            } else {
                // R1: a script asked what version answered, not for a sentence.
                line!(out, "{version}");
            }
        }
        "described" if human => print_described(data, out),
        // The discovery surface is a record of facts; one line of it composes.
        "described" => line!(out, "{}", raw_scalar(data)),
        "branch_comparison" => print_branch_comparison(data, format, out),
        "inference_generation" => print_inference_generation(data, human, out),
        "inference_text" => line!(out, "{}", data.as_str().unwrap_or_default()),
        "inference_token_ids" => print_token_ids(data, out),
        "inference_embeddings" if human => print_embeddings_summary(data, out),
        "inference_embeddings" => print_embedding_values(data, out),
        "inference_ranking" if human => print_ranking(data, out),
        "inference_ranking" => print_items(items_of(data), out),
        #[cfg(feature = "inference")]
        "inference_models" if human => print_inference_models(data, out),
        "inference_models" => print_items(items_of(data), out),
        "inference_status" if human => print_inference_status(data, out),
        "inference_status" => line!(out, "{}", raw_scalar(data)),
        "inference_model_pulled" if human => print_model_pulled(data, out),
        "inference_model_pulled" => line!(out, "{}", raw_scalar(data)),
        "inference_unload_result" if human => line!(
            out,
            "{}",
            if data.get("unloaded").and_then(Value::as_bool) == Some(true) {
                "unloaded"
            } else {
                "no cached entry"
            }
        ),
        "inference_unload_result" => line!(out, "{}", raw_scalar(data)),
        _ => {
            return Err(CliError::usage(format!(
                "no renderer for `{kind}` output: the command declares no display and has no arm"
            )))
        }
    }
    Ok(())
}

/// The rows of a list-shaped payload (`{items: [...]}`), or none.
fn items_of(data: &Value) -> &[Value] {
    data.get("items")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice)
}

/// `branch diff`: the two branches compared, then one row per changed entity
/// (output contract Q20). The rows live two levels down — a list per change
/// kind inside a list of spaces — and the CHANGE column is which of those
/// lists a row came from, so no `columns:` pointer can describe it and the
/// command stays `bespoke`.
fn print_branch_comparison(data: &Value, format: Format, out: &mut String) {
    let branches = [("branch_a", "/branch_a"), ("branch_b", "/branch_b")];
    if format == Format::Human {
        let width = branches
            .iter()
            .map(|(label, _)| label.chars().count())
            .max()
            .unwrap_or(0);
        for (label, pointer) in branches {
            push_field(
                out,
                0,
                label,
                width,
                &cell(data.pointer(pointer), None, format).text,
            );
        }
    }
    let mut table = Table::new(
        ["SPACE", "CAPABILITY", "CHANGE", "IDENTITY", "VERSION"]
            .iter()
            .map(|header| (*header).to_owned())
            .collect(),
    );
    for space in data
        .get("spaces")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
    {
        for change in ["added", "removed", "modified"] {
            for entity in space
                .get(change)
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                table.push(vec![
                    cell(space.get("space"), None, format),
                    cell(space.get("capability"), None, format),
                    Cell::text(escape_cell(change)),
                    cell(entity.get("identity"), Some(CliDisplayAs::Bytes), format),
                    cell(entity.get("version"), None, format),
                ]);
            }
        }
    }
    let rows = match format {
        Format::Human => {
            // The branch lines and the table are two answers to one question;
            // a blank line keeps them from reading as one block.
            out.push('\n');
            table.human()
        }
        Format::Raw | Format::Json | Format::Pretty => table.raw(),
    };
    out.push_str(&rows);
}

fn tagged_output(value: &Value) -> Option<(&str, &Value)> {
    let object = value.as_object()?;
    let kind = object.get("type")?.as_str()?;
    let data = object.get("data").unwrap_or(&Value::Null);
    Some((kind, data))
}

/// Prints generated text; with `stats` a trailing summary line follows so the
/// human can see why generation stopped (raw mode prints the text alone).
fn print_inference_generation(data: &Value, stats: bool, out: &mut String) {
    let choice = data
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());
    let text = choice
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or("");
    line!(out, "{text}");
    if stats {
        let stop = choice
            .and_then(|choice| choice.get("finish_reason"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let usage = data.get("usage");
        let prompt = usage
            .and_then(|usage| usage.get("prompt_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let completion = usage
            .and_then(|usage| usage.get("completion_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        line!(
            out,
            "-- stop: {stop} · prompt {prompt} tok · completion {completion} tok"
        );
    }
}

fn print_token_ids(data: &Value, out: &mut String) {
    let ids = data
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_u64)
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    line!(out, "{ids}");
}

/// Prints raw embedding vectors, one line per input, values space-joined.
fn print_embedding_values(data: &Value, out: &mut String) {
    let items = data
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for item in &items {
        let values = item
            .get("embedding")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_f64)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        line!(out, "{values}");
    }
}

fn print_embeddings_summary(data: &Value, out: &mut String) {
    let dimension = data.get("dimension").and_then(Value::as_u64).unwrap_or(0);
    let items = data
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    line!(out, "{} embeddings · dim {dimension}", items.len());
    for item in &items {
        let index = item.get("index").and_then(Value::as_u64).unwrap_or(0);
        let preview = item.get("embedding").and_then(Value::as_array).map_or_else(
            || "[]".to_string(),
            |values| {
                let head = values
                    .iter()
                    .take(6)
                    .filter_map(Value::as_f64)
                    .map(|value| format!("{value:.4}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let ellipsis = if values.len() > 6 { ", …" } else { "" };
                format!("[{head}{ellipsis}]")
            },
        );
        line!(out, "  [{index}] {preview}");
    }
}

fn print_ranking(data: &Value, out: &mut String) {
    let Some(items) = data.get("items").and_then(Value::as_array) else {
        line!(out, "(nil)");
        return;
    };
    let mut scored: Vec<(u64, f64)> = items
        .iter()
        .filter(|item| item.get("status").and_then(Value::as_str) == Some("ok"))
        .filter_map(|item| {
            Some((
                item.get("index").and_then(Value::as_u64)?,
                item.get("score").and_then(Value::as_f64)?,
            ))
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    for (index, score) in scored {
        line!(out, "{index}\t{score:.6}");
    }
    for item in items {
        if item.get("status").and_then(Value::as_str) == Some("error") {
            let code = item.get("code").and_then(Value::as_str).unwrap_or("error");
            line!(out, "failed: {code}");
        }
    }
}

/// The `inference_models` tag exists only with the feature (the executor's
/// `Output::InferenceModels` is gated on it), and so does the size formatter
/// the row reads — the inference crate's, the one the download offer's
/// refusal quotes (#3235).
#[cfg(feature = "inference")]
fn print_inference_models(data: &Value, out: &mut String) {
    let Some(items) = data.get("items").and_then(Value::as_array) else {
        line!(out, "(nil)");
        return;
    };
    if items.is_empty() {
        line!(out, "(none)");
        return;
    }
    let mut unavailable = 0usize;
    for item in items {
        let text = |key: &str| {
            item.get(key)
                .and_then(Value::as_str)
                .unwrap_or("-")
                .to_owned()
        };
        // #3124: the old column showed `is_local`, which is about the file on
        // disk, and read as "you can use this". A released binary reports the
        // file present for eleven models it cannot load. Report what the user
        // can actually do instead.
        let downloaded = item.get("is_local").and_then(Value::as_bool) == Some(true);
        let runnable = item.get("runnable").and_then(Value::as_bool) != Some(false);
        let status = match (runnable, downloaded) {
            (false, _) => {
                unavailable += 1;
                "unavailable"
            }
            (true, true) => "ready",
            (true, false) => "not downloaded",
        };
        let size = item
            .get("size_bytes")
            .and_then(Value::as_u64)
            .map_or_else(|| "-".to_owned(), strata_executor::format_model_size);
        line!(
            out,
            "{}\t{}\t{}\t{}\t{}\t{}",
            text("name"),
            text("task"),
            text("architecture"),
            text("default_quant"),
            status,
            size
        );
    }
    if unavailable > 0 {
        line!(
            out,
            "\n{unavailable} model(s) unavailable: this build cannot run local \
             models -- a bare name like these means a local model. Add local \
             execution with `strata inference install-local`, or name a cloud \
             model instead (`openai:<model>`, `google:<model>`, \
             `anthropic:<model>`)."
        );
    }
}

/// D11 (#3124): the answer to "will this work", before anything is attempted.
///
/// Every hint names a command that exists today — `local_remedy` arrives from
/// the runtime already pointing at `strata inference install-local` (D1/D2),
/// and the key hints below at `strata config set`. A hint naming something
/// that cannot be run would repeat the defect this command exists to fix.
fn print_inference_status(data: &Value, out: &mut String) {
    let flag = |key: &str| data.get(key).and_then(Value::as_bool) == Some(true);
    let local = flag("local_execution");

    line!(
        out,
        "build\t{}",
        if local {
            "local + cloud"
        } else {
            "cloud providers only"
        }
    );
    if let Some(remedy) = data.get("local_remedy").and_then(Value::as_str) {
        line!(out, "\tlocal models: {remedy}");
    }

    line!(out, "\nproviders");
    let providers = data.get("providers").and_then(Value::as_array);
    for provider in providers.into_iter().flatten() {
        let name = provider
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or("-");
        let enabled = provider.get("feature_enabled").and_then(Value::as_bool) == Some(true);
        let needs_key = provider.get("requires_api_key").and_then(Value::as_bool) == Some(true);
        let ready = provider.get("ready").and_then(Value::as_bool) == Some(true);
        let detail = if !enabled {
            "not in this build".to_owned()
        } else if ready {
            let prefix = provider
                .get("model_prefix")
                .and_then(Value::as_str)
                .unwrap_or_default();
            provider
                .get("key_source")
                .and_then(Value::as_str)
                .map_or_else(
                    || format!("ready -- use {prefix}<model>"),
                    |from| format!("ready -- key from {from}; use {prefix}<model>"),
                )
        } else if needs_key {
            // `strata config set` is the persistent path and the one an agent
            // can take: it writes the key at 0600 and every later run picks it
            // up. The environment variable still wins when both are set.
            provider
                .get("key_env_var")
                .and_then(Value::as_str)
                .map_or_else(
                    || "no key".to_owned(),
                    |var| {
                        format!(
                            "no key -- `strata config set {name}.api_key <key>`, or export {var}"
                        )
                    },
                )
        } else {
            "not ready".to_owned()
        };
        line!(out, "  {name}\t{detail}");
        // Only an override is worth a line: the public endpoint is the
        // expectation, a redirected one is what a reader needs to know.
        if let Some(from) = provider.get("base_url_source").and_then(Value::as_str) {
            let url = provider
                .get("base_url")
                .and_then(Value::as_str)
                .unwrap_or("-");
            line!(out, "\tat {url} (from {from})");
        }
    }

    let dir = data
        .get("models_dir")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let downloaded = data
        .get("models_downloaded")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let catalogued = data
        .get("models_catalogued")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    line!(out, "\nmodels\t{dir} (shared by every database)");
    line!(out, "  downloaded\t{downloaded} of {catalogued} catalogued");
    if !flag("model_download") && downloaded < catalogued {
        line!(
            out,
            "\tthis build cannot download models -- `strata inference \
             install-local` adds downloading along with local execution"
        );
    }
}

fn print_model_pulled(data: &Value, out: &mut String) {
    let model = data.get("model").and_then(Value::as_str).unwrap_or("model");
    let path = data.get("path").and_then(Value::as_str).unwrap_or("-");
    line!(out, "pulled {model} -> {path}");
}

fn print_items(items: &[Value], out: &mut String) {
    for item in items {
        line!(out, "{}", scalar_summary(item));
    }
    if items.is_empty() {
        line!(out, "(empty)");
    }
}

/// `describe` is the discovery surface (#2996/#2998): humans get a scannable
/// overview instead of the JSON envelope (`--json` keeps the envelope).
fn print_described(data: &Value, out: &mut String) {
    let version = data.get("version").and_then(Value::as_str).unwrap_or("?");
    let target = data.get("target").and_then(Value::as_str).unwrap_or("?");
    line!(out, "StrataDB {version} · {target}");
    line!(
        out,
        "branch {} · branches: {} · spaces: {}",
        data.get("branch").and_then(Value::as_str).unwrap_or("?"),
        join_string_items(data.get("branches")),
        join_string_items(data.get("spaces"))
    );
    if let Some(capabilities) = data.get("capabilities").and_then(Value::as_object) {
        let enabled: Vec<&str> = capabilities
            .iter()
            .filter(|(_, on)| on.as_bool() == Some(true))
            .map(|(name, _)| name.as_str())
            .collect();
        line!(out, "capabilities: {}", enabled.join(" "));
    }
    let Some(primitives) = data.get("primitives") else {
        return;
    };
    line!(
        out,
        "kv {} · json {} · events {}",
        count_field(primitives, "kv_count"),
        count_field(primitives, "json_count"),
        count_field(primitives, "event_count")
    );
    if let Some(collections) = primitives
        .get("vector_collections")
        .and_then(Value::as_array)
        .filter(|collections| !collections.is_empty())
    {
        let rendered: Vec<String> = collections
            .iter()
            .map(|collection| {
                format!(
                    "{} (dim {}, {}, {} vectors)",
                    collection
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("?"),
                    count_field(collection, "dimension"),
                    collection
                        .get("metric")
                        .and_then(Value::as_str)
                        .unwrap_or("?"),
                    count_field(collection, "count")
                )
            })
            .collect();
        line!(out, "vector collections: {}", rendered.join(", "));
    }
    if let Some(graphs) = primitives
        .get("graphs")
        .and_then(Value::as_array)
        .filter(|graphs| !graphs.is_empty())
    {
        let rendered: Vec<String> = graphs
            .iter()
            .map(|graph| {
                format!(
                    "{} ({} nodes, {} edges)",
                    graph.get("name").and_then(Value::as_str).unwrap_or("?"),
                    count_field(graph, "node_count"),
                    count_field(graph, "edge_count")
                )
            })
            .collect();
        line!(out, "graphs: {}", rendered.join(", "));
    }
}

fn join_string_items(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

fn count_field(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

/// The text a base64 wire string encodes, when its bytes are valid UTF-8.
fn decode_base64_text(encoded: &str) -> Option<String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    String::from_utf8(decoded).ok()
}

// `Bytes` fields arrive as canonical base64 strings (DSGN-5/DTO-2). The typed
// pre-pass above (`humanize_kv_bytes`) decodes the fields the schema declares
// as bytes; by the time values reach these untyped helpers there is nothing
// left to decode, and a schema-blind decode here would corrupt genuine strings
// that merely look like base64. Arrays are always genuine JSON arrays and
// render as JSON.
fn scalar_summary(value: &Value) -> String {
    match value {
        Value::Null => "(nil)".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) => serde_json::to_string(value).unwrap_or_else(|_| "<array>".to_owned()),
        Value::Object(_) => serde_json::to_string(value).unwrap_or_else(|_| "<object>".to_owned()),
    }
}

fn raw_scalar(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
        Value::Null => String::new(),
        Value::Bool(_) | Value::Number(_) => value.to_string(),
    }
}

/// Raw form of a `json get` leaf: like [`raw_scalar`], but a present JSON `null`
/// prints the literal `null` so a `--raw` caller can distinguish a null field
/// from a miss, which emits nothing (#3064).
fn raw_json_leaf(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        other => raw_scalar(other),
    }
}

// Errors teach (first-run D4): the human line carries the code, the message,
// the actionable hint, and the stable per-code docs ref, so a human or agent
// can self-correct without a docs round-trip.
fn human_error_line(value: &Value) -> String {
    let code = value.get("code").and_then(Value::as_str).unwrap_or("error");
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("command failed");
    let reference = value
        .get("reference_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut rendered = if reference.is_empty() {
        format!("{code}: {message}")
    } else {
        format!("{code}: {message} ({reference})")
    };
    if let Some(hint) = value
        .get("suggested_fix")
        .and_then(Value::as_str)
        .filter(|hint| !hint.trim().is_empty())
    {
        rendered.push_str("\n  hint: ");
        rendered.push_str(hint);
    }
    if let Some(docs) = value
        .get("docs_url")
        .and_then(Value::as_str)
        .filter(|docs| !docs.trim().is_empty())
    {
        rendered.push_str("\n  ref: ");
        rendered.push_str(docs);
    }
    rendered
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use strata_executor::{
        BranchComparisonItem, BranchItem, BranchParentItem, BranchStatus, Bytes, CommitDurability,
        CommitReceipt, ComparedCapability, ComparedEntityItem, EventData, EventVersionedData,
        GraphBfsData, GraphPagerankData, GraphWccData, HistoryItem, HistoryResult, JsonHistoryItem,
        MutationEffect, Output, PageInfo, SampleItem, SpaceComparisonItem, VectorMatch,
    };

    use strata_executor::cli_metadata::{CliDisplay, CliDisplayAs, CliDisplayField};
    use strata_executor::{AdminPing, Command, MutationEffectKind};

    use super::{
        cell, float_text, is_miss, render_map, render_mutation_ack, render_output, Format,
        Invocation, MapDecl, MutationAck, Rendered, RowsDecl,
    };
    use crate::table::Cell;
    use serde_json::Value;

    fn bytes(text: &str) -> Bytes {
        Bytes::new(text.as_bytes().to_vec())
    }

    /// Renders through the format-only path, the way every undeclared output
    /// (and every JSON/pretty output) reaches a reader.
    fn render(output: &Output, format: super::Format) -> Rendered {
        render_output(output, &Invocation::none(), format).expect("output renders")
    }

    /// Renders `output` the way the binary renders `command`'s result in
    /// `format`: through the command's `display:` declaration.
    fn render_for(command: &Command, output: &Output, format: Format) -> Rendered {
        let invocation = Invocation::of(command, format).expect("the declaration parses");
        render_output(output, &invocation, format).expect("output renders")
    }

    /// Renders `output` under the declaration of the command with wire name
    /// `wire`, for the read commands whose declarations never quote the
    /// request.
    fn render_wire(wire: &str, output: &Output, format: Format) -> Rendered {
        let invocation = Invocation::for_wire(wire, format, || {
            panic!("a read declaration does not quote the request")
        })
        .expect("the declaration parses");
        render_output(output, &invocation, format).expect("output renders")
    }

    fn both(text: &str, feedback: &str) -> Rendered {
        Rendered {
            stdout: text.to_owned(),
            stderr: feedback.to_owned(),
        }
    }

    fn only_stdout(text: &str) -> Rendered {
        Rendered {
            stdout: text.to_owned(),
            stderr: String::new(),
        }
    }

    fn only_stderr(text: &str) -> Rendered {
        Rendered {
            stdout: String::new(),
            stderr: text.to_owned(),
        }
    }

    fn commit() -> CommitReceipt {
        CommitReceipt::new(1, 10, CommitDurability::Standard, 1, 0)
    }

    /// The one envelope a `--json` rendering prints, after checking that it
    /// printed nothing else anywhere.
    fn envelope(rendered: &Rendered) -> serde_json::Value {
        assert!(
            rendered.stderr.is_empty(),
            "an envelope format never writes to stderr: {:?}",
            rendered.stderr
        );
        assert!(
            rendered.stdout.ends_with('\n'),
            "envelopes are newline-terminated"
        );
        serde_json::from_str(&rendered.stdout).expect("one JSON envelope")
    }

    fn kv_put(key: Bytes) -> Command {
        Command::KvPut {
            branch: None,
            space: None,
            key,
            value: bytes("value"),
        }
    }

    fn kv_write(key: Bytes, effect: MutationEffect) -> Output {
        Output::WriteResult {
            key,
            effect,
            commit: commit(),
        }
    }

    #[test]
    fn kv_put_renders_its_receipt_and_identity() {
        let command = kv_put(bytes("greeting"));
        let created = kv_write(bytes("greeting"), MutationEffect::created());
        assert_eq!(
            render_for(&command, &created, Format::Human),
            only_stdout("created greeting\n")
        );
        assert_eq!(
            render_for(&command, &created, Format::Raw),
            only_stdout("greeting\n")
        );
        let updated = kv_write(bytes("greeting"), MutationEffect::updated());
        assert_eq!(
            render_for(&command, &updated, Format::Human),
            only_stdout("updated greeting\n"),
            "the verb is the effect kind"
        );

        let json = envelope(&render_for(&command, &created, Format::Json));
        assert_eq!(json["type"], "write_result");
        assert_eq!(json["data"]["effect"]["kind"], "created");
        assert_eq!(json["data"]["commit"]["version"], 1);
        let pretty = envelope(&render_for(&command, &created, Format::Pretty));
        assert_eq!(pretty, json, "pretty is the same record, reflowed");
    }

    #[test]
    fn missed_delete_is_stderr_feedback_in_human_and_raw_only() {
        let command = Command::KvDelete {
            branch: None,
            space: None,
            key: bytes("nope"),
        };
        let missed = Output::DeleteResult {
            key: bytes("nope"),
            effect: MutationEffect::not_found(),
            commit: None,
        };
        for format in [Format::Human, Format::Raw] {
            assert_eq!(
                render_for(&command, &missed, format),
                only_stderr("no such key: nope\n"),
                "{format:?}: a miss is feedback, not an answer"
            );
        }
        let json = envelope(&render_for(&command, &missed, Format::Json));
        assert_eq!(json["type"], "delete_result");
        assert_eq!(json["data"]["effect"]["kind"], "not_found");
        assert_eq!(json["data"]["effect"]["applied"], false);
        envelope(&render_for(&command, &missed, Format::Pretty));

        let deleted = Output::DeleteResult {
            key: bytes("nope"),
            effect: MutationEffect::deleted(),
            commit: Some(commit()),
        };
        assert_eq!(
            render_for(&command, &deleted, Format::Human),
            only_stdout("deleted nope\n")
        );
        assert_eq!(
            render_for(&command, &deleted, Format::Raw),
            only_stdout("nope\n")
        );
    }

    #[test]
    fn bool_wire_reads_its_identity_from_the_request() {
        let command = Command::JsonDropIndex {
            branch: None,
            space: None,
            name: "by_name".to_owned(),
        };
        assert_eq!(
            render_for(&command, &Output::Bool(true), Format::Human),
            only_stdout("dropped index by_name\n")
        );
        assert_eq!(
            render_for(&command, &Output::Bool(true), Format::Raw),
            only_stdout("by_name\n")
        );
        for format in [Format::Human, Format::Raw] {
            assert_eq!(
                render_for(&command, &Output::Bool(false), format),
                only_stderr("no such index: by_name\n"),
                "{format:?}: a bare `false` is the miss signal"
            );
        }
        let json = envelope(&render_for(&command, &Output::Bool(false), Format::Json));
        assert_eq!(json, json!({"type": "bool", "data": false}));
    }

    #[test]
    fn request_is_serialized_only_for_a_declaration_that_quotes_it() {
        let invocation = Invocation::for_wire("kv_put", Format::Human, || {
            panic!("kv_put's declaration never reads the request")
        })
        .expect("kv_put is declared");
        assert!(invocation.ack().is_some());

        let invocation = Invocation::for_wire("json_drop_index", Format::Human, || {
            Ok(json!({"name": "x"}))
        })
        .expect("json_drop_index is declared");
        assert_eq!(
            invocation.ack().and_then(|ack| ack.request),
            Some(json!({"name": "x"}))
        );

        let failed = Invocation::for_wire("json_drop_index", Format::Human, || {
            Err(<serde_json::Error as serde::de::Error>::custom(
                "unserializable",
            ))
        });
        assert!(
            matches!(failed, Err(crate::CliError::Json(_))),
            "a request that cannot be serialized fails the command, not the renderer"
        );
    }

    #[test]
    fn invocation_reads_no_declaration_for_envelope_formats_or_undeclared_commands() {
        let command = kv_put(bytes("k"));
        for format in [Format::Json, Format::Pretty] {
            let invocation = Invocation::of(&command, format).expect("no catalog lookup");
            assert!(
                !invocation.is_declared(),
                "{format:?} is the record, not a receipt"
            );
        }
        assert!(Invocation::of(&command, Format::Human)
            .expect("kv_put is declared")
            .is_declared());
        assert!(Invocation::of(&command, Format::Raw)
            .expect("kv_put is declared")
            .is_declared());
        let ping = Invocation::of(&Command::Ping {}, Format::Human).expect("ping is bespoke");
        assert!(
            !ping.is_declared(),
            "a bespoke command renders through its family arm"
        );
        let unknown = Invocation::for_wire("no_such_wire", Format::Human, || Ok(json!({})))
            .expect("an unknown wire renders through the family path");
        assert!(!unknown.is_declared());
        // Every rule in the catalog is read now, `batch` included.
        let batch = Invocation::for_wire("kv_batch_get", Format::Human, || Ok(json!({})))
            .expect("a declared command parses");
        assert!(
            batch.is_declared(),
            "the batch rule renders from its columns"
        );
    }

    #[test]
    fn undeclared_outputs_take_the_family_path() {
        let output = Output::Pong(AdminPing {
            version: "1.2.1".to_owned(),
        });
        for format in [Format::Human, Format::Raw] {
            let declared = render_for(&Command::Ping {}, &output, format);
            assert_eq!(declared, render(&output, format), "{format:?}");
            assert!(declared.stderr.is_empty());
            assert!(
                declared.stdout.contains("1.2.1"),
                "{format:?}: {declared:?}"
            );
        }
    }

    #[test]
    fn non_utf8_identity_is_labelled_base64_for_a_reader() {
        let key = Bytes::new(vec![0xff]);
        let command = kv_put(key.clone());
        let output = kv_write(key, MutationEffect::created());
        assert_eq!(
            render_for(&command, &output, Format::Human),
            only_stdout("created base64:/w==\n")
        );
        assert_eq!(
            render_for(&command, &output, Format::Raw),
            only_stdout("/w==\n"),
            "raw keeps the bare wire form until S4 prints the bytes themselves"
        );
    }

    #[test]
    fn multi_value_identity_is_tab_separated_in_raw() {
        let command = Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "social".to_owned(),
            src: "alice".to_owned(),
            edge_type: "follows".to_owned(),
            dst: "bob".to_owned(),
            weight: None,
            properties: None,
        };
        let edge = |effect: MutationEffect| Output::GraphEdgeWriteResult {
            graph: "social".to_owned(),
            src: "alice".to_owned(),
            edge_type: "follows".to_owned(),
            dst: "bob".to_owned(),
            effect,
            commit: commit(),
        };
        assert_eq!(
            render_for(&command, &edge(MutationEffect::created()), Format::Human),
            only_stdout("created edge alice -[follows]-> bob in social\n")
        );
        assert_eq!(
            render_for(&command, &edge(MutationEffect::created()), Format::Raw),
            only_stdout("alice\tfollows\tbob\n")
        );
        assert_eq!(
            render_for(&command, &edge(MutationEffect::not_found()), Format::Raw),
            only_stderr("no such edge: alice follows bob\n"),
            "a miss line joins the identity with spaces in every format"
        );
    }

    #[test]
    fn bulk_delete_of_nothing_is_an_answer() {
        let command = Command::VectorDeleteAll {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
        };
        let bulk = |effect: MutationEffect, commit| Output::VectorBulkDeleteResult {
            collection: "docs".to_owned(),
            effect,
            commit,
        };
        let nothing = bulk(MutationEffect::not_found(), None);
        assert_eq!(
            render_for(&command, &nothing, Format::Human),
            only_stdout("deleted 0 vectors from docs\n"),
            "no noun: an empty bulk delete names no target and cannot miss"
        );
        assert_eq!(
            render_for(&command, &nothing, Format::Raw),
            only_stdout("0\n")
        );
        let one = bulk(
            MutationEffect::new(true, MutationEffectKind::Deleted, true, 1),
            Some(commit()),
        );
        assert_eq!(
            render_for(&command, &one, Format::Human),
            only_stdout("deleted 1 vector from docs\n")
        );
        let two = bulk(
            MutationEffect::new(true, MutationEffectKind::Deleted, true, 2),
            Some(commit()),
        );
        assert_eq!(
            render_for(&command, &two, Format::Human),
            only_stdout("deleted 2 vectors from docs\n")
        );
    }

    // --- #3314 S2: declared tables for page / history / search / analytics ---

    const FIXED_INSTANT_MICROS: u64 = 1_789_071_584_000_000;

    fn kv_history(items: Vec<HistoryItem>) -> Output {
        Output::VersionHistory(Some(HistoryResult::new(items)))
    }

    #[test]
    fn kv_history_is_a_table_of_its_declared_columns() {
        let output = kv_history(vec![
            HistoryItem::new(Some(bytes("two")), false, 4, 40)
                .with_committed_at(Some(FIXED_INSTANT_MICROS)),
            HistoryItem::new(Some(Bytes::new(vec![0xff])), false, 12, 30),
            HistoryItem::new(None, true, 2, 20).with_committed_at(Some(FIXED_INSTANT_MICROS)),
        ]);
        assert_eq!(
            render_wire("kv_history", &output, Format::Human),
            only_stdout(concat!(
                "VERSION  COMMITTED_AT                    VALUE\n",
                "      4  2026-09-10 20:19:44.000000 UTC  two\n",
                "     12  -                               base64:/w==\n",
                "      2  2026-09-10 20:19:44.000000 UTC  -\n",
            )),
            "numbers right-align, a date is a UTC instant, bytes decode, a null is `-`"
        );
        assert_eq!(
            render_wire("kv_history", &output, Format::Raw),
            only_stdout("4\t1789071584000000\ttwo\n12\t\t/w==\n2\t1789071584000000\t\n"),
            "raw keeps the epoch micros and bare base64, and an empty cell for null"
        );
    }

    #[test]
    fn a_missing_history_is_nil_and_an_empty_one_is_empty_for_a_reader_only() {
        let missing = Output::VersionHistory(None);
        assert_eq!(
            render_wire("kv_history", &missing, Format::Human),
            only_stdout("(nil)\n")
        );
        assert_eq!(
            render_wire("kv_history", &missing, Format::Raw),
            only_stdout("")
        );
        let empty = kv_history(Vec::new());
        assert_eq!(
            render_wire("kv_history", &empty, Format::Human),
            only_stdout("(empty)\n")
        );
        assert_eq!(
            render_wire("kv_history", &empty, Format::Raw),
            only_stdout("")
        );
    }

    #[test]
    fn a_bare_array_history_reads_its_rows_at_data() {
        let output = Output::JsonVersionHistory(Some(vec![JsonHistoryItem::new(
            Some(json!({"name": "Ada", "age": 36})),
            4,
            40,
            Some(2),
            false,
        )]));
        assert_eq!(
            render_wire("json_history", &output, Format::Human),
            only_stdout(concat!(
                "VERSION  DOCUMENT_VERSION  COMMITTED_AT  VALUE\n",
                "      4                 2  -             {\"age\":36,\"name\":\"Ada\"}\n",
            ))
        );
        assert_eq!(
            render_wire("json_history", &output, Format::Raw),
            only_stdout("4\t2\t\t{\"age\":36,\"name\":\"Ada\"}\n")
        );
        assert_eq!(
            render_wire(
                "json_history",
                &Output::JsonVersionHistory(None),
                Format::Human
            ),
            only_stdout("(nil)\n")
        );
    }

    fn branch(name: &str, generation: u64, parent: Option<&str>) -> BranchItem {
        BranchItem::new(
            name.to_owned(),
            format!("id-{name}"),
            generation,
            BranchStatus::Active,
            parent.map(|parent| {
                BranchParentItem::new(parent.to_owned(), "id".to_owned(), 1, 7, None)
            }),
            None,
            None,
            None,
            0,
        )
    }

    #[test]
    fn a_page_is_a_table_and_its_continuation_is_stderr_feedback_for_a_reader() {
        let output = Output::Branches {
            items: vec![
                branch("default", 1, None),
                branch("feature", 2, Some("default")),
            ],
            page: PageInfo::new(true, Some("feature".to_owned())),
        };
        assert_eq!(
            render_wire("branch_list", &output, Format::Human),
            both(
                "NAME     PARENT   STATUS  GENERATION\n\
                 default  -        active           1\n\
                 feature  default  active           2\n",
                "-- more: add --cursor feature to the same command\n"
            ),
            "a nested pointer reads through the row; `header:` names the column"
        );
        assert_eq!(
            render_wire("branch_list", &output, Format::Raw),
            only_stdout("default\t\tactive\t1\nfeature\tdefault\tactive\t2\n"),
            "a script reads `has_more` from --json, not from a hint"
        );
        let last = Output::Branches {
            items: vec![branch("default", 1, None)],
            page: PageInfo::terminal(),
        };
        assert!(
            render_wire("branch_list", &last, Format::Human)
                .stderr
                .is_empty(),
            "a terminal page needs no attention"
        );
    }

    #[test]
    fn a_scalar_page_is_one_cell_per_line_with_no_header() {
        let output = Output::KeysPage {
            items: vec![bytes("user:1"), Bytes::new(vec![0xff])],
            page: PageInfo::new(true, Some(bytes("next"))),
        };
        assert_eq!(
            render_wire("kv_list", &output, Format::Human),
            both(
                "user:1\nbase64:/w==\n",
                "-- more: add --cursor bmV4dA== to the same command\n"
            ),
            "the cursor stays base64: --cursor takes it verbatim"
        );
        assert_eq!(
            render_wire("kv_list", &output, Format::Raw),
            only_stdout("user:1\n/w==\n")
        );
        let empty = Output::KeysPage {
            items: Vec::new(),
            page: PageInfo::terminal(),
        };
        assert_eq!(
            render_wire("kv_list", &empty, Format::Human),
            only_stdout("(empty)\n")
        );
        assert_eq!(render_wire("kv_list", &empty, Format::Raw), only_stdout(""));
    }

    fn kv_sample(total_count: u64, page: PageInfo<Bytes>) -> Output {
        Output::SampleResult {
            total_count,
            items: vec![SampleItem::new(bytes("a"), bytes("1"), 3, 30)],
            page,
        }
    }

    #[test]
    fn a_sample_smaller_than_its_population_says_so_on_stderr() {
        let sampled = kv_sample(5, PageInfo::terminal());
        assert_eq!(
            render_wire("kv_sample", &sampled, Format::Human),
            both(
                "KEY  VERSION  VALUE\na          3  1\n",
                "-- sampled 1 of 5\n"
            )
        );
        assert_eq!(
            render_wire("kv_sample", &sampled, Format::Raw),
            only_stdout("a\t3\t1\n")
        );
        let whole = kv_sample(1, PageInfo::terminal());
        assert!(
            render_wire("kv_sample", &whole, Format::Human)
                .stderr
                .is_empty(),
            "a sample that is the whole population is not a sample"
        );
        let sampled_and_more = kv_sample(5, PageInfo::new(true, Some(bytes("b"))));
        assert_eq!(
            render_wire("kv_sample", &sampled_and_more, Format::Human).stderr,
            "-- sampled 1 of 5\n-- more: add --cursor Yg== to the same command\n",
            "the sample notice comes before the continuation"
        );
    }

    #[test]
    fn a_search_result_is_a_key_score_metadata_table() {
        let output = Output::VectorMatches(vec![
            VectorMatch::new("a".to_owned(), 1.0, None),
            VectorMatch::new("b".to_owned(), 0.1, Some(json!({"lang": "en"}))),
        ]);
        assert_eq!(
            render_wire("vector_query", &output, Format::Human),
            only_stdout(
                "KEY  SCORE  METADATA\n\
                 a      1.0  -\n\
                 b      0.1  {\"lang\":\"en\"}\n"
            ),
            "a reader's float keeps `.0` and drops the noise past six decimals"
        );
        assert_eq!(
            render_wire("vector_query", &output, Format::Raw),
            only_stdout("a\t1.0\t\nb\t0.10000000149011612\t{\"lang\":\"en\"}\n"),
            "a script gets the wire's full precision"
        );
        assert_eq!(
            render_wire(
                "vector_query",
                &Output::VectorMatches(Vec::new()),
                Format::Human
            ),
            only_stdout("(empty)\n")
        );
    }

    #[test]
    fn an_analytics_map_orders_by_value_with_ties_by_node() {
        let ranks = [
            ("a", 0.317_460_304_794_368_7),
            ("b", 0.208_127_577_234_3),
            ("c", 0.474_412_117_971_211_4),
            ("d", 0.474_412_117_971_211_4),
        ]
        .into_iter()
        .map(|(node, rank)| (node.to_owned(), rank))
        .collect();
        let output =
            Output::GraphPagerankResult(GraphPagerankData::new("g".to_owned(), ranks, 20, false));
        assert_eq!(
            render_wire("graph_pagerank", &output, Format::Human),
            only_stdout(
                "NODE  RANK\n\
                 c     0.474412\n\
                 d     0.474412\n\
                 a      0.31746\n\
                 b     0.208128\n"
            ),
            "desc: highest rank first, equal ranks by node"
        );
        assert_eq!(
            render_wire("graph_pagerank", &output, Format::Raw),
            only_stdout(
                "c\t0.4744121179712114\nd\t0.4744121179712114\na\t0.3174603047943687\nb\t0.2081275772343\n"
            )
        );
    }

    #[test]
    fn an_analytics_map_orders_ascending_or_by_node_as_declared() {
        // `10` sorts before `9` as text: the order must be numeric.
        let depths = [("d", 10), ("a", 0), ("c", 9), ("b", 1), ("e", 1)]
            .into_iter()
            .map(|(node, depth)| (node.to_owned(), depth))
            .collect();
        let bfs = Output::GraphBfsResult(GraphBfsData::new(
            "g".to_owned(),
            "a".to_owned(),
            vec!["a".to_owned()],
            depths,
            Vec::new(),
            false,
        ));
        assert_eq!(
            render_wire("graph_bfs", &bfs, Format::Human),
            only_stdout(concat!(
                "NODE  DEPTH\n",
                "a         0\n",
                "b         1\n",
                "e         1\n",
                "c         9\n",
                "d        10\n",
            )),
            "asc: nearest first, equal depths by node, and `10` after `9`"
        );
        let components = [("b", "a"), ("a", "a"), ("c", "c")]
            .into_iter()
            .map(|(node, component)| (node.to_owned(), component.to_owned()))
            .collect();
        let wcc = Output::GraphWccResult(GraphWccData::new("g".to_owned(), components, 2));
        assert_eq!(
            render_wire("graph_wcc", &wcc, Format::Human),
            only_stdout("NODE  COMPONENT\na     a\nb     a\nc     c\n"),
            "key: by node; a text value column reads left to right"
        );
        assert_eq!(
            render_wire("graph_wcc", &wcc, Format::Raw),
            only_stdout("a\ta\nb\ta\nc\tc\n")
        );
        let none = Output::GraphWccResult(GraphWccData::new(
            "g".to_owned(),
            std::collections::BTreeMap::new(),
            0,
        ));
        assert_eq!(
            render_wire("graph_wcc", &none, Format::Human),
            only_stdout("(empty)\n")
        );
        assert_eq!(
            render_wire("graph_wcc", &none, Format::Raw),
            only_stdout("")
        );
    }

    #[test]
    fn an_absent_analytics_map_is_nil_for_a_reader_only() {
        // No wire shape omits its map; the rule still answers like `history`
        // does for a key that never existed, so a reader can tell "no map"
        // from "no entries".
        let decl = MapDecl::parse(&CliDisplay {
            map: Some("/data/ranks".to_owned()),
            header: Some("RANK".to_owned()),
            ..CliDisplay::default()
        })
        .expect("a map with a header parses");
        let without = json!({"data": {"graph": "g"}});
        assert_eq!(
            render_map(&without, &decl, Format::Human),
            only_stdout("(nil)\n")
        );
        assert_eq!(render_map(&without, &decl, Format::Raw), only_stdout(""));
        let with = json!({"data": {"ranks": {"n": 1}}});
        assert_eq!(
            render_map(&with, &decl, Format::Human),
            only_stdout("NODE  RANK\nn        1\n")
        );
    }

    #[test]
    fn a_cell_presents_each_declared_as_in_both_layouts() {
        use CliDisplayAs::{Bytes as B, Date, Json, List, Size};
        let human =
            |value: &Value, as_: Option<CliDisplayAs>| cell(Some(value), as_, Format::Human);
        let raw = |value: &Value, as_: Option<CliDisplayAs>| cell(Some(value), as_, Format::Raw);
        let micros = json!(FIXED_INSTANT_MICROS);
        assert_eq!(
            human(&micros, Some(Date)),
            Cell::text("2026-09-10 20:19:44.000000 UTC".to_owned())
        );
        assert_eq!(raw(&micros, Some(Date)), Cell::number(micros.to_string()));
        let size = json!(3_000_000);
        assert_eq!(human(&size, Some(Size)), Cell::text("3 MB".to_owned()));
        assert_eq!(raw(&size, Some(Size)), Cell::number("3000000".to_owned()));
        let list = json!(["kv", "json"]);
        assert_eq!(human(&list, Some(List)), Cell::text("kv json".to_owned()));
        assert_eq!(
            raw(&list, Some(List)),
            Cell::text("[\"kv\",\"json\"]".to_owned()),
            "a script gets the list as one JSON cell"
        );
        let doc = json!({"a": [1, 2]});
        assert_eq!(
            human(&doc, Some(Json)),
            Cell::text("{\"a\":[1,2]}".to_owned())
        );
        assert_eq!(raw(&doc, Some(Json)), human(&doc, Some(Json)));
        let text = json!("aGVsbG8=");
        assert_eq!(human(&text, Some(B)), Cell::text("hello".to_owned()));
        assert_eq!(raw(&text, Some(B)), human(&text, Some(B)));
        let binary = json!("/w==");
        assert_eq!(
            human(&binary, Some(B)),
            Cell::text("base64:/w==".to_owned())
        );
        assert_eq!(
            raw(&binary, Some(B)),
            Cell::text("/w==".to_owned()),
            "a script gets bytes that are not text as bare base64"
        );
        // A value of the wrong shape for its `as:` is shown as it is, never
        // dropped: the declaration is a presentation, not a filter.
        let odd = json!("not a number");
        for as_ in [Some(Date), Some(Size), Some(List)] {
            assert_eq!(
                human(&odd, as_),
                Cell::text("not a number".to_owned()),
                "{as_:?}"
            );
        }
        assert_eq!(cell(None, Some(Date), Format::Human), Cell::null("-"));
        assert_eq!(
            cell(Some(&Value::Null), Some(Size), Format::Raw),
            Cell::null("")
        );
    }

    #[test]
    fn an_event_row_shows_its_date_and_its_payload_compact() {
        let output = Output::EventRecords {
            items: vec![EventVersionedData::new(
                EventData::new(
                    0,
                    "user.created".to_owned(),
                    json!({"id": 1, "note": "line one\nline two"}),
                    FIXED_INSTANT_MICROS,
                    String::new(),
                    "h".to_owned(),
                ),
                1,
                10,
            )],
            page: PageInfo::terminal(),
        };
        assert_eq!(
            render_wire("event_list", &output, Format::Human),
            only_stdout(concat!(
                "SEQUENCE  EVENT_TYPE    TIMESTAMP                       PAYLOAD\n",
                "       0  user.created  2026-09-10 20:19:44.000000 UTC  {\"id\":1,\"note\":\"line one\\nline two\"}\n",
            )),
            "compact JSON already spells a newline inside a string"
        );
        assert_eq!(
            render_wire("event_list", &output, Format::Raw),
            only_stdout(
                "0\tuser.created\t1789071584000000\t{\"id\":1,\"note\":\"line one\\nline two\"}\n"
            )
        );
    }

    #[test]
    fn a_cell_holds_one_line_in_both_layouts() {
        let output = Output::SampleResult {
            total_count: 1,
            items: vec![SampleItem::new(bytes("k\tv"), bytes("one\ntwo"), 3, 30)],
            page: PageInfo::terminal(),
        };
        assert_eq!(
            render_wire("kv_sample", &output, Format::Human),
            only_stdout("KEY   VERSION  VALUE\nk\\tv        3  one\\ntwo\n")
        );
        assert_eq!(
            render_wire("kv_sample", &output, Format::Raw),
            only_stdout("k\\tv\t3\tone\\ntwo\n"),
            "a tab inside a value never splits a raw row"
        );
    }

    #[test]
    fn a_reader_float_has_at_most_six_decimals_and_always_a_point() {
        for (float, text) in [
            (1.0, "1.0"),
            (0.5, "0.5"),
            (0.0, "0.0"),
            (100.0, "100.0"),
            (0.317_460_304_794_368_7, "0.31746"),
            (0.474_412_117_971_211_4, "0.474412"),
            (2.000_000_1, "2.0"),
            (-0.000_001, "-0.000001"),
            (123_456.789, "123456.789"),
        ] {
            assert_eq!(float_text(float), text, "{float}");
        }
    }

    #[test]
    fn a_declaration_whose_columns_disagree_on_the_row_source_is_refused() {
        let column = |field: &str| CliDisplayField {
            field: field.to_owned(),
            header: None,
            as_: None,
            fields: Vec::new(),
            columns: Vec::new(),
        };
        let disagree = CliDisplay {
            columns: vec![column("/data/items/*/a"), column("/data/rows/*/b")],
            ..CliDisplay::default()
        };
        let error = RowsDecl::parse(&disagree, None).expect_err("two row sources");
        assert!(error.contains("/data/rows"), "{error}");
        let no_rows = CliDisplay {
            columns: vec![column("/data/a")],
            ..CliDisplay::default()
        };
        assert!(RowsDecl::parse(&no_rows, None).is_err());
        assert!(RowsDecl::parse(&CliDisplay::default(), None).is_err());
        let headed = CliDisplay {
            columns: vec![
                column("/data/items/*/parent/name"),
                CliDisplayField {
                    header: Some("SIZE".to_owned()),
                    as_: Some(CliDisplayAs::Size),
                    ..column("/data/items/*/size_bytes")
                },
            ],
            ..CliDisplay::default()
        };
        let parsed = RowsDecl::parse(&headed, None).expect("parses");
        assert_eq!(parsed.rows, "/data/items");
        assert_eq!(parsed.columns[0].header, "NAME");
        assert_eq!(parsed.columns[0].path, "/parent/name");
        assert_eq!(parsed.columns[1].header, "SIZE");
        assert!(!parsed.is_scalar_list());
        let scalar = CliDisplay {
            columns: vec![column("/data/items/*")],
            ..CliDisplay::default()
        };
        assert!(RowsDecl::parse(&scalar, None)
            .expect("parses")
            .is_scalar_list());
        assert!(MapDecl::parse(&CliDisplay::default()).is_err());
        let no_header = CliDisplay {
            map: Some("/data/ranks".to_owned()),
            ..CliDisplay::default()
        };
        assert!(MapDecl::parse(&no_header).is_err());
    }

    fn declared(receipt: &str, noun: Option<&str>, identity: &[&str]) -> MutationAck {
        let display = CliDisplay {
            receipt: Some(receipt.to_owned()),
            noun: noun.map(str::to_owned),
            identity: identity.iter().map(|entry| (*entry).to_owned()).collect(),
            ..CliDisplay::default()
        };
        MutationAck::parse(&display).expect("the declaration parses")
    }

    #[test]
    fn unchanged_write_is_not_a_miss() {
        let ack = declared("{verb} {/data/key}", Some("key"), &["/data/key"]);
        let unchanged = json!({"type": "t", "data": {"key": "k", "effect": {
            "applied": false, "kind": "unchanged", "matched": true, "affected_count": 0
        }}});
        assert_eq!(
            render_mutation_ack(&unchanged, &ack, Format::Human),
            only_stdout("unchanged k\n"),
            "a create that met an existing target is a hit whose verb says so"
        );
        assert_eq!(
            render_mutation_ack(&unchanged, &ack, Format::Raw),
            only_stdout("k\n")
        );
        let missed = json!({"type": "t", "data": {"key": "k", "effect": {
            "applied": false, "kind": "not_found", "matched": false, "affected_count": 0
        }}});
        assert_eq!(
            render_mutation_ack(&missed, &ack, Format::Human),
            only_stderr("no such key: k\n")
        );
    }

    #[test]
    fn miss_needs_a_false_applied_signal_and_no_other_kind() {
        let effect = |applied: bool, kind: Option<&str>| {
            let mut effect = json!({"applied": applied});
            if let Some(kind) = kind {
                effect["kind"] = json!(kind);
            }
            json!({"type": "t", "data": {"effect": effect}})
        };
        assert!(is_miss(&json!({"type": "bool", "data": false})));
        assert!(!is_miss(&json!({"type": "bool", "data": true})));
        assert!(is_miss(&effect(false, Some("not_found"))));
        assert!(is_miss(&effect(false, None)));
        assert!(!is_miss(&effect(false, Some("unchanged"))));
        assert!(!is_miss(&effect(true, Some("not_found"))), "applied wins");
        assert!(!is_miss(&effect(true, Some("created"))));
        assert!(
            !is_miss(&json!({"type": "t", "data": {"key": "k"}})),
            "no applied signal at all is a hit"
        );
    }

    #[test]
    fn receipt_filters_render_len_plural_and_size() {
        let ack = declared(
            "{/data/items|len} items, {/data/n|plural:row}, {/data/size|size}",
            None,
            &["/data/n", "/data/size|size"],
        );
        let doc = |n: u64, size: u64| json!({"type": "t", "data": {"items": [1, 2, 3], "n": n, "size": size}});
        assert_eq!(
            render_mutation_ack(&doc(1, 12), &ack, Format::Human),
            only_stdout("3 items, 1 row, 12 B\n")
        );
        assert_eq!(
            render_mutation_ack(&doc(0, 1_600_000), &ack, Format::Human),
            only_stdout("3 items, 0 rows, 1.6 MB\n")
        );
        assert_eq!(
            render_mutation_ack(&doc(2, 2_500_000_000), &ack, Format::Human),
            only_stdout("3 items, 2 rows, 2.5 GB\n")
        );
        assert_eq!(
            render_mutation_ack(&doc(2, 2_500_000_000), &ack, Format::Raw),
            only_stdout("2\t2.5 GB\n"),
            "filters apply to identity columns too"
        );
    }

    #[test]
    fn absent_placeholder_values_render_as_the_format_empty_value() {
        let ack = declared(
            "{verb} {/data/missing} {/data/missing|bytes} {/data/missing|len}",
            None,
            &["/data/missing"],
        );
        let doc = json!({"type": "t", "data": {"key": "k"}});
        assert_eq!(
            render_mutation_ack(&doc, &ack, Format::Human),
            only_stdout("(nil) (nil) (nil) (nil)\n")
        );
        assert_eq!(
            render_mutation_ack(&doc, &ack, Format::Raw),
            only_stdout("\n")
        );
        let null = json!({"type": "t", "data": {"missing": null}});
        assert_eq!(
            render_mutation_ack(&null, &ack, Format::Raw),
            only_stdout("\n"),
            "null and absent are the same hole"
        );
    }

    #[cfg(feature = "inference")]
    #[test]
    fn size_text_matches_the_inference_registry() {
        for bytes in [
            0,
            1,
            949,
            // Every boundary the rule turns on: a unit's floor, a count that
            // rounds up into the next unit, and one that stops just short.
            999,
            1_000,
            1_024,
            40_960,
            949_999,
            999_999,
            1_000_000,
            1_048_576,
            1_600_000,
            67_108_864,
            536_870_912,
            999_999_999,
            1_000_000_000,
            4_700_000_000,
            26_646_880_256,
        ] {
            assert_eq!(
                super::size_text(bytes),
                strata_executor::format_model_size(bytes),
                "{bytes}"
            );
        }
    }

    /// The executor result-type tags the human/raw renderers dispatch on
    /// specially (everything else falls through to the generic renderer). This
    /// is the executable inventory of special-cased render coverage — the
    /// `rendered_tag_inventory_matches_dispatch_arms` guard keeps it in sync
    /// with the actual `match` arms, so a new special-cased tag can't be added
    /// (or removed) without updating this list and its rendering test.
    #[test]
    fn described_renders_a_scannable_overview_not_json() {
        let value = serde_json::json!({"type": "described", "data": {
            "version": "1.1.0", "target": "durable_local", "branch": "default",
            "branches": ["default", "risky"], "spaces": ["default", "staging"],
            "capabilities": {"json": true, "kv": true, "arrow": false},
            "primitives": {
                "kv_count": 264, "json_count": 783, "event_count": 0,
                "vector_collections": [
                    {"name": "embeddings", "dimension": 4, "metric": "cosine", "count": 0}
                ],
                "graphs": [{"name": "net", "node_count": 2, "edge_count": 1}]
            }
        }});
        let rendered = human(&value);
        assert_eq!(
            rendered,
            "StrataDB 1.1.0 · durable_local\n\
             branch default · branches: default, risky · spaces: default, staging\n\
             capabilities: json kv\n\
             kv 264 · json 783 · events 0\n\
             vector collections: embeddings (dim 4, cosine, 0 vectors)\n\
             graphs: net (2 nodes, 1 edges)\n"
        );
    }

    #[test]
    fn described_omits_empty_collection_and_graph_lines() {
        let value = serde_json::json!({"type": "described", "data": {
            "version": "1.1.0", "target": "cache", "branch": "default",
            "branches": ["default"], "spaces": ["default"],
            "capabilities": {"kv": true},
            "primitives": {"kv_count": 0, "json_count": 0, "event_count": 0,
                "vector_collections": [], "graphs": []}
        }});
        let rendered = human(&value);
        assert!(
            !rendered.contains("vector collections") && !rendered.contains("graphs:"),
            "empty inventories stay silent: {rendered}"
        );
    }

    const RENDERED_TAGS: &[&str] = &[
        "branch_comparison",
        "described",
        "inference_embeddings",
        "inference_generation",
        "inference_model_pulled",
        "inference_models",
        "inference_ranking",
        "inference_status",
        "inference_text",
        "inference_token_ids",
        "inference_unload_result",
        "pong",
    ];

    /// Extracts the string literals that head a `match` arm (`"tag" =>`,
    /// `"tag" | "tag2" =>`, or `"tag" if <guard> =>`) from the render source — the tags the renderers
    /// special-case. Format-string arguments (`line!(out, "...")`) never sit
    /// before `=>`/`|`, so they are not mistaken for tags.
    fn dispatch_tags() -> std::collections::BTreeSet<String> {
        let full = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/render.rs"))
            .expect("read render.rs source");
        // Scan only the production code; the test module below contains `"tag"`
        // examples in its comments that are not real dispatch arms. (The
        // test-only accessors on `Invocation` are `#[cfg(test)]` too, so the
        // cut is at the module, not the first attribute.)
        let source = full
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .unwrap_or(&full);
        let mut tags = std::collections::BTreeSet::new();
        let mut cursor = 0;
        while let Some(rel) = source[cursor..].find('"') {
            let start = cursor + rel + 1;
            let Some(end_rel) = source[start..].find('"') else {
                break;
            };
            let end = start + end_rel;
            let content = &source[start..end];
            let rest = source[end + 1..].trim_start();
            let is_tag = !content.is_empty()
                && content.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && (rest.starts_with("=>") || rest.starts_with('|') || rest.starts_with("if "));
            if is_tag {
                tags.insert(content.to_owned());
            }
            cursor = end + 1;
        }
        tags
    }

    #[test]
    fn rendered_tag_inventory_matches_dispatch_arms() {
        let arms = dispatch_tags();
        let inventory: std::collections::BTreeSet<String> =
            RENDERED_TAGS.iter().map(|tag| (*tag).to_owned()).collect();
        assert_eq!(
            arms, inventory,
            "the render dispatch arms drifted from RENDERED_TAGS; \
             add or remove the tag in the list (and its rendering test)"
        );
    }

    fn kv_comparison(entities: Vec<ComparedEntityItem>) -> Output {
        let space = SpaceComparisonItem::new(
            "default".to_owned(),
            ComparedCapability::Kv,
            Vec::new(),
            Vec::new(),
            entities,
        );
        Output::BranchComparison(BranchComparisonItem::new(
            "default".to_owned(),
            "cleaned".to_owned(),
            vec![space],
        ))
    }

    /// A write batch's wire: every item carries its position, how it landed,
    /// what it did, and the command's declared result.
    fn kv_batch_put(items: &serde_json::Value) -> Output {
        output(json!({
            "type": "batch_results",
            "data": {
                "mode": "itemwise",
                "status": "ok",
                "applied": true,
                "commit": null,
                "items": items
            }
        }))
    }

    fn batch_item(index: u64, status: &str, kind: &str, key: &str) -> serde_json::Value {
        json!({
            "index": index,
            "status": status,
            "applied": true,
            "effect": { "applied": true, "kind": kind, "matched": false, "affected_count": 1 },
            "commit": null,
            "result": { "key": key },
            "error": null
        })
    }

    #[test]
    fn a_write_batch_is_one_row_per_item_saying_what_each_one_did() {
        let batch = kv_batch_put(&json!([
            batch_item(0, "ok", "created", "YQ=="),
            batch_item(1, "ok", "updated", "Yg=="),
        ]));
        assert_eq!(
            render_wire("kv_batch_put", &batch, Format::Human),
            only_stdout(concat!(
                "#  STATUS  EFFECT   KEY\n",
                "0  ok      created  a\n",
                "1  ok      updated  b\n",
            )),
            "a clean batch is the table alone (Q11)"
        );
        assert_eq!(
            render_wire("kv_batch_put", &batch, Format::Raw),
            only_stdout("0\tok\tcreated\ta\n1\tok\tupdated\tb\n")
        );
    }

    #[test]
    fn a_read_batch_has_no_effect_to_report_and_says_what_missed() {
        let batch = output(json!({
            "type": "batch_get_results",
            "data": {
                "mode": "itemwise",
                "status": "partial",
                "applied": false,
                "commit": null,
                "items": [
                    {
                        "index": 0, "status": "ok", "applied": false, "effect": null,
                        "commit": null, "error": null,
                        "result": { "found": true, "key": "YQ==", "value": "b25l", "version": 3, "timestamp": 3 }
                    },
                    {
                        "index": 1, "status": "miss", "applied": false, "effect": null,
                        "commit": null, "error": null,
                        "result": { "found": false, "key": "bWlzc2luZw==", "value": null, "version": null, "timestamp": null }
                    }
                ]
            }
        }));
        // A read applies nothing, so there is no EFFECT column to show.
        assert_eq!(
            render_wire("kv_batch_get", &batch, Format::Human),
            both(
                concat!(
                    "#  STATUS  KEY      VERSION  VALUE\n",
                    "0  ok      a              3  one\n",
                    "1  miss    missing        -  -\n",
                ),
                "-- itemwise: 1 ok, 1 miss\n"
            )
        );
        // A script reads the rows; the tally is a reader's line (R5).
        assert_eq!(
            render_wire("kv_batch_get", &batch, Format::Raw),
            only_stdout("0\tok\ta\t3\tone\n1\tmiss\tmissing\t\t\n")
        );
    }

    #[test]
    fn a_failed_item_gives_the_whole_batch_an_error_column() {
        let mut failed = batch_item(1, "error", "created", "Yg==");
        failed["error"] = json!({
            "class": "invalid_argument",
            "code": "invalid_argument.executor.batch_item",
            "message": "invalid key",
            "retryable": false,
            "retry_policy": "never",
            "commit_outcome": "not_started",
            "suggested_fix": "Correct the batch item input and retry.",
            "docs_url": "https://stratadb.org/e/invalid_argument.executor.batch_item",
            "reference_id": "err-test-000001",
            "details": [],
            "hints": []
        });
        let batch = kv_batch_put(&json!([batch_item(0, "ok", "created", "YQ=="), failed]));
        // The column appears for the batch, not for the row: a reader should
        // not have to notice a column that comes and goes.
        assert_eq!(
            render_wire("kv_batch_put", &batch, Format::Human),
            both(
                concat!(
                    "#  STATUS  EFFECT   KEY  ERROR\n",
                    "0  ok      created  a    -\n",
                    "1  error   created  b    invalid_argument.executor.batch_item\n",
                ),
                "-- itemwise: 1 ok, 1 error\n"
            ),
            "the code names what went wrong; the whole status is in --json"
        );
    }

    #[test]
    fn a_batch_with_no_items_says_so_to_a_reader_and_nothing_to_a_script() {
        let empty = kv_batch_put(&json!([]));
        assert_eq!(
            render_wire("kv_batch_put", &empty, Format::Human),
            only_stdout("(empty)\n")
        );
        assert_eq!(
            render_wire("kv_batch_put", &empty, Format::Raw),
            only_stdout("")
        );
    }

    #[test]
    fn an_output_with_no_declaration_and_no_arm_is_reported_not_guessed_at() {
        let mut out = String::new();
        let error = super::render_bespoke("no_such_output", &json!({}), Format::Human, &mut out)
            .expect_err("an unrenderable output is a renderer bug");
        assert!(
            error
                .to_string()
                .contains("no renderer for `no_such_output`"),
            "{error}"
        );
        assert!(out.is_empty(), "nothing is guessed at: {out:?}");
    }

    #[test]
    fn ping_answers_a_reader_with_a_sentence_and_a_script_with_the_version() {
        let pong = json!({ "type": "pong", "data": { "version": "1.2.1" } });
        assert_eq!(human(&pong), "pong 1.2.1\n");
        assert_eq!(raw(&pong), "1.2.1\n");
    }

    #[test]
    fn branch_diff_names_its_branches_then_one_row_per_change() {
        // Every change group becomes rows of its own, named by the column the
        // wire has no field for.
        let space = SpaceComparisonItem::new(
            "default".to_owned(),
            ComparedCapability::Kv,
            vec![ComparedEntityItem::new(bytes("added:key"), 42)],
            vec![ComparedEntityItem::new(bytes("removed:key"), 40)],
            vec![ComparedEntityItem::new(bytes("meta:survival_rate"), 41)],
        );
        let output = Output::BranchComparison(BranchComparisonItem::new(
            "default".to_owned(),
            "cleaned".to_owned(),
            vec![space],
        ));
        assert_eq!(
            render(&output, Format::Human),
            only_stdout(concat!(
                "branch_a  default\n",
                "branch_b  cleaned\n",
                "\n",
                "SPACE    CAPABILITY  CHANGE    IDENTITY            VERSION\n",
                "default  kv          added     added:key                42\n",
                "default  kv          removed   removed:key              40\n",
                "default  kv          modified  meta:survival_rate       41\n",
            ))
        );
        // A script gets the rows alone: the two branch names are what it asked
        // for, and every row repeats them.
        assert_eq!(
            render(&output, Format::Raw),
            only_stdout(concat!(
                "default\tkv\tadded\tadded:key\t42\n",
                "default\tkv\tremoved\tremoved:key\t40\n",
                "default\tkv\tmodified\tmeta:survival_rate\t41\n",
            ))
        );
    }

    #[test]
    fn branch_diff_non_utf8_identity_keeps_base64() {
        // Direction control: an identity that is not text is labelled for a
        // reader and bare for a script, like every other declared byte cell.
        let output = kv_comparison(vec![ComparedEntityItem::new(
            Bytes::new(vec![0xff, 0xfe]),
            1,
        )]);
        assert!(
            render(&output, Format::Human)
                .stdout
                .contains("base64://4="),
            "{:?}",
            render(&output, Format::Human).stdout
        );
        assert!(
            render(&output, Format::Raw).stdout.contains("\t//4=\t"),
            "{:?}",
            render(&output, Format::Raw).stdout
        );
    }

    #[test]
    fn branch_diff_human_decodes_but_json_stays_wire_true() {
        // End-to-end at the call site: the human/raw formats decode, JSON stays
        // base64 (machine-consumable), matching the KV commands (#3061).
        let output = kv_comparison(vec![ComparedEntityItem::new(
            bytes("meta:survival_rate"),
            41,
        )]);
        let human = render(&output, super::Format::Human).stdout;
        assert!(
            human.contains("meta:survival_rate"),
            "human output decodes the identity: {human}"
        );
        let json = render(&output, super::Format::Json).stdout;
        assert!(
            json.contains("bWV0YTpzdXJ2aXZhbF9yYXRl") && !json.contains("meta:survival_rate"),
            "json output stays base64: {json}"
        );
    }

    /// Renders a tagged output through its hand-written arm, the way
    /// `render_output` does for a `display: bespoke` command.
    fn bespoke(value: &serde_json::Value, format: Format) -> String {
        let (kind, data) = super::tagged_output(value).expect("a tagged output");
        let mut out = String::new();
        super::render_bespoke(kind, data, format, &mut out).expect("a bespoke arm renders");
        out
    }

    fn human(value: &serde_json::Value) -> String {
        bespoke(value, Format::Human)
    }

    fn raw(value: &serde_json::Value) -> String {
        bespoke(value, Format::Raw)
    }

    /// A wire record, read back into the typed `Output` the binary renders —
    /// the same round trip the contract harness makes.
    fn output(wire: serde_json::Value) -> Output {
        serde_json::from_value(wire).expect("wire deserializes into Output")
    }

    #[test]
    fn a_record_prints_its_declared_fields_and_a_script_gets_the_wire_names() {
        let branch = output(json!({
            "type": "branch",
            "data": {
                "branch_id": "dc42122c-83b7-5436-89bc-9ffa4299697c",
                "created_at": 3,
                "deleted_at": null,
                "generation": 1,
                "name": "feature",
                "parent": null,
                "state_revision": 0,
                "status": "active"
            }
        }));
        // Declared order, labels padded to the widest, `-` for a hole, and
        // only the declared facts: `branch_id` and `state_revision` are on
        // the wire and not in the block.
        assert_eq!(
            render_wire("branch_get", &branch, Format::Human),
            only_stdout(concat!(
                "name        feature\n",
                "parent      -\n",
                "status      active\n",
                "generation  1\n",
                "created_at  3\n",
                "deleted_at  -\n",
            ))
        );
        // The declared `header: parent` is a reader's label; a script reads
        // the pointer's own name, relative to the record's root (Q18).
        assert_eq!(
            render_wire("branch_get", &branch, Format::Raw),
            only_stdout(concat!(
                "name\tfeature\n",
                "parent.name\t\n",
                "status\tactive\n",
                "generation\t1\n",
                "created_at\t3\n",
                "deleted_at\t\n",
            ))
        );
    }

    #[test]
    fn a_nested_record_is_a_block_under_its_label_and_dotted_keys_for_a_script() {
        let info = output(json!({
            "type": "database_info",
            "data": {
                "branch_count": 1,
                "created": true,
                "default_branch": "default",
                "durable": false,
                "memory_budget": {
                    "source": "derived_from_host",
                    "total_bytes": 536_870_912,
                    "usable_host_bytes": 2_147_483_648_u64
                },
                "open": true,
                "space_count": 1,
                "target": "cache",
                "version": "1.2.1"
            }
        }));
        // The nested block has its own label width and its own indent, and
        // its byte counts read as sizes under their declared headers.
        assert_eq!(
            render_wire("info", &info, Format::Human),
            only_stdout(concat!(
                "target          cache\n",
                "version         1.2.1\n",
                "durable         false\n",
                "default_branch  default\n",
                "branch_count    1\n",
                "space_count     1\n",
                "memory_budget\n",
                "  source       derived_from_host\n",
                "  total        536.9 MB\n",
                "  usable_host  2.1 GB\n",
            ))
        );
        // A script gets the wire's own names and numbers, one dot per level,
        // and no line for the parent itself.
        assert_eq!(
            render_wire("info", &info, Format::Raw),
            only_stdout(concat!(
                "target\tcache\n",
                "version\t1.2.1\n",
                "durable\tfalse\n",
                "default_branch\tdefault\n",
                "branch_count\t1\n",
                "space_count\t1\n",
                "memory_budget.source\tderived_from_host\n",
                "memory_budget.total_bytes\t536870912\n",
                "memory_budget.usable_host_bytes\t2147483648\n",
            ))
        );
    }

    #[test]
    fn a_declared_table_inside_a_record_is_indented_under_its_label() {
        let preview = output(json!({
            "type": "branch_preview",
            "data": {
                "branch_point": 3,
                "capabilities_covered": ["kv"],
                "capabilities_unsupported": [],
                "conflicts": [{
                    "capability": "kv",
                    "identity": "YQ==",
                    "kind": "value_divergence",
                    "source_value": "dHdv",
                    "space": "default",
                    "strategy_result": "refused",
                    "target_value": "dGhyZWU="
                }],
                "derived_state": [],
                "source": "feature",
                "spaces_covered": ["default"],
                "strategy": "strict",
                "target": "default"
            }
        }));
        let human = render_wire("branch_preview", &preview, Format::Human).stdout;
        assert!(
            human.contains(concat!(
                "conflicts\n",
                "  CAPABILITY  IDENTITY  KIND              SOURCE_VALUE  SPACE    STRATEGY_RESULT  TARGET_VALUE\n",
                "  kv          a         value_divergence  two           default  refused          three\n",
            )),
            "the table is indented under its label, with its declared byte \
             columns decoded (Q20): {human}"
        );
        // An empty list or table has nothing to show: `-`, not `[]`.
        assert!(
            human.contains("capabilities_unsupported  -\n")
                && human.contains("derived_state             -\n"),
            "{human}"
        );
        let raw = render_wire("branch_preview", &preview, Format::Raw).stdout;
        assert!(
            raw.contains("derived_state\t[]\n") && raw.contains("capabilities_covered\t[\"kv\"]\n"),
            "a script gets the wire array, compact: {raw}"
        );
    }

    #[test]
    fn a_record_with_nothing_to_show_is_nil_for_a_reader_and_silent_for_a_script() {
        // A `{found, value}` wire says so itself.
        let missing = output(json!({
            "type": "kv_versioned_value",
            "data": { "found": false, "value": null }
        }));
        assert_eq!(
            render_wire("kv_get", &missing, Format::Human),
            only_stdout("(nil)\n")
        );
        assert_eq!(
            render_wire("kv_get", &missing, Format::Raw),
            only_stdout("")
        );
        // A wire without one is a miss when the record itself is null — for
        // `remote`, the record sits at `/data/origin`, which is where every
        // declared field agrees it is.
        let no_origin =
            output(json!({ "type": "remote_origin_result", "data": { "origin": null } }));
        assert_eq!(
            render_wire("remote_get", &no_origin, Format::Human),
            only_stdout("(nil)\n")
        );
        assert_eq!(
            render_wire("remote_get", &no_origin, Format::Raw),
            only_stdout("")
        );
        // A status read always has an answer, and `false` is one of them.
        let exists = output(json!({ "type": "bool", "data": false }));
        assert_eq!(
            render_wire("kv_exists", &exists, Format::Human),
            only_stdout("false\n")
        );
        assert_eq!(
            render_wire("kv_exists", &exists, Format::Raw),
            only_stdout("false\n")
        );
    }

    #[test]
    fn a_declared_value_is_the_whole_answer() {
        let stored = |value: &str| {
            output(json!({
                "type": "kv_versioned_value",
                "data": { "found": true, "value": { "value": value, "version": 1, "timestamp": 10 } }
            }))
        };
        // `as: bytes`: the text the bytes spell, labelled for a reader when
        // they are not text at all.
        assert_eq!(
            render_wire("kv_get", &stored("aGVsbG8="), Format::Human),
            only_stdout("hello\n")
        );
        assert_eq!(
            render_wire("kv_get", &stored("/w=="), Format::Human),
            only_stdout("base64:/w==\n")
        );
        // `as: json`: a reader gets the document laid out, a script the leaf.
        let document = output(json!({
            "type": "json_versioned_value",
            "data": { "found": true, "value": {
                "value": { "name": "Ada" }, "version": 3, "timestamp": 30, "document_version": 1
            } }
        }));
        assert_eq!(
            render_wire("json_get", &document, Format::Human),
            only_stdout("{\n  \"name\": \"Ada\"\n}\n")
        );
        assert_eq!(
            render_wire("json_get", &document, Format::Raw),
            only_stdout("{\"name\":\"Ada\"}\n")
        );
        // #3064: a stored JSON null is a value, and says so in both formats —
        // a miss is what prints nothing.
        let stored_null = output(json!({
            "type": "json_versioned_value",
            "data": { "found": true, "value": {
                "value": null, "version": 3, "timestamp": 30, "document_version": 1
            } }
        }));
        assert_eq!(
            render_wire("json_get", &stored_null, Format::Human),
            only_stdout("null\n")
        );
        assert_eq!(
            render_wire("json_get", &stored_null, Format::Raw),
            only_stdout("null\n")
        );
    }

    #[test]
    fn an_action_prints_a_receipt_for_a_reader_and_its_record_for_a_script() {
        let exported = output(json!({
            "type": "arrow_export_result",
            "data": {
                "format": "csv",
                "paths": ["kv_out.csv"],
                "primitive": "kv",
                "row_count": 3,
                "size_bytes": 135
            }
        }));
        assert_eq!(
            render_wire("arrow_export", &exported, Format::Human),
            only_stdout("exported 3 rows of kv to kv_out.csv (135 B)\n")
        );
        // Q17: an action declares no fields to choose between, so a script
        // gets the whole record, in the wire's own order.
        assert_eq!(
            render_wire("arrow_export", &exported, Format::Raw),
            only_stdout(concat!(
                "format\tcsv\n",
                "paths\t[\"kv_out.csv\"]\n",
                "primitive\tkv\n",
                "row_count\t3\n",
                "size_bytes\t135\n",
            ))
        );
    }

    #[test]
    fn a_declared_value_that_is_null_has_nothing_to_show() {
        // Reachable only through a declaration whose pointer resolves to a
        // null the rule does not call a miss; the answer is the same one a
        // miss gives, not an empty line.
        assert_eq!(
            super::value_line(&Value::Null, None, Format::Human),
            "(nil)\n"
        );
        assert_eq!(super::value_line(&Value::Null, None, Format::Raw), "");
        // `as: json` is the exception: there, a null is the value (#3064).
        assert_eq!(
            super::value_line(&Value::Null, Some(CliDisplayAs::Json), Format::Human),
            "null\n"
        );
    }

    #[test]
    fn an_action_receipt_that_reads_the_request_is_refused() {
        // A write ack may quote the request when its wire carries no identity
        // (Q15); an action reports what it did, and its response says what
        // that was — so the two roots do not mix.
        let display = CliDisplay {
            receipt: Some("cloned {/request/dataset}".to_owned()),
            ..CliDisplay::default()
        };
        let error = super::ReceiptDecl::parse(&display).expect_err("the request root is refused");
        assert!(error.contains("reads the response"), "{error}");
        super::ReceiptDecl::parse(&CliDisplay {
            receipt: Some("cloned {/data/dataset}".to_owned()),
            ..CliDisplay::default()
        })
        .expect("a receipt that reads the response parses");
    }

    #[test]
    fn a_search_shows_its_diagnostics_to_a_reader_only() {
        let found = output(json!({
            "type": "vector_index_query",
            "data": {
                "matches": [{ "key": "doc-a", "score": 1.0, "metadata": null }],
                "diagnostics": {
                    "active_delta_count": 0,
                    "active_delta_seal_threshold": 16,
                    "active_delta_source_count": 0,
                    "artifact_sources": [],
                    "collection": "docs",
                    "collection_exact_threshold": 64,
                    "derived_bytes": 0,
                    "exact_fallback_count": 0,
                    "exact_source_count": 1,
                    "filtered_underfill_fallback": false,
                    "flat_source_count": 0,
                    "hnsw_graph_builds": 0,
                    "hnsw_memory_budget_bytes": 67_108_864,
                    "hnsw_source_count": 0,
                    "indexed_source_count": 0,
                    "indexed_vector_count": 0,
                    "last_query_fallback_reason": null,
                    "last_query_used_index": false,
                    "manifest_generation": null,
                    "manifest_inherited_ref_count": 0,
                    "manifest_owned_ref_count": 0,
                    "manifest_ref_count": 0,
                    "manifest_status": "missing",
                    "overfetch_factor": 4,
                    "policy_mode": "auto",
                    "resolved_index_kind_summary": "exact",
                    "source_candidate_limit": 64,
                    "source_flat_threshold": 64,
                    "source_hnsw_threshold": 64
                }
            }
        }));
        let human = render_wire("vector_index_query", &found, Format::Human).stdout;
        assert!(
            human.starts_with("KEY    SCORE  METADATA\ndoc-a    1.0  -\n\ndiagnostics\n"),
            "the block follows the table after a blank line: {human}"
        );
        assert!(
            human.contains("  artifact_sources              -\n")
                && human.contains("  hnsw_memory_budget            67.1 MB\n"),
            "{human}"
        );
        // A script's rows stay one record per line: the diagnostics are a
        // reader's footer, not a column.
        assert_eq!(
            render_wire("vector_index_query", &found, Format::Raw),
            only_stdout("doc-a\t1.0\t\n")
        );
    }

    #[test]
    fn a_raw_key_is_only_trimmed_at_a_segment_boundary() {
        // The root of a record is an ancestor of its fields, so the trim is
        // always a whole segment; a name that merely starts with the root's
        // text keeps its pointer rather than losing three characters.
        assert_eq!(
            super::raw_key("/data/memory_budget/total_bytes", "/data"),
            "memory_budget.total_bytes"
        );
        assert_eq!(
            super::raw_key("/data/origin/dataset", "/data/origin"),
            "dataset"
        );
        assert_eq!(super::raw_key("/database/name", "/data"), "database.name");
    }

    #[test]
    fn human_inference_generation_has_stats_line() {
        let value = json!({
            "type": "inference_generation",
            "data": {
                "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
                "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
            }
        });
        assert_eq!(
            human(&value),
            "hi\n-- stop: stop · prompt 3 tok · completion 2 tok\n"
        );
    }

    #[test]
    fn raw_inference_generation_omits_stats_line() {
        let value = json!({
            "type": "inference_generation",
            "data": {
                "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
                "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
            }
        });
        assert_eq!(raw(&value), "hi\n");
    }

    #[test]
    fn human_inference_text() {
        let value = json!({ "type": "inference_text", "data": "hello" });
        assert_eq!(human(&value), "hello\n");
    }

    #[test]
    fn human_inference_token_ids() {
        let value = json!({ "type": "inference_token_ids", "data": [1, 2, 3] });
        assert_eq!(human(&value), "1 2 3\n");
    }

    #[test]
    fn human_inference_embeddings_summary_and_raw_values() {
        let value = json!({
            "type": "inference_embeddings",
            "data": { "dimension": 4, "data": [{ "index": 0, "embedding": [0.1, 0.2, 0.3, 0.4] }] }
        });
        assert_eq!(
            human(&value),
            "1 embeddings · dim 4\n  [0] [0.1000, 0.2000, 0.3000, 0.4000]\n"
        );
        assert_eq!(raw(&value), "0.1 0.2 0.3 0.4\n");
    }

    #[test]
    fn human_inference_ranking_sorts_scores_and_reports_errors() {
        let value = json!({
            "type": "inference_ranking",
            "data": { "items": [
                { "status": "ok", "index": 0, "score": 0.9 },
                { "status": "ok", "index": 1, "score": 0.5 },
                { "status": "error", "code": "bad" }
            ] }
        });
        assert_eq!(human(&value), "0\t0.900000\n1\t0.500000\nfailed: bad\n");
    }

    #[cfg(feature = "inference")]
    #[test]
    fn human_inference_models_list_none_and_nil() {
        let list = json!({
            "type": "inference_models",
            "data": { "items": [{
                "name": "m", "task": "chat", "architecture": "llama",
                "default_quant": "q4", "is_local": true, "runnable": true,
                "size_bytes": 1_048_576
            }] }
        });
        assert_eq!(human(&list), "m\tchat\tllama\tq4\tready\t1 MB\n");
        let none = json!({ "type": "inference_models", "data": { "items": [] } });
        assert_eq!(human(&none), "(none)\n");
        let nil = json!({ "type": "inference_models", "data": {} });
        assert_eq!(human(&nil), "(nil)\n");
    }

    /// #3235: the table's size column is the inference crate's formatter —
    /// decimal units, the same text the download offer's refusal quotes — not
    /// a binary-unit rendering of its own.
    #[cfg(feature = "inference")]
    #[test]
    fn human_inference_models_sizes_in_the_units_the_refusal_uses() {
        let row = |size_bytes: u64| {
            human(&json!({
                "type": "inference_models",
                "data": { "items": [{
                    "name": "m", "task": "chat", "architecture": "llama",
                    "default_quant": "q4", "is_local": true, "runnable": true,
                    "size_bytes": size_bytes
                }] }
            }))
        };
        assert_eq!(row(1_100_000_000), "m\tchat\tllama\tq4\tready\t1.1 GB\n");
        assert_eq!(row(45_000_000), "m\tchat\tllama\tq4\tready\t45 MB\n");
        assert_eq!(row(999_999), "m\tchat\tllama\tq4\tready\t1 MB\n");
        assert_eq!(
            row(1_100_000_000).trim_end(),
            format!(
                "m\tchat\tllama\tq4\tready\t{}",
                strata_executor::format_model_size(1_100_000_000)
            )
        );
    }

    /// #3124: a released binary lists models it cannot load. The row must say
    /// so, and the footer must name both ways forward.
    #[cfg(feature = "inference")]
    #[test]
    fn human_inference_models_marks_what_this_build_cannot_run() {
        let list = json!({
            "type": "inference_models",
            "data": { "items": [
                {
                    "name": "miniLM", "task": "embed", "architecture": "bert",
                    "default_quant": "f16", "is_local": true, "runnable": false,
                    "size_bytes": 1_048_576
                },
                {
                    "name": "gpt2", "task": "generate", "architecture": "gpt2",
                    "default_quant": "q8_0", "is_local": false, "runnable": false,
                    "size_bytes": 2_097_152
                }
            ] }
        });
        let rendered = human(&list);
        // The file being present must not read as "usable".
        assert!(
            rendered.contains("miniLM\tembed\tbert\tf16\tunavailable\t1 MB"),
            "a downloaded but unrunnable model must still read unavailable: {rendered}"
        );
        assert!(rendered.contains("2 model(s) unavailable"));
        assert!(rendered.contains("strata inference install-local"));
        assert!(
            rendered.contains("openai:"),
            "the footer names the no-rebuild path"
        );
    }

    /// A build that can run local models says nothing about unavailability, and
    /// distinguishes downloaded from not.
    #[cfg(feature = "inference")]
    #[test]
    fn human_inference_models_separates_ready_from_not_downloaded() {
        let list = json!({
            "type": "inference_models",
            "data": { "items": [
                {
                    "name": "here", "task": "embed", "architecture": "bert",
                    "default_quant": "f16", "is_local": true, "runnable": true,
                    "size_bytes": 1_048_576
                },
                {
                    "name": "absent", "task": "embed", "architecture": "bert",
                    "default_quant": "f16", "is_local": false, "runnable": true,
                    "size_bytes": 1_048_576
                }
            ] }
        });
        let rendered = human(&list);
        assert!(rendered.contains("here\tembed\tbert\tf16\tready\t1 MB"));
        assert!(rendered.contains("absent\tembed\tbert\tf16\tnot downloaded\t1 MB"));
        assert!(
            !rendered.contains("unavailable"),
            "nothing is unavailable in a build that can run them: {rendered}"
        );
    }

    /// The download footer fires only when this build cannot download AND
    /// something is missing — the mutation gate found both halves untested.
    #[test]
    fn the_models_footer_tracks_both_download_ability_and_what_is_missing() {
        const FOOTER: &str = "cannot download models";

        let status = |can_download: bool, downloaded: u64, catalogued: u64| {
            human(&json!({
                "type": "inference_status",
                "data": {
                    "local_execution": false,
                    "model_download": can_download,
                    "providers": [],
                    "models_dir": "/models",
                    "models_downloaded": downloaded,
                    "models_catalogued": catalogued,
                }
            }))
        };

        // Cannot download, and models are missing: say so.
        assert!(status(false, 1, 3).contains(FOOTER));

        // Cannot download, but nothing is missing — nothing to say.
        assert!(
            !status(false, 3, 3).contains(FOOTER),
            "a complete set needs no download advice"
        );

        // Can download: the advice is irrelevant however many are missing.
        assert!(
            !status(true, 1, 3).contains(FOOTER),
            "a build that can download does not need telling"
        );
    }

    /// A provider's row says where a request goes only when that is not the
    /// public endpoint (#3270): the override and its source, never a line for
    /// the default.
    #[test]
    fn a_provider_row_names_a_redirected_endpoint_and_its_source() {
        let status = |base_url_source: Option<&str>| {
            human(&json!({
                "type": "inference_status",
                "data": {
                    "local_execution": false,
                    "model_download": true,
                    "providers": [{
                        "provider": "openai",
                        "feature_enabled": true,
                        "requires_api_key": true,
                        "ready": true,
                        "model_prefix": "openai:",
                        "key_source": "OPENAI_API_KEY",
                        "base_url": "http://127.0.0.1:8000/v1",
                        "base_url_source": base_url_source,
                    }],
                    "models_dir": "/models",
                    "models_downloaded": 0,
                    "models_catalogued": 0,
                }
            }))
        };

        let redirected = status(Some("OPENAI_BASE_URL"));
        assert!(
            redirected.contains("  openai\tready -- key from OPENAI_API_KEY; use openai:<model>\n\tat http://127.0.0.1:8000/v1 (from OPENAI_BASE_URL)\n"),
            "{redirected}"
        );
        let public = status(None);
        assert!(
            !public.contains("\tat "),
            "the public endpoint earns no line: {public}"
        );
    }

    #[test]
    fn human_inference_model_pulled() {
        let value = json!({
            "type": "inference_model_pulled",
            "data": { "model": "m", "path": "/p" }
        });
        assert_eq!(human(&value), "pulled m -> /p\n");
    }

    #[test]
    fn human_inference_unload_result_both_branches() {
        let unloaded = json!({ "type": "inference_unload_result", "data": { "unloaded": true } });
        assert_eq!(human(&unloaded), "unloaded\n");
        let cold = json!({ "type": "inference_unload_result", "data": { "unloaded": false } });
        assert_eq!(human(&cold), "no cached entry\n");
    }

    #[test]
    fn a_cli_report_shows_its_payload_not_its_envelope() {
        // `doctor`, `init`, `ipc start`, the REPL's context line: the CLI's own
        // JSON, which no declaration describes. The name is for `--json`; the
        // lines show what it carries.
        let report = json!({ "type": "doctor", "data": { "binary": "1.2.1" } });
        assert_eq!(
            super::value_to_string(&report, Format::Human).expect("renders"),
            "{\n  \"binary\": \"1.2.1\"\n}\n"
        );
        assert_eq!(
            super::value_to_string(&report, Format::Raw).expect("renders"),
            "{\"binary\":\"1.2.1\"}\n"
        );
        // A scalar payload reads as itself, and an absent one says so.
        let scalar = json!({ "type": "ipc_stopped", "data": true });
        assert_eq!(
            super::value_to_string(&scalar, Format::Human).expect("renders"),
            "true\n"
        );
        assert_eq!(
            super::value_to_string(&json!(null), Format::Human).expect("renders"),
            "(nil)\n"
        );
        assert_eq!(
            super::value_to_string(&json!(null), Format::Raw).expect("renders"),
            ""
        );
    }
}
