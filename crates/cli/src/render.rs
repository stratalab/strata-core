//! CLI response rendering.

use base64::Engine as _;
use serde::Serialize;
use serde_json::Value;
use strata_executor::cli_metadata::{
    CliDisplay, CliDisplayDecl, CliRenderRule, ReceiptFilter, ReceiptPlaceholder, ReceiptSegment,
    ReceiptTemplate, ReceiptValue,
};
use strata_executor::{Command, Output};

use crate::catalog;
use crate::options::Format;
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
/// command's `display:` declaration from the embedded catalog and, when the
/// declared receipt quotes the request (`{/request/name}`), the request
/// itself. `--json` and `--pretty` read no declaration — the envelope is the
/// record — so they carry none. Wasm-safe.
#[derive(Clone, Debug)]
pub struct Invocation {
    ack: Option<MutationAck>,
}

/// A parsed `mutation_ack` declaration (output contract R1/R2).
#[derive(Clone, Debug)]
struct MutationAck {
    receipt: ReceiptTemplate,
    identity: Vec<ReceiptPlaceholder>,
    noun: Option<String>,
    request: Option<Value>,
}

impl Invocation {
    /// A rendering with no declaration: JSON/pretty output, progress events,
    /// and every output the catalog does not describe.
    pub const fn none() -> Self {
        Self { ack: None }
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
        if entry.render != CliRenderRule::MutationAck {
            return Ok(Self::none());
        }
        let mut ack = MutationAck::parse(display).map_err(|reason| {
            CliError::usage(format!(
                "display declaration for `{wire}` is invalid: {reason}"
            ))
        })?;
        if ack.quotes_request() {
            ack.request = Some(request()?);
        }
        Ok(Self { ack: Some(ack) })
    }
}

impl MutationAck {
    /// Parses a `mutation_ack` display. The IDL guard has already accepted
    /// every shipped declaration; an error here means the embedded catalog
    /// and the guard disagree.
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
        self.receipt
            .placeholders()
            .chain(&self.identity)
            .any(|placeholder| match placeholder {
                ReceiptPlaceholder::Value(value) => value.pointer.starts_with("/request/"),
                ReceiptPlaceholder::Verb => false,
            })
    }
}

/// Renders an executor `Output` for `format`, without touching stdio. JSON
/// and pretty print the envelope; human and raw render a declared write
/// through its `display:` declaration (`invocation`) and everything else
/// through the family renderers. Wasm-safe: the binary prints the result
/// (`print_output`), the playground returns it (`run_line`).
pub fn render_output(
    output: &Output,
    invocation: &Invocation,
    format: Format,
) -> Result<Rendered, CliError> {
    let mut value = serde_json::to_value(output)?;
    if matches!(format, Format::Json | Format::Pretty) {
        return Ok(Rendered::stdout(terminated(
            value_to_string(&value, format)?,
            format,
        )));
    }
    if let Some(ack) = &invocation.ack {
        return Ok(render_mutation_ack(&value, ack, format));
    }
    // Human and raw formats show KV keys/values as text when possible. The
    // decode happens here — with the typed `Output` in hand — so only fields
    // the schema declares as `Bytes` are touched (see `humanize_kv_bytes`).
    // JSON and pretty formats stay wire-true (base64).
    humanize_kv_bytes(output, &mut value);
    // #3112 S5: a wall-clock instant is only useful to a reader as a date.
    // JSON and pretty stay wire-true (raw epoch micros) so machine
    // consumers keep an unambiguous number.
    humanize_committed_at(&mut value);
    Ok(Rendered::stdout(value_to_string(&value, format)?))
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
        Format::Human => {
            for segment in ack.receipt.segments() {
                match segment {
                    ReceiptSegment::Literal(text) => out.push_str(text),
                    ReceiptSegment::Placeholder(placeholder) => {
                        out.push_str(&render_placeholder(placeholder, envelope, request, format));
                    }
                }
            }
        }
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
/// non-inference builds cannot import it back.
fn size_text(bytes: u64) -> String {
    const GB: u64 = 1_000_000_000;
    const MB: u64 = 1_000_000;
    #[allow(clippy::cast_precision_loss)] // A byte count shown to one decimal.
    let scaled = |unit: u64| bytes as f64 / unit as f64;
    if bytes >= GB {
        format!("{:.1} GB", scaled(GB))
    } else if bytes >= MB {
        format!("{:.0} MB", scaled(MB))
    } else {
        format!("{bytes} bytes")
    }
}

/// Renders a already-serialized envelope `Value` to its display string for
/// `format`, without touching stdio. Wasm-safe.
pub fn value_to_string(value: &Value, format: Format) -> Result<String, CliError> {
    Ok(match format {
        Format::Json => serde_json::to_string(value)?,
        Format::Pretty => serde_json::to_string_pretty(value)?,
        Format::Human => {
            let mut out = String::new();
            render_human(value, &mut out)?;
            out
        }
        Format::Raw => {
            let mut out = String::new();
            render_raw(value, &mut out);
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

fn render_human(value: &Value, out: &mut String) -> Result<(), CliError> {
    if let Some((kind, data)) = tagged_output(value) {
        match kind {
            "pong" => line!(
                out,
                "pong {}",
                data.get("version").and_then(Value::as_str).unwrap_or("")
            ),
            "bool" | "uint" => line!(out, "{}", scalar_summary(data)),
            "event_count" => print_count(data, out),
            "kv_versioned_value" => print_optional_data(data, out),
            "vector_data" | "event_record" | "graph_node_result" | "graph_edge_result" => {
                print_optional_record(data, out)?;
            }
            "json_value" | "json_versioned_value" => print_maybe_json(kind, data, out)?,
            "json_version_history" => print_bare_items(data, out),
            "vector_matches" => print_matches_data(data, out),
            "inference_generation" => print_inference_generation(data, true, out),
            "inference_text" => line!(out, "{}", data.as_str().unwrap_or_default()),
            "inference_token_ids" => print_token_ids(data, out),
            "inference_embeddings" => print_embeddings_summary(data, out),
            "inference_ranking" => print_ranking(data, out),
            #[cfg(feature = "inference")]
            "inference_models" => print_inference_models(data, out),
            "inference_status" => print_inference_status(data, out),
            "inference_model_pulled" => print_model_pulled(data, out),
            "inference_unload_result" => line!(
                out,
                "{}",
                if data.get("unloaded").and_then(Value::as_bool) == Some(true) {
                    "unloaded"
                } else {
                    "no cached entry"
                }
            ),
            "described" => print_described(data, out),
            _ => render_human_data(data, out)?,
        }
        return Ok(());
    }

    render_human_data(value, out)
}

fn render_human_data(data: &Value, out: &mut String) -> Result<(), CliError> {
    if data.is_null() {
        line!(out, "(nil)");
        return Ok(());
    }

    if let Some(items) = data.get("items").and_then(Value::as_array) {
        print_items(items, out);
        print_page_tail(data, out);
        return Ok(());
    }

    if let Some(items) = data.get("matches").and_then(Value::as_array) {
        print_vector_matches(items, out);
        return Ok(());
    }

    if let Some(found) = data.get("found").and_then(Value::as_bool) {
        if !found {
            line!(out, "(nil)");
            return Ok(());
        }
        if let Some(value) = data.get("value") {
            line!(out, "{}", scalar_summary(value));
            return Ok(());
        }
    }

    match data {
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            line!(out, "{}", scalar_summary(data));
        }
        _ => line!(out, "{}", serde_json::to_string_pretty(data)?),
    }
    Ok(())
}

fn render_raw(value: &Value, out: &mut String) {
    let (kind, data) = tagged_output(value).unwrap_or(("", value));

    if data.is_null() {
        return;
    }

    match kind {
        "json_value" | "json_versioned_value" => {
            let found = data.get("found").and_then(Value::as_bool).unwrap_or(false);
            if let Some(leaf) = json_leaf(kind, data, found) {
                line!(out, "{}", raw_json_leaf(leaf));
            }
            return;
        }
        "kv_versioned_value" => {
            // The KV record nests the stored value under its own `value` field.
            if let Some(value) = point_read_record(data).and_then(|record| record.get("value")) {
                line!(out, "{}", raw_scalar(value));
            }
            return;
        }
        "json_version_history" => {
            if let Some(items) = data.as_array() {
                print_items(items, out);
            }
            return;
        }
        "vector_matches" => {
            print_matches_data(data, out);
            return;
        }
        "event_count" => {
            print_count(data, out);
            return;
        }
        "inference_generation" => {
            print_inference_generation(data, false, out);
            return;
        }
        "inference_text" => {
            line!(out, "{}", data.as_str().unwrap_or_default());
            return;
        }
        "inference_token_ids" => {
            print_token_ids(data, out);
            return;
        }
        "inference_embeddings" => {
            print_embedding_values(data, out);
            return;
        }
        _ => {}
    }

    if let Some(items) = data.get("items").and_then(Value::as_array) {
        print_items(items, out);
        return;
    }

    if let Some(items) = data.get("matches").and_then(Value::as_array) {
        print_vector_matches(items, out);
        return;
    }

    if let Some(found) = data.get("found").and_then(Value::as_bool) {
        if !found {
            return;
        }
        if let Some(value) = data.get("value") {
            line!(out, "{}", raw_scalar(value));
            return;
        }
    }

    if let Some(value) = data.get("value") {
        line!(out, "{}", raw_scalar(value));
        return;
    }

    line!(out, "{}", raw_scalar(data));
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

/// Unwraps a `{found, value}` point-read envelope to its record, or `None`
/// when the record is absent.
fn point_read_record(data: &Value) -> Option<&Value> {
    match data.get("found").and_then(Value::as_bool) {
        Some(true) => data.get("value"),
        _ => None,
    }
}

// The local `line!` macro expands to an inline block, which trips the pedantic
// `single_match_else` lint on these Option matches where the std `println!`
// macro (opaque to the lint) did not. The match form is intentional here.
#[allow(clippy::single_match_else)]
fn print_optional_data(data: &Value, out: &mut String) {
    // KV point reads answer with a {found, value} envelope whose record nests
    // the stored value under its own `value` field.
    match point_read_record(data) {
        None => line!(out, "(nil)"),
        Some(record) => match record.get("value") {
            Some(value) => line!(out, "{}", scalar_summary(value)),
            None => line!(out, "{}", scalar_summary(record)),
        },
    }
}

fn print_optional_record(data: &Value, out: &mut String) -> Result<(), CliError> {
    // Vector/event/graph point reads share the envelope but carry structured
    // records; show the record itself, or `(nil)` when absent.
    match point_read_record(data) {
        None => line!(out, "(nil)"),
        Some(record) => render_human_data(record, out)?,
    }
    Ok(())
}

#[allow(clippy::single_match_else)]
fn print_maybe_json(kind: &str, data: &Value, out: &mut String) -> Result<(), CliError> {
    let Some(found) = data.get("found").and_then(Value::as_bool) else {
        line!(out, "{}", serde_json::to_string_pretty(data)?);
        return Ok(());
    };
    match json_leaf(kind, data, found) {
        // Human output shows the JSON encoding of the leaf value so `"null"`
        // vs `null` and strings vs numbers stay unambiguous; raw output
        // unwraps strings for scripting.
        Some(leaf) => line!(out, "{}", serde_json::to_string(leaf)?),
        None => line!(out, "(nil)"),
    }
    Ok(())
}

/// Extracts the leaf JSON value from a maybe-json envelope. The
/// `json_versioned_value` shape nests the document value inside commit facts
/// (`{found, value: {value, version, timestamp, document_version}}`), so the
/// leaf sits one level deeper than in `json_value`.
fn json_leaf<'a>(kind: &str, data: &'a Value, found: bool) -> Option<&'a Value> {
    if !found {
        return None;
    }
    let value = data.get("value")?;
    if kind == "json_versioned_value" {
        value.get("value")
    } else {
        Some(value)
    }
}

/// `json_version_history` serializes as a bare item array (or null when the
/// document never existed), unlike the `{count, items}` KV history envelope.
fn print_bare_items(data: &Value, out: &mut String) {
    if let Value::Array(items) = data {
        print_items(items, out);
    } else {
        line!(out, "(nil)");
    }
}

/// `vector_matches` serializes its match list as the bare `data` array, so the
/// tabular key/score renderer must be dispatched by tag.
#[allow(clippy::single_match_else)]
fn print_matches_data(data: &Value, out: &mut String) {
    match data.as_array() {
        Some(items) => print_vector_matches(items, out),
        None => line!(out, "(empty)"),
    }
}

/// `event_count` wraps its count in `{count}`; humans and scripts get the
/// bare number, matching how `kv count` (a plain `uint`) renders.
fn print_count(data: &Value, out: &mut String) {
    line!(
        out,
        "{}",
        data.get("count").map_or_else(String::new, scalar_summary)
    );
}

fn print_items(items: &[Value], out: &mut String) {
    for item in items {
        line!(out, "{}", scalar_summary(item));
    }
    if items.is_empty() {
        line!(out, "(empty)");
    }
}

fn print_page_tail(data: &Value, out: &mut String) {
    if data.get("has_more").and_then(Value::as_bool) == Some(true) {
        if let Some(cursor) = data.get("cursor") {
            // Actionable, not just a token (#2998): tell the reader how to
            // fetch the next page.
            line!(
                out,
                "-- more: add --cursor {} to the same command",
                scalar_summary(cursor)
            );
        }
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

fn print_vector_matches(items: &[Value], out: &mut String) {
    for item in items {
        let key = item
            .get("key")
            .map_or_else(|| scalar_summary(item), scalar_summary);
        let score = item
            .get("score")
            .or_else(|| item.get("distance"))
            .map(scalar_summary)
            .unwrap_or_default();
        if score.is_empty() {
            line!(out, "{key}");
        } else {
            line!(out, "{key}\t{score}");
        }
    }
    if items.is_empty() {
        line!(out, "(empty)");
    }
}

/// Rewrites schema-declared `Bytes` fields from base64 to readable text for
/// human/raw output.
///
/// Driven by the typed `Output` variant, never by value shape, so a genuine
/// string that merely looks like base64 is never touched — the defect that
/// retired the old integer-array heuristic. Fields whose bytes are not valid
/// UTF-8 keep their base64 form. Continuation cursors deliberately stay
/// base64: they are opaque tokens that `--cursor` accepts verbatim.
fn humanize_kv_bytes(output: &Output, value: &mut Value) {
    let Some(data) = value.get_mut("data") else {
        return;
    };
    match output {
        // The stored value sits inside the {found, value} point-read envelope,
        // one level below `data`.
        Output::KvVersionedValue(_) => {
            if let Some(record) = data.get_mut("value") {
                decode_bytes_fields(record, &["value"]);
            }
        }
        Output::VersionHistory(_) => decode_bytes_item_fields(data, &["value"]),
        Output::KeysPage { .. } => {
            if let Some(items) = data.get_mut("items").and_then(Value::as_array_mut) {
                for item in items {
                    decode_bytes_value(item);
                }
            }
        }
        Output::KvScanResult { .. } | Output::SampleResult { .. } => {
            decode_bytes_item_fields(data, &["key", "value"]);
        }
        // Branch diff/merge/preview identities (and values) are logical keys —
        // decode them like `kv history` does, so the one command whose job is
        // to be read by a human is readable (#3061).
        Output::BranchComparison(_) => {
            if let Some(spaces) = data.get_mut("spaces").and_then(Value::as_array_mut) {
                for space in spaces {
                    for group in ["added", "removed", "modified"] {
                        decode_bytes_in_array(space, group, &["identity"]);
                    }
                }
            }
        }
        Output::BranchMerge(_) => {
            decode_bytes_in_array(data, "applied", &["identity", "value"]);
            decode_bytes_in_array(data, "deleted", &["identity", "value"]);
            decode_bytes_in_array(
                data,
                "conflicts",
                &["identity", "source_value", "target_value"],
            );
        }
        Output::BranchPreview(_) => {
            decode_bytes_in_array(
                data,
                "conflicts",
                &["identity", "source_value", "target_value"],
            );
        }
        // Batch outputs also carry Bytes but are not reachable from any CLI
        // verb today; their base64 form is still correct if that changes.
        _ => {}
    }
}

fn decode_bytes_item_fields(data: &mut Value, fields: &[&str]) {
    decode_bytes_in_array(data, "items", fields);
}

/// Decodes `fields` on every object in `object[array_field]`, when that is an
/// array. Used for the item lists KV, branch diff, and promotion outputs carry.
fn decode_bytes_in_array(object: &mut Value, array_field: &str, fields: &[&str]) {
    if let Some(items) = object.get_mut(array_field).and_then(Value::as_array_mut) {
        for item in items {
            decode_bytes_fields(item, fields);
        }
    }
}

fn decode_bytes_fields(object: &mut Value, fields: &[&str]) {
    for field in fields {
        if let Some(value) = object.get_mut(*field) {
            decode_bytes_value(value);
        }
    }
}

fn decode_bytes_value(value: &mut Value) {
    let Value::String(encoded) = value else {
        return;
    };
    if let Some(text) = decode_base64_text(encoded) {
        *value = Value::String(text);
    }
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

/// #3112 S5: renders every `committed_at` in an envelope as a local date-time
/// with its offset.
///
/// Recurses because instants appear at several depths — on a write ack's
/// commit receipt, and on every row of a history list. Only this one field is
/// touched, and only for human-facing formats; anything that is not a number
/// is left exactly as it is, so an already-formatted or absent value passes
/// through untouched.
fn humanize_committed_at(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields.iter_mut() {
                if key == "committed_at" {
                    if let Some(micros) = child.as_u64() {
                        *child = Value::String(crate::wall_clock::format_instant(micros));
                    }
                } else {
                    humanize_committed_at(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(humanize_committed_at),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use strata_executor::{
        BranchComparisonItem, BranchPreviewItem, Bytes, CommitDurability, CommitReceipt,
        ComparedCapability, ComparedEntityItem, ConflictKind, ConflictStrategyResult, Maybe,
        MutationEffect, Output, PageInfo, PreviewConflictItem, PromotedEntityItem,
        PromotionOutcomeItem, PromotionStrategy, ScanItem, SpaceComparisonItem, VersionedValue,
    };

    use strata_executor::cli_metadata::CliDisplay;
    use strata_executor::{AdminPing, Command, MutationEffectKind};

    use super::{
        humanize_kv_bytes, is_miss, render_mutation_ack, render_output, Format, Invocation,
        MutationAck, Rendered,
    };

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
        assert!(invocation.ack.is_some());

        let invocation = Invocation::for_wire("json_drop_index", Format::Human, || {
            Ok(json!({"name": "x"}))
        })
        .expect("json_drop_index is declared");
        assert_eq!(
            invocation.ack.and_then(|ack| ack.request),
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
                invocation.ack.is_none(),
                "{format:?} is the record, not a receipt"
            );
        }
        assert!(Invocation::of(&command, Format::Human)
            .expect("kv_put is declared")
            .ack
            .is_some());
        assert!(Invocation::of(&command, Format::Raw)
            .expect("kv_put is declared")
            .ack
            .is_some());
        let ping = Invocation::of(&Command::Ping {}, Format::Human).expect("ping is bespoke");
        assert!(
            ping.ack.is_none(),
            "a bespoke command renders through its family arm"
        );
        let unknown = Invocation::for_wire("no_such_wire", Format::Human, || Ok(json!({})))
            .expect("an unknown wire renders through the family path");
        assert!(unknown.ack.is_none());
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
            only_stdout("3 items, 1 row, 12 bytes\n")
        );
        assert_eq!(
            render_mutation_ack(&doc(0, 1_600_000), &ack, Format::Human),
            only_stdout("3 items, 0 rows, 2 MB\n")
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
            999,
            1_000_000,
            1_600_000,
            999_999_999,
            1_000_000_000,
            4_700_000_000,
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
        let rendered =
            super::value_to_string(&value, super::Format::Human).expect("describe renders");
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
        let rendered =
            super::value_to_string(&value, super::Format::Human).expect("describe renders");
        assert!(
            !rendered.contains("vector collections") && !rendered.contains("graphs:"),
            "empty inventories stay silent: {rendered}"
        );
    }

    #[test]
    fn page_tail_tells_how_to_fetch_the_next_page() {
        let value = serde_json::json!({"type": "keys_page", "data": {
            "items": ["alpha"], "has_more": true, "cursor": "b64token"
        }});
        let rendered = super::value_to_string(&value, super::Format::Human).expect("page renders");
        assert!(
            rendered.contains("-- more: add --cursor b64token to the same command"),
            "the continuation must be actionable: {rendered}"
        );
    }

    const RENDERED_TAGS: &[&str] = &[
        "bool",
        "described",
        "event_count",
        "event_record",
        "graph_edge_result",
        "graph_node_result",
        "inference_embeddings",
        "inference_generation",
        "inference_model_pulled",
        "inference_models",
        "inference_ranking",
        "inference_status",
        "inference_text",
        "inference_token_ids",
        "inference_unload_result",
        "json_value",
        "json_version_history",
        "json_versioned_value",
        "kv_versioned_value",
        "pong",
        "uint",
        "vector_data",
        "vector_matches",
    ];

    /// Extracts the string literals that head a `match` arm (`"tag" =>` or
    /// `"tag" | "tag2" =>`) from the render source — the tags the renderers
    /// special-case. Format-string arguments (`line!(out, "...")`) never sit
    /// before `=>`/`|`, so they are not mistaken for tags.
    fn dispatch_tags() -> std::collections::BTreeSet<String> {
        let full = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/render.rs"))
            .expect("read render.rs source");
        // Scan only the production code; the test module below contains `"tag"`
        // examples in its comments that are not real dispatch arms.
        let source = full.split("#[cfg(test)]").next().unwrap_or(&full);
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
                && (rest.starts_with("=>") || rest.starts_with('|'));
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

    #[test]
    fn kv_value_decodes_to_text_for_human_output() {
        let output =
            Output::KvVersionedValue(Maybe::found(VersionedValue::new(bytes("one"), 1, 10)));
        let mut value = serde_json::to_value(&output).expect("output serializes");
        assert_eq!(value["data"]["value"]["value"], json!("b25l"));
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(value["data"]["value"]["value"], json!("one"));
    }

    #[test]
    fn key_items_decode_but_the_continuation_cursor_stays_base64() {
        let output = Output::KeysPage {
            items: vec![bytes("a")],
            page: PageInfo::new(true, Some(bytes("b"))),
        };
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(value["data"]["items"][0], json!("a"));
        assert_eq!(value["data"]["cursor"], json!("Yg=="));
    }

    #[test]
    fn scan_items_decode_key_and_value() {
        let output = Output::KvScanResult {
            items: vec![ScanItem::new(bytes("a"), bytes("one"), 1, 10)],
            page: PageInfo::terminal(),
        };
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(value["data"]["items"][0]["key"], json!("a"));
        assert_eq!(value["data"]["items"][0]["value"], json!("one"));
    }

    #[test]
    fn non_utf8_bytes_keep_their_base64_form() {
        let output = Output::KvVersionedValue(Maybe::found(VersionedValue::new(
            Bytes::new(vec![0xff, 0xfe]),
            1,
            10,
        )));
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(value["data"]["value"]["value"], json!("//4="));
    }

    // --- #3061: branch diff/merge/preview identities and values decode ---

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

    #[test]
    fn branch_diff_identities_decode_for_human_output() {
        // Every change group (added/removed/modified) decodes, not just one.
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
        let mut value = serde_json::to_value(&output).expect("output serializes");
        assert_eq!(
            value["data"]["spaces"][0]["modified"][0]["identity"],
            json!("bWV0YTpzdXJ2aXZhbF9yYXRl")
        );
        humanize_kv_bytes(&output, &mut value);
        let space = &value["data"]["spaces"][0];
        assert_eq!(space["added"][0]["identity"], json!("added:key"));
        assert_eq!(space["removed"][0]["identity"], json!("removed:key"));
        assert_eq!(
            space["modified"][0]["identity"],
            json!("meta:survival_rate")
        );
    }

    #[test]
    fn branch_diff_non_utf8_identity_keeps_base64() {
        // Direction control: a non-UTF-8 identity falls back to base64.
        let output = kv_comparison(vec![ComparedEntityItem::new(
            Bytes::new(vec![0xff, 0xfe]),
            1,
        )]);
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(
            value["data"]["spaces"][0]["modified"][0]["identity"],
            json!("//4=")
        );
    }

    #[test]
    fn branch_merge_identities_and_values_decode() {
        // applied (identity + value), deleted (identity, no value), and any
        // conflicts (identity + both sides) all decode.
        let applied = PromotedEntityItem::new(
            ComparedCapability::Kv,
            "default".to_owned(),
            bytes("meta:survival_rate"),
            Some(bytes("0.62")),
        );
        let deleted = PromotedEntityItem::new(
            ComparedCapability::Kv,
            "default".to_owned(),
            bytes("meta:stale_key"),
            None,
        );
        let conflict = PreviewConflictItem::new(
            ComparedCapability::Kv,
            "default".to_owned(),
            bytes("meta:disputed"),
            Some(bytes("mine")),
            Some(bytes("theirs")),
            ConflictKind::ValueDivergence,
            ConflictStrategyResult::SourceWins,
        );
        let output = Output::BranchMerge(PromotionOutcomeItem::new(
            "cleaned".to_owned(),
            "default".to_owned(),
            10,
            PromotionStrategy::SourceWins,
            Some(44),
            Some(94),
            vec![applied],
            vec![deleted],
            vec![conflict],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        let data = &value["data"];
        assert_eq!(data["applied"][0]["identity"], json!("meta:survival_rate"));
        assert_eq!(data["applied"][0]["value"], json!("0.62"));
        assert_eq!(data["deleted"][0]["identity"], json!("meta:stale_key"));
        assert_eq!(data["conflicts"][0]["identity"], json!("meta:disputed"));
        assert_eq!(data["conflicts"][0]["source_value"], json!("mine"));
        assert_eq!(data["conflicts"][0]["target_value"], json!("theirs"));
    }

    #[test]
    fn branch_preview_conflict_identities_and_values_decode() {
        let conflict = PreviewConflictItem::new(
            ComparedCapability::Kv,
            "default".to_owned(),
            bytes("meta:survival_rate"),
            Some(bytes("0.62")),
            Some(bytes("0.5")),
            ConflictKind::ValueDivergence,
            ConflictStrategyResult::SourceWins,
        );
        let output = Output::BranchPreview(BranchPreviewItem::new(
            "cleaned".to_owned(),
            "default".to_owned(),
            10,
            PromotionStrategy::SourceWins,
            vec![conflict],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        let rendered = &value["data"]["conflicts"][0];
        assert_eq!(rendered["identity"], json!("meta:survival_rate"));
        assert_eq!(rendered["source_value"], json!("0.62"));
        assert_eq!(rendered["target_value"], json!("0.5"));
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

    #[test]
    fn missing_reads_are_untouched() {
        let output = Output::KvVersionedValue(Maybe::missing());
        let mut value = serde_json::to_value(&output).expect("output serializes");
        humanize_kv_bytes(&output, &mut value);
        assert_eq!(value["data"], json!({ "found": false, "value": null }));
    }

    #[test]
    fn json_leaf_unwraps_the_versioned_envelope() {
        let data = json!({
            "found": true,
            "value": {"value": {"name": "Ada"}, "version": 3, "timestamp": 30, "document_version": 1}
        });
        assert_eq!(
            super::json_leaf("json_versioned_value", &data, true),
            Some(&json!({"name": "Ada"}))
        );
    }

    #[test]
    fn json_leaf_reads_plain_values_directly_and_respects_found() {
        let data = json!({"found": true, "value": null});
        assert_eq!(
            super::json_leaf("json_value", &data, true),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(super::json_leaf("json_value", &data, false), None);
    }

    fn human(value: &serde_json::Value) -> String {
        let mut out = String::new();
        super::render_human(value, &mut out).expect("render_human");
        out
    }

    fn raw(value: &serde_json::Value) -> String {
        let mut out = String::new();
        super::render_raw(value, &mut out);
        out
    }

    #[test]
    fn human_event_count_prints_bare_number() {
        let value = json!({ "type": "event_count", "data": { "count": 5 } });
        assert_eq!(human(&value), "5\n");
    }

    #[test]
    fn raw_event_count_prints_bare_number() {
        let value = json!({ "type": "event_count", "data": { "count": 5 } });
        assert_eq!(raw(&value), "5\n");
    }

    #[test]
    fn human_bool_and_uint_scalars() {
        assert_eq!(human(&json!({ "type": "bool", "data": true })), "true\n");
        assert_eq!(human(&json!({ "type": "uint", "data": 42 })), "42\n");
    }

    #[test]
    fn human_kv_versioned_value_found_and_missing() {
        let found = json!({
            "type": "kv_versioned_value",
            "data": { "found": true, "value": { "value": "hello", "version": 1, "timestamp": 10 } }
        });
        assert_eq!(human(&found), "hello\n");
        let missing = json!({
            "type": "kv_versioned_value",
            "data": { "found": false, "value": null }
        });
        assert_eq!(human(&missing), "(nil)\n");
    }

    #[test]
    fn raw_kv_versioned_value_found() {
        let found = json!({
            "type": "kv_versioned_value",
            "data": { "found": true, "value": { "value": "hello", "version": 1, "timestamp": 10 } }
        });
        assert_eq!(raw(&found), "hello\n");
    }

    #[test]
    fn raw_json_get_distinguishes_a_present_null_from_a_miss() {
        // #3064: a present JSON `null` must print the literal `null`, distinct
        // from a miss (which emits nothing), so a `--raw` script can tell a
        // field that is null from one that is absent.
        let present_null = json!({
            "type": "json_value",
            "data": { "found": true, "value": null }
        });
        assert_eq!(raw(&present_null), "null\n");

        let missing = json!({
            "type": "json_value",
            "data": { "found": false, "value": null }
        });
        assert_eq!(raw(&missing), "");

        // A present non-null leaf stays unquoted/script-friendly.
        let present_value = json!({
            "type": "json_value",
            "data": { "found": true, "value": 1 }
        });
        assert_eq!(raw(&present_value), "1\n");

        // The versioned envelope's inner null is treated the same.
        let versioned_null = json!({
            "type": "json_versioned_value",
            "data": { "found": true, "value": {
                "value": null, "version": 3, "timestamp": 30, "document_version": 1
            } }
        });
        assert_eq!(raw(&versioned_null), "null\n");
    }

    #[test]
    fn human_json_value_and_versioned_value() {
        let plain = json!({
            "type": "json_value",
            "data": { "found": true, "value": { "name": "Ada" } }
        });
        assert_eq!(human(&plain), "{\"name\":\"Ada\"}\n");
        let versioned = json!({
            "type": "json_versioned_value",
            "data": { "found": true, "value": {
                "value": { "name": "Ada" }, "version": 3, "timestamp": 30, "document_version": 1
            } }
        });
        assert_eq!(human(&versioned), "{\"name\":\"Ada\"}\n");
    }

    #[test]
    fn human_json_version_history_items_and_nil() {
        let items = json!({
            "type": "json_version_history",
            "data": [{ "version": 1 }, { "version": 2 }]
        });
        assert_eq!(human(&items), "{\"version\":1}\n{\"version\":2}\n");
        let nil = json!({ "type": "json_version_history", "data": null });
        assert_eq!(human(&nil), "(nil)\n");
    }

    #[test]
    fn human_vector_matches_some_and_empty() {
        let some = json!({
            "type": "vector_matches",
            "data": [{ "key": "a", "score": 0.5 }]
        });
        assert_eq!(human(&some), "a\t0.5\n");
        let empty = json!({ "type": "vector_matches", "data": [] });
        assert_eq!(human(&empty), "(empty)\n");
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
        assert_eq!(row(999_999), "m\tchat\tllama\tq4\tready\t999999 bytes\n");
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
    fn human_data_generic_items_empty_and_nil() {
        let items = json!({ "items": [1, 2] });
        assert_eq!(human(&items), "1\n2\n");
        let empty = json!({ "items": [] });
        assert_eq!(human(&empty), "(empty)\n");
        assert_eq!(human(&serde_json::Value::Null), "(nil)\n");
    }
}
