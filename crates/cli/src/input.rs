//! CLI input helpers.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use serde_json::Value;
use strata_executor::{Bytes, VectorMetadataFilter};

use crate::CliError;

pub(crate) fn bytes_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
) -> Result<Bytes, CliError> {
    Ok(Bytes::new(read_bytes_argument(value, file)?))
}

/// Decodes a `--cursor` continuation token.
///
/// KV cursors are opaque base64 tokens — the exact string printed by the
/// previous page (and the `Bytes` wire encoding). Decoding here restores the
/// underlying key bytes, so continuation works for non-UTF-8 keys too.
pub(crate) fn cursor_argument(value: &str) -> Result<Bytes, CliError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map(Bytes::new)
        .map_err(|error| {
            CliError::usage(format!(
                "invalid --cursor `{value}`: expected the base64 token printed by the previous page ({error})"
            ))
        })
}

pub(crate) fn text_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
    label: &str,
) -> Result<String, CliError> {
    let bytes = read_bytes_argument(value, file)?;
    String::from_utf8(bytes).map_err(|error| {
        CliError::usage(format!(
            "{label} must be valid UTF-8 when read as text: {error}"
        ))
    })
}

pub(crate) fn parse_relaxed_json_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
    label: &str,
) -> Result<Value, CliError> {
    let text = text_argument(value, file, label)?;
    // Non-JSON text is stored as a plain string; but when the argument *is* JSON,
    // reject an integer serde_json would coerce to a lossy f64 rather than storing
    // the loss (a plain string that merely contains big digits still parses as a
    // string and is unaffected).
    match serde_json::from_str::<Value>(&text) {
        Ok(parsed) => {
            strata_executor::guard_json_integers(&text).map_err(CliError::from)?;
            Ok(parsed)
        }
        // Shaped like JSON and did not parse: a malformed document, not someone
        // meaning a string. Storing it as one loses the write in silence, which
        // is how `{"name":"Ada"}` typed at the REPL became the string
        // `{name:Ada}` — shlex ate the quotes — and a later `$.name` read nil
        // with no error anywhere (#2571).
        Err(error) if looks_like_json_structure(&text) => Err(CliError::usage(format!(
            "{label} looks like JSON but does not parse: {error}. Quote it to store it as \
             text, or fix the document — inside the REPL, wrap JSON in single quotes \
             (`json set doc $ '{{\"name\":\"Ada\"}}'`) so the shell-style tokenizer keeps \
             its double quotes."
        ))),
        Err(_) => Ok(Value::String(text)),
    }
}

/// Whether `text` is *shaped* like a JSON object, array or string — the forms a
/// caller writes on purpose rather than as incidental prose.
///
/// Deliberately structural rather than a parse attempt: the question is what
/// the caller MEANT, and a leading `{`, `[` or `"` answers it. Bare words,
/// numbers and sentences stay strings under the relaxed contract even when they
/// fail to parse.
fn looks_like_json_structure(text: &str) -> bool {
    matches!(
        text.trim_start().as_bytes().first(),
        Some(b'{' | b'[' | b'"')
    )
}

pub(crate) fn parse_json_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
    label: &str,
) -> Result<Value, CliError> {
    let text = text_argument(value, file, label)?;
    // Reject an integer serde_json would coerce to a lossy f64 before parsing.
    strata_executor::guard_json_integers(&text).map_err(CliError::from)?;
    serde_json::from_str(&text).map_err(CliError::from)
}

pub(crate) fn parse_optional_json_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
    label: &str,
) -> Result<Option<Value>, CliError> {
    if value.is_none() && file.is_none() {
        return Ok(None);
    }
    parse_json_argument(value, file, label).map(Some)
}

pub(crate) fn parse_vector_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
    label: &str,
) -> Result<Vec<f64>, CliError> {
    let text = text_argument(value, file, label)?;
    parse_vector_text(&text)
}

pub(crate) fn parse_filter_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
) -> Result<VectorMetadataFilter, CliError> {
    let text = text_argument(value, file, "vector metadata filter")?;
    serde_json::from_str(&text).map_err(CliError::from)
}

pub(crate) fn parse_optional_filter_argument(
    value: Option<&str>,
    file: Option<&PathBuf>,
) -> Result<Option<VectorMetadataFilter>, CliError> {
    if value.is_none() && file.is_none() {
        return Ok(None);
    }
    parse_filter_argument(value, file).map(Some)
}

pub(crate) fn read_text_file(path: &Path) -> Result<String, CliError> {
    fs::read_to_string(path).map_err(CliError::from)
}

fn read_bytes_argument(value: Option<&str>, file: Option<&PathBuf>) -> Result<Vec<u8>, CliError> {
    match (value, file) {
        (Some(_), Some(_)) => Err(CliError::usage(
            "provide either an inline value or --file, not both",
        )),
        (Some(value), None) => read_inline_or_file_shorthand(value),
        (None, Some(path)) => fs::read(path).map_err(CliError::from),
        (None, None) => Err(CliError::usage(
            "missing required value; pass a value or --file",
        )),
    }
}

fn read_inline_or_file_shorthand(value: &str) -> Result<Vec<u8>, CliError> {
    if value == "-" {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        return Ok(bytes);
    }
    if let Some(path) = value.strip_prefix('@') {
        if path.is_empty() {
            return Err(CliError::usage("@ file shorthand requires a path"));
        }
        return fs::read(path).map_err(CliError::from);
    }
    Ok(value.as_bytes().to_vec())
}

fn parse_vector_text(value: &str) -> Result<Vec<f64>, CliError> {
    if value.trim_start().starts_with('[') {
        return serde_json::from_str(value).map_err(CliError::from);
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<f64>().map_err(|error| {
                CliError::usage(format!("invalid vector element `{part}`: {error}"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_arguments_reject_integers_beyond_i64_u64_range() {
        // Strict JSON argument (json.patch, event payload, metadata): an
        // out-of-range integer is rejected rather than stored as a lossy f64.
        // (The executor guard test pins the stable error code.)
        assert!(parse_json_argument(Some(r#"{"i":18446744073709551616}"#), None, "v").is_err());
        // Representable extremes still parse.
        parse_json_argument(Some(r#"{"max":18446744073709551615}"#), None, "v")
            .expect("u64::MAX parses");

        // Relaxed argument (json.set value): rejects a JSON integer, but a plain
        // string that merely contains big digits is stored as a string untouched.
        assert!(parse_relaxed_json_argument(Some("18446744073709551616"), None, "v").is_err());
        assert_eq!(
            parse_relaxed_json_argument(Some("order 18446744073709551616 shipped"), None, "v")
                .expect("plain string stored"),
            Value::String("order 18446744073709551616 shipped".to_owned())
        );
    }

    /// `json set` accepts non-JSON text as a plain string — that is the point
    /// of the relaxed parse, so `json set doc $ hello` stores `"hello"`. But
    /// text that is *shaped* like JSON and fails to parse is not someone
    /// meaning a string; it is a malformed document, and storing it as a string
    /// loses the write silently.
    ///
    /// The REPL is where this bites. Its tokenizer is `shlex`, so typing
    /// `json set profile $ {"name":"Ada"}` hands the parser `{name:Ada}` —
    /// the shell quoting ate the JSON quoting — which parsed as nothing, stored
    /// as the string `{name:Ada}`, and made a later `json get profile $.name`
    /// return nil with no error anywhere (#2571).
    #[test]
    fn relaxed_json_refuses_text_shaped_like_json_that_does_not_parse() {
        // What the REPL actually hands over after shlex eats the quotes.
        let error = parse_relaxed_json_argument(Some("{name:Ada}"), None, "json value")
            .expect_err("a malformed object is not a string");
        let rendered = error.to_string();
        assert!(
            rendered.contains("quote"),
            "the remedy must mention quoting, which is how the REPL loses them: {rendered}"
        );

        for malformed in [
            "[1, 2",
            "{\"a\": }",
            "[\"a\", ]",
            "{a:1}",
            // A leading double quote is the third JSON-structural opener.
            "\"unterminated",
            // Leading whitespace must not hide the shape.
            "   {name:Ada}",
            "\t[1, 2",
        ] {
            assert!(
                parse_relaxed_json_argument(Some(malformed), None, "json value").is_err(),
                "{malformed:?} is shaped like JSON and does not parse"
            );
        }

        // Direction control: plain text is still a string, and well-formed JSON
        // is still parsed. The relaxed contract is intact for everything that
        // is not an unparseable structure.
        assert_eq!(
            parse_relaxed_json_argument(Some("hello"), None, "json value").expect("plain string"),
            Value::String("hello".to_owned())
        );
        assert_eq!(
            parse_relaxed_json_argument(Some("order 42 shipped"), None, "json value")
                .expect("plain string with digits"),
            Value::String("order 42 shipped".to_owned())
        );
        assert_eq!(
            parse_relaxed_json_argument(Some(r#"{"name":"Ada"}"#), None, "json value")
                .expect("well-formed object"),
            serde_json::json!({"name": "Ada"})
        );
        assert_eq!(
            parse_relaxed_json_argument(Some("[1,2,3]"), None, "json value")
                .expect("well-formed array"),
            serde_json::json!([1, 2, 3])
        );
    }

    #[test]
    fn parses_comma_vector() {
        assert_eq!(
            parse_vector_text("1, 2.5,3").expect("parse vector"),
            vec![1.0, 2.5, 3.0]
        );
    }

    #[test]
    fn parses_json_vector() {
        assert_eq!(
            parse_vector_text("[1,2,3]").expect("parse vector"),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn cursor_argument_decodes_the_printed_base64_token() {
        assert_eq!(
            cursor_argument("YQ==").expect("valid cursor"),
            Bytes::new(b"a".to_vec())
        );
    }

    #[test]
    fn cursor_argument_rejects_non_base64_input() {
        let error = cursor_argument("not base64!").expect_err("invalid cursor");
        assert!(matches!(error, CliError::Usage(_)));
    }
}
