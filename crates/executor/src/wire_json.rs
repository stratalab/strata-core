//! Wire-command JSON ingress guard.
//!
//! `serde_json` parses a JSON integer outside `[i64::MIN, u64::MAX]` into a lossy
//! `f64`, and the loss is irreversible: once it is an `f64`, `2^64` is
//! indistinguishable from a genuine `1.8e19`. The coercion also happens during
//! the `Content` buffering that `#[serde(tag = "type")]` performs on [`Command`],
//! *before* any field-level hook runs — so it cannot be caught while
//! deserializing a command. The only place the raw digits still exist is the
//! wire text itself.
//!
//! [`guard_json_integers`] scans that raw text before `from_str::<Command>` and
//! refuses any integer literal that would be coerced. Because it scans the whole
//! command, one call covers every user-JSON field in every command (a `json.set`
//! value, an `event.append` payload, vector metadata, …); a `> u64` integer
//! cannot appear legitimately in a typed field, which would fail to deserialize
//! on its own. Bindings that build a [`Command`] from a JSON string
//! (`crates/wasm`, the Python SDK bridge) call this first.
//!
//! [`Command`]: crate::command::Command

use crate::error::ExecutorError;

/// Rejects a JSON text — a wire command, or a user-supplied JSON argument — that
/// contains an integer literal outside `[i64::MIN, u64::MAX]` (which `serde_json`
/// would silently coerce to a lossy `f64`) with
/// `invalid_argument.executor.json_number`. Wire bindings call this before
/// `serde_json::from_str::<Command>`; the CLI calls it on a JSON argument before
/// parsing it to a `Value`.
pub fn guard_json_integers(json: &str) -> Result<(), ExecutorError> {
    if has_unrepresentable_integer(json) {
        return Err(ExecutorError::new(
            "invalid_argument.executor.json_number",
            "a JSON integer is outside the supported i64/u64 range",
        ));
    }
    Ok(())
}

/// Returns true if `text` contains an integer literal that fits neither `i64`
/// nor `u64` — the case `serde_json` coerces to a lossy `f64`. Numbers inside
/// strings are ignored, and float literals (any with `.`, `e`, or `E`) are
/// representable and ignored.
fn has_unrepresentable_integer(text: &str) -> bool {
    let mut bytes = text.bytes().peekable();
    let mut in_string = false;
    while let Some(byte) = bytes.next() {
        if in_string {
            match byte {
                // Skip an escaped character (e.g. `\"`) so its bytes are not
                // mistaken for structure or a number.
                b'\\' => {
                    bytes.next();
                }
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'-' | b'0'..=b'9' => {
                let mut token = String::from(byte as char);
                let mut is_integer = true;
                while let Some(&next) = bytes.peek() {
                    match next {
                        b'0'..=b'9' | b'+' | b'-' => {}
                        b'.' | b'e' | b'E' => is_integer = false,
                        _ => break,
                    }
                    token.push(next as char);
                    bytes.next();
                }
                if is_integer && token.parse::<i64>().is_err() && token.parse::<u64>().is_err() {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{guard_json_integers, has_unrepresentable_integer};
    use crate::ExecutorErrorClass;

    #[test]
    fn representable_numbers_are_accepted() {
        for text in [
            "18446744073709551615", // u64::MAX
            "9223372036854775808",  // 2^63
            "-9223372036854775808", // i64::MIN
            "42",
            "-1",
            "1.5",
            "1e40",                   // a large float, representable
            "1.8446744073709552e19",  // a genuine float
            "18446744073709551616.5", // a float whose integer part exceeds u64
            "18446744073709551616e2", // ditto, exponent form
            r#"{"a": 1, "b": [2, 3], "c": -4}"#,
            r#"{"18446744073709551616": "a key, not a number"}"#,
            r#"{"note": "1e999 inside a string"}"#,
            // An escaped quote must not end string tracking early and expose the
            // big number inside as a bare literal.
            r#"{"msg": "he said \"18446744073709551616\" loudly", "ok": 7}"#,
        ] {
            assert!(
                !has_unrepresentable_integer(text),
                "wrongly rejected: {text}"
            );
        }
    }

    #[test]
    fn integers_beyond_i64_u64_are_flagged() {
        for text in [
            "18446744073709551616", // u64::MAX + 1
            "18446744073709551617",
            "-9223372036854775809", // i64::MIN - 1
            "100000000000000000000",
            r#"{"deep": {"nested": [1, 18446744073709551616]}}"#,
            r#"{"n": -99999999999999999999}"#,
        ] {
            assert!(has_unrepresentable_integer(text), "missed: {text}");
        }
    }

    #[test]
    fn guard_reports_the_stable_code_for_an_oversized_integer() {
        let error = guard_json_integers(
            r#"{"type":"json_set","key":"k","path":"$","value":{"i":18446744073709551616}}"#,
        )
        .expect_err("oversized integer rejected");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.executor.json_number");
        guard_json_integers(r#"{"type":"kv_get","key":"k"}"#).expect("ordinary command passes");
    }
}
