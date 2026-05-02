use strata_executor::{Error, Output, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenderMode {
    Human,
    Json,
    Raw,
}

pub(crate) fn render_output(output: &Output, mode: RenderMode) -> String {
    match mode {
        RenderMode::Json => render_json(output),
        RenderMode::Raw => render_raw(output),
        RenderMode::Human => render_human(output),
    }
}

pub(crate) fn render_error(error: &Error, mode: RenderMode) -> String {
    match mode {
        RenderMode::Json => {
            serde_json::to_string_pretty(error).expect("executor errors must serialize to JSON")
        }
        RenderMode::Raw | RenderMode::Human => error.to_string(),
    }
}

fn render_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_string())
}

fn render_raw(output: &Output) -> String {
    match output {
        Output::Unit => String::new(),
        Output::Bool(value) => {
            if *value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Output::Uint(value) | Output::Version(value) => value.to_string(),
        Output::Text(text) => text.clone(),
        Output::Maybe(None) | Output::MaybeVersioned(None) => String::new(),
        Output::Maybe(Some(value)) => render_raw_value(value),
        Output::MaybeVersioned(Some(versioned)) => render_raw_value(&versioned.value),
        Output::Keys(keys) | Output::SpaceList(keys) => keys.join("\n"),
        Output::VectorMatches(matches) => matches
            .iter()
            .map(|hit| format!("{}\t{}", hit.key, hit.score))
            .collect::<Vec<_>>()
            .join("\n"),
        Output::Embedding(vector) => serde_json::to_string(vector).unwrap_or_default(),
        Output::Embeddings(vectors) => serde_json::to_string(vectors).unwrap_or_default(),
        Output::TokenIds(result) => result
            .ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" "),
        _ => serde_json::to_string(output).unwrap_or_default(),
    }
}

fn render_human(output: &Output) -> String {
    match output {
        Output::Unit => "OK".to_string(),
        Output::Bool(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Output::Uint(value) | Output::Version(value) => value.to_string(),
        Output::Text(text) => text.clone(),
        Output::Maybe(None) | Output::MaybeVersioned(None) => "(nil)".to_string(),
        Output::Maybe(Some(value)) => render_human_value(value),
        Output::MaybeVersioned(Some(versioned)) => render_human_value(&versioned.value),
        Output::Keys(keys) | Output::SpaceList(keys) => keys.join("\n"),
        Output::TxnBegun => "OK".to_string(),
        Output::TxnCommitted { version } => format!("committed v{version}"),
        Output::TxnAborted => "rolled back".to_string(),
        _ => render_json(output),
    }
}

fn render_human_value(value: &Value) -> String {
    match value {
        Value::Null => "(nil)".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(text) => text.clone(),
        _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn render_raw_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(text) => text.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{render_error, render_output, RenderMode};
    use strata_executor::{Error, Output, Value};

    #[test]
    fn human_render_handles_core_success_shapes() {
        assert_eq!(render_output(&Output::Unit, RenderMode::Human), "OK");
        assert_eq!(
            render_output(&Output::Bool(true), RenderMode::Human),
            "true"
        );
        assert_eq!(
            render_output(&Output::Maybe(None), RenderMode::Human),
            "(nil)"
        );
        assert_eq!(
            render_output(
                &Output::Maybe(Some(Value::String("hello".into()))),
                RenderMode::Human
            ),
            "hello"
        );
        assert_eq!(
            render_output(&Output::TxnCommitted { version: 9 }, RenderMode::Human),
            "committed v9"
        );
    }

    #[test]
    fn raw_render_handles_core_success_shapes() {
        assert_eq!(render_output(&Output::Bool(true), RenderMode::Raw), "1");
        assert_eq!(render_output(&Output::Bool(false), RenderMode::Raw), "0");
        assert_eq!(render_output(&Output::Uint(42), RenderMode::Raw), "42");
        assert_eq!(
            render_output(&Output::Keys(vec!["a".into(), "b".into()]), RenderMode::Raw),
            "a\nb"
        );
        assert_eq!(
            render_output(&Output::Maybe(Some(Value::Int(7))), RenderMode::Raw),
            "7"
        );
    }

    #[test]
    fn json_render_uses_structured_encoding() {
        let rendered = render_output(&Output::Bool(true), RenderMode::Json);
        assert!(rendered.contains("\"Bool\""));
        assert!(rendered.contains("true"));
    }

    #[test]
    fn error_rendering_is_structured_in_json_mode() {
        let error = Error::InvalidInput {
            reason: "bad input".to_string(),
            hint: Some("fix it".to_string()),
        };

        assert_eq!(
            render_error(&error, RenderMode::Human),
            "invalid input: bad input. fix it"
        );

        let rendered = render_error(&error, RenderMode::Json);
        assert!(rendered.contains("\"InvalidInput\""));
        assert!(rendered.contains("bad input"));
    }
}
