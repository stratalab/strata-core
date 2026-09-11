//! Browser boundary for Strata.
//!
//! A thin `wasm-bindgen` adapter over the executor's serialized command
//! boundary — the same wire the CLI, MCP server, and SDK bindings speak.
//! Commands go in as JSON strings, envelopes come out as JSON strings:
//! `{"type": ..., "data": ...}` on success, `{"error": {...}}` on domain
//! failure. This crate adds no semantics of its own (the executor owns
//! command behavior; frontends stay transport-thin).
//!
//! Only cache mode exists in the browser: `wasm32-unknown-unknown` has no
//! filesystem, and cache mode is non-durable by design — no WAL, manifest,
//! snapshot, or lock objects — so the whole database lives and dies with the
//! page. That makes the browser a faithful demo of the volatile session
//! surface, not a persistence story.

use strata_executor::{guard_json_integers, Command, Executor};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsError;

/// A volatile in-memory Strata database session.
///
/// Wraps one cache-mode executor handle. Dropping the session (or letting the
/// page unload) discards the database.
#[wasm_bindgen]
pub struct StrataSession {
    executor: Executor,
}

#[wasm_bindgen]
impl StrataSession {
    /// Opens a fresh cache-mode database.
    ///
    /// # Errors
    ///
    /// Throws if the engine fails to open a volatile database (out of
    /// memory is the realistic case).
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<StrataSession, JsError> {
        console_error_panic_hook::set_once();
        let executor = Executor::open_cache()
            .map_err(|error| JsError::new(&format!("failed to open cache database: {error}")))?;
        Ok(Self { executor })
    }

    /// Executes one serialized command and returns its JSON envelope.
    ///
    /// The input is a command object on the executor wire, for example
    /// `{"type":"kv_put","key":"greeting","value":"aGVsbG8="}` (KV bytes are
    /// base64). Success returns `{"type": ..., "data": ...}`; a domain
    /// failure returns `{"error": {class, code, message, suggested_fix,
    /// docs_url, ...}}` — the same envelope every other Strata frontend
    /// emits.
    ///
    /// # Errors
    ///
    /// Throws only when `command_json` is not a valid command object — the
    /// executor's own deserializer names the offending field and the valid
    /// set, so the thrown message is the teaching. Executed commands never
    /// throw; their failures are `{"error": ...}` envelopes.
    pub fn execute(&mut self, command_json: &str) -> Result<String, JsError> {
        // serde_json coerces an out-of-range JSON integer to a lossy f64 during
        // deserialization; reject it on the raw text first (see
        // `strata_executor::guard_json_integers`).
        guard_json_integers(command_json)
            .map_err(|error| JsError::new(&format!("invalid command: {error}")))?;
        let command: Command = serde_json::from_str(command_json)
            .map_err(|error| JsError::new(&format!("invalid command: {error}")))?;
        let envelope = match self.executor.execute(command) {
            Ok(output) => serde_json::to_value(&output),
            Err(error) => serde_json::to_value(serde_json::json!({ "error": error })),
        };
        envelope
            .and_then(|value| serde_json::to_string(&value))
            .map_err(|error| JsError::new(&format!("envelope serialization failed: {error}")))
    }

    /// Runs one `strata` CLI line and returns what the installed binary would
    /// have printed for it — stdout, then stderr, as one string.
    ///
    /// The input is a command line exactly as typed at the `strata` prompt —
    /// `kv put greeting hello`, `--json kv get greeting`, … — parsed by the
    /// same clap grammar the installed binary uses, run against this session's
    /// in-memory database, and rendered in the format the line's flags chose
    /// (`--json`, `--raw`, or the human default). A parse error, `--help`, or
    /// a host-only command (`mcp`, `init`, `clone`, …) returns its message as
    /// the string to display.
    ///
    /// # Errors
    ///
    /// Throws only on an internal rendering failure; command failures and usage
    /// errors come back as the returned display string, not as thrown errors.
    #[wasm_bindgen(js_name = executeCli)]
    pub fn execute_cli(&mut self, line: &str) -> Result<String, JsError> {
        strata_cli::run_line(&mut self.executor, line)
            .map_err(|error| JsError::new(&format!("render failed: {error}")))
    }

    /// Returns the session's default branch (used when commands omit theirs).
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn branch(&self) -> String {
        self.executor.default_branch().to_owned()
    }

    /// Sets the session's default branch.
    ///
    /// # Errors
    ///
    /// Throws when the branch name is invalid; the message carries the
    /// executor's validation error.
    #[wasm_bindgen(js_name = setBranch)]
    pub fn set_branch(&mut self, branch: &str) -> Result<(), JsError> {
        self.executor
            .set_default_branch(branch)
            .map_err(|error| JsError::new(&format!("invalid branch: {error}")))
    }

    /// Returns the session's default product space.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn space(&self) -> String {
        self.executor.default_space().to_owned()
    }

    /// Sets the session's default product space.
    ///
    /// # Errors
    ///
    /// Throws when the space name is invalid; the message carries the
    /// executor's validation error.
    #[wasm_bindgen(js_name = setSpace)]
    pub fn set_space(&mut self, space: &str) -> Result<(), JsError> {
        self.executor
            .set_default_space(space)
            .map_err(|error| JsError::new(&format!("invalid space: {error}")))
    }

    /// Closes the underlying database handle.
    ///
    /// The session is unusable afterwards; a new [`StrataSession`] opens a
    /// fresh empty database.
    ///
    /// # Errors
    ///
    /// Throws when the engine reports a close failure.
    pub fn close(&mut self) -> Result<(), JsError> {
        self.executor
            .close()
            .map_err(|error| JsError::new(&format!("close failed: {error}")))
    }
}

/// Reports the crate version compiled into the wasm module.
#[wasm_bindgen(js_name = engineVersion)]
#[must_use]
pub fn engine_version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
}
