# Provider API keys

Strata is an **embedded** database — it runs inside your process, like SQLite or
DuckDB, and ships **no API keys of its own**. To use a cloud inference provider
(OpenAI, Anthropic, or Google) you bring your own key. Local GGUF models need no
key. The default binary runs cloud models only: `strata inference install-local`
swaps in the local-capable build, `strata inference status` reports what your
build can run and how many models are downloaded, and
`strata inference models local` lists them. Inference commands work on models,
not on a database's data, so they need no database: run them from any
directory (a `--db` or `STRATA_DB` target is honored when given).

## Get a key

| Provider | Get a key | Environment variable |
|---|---|---|
| OpenAI | <https://platform.openai.com/api-keys> | `OPENAI_API_KEY` |
| Anthropic | <https://console.anthropic.com/settings/keys> | `ANTHROPIC_API_KEY` |
| Google (Gemini) | <https://aistudio.google.com/apikey> | `GOOGLE_API_KEY` |

Each provider bills your account for usage. Keep the key secret — anyone with it
can spend against your account.

## Set a key

There are two ways. **The environment variable always wins over the config
file**, so you can pin a key globally and override it per-shell or in CI.

### 1. Environment variable (recommended for CI / 12-factor)

```bash
export OPENAI_API_KEY=sk-...
strata inference generate openai:gpt-4o-mini "Hello"
```

### 2. Stored in the Strata config (persists across sessions)

```bash
strata config set openai.api_key sk-...
strata config set anthropic.api_key sk-ant-...
strata config set google.api_key ...
```

This writes the key to the **global** Strata config file
(`<config-dir>/strata/config.toml`, e.g. `~/.config/strata/config.toml` on
Linux) with `0600` permissions. Settable keys are `openai.api_key`,
`anthropic.api_key`, and `google.api_key` (plus `hub.url`).

Check or remove a stored key (values are printed **redacted**, never in full):

```bash
strata config get-key anthropic.api_key   # {"key":"anthropic.api_key","set":true,"value":"sk-ant-****"}
strata config unset anthropic.api_key
strata config path                         # where the config file lives
```

## Resolution order

For each provider, Strata resolves the key as:

1. the environment variable (`OPENAI_API_KEY`, …), then
2. the stored config value (`<provider>.api_key`).

The first one set wins. If neither is set, a cloud request fails with
`failed_precondition.inference.missing_api_key` and an error that names the
variable and links where to get a key.

The same lookup runs wherever a key is needed — the CLI, the SDK, and every
embedded use — because it is part of the inference runtime a database opens
with, not something the CLI does before a command runs. `strata inference
status` reports it as `key_source`: the variable name for an exported key, the
config file's path for a stored one, and `strata doctor` reads the same
runtime.

## Security notes

- The config file is written `0600` (owner read/write only) and keys are
  redacted whenever Strata prints them.
- Keys are stored in the **global** config only — never in a per-project
  `.strata/config.toml` — so they cannot be accidentally committed to a repo.
- Prefer environment variables (or a secrets manager) in CI and shared
  environments; the stored-config option is a convenience for local use.

## Which commands need a key

Any cloud request: `strata inference generate <provider>:<model> …`,
`strata inference embed`, and the SDK equivalents. `strata inference capability
<provider>:<model>` reports `requires_api_key` and the supported features
without making a network call.
