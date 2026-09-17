# Examples

Runnable scenarios, one file each. Every `console` block in this directory is
executed against a real `strata` binary by `crates/cli/tests/examples.rs`, so an
example that lies fails the build rather than reaching a reader.

Each example mirrors a guide on [stratadb.org](https://stratadb.org/docs) and
says so in its header. That is deliberate: the site's guides are the teaching
material, and a second set written here would drift from them. These files are
the one copy — the site fetches them (stratalab/stratadb.org#21) rather than
keeping its own.

| example | guide |
|---|---|
| [`persist-agent-memory.md`](persist-agent-memory.md) | [Persist agent memory](https://stratadb.org/docs/guides/agents/persist-agent-memory) |
| [`read-historical-state.md`](read-historical-state.md) | [Read historical state](https://stratadb.org/docs/guides/branching/read-historical-state) |
| [`isolate-an-experiment.md`](isolate-an-experiment.md) | [Isolate an experiment](https://stratadb.org/docs/guides/branching/isolate-an-experiment) |
| [`record-tool-activity.md`](record-tool-activity.md) | [Record tool activity](https://stratadb.org/docs/guides/agents/record-tool-activity) |

## Running one yourself

Every example works on a database it creates itself, so copy the commands into
a scratch directory and they run as written:

```
cargo run -p strata-cli -- ./scratch kv put greeting hello
```

## Writing one

A `console` block is a transcript: `$ ` lines are commands, everything under
them is what the binary printed. The runner executes each command in order,
against one database per example, and compares.

Output that varies by run or by machine — a timestamp, a version, a path —
is written as `…`, which matches any text on that part of the line. Use it for
what genuinely varies and nothing else: an `…` over a value the example is
about is an example that checks nothing.

A fence that is not a transcript (a file's contents, a shape sketch) has no
`$ ` prompts and is left alone.

Refusals count as output. Errors print on stderr and are compared like anything
else, so an example that shows a failure is held to the real error code — which
is how the first draft of these caught itself inventing one.
