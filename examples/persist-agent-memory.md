# Persist agent memory

Give an agent memory that survives the process and outlives a single run.

Mirrors [Persist agent memory](https://stratadb.org/docs/guides/agents/persist-agent-memory).

## Memory is just data on `default`

There is no memory API to learn. The agent's memory is whatever it writes to a
durable database, and the choice that matters is which shape: key-value for
opaque values looked up by name, JSON when the shape matters and you want to
read or update one field.

```console
$ strata ./agent kv put memory.goal "ship the feature"
created memory.goal
$ strata ./agent json set memory.user '$' '{"name":"Ada","prefers":"terse"}'
created memory.user
```

One field, read and updated on its own:

```console
$ strata ./agent json set memory.user '$.prefers' '"verbose"'
updated memory.user
$ strata ./agent json get memory.user '$.prefers'
"verbose"
```

## Keep memory on `default`, work on branches

This is the pattern worth adopting early. Memory lives on `default`, where it
accumulates. A run forks, does its work in isolation, and merges back only what
it learned:

```console
$ strata ./agent branch fork default run-1
forked run-1 from default
$ strata ./agent --branch run-1 kv put memory.result "done"
created memory.result
$ strata ./agent branch merge run-1 default
merged run-1 into default: 1 applied, 0 deleted, 0 conflicts (version …)
```

A run that goes badly is deleted rather than unwound, and memory never saw it:

```console
$ strata ./agent branch fork default run-2
forked run-2 from default
$ strata ./agent --branch run-2 kv put memory.result "wrong turn"
updated memory.result
$ strata ./agent branch delete run-2
deleted branch run-2
$ strata ./agent kv get memory.result
done
```

## Nothing is ever overwritten

Updating memory does not lose the previous value. Every version is retained and
readable, so "what did it believe earlier" is a read rather than an archaeology
project:

```console
$ strata ./agent kv put memory.goal "ship the fix"
updated memory.goal
$ strata ./agent kv history memory.goal
…VALUE
…ship the fix
…ship the feature
```

That is also the cost: roughly 140 bytes per version for a small value, with no
command to prune. An agent that rewrites the same key in a tight loop will grow
the database. Write when something changes, not on every tick.
