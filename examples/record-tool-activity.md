# Record tool activity

Keep an ordered, tamper-evident log of what an agent did.

Mirrors [Record tool activity](https://stratadb.org/docs/guides/agents/record-tool-activity).

## Append as it happens

```console
$ strata ./agent event append run.start '{"goal":"fix the bug"}'
appended run.start #0
$ strata ./agent event append tool.call '{"tool":"read","path":"app.py"}'
appended tool.call #1
$ strata ./agent event append tool.call '{"tool":"edit","path":"app.py"}'
appended tool.call #2
$ strata ./agent event append run.end '{"status":"ok"}'
appended run.end #3
```

The number is the sequence. It starts at zero and increases by one, so the log
is ordered by construction rather than by a timestamp you have to trust.

## Read it back

```console
$ strata ./agent event list
…PAYLOAD
…{"goal":"fix the bug"}
…{"path":"app.py","tool":"read"}
…{"path":"app.py","tool":"edit"}
…{"status":"ok"}
```

Narrow it by type when the log gets long:

```console
$ strata ./agent event list --event-type tool.call
…PAYLOAD
…{"path":"app.py","tool":"read"}
…{"path":"app.py","tool":"edit"}
```

## Prove nothing was altered

Each entry carries a hash of itself and the one before it, so the log can be
checked rather than trusted:

```console
$ strata ./agent event verify-chain
valid          true
length         4
first_invalid  -
error          -
```
