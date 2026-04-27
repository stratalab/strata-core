#!/usr/bin/env python3
"""External installed-binary CLI suite for Strata.

This suite is intentionally external-facing:
- it does not import repo crates or test helpers
- it executes a real `strata` binary
- it validates shell mode, pipe mode, and REPL mode behavior

Typical usage:

    cargo install strata-cli
    python3 tests/cli_external_suite.py

Or against a local build:

    cargo build -p strata-cli
    python3 tests/cli_external_suite.py --strata-bin ./target/debug/strata

If you want to pin the expected command surface size:

    python3 tests/cli_external_suite.py --expected-leaf-count 114
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import pty
import re
import select
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import termios
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence


HELP_EXIT_CODES = {0, 2}
PROMPT_RE = re.compile(r"(strata:[^\r\n]*> )$")
ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
MANIFEST_PATH = pathlib.Path(__file__).with_name("cli_external_suite_manifest.json")
TIER_ORDER = {"smoke": 0, "standard": 1, "adversarial": 2, "full": 3}
SEARCH_TIMEOUT_SECS = 30.0
PANIC_NEEDLES = (
    "thread 'main' panicked",
    "panicked at",
    "stack backtrace",
    "fatal runtime error",
)


class SuiteFailure(RuntimeError):
    """Raised for a single test case failure."""


@dataclass
class CommandResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    trace_id: int

    @property
    def combined(self) -> str:
        if self.stdout and self.stderr:
            return f"{self.stdout}\n{self.stderr}"
        return self.stdout or self.stderr


@dataclass(frozen=True)
class SuiteCase:
    name: str
    fn: Callable[[], None]
    tier: str = "standard"


@dataclass
class CommandTrace:
    trace_id: int
    case: str
    command: list[str]
    stdin: str | None
    stdout: str
    stderr: str
    returncode: int
    check: str = "command exits successfully"


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise SuiteFailure(message)


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def parse_json_stream(text: str) -> list[Any]:
    text = text.strip()
    if not text:
        return []
    decoder = json.JSONDecoder()
    docs: list[Any] = []
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        doc, end = decoder.raw_decode(text, index)
        docs.append(doc)
        index = end
    return docs


def first_parsable_json_stream(*streams: str) -> list[Any]:
    last_error: Exception | None = None
    for stream in streams:
        if not stream.strip():
            continue
        try:
            docs = parse_json_stream(stream)
        except Exception as exc:  # pragma: no cover - exercised externally
            last_error = exc
            continue
        if docs:
            return docs
    if last_error is not None:
        raise SuiteFailure(f"Failed to parse JSON output: {last_error}")
    raise SuiteFailure("No JSON output found")


def as_dict(doc: Any, context: str) -> dict[str, Any]:
    ensure(isinstance(doc, dict), f"{context}: expected JSON object, got {type(doc).__name__}")
    return doc


def require_key(mapping: dict[str, Any], key: str, context: str) -> Any:
    ensure(key in mapping, f"{context}: missing {key}")
    return mapping[key]


def only_doc(docs: list[Any], context: str) -> Any:
    ensure(len(docs) == 1, f"{context}: expected exactly one JSON document, got {len(docs)}")
    return docs[0]


def internal_reason(doc: Any, context: str) -> str:
    internal = require_key(as_dict(doc, context), "Internal", context)
    ensure(isinstance(internal, dict), f"{context}: Internal payload should be an object")
    reason = require_key(internal, "reason", context)
    ensure(isinstance(reason, str), f"{context}: Internal.reason should be a string")
    return reason


def unwrap_maybe(doc: Any, context: str) -> dict[str, Any]:
    maybe = require_key(as_dict(doc, context), "Maybe", context)
    ensure(isinstance(maybe, dict), f"{context}: Maybe payload should be an object")
    return maybe


def unwrap_maybe_versioned(doc: Any, context: str) -> dict[str, Any]:
    maybe = require_key(as_dict(doc, context), "MaybeVersioned", context)
    ensure(isinstance(maybe, dict), f"{context}: MaybeVersioned payload should be an object")
    return maybe


def unwrap_value(value: Any, variant: str, context: str) -> Any:
    body = as_dict(value, context)
    ensure(variant in body, f"{context}: expected {variant} value, got {body}")
    return body[variant]


def wait_until_timestamp_passed(timestamp_micros: int) -> None:
    deadline = time.monotonic() + 0.1
    while time.time_ns() // 1_000 <= timestamp_micros:
        if time.monotonic() >= deadline:
            raise SuiteFailure(
                f"Timed out waiting for wall clock to pass timestamp {timestamp_micros}"
            )
        time.sleep(0.0005)


def command_lines(text: str) -> list[str]:
    lines = text.splitlines()
    in_commands = False
    commands: list[str] = []
    for line in lines:
        if line.startswith("Commands:"):
            in_commands = True
            continue
        if not in_commands:
            continue
        if line.startswith("Options:") or line.startswith("Arguments:"):
            break
        if not line.strip():
            continue
        match = re.match(r"\s{2,}([a-z0-9][a-z0-9-]*)\s{2,}", line)
        if match:
            command = match.group(1)
            if command != "help":
                commands.append(command)
        elif not line.startswith(" "):
            break
    return commands


def one_line(text: str | None, *, limit: int = 220) -> str:
    if not text:
        return "<empty>"
    compact = " ".join(text.split())
    if len(compact) > limit:
        return compact[: limit - 3] + "..."
    return compact


def compact_input(text: str | None, *, limit: int = 120) -> str:
    if not text:
        return "-"
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    compact = " ; ".join(lines)
    if len(compact) > limit:
        return compact[: limit - 3] + "..."
    return compact


class ReplSession:
    def __init__(self, binary: pathlib.Path, db_path: pathlib.Path):
        self.binary = binary
        self.db_path = db_path
        self.default_timeout = float(os.environ.get("STRATA_CLI_REPL_TIMEOUT_SECS", "10.0"))
        self.master_fd, slave_fd = pty.openpty()
        attrs = termios.tcgetattr(slave_fd)
        attrs[3] &= ~termios.ECHO
        termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
        self.proc = subprocess.Popen(
            [str(self.binary), "--db", str(self.db_path), "-j"],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
            text=False,
        )
        os.close(slave_fd)
        self.prompt = self._read_until_prompt(timeout=self.default_timeout)[1]

    def _read_until_prompt(self, timeout: float) -> tuple[str, str]:
        deadline = time.monotonic() + timeout
        buffer = ""
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise SuiteFailure(
                    f"REPL exited unexpectedly with code {self.proc.returncode}. Output so far:\n{buffer}"
                )
            remaining = max(0.0, deadline - time.monotonic())
            ready, _, _ = select.select([self.master_fd], [], [], min(0.25, remaining))
            if not ready:
                continue
            chunk = os.read(self.master_fd, 4096).decode("utf-8", errors="replace")
            buffer += strip_ansi(chunk).replace("\r", "")
            match = PROMPT_RE.search(buffer)
            if match:
                prompt = match.group(1)
                payload = buffer[: match.start()]
                return payload.strip(), prompt
        raise SuiteFailure(f"Timed out waiting for REPL prompt. Output so far:\n{buffer}")

    def run(self, line: str, *, timeout: float | None = None) -> tuple[str, str]:
        os.write(self.master_fd, (line + "\n").encode("utf-8"))
        payload, prompt = self._read_until_prompt(timeout or self.default_timeout)
        stripped_line = line.strip()
        if payload == stripped_line:
            payload = ""
        elif payload.startswith(stripped_line + "\n"):
            payload = payload[len(stripped_line) + 1 :].lstrip()
        return payload, prompt

    def close(self) -> None:
        if self.proc.poll() is None:
            try:
                os.write(self.master_fd, b"quit\n")
                self.proc.wait(timeout=2.0)
            except Exception:
                self.proc.send_signal(signal.SIGTERM)
        os.close(self.master_fd)


class ExternalCliSuite:
    def __init__(
        self,
        binary: pathlib.Path,
        *,
        expected_leaf_count: int | None,
        keep_temp: bool,
        verbose: bool,
        tier: str,
    ) -> None:
        self.binary = binary
        self.expected_leaf_count = expected_leaf_count
        self.verbose = verbose
        self.tier = tier
        self.failures: list[str] = []
        self.passes: list[str] = []
        self._manifest: dict[str, Any] | None = None
        self.case_names: set[str] = set()
        self.case_tiers: dict[str, str] = {}
        self.current_case: str = "<bootstrap>"
        self._trace_counter = 0
        self._traces_by_case: dict[str, list[CommandTrace]] = {}
        if keep_temp:
            self.temp_root = pathlib.Path(tempfile.mkdtemp(prefix="strata-cli-suite-"))
            self._temp_handle = None
        else:
            self._temp_handle = tempfile.TemporaryDirectory(prefix="strata-cli-suite-")
            self.temp_root = pathlib.Path(self._temp_handle.name)
        self._case_counter = 0

    def case_db(self, label: str) -> pathlib.Path:
        self._case_counter += 1
        name = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
        root = self.temp_root / f"{self._case_counter:02d}-{name}"
        root.mkdir(parents=True, exist_ok=True)
        return root / "db"

    def log(self, message: str) -> None:
        if self.verbose:
            print(message)

    def display_command(self, argv: Sequence[str]) -> str:
        rendered: list[str] = []
        skip_next_db = False
        for i, arg in enumerate(argv):
            if skip_next_db:
                skip_next_db = False
                continue
            if i == 0:
                rendered.append("strata")
                continue
            if arg == "--db" and i + 1 < len(argv):
                rendered.extend(["--db", "<db>"])
                skip_next_db = True
                continue
            rendered.append(arg)
        return shlex.join(rendered)

    def split_command_input(self, argv: Sequence[str], stdin: str | None) -> tuple[str, str]:
        tokens: list[str] = []
        skip_next = False
        for i, arg in enumerate(argv):
            if i == 0:
                continue
            if skip_next:
                skip_next = False
                continue
            if arg in {"--db", "--branch", "--space"} and i + 1 < len(argv):
                skip_next = True
                continue
            if arg in {"-j", "-r", "--cache", "--read-only", "--follower"}:
                continue
            tokens.append(arg)

        if not tokens:
            return "interactive shell", compact_input(stdin)

        command_keys = set(self.load_manifest().get("commands", {}).keys())
        command = tokens[0]
        input_tokens = tokens[1:]
        for end in range(len(tokens), 0, -1):
            candidate = " ".join(tokens[:end])
            if candidate in command_keys:
                command = candidate
                input_tokens = tokens[end:]
                break

        pieces: list[str] = []
        if input_tokens:
            pieces.append(shlex.join(input_tokens))
        if stdin and stdin.strip():
            pieces.append(f"stdin={compact_input(stdin)}")
        return command, " ; ".join(pieces) if pieces else "-"

    def trace_result(self, result: CommandResult, *, check: str | None = None) -> None:
        traces = self._traces_by_case.get(self.current_case, [])
        for trace in traces:
            if trace.trace_id == result.trace_id:
                if check is not None:
                    trace.check = check
                return

    def emit_case_traces(self, case_name: str, status: str) -> None:
        if not self.verbose:
            return
        traces = self._traces_by_case.get(case_name, [])
        if case_name == "help-tree":
            print(
                "LOG  "
                f"[{status:<4}]"
                f" | case={case_name}"
                f" | cmds={len(traces)}"
                " | check=help inventory matches manifest"
                " | strata=command tree crawl completed"
            )
            return
        for trace in traces:
            command_name, input_value = self.split_command_input(trace.command, trace.stdin)
            if trace.stdout and trace.stderr:
                actual = f"rc={trace.returncode} out={one_line(trace.stdout, limit=90)} err={one_line(trace.stderr, limit=90)}"
            elif trace.stdout:
                actual = f"rc={trace.returncode} out={one_line(trace.stdout, limit=110)}"
            elif trace.stderr:
                actual = f"rc={trace.returncode} err={one_line(trace.stderr, limit=110)}"
            else:
                actual = f"rc={trace.returncode} out=<empty>"
            print(
                "LOG  "
                f"[{status:<4}]"
                f" | case={case_name}"
                f" | cmd={command_name}"
                f" | input={input_value}"
                f" | check={one_line(trace.check, limit=60)}"
                f" | strata={actual}"
            )

    def load_manifest(self) -> dict[str, Any]:
        if self._manifest is None:
            self._manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        return self._manifest

    def tier_in_scope(self, case_tier: str) -> bool:
        return TIER_ORDER[case_tier] <= TIER_ORDER[self.tier]

    def run_process(
        self,
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        timeout: float = 10.0,
    ) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return self.make_result(
            argv,
            stdin=input_text,
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
        )

    def make_result(
        self,
        argv: Sequence[str],
        *,
        stdin: str | None,
        stdout: str,
        stderr: str,
        returncode: int,
    ) -> CommandResult:
        self._trace_counter += 1
        trace = CommandTrace(
            trace_id=self._trace_counter,
            case=self.current_case,
            command=list(argv),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )
        self._traces_by_case.setdefault(self.current_case, []).append(trace)
        result = CommandResult(
            argv=list(argv),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            trace_id=self._trace_counter,
        )
        return result

    def run_with_sigkill(
        self,
        argv: Sequence[str],
        *,
        input_lines: Sequence[str],
        kill_after: float,
        wait_for_stdout: str | None = None,
        wait_timeout: float = 10.0,
    ) -> CommandResult:
        payload = "\n".join(input_lines) + "\n"
        proc = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
            bufsize=0,
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def drain_ready(stream, sink: list[str]) -> None:
            if stream is None:
                return
            while True:
                try:
                    chunk = os.read(stream.fileno(), 4096)
                except BlockingIOError:
                    return
                if not chunk:
                    return
                sink.append(chunk.decode("utf-8", errors="replace"))
                if len(chunk) < 4096:
                    return

        try:
            assert proc.stdin is not None
            proc.stdin.write(payload.encode("utf-8"))
            proc.stdin.flush()
            proc.stdin.close()
            proc.stdin = None
            if wait_for_stdout is not None:
                assert proc.stdout is not None
                assert proc.stderr is not None
                deadline = time.monotonic() + wait_timeout
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        drain_ready(proc.stdout, stdout_parts)
                        drain_ready(proc.stderr, stderr_parts)
                        break
                    ready, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.1)
                    for stream in ready:
                        if stream is proc.stdout:
                            drain_ready(proc.stdout, stdout_parts)
                        else:
                            drain_ready(proc.stderr, stderr_parts)
                    if wait_for_stdout in "".join(stdout_parts):
                        break
                else:
                    raise SuiteFailure(
                        f"Timed out waiting for {wait_for_stdout!r} before SIGKILL\n"
                        f"stdout:\n{''.join(stdout_parts)}\n"
                        f"stderr:\n{''.join(stderr_parts)}"
                    )
            time.sleep(kill_after)
            if proc.poll() is None:
                os.kill(proc.pid, signal.SIGKILL)
            stdout_tail, stderr_tail = proc.communicate(timeout=5.0)
            stdout_parts.append(stdout_tail.decode("utf-8", errors="replace"))
            stderr_parts.append(stderr_tail.decode("utf-8", errors="replace"))
        finally:
            if proc.poll() is None:
                proc.kill()
        return self.make_result(
            argv,
            stdin=payload,
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            returncode=proc.returncode,
        )

    def wait_for_live_process_stdout(
        self,
        proc: subprocess.Popen[str],
        needle: str,
        *,
        timeout: float,
        context: str,
    ) -> None:
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        deadline = time.monotonic() + timeout
        assert proc.stdout is not None
        assert proc.stderr is not None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            ready, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.1)
            for stream in ready:
                line = stream.readline()
                if not line:
                    continue
                if stream is proc.stdout:
                    stdout_parts.append(line)
                else:
                    stderr_parts.append(line)
            if needle in "".join(stdout_parts):
                return
        raise SuiteFailure(
            f"{context}: timed out waiting for stdout marker {needle!r}\n"
            f"stdout:\n{''.join(stdout_parts)}\n"
            f"stderr:\n{''.join(stderr_parts)}"
        )

    def run_shell(
        self,
        db_path: pathlib.Path | None,
        *args: str,
        json_mode: bool = True,
        raw_mode: bool = False,
        timeout: float = 10.0,
    ) -> CommandResult:
        argv = [str(self.binary)]
        if db_path is not None:
            argv += ["--db", str(db_path)]
        if json_mode:
            argv.append("-j")
        if raw_mode:
            argv.append("-r")
        argv += list(args)
        return self.run_process(argv, timeout=timeout)

    def run_shell_human(
        self,
        db_path: pathlib.Path | None,
        *args: str,
        timeout: float = 10.0,
    ) -> CommandResult:
        return self.run_shell(db_path, *args, json_mode=False, raw_mode=False, timeout=timeout)

    def run_pipe(
        self,
        db_path: pathlib.Path,
        lines: Sequence[str],
        *,
        json_mode: bool = True,
        timeout: float = 10.0,
    ) -> CommandResult:
        argv = [str(self.binary), "--db", str(db_path)]
        if json_mode:
            argv.append("-j")
        payload = "\n".join(lines) + "\n"
        return self.run_process(argv, input_text=payload, timeout=timeout)

    def expect_json_docs(self, result: CommandResult, context: str) -> list[Any]:
        try:
            docs = first_parsable_json_stream(result.stdout, result.stderr)
        except SuiteFailure as exc:
            self.trace_result(result, check=f"parseable JSON for {context}")
            raise SuiteFailure(
                f"{context}: could not parse JSON from command {' '.join(result.argv)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}\n"
                f"error: {exc}"
            ) from exc
        self.trace_result(result, check=f"parseable JSON for {context}")
        return docs

    def assert_no_panic(self, result: CommandResult, context: str) -> None:
        combined = result.combined.lower()
        ensure(
            not any(needle in combined for needle in PANIC_NEEDLES),
            f"{context}: command surfaced a panic instead of a typed failure\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )

    def assert_typed_error(
        self,
        result: CommandResult,
        context: str,
        *variants: str,
        reason_needles: Sequence[str] = (),
    ) -> dict[str, Any]:
        self.assert_no_panic(result, context)
        ensure(result.returncode != 0, f"{context}: expected non-zero exit code for typed error")
        doc = only_doc(self.expect_json_docs(result, context), context)
        body = as_dict(doc, context)
        matched = next((variant for variant in variants if variant in body), None)
        ensure(
            matched is not None,
            f"{context}: expected one of {variants!r}, got {sorted(body)!r}",
        )
        payload = body[matched]
        reason = payload.get("reason", "") if isinstance(payload, dict) else ""
        if reason_needles:
            lowered = reason.lower()
            ensure(
                any(needle.lower() in lowered for needle in reason_needles),
                f"{context}: reason {reason!r} did not mention any of {list(reason_needles)!r}",
            )
        return body

    def assert_usage_failure(
        self,
        result: CommandResult,
        context: str,
        *needles: str,
    ) -> None:
        self.assert_no_panic(result, context)
        ensure(result.returncode != 0, f"{context}: expected command to fail")
        surface = (result.stdout + "\n" + result.stderr).lower()
        ensure(surface.strip(), f"{context}: expected failure output")
        for needle in needles:
            ensure(needle.lower() in surface, f"{context}: missing {needle!r} in failure output")

    def assert_open_lock_refusal(self, result: CommandResult, context: str) -> None:
        self.assert_no_panic(result, context)
        ensure(result.returncode != 0, f"{context}: expected writer lock refusal")
        combined = result.combined.lower()
        if combined.strip().startswith("{"):
            self.assert_typed_error(result, context, "Internal", reason_needles=["locked"])
            return
        ensure(
            "database is locked" in combined,
            f"{context}: expected database lock refusal, got:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )

    def expect_help(self, path: Sequence[str]) -> str:
        result = self.run_process([str(self.binary), *path, "--help"], timeout=5.0)
        label = f"help {' '.join(path) or '<root>'}"
        self.trace_result(result, check=f"help text with Usage for {label}")
        ensure(
            result.returncode in HELP_EXIT_CODES,
            f"help for {' '.join(path) or '<root>'} exited {result.returncode}",
        )
        text = result.stdout or result.stderr
        ensure("Usage:" in text, f"help for {' '.join(path) or '<root>'} did not contain Usage")
        return text

    def record(self, name: str, fn: Callable[[], None]) -> None:
        self.current_case = name
        self._traces_by_case[name] = []
        try:
            fn()
        except Exception as exc:
            message = f"{name}: {exc}"
            self.failures.append(message)
            self.emit_case_traces(name, "FAIL")
            print(f"FAIL  {message}")
        else:
            self.passes.append(name)
            self.emit_case_traces(name, "PASS")
            print(f"PASS  {name}")
        if self.verbose:
            print()

    def discover_leaves(self) -> list[tuple[str, ...]]:
        leaves: list[tuple[str, ...]] = []

        def walk(path: tuple[str, ...]) -> None:
            text = self.expect_help(path)
            children = command_lines(text)
            if not children:
                leaves.append(path)
                return
            for child in children:
                walk(path + (child,))

        walk(tuple())
        return leaves

    def test_help_tree(self) -> None:
        leaves = self.discover_leaves()
        ensure(leaves, "no CLI leaf commands discovered")
        discovered = {" ".join(path) for path in leaves}
        if self.expected_leaf_count is not None:
            ensure(
                len(discovered) == self.expected_leaf_count,
                f"expected {self.expected_leaf_count} leaf commands, discovered {len(discovered)}",
            )
        manifest = self.load_manifest()
        commands = manifest.get("commands", {})
        ensure(isinstance(commands, dict), "manifest missing `commands` object")
        manifest_leaves = set(commands)
        valid_coverages = {
            "success",
            "typed-refusal",
            "negative-usage",
            "state-machine-violation",
            "concurrency-conflict",
            "crash-recovery",
            "scale-stress",
        }

        def validate_coverage_entry(leaf: str, entry: Any, *, label: str) -> None:
            ensure(isinstance(entry, dict), f"{label} for {leaf!r} must be an object")
            owner = entry.get("owner")
            coverage = entry.get("coverage")
            tier = entry.get("tier")
            ensure(owner in self.case_names, f"{label} owner {owner!r} for {leaf!r} is not a known case")
            ensure(
                coverage in valid_coverages,
                (
                    f"{label} coverage for {leaf!r} must be one of "
                    f"{sorted(valid_coverages)!r}"
                ),
            )
            ensure(tier in TIER_ORDER, f"{label} tier for {leaf!r} must be one of {sorted(TIER_ORDER)}")
            owner_tier = self.case_tiers[owner]
            ensure(
                TIER_ORDER[owner_tier] <= TIER_ORDER[tier],
                f"{label} tier {tier!r} for {leaf!r} is broader than owner {owner!r} tier {owner_tier!r}",
            )

        ensure(
            discovered == manifest_leaves,
            "command inventory mismatch between binary help tree and checked-in manifest",
        )
        for leaf, entry in commands.items():
            ensure(isinstance(entry, dict), f"manifest entry for {leaf!r} must be an object")
            validate_coverage_entry(leaf, entry, label="manifest")
            extras = entry.get("additional_coverage", [])
            ensure(
                isinstance(extras, list),
                f"manifest additional_coverage for {leaf!r} must be a list when present",
            )
            for index, extra in enumerate(extras):
                validate_coverage_entry(leaf, extra, label=f"manifest additional_coverage[{index}]")
        print(f"INFO  discovered {len(discovered)} leaf commands")

    def test_shell_admin(self) -> None:
        db = self.case_db("shell-admin")

        pong = only_doc(self.expect_json_docs(self.run_shell(db, "ping"), "ping"), "ping")
        ensure("Pong" in as_dict(pong, "ping"), "ping did not return Pong")
        human_ping = self.run_shell_human(db, "ping")
        ensure(human_ping.returncode == 0, f"human ping failed: {human_ping.combined}")
        ensure("Pong" in human_ping.stdout, "human ping should include Pong")

        info = only_doc(self.expect_json_docs(self.run_shell(db, "info"), "info"), "info")
        ensure("DatabaseInfo" in as_dict(info, "info"), "info did not return DatabaseInfo")

        health = only_doc(self.expect_json_docs(self.run_shell(db, "health"), "health"), "health")
        health_body = as_dict(health, "health").get("Health")
        ensure(isinstance(health_body, dict), "health missing Health payload")
        ensure(health_body.get("status") in {"Healthy", "Degraded"}, "unexpected health status")

        metrics = only_doc(self.expect_json_docs(self.run_shell(db, "metrics"), "metrics"), "metrics")
        metrics_body = as_dict(metrics, "metrics").get("Metrics")
        ensure(isinstance(metrics_body, dict), "metrics missing Metrics payload")
        ensure("transactions" in metrics_body, "metrics missing transactions")

        flushed = only_doc(self.expect_json_docs(self.run_shell(db, "flush"), "flush"), "flush")
        ensure(flushed == "Unit", f"flush expected \"Unit\", got {flushed!r}")

        compact = only_doc(self.expect_json_docs(self.run_shell(db, "compact"), "compact"), "compact")
        ensure(
            compact == "Unit" or "InvalidInput" in as_dict(compact, "compact"),
            "compact did not return Unit or InvalidInput",
        )

        described = only_doc(
            self.expect_json_docs(self.run_shell(db, "describe"), "describe"),
            "describe",
        )
        described_body = as_dict(described, "describe").get("Described")
        ensure(isinstance(described_body, dict), "describe missing Described payload")
        ensure(described_body.get("path") == str(db), "describe path did not match db path")

        counters = only_doc(
            self.expect_json_docs(self.run_shell(db, "durability-counters"), "durability-counters"),
            "durability-counters",
        )
        body = as_dict(counters, "durability-counters").get("DurabilityCounters")
        ensure(isinstance(body, dict), "durability-counters missing payload")
        ensure("wal_appends" in body, "durability-counters missing wal_appends")

    def test_shell_persistence(self) -> None:
        db = self.case_db("shell-persistence")

        self.run_shell(db, "kv", "put", "persist:key", "41")
        self.run_shell(db, "json", "set", "persist:doc", "$", '{"name":"Persist","ok":true}')
        self.run_shell(db, "event", "append", "persist.created", '{"id":1}')
        self.run_shell(db, "vector", "create", "persist-vectors", "3")
        self.run_shell(db, "vector", "upsert", "persist-vectors", "doc1", "[1,2,3]")
        self.run_shell(db, "graph", "create", "persist-graph")
        self.run_shell(db, "graph", "add-node", "persist-graph", "n1", "--properties", '{"kind":"persist"}')
        self.run_shell(db, "config", "set", "durability", "always")
        self.run_shell(db, "configure-model", "http://localhost:11434/v1", "qwen3:1.7b")

        flushed = only_doc(self.expect_json_docs(self.run_shell(db, "flush"), "persistence flush"), "persistence flush")
        ensure(flushed == "Unit", f"persistence flush expected Unit, got {flushed!r}")
        compacted = only_doc(self.expect_json_docs(self.run_shell(db, "compact"), "persistence compact"), "persistence compact")
        ensure(
            compacted == "Unit" or "InvalidInput" in as_dict(compacted, "persistence compact"),
            "persistence compact did not return Unit or InvalidInput",
        )

        kv_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "get", "persist:key"), "persist kv get"),
            "persist kv get",
        )
        ensure(unwrap_value(unwrap_maybe_versioned(kv_after, "persist kv get")["value"], "Int", "persist kv get") == 41, "persisted kv value should survive reopen")
        raw_kv_after = self.run_shell(db, "kv", "get", "persist:key", json_mode=False, raw_mode=True)
        ensure(raw_kv_after.returncode == 0, f"raw persisted kv get failed: {raw_kv_after.combined}")
        ensure(raw_kv_after.stdout.strip() == "41", f"raw persisted kv get expected '41', got {raw_kv_after.stdout.strip()!r}")

        json_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "get", "persist:doc", "$"), "persist json get"),
            "persist json get",
        )
        persist_obj = unwrap_value(unwrap_maybe_versioned(json_after, "persist json get")["value"], "Object", "persist json get")
        ensure(unwrap_value(require_key(persist_obj, "name", "persist json object"), "String", "persist json object name") == "Persist", "persisted json name should survive reopen")
        ensure(unwrap_value(require_key(persist_obj, "ok", "persist json object"), "Bool", "persist json object ok") is True, "persisted json flag should survive reopen")

        event_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "list", "--type", "persist.created"), "persist event list"),
            "persist event list",
        )
        event_values = as_dict(event_after, "persist event list").get("VersionedValues", [])
        ensure(len(event_values) == 1, f"persist event list should contain one event, got {len(event_values)}")
        ensure(
            unwrap_value(require_key(unwrap_value(event_values[0]["value"], "Object", "persist event payload"), "id", "persist event payload"), "Int", "persist event id") == 1,
            "persisted event payload should survive reopen",
        )

        vector_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "get", "persist-vectors", "doc1"), "persist vector get"),
            "persist vector get",
        )
        embedding = as_dict(vector_after, "persist vector get").get("VectorData", {}).get("data", {}).get("embedding", [])
        ensure(embedding == [1.0, 2.0, 3.0], f"persisted vector embedding mismatch: {embedding!r}")

        graph_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "get-node", "persist-graph", "n1"), "persist graph get-node"),
            "persist graph get-node",
        )
        graph_obj = unwrap_value(unwrap_maybe(graph_after, "persist graph get-node"), "Object", "persist graph object")
        properties = unwrap_value(require_key(graph_obj, "properties", "persist graph object"), "Object", "persist graph properties")
        ensure(unwrap_value(require_key(properties, "kind", "persist graph properties"), "String", "persist graph kind") == "persist", "persisted graph node should survive reopen")

        durability_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "get", "durability"), "persist config get durability"),
            "persist config get durability",
        )
        ensure(as_dict(durability_after, "persist config get durability").get("ConfigValue") == "always", "durability config should survive reopen")

        cfg_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "list"), "persist config list"),
            "persist config list",
        )
        model_cfg = as_dict(cfg_after, "persist config list").get("Config", {}).get("model", {})
        ensure(model_cfg.get("endpoint") == "http://localhost:11434/v1", "configure-model endpoint should survive reopen")
        ensure(model_cfg.get("model") == "qwen3:1.7b", "configure-model model should survive reopen")

    def test_shell_branch_space(self) -> None:
        db = self.case_db("shell-branch-space")

        exists_default = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "exists", "default"), "branch exists default"),
            "branch exists default",
        )
        ensure(as_dict(exists_default, "branch exists default").get("Bool") is True, "default branch should exist")

        created = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "create", "feature"), "branch create feature"),
            "branch create feature",
        )
        info = as_dict(created, "branch create feature").get("BranchWithVersion", {})
        ensure(info.get("info", {}).get("id") == "feature", "branch create did not create feature")

        listed = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "list"), "branch list"),
            "branch list",
        )
        items = as_dict(listed, "branch list").get("BranchInfoList", [])
        names = {item["info"]["id"] for item in items}
        ensure({"default", "feature"}.issubset(names), "branch list missing expected names")

        feature_info = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "info", "feature"), "branch info feature"),
            "branch info feature",
        )
        maybe = as_dict(feature_info, "branch info feature").get("MaybeBranchInfo", {})
        ensure(maybe.get("info", {}).get("id") == "feature", "branch info did not return feature")

        exists_feature = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "exists", "feature"), "branch exists feature"),
            "branch exists feature",
        )
        ensure(as_dict(exists_feature, "branch exists feature").get("Bool") is True, "feature branch should exist")

        deleted = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "del", "feature"), "branch del feature"),
            "branch del feature",
        )
        ensure(deleted == "Unit", f"branch del feature expected Unit, got {deleted!r}")

        missing = only_doc(
            self.expect_json_docs(self.run_shell(db, "branch", "del", "missing"), "branch del missing"),
            "branch del missing",
        )
        ensure("BranchNotFound" in as_dict(missing, "branch del missing"), "missing branch delete did not return BranchNotFound")

        create_space = only_doc(
            self.expect_json_docs(self.run_shell(db, "space", "create", "analytics"), "space create analytics"),
            "space create analytics",
        )
        ensure(create_space == "Unit", f"space create analytics expected Unit, got {create_space!r}")

        listed_spaces = only_doc(
            self.expect_json_docs(self.run_shell(db, "space", "list"), "space list"),
            "space list",
        )
        spaces = set(as_dict(listed_spaces, "space list").get("SpaceList", []))
        ensure({"default", "analytics"}.issubset(spaces), "space list missing expected spaces")

        space_exists = only_doc(
            self.expect_json_docs(self.run_shell(db, "space", "exists", "analytics"), "space exists analytics"),
            "space exists analytics",
        )
        ensure(as_dict(space_exists, "space exists analytics").get("Bool") is True, "analytics space should exist")

        cannot_delete_default = only_doc(
            self.expect_json_docs(self.run_shell(db, "space", "del", "default"), "space del default"),
            "space del default",
        )
        ensure(
            "ConstraintViolation" in as_dict(cannot_delete_default, "space del default"),
            "space del default should be refused",
        )

        deleted_space = only_doc(
            self.expect_json_docs(self.run_shell(db, "space", "del", "analytics"), "space del analytics"),
            "space del analytics",
        )
        ensure(deleted_space == "Unit", f"space del analytics expected Unit, got {deleted_space!r}")

    def test_shell_kv(self) -> None:
        db = self.case_db("shell-kv")

        put1 = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "put", "user:a", "1"), "kv put user:a 1"),
            "kv put user:a 1",
        )
        ensure("WriteResult" in as_dict(put1, "kv put user:a 1"), "kv put did not return WriteResult")

        get1 = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "get", "user:a"), "kv get user:a"),
            "kv get user:a",
        )
        value = unwrap_maybe_versioned(get1, "kv get user:a")["value"]
        ensure(unwrap_value(value, "Int", "kv get user:a") == 1, f"kv get user:a expected 1, got {value!r}")

        put2 = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "put", "user:a", "2"), "kv put user:a 2"),
            "kv put user:a 2",
        )
        ensure("WriteResult" in as_dict(put2, "kv put user:a 2"), "second kv put did not return WriteResult")
        first_timestamp = as_dict(
            only_doc(
                self.expect_json_docs(self.run_shell(db, "kv", "history", "user:a"), "kv history user:a after first overwrite"),
                "kv history user:a after first overwrite",
            ),
            "kv history user:a after first overwrite",
        ).get("VersionHistory", [{}])[0].get("timestamp")
        ensure(isinstance(first_timestamp, int), "kv history should expose a timestamp for time-travel checks")
        wait_until_timestamp_passed(first_timestamp)
        self.run_shell(db, "kv", "put", "user:b", "3")
        self.run_shell(db, "kv", "put", "user:c", "4")
        self.run_shell(db, "kv", "put", "flag:enabled", "true")

        history = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "history", "user:a"), "kv history user:a"),
            "kv history user:a",
        )
        versions = as_dict(history, "kv history user:a").get("VersionHistory", [])
        ensure(len(versions) >= 2, "kv history should contain two versions")
        ensure(versions[0]["value"]["Int"] == 2, "kv history newest value should be 2")
        ensure(versions[1]["value"]["Int"] == 1, "kv history older value should be 1")

        getv = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "getv", "user:a"), "kv getv user:a"),
            "kv getv user:a",
        )
        getv_versions = as_dict(getv, "kv getv user:a").get("VersionHistory", [])
        ensure(len(getv_versions) >= 2, "kv getv should contain two versions")

        as_of = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "kv", "get", "user:a", "--as-of", str(versions[1]["timestamp"])),
                "kv get user:a --as-of",
            ),
            "kv get user:a --as-of",
        )
        ensure(
            as_dict(as_of, "kv get user:a --as-of").get("Maybe", {}).get("Int") == 1,
            "time-travel kv get should return the older value",
        )

        page1 = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "kv", "list", "--prefix", "user:", "--limit", "2"),
                "kv list --prefix user: --limit 2",
            ),
            "kv list --prefix user: --limit 2",
        )
        keys_page1 = as_dict(page1, "kv list --prefix user: --limit 2").get("KeysPage", {})
        ensure(
            keys_page1.get("keys") == ["user:a", "user:b"],
            f"unexpected kv page1 keys: {keys_page1.get('keys')!r}",
        )
        ensure(keys_page1.get("has_more") is True, "kv page1 should report has_more")
        cursor = keys_page1.get("cursor")
        ensure(cursor == "user:b", f"unexpected kv page cursor: {cursor!r}")

        page2 = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "kv", "list", "--prefix", "user:", "--limit", "2", "--cursor", cursor),
                "kv list page2",
            ),
            "kv list page2",
        )
        keys_page2 = as_dict(page2, "kv list page2").get("KeysPage", {})
        ensure(keys_page2.get("keys") == ["user:c"], f"unexpected kv page2 keys: {keys_page2.get('keys')!r}")

        scanned = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "scan", "--start", "user:b"), "kv scan --start user:b"),
            "kv scan --start user:b",
        )
        scanned_pairs = as_dict(scanned, "kv scan --start user:b").get("KvScanResult", [])
        scanned_keys = [pair[0] for pair in scanned_pairs]
        ensure(scanned_keys[:2] == ["user:b", "user:c"], f"unexpected kv scan keys: {scanned_keys!r}")

        counted = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "count", "--prefix", "user:"), "kv count --prefix user:"),
            "kv count --prefix user:",
        )
        counted_value = as_dict(counted, "kv count --prefix user:").get("Uint")
        ensure(counted_value == 3, f"kv count expected 3 user-visible keys, got {counted_value!r}")

        raw_get = self.run_shell(db, "kv", "get", "user:a", json_mode=False, raw_mode=True)
        ensure(raw_get.returncode == 0, f"raw kv get failed: {raw_get.combined}")
        ensure(raw_get.stdout.strip() == "2", f"raw kv get expected '2', got {raw_get.stdout.strip()!r}")

        raw_count = self.run_shell(db, "kv", "count", "--prefix", "user:", json_mode=False, raw_mode=True)
        ensure(raw_count.returncode == 0, f"raw kv count failed: {raw_count.combined}")
        ensure(raw_count.stdout.strip() == "3", f"raw kv count expected '3', got {raw_count.stdout.strip()!r}")

        deleted = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "del", "user:b", "user:c"), "kv del user:b user:c"),
            "kv del user:b user:c",
        )
        batch_results = as_dict(deleted, "kv del user:b user:c").get("BatchResults", [])
        ensure(len(batch_results) == 2, "kv del multi-key should return two batch results")
        ensure(all(item.get("error") is None for item in batch_results), "kv del multi-key should not return errors")

        after_delete = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "get", "user:b"), "kv get user:b after delete"),
            "kv get user:b after delete",
        )
        ensure(
            as_dict(after_delete, "kv get user:b after delete").get("MaybeVersioned") is None,
            "deleted key should no longer be visible",
        )

    def test_shell_json(self) -> None:
        db = self.case_db("shell-json")

        set1 = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "json", "set", "user:doc", "$", '{"name":"Alice","n":1}'),
                "json set user:doc",
            ),
            "json set user:doc",
        )
        ensure("WriteResult" in as_dict(set1, "json set user:doc"), "json set did not return WriteResult")

        get1 = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "get", "user:doc", "$"), "json get user:doc"),
            "json get user:doc",
        )
        obj = unwrap_value(unwrap_maybe_versioned(get1, "json get user:doc")["value"], "Object", "json get user:doc")
        ensure(
            unwrap_value(require_key(obj, "name", "json get user:doc"), "String", "json get user:doc name") == "Alice",
            "json get returned wrong name",
        )

        first_timestamp = as_dict(
            only_doc(
                self.expect_json_docs(self.run_shell(db, "json", "history", "user:doc"), "json history user:doc after first write"),
                "json history user:doc after first write",
            ),
            "json history user:doc after first write",
        ).get("VersionHistory", [{}])[0].get("timestamp")
        ensure(isinstance(first_timestamp, int), "json history should expose a timestamp for time-travel checks")
        wait_until_timestamp_passed(first_timestamp)
        self.run_shell(db, "json", "set", "user:doc", "$", '{"name":"Bob","n":2}')
        self.run_shell(db, "json", "set", "user:doc2", "$", '{"name":"Carol","n":3}')

        history = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "history", "user:doc"), "json history user:doc"),
            "json history user:doc",
        )
        versions = as_dict(history, "json history user:doc").get("VersionHistory", [])
        ensure(len(versions) >= 2, "json history should contain two versions")
        ensure(versions[0]["value"]["Object"]["name"]["String"] == "Bob", "json history newest value should be Bob")
        ensure(versions[1]["value"]["Object"]["name"]["String"] == "Alice", "json history older value should be Alice")

        getv = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "getv", "user:doc"), "json getv user:doc"),
            "json getv user:doc",
        )
        getv_versions = as_dict(getv, "json getv user:doc").get("VersionHistory", [])
        ensure(len(getv_versions) >= 2, "json getv should contain two versions")

        as_of = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "json", "get", "user:doc", "$", "--as-of", str(versions[1]["timestamp"])),
                "json get user:doc --as-of",
            ),
            "json get user:doc --as-of",
        )
        as_of_obj = unwrap_value(unwrap_maybe(as_of, "json get user:doc --as-of"), "Object", "json get user:doc --as-of")
        ensure(
            unwrap_value(require_key(as_of_obj, "name", "json get user:doc --as-of"), "String", "json get user:doc --as-of name") == "Alice",
            "time-travel json get should return Alice",
        )

        listed = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "list", "--prefix", "user:"), "json list --prefix user:"),
            "json list --prefix user:",
        )
        list_body = as_dict(listed, "json list --prefix user:").get("JsonListResult", {})
        ensure(list_body.get("keys") == ["user:doc", "user:doc2"], f"unexpected json list keys: {list_body.get('keys')!r}")
        ensure(list_body.get("has_more") is False, "json list should not have more pages")

        counted = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "count", "--prefix", "user:"), "json count --prefix user:"),
            "json count --prefix user:",
        )
        ensure(as_dict(counted, "json count --prefix user:").get("Uint") == 2, "json count should be 2")

        deleted = only_doc(
            self.expect_json_docs(self.run_shell(db, "json", "del", "user:doc2", "$"), "json del user:doc2"),
            "json del user:doc2",
        )
        delete_result = as_dict(deleted, "json del user:doc2").get("DeleteResult", {})
        ensure(delete_result.get("deleted") is True, "json del should report deleted=true")

    def test_shell_event(self) -> None:
        db = self.case_db("shell-event")

        append1 = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "event", "append", "user.created", '{"id":1}'),
                "event append 1",
            ),
            "event append 1",
        )
        ensure(as_dict(append1, "event append 1").get("EventAppendResult", {}).get("sequence") == 0, "first event should have sequence 0")
        self.run_shell(db, "event", "append", "user.updated", '{"id":1,"name":"A"}')
        self.run_shell(db, "event", "append", "user.updated", '{"id":2,"name":"B"}')

        got = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "get", "0"), "event get 0"),
            "event get 0",
        )
        got_obj = unwrap_value(unwrap_maybe_versioned(got, "event get 0")["value"], "Object", "event get 0")
        ensure(unwrap_value(require_key(got_obj, "id", "event get 0"), "Int", "event get 0 id") == 1, "event get 0 returned wrong payload")

        listed = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "list"), "event list"),
            "event list",
        )
        values = as_dict(listed, "event list").get("VersionedValues", [])
        ensure(len(values) == 3, f"event list should contain 3 events, got {len(values)}")

        typed = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "list", "--type", "user.updated"), "event list --type"),
            "event list --type",
        )
        typed_values = as_dict(typed, "event list --type").get("VersionedValues", [])
        ensure(len(typed_values) == 2, f"event list --type should contain 2 events, got {len(typed_values)}")

        counted = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "len"), "event len"),
            "event len",
        )
        ensure(as_dict(counted, "event len").get("Uint") == 3, "event len should be 3")

        types = only_doc(
            self.expect_json_docs(self.run_shell(db, "event", "types"), "event types"),
            "event types",
        )
        ensure(
            set(as_dict(types, "event types").get("Keys", [])) == {"user.created", "user.updated"},
            "event types did not match appended types",
        )

        range_by_seq = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "event", "range", "0", "--end-seq", "1"),
                "event range",
            ),
            "event range",
        )
        seq_events = as_dict(range_by_seq, "event range").get("EventRangeResult", {}).get("events", [])
        ensure(len(seq_events) == 1, f"event range expected 1 event, got {len(seq_events)}")

        reverse_by_seq = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "event", "range", "0", "--end-seq", "3", "--direction", "reverse"),
                "event range reverse",
            ),
            "event range reverse",
        )
        reverse_events = as_dict(reverse_by_seq, "event range reverse").get("EventRangeResult", {}).get("events", [])
        ensure(len(reverse_events) == 3, f"reverse event range expected 3 events, got {len(reverse_events)}")
        reverse_ids = [
            unwrap_value(
                require_key(unwrap_value(event["value"], "Object", "event range reverse item"), "id", "event range reverse item"),
                "Int",
                "event range reverse item id",
            )
            for event in reverse_events
        ]
        ensure(reverse_ids == [2, 1, 1], f"reverse event range expected ids [2, 1, 1], got {reverse_ids!r}")

        start_ts = values[0]["timestamp"]
        end_ts = values[1]["timestamp"]
        range_by_time = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "event", "range-time", str(start_ts)),
                "event range-time",
            ),
            "event range-time",
        )
        time_events = as_dict(range_by_time, "event range-time").get("EventRangeResult", {}).get("events", [])
        ensure(len(time_events) == 3, f"event range-time expected 3 events, got {len(time_events)}")

        range_by_time_window = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "event", "range-time", str(start_ts), "--end-ts", str(end_ts), "--direction", "forward"),
                "event range-time window",
            ),
            "event range-time window",
        )
        time_window_events = as_dict(range_by_time_window, "event range-time window").get("EventRangeResult", {}).get("events", [])
        ensure(len(time_window_events) == 2, f"event range-time window expected 2 events, got {len(time_window_events)}")

    def test_shell_vector(self) -> None:
        db = self.case_db("shell-vector")

        created = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "create", "docs", "3"), "vector create"),
            "vector create",
        )
        ensure(as_dict(created, "vector create").get("Version") == 1, "vector create should return Version 1")

        upsert = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "vector", "upsert", "docs", "doc1", "[1,2,3]"),
                "vector upsert",
            ),
            "vector upsert",
        )
        ensure(as_dict(upsert, "vector upsert").get("VectorWriteResult", {}).get("key") == "doc1", "vector upsert returned wrong key")

        fetched = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "get", "docs", "doc1"), "vector get"),
            "vector get",
        )
        embedding = as_dict(fetched, "vector get").get("VectorData", {}).get("data", {}).get("embedding", [])
        ensure(embedding == [1.0, 2.0, 3.0], f"vector get returned wrong embedding: {embedding!r}")

        history = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "history", "docs", "doc1"), "vector history"),
            "vector history",
        )
        ensure(len(as_dict(history, "vector history").get("VectorVersionHistory", [])) == 1, "vector history should contain one version")

        matches = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "search", "docs", "[1,2,3]"), "vector search"),
            "vector search",
        )
        match_items = as_dict(matches, "vector search").get("VectorMatches", [])
        ensure(match_items and match_items[0]["key"] == "doc1", "vector search should return doc1 first")

        self.run_shell(db, "vector", "upsert", "docs", "doc1", "[3,4,5]")
        getv = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "getv", "docs", "doc1"), "vector getv"),
            "vector getv",
        )
        getv_history = as_dict(getv, "vector getv").get("VectorVersionHistory", [])
        ensure(len(getv_history) >= 2, "vector getv should contain two versions")

        batch = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "vector",
                    "batch-upsert",
                    "docs",
                    '[{"key":"doc2","vector":[5,6,7]},{"key":"doc3","vector":[7,8,9]}]',
                ),
                "vector batch-upsert",
            ),
            "vector batch-upsert",
        )
        ensure(as_dict(batch, "vector batch-upsert").get("Versions") == [1, 1], "vector batch-upsert should return per-entry versions")

        collections = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "collections"), "vector collections"),
            "vector collections",
        )
        coll_items = as_dict(collections, "vector collections").get("VectorCollectionList", [])
        ensure(len(coll_items) == 1 and coll_items[0]["name"] == "docs", "vector collections should include docs")
        ensure(coll_items[0]["count"] == 3, "vector collections should report three vectors after batch-upsert")

        raw_collections = self.run_shell(db, "vector", "collections", json_mode=False, raw_mode=True)
        ensure(raw_collections.returncode == 0, f"raw vector collections failed: {raw_collections.combined}")
        ensure(
            '"name":"docs"' in raw_collections.stdout and '"count":3' in raw_collections.stdout,
            "raw vector collections should include docs with count 3",
        )

        stats = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "stats", "docs"), "vector stats"),
            "vector stats",
        )
        stat_items = as_dict(stats, "vector stats").get("VectorCollectionList", [])
        ensure(
            len(stat_items) == 1 and stat_items[0]["count"] == 3,
            "vector stats should report three vectors after batch-upsert",
        )

        deleted = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "del", "docs", "doc2"), "vector del"),
            "vector del",
        )
        ensure(
            as_dict(deleted, "vector del").get("VectorDeleteResult", {}).get("deleted") is True,
            "vector del should report deleted=true",
        )

        after_delete = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "get", "docs", "doc2"), "vector get doc2 after delete"),
            "vector get doc2 after delete",
        )
        ensure(as_dict(after_delete, "vector get doc2 after delete").get("VectorData") is None, "deleted vector should not be visible")

        dropped = only_doc(
            self.expect_json_docs(self.run_shell(db, "vector", "drop", "docs"), "vector drop"),
            "vector drop",
        )
        ensure(as_dict(dropped, "vector drop").get("Bool") is True, "vector drop should return Bool true")

    def test_shell_graph(self) -> None:
        db = self.case_db("shell-graph")

        created = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "create", "demo"), "graph create"),
            "graph create",
        )
        ensure(created == "Unit", f"graph create expected Unit, got {created!r}")

        graphs = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "list"), "graph list"),
            "graph list",
        )
        ensure(as_dict(graphs, "graph list").get("Keys") == ["demo"], "graph list should contain demo")

        node_a = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "graph", "add-node", "demo", "a", "--properties", '{"kind":"root"}'),
                "graph add-node a",
            ),
            "graph add-node a",
        )
        ensure(as_dict(node_a, "graph add-node a").get("GraphWriteResult", {}).get("node_id") == "a", "graph add-node a failed")

        self.run_shell(db, "graph", "add-node", "demo", "b", "--properties", '{"kind":"leaf"}')
        edge = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "graph",
                    "add-edge",
                    "demo",
                    "a",
                    "b",
                    "child",
                    "--properties",
                    '{"w":1}',
                ),
                "graph add-edge",
            ),
            "graph add-edge",
        )
        ensure(as_dict(edge, "graph add-edge").get("GraphEdgeWriteResult", {}).get("dst") == "b", "graph add-edge failed")

        fetched = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "get-node", "demo", "a"), "graph get-node"),
            "graph get-node",
        )
        maybe = as_dict(fetched, "graph get-node").get("Maybe", {}).get("Object", {})
        ensure(maybe.get("properties", {}).get("Object", {}).get("kind", {}).get("String") == "root", "graph get-node returned wrong properties")

        neighbors = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "neighbors", "demo", "a"), "graph neighbors"),
            "graph neighbors",
        )
        n_items = as_dict(neighbors, "graph neighbors").get("GraphNeighbors", [])
        ensure(n_items and n_items[0]["node_id"] == "b", "graph neighbors should include b")

        listed = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "list-nodes", "demo"), "graph list-nodes"),
            "graph list-nodes",
        )
        ensure(as_dict(listed, "graph list-nodes").get("Keys") == ["a", "b"], "graph list-nodes should return a,b")

        info = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "info", "demo"), "graph info"),
            "graph info",
        )
        ensure("Maybe" in as_dict(info, "graph info"), "graph info should return Maybe payload")

        removed_edge = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "graph", "remove-edge", "demo", "a", "b", "child"),
                "graph remove-edge",
            ),
            "graph remove-edge",
        )
        ensure(removed_edge == "Unit", f"graph remove-edge expected Unit, got {removed_edge!r}")

        removed_node = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "graph", "remove-node", "demo", "b"),
                "graph remove-node",
            ),
            "graph remove-node",
        )
        ensure(removed_node == "Unit", f"graph remove-node expected Unit, got {removed_node!r}")

        deleted = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "delete", "demo"), "graph delete"),
            "graph delete",
        )
        ensure(deleted == "Unit", f"graph delete expected Unit, got {deleted!r}")

    def test_shell_branch_power(self) -> None:
        source_db = self.case_db("shell-branch-power")
        bundle_path = source_db.parent / "feature.branchbundle.tar.zst"
        imported_db = source_db.parent / "imported-db"

        self.run_shell(source_db, "kv", "put", "shared", "1")
        forked = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "fork", "feature"), "branch fork"),
            "branch fork",
        )
        ensure(
            as_dict(forked, "branch fork").get("BranchForked", {}).get("destination") == "feature",
            "branch fork should create feature",
        )

        self.run_shell(source_db, "--branch", "feature", "kv", "put", "shared", "2")
        self.run_shell(source_db, "--branch", "feature", "kv", "put", "only_feature", "9")

        diff = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "diff", "default", "feature"), "branch diff"),
            "branch diff",
        )
        summary = as_dict(diff, "branch diff").get("BranchDiff", {}).get("summary", {})
        ensure(summary.get("total_added") == 1, "branch diff should show one added key")
        ensure(summary.get("total_modified") == 1, "branch diff should show one modified key")

        merge_base = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "merge-base", "default", "feature"), "branch merge-base"),
            "branch merge-base",
        )
        ensure("MergeBaseInfo" in as_dict(merge_base, "branch merge-base"), "branch merge-base should return MergeBaseInfo")

        diff3 = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "diff3", "default", "feature"), "branch diff3"),
            "branch diff3",
        )
        entries = as_dict(diff3, "branch diff3").get("ThreeWayDiff", {}).get("entries", [])
        ensure(len(entries) == 2, "branch diff3 should expose two divergent entries")

        merged = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "merge", "feature"), "branch merge"),
            "branch merge",
        )
        merge_info = as_dict(merged, "branch merge").get("BranchMerged", {})
        ensure(merge_info.get("keys_applied") == 2, "branch merge should apply two keys")
        merge_version = merge_info.get("merge_version")
        ensure(merge_version is not None, "branch merge should return a merge version")

        shared_after_merge = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "kv", "get", "shared"), "kv get shared after merge"),
            "kv get shared after merge",
        )
        ensure(
            as_dict(shared_after_merge, "kv get shared after merge").get("MaybeVersioned", {}).get("value", {}).get("Int") == 2,
            "merged branch should expose updated shared value",
        )

        only_feature_after_merge = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "kv", "get", "only_feature"), "kv get only_feature after merge"),
            "kv get only_feature after merge",
        )
        ensure(
            as_dict(only_feature_after_merge, "kv get only_feature after merge").get("MaybeVersioned", {}).get("value", {}).get("Int") == 9,
            "merged branch should expose added feature-only key",
        )

        reverted = only_doc(
            self.expect_json_docs(
                self.run_shell(source_db, "branch", "revert", "default", str(merge_version), str(merge_version)),
                "branch revert",
            ),
            "branch revert",
        )
        ensure(
            as_dict(reverted, "branch revert").get("BranchReverted", {}).get("keys_reverted") == 2,
            "branch revert should revert the merged keys",
        )

        shared_after_revert = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "kv", "get", "shared"), "kv get shared after revert"),
            "kv get shared after revert",
        )
        ensure(
            as_dict(shared_after_revert, "kv get shared after revert").get("MaybeVersioned", {}).get("value", {}).get("Int") == 1,
            "branch revert should restore shared to its original value",
        )

        cherry_picked = only_doc(
            self.expect_json_docs(
                self.run_shell(source_db, "branch", "cherry-pick", "default", "feature", "--pick", "default", "shared"),
                "branch cherry-pick",
            ),
            "branch cherry-pick",
        )
        ensure(
            as_dict(cherry_picked, "branch cherry-pick").get("BranchCherryPicked", {}).get("keys_applied") == 1,
            "branch cherry-pick should apply one picked key",
        )

        exported = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "export", "feature", str(bundle_path)), "branch export"),
            "branch export",
        )
        ensure(
            as_dict(exported, "branch export").get("BranchExported", {}).get("path") == str(bundle_path),
            "branch export should write the expected bundle path",
        )
        ensure(bundle_path.exists(), "branch export should create a bundle on disk")

        validated = only_doc(
            self.expect_json_docs(self.run_shell(source_db, "branch", "validate", str(bundle_path)), "branch validate"),
            "branch validate",
        )
        ensure(
            as_dict(validated, "branch validate").get("BundleValidated", {}).get("checksums_valid") is True,
            "branch validate should confirm valid checksums",
        )

        imported = only_doc(
            self.expect_json_docs(self.run_shell(imported_db, "branch", "import", str(bundle_path)), "branch import"),
            "branch import",
        )
        ensure(
            as_dict(imported, "branch import").get("BranchImported", {}).get("branch_id") == "feature",
            "branch import should restore the feature branch",
        )

        exists_imported = only_doc(
            self.expect_json_docs(self.run_shell(imported_db, "branch", "exists", "feature"), "branch exists imported"),
            "branch exists imported",
        )
        ensure(as_dict(exists_imported, "branch exists imported").get("Bool") is True, "imported branch should exist")

        imported_value = only_doc(
            self.expect_json_docs(self.run_shell(imported_db, "--branch", "feature", "kv", "get", "shared"), "imported branch kv get"),
            "imported branch kv get",
        )
        ensure(
            as_dict(imported_value, "imported branch kv get").get("MaybeVersioned", {}).get("value", {}).get("Int") == 1,
            "imported branch should preserve branch data",
        )

    def test_shell_graph_power(self) -> None:
        db = self.case_db("shell-graph-power")

        self.run_shell(db, "graph", "create", "demo")
        self.run_shell(db, "graph", "ontology", "define", "demo", '{"name":"Person","fields":{}}')
        self.run_shell(db, "graph", "ontology", "define", "demo", '{"name":"Company","fields":{}}')
        self.run_shell(db, "graph", "ontology", "define", "demo", '{"name":"WORKS_AT","source":"Person","target":"Company"}')

        status = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "status", "demo"), "graph ontology status"),
            "graph ontology status",
        )
        ensure(
            as_dict(status, "graph ontology status").get("Maybe", {}).get("String") == "draft",
            "ontology status should start as draft",
        )

        summary = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "summary", "demo"), "graph ontology summary"),
            "graph ontology summary",
        )
        summary_body = as_dict(summary, "graph ontology summary").get("Maybe", {}).get("Object", {})
        ensure("object_types" in summary_body and "link_types" in summary_body, "ontology summary should contain type maps")

        listed = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "list", "demo"), "graph ontology list"),
            "graph ontology list",
        )
        ensure(
            set(as_dict(listed, "graph ontology list").get("Keys", [])) == {"Person", "Company", "WORKS_AT"},
            "ontology list should include all defined types",
        )

        person_type = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "get", "demo", "Person", "--kind", "object"), "graph ontology get object"),
            "graph ontology get object",
        )
        ensure(
            as_dict(person_type, "graph ontology get object").get("Maybe", {}).get("Object", {}).get("name", {}).get("String") == "Person",
            "graph ontology get object should return Person",
        )

        link_type = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "get", "demo", "WORKS_AT", "--kind", "link"), "graph ontology get link"),
            "graph ontology get link",
        )
        ensure(
            as_dict(link_type, "graph ontology get link").get("Maybe", {}).get("Object", {}).get("name", {}).get("String") == "WORKS_AT",
            "graph ontology get link should return WORKS_AT",
        )

        missing_kind = self.run_process(
            [str(self.binary), "--db", str(db), "-j", "graph", "ontology", "get", "demo", "Person"],
            timeout=10.0,
        )
        ensure(missing_kind.returncode == 2, f"graph ontology get without --kind should exit 2, got {missing_kind.returncode}")

        self.run_shell(db, "graph", "add-node", "demo", "alice", "--object-type", "Person")
        self.run_shell(db, "graph", "add-node", "demo", "bob", "--object-type", "Person")
        self.run_shell(db, "graph", "add-node", "demo", "acme", "--object-type", "Company")
        self.run_shell(db, "graph", "add-edge", "demo", "alice", "acme", "WORKS_AT")
        self.run_shell(db, "graph", "add-edge", "demo", "bob", "acme", "WORKS_AT")

        typed_nodes = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "list-nodes", "demo", "--type", "Person"), "graph list-nodes --type"),
            "graph list-nodes --type",
        )
        ensure(as_dict(typed_nodes, "graph list-nodes --type").get("Keys") == ["alice", "bob"], "typed list-nodes should return only Person nodes")

        page1 = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "list-nodes", "demo", "--limit", "2"), "graph list-nodes page1"),
            "graph list-nodes page1",
        )
        page1_body = as_dict(page1, "graph list-nodes page1").get("GraphPage", {})
        ensure(page1_body.get("items") == ["acme", "alice"], f"unexpected graph page1 items: {page1_body.get('items')!r}")
        cursor = page1_body.get("next_cursor")
        ensure(cursor == "alice", f"unexpected graph page1 cursor: {cursor!r}")

        page2 = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "list-nodes", "demo", "--limit", "2", "--cursor", cursor), "graph list-nodes page2"),
            "graph list-nodes page2",
        )
        ensure(
            as_dict(page2, "graph list-nodes page2").get("GraphPage", {}).get("items") == ["bob"],
            "graph page2 should contain bob",
        )

        bulk = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "graph",
                    "bulk-insert",
                    "demo",
                    "--nodes",
                    '[{"node_id":"isolated-1"},{"node_id":"isolated-2"}]',
                    "--edges",
                    '[{"src":"isolated-1","dst":"isolated-2","edge_type":"LINK"}]',
                ),
                "graph bulk-insert",
            ),
            "graph bulk-insert",
        )
        bulk_body = as_dict(bulk, "graph bulk-insert").get("GraphBulkInsertResult", {})
        ensure(bulk_body.get("nodes_inserted") == 2 and bulk_body.get("edges_inserted") == 1, "graph bulk-insert should insert two nodes and one edge")

        bfs = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "bfs", "demo", "alice"), "graph bfs"),
            "graph bfs",
        )
        ensure(
            as_dict(bfs, "graph bfs").get("GraphBfs", {}).get("visited") == ["alice", "acme"],
            "graph bfs should visit alice then acme",
        )

        wcc = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "analytics", "wcc", "demo"), "graph analytics wcc"),
            "graph analytics wcc",
        )
        ensure(
            as_dict(wcc, "graph analytics wcc").get("GraphGroupSummary", {}).get("group_count") == 2,
            "wcc should report two components",
        )

        cdlp = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "analytics", "cdlp", "demo"), "graph analytics cdlp"),
            "graph analytics cdlp",
        )
        ensure("GraphGroupSummary" in as_dict(cdlp, "graph analytics cdlp"), "cdlp should return GraphGroupSummary")

        pagerank = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "analytics", "pagerank", "demo"), "graph analytics pagerank"),
            "graph analytics pagerank",
        )
        ensure(
            as_dict(pagerank, "graph analytics pagerank").get("GraphScoreSummary", {}).get("node_count") == 5,
            "pagerank should see five nodes after bulk insert",
        )

        lcc = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "analytics", "lcc", "demo"), "graph analytics lcc"),
            "graph analytics lcc",
        )
        ensure("GraphScoreSummary" in as_dict(lcc, "graph analytics lcc"), "lcc should return GraphScoreSummary")

        sssp = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "analytics", "sssp", "demo", "alice"), "graph analytics sssp"),
            "graph analytics sssp",
        )
        farthest = as_dict(sssp, "graph analytics sssp").get("GraphScoreSummary", {}).get("farthest", [])
        ensure(farthest and farthest[0]["node_id"] == "acme", "sssp farthest list should include acme")

        deleted_link = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "delete", "demo", "WORKS_AT", "--kind", "link"), "graph ontology delete link"),
            "graph ontology delete link",
        )
        ensure(deleted_link == "Unit", "ontology delete should succeed before freeze")

        self.run_shell(db, "graph", "ontology", "define", "demo", '{"name":"WORKS_AT","source":"Person","target":"Company"}')
        frozen = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "freeze", "demo"), "graph ontology freeze"),
            "graph ontology freeze",
        )
        ensure(frozen == "Unit", "graph ontology freeze should return Unit")

        delete_after_freeze = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "delete", "demo", "WORKS_AT", "--kind", "link"), "graph ontology delete after freeze"),
            "graph ontology delete after freeze",
        )
        ensure(
            "InvalidInput" in as_dict(delete_after_freeze, "graph ontology delete after freeze"),
            "graph ontology delete after freeze should be refused",
        )

    def test_shell_mixed(self) -> None:
        db = self.case_db("shell-mixed")

        begun = only_doc(
            self.expect_json_docs(self.run_shell(db, "begin"), "begin"),
            "begin",
        )
        ensure(begun == "TxnBegun", f"begin expected TxnBegun, got {begun!r}")

        not_active_commit = only_doc(
            self.expect_json_docs(self.run_shell(db, "commit"), "commit without active txn"),
            "commit without active txn",
        )
        ensure(
            "TransactionNotActive" in as_dict(not_active_commit, "commit without active txn"),
            "commit without active txn should return TransactionNotActive",
        )

        not_active_rollback = only_doc(
            self.expect_json_docs(self.run_shell(db, "rollback"), "rollback without active txn"),
            "rollback without active txn",
        )
        ensure(
            "TransactionNotActive" in as_dict(not_active_rollback, "rollback without active txn"),
            "rollback without active txn should return TransactionNotActive",
        )

        active = only_doc(
            self.expect_json_docs(self.run_shell(db, "txn", "active"), "txn active"),
            "txn active",
        )
        ensure(as_dict(active, "txn active").get("Bool") is False, "txn active should be false in a fresh shell process")

        info = only_doc(
            self.expect_json_docs(self.run_shell(db, "txn", "info"), "txn info"),
            "txn info",
        )
        ensure(as_dict(info, "txn info").get("TxnInfo") is None, "txn info should be null when no txn is active")

        self.run_shell(db, "kv", "put", "search:key", '"alpha world"')
        search_key_ts = as_dict(
            only_doc(self.expect_json_docs(self.run_shell(db, "kv", "history", "search:key"), "search:key history"), "search:key history"),
            "search:key history",
        ).get("VersionHistory", [{}])[0].get("timestamp")
        ensure(isinstance(search_key_ts, int), "search:key history should expose a timestamp")
        wait_until_timestamp_passed(search_key_ts)
        self.run_shell(db, "json", "set", "search:doc", "$", '{"text":"alpha json"}')
        self.run_shell(db, "event", "append", "search.note", '{"text":"alpha event"}')
        self.run_shell(db, "graph", "create", "search-graph")
        self.run_shell(db, "graph", "add-node", "search-graph", "n1", "--properties", '{"text":"alpha graph"}')

        search = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "search", "alpha", timeout=SEARCH_TIMEOUT_SECS),
                "search alpha",
            ),
            "search alpha",
        )
        search_body = as_dict(search, "search alpha").get("SearchResults", {})
        hits = search_body.get("hits", [])
        ensure(len(hits) >= 4, "search alpha should return hits across multiple primitives")
        kinds = {hit["entity_ref"]["kind"] for hit in hits}
        ensure({"kv", "json", "event", "graph"}.issubset(kinds), f"unexpected search kinds: {kinds!r}")

        search_as_of = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "search",
                    "alpha",
                    "--as-of",
                    str(search_key_ts),
                    timeout=SEARCH_TIMEOUT_SECS,
                ),
                "search alpha --as-of",
            ),
            "search alpha --as-of",
        )
        as_of_hits = as_dict(search_as_of, "search alpha --as-of").get("SearchResults", {}).get("hits", [])
        ensure(as_of_hits, "search --as-of should still return earlier hits")
        ensure(len(as_of_hits) < len(hits), f"search --as-of should narrow the hit set, got {len(as_of_hits)} vs {len(hits)}")
        ensure(any(hit["entity_ref"]["kind"] == "kv" for hit in as_of_hits), "search --as-of should include the earlier kv hit")

        search_diff = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "search",
                    "alpha",
                    "--diff-start",
                    "0",
                    "--diff-end",
                    str(search_key_ts),
                    timeout=SEARCH_TIMEOUT_SECS,
                ),
                "search alpha --diff",
            ),
            "search alpha --diff",
        )
        diff = as_dict(search_diff, "search alpha --diff").get("SearchResults", {}).get("diff", {})
        ensure(isinstance(diff, dict), "search --diff should return a diff payload")
        ensure(diff.get("added"), "search --diff should report added hits")

        cfg_list = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "list"), "config list"),
            "config list",
        )
        ensure("Config" in as_dict(cfg_list, "config list"), "config list should return Config")

        cfg_get = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "get", "durability"), "config get durability"),
            "config get durability",
        )
        ensure(as_dict(cfg_get, "config get durability").get("ConfigValue") == "standard", "default durability should be standard")

        cfg_set = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "set", "durability", "always"), "config set durability always"),
            "config set durability always",
        )
        ensure(
            as_dict(cfg_set, "config set durability always").get("ConfigSetResult", {}).get("new_value") == "always",
            "config set durability always did not stick",
        )

        cfg_invalid = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "set", "durability", "sync"), "config set invalid durability"),
            "config set invalid durability",
        )
        ensure("InvalidInput" in as_dict(cfg_invalid, "config set invalid durability"), "invalid durability should return InvalidInput")

        configured = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "configure-model", "http://localhost:11434/v1", "qwen3:1.7b"),
                "configure-model",
            ),
            "configure-model",
        )
        ensure(configured == "Unit", f"configure-model expected Unit, got {configured!r}")

        cfg_list_after = only_doc(
            self.expect_json_docs(self.run_shell(db, "config", "list"), "config list after configure-model"),
            "config list after configure-model",
        )
        model_cfg = as_dict(cfg_list_after, "config list after configure-model").get("Config", {}).get("model", {})
        ensure(model_cfg.get("model") == "qwen3:1.7b", "configure-model did not update config")

        recipe_show = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "show"), "recipe show"),
            "recipe show",
        )
        ensure("Maybe" in as_dict(recipe_show, "recipe show"), "recipe show should return Maybe")

        seeded = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "seed"), "recipe seed"),
            "recipe seed",
        )
        ensure(seeded == "Unit", "recipe seed should return Unit")

        seeded_again = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "seed"), "recipe seed again"),
            "recipe seed again",
        )
        ensure(seeded_again == "Unit", "recipe seed should be idempotent")

        recipe_list = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "list"), "recipe list"),
            "recipe list",
        )
        recipe_names = set(as_dict(recipe_list, "recipe list").get("Keys", []))
        ensure({"default", "keyword", "semantic"}.issubset(recipe_names), "recipe list should include seeded built-ins")

        recipe_set = only_doc(
            self.expect_json_docs(
                self.run_shell(
                    db,
                    "recipe",
                    "set",
                    "--name",
                    "smoke",
                    '{"version":1,"retrieve":{"bm25":{"k":10},"vector":{}},"expansion":{"strategy":"none"},"fusion":{"method":"rrf","k":60},"rerank":{"top_n":5,"min_candidates":1},"transform":{"limit":5},"control":{"budget_ms":1000}}',
                ),
                "recipe set smoke",
            ),
            "recipe set smoke",
        )
        ensure(recipe_set == "Unit", f"recipe set smoke expected Unit, got {recipe_set!r}")

        recipe_get = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "get", "smoke"), "recipe get smoke"),
            "recipe get smoke",
        )
        recipe_payload = as_dict(recipe_get, "recipe get smoke").get("Maybe", {}).get("String", "")
        ensure('"strategy":"none"' in recipe_payload, "recipe get smoke returned the wrong recipe")

        recipe_delete = only_doc(
            self.expect_json_docs(self.run_shell(db, "recipe", "delete", "smoke"), "recipe delete smoke"),
            "recipe delete smoke",
        )
        ensure(recipe_delete == "Unit", f"recipe delete smoke expected Unit, got {recipe_delete!r}")

        self.run_shell(db, "kv", "put", "export-key", "1")
        exported = only_doc(
            self.expect_json_docs(self.run_shell(db, "export", "kv", "--prefix", "export-"), "export kv"),
            "export kv",
        )
        export_body = as_dict(exported, "export kv").get("Exported", {})
        ensure(export_body.get("row_count") == 1, "export kv should report one row")
        ensure('"key": "export-key"' in export_body.get("data", ""), "export kv data did not contain the exported key")

        export_file = db.parent / "kv.jsonl"
        exported_file = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "export", "kv", "--format", "jsonl", "--path", str(export_file), "--prefix", "export-"),
                "export kv file",
            ),
            "export kv file",
        )
        exported_file_body = as_dict(exported_file, "export kv file").get("Exported", {})
        ensure(exported_file_body.get("path") == str(export_file), "export kv file should report the output path")
        ensure(export_file.exists(), "export kv file should create the export file on disk")

        sample_file = db.parent / "import.jsonl"
        sample_file.write_text('{"key":"a","value":1}\n', encoding="utf-8")
        imported = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "import", str(sample_file), "kv"),
                "import kv",
            ),
            "import kv",
        )
        imported_body = as_dict(imported, "import kv")
        if "ArrowImported" in imported_body:
            import_result = imported_body["ArrowImported"]
            ensure(import_result.get("rows_imported") == 1, "import kv should report one imported row when arrow is enabled")
            imported_kv = only_doc(
                self.expect_json_docs(self.run_shell(db, "kv", "get", "a"), "imported kv get a"),
                "imported kv get a",
            )
            ensure(
                unwrap_value(unwrap_maybe_versioned(imported_kv, "imported kv get a")["value"], "Int", "imported kv get a") == 1,
                "import kv should write the imported value when arrow is enabled",
            )
        else:
            # When the binary is built without the `arrow` feature, the
            # executor surfaces this as a user-facing InvalidInput rather
            # than an engine Internal — feature-not-compiled is a build
            # configuration error, not an invariant violation.
            invalid = require_key(imported_body, "InvalidInput", "import kv")
            ensure(isinstance(invalid, dict), "import kv: InvalidInput payload should be an object")
            reason = require_key(invalid, "reason", "import kv")
            ensure(isinstance(reason, str), "import kv: InvalidInput.reason should be a string")
            ensure("arrow" in reason.lower(), "import failure should mention the arrow feature")

    def test_shell_open_modes(self) -> None:
        db = self.case_db("shell-open-modes")

        self.run_shell(db, "kv", "put", "user:1", "1")

        read_only_put = self.run_process([str(self.binary), "--db", str(db), "--read-only", "-j", "kv", "put", "user:2", "2"])
        ensure(read_only_put.returncode == 1, f"read-only put should exit 1, got {read_only_put.returncode}")
        read_only_doc = only_doc(self.expect_json_docs(read_only_put, "read-only kv put"), "read-only kv put")
        ensure("AccessDenied" in as_dict(read_only_doc, "read-only kv put"), "read-only kv put should return AccessDenied")

        follower_get = self.run_process([str(self.binary), "--db", str(db), "--follower", "-j", "kv", "get", "user:1"])
        ensure(follower_get.returncode == 0, f"follower get should succeed, got {follower_get.returncode}")
        follower_doc = only_doc(self.expect_json_docs(follower_get, "follower kv get"), "follower kv get")
        ensure(
            as_dict(follower_doc, "follower kv get").get("MaybeVersioned", {}).get("value", {}).get("Int") == 1,
            "follower kv get should see committed values",
        )

        follower_put = self.run_process([str(self.binary), "--db", str(db), "--follower", "-j", "kv", "put", "user:2", "2"])
        ensure(follower_put.returncode == 1, f"follower put should exit 1, got {follower_put.returncode}")
        follower_put_doc = only_doc(self.expect_json_docs(follower_put, "follower kv put"), "follower kv put")
        ensure("AccessDenied" in as_dict(follower_put_doc, "follower kv put"), "follower kv put should return AccessDenied")

        raw_txn = self.run_shell(db, "txn", "active", json_mode=False, raw_mode=True)
        ensure(raw_txn.returncode == 0, f"raw txn active failed: {raw_txn.combined}")
        ensure(raw_txn.stdout.strip() == "0", f"raw txn active expected '0', got {raw_txn.stdout.strip()!r}")

        human_txn = self.run_shell_human(db, "txn", "active")
        ensure(human_txn.returncode == 0, f"human txn active failed: {human_txn.combined}")
        ensure(human_txn.stdout.strip() == "false", f"human txn active expected 'false', got {human_txn.stdout.strip()!r}")

        raw_event_db = self.case_db("shell-open-modes-event")
        self.run_shell(raw_event_db, "event", "append", "note", '{"id":1}')
        raw_event_len = self.run_shell(raw_event_db, "event", "len", json_mode=False, raw_mode=True)
        ensure(raw_event_len.returncode == 0, f"raw event len failed: {raw_event_len.combined}")
        ensure(raw_event_len.stdout.strip() == "1", f"raw event len expected '1', got {raw_event_len.stdout.strip()!r}")

    def test_shell_init(self) -> None:
        db = self.case_db("shell-init")
        result = self.run_process(
            [str(self.binary), "init", "--db", str(db), "--non-interactive"],
            timeout=20.0,
        )
        ensure(result.returncode == 0, f"init should succeed, got {result.returncode}\n{result.combined}")
        text = result.stdout or result.stderr
        ensure("Database ready" in text, "init output should confirm database setup")
        ensure(db.exists(), "init should create the database directory")
        ping_after_init = only_doc(
            self.expect_json_docs(self.run_shell(db, "ping"), "ping after init"),
            "ping after init",
        )
        ensure("Pong" in as_dict(ping_after_init, "ping after init"), "initialized database should be openable")

    def test_shell_local_admin(self) -> None:
        db = self.case_db("shell-local-admin")
        init = self.run_process([str(self.binary), "init", "--db", str(db), "--non-interactive"], timeout=20.0)
        ensure(init.returncode == 0, f"local admin init should succeed: {init.combined}")
        try:
            up = self.run_process([str(self.binary), "up", "--db", str(db)], timeout=20.0)
            ensure(up.returncode == 0, f"up should succeed: {up.combined}")
            ensure((db / "strata.pid").exists(), "up should create a pid file")
            ensure((db / "strata.sock").exists(), "up should create a socket file")

            uninstall = self.run_process([str(self.binary), "uninstall", "--db", str(db), "-y"], timeout=20.0)
            ensure(uninstall.returncode == 2, f"uninstall --db should fail fast with usage error, got {uninstall.returncode}")
            ensure("--db is not supported" in uninstall.combined, "uninstall --db should explain the unsupported flag")
        finally:
            down = self.run_process([str(self.binary), "down", "--db", str(db)], timeout=20.0)
            ensure(down.returncode == 0, f"down should succeed: {down.combined}")
            ensure(not (db / "strata.pid").exists(), "down should remove the pid file")
            ensure(not (db / "strata.sock").exists(), "down should remove the socket file")

    def test_shell_guardrails(self) -> None:
        db = self.case_db("shell-guardrails")
        self.run_shell(db, "graph", "create", "demo")

        # These usage-error assertions intentionally match clap's current text so
        # parser/help regressions surface here. If clap wording changes, update
        # the needles rather than weakening the guardrail coverage.
        missing_required = self.run_process([str(self.binary), "--db", str(db), "-j", "kv", "get"], timeout=10.0)
        ensure(missing_required.returncode == 2, f"kv get without key should exit 2, got {missing_required.returncode}")
        ensure("required arguments were not provided" in missing_required.stderr, "kv get without key should report missing args")

        malformed_json = self.run_process(
            [str(self.binary), "--db", str(db), "-j", "json", "set", "broken", "$", "{not-json}"],
            timeout=10.0,
        )
        ensure(malformed_json.returncode == 2, f"json set with malformed JSON should exit 2, got {malformed_json.returncode}")
        ensure("Invalid JSON" in malformed_json.stderr, "json set with malformed JSON should explain the parse error")

        unknown_branch = self.run_process(
            [str(self.binary), "--db", str(db), "-j", "--branch", "missing", "kv", "get", "x"],
            timeout=10.0,
        )
        ensure(unknown_branch.returncode == 1, f"--branch missing should exit 1, got {unknown_branch.returncode}")
        ensure("Branch not found" in unknown_branch.stderr, "unknown branch should be surfaced clearly")

        invalid_kind = self.run_process(
            [str(self.binary), "--db", str(db), "-j", "graph", "ontology", "list", "demo", "--kind", "weird"],
            timeout=10.0,
        )
        ensure(invalid_kind.returncode == 2, f"graph ontology list --kind weird should exit 2, got {invalid_kind.returncode}")
        ensure("invalid value 'weird'" in invalid_kind.stderr, "invalid kind should mention the invalid enum value")

        missing_kind = self.run_process(
            [str(self.binary), "--db", str(db), "-j", "graph", "ontology", "get", "demo", "Person"],
            timeout=10.0,
        )
        ensure(missing_kind.returncode == 2, f"graph ontology get without --kind should exit 2, got {missing_kind.returncode}")
        ensure("--kind <kind>" in missing_kind.stderr, "graph ontology get should require --kind")

        protected_branch = only_doc(
            self.expect_json_docs(self.run_process([str(self.binary), "--db", str(db), "-j", "branch", "del", "default"], timeout=10.0), "branch del default"),
            "branch del default",
        )
        ensure("ConstraintViolation" in as_dict(protected_branch, "branch del default"), "branch del default should be refused")

        protected_space = only_doc(
            self.expect_json_docs(self.run_process([str(self.binary), "--db", str(db), "-j", "space", "del", "default"], timeout=10.0), "space del default"),
            "space del default",
        )
        ensure("ConstraintViolation" in as_dict(protected_space, "space del default"), "space del default should be refused")

        human_error = self.run_shell_human(db, "branch", "del", "missing")
        ensure(human_error.returncode == 1, f"human branch del missing should exit 1, got {human_error.returncode}")
        ensure("branch not found" in human_error.stderr.lower(), "human branch del missing should print a readable error")

    def test_shell_feature_gated_failures(self) -> None:
        db = self.case_db("shell-feature-gates")

        cases = [
            {
                "argv": ("embed", "hello"),
                "disabled_needle": "embed",
                "enabled_ok": lambda doc, _reason: "Embedding" in as_dict(doc, "embed hello"),
                "enabled_msg": "embed hello should return an Embedding payload when embed is enabled",
            },
            {
                "argv": ("models", "list"),
                "disabled_needle": "model management",
                "enabled_ok": lambda doc, _reason: "ModelsList" in as_dict(doc, "models list"),
                "enabled_msg": "models list should return ModelsList when embed is enabled",
            },
            {
                "argv": ("models", "local"),
                "disabled_needle": "model management",
                "enabled_ok": lambda doc, _reason: "ModelsList" in as_dict(doc, "models local"),
                "enabled_msg": "models local should return ModelsList when embed is enabled",
            },
            {
                "argv": ("models", "pull", "foo"),
                "disabled_needle": "model management",
                "enabled_ok": lambda doc, reason: "unknown model" in reason.lower(),
                "enabled_msg": "models pull foo should fail because the model is unknown, not because the feature is missing",
            },
            {
                "argv": ("generate", "model", "prompt"),
                "disabled_needle": "generation",
                "enabled_ok": lambda doc, reason: "unknown model" in reason.lower(),
                "enabled_msg": "generate model prompt should fail because the model is unknown, not because generation is unavailable",
            },
            {
                "argv": ("tokenize", "model", "text"),
                "disabled_needle": "generation",
                "enabled_ok": lambda doc, reason: "unknown model" in reason.lower(),
                "enabled_msg": "tokenize model text should fail because the model is unknown, not because generation is unavailable",
            },
            {
                "argv": ("detokenize", "model", "1", "2"),
                "disabled_needle": "generation",
                "enabled_ok": lambda doc, reason: "unknown model" in reason.lower(),
                "enabled_msg": "detokenize model 1 2 should fail because the model is unknown, not because generation is unavailable",
            },
        ]

        for case in cases:
            argv = case["argv"]
            label = " ".join(argv)
            result = self.run_shell(db, *argv, timeout=SEARCH_TIMEOUT_SECS)
            doc = only_doc(self.expect_json_docs(result, label), label)

            if result.returncode == 0:
                ensure(case["enabled_ok"](doc, ""), case["enabled_msg"])
                continue

            reason = internal_reason(doc, label)
            reason_lower = reason.lower()
            disabled_needle = case["disabled_needle"]
            feature_gated = "compile with --features embed" in reason_lower or "not enabled" in reason_lower

            if feature_gated:
                ensure(
                    disabled_needle in reason_lower,
                    f"{label} did not mention {disabled_needle!r} in its feature-gated failure reason",
                )
                continue

            ensure(case["enabled_ok"](doc, reason), case["enabled_msg"])

    def test_hostile_input(self) -> None:
        db = self.case_db("hostile-input")
        self.run_shell(db, "vector", "create", "docs", "3")

        cases = [
            {
                "label": "json set malformed payload",
                "result": self.run_process(
                    [str(self.binary), "--db", str(db), "-j", "json", "set", "bad", "$", '{"name":']
                ),
                "kind": "usage",
                "needles": ("invalid json",),
            },
            {
                "label": "global --db with --cache",
                "result": self.run_process(
                    [str(self.binary), "--db", str(db), "--cache", "-j", "ping"]
                ),
                "kind": "usage",
                "needles": ("cannot be used with", "usage:"),
            },
            {
                "label": "import missing file",
                "result": self.run_shell(db, "import", str(db.parent / "missing.jsonl"), "kv"),
                "kind": "typed",
                "variants": ("InvalidInput",),
                "reason_needles": ("file not found",),
            },
            {
                "label": "vector upsert dimension mismatch",
                "result": self.run_shell(db, "vector", "upsert", "docs", "bad", "[1,2]"),
                "kind": "typed",
                "variants": ("DimensionMismatch",),
            },
            {
                "label": "reserved branch name",
                "result": self.run_shell(db, "branch", "create", "_system_"),
                "kind": "typed",
                "variants": ("InvalidInput",),
                "reason_needles": ("reserved", "cannot start with '_'"),
            },
        ]

        for case in cases:
            result = case["result"]
            if case["kind"] == "usage":
                self.assert_usage_failure(result, case["label"], *case["needles"])
            else:
                self.assert_typed_error(
                    result,
                    case["label"],
                    *case["variants"],
                    reason_needles=case.get("reason_needles", ()),
                )

    def test_illegal_sequences(self) -> None:
        db = self.case_db("illegal-sequences")

        pipe = self.run_pipe(
            db,
            [
                "begin",
                "begin",
                "kv put after 9",
                "commit",
                "quit",
            ],
        )
        self.assert_no_panic(pipe, "pipe double begin")
        ensure(pipe.returncode == 1, "double begin should fail the pipe process overall")
        pipe_docs = parse_json_stream(pipe.stdout)
        pipe_err_docs = parse_json_stream(pipe.stderr)
        ensure(pipe_docs[0] == "TxnBegun", "first begin should start a transaction")
        ensure(
            len(pipe_err_docs) == 1 and "TransactionAlreadyActive" in as_dict(pipe_err_docs[0], "pipe second begin"),
            "second begin should return TransactionAlreadyActive",
        )
        ensure(
            as_dict(pipe_docs[1], "pipe recovery put").get("WriteResult", {}).get("key") == "after",
            "pipe session should continue after double begin refusal",
        )
        ensure(
            as_dict(pipe_docs[2], "pipe commit after refusal").get("TxnCommitted", {}).get("version") is not None,
            "pipe session should still commit after a refusal",
        )
        after = only_doc(
            self.expect_json_docs(self.run_shell(db, "kv", "get", "after"), "kv get after illegal pipe sequence"),
            "kv get after illegal pipe sequence",
        )
        ensure(
            unwrap_value(unwrap_maybe_versioned(after, "kv get after")["value"], "Int", "kv get after") == 9,
            "committed write after pipe refusal should persist",
        )

        repl = ReplSession(self.binary, db)
        try:
            output, prompt = repl.run("branch del default")
            ensure(prompt == "strata:default/default> ", f"unexpected prompt after branch del default refusal: {prompt!r}")
            repl_docs = parse_json_stream(output)
            ensure(
                "ConstraintViolation" in as_dict(only_doc(repl_docs, "repl branch del default"), "repl branch del default"),
                "repl branch del default should return ConstraintViolation",
            )

            output, _ = repl.run("kv put repl-survived 7")
            docs = parse_json_stream(output)
            ensure(
                as_dict(only_doc(docs, "repl survived put"), "repl survived put").get("WriteResult", {}).get("key") == "repl-survived",
                "repl should accept a valid write after a refusal",
            )

            output, _ = repl.run("kv get repl-survived")
            docs = parse_json_stream(output)
            ensure(
                unwrap_value(
                    unwrap_maybe_versioned(only_doc(docs, "repl survived get"), "repl survived get")["value"],
                    "Int",
                    "repl survived get",
                )
                == 7,
                "repl should remain usable after a refusal",
            )
        finally:
            repl.close()

        self.run_shell(db, "graph", "create", "frozen-demo")
        self.run_shell(db, "graph", "ontology", "define", "frozen-demo", '{"name":"Person","fields":{}}')
        self.run_shell(db, "graph", "ontology", "freeze", "frozen-demo")
        frozen_define = self.run_shell(
            db,
            "graph",
            "ontology",
            "define",
            "frozen-demo",
            '{"name":"Company","fields":{}}',
        )
        self.assert_typed_error(
            frozen_define,
            "graph ontology define after freeze",
            "InvalidInput",
            reason_needles=("frozen",),
        )
        frozen_status = only_doc(
            self.expect_json_docs(self.run_shell(db, "graph", "ontology", "status", "frozen-demo"), "frozen ontology status"),
            "frozen ontology status",
        )
        ensure(
            as_dict(frozen_status, "frozen ontology status").get("Maybe", {}).get("String") == "frozen",
            "frozen ontology should remain queryable after a refused define",
        )

    def test_concurrency_conflict(self) -> None:
        db = self.case_db("concurrency-conflict")
        self.run_shell(db, "kv", "put", "seed", "1")

        holder = subprocess.Popen(
            [str(self.binary), "--db", str(db), "-j"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert holder.stdin is not None
            holder.stdin.write("begin\n")
            holder.stdin.flush()
            self.wait_for_live_process_stdout(
                holder,
                "TxnBegun",
                timeout=10.0,
                context="holder begin for writer-lock test",
            )

            writer = self.run_process([str(self.binary), "--db", str(db), "-j", "kv", "put", "raced", "2"])
            self.assert_open_lock_refusal(writer, "second writer while first writer holds the db")

            follower = self.run_process(
                [str(self.binary), "--db", str(db), "--follower", "-j", "kv", "get", "seed"]
            )
            ensure(follower.returncode == 0, f"follower get should succeed during writer lock, got {follower.returncode}")
            follower_doc = only_doc(self.expect_json_docs(follower, "follower get during writer lock"), "follower get during writer lock")
            ensure(
                unwrap_value(
                    unwrap_maybe_versioned(follower_doc, "follower get during writer lock")["value"],
                    "Int",
                    "follower get during writer lock",
                )
                == 1,
                "follower should observe committed state while a writer holds the db",
            )
        finally:
            try:
                if holder.poll() is None and holder.stdin is not None:
                    holder.stdin.write("quit\n")
                    holder.stdin.flush()
                holder.communicate(timeout=2.0)
            except Exception:
                holder.kill()

        daemon_db = self.case_db("concurrency-conflict-daemon")
        self.run_shell_human(daemon_db, "init", "--non-interactive", timeout=20.0)
        up_result = self.run_shell_human(daemon_db, "up", timeout=20.0)
        ensure(up_result.returncode == 0, f"daemon up should succeed:\n{up_result.combined}")
        try:
            ping = self.run_shell(daemon_db, "ping", timeout=10.0)
            ensure(ping.returncode == 0, f"ping against daemon-backed db should succeed:\n{ping.combined}")
            pong = only_doc(self.expect_json_docs(ping, "daemon-backed ping"), "daemon-backed ping")
            ensure("Pong" in as_dict(pong, "daemon-backed ping"), "daemon-backed ping should return Pong")

            pid_file = daemon_db / "strata.pid"
            ensure(pid_file.exists(), f"daemon pid file missing at {pid_file}")
            daemon_pid = int(pid_file.read_text().strip())
            os.kill(daemon_pid, signal.SIGKILL)
            time.sleep(0.2)

            restarted = self.run_shell_human(daemon_db, "up", timeout=20.0)
            ensure(restarted.returncode == 0, f"daemon restart after SIGKILL should succeed:\n{restarted.combined}")
        finally:
            down_result = self.run_shell_human(daemon_db, "down", timeout=20.0)
            ensure(
                down_result.returncode == 0,
                f"daemon down should succeed during concurrency cleanup:\n{down_result.combined}",
            )

    def test_crash_recovery(self) -> None:
        db_uncommitted = self.case_db("crash-recovery-uncommitted")
        crashed_uncommitted = self.run_with_sigkill(
            [str(self.binary), "--db", str(db_uncommitted), "-j"],
            input_lines=["begin", "kv put crash:key 41"],
            wait_for_stdout="WriteResult",
            kill_after=0.05,
        )
        self.assert_no_panic(crashed_uncommitted, "sigkill during uncommitted transaction")
        missing = only_doc(
            self.expect_json_docs(self.run_shell(db_uncommitted, "kv", "get", "crash:key"), "kv get crash:key after sigkill"),
            "kv get crash:key after sigkill",
        )
        ensure(
            as_dict(missing, "kv get crash:key after sigkill").get("MaybeVersioned") is None,
            "uncommitted write should not survive SIGKILL",
        )

        db_committed = self.case_db("crash-recovery-committed")
        crashed_committed = self.run_with_sigkill(
            [str(self.binary), "--db", str(db_committed), "-j"],
            input_lines=["begin", "kv put live:key 99", "commit"],
            wait_for_stdout="TxnCommitted",
            kill_after=0.05,
        )
        self.assert_no_panic(crashed_committed, "sigkill after commit")
        present = only_doc(
            self.expect_json_docs(self.run_shell(db_committed, "kv", "get", "live:key"), "kv get live:key after sigkill"),
            "kv get live:key after sigkill",
        )
        ensure(
            unwrap_value(
                unwrap_maybe_versioned(present, "kv get live:key after sigkill")["value"],
                "Int",
                "kv get live:key after sigkill",
            )
            == 99,
            "committed write should survive SIGKILL",
        )

        bundle_db = self.case_db("crash-recovery-bundle")
        bundle = bundle_db.parent / "feature.bundle"
        self.run_shell(bundle_db, "branch", "fork", "feature")
        self.run_shell(bundle_db, "branch", "export", "feature", str(bundle))
        raw = bundle.read_bytes()

        trunc_bundle = bundle_db.parent / "feature-trunc.bundle"
        trunc_bundle.write_bytes(raw[: len(raw) // 2])
        flip_bundle = bundle_db.parent / "feature-flip.bundle"
        flip_bundle.write_bytes(raw[:10] + bytes([raw[10] ^ 0xFF]) + raw[11:])

        self.assert_typed_error(
            self.run_shell(bundle_db, "branch", "validate", str(trunc_bundle)),
            "branch validate truncated bundle",
            "Io",
            reason_needles=("incomplete frame",),
        )
        self.assert_typed_error(
            self.run_shell(bundle_db.parent / "feature-import-db", "branch", "import", str(flip_bundle)),
            "branch import byte-flipped bundle",
            "Io",
            reason_needles=("data corruption",),
        )

    def test_scale_stress(self) -> None:
        db = self.case_db("scale-stress")
        total_keys = 10_000
        lines = [f"kv put bulk:{i:04d} {i}" for i in range(total_keys)] + ["quit"]
        pipe = self.run_pipe(db, lines, timeout=60.0)
        ensure(pipe.returncode == 0, f"scale pipe load should succeed, got {pipe.returncode}\nstderr:\n{pipe.stderr}")
        docs = self.expect_json_docs(pipe, "scale kv load")
        ensure(len(docs) == total_keys, f"scale kv load should emit {total_keys} results, got {len(docs)}")

        seen: list[str] = []
        cursor: str | None = None
        while True:
            args = ["kv", "list", "--prefix", "bulk:", "--limit", "100"]
            if cursor is not None:
                args += ["--cursor", cursor]
            page = only_doc(
                self.expect_json_docs(self.run_shell(db, *args, timeout=20.0), f"scale kv list {cursor or '<start>'}"),
                f"scale kv list {cursor or '<start>'}",
            )
            page_body = as_dict(page, "scale kv list").get("KeysPage", {})
            keys = page_body.get("keys", [])
            seen.extend(keys)
            if not page_body.get("has_more"):
                break
            cursor = page_body.get("cursor")
            ensure(isinstance(cursor, str) and cursor, "scale kv list should expose a cursor while has_more is true")

        expected = [f"bulk:{i:04d}" for i in range(total_keys)]
        ensure(seen == expected, "scale kv pagination should walk the full ordered keyset exactly once")

        self.run_shell(db, "vector", "create", "ranked", "3")
        vector_entries = json.dumps(
            [
                {"key": f"doc{i:03d}", "vector": [1.0, i / 100.0, 0.0]}
                for i in range(100)
            ]
        )
        batch = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "vector", "batch-upsert", "ranked", vector_entries, timeout=20.0),
                "scale vector batch-upsert",
            ),
            "scale vector batch-upsert",
        )
        versions = as_dict(batch, "scale vector batch-upsert").get("Versions", [])
        ensure(len(versions) == 100, "scale vector batch-upsert should return one version per inserted vector")

        ranked = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "vector", "search", "ranked", "[1.0,0.0,0.0]", timeout=20.0),
                "scale vector search",
            ),
            "scale vector search",
        )
        matches = as_dict(ranked, "scale vector search").get("VectorMatches", [])
        top_three = [match["key"] for match in matches[:3]]
        ensure(top_three == ["doc000", "doc001", "doc002"], f"unexpected vector ranking order: {top_three!r}")

        graph_name = "stress-graph"
        self.run_shell(db, "graph", "create", graph_name, timeout=20.0)
        node_count = 2_000
        graph_lines = [f"graph add-node {graph_name} n{i:04d}" for i in range(node_count)]
        graph_lines += [
            f"graph add-edge {graph_name} n{i:04d} n{i + 1:04d} link"
            for i in range(node_count - 1)
        ]
        graph_lines.append("quit")
        graph_load = self.run_pipe(db, graph_lines, timeout=120.0)
        ensure(graph_load.returncode == 0, f"scale graph load should succeed, got {graph_load.returncode}\nstderr:\n{graph_load.stderr}")

        bfs = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "graph", "bfs", graph_name, "n0000", "--max-depth", "5", timeout=30.0),
                "scale graph bfs",
            ),
            "scale graph bfs",
        )
        bfs_body = as_dict(bfs, "scale graph bfs").get("GraphBfs", {})
        visited = bfs_body.get("visited", [])
        ensure(isinstance(visited, list) and visited, "scale graph bfs should return visited nodes")
        ensure(visited[0] == "n0000", f"scale graph bfs should start from n0000, got {visited[:3]!r}")

        pagerank = only_doc(
            self.expect_json_docs(
                self.run_shell(db, "graph", "analytics", "pagerank", graph_name, timeout=30.0),
                "scale graph pagerank",
            ),
            "scale graph pagerank",
        )
        pagerank_body = as_dict(pagerank, "scale graph pagerank").get("GraphScoreSummary", {})
        ensure(
            pagerank_body.get("node_count") == node_count,
            f"scale graph pagerank should report {node_count} nodes, got {pagerank_body!r}",
        )

    def test_pipe_mode(self) -> None:
        db = self.case_db("pipe-mode")
        lines = [
            "# comment should be ignored",
            "",
            "ping",
            "space create analytics",
            "branch create feature",
            "use feature analytics",
            "begin",
            "txn active",
            "durability-counters",
            "kv put pipe-key 11",
            "kv get pipe-key",
            "rollback",
            "txn active",
            "begin",
            "kv put pipe-key 11",
            "commit",
            "json set pipe-doc '$' '{\"ok\":true}'",
            "json get pipe-doc $",
            "event append user.created '{\"id\":7}'",
            "event len",
            "durability-counters",
            "quit",
        ]
        result = self.run_pipe(db, lines)
        ensure(result.returncode == 0, f"pipe mode should succeed, got {result.returncode}\nstderr:\n{result.stderr}")
        docs = self.expect_json_docs(result, "pipe mode")
        ensure(len(docs) >= 18, f"pipe mode expected multiple JSON outputs, got {len(docs)}")
        ensure("Pong" in docs[0] and "version" in docs[0]["Pong"], "pipe ping did not return Pong")
        ensure(docs[1] == "Unit", "pipe space create should return Unit")
        ensure(as_dict(docs[2], "pipe branch create").get("BranchWithVersion", {}).get("info", {}).get("id") == "feature", "pipe branch create failed")
        ensure(docs[3] == "TxnBegun", "pipe begin did not begin a transaction")
        ensure(as_dict(docs[4], "pipe txn active").get("Bool") is True, "txn active should be true inside pipe transaction")
        counters_before = as_dict(docs[5], "pipe durability-counters").get("DurabilityCounters", {})
        ensure(isinstance(counters_before.get("wal_appends"), int), "pipe durability-counters should expose wal_appends")
        ensure(as_dict(docs[6], "pipe kv put").get("WriteResult", {}).get("key") == "pipe-key", "pipe kv put failed")
        ensure(as_dict(docs[7], "pipe kv get").get("Maybe", {}).get("Int") == 11, "pipe kv get returned wrong value")
        ensure(docs[8] == "TxnAborted", f"pipe rollback expected TxnAborted, got {docs[8]!r}")
        ensure(as_dict(docs[9], "pipe txn active after rollback").get("Bool") is False, "txn active should be false after rollback")
        ensure(docs[10] == "TxnBegun", "second pipe begin did not begin a transaction")
        ensure(as_dict(docs[11], "pipe second kv put").get("WriteResult", {}).get("key") == "pipe-key", "pipe second kv put failed")
        ensure(as_dict(docs[12], "pipe commit").get("TxnCommitted", {}).get("version") is not None, "pipe commit failed")
        ensure(as_dict(docs[13], "pipe json set").get("WriteResult", {}).get("key") == "pipe-doc", "pipe json set failed")
        ensure(unwrap_value(require_key(unwrap_value(unwrap_maybe_versioned(docs[14], "pipe json get")["value"], "Object", "pipe json get"), "ok", "pipe json get"), "Bool", "pipe json get ok") is True, "pipe json get returned wrong value")
        ensure(as_dict(docs[15], "pipe event append").get("EventAppendResult", {}).get("sequence") == 0, "pipe event append sequence should be 0")
        ensure(as_dict(docs[16], "pipe event len").get("Uint") == 1, "pipe event len should be 1")
        counters_after = as_dict(docs[17], "pipe durability-counters after writes").get("DurabilityCounters", {})
        ensure(isinstance(counters_after.get("wal_appends"), int), "pipe durability-counters after writes should expose wal_appends")
        ensure(
            counters_after["wal_appends"] > counters_before["wal_appends"],
            "pipe durability counters should advance after writes in one process",
        )

    def test_pipe_mode_failure_recovery(self) -> None:
        db = self.case_db("pipe-mode-failure-recovery")
        lines = [
            "use missing",
            "kv put recovered 5",
            "kv get recovered",
            "quit",
        ]
        result = self.run_pipe(db, lines)
        ensure(result.returncode == 1, f"pipe mode should report failure when a command fails, got {result.returncode}")
        ensure("Branch not found" in result.stderr, "pipe mode failure should mention the missing branch")
        docs = self.expect_json_docs(result, "pipe mode failure recovery")
        ensure(len(docs) == 2, f"pipe mode failure recovery expected 2 JSON docs, got {len(docs)}")
        ensure(as_dict(docs[0], "pipe recovery kv put").get("WriteResult", {}).get("key") == "recovered", "pipe mode should continue after a failure")
        ensure(
            unwrap_value(unwrap_maybe_versioned(docs[1], "pipe recovery kv get")["value"], "Int", "pipe recovery kv get") == 5,
            "pipe recovery kv get returned wrong value",
        )

    def test_repl_mode(self) -> None:
        db = self.case_db("repl-mode")
        repl = ReplSession(self.binary, db)
        try:
            ensure(repl.prompt == "strata:default/default> ", f"unexpected initial prompt: {repl.prompt!r}")

            output, _ = repl.run("help kv")
            ensure("kv" in output and "Usage:" in output, f"repl help should render command help, got {output!r}")

            output, prompt = repl.run("clear")
            ensure(output == "", f"repl clear should not render output, got {output!r}")
            ensure(prompt == "strata:default/default> ", f"clear should preserve the prompt, got {prompt!r}")

            output, _ = repl.run("space create analytics")
            docs = parse_json_stream(output)
            ensure(only_doc(docs, "repl space create") == "Unit", "repl space create failed")

            output, prompt = repl.run("begin")
            docs = parse_json_stream(output)
            ensure(only_doc(docs, "repl begin") == "TxnBegun", "repl begin did not return TxnBegun")
            ensure(prompt == "strata:default/default(txn)> ", f"unexpected txn prompt: {prompt!r}")

            output, prompt = repl.run("kv put repl-key 7")
            docs = parse_json_stream(output)
            ensure(as_dict(only_doc(docs, "repl kv put"), "repl kv put").get("WriteResult", {}).get("key") == "repl-key", "repl kv put failed")
            ensure(prompt == "strata:default/default(txn)> ", "txn prompt should stay active after kv put")

            output, prompt = repl.run("commit")
            docs = parse_json_stream(output)
            ensure("TxnCommitted" in as_dict(only_doc(docs, "repl commit"), "repl commit"), "repl commit failed")
            ensure(prompt == "strata:default/default> ", f"unexpected prompt after commit: {prompt!r}")

            output, prompt = repl.run("begin")
            docs = parse_json_stream(output)
            ensure(only_doc(docs, "repl second begin") == "TxnBegun", "repl second begin did not return TxnBegun")
            ensure(prompt == "strata:default/default(txn)> ", f"unexpected txn prompt on second begin: {prompt!r}")

            output, _ = repl.run("kv put throwaway 8")
            docs = parse_json_stream(output)
            ensure(as_dict(only_doc(docs, "repl throwaway kv put"), "repl throwaway kv put").get("WriteResult", {}).get("key") == "throwaway", "repl throwaway kv put failed")

            output, prompt = repl.run("rollback")
            docs = parse_json_stream(output)
            ensure(only_doc(docs, "repl rollback") == "TxnAborted", "repl rollback should return TxnAborted")
            ensure(prompt == "strata:default/default> ", f"unexpected prompt after rollback: {prompt!r}")

            output, _ = repl.run("branch create feature")
            docs = parse_json_stream(output)
            ensure(as_dict(only_doc(docs, "repl branch create"), "repl branch create").get("BranchWithVersion", {}).get("info", {}).get("id") == "feature", "repl branch create failed")

            output, prompt = repl.run("use feature analytics")
            ensure(output == "", f"repl use feature should not render output, got {output!r}")
            ensure(prompt == "strata:feature/analytics> ", f"unexpected prompt after use feature analytics: {prompt!r}")

            output, _ = repl.run("kv put feature-key 9")
            docs = parse_json_stream(output)
            ensure(as_dict(only_doc(docs, "repl feature kv put"), "repl feature kv put").get("WriteResult", {}).get("key") == "feature-key", "repl feature kv put failed")

            output, prompt = repl.run("branch del feature")
            docs = parse_json_stream(output)
            ensure(only_doc(docs, "repl branch del") == "Unit", "repl branch del feature should return Unit")
            ensure(prompt == "strata:default/default> ", f"prompt should reset after deleting current branch, got {prompt!r}")
        finally:
            repl.close()

    def run(self) -> int:
        print(f"INFO  using binary: {self.binary}")
        print(f"INFO  temp root: {self.temp_root}")

        cases: list[SuiteCase] = [
            SuiteCase("help-tree", self.test_help_tree, "smoke"),
            SuiteCase("shell-admin", self.test_shell_admin, "smoke"),
            SuiteCase("shell-init", self.test_shell_init, "standard"),
            SuiteCase("shell-persistence", self.test_shell_persistence, "standard"),
            SuiteCase("shell-branch-space", self.test_shell_branch_space, "smoke"),
            SuiteCase("shell-branch-power", self.test_shell_branch_power, "standard"),
            SuiteCase("shell-kv", self.test_shell_kv, "smoke"),
            SuiteCase("shell-json", self.test_shell_json, "smoke"),
            SuiteCase("shell-event", self.test_shell_event, "smoke"),
            SuiteCase("shell-vector", self.test_shell_vector, "standard"),
            SuiteCase("shell-graph", self.test_shell_graph, "smoke"),
            SuiteCase("shell-graph-power", self.test_shell_graph_power, "standard"),
            SuiteCase("shell-mixed", self.test_shell_mixed, "standard"),
            SuiteCase("shell-open-modes", self.test_shell_open_modes, "standard"),
            SuiteCase("shell-guardrails", self.test_shell_guardrails, "standard"),
            SuiteCase("shell-feature-gates", self.test_shell_feature_gated_failures, "standard"),
            SuiteCase("hostile-input", self.test_hostile_input, "adversarial"),
            SuiteCase("illegal-sequences", self.test_illegal_sequences, "adversarial"),
            SuiteCase("concurrency-conflict", self.test_concurrency_conflict, "adversarial"),
            SuiteCase("crash-recovery", self.test_crash_recovery, "adversarial"),
            SuiteCase("scale-stress", self.test_scale_stress, "full"),
            SuiteCase("shell-local-admin", self.test_shell_local_admin, "full"),
            SuiteCase("pipe-mode", self.test_pipe_mode, "smoke"),
            SuiteCase("pipe-mode-failure-recovery", self.test_pipe_mode_failure_recovery, "standard"),
            SuiteCase("repl-mode", self.test_repl_mode, "smoke"),
        ]
        self.case_names = {case.name for case in cases}
        self.case_tiers = {case.name: case.tier for case in cases}
        print(f"INFO  tier: {self.tier}")

        for case in cases:
            if not self.tier_in_scope(case.tier):
                continue
            self.record(case.name, case.fn)

        print(f"\nSUMMARY  {len(self.passes)} passed, {len(self.failures)} failed")
        if self.failures:
            print("FAILED CASES")
            for failure in self.failures:
                print(f"  - {failure}")
            return 1
        return 0


def resolve_binary(path: str | None) -> pathlib.Path:
    if path:
        binary = pathlib.Path(path).expanduser().resolve()
    else:
        found = shutil.which("strata")
        if not found:
            raise SystemExit("Could not find `strata` in PATH. Pass --strata-bin explicitly.")
        binary = pathlib.Path(found).resolve()
    if not binary.exists():
        raise SystemExit(f"Strata binary does not exist: {binary}")
    if not os.access(binary, os.X_OK):
        raise SystemExit(f"Strata binary is not executable: {binary}")
    return binary


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strata-bin", help="Path to the installed `strata` binary")
    parser.add_argument(
        "--tier",
        choices=sorted(TIER_ORDER, key=TIER_ORDER.get),
        default="standard",
        help="Execution tier: smoke, standard, adversarial, or full (default: standard)",
    )
    parser.add_argument(
        "--expected-leaf-count",
        type=int,
        help="Optional exact expected leaf-command count from the help crawler",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the temporary databases after the run",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every executed subprocess command plus stdin/stdout/stderr",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    binary = resolve_binary(args.strata_bin)
    suite = ExternalCliSuite(
        binary,
        expected_leaf_count=args.expected_leaf_count,
        keep_temp=args.keep_temp,
        verbose=args.verbose,
        tier=args.tier,
    )
    return suite.run()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
