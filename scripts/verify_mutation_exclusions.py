#!/usr/bin/env python3
"""Verify each mutation lane's "not compiled in this lane" exclusions (#3220).

cargo-mutants mutates source *text* and does not evaluate conditional
compilation, so a mutant on code a lane never compiles always survives, and
vacuously. Lane configs exclude such code with `exclude_globs`, justified as
"not compiled in this lane" — a prose claim nothing checked. Three such claims
were false (#3220): `--features X` *adds to* default features, so
`strata-inference`'s `download,testkit` lane compiles every cloud provider, and
three exclusions written on the opposite assumption silenced ten killable
mutants.

This turns the claim into a checked fact. For a lane it builds the same targets
and features cargo-mutants uses — `cargo check --tests` — harvests the exact set
of source files that compiled from the emitted dep-info (`.d`) files, and fails
any `exclude_glob` that reaches under a package's `src/` (a vacuity candidate,
as opposed to a whole-package `crates/X/**` partition marker) yet matches a file
the lane demonstrably compiled — unless that glob is on the reasoned allowlist
below.

The allowlist inverts the burden: "not compiled" globs self-verify (nothing to
trust — dep-info shows them absent), while deliberately excluding *compiled*
code must be justified here. That is exactly the mistake #3220 was: a compiled
file excluded on a false "not compiled" claim would now fail rather than pass.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

# Globs that exclude code the lane DOES compile, on purpose — NOT a "not
# compiled" claim. Each needs a reason. A compiled exclusion absent from here is
# the #3220 failure and reds the audit.
COMPILED_EXCLUSION_ALLOWLIST: dict[str, str] = {
    "crates/engine/src/persistence/fault.rs": (
        "test/testkit fault-injection scaffolding (#3156): compiles for the lib "
        "unit-test target, but the tests that kill its mutants open with "
        "`#![cfg(feature = \"testkit\")]` and run in the `-p strata-engine "
        "--features testkit` lane of the `test` job, not here."
    ),
    "crates/storage/src/testkit/simulation/**": (
        "DST simulation harness (TCP4.11): mutated to saturation, strength is its "
        "exact-facts pins; the full-file mutant set times out the per-PR budget."
    ),
    "crates/storage/src/testkit/dual_mutation.rs": (
        "dual-mutation fuzz harness (TCP4.6c): same saturation doctrine, "
        "pin-verified; one full round timed out the 90-minute budget."
    ),
    "crates/executor/src/bin/**": (
        "executor binaries are thin entry points, not a mutation target; their "
        "logic is exercised through the library and the IDL tooling tests."
    ),
}


def segment_matches(pattern: str, segment: str) -> bool:
    """One path segment against one glob segment (`*` matches within a segment).

    Mirrors `segment_matches` in crates/storage/tests/mutation_partition_guard.rs.
    """
    if "*" not in pattern:
        return pattern == segment
    prefix, suffix = pattern.split("*", 1)
    return (
        len(segment) >= len(prefix) + len(suffix)
        and segment.startswith(prefix)
        and segment.endswith(suffix)
    )


def glob_matches(pattern: str, path: str) -> bool:
    """`**` matches any number of path segments; mirrors the Rust guard's matcher."""

    def rec(pat: list[str], pth: list[str]) -> bool:
        if not pat:
            return len(pth) == 0
        head, rest = pat[0], pat[1:]
        if head == "**":
            return any(rec(rest, pth[i:]) for i in range(len(pth) + 1))
        if pth and segment_matches(head, pth[0]):
            return rec(rest, pth[1:])
        return False

    return rec(pattern.split("/"), path.split("/"))


def is_vacuity_candidate(glob: str) -> bool:
    """A glob reaching under `crates/<pkg>/src/` is a "not compiled" candidate.

    A whole-package `crates/X/**` glob is a partition marker (the package is
    mutated in another lane, and `mutation_partition_guard.rs` checks that), not
    a vacuity claim, so it is out of this audit's scope.
    """
    parts = glob.split("/")
    return len(parts) >= 4 and parts[0] == "crates" and parts[2] == "src"


def harvest_compiled(packages: list[str], features: str | None) -> set[str]:
    """Repo-relative source files a lane compiles, from `cargo check --tests`.

    `--tests` matches cargo-mutants' build: it judges a mutant by running the
    package's tests, so the lib is compiled with `--cfg test`, and a
    `#[cfg(test)]`/`#[cfg(any(test, ...))]` item is live exactly as it is there.
    """
    tmp = tempfile.mkdtemp(prefix="mutexcl-depinfo-")
    try:
        cmd = ["cargo", "check", "--tests", "--target-dir", tmp]
        for pkg in packages:
            cmd += ["-p", pkg]
        if features:
            cmd += ["--features", features]
        subprocess.run(cmd, check=True)
        return compiled_from_dep_info(Path(tmp))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


_CRATE_SRC = re.compile(r"crates/[\w-]+/src/[^ :\\]*\.rs")


def compiled_from_dep_info(target_dir: Path) -> set[str]:
    """Every `crates/*/src/*.rs` referenced by any dep-info `.d` file.

    `.d` files are Makefile-format (`output: dep dep ...`) and hold absolute
    paths; the repo-relative portion is what matches an `exclude_glob`.
    """
    compiled: set[str] = set()
    for dep in target_dir.rglob("*.d"):
        try:
            text = dep.read_text()
        except OSError:
            continue
        compiled.update(_CRATE_SRC.findall(text))
    return compiled


def audit_globs(globs: list[str], compiled: set[str], out=sys.stdout) -> list[str]:
    """Return the globs that fail: a vacuity-candidate that matches a compiled
    file and is not allowlisted. Prints a per-glob verdict."""
    failures: list[str] = []
    for glob in globs:
        if not is_vacuity_candidate(glob):
            print(f"  skip  (partition marker)     {glob}", file=out)
            continue
        matched = sorted(f for f in compiled if glob_matches(glob, f))
        if not matched:
            print(f"  ok    (not compiled)         {glob}", file=out)
        elif glob in COMPILED_EXCLUSION_ALLOWLIST:
            print(
                f"  ok    (compiled, allowlisted) {glob}  [{len(matched)} file(s)]",
                file=out,
            )
        else:
            failures.append(glob)
            print(
                f"  FAIL  (compiled, unjustified) {glob}\n"
                f"        the lane compiles e.g. {matched[0]} — this exclusion "
                f"silences killable mutants (#3220).\n"
                f"        Remove it, or add it to COMPILED_EXCLUSION_ALLOWLIST "
                f"with a reason.",
                file=out,
            )
    return failures


def audit_lane(lane: str, config: Path, packages: list[str], features: str | None) -> int:
    globs = tomllib.loads(config.read_text()).get("exclude_globs", [])
    print(f"== lane {lane}: {config} ({len(globs)} exclude_globs) ==")
    compiled = harvest_compiled(packages, features)
    print(f"   compiled source files: {len(compiled)}")
    failures = audit_globs(globs, compiled)
    if failures:
        print(f"lane {lane}: {len(failures)} unverified 'not compiled' exclusion(s)")
        return 1
    print(f"lane {lane}: all vacuity exclusions verified not-compiled or allowlisted")
    return 0


def self_test() -> int:
    """Decision-logic checks on fabricated inputs — no build, runs in the
    `test` job via crates/storage/tests/mutation_exclusion_audit.rs."""
    compiled = {
        "crates/inference/src/runtime.rs",
        "crates/engine/src/persistence/fault.rs",
        "crates/storage/src/testkit/simulation/harness.rs",
        "crates/executor/src/executor/vector.rs",
    }
    cases = [
        # (name, globs, expect_failures)
        ("not-compiled passes", ["crates/inference/src/llama/**"], []),
        ("partition skipped", ["crates/executor/**"], []),
        ("allowlisted compiled passes", ["crates/engine/src/persistence/fault.rs"], []),
        (
            "allowlisted glob (**) compiled passes",
            ["crates/storage/src/testkit/simulation/**"],
            [],
        ),
        (
            "compiled + unjustified FAILS",
            ["crates/inference/src/runtime.rs"],
            ["crates/inference/src/runtime.rs"],
        ),
        (
            "compiled + unjustified via ** FAILS",
            ["crates/executor/src/executor/*.rs"],
            ["crates/executor/src/executor/*.rs"],
        ),
    ]
    import io

    ok = True
    for name, globs, expect in cases:
        got = audit_globs(globs, compiled, out=io.StringIO())
        if got != expect:
            print(f"SELF-TEST FAIL: {name}: expected {expect}, got {got}")
            ok = False
        else:
            print(f"self-test ok: {name}")
    # Glob-matcher parity spot-checks with the Rust guard's semantics.
    assert glob_matches("crates/storage/src/**/*_loom.rs", "crates/storage/src/a_loom.rs")
    assert glob_matches(
        "crates/storage/src/**/*_loom.rs", "crates/storage/src/a/b_loom.rs"
    )
    assert not glob_matches("crates/storage/src/**/*_loom.rs", "crates/storage/src/a.rs")
    assert glob_matches("crates/executor/src/bin/**", "crates/executor/src/bin/x.rs")
    assert not glob_matches("crates/executor/**", "crates/storage/src/x.rs")
    print("self-test ok: glob-matcher parity")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lane", help="lane label for reporting (A/B/C)")
    ap.add_argument("--config", type=Path, help="the lane's mutants .toml")
    ap.add_argument(
        "--package",
        action="append",
        default=[],
        help="package to build; omit for the whole default-members workspace (lane A)",
    )
    ap.add_argument("--features", help="the lane's --features string")
    ap.add_argument(
        "--compiled-list",
        type=Path,
        help="inject mode: read the compiled-file set from this file instead of building",
    )
    ap.add_argument("--self-test", action="store_true", help="run decision-logic checks and exit")
    args = ap.parse_args()

    if args.self_test:
        return self_test()
    if not args.config:
        ap.error("--config is required unless --self-test")

    if args.compiled_list:
        compiled = {
            line.strip()
            for line in args.compiled_list.read_text().splitlines()
            if line.strip()
        }
        globs = tomllib.loads(args.config.read_text()).get("exclude_globs", [])
        failures = audit_globs(globs, compiled)
        return 1 if failures else 0

    return audit_lane(args.lane or "?", args.config, args.package, args.features)


if __name__ == "__main__":
    sys.exit(main())
