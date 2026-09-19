#!/usr/bin/env bash
#
# Decide whether one mutation lane passed, from its artifacts rather than its
# exit code.
#
# cargo-mutants reports timeout AHEAD of missed, so exit 3 — "timeout" — is
# also what a run returns when it hung on one mutant and left others alive.
# The gate used to read exit 3 as "timeout only, nothing missed" and pass it,
# which handed a free pass on every missed mutant to any PR whose diff carried
# a hanging mutant (#3225). Measured on cargo-mutants 27.1.0, the version
# `taiki-e/install-action@cargo-mutants` installs: a run with 5 missed and 1
# timeout exits 3.
#
# `mutants.out/missed.txt` lists the survivors whatever the exit code, so it is
# the authority here and the exit code is consulted only to tell a verdict from
# a tool failure. This lives in a script, not in the workflow, so that
# `crates/storage/tests/mutation_gate_verdict.rs` can run the decision against
# fabricated artifacts instead of asserting a description of it.
#
# Usage: mutation-verdict.sh <lane> <cargo-mutants-exit-code> [mutants.out dir]

set -uo pipefail

lane=$1
rc=$2
out=${3:-mutants.out}

case "$rc" in
  0 | 2 | 3) ;;
  *)
    # Not a verdict: the lane failed to build, or the invocation was wrong.
    # It judged nothing, so it cannot pass.
    echo "::error::mutation lane $lane: cargo-mutants exited $rc (no verdict)"
    exit "$rc"
    ;;
esac

missed="$out/missed.txt"
if [ -s "$missed" ]; then
  echo "::error::mutation lane $lane left $(wc -l <"$missed") mutant(s) alive"
  cat "$missed"
  exit 2
fi

if [ "$rc" -ne 0 ]; then
  # Exit 2 cannot reach here (it means missed > 0, and missed.txt is empty),
  # so this is exit 3 with nothing alive behind it: the mutants were detected,
  # by hanging rather than by an assertion. Name them — "killed by hang" is a
  # claim a reviewer should be able to check.
  echo "mutation lane $lane: exit $rc, no survivors — mutants killed by hang:"
  cat "$out/timeout.txt" 2>/dev/null
fi

exit 0
