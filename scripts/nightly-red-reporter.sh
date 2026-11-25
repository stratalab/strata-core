#!/usr/bin/env bash
# The nightly failure reporter (#3309): a red lane must notify someone.
#
# Reads the run's own job list from the API — not a hand-kept lane list, so a
# lane missing from the reporter job's `needs` is still reported — and for
# every red job either files a `nightly-red` issue naming that lane or
# comments the new run on the open one, so a streak is one issue with a
# growing tail instead of nine tabs nobody opened. Before this job, five
# streaks (6-22 nights: #3307/#2898/#2900/#2764/#2763) were found only by
# chance.
#
# A job is red when it failed, or when it ran into its own `timeout-minutes`:
# GitHub concludes a timed-out job `cancelled`, not `failure`, and Miri timed
# out 60 nights running while this script, then reading only `failure`,
# reported three of them (#3740). The jobs API does not say why a job was
# cancelled; the job's check-run annotations do. A lane cancelled by a person
# or by a cancelled run carries no timeout annotation and is not a finding;
# skipped lanes are not findings either.
# Extracted from nightly.yml so a test can run the decision against a fake
# `gh` (the same falsifiable-gate shape as scripts/mutation-verdict.sh).
set -euo pipefail

repo=$1
run_id=$2

run_url="https://github.com/$repo/actions/runs/$run_id"
sha=$(gh api "repos/$repo/actions/runs/$run_id" --jq '.head_sha')

# The message GitHub annotates a job with when it hits `timeout-minutes`.
timeout_marker='has exceeded the maximum execution time'

candidates=$(gh api "repos/$repo/actions/runs/$run_id/jobs?per_page=100" \
  --jq '.jobs[] | select(.conclusion == "failure" or .conclusion == "cancelled")
        | "\(.id)\t\(.conclusion)\t\(.name)"')

red=""
while IFS=$'\t' read -r id conclusion name; do
  [ -n "$id" ] || continue
  if [ "$conclusion" = "failure" ]; then
    red+="$name"$'\t'"failed"$'\n'
    continue
  fi
  # Captured, not piped into `grep -q`: under pipefail an early grep exit
  # can SIGPIPE gh and turn a match into a miss.
  annotations=$(gh api "repos/$repo/check-runs/$id/annotations" --jq '.[].message')
  if [[ "$annotations" == *"$timeout_marker"* ]]; then
    red+="$name"$'\t'"timed out"$'\n'
  fi
done <<<"$candidates"

if [ -z "$red" ]; then
  echo "no failed or timed-out jobs in run $run_id — nothing to report"
  exit 0
fi

# Idempotent: --force updates the label when it already exists.
gh label create nightly-red --repo "$repo" --color B60205 \
  --description "an unattended red nightly lane (filed by the #3309 reporter)" --force

while IFS=$'\t' read -r job reason; do
  [ -n "$job" ] || continue
  title="nightly-red: $job"
  existing=$(gh issue list --repo "$repo" --state open --label nightly-red \
    --json number,title \
    | jq -r --arg title "$title" '.[] | select(.title == $title) | .number' | head -1)
  if [ -n "$existing" ]; then
    gh issue comment "$existing" --repo "$repo" \
      --body "Still red ($reason): $run_url (commit $sha)"
    echo "commented on open issue #$existing for lane '$job'"
  else
    gh issue create --repo "$repo" --title "$title" --label "bug,nightly-red" \
      --body "Nightly lane \`$job\` $reason.

Run: $run_url
Commit: $sha

Filed automatically by the nightly failure reporter (#3309). Repeat reds of
this lane are added below as comments; close with the fixing PR (\`Closes #N\`)
so the next green run is the verdict."
    echo "created a nightly-red issue for lane '$job'"
  fi
done <<<"$red"
