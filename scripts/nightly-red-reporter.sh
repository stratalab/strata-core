#!/usr/bin/env bash
# The nightly failure reporter (#3309): a red lane must notify someone.
#
# Reads the run's own job list from the API — not a hand-kept lane list, so a
# lane missing from the reporter job's `needs` is still reported — and for
# every job whose conclusion is `failure` either files a `nightly-red` issue
# naming that lane or comments the new run on the open one, so a streak is
# one issue with a growing tail instead of nine tabs nobody opened. Before
# this job, five streaks (6-22 nights: #3307/#2898/#2900/#2764/#2763) were
# found only by chance.
#
# Cancelled and skipped lanes are not findings; only `failure` reports.
# Extracted from nightly.yml so a test can run the decision against a fake
# `gh` (the same falsifiable-gate shape as scripts/mutation-verdict.sh).
set -euo pipefail

repo=$1
run_id=$2

run_url="https://github.com/$repo/actions/runs/$run_id"
sha=$(gh api "repos/$repo/actions/runs/$run_id" --jq '.head_sha')

failed=$(gh api "repos/$repo/actions/runs/$run_id/jobs?per_page=100" \
  --jq '.jobs[] | select(.conclusion == "failure") | .name')

if [ -z "$failed" ]; then
  echo "no failed jobs in run $run_id — nothing to report"
  exit 0
fi

# Idempotent: --force updates the label when it already exists.
gh label create nightly-red --repo "$repo" --color B60205 \
  --description "an unattended red nightly lane (filed by the #3309 reporter)" --force

while IFS= read -r job; do
  title="nightly-red: $job"
  existing=$(gh issue list --repo "$repo" --state open --label nightly-red \
    --json number,title \
    | jq -r --arg title "$title" '.[] | select(.title == $title) | .number' | head -1)
  if [ -n "$existing" ]; then
    gh issue comment "$existing" --repo "$repo" \
      --body "Still red: $run_url (commit $sha)"
    echo "commented on open issue #$existing for lane '$job'"
  else
    gh issue create --repo "$repo" --title "$title" --label "bug,nightly-red" \
      --body "Nightly lane \`$job\` failed.

Run: $run_url
Commit: $sha

Filed automatically by the nightly failure reporter (#3309). Repeat reds of
this lane are added below as comments; close with the fixing PR (\`Closes #N\`)
so the next green run is the verdict."
    echo "created a nightly-red issue for lane '$job'"
  fi
done <<<"$failed"
