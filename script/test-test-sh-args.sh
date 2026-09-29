#!/bin/sh
# Checks that test.sh refuses --phase and --candidate-dir when they are not the
# first arguments, and an unknown phase, before it runs anything.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
fail=0
expect_refusal() {
  description=$1
  shift
  status=0
  output=$("$here/test.sh" "$@" 2>&1) || status=$?
  if [ "$status" = 2 ] && printf '%s' "$output" | grep -q "must come before every other argument\|unknown phase"; then
    echo "PASS refused: $description"
  else
    echo "FAIL $description: exit $status: $output"
    fail=1
  fi
}
expect_refusal "--phase after --locked" --locked --phase build
expect_refusal "--phase=build after --locked" --locked --phase=build
expect_refusal "--candidate-dir after --locked" --locked --candidate-dir /tmp/x
expect_refusal "--phase after an initial phase" --phase build --locked --phase shared
expect_refusal "unknown phase" --phase nonsense
# shared and lifecycle without a candidate directory: refused, without any work,
# when the worktree has no target/candidate/env (run from an empty directory).
empty=$(mktemp -d "${TMPDIR:-/tmp}/test-sh-args.XXXXXX")
trap 'rm -rf "$empty"' EXIT HUP INT TERM
for phase in shared lifecycle; do
  status=0
  output=$(cd "$empty" && "$here/test.sh" --phase "$phase" --locked 2>&1) || status=$?
  if [ "$status" = 2 ] && printf '%s' "$output" | grep -q "needs the candidate directory of a --phase build run"; then
    echo "PASS refused: --phase $phase without a candidate directory"
  else
    echo "FAIL --phase $phase without a candidate directory: exit $status: $output"
    fail=1
  fi
done
[ "$fail" = 0 ]
