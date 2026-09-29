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
[ "$fail" = 0 ]
