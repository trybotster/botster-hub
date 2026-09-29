#!/bin/sh
# Checks that the env file of test.sh --phase build restores every value
# exactly, including values with quotes, spaces, dollar signs and backticks.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
. "$here/candidate-env.sh"
dir=$(mktemp -d "${TMPDIR:-/tmp}/candidate-env-test.XXXXXX")
trap 'rm -rf "$dir"' EXIT HUP INT TERM
fail=0
check() {
  BOTSTER_HUB_BIN=$1
  BOTSTER_SESSION_WORKER_BIN="$1/worker"
  BOTSTER_CANDIDATE_MANIFEST="$1 manifest"
  BOTSTER_HUB_CLIENT_ADAPTER_BIN='$HOME `id` "x"'
  write_candidate_env "$dir/env"
  sh -n "$dir/env" || { echo "FAIL syntax for: $1"; fail=1; return; }
  (
    unset BOTSTER_HUB_BIN BOTSTER_SESSION_WORKER_BIN BOTSTER_CANDIDATE_MANIFEST BOTSTER_HUB_CLIENT_ADAPTER_BIN
    . "$dir/env"
    [ "$BOTSTER_HUB_BIN" = "$1" ] && [ "$BOTSTER_SESSION_WORKER_BIN" = "$1/worker" ] \
      && [ "$BOTSTER_CANDIDATE_MANIFEST" = "$1 manifest" ] \
      && [ "$BOTSTER_HUB_CLIENT_ADAPTER_BIN" = '$HOME `id` "x"' ]
  ) && echo "PASS restores: $1" || { echo "FAIL restore for: $1"; fail=1; }
}
check /tmp/plain/hub
check "/tmp/O'Brien/hub"
check "/tmp/two''quotes/hub"
check '/tmp/dollar $HOME/hub'
check '/tmp/back`tick`/hub'
check '/tmp/space and "double"/hub'
[ "$fail" = 0 ]
