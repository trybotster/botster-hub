#!/usr/bin/env sh
set -eu

node packages/hub-test-support/scripts/sync-assets.mjs --check
BOTSTER_ENV=test cargo test "$@"
