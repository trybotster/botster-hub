#!/usr/bin/env sh
set -eu

node packages/hub-test-support/scripts/sync-assets.mjs --check
# --workspace is load-bearing. The root package `botster-hub` is itself a
# workspace member and no `default-members` is declared, so a bare `cargo test`
# run from here tests the current package ONLY. Every other member crate's
# assertions — including the installer's crash, rollback, lease, and signature
# proofs — would compile but never execute, so a regression in them would pass
# this gate. Targeted forms such as `./test.sh --test hub_daemon_lifecycle_test`
# keep working unchanged.
BOTSTER_ENV=test cargo test --workspace "$@"
