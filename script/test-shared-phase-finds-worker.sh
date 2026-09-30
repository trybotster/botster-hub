#!/usr/bin/env sh
# Proves the shared phase survives a target clean between phases (botsterq's
# target cap does this): after a build phase, the worker binary that library
# tests find beside their executable is deleted, and one worker-spawning
# library test must still pass under `--phase shared`. Slow (a build); run it
# when test.sh's phase handling changes.
set -eu
cd "$(dirname "$0")/.."
./test.sh --phase build --locked
rm -f "${CARGO_TARGET_DIR:-target}/debug/botster-session-worker"
./test.sh --phase shared --locked -- dropped_delivery_receiver_confirms_owner_cleanup
echo "PASS shared phase rebuilds the deleted worker"
