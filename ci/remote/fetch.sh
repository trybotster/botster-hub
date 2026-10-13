#!/usr/bin/env bash
# The fetch step of a gate job on the shared test host (ci/remote/job.sh). It runs in its own container with the host's
# GitHub token, and downloads every dependency into the cargo volume, so the gate container runs with no network: the
# workspace's dependencies, botster-contracts and botster-core (private, with its submodules) included. It runs no code of
# this repo, only cargo's resolver.
set -euo pipefail

cargo fetch --locked -q
echo "fetch: the dependencies of the workspace are in the cargo volume"
