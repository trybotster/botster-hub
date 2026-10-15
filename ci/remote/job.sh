#!/usr/bin/env bash
# One queued Botster gate on the shared test host, started by `testq submit` (botster-gate; ~/testq/README.md on the host).
# Runs a command in the snapshot inside the gate image, limited to the job's CPUs. The snapshot is a git checkout of the
# exact head, with the base branch at refs/remotes/origin/<base>, so the diff checks have their base.
#
#   ci/remote/job.sh <snapshot-dir> <cpus> [--run=<id>] [--branch=<name>] [--base=<sha>] [--lease=<file>]
#                    -- <command and args...>
#
# --run names the job for its caller: botster-gate finds and cancels the job by it. A failed job keeps its mutation reports
# ($artifact_paths below) in ~/testq/projects/<project>/artifacts/<run> for 7 days.
# --branch picks the target volume: one per project and branch, so a branch's gates build incrementally. botster-gate
# holds a host lock per branch for the whole gate, so two gates never share a target volume at once.
# --base sets BOTSTER_CI_BASE_REF to the base commit that the client recorded, so the diff checks use it.
# --lease is the client's lease file (~/testq/projects/botster-locks/leases/<run>). The client's holder process keeps it
# locked while the client lives and sends heartbeats. When the lock is free, the client is gone: the job does not start,
# or it stops and cleans up.
#
# Volumes: <project>-cargo (cargo's registry and git checkouts, shared by the project's jobs), <project>-target-<branch>,
# and <project>-npm (the npm cache). A target volume that no gate used for 7 days is removed.
#
# This file holds only the settings of this repository. The rest of the job is ci/remote/job-common.sh, which
# botster-contracts owns and botster-core and botster-hub copy byte for byte; a line that only one repository needs goes
# here.
set -euo pipefail

# --- settings -----------------------------------------------------------------------------------------------------------
project=botster-hub
# 1: Cargo.lock has private git dependencies (trybotster: botster-contracts, and botster-core for botster-gate-kit). A
# separate fetch container gets the host's `gh auth token`; the gate container then runs with no network and no token.
private_git_deps=1
# Extra build arguments for the image, one per line, read from the snapshot. They are part of the image tag. None: the
# Hub's gate has no nightly step.
image_args() { :; }
# Files whose content picks the image tag, besides the Dockerfile.
image_inputs=(rust-toolchain.toml)
# Volumes besides cargo, target and npm: "<suffix>:<mount>". zig: Zig's global cache and the libghostty package store.
extra_volumes=(zig:/zig)
# Environment of the fetch and gate containers. libghostty's build.rs (botster-core, a dependency) reads its Zig packages
# from the store that fetch-public.sh fills, and runs Zig under a network denial: the gate container has no network at all
# (--network none), so it declares that with BOTSTER_ZIG_NETWORK_DENIED=1.
extra_env=(BOTSTER_ZIG_PACKAGES=/zig/packages BOTSTER_ZIG_NETWORK_DENIED=1)
# The reports that a failed job keeps, as paths in the tree (target/ is the target volume). None.
artifact_paths=()
# --------------------------------------------------------------------------------------------------------------------------

. "$(dirname "$0")/job-common.sh"
