#!/usr/bin/env sh
# The gate. Phases (the first arguments; everything after goes to cargo test):
#   ./test.sh [--phase all]                  the whole gate (the default)
#   ./test.sh --phase build [--candidate-dir <dir>]
#       checks, candidate artifacts, adapter and worker builds, and
#       `cargo test --workspace --no-run`. The candidate directory is kept, holds
#       an `env` file, and is printed as `candidate_dir=<dir>` on the last line.
#       Without --candidate-dir it is `target/candidate` of this worktree (or of
#       CARGO_TARGET_DIR), replaced by each build phase: one per worktree, inside
#       the target that botsterq's cap covers, nothing left in /tmp.
#   ./test.sh --phase shared [--candidate-dir <dir>]
#       every test target except the lifecycle target, and the doc tests.
#   ./test.sh --phase lifecycle [--candidate-dir <dir>]
#       the lifecycle target (tests/hub_daemon_lifecycle_test.rs) alone.
#       Without --candidate-dir both use the default directory of the build phase.
# build, shared and lifecycle together run what `all` runs.
#
# Repeating one test (a flake hunt) needs one build, not one per run:
#   ./test.sh --phase build --locked                                  # once
#   for i in 1 2 3; do
#     ./test.sh --phase lifecycle --locked -- <test name> --exact
#   done                                                              # the runs
# Which phase needs an exclusive queue slot is the queue's rule, not this
# script's: today the lifecycle phase is the exclusive one. Whether two shared
# phases may overlap safely has not been measured.
set -eu

phase=all
candidate_dir=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --phase) phase=${2:?--phase needs all, build, shared or lifecycle}; shift 2 ;;
    --candidate-dir) candidate_dir=${2:?--candidate-dir needs a directory}; shift 2 ;;
    *) break ;;
  esac
done
case "$phase" in
  all|build|shared|lifecycle) ;;
  *) echo "unknown phase: $phase (all, build, shared or lifecycle)" >&2; exit 2 ;;
esac
# --phase and --candidate-dir belong before every other argument. Anywhere else
# they would reach cargo test as ordinary arguments and the script would run the
# whole gate (phase all), the worst silent outcome; refuse them instead.
for arg in "$@"; do
  case "$arg" in
    --) break ;;
    --phase|--phase=*|--candidate-dir|--candidate-dir=*)
      echo "test.sh: $arg must come before every other argument (usage: ./test.sh [--phase all|build|shared|lifecycle] [--candidate-dir <dir>] [cargo test arguments])" >&2
      exit 2
      ;;
  esac
done

# The candidate directory the phases hand over. Default: target/candidate of this
# worktree, replaced by each build phase; the shared and lifecycle phases need
# the env file a build phase left there (or in the directory given).
default_candidate_dir=
if [ -z "$candidate_dir" ] && { [ "$phase" = build ] || [ "$phase" = shared ] || [ "$phase" = lifecycle ]; }; then
  target_root=${CARGO_TARGET_DIR:-target}
  case "$target_root" in
    /*) ;;
    *) target_root=$(pwd)/$target_root ;;
  esac
  candidate_dir=$target_root/candidate
  default_candidate_dir=$candidate_dir
fi
case "$phase" in
  shared|lifecycle)
    [ -f "$candidate_dir/env" ] || {
      echo "test.sh: --phase $phase needs the candidate directory of a --phase build run; $candidate_dir/env is missing (run ./test.sh --phase build first, or pass --candidate-dir <dir>)" >&2
      exit 2
    }
    ;;
esac

node packages/hub-test-support/scripts/sync-assets.mjs --check

export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0

case "$phase" in
  shared|lifecycle)
    . "$candidate_dir/env"
    ;;
esac

candidate_path_count=0
[ -n "${BOTSTER_HUB_BIN:-}" ] && candidate_path_count=$((candidate_path_count + 1))
[ -n "${BOTSTER_SESSION_WORKER_BIN:-}" ] && candidate_path_count=$((candidate_path_count + 1))
[ -n "${BOTSTER_CANDIDATE_MANIFEST:-}" ] && candidate_path_count=$((candidate_path_count + 1))
if [ "$phase" = all ]; then
  candidate_dir=$(mktemp -d "${TMPDIR:-/tmp}/botster-hub-candidate.XXXXXX")
  trap 'rm -rf "$candidate_dir"' EXIT HUP INT TERM
elif [ "$phase" = build ]; then
  # Kept for the shared and lifecycle phases. The default directory is replaced
  # by each build phase; a directory given with --candidate-dir is never emptied.
  if [ -n "$default_candidate_dir" ]; then
    rm -rf "$candidate_dir"
  fi
  mkdir -p "$candidate_dir"
fi
if [ "$phase" = shared ] || [ "$phase" = lifecycle ]; then
  :  # candidate artifacts come from the build phase (sourced above)
elif [ "$candidate_path_count" -eq 0 ]; then
  candidate_exports=$(script/build-dev-artifacts --out-dir "$candidate_dir" --with-harness-adapter)
  printf '%s\n' "$candidate_exports"
  BOTSTER_HUB_BIN=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_HUB_BIN=//p')
  BOTSTER_SESSION_WORKER_BIN=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_SESSION_WORKER_BIN=//p')
  BOTSTER_CANDIDATE_MANIFEST=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_CANDIDATE_MANIFEST=//p')
  BOTSTER_HUB_CLIENT_ADAPTER_BIN=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_HUB_CLIENT_ADAPTER_BIN=//p')
elif [ "$candidate_path_count" -ne 3 ]; then
  echo "BOTSTER_HUB_BIN, BOTSTER_SESSION_WORKER_BIN, and BOTSTER_CANDIDATE_MANIFEST must be set together" >&2
  exit 1
fi
# Resource tests drive the Hub through the harness_control client adapter.
# A supplied candidate usually predates the adapter, so build it from this
# checkout and run an immutable copy, as the test library is built here too.
if [ "$phase" != shared ] && [ "$phase" != lifecycle ] && [ -z "${BOTSTER_HUB_CLIENT_ADAPTER_BIN:-}" ]; then
  cargo build --locked -p botster-hub-client --example harness_control
  cp "${CARGO_TARGET_DIR:-target}/debug/examples/harness_control" "$candidate_dir/harness_control"
  BOTSTER_HUB_CLIENT_ADAPTER_BIN="$candidate_dir/harness_control"
fi
export BOTSTER_HUB_BIN BOTSTER_SESSION_WORKER_BIN BOTSTER_CANDIDATE_MANIFEST BOTSTER_HUB_CLIENT_ADAPTER_BIN
[ -x "$BOTSTER_HUB_CLIENT_ADAPTER_BIN" ] || { echo "harness adapter is not executable: $BOTSTER_HUB_CLIENT_ADAPTER_BIN" >&2; exit 1; }
[ -x "$BOTSTER_HUB_BIN" ] || { echo "candidate Hub is not executable: $BOTSTER_HUB_BIN" >&2; exit 1; }
[ -x "$BOTSTER_SESSION_WORKER_BIN" ] || { echo "candidate worker is not executable: $BOTSTER_SESSION_WORKER_BIN" >&2; exit 1; }
[ -f "$BOTSTER_CANDIDATE_MANIFEST" ] || { echo "candidate manifest is missing: $BOTSTER_CANDIDATE_MANIFEST" >&2; exit 1; }
node --input-type=module - "$BOTSTER_CANDIDATE_MANIFEST" "$BOTSTER_HUB_BIN" "$BOTSTER_SESSION_WORKER_BIN" <<'NODE'
import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";

const [manifestPath, hubPath, workerPath] = process.argv.slice(2);
const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
for (const name of ["botster_hub", "botster_core"]) {
  if (typeof manifest.source_revisions?.[name] !== "string" || manifest.source_revisions[name].trim() === "") {
    throw new Error(`source_revisions.${name} must be a non-empty string`);
  }
}
for (const [name, path] of [["botster-hub", hubPath], ["botster-session-worker", workerPath]]) {
  const matches = manifest.artifacts?.filter((artifact) => artifact.name === name) ?? [];
  if (matches.length !== 1) throw new Error(`expected one ${name} artifact, found ${matches.length}`);
  const artifact = matches[0];
  if (!Number.isSafeInteger(artifact.size) || artifact.size <= 0) throw new Error(`${name} size must be a positive integer`);
  if (!/^[0-9a-f]{64}$/.test(artifact.sha256)) throw new Error(`${name} SHA-256 must be lowercase hexadecimal`);
  const bytes = readFileSync(path);
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (statSync(path).size !== artifact.size) throw new Error(`${name} size does not match the manifest`);
  if (sha256 !== artifact.sha256) throw new Error(`${name} SHA-256 does not match the manifest`);
}
NODE
printf '%s\n' "candidate_manifest_verified=$BOTSTER_CANDIDATE_MANIFEST"
printf '%s\n' "candidate_manifest=$BOTSTER_CANDIDATE_MANIFEST"
printf '%s\n' "harness_adapter=$BOTSTER_HUB_CLIENT_ADAPTER_BIN"
cat "$BOTSTER_CANDIDATE_MANIFEST"

# Library tests find the session worker beside their executable in
# target/debug (src/runtime.rs). `--workspace` does not build it because it
# belongs to the pinned Core dependency, so build it from that pin here.
if [ "$phase" = all ] || [ "$phase" = build ]; then
  cargo build --locked -p botster-core-daemon --bin botster-session-worker
fi

# --workspace is load-bearing. The root package `botster-hub` is itself a
# workspace member and no `default-members` is declared, so a bare `cargo test`
# run from here tests the current package ONLY. Every other member crate's
# assertions — including the installer's crash, rollback, lease, and signature
# proofs — would compile but never execute, so a regression in them would pass
# this gate. Targeted forms such as `./test.sh --test hub_daemon_lifecycle_test`
# use this same candidate set. A bare daemon-spawning `cargo test` command must
# receive BOTSTER_HUB_BIN, BOTSTER_SESSION_WORKER_BIN, and
# BOTSTER_CANDIDATE_MANIFEST from script/build-dev-artifacts.
case "$phase" in
  build)
    # The build phase ends after compiling every test binary, and records what
    # the test phases need.
    BOTSTER_ENV=test cargo test --workspace --no-run "$@"
    . script/candidate-env.sh
    write_candidate_env "$candidate_dir/env"
    printf 'candidate_dir=%s\n' "$candidate_dir"
    exit 0
    ;;
  lifecycle)
    # --workspace, not -p botster-hub: a package selection unifies dev-dependency
    # features differently, so `-p` rebuilt botster-hub and two test-support
    # crates after the build phase had compiled them for --workspace.
    BOTSTER_ENV=test exec cargo test --workspace --no-fail-fast --test hub_daemon_lifecycle_test "$@"
    ;;
  shared)
    # Every test target except the lifecycle target, from cargo metadata, so a
    # new target is covered without editing this script; then the doc tests. Both
    # run even if the first fails, as --no-fail-fast does within one run.
    shared_targets=$(node script/list-shared-test-targets.mjs)
    shared_status=0
    # shellcheck disable=SC2086
    BOTSTER_ENV=test cargo test --workspace --no-fail-fast --lib --bins $shared_targets "$@" || shared_status=$?
    BOTSTER_ENV=test cargo test --workspace --no-fail-fast --doc "$@" || shared_status=$?
    exit "$shared_status"
    ;;
esac

# --no-fail-fast runs every test target even after one fails, so a red target
# cannot hide another; any failure still fails the gate.
BOTSTER_ENV=test cargo test --workspace --no-fail-fast "$@"
