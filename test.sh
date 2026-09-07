#!/usr/bin/env sh
set -eu

node packages/hub-test-support/scripts/sync-assets.mjs --check

export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0

candidate_path_count=0
[ -n "${BOTSTER_HUB_BIN:-}" ] && candidate_path_count=$((candidate_path_count + 1))
[ -n "${BOTSTER_SESSION_WORKER_BIN:-}" ] && candidate_path_count=$((candidate_path_count + 1))
[ -n "${BOTSTER_CANDIDATE_MANIFEST:-}" ] && candidate_path_count=$((candidate_path_count + 1))
if [ "$candidate_path_count" -eq 0 ]; then
  candidate_dir=$(mktemp -d "${TMPDIR:-/tmp}/botster-hub-candidate.XXXXXX")
  trap 'rm -rf "$candidate_dir"' EXIT HUP INT TERM
  candidate_exports=$(script/build-dev-artifacts --out-dir "$candidate_dir")
  printf '%s\n' "$candidate_exports"
  BOTSTER_HUB_BIN=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_HUB_BIN=//p')
  BOTSTER_SESSION_WORKER_BIN=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_SESSION_WORKER_BIN=//p')
  BOTSTER_CANDIDATE_MANIFEST=$(printf '%s\n' "$candidate_exports" | sed -n 's/^BOTSTER_CANDIDATE_MANIFEST=//p')
elif [ "$candidate_path_count" -ne 3 ]; then
  echo "BOTSTER_HUB_BIN, BOTSTER_SESSION_WORKER_BIN, and BOTSTER_CANDIDATE_MANIFEST must be set together" >&2
  exit 1
fi
export BOTSTER_HUB_BIN BOTSTER_SESSION_WORKER_BIN BOTSTER_CANDIDATE_MANIFEST
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
cat "$BOTSTER_CANDIDATE_MANIFEST"

# --workspace is load-bearing. The root package `botster-hub` is itself a
# workspace member and no `default-members` is declared, so a bare `cargo test`
# run from here tests the current package ONLY. Every other member crate's
# assertions — including the installer's crash, rollback, lease, and signature
# proofs — would compile but never execute, so a regression in them would pass
# this gate. Targeted forms such as `./test.sh --test hub_daemon_lifecycle_test`
# use this same candidate set. A bare daemon-spawning `cargo test` command must
# receive BOTSTER_HUB_BIN, BOTSTER_SESSION_WORKER_BIN, and
# BOTSTER_CANDIDATE_MANIFEST from script/build-dev-artifacts.
BOTSTER_ENV=test cargo test --workspace "$@"
