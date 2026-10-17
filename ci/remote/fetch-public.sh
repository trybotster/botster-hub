#!/usr/bin/env bash
# The public fetch step of a gate (.pool.toml `fetch.public_command`: the pool runs it with network and without any
# secret). The Hub depends on botster-core, whose botster-terminal-ghostty crate builds libghostty with Zig. Its build.rs
# fetches nothing, so the store of Zig packages must be full before the gate runs with no network.
#
# This step reads no cargo checkout: the packages are data in ci/zig-prefetch/build.zig.zon (URLs and hashes), and
# `zig build --fetch` downloads them and checks each against its hash. `cargo xtask zig-packages` fails when that list
# differs from the one in botster-core's pin. Each archive is published into the store with an atomic rename and never
# overwritten, so a gate never reads a partial archive. An archive that fails `gzip -t` counts as missing.
set -euo pipefail

# Stage 2 gates of botster-hub run on the Linux pool only: this step needs flock.
if [ "$(uname -s)" = Darwin ]; then
  echo "botster-hub: Mac gates are not supported in Stage 2; use botster-gate --on linux" >&2
  exit 64
fi

here=$(cd "$(dirname "$0")/../zig-prefetch" && pwd)
store=${BOTSTER_ZIG_PACKAGES:-$HOME/.cache/botster/zig-packages}

mkdir -p "$store/p"
# The package hashes, each once.
hashes=$(sed -n 's/^ *\.hash = "\(.*\)",$/\1/p' "$here/build.zig.zon" | sort -u)
[ -n "$hashes" ] || { echo "fetch-public.sh: no package hash in $here/build.zig.zon" >&2; exit 1; }
# Prints the hashes whose archive is absent or broken.
missing() {
  local hash
  for hash in $hashes; do
    gzip -t "$store/p/$hash.tar.gz" 2>/dev/null || echo "$hash"
  done
}
if [ -z "$(missing)" ]; then
  echo "fetch: every Zig package of ci/zig-prefetch is in $store"
else
  # Under the store lock: check again (another gate may have published them meanwhile), fetch into a staging directory on
  # the same file system, and publish each missing archive with a rename.
  export store hashes here
  export -f missing
  flock "$store/.prefetch.lock" bash -c '
    set -euo pipefail
    todo=$(missing)
    [ -n "$todo" ] || { echo "fetch: another gate published the Zig packages"; exit 0; }
    staging=$(mktemp -d "$store/.staging.XXXXXX")
    trap "rm -rf \"$staging\"" EXIT
    echo "fetch: $(echo $todo | wc -w | tr -d " ") Zig packages are missing from $store; running zig build --fetch"
    # Zig also keeps the packages in a zig-pkg directory of the project, and then skips a package it finds there: run in a
    # copy, so the tree stays clean and every run fetches.
    mkdir -p "$staging/global" "$staging/local" "$staging/project"
    cp "$here/build.zig" "$here/build.zig.zon" "$staging/project/"
    # A download can fail on a TLS or network fault (seen with codeberg.org): three tries, each in a clean cache.
    for try in 1 2 3; do
      rm -rf "$staging/global" "$staging/local" "$staging/project/zig-pkg"
      mkdir -p "$staging/global" "$staging/local"
      (cd "$staging/project" && zig build --fetch --global-cache-dir "$staging/global" --cache-dir "$staging/local") && break
      [ "$try" -lt 3 ] || { echo "fetch: zig build --fetch failed three times" >&2; exit 1; }
      sleep 5
    done
    for hash in $todo; do
      gzip -t "$staging/global/p/$hash.tar.gz"
      mv -f "$staging/global/p/$hash.tar.gz" "$store/p/$hash.tar.gz"
    done
    echo "fetch: published $(echo $todo | wc -w | tr -d " ") Zig packages in $store"'
fi
