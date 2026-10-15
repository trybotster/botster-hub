#!/usr/bin/env bash
# The public fetch step of a gate (.pool.toml `fetch.public_command`: the pool runs it after the private cargo fetch, with
# network and without any GitHub token). The Hub depends on botster-core, whose botster-terminal-ghostty crate builds libghostty with
# Zig. Its build.rs fetches nothing, so the store of Zig packages that build_data.rs lists must be full before the gate runs
# with no network. The crate is in cargo's checkout of botster-core (CARGO_HOME), with its prefetch-zig.sh (a Zig build of
# the pinned Ghostty source). Each archive is published into the store with an atomic rename and never overwritten, so a
# gate never reads a partial archive. An archive that fails `gzip -t` counts as missing.
set -euo pipefail

# Stage 2 gates of botster-hub run on the Linux pool only: this step needs flock and the cargo home that the private fetch
# filled.
if [ "$(uname -s)" = Darwin ]; then
  echo "botster-hub: Mac gates are not supported in Stage 2; use botster-gate --on linux" >&2
  exit 64
fi

: "${CARGO_HOME:=$HOME/.cargo}"
store=${BOTSTER_ZIG_PACKAGES:-$HOME/.cache/botster/zig-packages}

# The crate, in the checkout of the botster-core commit that Cargo.lock names (botster-gate-kit comes from another commit of
# the same repository, so there can be several).
ghostty=
for sha in $(sed -n 's|^source = "git+https://github.com/trybotster/botster-core?rev=[^#]*#\([0-9a-f]*\)"$|\1|p' Cargo.lock | sort -u); do
  for candidate in "$CARGO_HOME"/git/checkouts/botster-core-*/"${sha:0:7}"/crates/botster-terminal-ghostty; do
    [ -f "$candidate/prefetch-zig.sh" ] && ghostty=$candidate
  done
done
[ -n "$ghostty" ] || { echo "fetch-public.sh: no cargo checkout of botster-terminal-ghostty for Cargo.lock's botster-core commits" >&2; exit 1; }

mkdir -p "$store/p"
# The same extraction as prefetch-zig.sh (it also matches ZIG_PACKAGES_IN_ZON); each hash once.
hashes=$(sed -n '/pub const ZIG_PACKAGES/,/^];/p' "$ghostty/build_data.rs" | sed -n 's/^ *"\(.*\)",$/\1/p' | sort -u)
# Prints the hashes whose archive is absent or broken.
missing() {
  local hash
  for hash in $hashes; do
    gzip -t "$store/p/$hash.tar.gz" 2>/dev/null || echo "$hash"
  done
}
if [ -z "$(missing)" ]; then
  echo "fetch: every Zig package of build_data.rs is in $store"
else
  # Under the store lock: check again (another gate may have published them meanwhile), prefetch into a staging directory on
  # the same file system, and publish each missing archive with a rename.
  export store hashes ghostty
  export -f missing
  flock "$store/.prefetch.lock" bash -c '
    set -euo pipefail
    todo=$(missing)
    [ -n "$todo" ] || { echo "fetch: another gate published the Zig packages"; exit 0; }
    staging=$(mktemp -d "$store/.staging.XXXXXX")
    trap "rm -rf \"$staging\"" EXIT
    echo "fetch: $(echo $todo | wc -w | tr -d " ") Zig packages are missing from $store; running prefetch-zig.sh"
    BOTSTER_ZIG_PACKAGES=$staging bash "$ghostty/prefetch-zig.sh"
    for hash in $todo; do
      gzip -t "$staging/p/$hash.tar.gz"
      mv -f "$staging/p/$hash.tar.gz" "$store/p/$hash.tar.gz"
    done
    echo "fetch: published $(echo $todo | wc -w | tr -d " ") Zig packages in $store"'
fi
