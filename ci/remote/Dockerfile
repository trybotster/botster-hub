# Gate image for the shared test host (ci/remote/job.sh; botster-contracts docs/BUILD.md "How each stage runs"). It holds
# only the toolchain and the cargo tools. The source is mounted at run time, and cargo's home and the target directory live
# in volumes. job.sh tags the image with the hash of this file and rust-toolchain.toml, so a toolchain change builds a new
# image. Core's image (botster-core ci/remote/Dockerfile) without Zig, the nightly, cargo-bolero and cargo-public-api: the
# Hub's gate has no libghostty build, no fuzz and no public-api step.
FROM docker.io/library/debian:bookworm-slim

# The stable channel of rust-toolchain.toml (job.sh passes it).
ARG RUST_TOOLCHAIN
ARG BUILD_JOBS=4

RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y \
      build-essential ca-certificates curl git libssl-dev pkg-config procps xz-utils && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

ENV RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH

RUN test -n "$RUST_TOOLCHAIN" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      CARGO_HOME=/usr/local/cargo sh -s -- -y -q --no-modify-path --profile minimal --default-toolchain none && \
    CARGO_HOME=/usr/local/cargo rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal -c rustfmt -c clippy && \
    CARGO_HOME=/usr/local/cargo rustup default "$RUST_TOOLCHAIN"

# The versions of the Mac gate. Tools live in /usr/local/cargo/bin; cargo finds them on PATH while CARGO_HOME points at
# the volume.
RUN CARGO_HOME=/usr/local/cargo CARGO_BUILD_JOBS=$BUILD_JOBS cargo install -q --locked \
      cargo-nextest@0.9.146 cargo-mutants@27.1.0 && \
    rm -rf /usr/local/cargo/registry /usr/local/cargo/git

# cargo-mutants builds its mutant copies under TMPDIR. The gate's /tmp is a tmpfs (job.sh: test temp roots off the disk,
# so writeback of a heavy writer in the same container cannot stall them), so this shim, first on PATH, gives
# cargo-mutants a directory on the target volume instead.
# That directory is inside the /work checkout, so the builds and tests that cargo-mutants starts get TMPDIR back on
# /tmp: a test's tempdir must not have /work as a parent repository. cargo-mutants starts every cargo call with $CARGO
# (cargo-mutants 27.1.0, src/cargo.rs cargo_bin), so the shim points $CARGO at a wrapper that sets TMPDIR and runs the
# cargo that started `cargo mutants`. The wrapper is not on PATH.
RUN mkdir -p /usr/local/botster/bin /usr/local/botster/libexec && \
    printf '%s\n' '#!/bin/sh' \
      'export BOTSTER_MUTANTS_CARGO="${CARGO:-cargo}"' \
      'export CARGO=/usr/local/botster/libexec/mutants-cargo' \
      'export TMPDIR="${BOTSTER_MUTANTS_TMPDIR:-/work/target/tmp}"' \
      'mkdir -p "$TMPDIR"' \
      'exec /usr/local/cargo/bin/cargo-mutants "$@"' > /usr/local/botster/bin/cargo-mutants && \
    printf '%s\n' '#!/bin/sh' \
      'export TMPDIR="${BOTSTER_TEST_TMPDIR:-/tmp}"' \
      'exec "$BOTSTER_MUTANTS_CARGO" "$@"' > /usr/local/botster/libexec/mutants-cargo && \
    chmod 755 /usr/local/botster/bin/cargo-mutants /usr/local/botster/libexec/mutants-cargo

ENV PATH=/usr/local/botster/bin:$PATH \
    CARGO_HOME=/cargo \
    CARGO_TERM_COLOR=never \
    TZ=UTC
