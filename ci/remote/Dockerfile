# Gate image for the shared test host (ci/remote/job.sh; botster-contracts docs/BUILD.md "How each stage runs"). It holds
# only the toolchain and the cargo tools. The source is mounted at run time, and cargo's home and the target directory live
# in volumes. job.sh tags the image with the hash of this file and rust-toolchain.toml, so a toolchain change builds a new
# image. Core's image (botster-core ci/remote/Dockerfile) without the nightly, cargo-bolero and cargo-public-api: the Hub's
# gate has no fuzz and no public-api step. It has Zig, because the Hub depends on botster-core, whose botster-terminal-ghostty
# builds libghostty with Zig.
FROM docker.io/library/debian:bookworm-slim

# The stable channel of rust-toolchain.toml (job.sh passes it).
ARG RUST_TOOLCHAIN
# libghostty builds with Zig (the same version and checksum as botster-core ci/remote/Dockerfile).
ARG ZIG_VERSION=0.16.0
ARG ZIG_SHA256=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00
ARG BUILD_JOBS=4

RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y \
      build-essential ca-certificates curl git libssl-dev pkg-config procps xz-utils && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

ENV RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:/usr/local/zig:$PATH

RUN test -n "$RUST_TOOLCHAIN" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      CARGO_HOME=/usr/local/cargo sh -s -- -y -q --no-modify-path --profile minimal --default-toolchain none && \
    CARGO_HOME=/usr/local/cargo rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal -c rustfmt -c clippy && \
    CARGO_HOME=/usr/local/cargo rustup default "$RUST_TOOLCHAIN"

# The versions of the Mac gate. Tools live in /usr/local/cargo/bin; cargo finds them on PATH while CARGO_HOME points at
# the volume.
RUN CARGO_HOME=/usr/local/cargo CARGO_BUILD_JOBS=$BUILD_JOBS cargo install -q --locked \
      cargo-nextest@0.9.146 && \
    rm -rf /usr/local/cargo/registry /usr/local/cargo/git

RUN curl -fsSL -o /tmp/zig.tar.xz "https://ziglang.org/download/$ZIG_VERSION/zig-x86_64-linux-$ZIG_VERSION.tar.xz" && \
    echo "$ZIG_SHA256  /tmp/zig.tar.xz" | sha256sum -c - && \
    mkdir /usr/local/zig && tar -xJf /tmp/zig.tar.xz -C /usr/local/zig --strip-components=1 && rm /tmp/zig.tar.xz && \
    zig version

ENV CARGO_HOME=/cargo \
    ZIG_GLOBAL_CACHE_DIR=/zig \
    CARGO_TERM_COLOR=never \
    TZ=UTC
