# syntax=docker/dockerfile:1.7

# Stage 1 — chef: install cargo-chef on top of the rust toolchain image.
FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# Stage 2 — planner: compute the dependency-only recipe from Cargo manifests.
# Only Cargo.toml / Cargo.lock changes invalidate this layer, so source-only
# edits skip straight to the cached `cook` layer below.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3 — cook: build *only* the dependency graph using the recipe.
# This layer is reused as long as the recipe hash is unchanged.
FROM chef AS cook
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --no-default-features \
    --features fuse,vendored-openssl --recipe-path recipe.json

# Stage 4 — build the actual binaries; deps come from the cooked cache.
FROM cook AS builder
COPY . .
RUN cargo build --release --no-default-features --features fuse,vendored-openssl \
    --bin hf-mount-fuse --bin hf-mount-fuse-sidecar

# Runtime
FROM debian:bookworm-slim
# `upgrade` pulls the Debian security updates the base image is behind on at build
# time (2026-09-28: openssl/libssl3 3.0.20-1~deb12u2 shipped while 3.0.22-1~deb12u1
# fixed CVE-2026-75803), so a rebuild is enough to clear an OS-package CVE. The layer
# is cached in the registry: APT_REFRESH is set to the ISO week by the build, so the
# cache expires weekly instead of at the next base-image bump. BuildKit only misses
# the cache where the ARG is used, hence the echo.
ARG APT_REFRESH=unset
RUN echo "apt refresh: ${APT_REFRESH}" && apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends libfuse3-3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hf-mount-fuse /usr/local/bin/
COPY --from=builder /build/target/release/hf-mount-fuse-sidecar /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/hf-mount-fuse"]
