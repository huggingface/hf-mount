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
# Cargo features baked into the image. Override at build time, e.g.
# `--build-arg CARGO_FEATURES=fuse,vendored-openssl,heap-profiling` for a
# profiling-enabled canary.
ARG CARGO_FEATURES=fuse,vendored-openssl
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --no-default-features \
    --features "${CARGO_FEATURES}" --recipe-path recipe.json

# Stage 4 — build the actual binaries; deps come from the cooked cache.
FROM cook AS builder
ARG CARGO_FEATURES
COPY . .
RUN cargo build --release --no-default-features --features "${CARGO_FEATURES}" \
    --bin hf-mount-fuse --bin hf-mount-fuse-sidecar

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libfuse3-3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hf-mount-fuse /usr/local/bin/
COPY --from=builder /build/target/release/hf-mount-fuse-sidecar /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/hf-mount-fuse"]
