# Multi-stage build for rd-rs (Real-Debrid FUSE server)

# Use BUILDPLATFORM so the builder runs natively (avoiding QEMU slow down)
FROM --platform=$BUILDPLATFORM rust:1.94-slim AS builder

WORKDIR /build

# TARGETARCH is automatically set by Docker Buildx (e.g., amd64, arm64)
ARG TARGETARCH

# Install native and cross-compilation C toolchain components
# so the `cc` crate can compile bundled C dependencies (like sqlite3) natively or cross.
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    make \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*

# Add Rust targets for supported architectures
RUN rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src

# Conditionally compile and link based on TARGETARCH
RUN set -ex; \
    mkdir -p target/release; \
    if [ "$TARGETARCH" = "arm64" ]; then \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc; \
        export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc; \
        export CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++; \
        cargo build --release --locked --target aarch64-unknown-linux-gnu --bin rd-rs; \
        strip target/aarch64-unknown-linux-gnu/release/rd-rs; \
        cp target/aarch64-unknown-linux-gnu/release/rd-rs target/release/rd-rs; \
    else \
        cargo build --release --locked --target x86_64-unknown-linux-gnu --bin rd-rs; \
        strip target/x86_64-unknown-linux-gnu/release/rd-rs; \
        cp target/x86_64-unknown-linux-gnu/release/rd-rs target/release/rd-rs; \
    fi

# The runtime image
FROM debian:stable-slim

# Note: Since SQLite is statically bundled, we only need fuse3 and ca-certs
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
    fuse3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/rd-rs /app/rd-rs
COPY config.example.toml /app/config.toml

RUN mkdir -p /data/cache /mnt/rd

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/app/rd-rs"]
