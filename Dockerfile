# Multi-stage build for rd-rs (Real-Debrid FUSE server)

FROM rust:1.94-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libsqlite3-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src
RUN cargo build --release --locked --bin rd-rs && \
    strip target/release/rd-rs

FROM debian:stable-slim

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
