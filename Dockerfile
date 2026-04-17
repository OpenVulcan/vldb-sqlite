# syntax=docker/dockerfile:1.7

FROM rust:1.94-bookworm AS builder

WORKDIR /workspace

ARG TARGETPLATFORM

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libprotobuf-dev \
        libssl-dev \
        protobuf-compiler \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs rust-toolchain.toml ./
COPY proto ./proto
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=vldb-sqlite-cargo-registry-${TARGETPLATFORM},sharing=locked \
    --mount=type=cache,target=/workspace/target,id=vldb-sqlite-target-${TARGETPLATFORM},sharing=locked \
    cargo build --locked --release --bin vldb-sqlite \
    && install -Dm755 /workspace/target/release/vldb-sqlite /out/vldb-sqlite

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/config /app/data

COPY --from=builder /out/vldb-sqlite /usr/local/bin/vldb-sqlite
COPY docker/vldb-sqlite.json /app/config/vldb-sqlite.json

EXPOSE 19501
VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/vldb-sqlite"]
CMD ["--config", "/app/config/vldb-sqlite.json"]
