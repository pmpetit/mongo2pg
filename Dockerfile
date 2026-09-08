FROM rust:1.89-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir .cargo
COPY .cargo/config.toml .cargo/config.toml
COPY src ./src

RUN --mount=type=secret,id=cargo_token,env=CARGO_REGISTRIES_ARTIFACTORY_TOKEN \
    cargo build --release --bin mongo2pg

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gzip \
    libssl3 \
    postgresql-client \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

COPY --from=builder /app/target/release/mongo2pg /usr/local/bin/mongo2pg

ENTRYPOINT ["/usr/local/bin/mongo2pg"]
CMD ["--help"]