# MQ-rust: NameServer + Broker + TC (Raft) — single image
FROM rust:1.100-bookworm AS builder
WORKDIR /build
ENV CARGO_BUILD_JOBS=2
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN cargo build --release -p mq-broker -p mq-namesrv -p mq-tc

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/mq-broker /usr/local/bin/mq-broker
COPY --from=builder /build/target/release/mq-namesrv /usr/local/bin/mq-namesrv
COPY --from=builder /build/target/release/mq-tc /usr/local/bin/mq-tc
COPY deploy/native/broker.toml /etc/mq-rust/broker.toml
EXPOSE 9877 10921 9090 9091 9191
ENTRYPOINT ["/usr/local/bin/mq-broker"]
