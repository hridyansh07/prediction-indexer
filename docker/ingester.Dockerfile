FROM rust:1.85-bookworm AS builder

WORKDIR /build
COPY ingester/Cargo.toml ingester/Cargo.lock ./
COPY ingester/crates/ ./crates/
COPY encoder/rust/ /encoder/rust/
RUN cargo build --locked --release --package indexer-cli

FROM debian:bookworm-slim

ARG APP_UID=10001
ARG APP_GID=10001

RUN groupadd --gid "${APP_GID}" indexer \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --create-home --shell /usr/sbin/nologin indexer \
    && mkdir -p /var/lib/prediction-indexer \
    && chown indexer:indexer /var/lib/prediction-indexer

# The ingest and finalize binaries ship with the canonical audit and one-shot
# ingest-store retention commands. The first two read the same spool and assign two
# different global orders over it — `indexer-ingest` in filename order
# (`file_order`), `indexer-finalize` on `(visible_ns, lane_rank,
# delivery_index)`. The finalizer service overrides the entrypoint.
COPY --from=builder /build/target/release/indexer-ingest /usr/local/bin/indexer-ingest
COPY --from=builder /build/target/release/indexer-store-reap /usr/local/bin/indexer-store-reap
COPY --from=builder /build/target/release/indexer-finalize /usr/local/bin/indexer-finalize
COPY --from=builder /build/target/release/indexer-canonical-audit /usr/local/bin/indexer-canonical-audit

USER indexer:indexer
ENTRYPOINT ["/usr/local/bin/indexer-ingest"]
