FROM rust:1.90-bookworm AS rust-builder

WORKDIR /build
COPY encoder/rust/ encoder/rust/
COPY ingester/ ingester/
COPY engine/ engine/
RUN cargo build --locked --release --manifest-path engine/Cargo.toml \
      -p replay-transport --bin replay-publish \
    && cargo build --locked --release --manifest-path engine/Cargo.toml \
      -p replay-normalizers --example materialize_range

FROM python:3.13-slim-bookworm

ARG APP_UID=1000
ARG APP_GID=1000
ARG REPLAY_IMAGE_REVISION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    REPLAY_IMAGE_REVISION=${REPLAY_IMAGE_REVISION} \
    REPLAY_MATERIALIZER=/usr/local/bin/materialize_range \
    REPLAY_PUBLISHER=/usr/local/bin/replay-publish \
    REPLAY_DATA_ROOT=/var/lib/replay \
    EVENT_UNIVERSE_CONFIG=/etc/prediction-indexer/event_universe.json

RUN case "${REPLAY_IMAGE_REVISION}" in \
      [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;; \
      *) echo "REPLAY_IMAGE_REVISION must be a full Git SHA" >&2; exit 1 ;; \
    esac \
    && groupadd --gid "${APP_GID}" replay \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /usr/sbin/nologin replay \
    && install -d -m 0700 -o replay -g replay /var/lib/replay \
    && printf '%s\n' "${REPLAY_IMAGE_REVISION}" > /etc/prediction-indexer-replay-image-revision

WORKDIR /app
COPY pyproject.toml ./
COPY encoder/ ./encoder/
COPY splices/__init__.py ./splices/
COPY splices/common/ ./splices/common/
COPY analysis/ ./analysis/
COPY replay/ ./replay/
COPY archive/ ./archive/
COPY targeter/ ./targeter/
COPY universe/ ./universe/
COPY configs/event_universe.json /etc/prediction-indexer/event_universe.json
COPY configs/replay_runner.json /etc/prediction-indexer/replay_runner.json
COPY --from=rust-builder /build/engine/target/release/replay-publish /usr/local/bin/replay-publish
COPY --from=rust-builder /build/engine/target/release/examples/materialize_range /usr/local/bin/materialize_range

RUN python -m pip install ".[replay-redis]" \
    && test -x /usr/local/bin/replay-publish \
    && materialize_range --describe >/tmp/materializer-descriptor.json \
    && python -c "import json,replay.jobs,replay.ops; json.load(open('/tmp/materializer-descriptor.json'))" \
    && rm /tmp/materializer-descriptor.json

LABEL org.opencontainers.image.revision=${REPLAY_IMAGE_REVISION}

USER replay:replay

ENTRYPOINT ["python", "-m", "replay.jobs"]
CMD ["tick", "/etc/prediction-indexer/replay_runner.json"]
