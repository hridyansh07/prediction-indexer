# Corpus runner (replay/corpus) for a dedicated Linux machine.
#
# This image is the orchestrator only: Python with the repository's
# dependencies, the Linux materializer, git, and the Docker CLI. Each bundle's
# bench still runs in its own replay-runner image and Redis container, started
# through the host daemon's socket. compose.corpus.yaml runs the mounted
# repository checkout, so the orchestrator and the bench image come from the
# same commit.

FROM rust:1.90-bookworm AS rust-builder

WORKDIR /build
COPY encoder/rust/ encoder/rust/
COPY ingester/ ingester/
COPY engine/ engine/
COPY replay/streams/attempt.lua replay/streams/attempt.lua
RUN cargo build --locked --release --manifest-path engine/Cargo.toml \
      -p replay-normalizers --example materialize_range

FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Docker's own Debian repository: the CLI and buildx only, never the daemon.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-buildx-plugin \
    && apt-get purge -y gnupg && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dependencies
COPY pyproject.toml ./
COPY encoder/ ./encoder/
COPY splices/__init__.py ./splices/
COPY splices/common/ ./splices/common/
COPY analysis/ ./analysis/
COPY replay/ ./replay/
COPY archive/ ./archive/
COPY targeter/ ./targeter/
COPY universe/ ./universe/
# Dependencies only. The package itself is the mounted checkout, so drop it.
RUN python -m pip install ".[replay-redis]" \
    && python -m pip uninstall -y prediction-indexer \
    && rm -rf /opt/dependencies

COPY --from=rust-builder /build/engine/target/release/examples/materialize_range /usr/local/bin/materialize_range
RUN materialize_range --describe >/dev/null && docker --version && docker buildx version

ENTRYPOINT ["python", "-m", "replay.corpus"]
