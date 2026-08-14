FROM python:3.13-slim-bookworm

ARG APP_UID=10001
ARG APP_GID=10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --gid "${APP_GID}" indexer \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --create-home --shell /usr/sbin/nologin indexer \
    && mkdir -p /var/lib/prediction-indexer \
    && chown indexer:indexer /var/lib/prediction-indexer

WORKDIR /app

# Install the declared runtime dependencies in a layer that changes only when
# packaging metadata or an installable package changes.
COPY pyproject.toml ./
COPY analysis/ ./analysis/
COPY replay/ ./replay/
# The shared Zstandard codec. Installed rather than copied beside the entrypoints
# because the archiver imports it as a library, and Zstd Step 2 gives the Rust
# finalizer the matching crate from the same directory.
COPY encoder/ ./encoder/
RUN python -m pip install .

# Splices, targeter and the archive commands are intentionally source-level
# process entrypoints. They share this immutable image but are launched as
# independent Compose services.
COPY splices/ ./splices/
COPY targeter/ ./targeter/
COPY archive/ ./archive/
COPY universe/ ./universe/
COPY configs/ ./configs/
COPY docker/wait_for_target.py ./docker/wait_for_target.py
# Named one file at a time, as `wait_for_target.py` is. Most of `scripts/` is
# offline research tooling with dependencies this image deliberately does not
# carry; this one is an operational repair that has to run where the data is.
COPY scripts/backfill_coverage.py ./scripts/backfill_coverage.py

USER indexer:indexer
