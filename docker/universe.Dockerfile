FROM python:3.13-slim-bookworm

ARG APP_UID=10001
ARG APP_GID=10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    EVENT_UNIVERSE_CONFIG=/etc/prediction-indexer/event_universe.json

RUN groupadd --gid "${APP_GID}" universe \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --create-home --shell /usr/sbin/nologin universe \
    && install -d -o universe -g universe /var/lib/event-universe \
    && python -m pip install "boto3>=1.43,<2" "google-cloud-storage>=3.4,<4" "zstandard>=0.23"

WORKDIR /app

COPY encoder/ ./encoder/
COPY archive/ ./archive/
COPY targeter/ ./targeter/
COPY universe/ ./universe/
COPY configs/event_universe.json /etc/prediction-indexer/event_universe.json

USER universe:universe

EXPOSE 8080

CMD ["python", "-u", "universe/run_server.py"]
