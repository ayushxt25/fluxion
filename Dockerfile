FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN adduser --disabled-password --gecos "" --uid 10001 fluxion

COPY pyproject.toml README.md requirements-ci.txt ./
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
RUN mkdir -p /app/scripts
COPY scripts/start_demo_runtime.sh ./scripts/start_demo_runtime.sh
COPY scripts/start_render_demo.sh ./scripts/start_render_demo.sh

RUN python -m pip install --no-cache-dir --upgrade -c requirements-ci.txt pip \
    && python -m pip install --no-cache-dir -c requirements-ci.txt hatchling \
    && python -m pip install --no-cache-dir --no-build-isolation -c requirements-ci.txt .
RUN chmod 755 /app/scripts/start_demo_runtime.sh /app/scripts/start_render_demo.sh

USER fluxion

CMD ["fluxion-api"]
