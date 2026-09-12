FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN adduser --disabled-password --gecos "" fluxion

COPY pyproject.toml README.md requirements-ci.txt ./
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN python -m pip install --no-cache-dir --upgrade -c requirements-ci.txt pip \
    && python -m pip install --no-cache-dir -c requirements-ci.txt hatchling \
    && python -m pip install --no-cache-dir --no-build-isolation -c requirements-ci.txt .

USER fluxion

CMD ["fluxion-api"]
