#!/usr/bin/env sh
set -eu

until pg_isready -h "${POSTGRES_HOST:-postgres}" -U fluxion -d fluxion_test; do sleep 1; done
until redis-cli -h "${REDIS_HOST:-redis}" ping | grep -q PONG; do sleep 1; done

export DATABASE_URL="${DATABASE_URL:-postgresql+asyncpg://fluxion:fluxion@postgres:5432/fluxion_test}"
export TEST_DATABASE_URL="${TEST_DATABASE_URL:-$DATABASE_URL}"
export REDIS_URL="${REDIS_URL:-redis://redis:6379/0}"
export JWT_SECRET="${JWT_SECRET:-phase28-validation-secret}"

python -m pip install -q -r requirements-ci.txt -e .
python -m pytest tests/integration/test_webhooks.py tests/integration/test_webhook_api.py tests/integration/test_webhook_delivery.py -v
python -m pytest -m "not stress" -v
python -m pytest -v
python -m ruff check .
python -m compileall app tests alembic examples
git diff --check
python -m build

validation_dir="$(mktemp -d)"
python -m venv "$validation_dir/venv"
"$validation_dir/venv/bin/pip" install -q "$(pwd)/dist/fluxion-"*.whl
cd "$validation_dir"
"$validation_dir/venv/bin/fluxion" --version
"$validation_dir/venv/bin/fluxion-webhook" --help
"$validation_dir/venv/bin/fluxion" webhook --help
"$validation_dir/venv/bin/python" -c 'from app.sdk import FluxionClient, AsyncFluxionClient, verify_webhook_signature; import app.runtime.webhooks; print("SDK OK")'
