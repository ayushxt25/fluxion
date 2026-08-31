# Fluxion

Fluxion is a Python backend project that will grow into a distributed workflow
execution engine for DAG-based workflows.

The project is under active development. The current codebase is only the
initial FastAPI repository foundation and health-check surface.

## Planned Capabilities

- Workflow definitions and DAG validation
- Durable workflow and task run state
- Scheduling and dispatching
- Worker execution, heartbeats, and lease-based ownership
- Retries, task re-queuing, failure recovery, and idempotency
- PostgreSQL persistence and Redis coordination
- Concurrency control, observability, and horizontal scaling

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Copy `.env.example` to `.env` for local configuration.

## Run The API

```bash
uvicorn app.main:app --reload
```

Health check:

```bash
curl http://127.0.0.1:8000/health
```

## Tests And Linting

```bash
python -m pytest
ruff check .
```

## Current Status

Fluxion currently provides a modular async-first FastAPI skeleton, settings
management, and a health endpoint. Distributed execution features are planned
but not implemented yet.
