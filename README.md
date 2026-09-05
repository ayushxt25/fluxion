# Fluxion

Fluxion is a Python backend project that will grow into a distributed workflow
execution engine for DAG-based workflows.

The project is under active development. The current codebase includes the
initial FastAPI repository foundation, health-check surface, workflow/task
specification models, DAG validation, deterministic topological ordering,
in-memory workflow run state, dependency-based task readiness transitions, and
single-process local asynchronous workflow execution. Fluxion also includes
PostgreSQL-backed persistence models, repositories, and Alembic migrations for
workflow definitions, workflow run state, and durable local execution
transitions. Fluxion can also recover persisted local crash states by marking
abandoned `RUNNING` tasks as `INTERRUPTED` and can safely resume unambiguous
incomplete durable runs. Task execution now records explicit attempts with
configurable retry policy and deterministic exponential backoff. Task callables
can also receive an immutable execution context containing stable attempt and
task idempotency identities.

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

For local persistence, create PostgreSQL databases matching your configured
`DATABASE_URL` and `TEST_DATABASE_URL`, then run:

```bash
alembic upgrade head
```

Integration tests require `TEST_DATABASE_URL` and only run against databases
whose name ends with `_test`.

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
management, a health endpoint, immutable workflow specification models, and a
validated workflow DAG abstraction. It also tracks in-memory workflow run state
and task readiness based on completed dependencies, then can execute registered
Python task callables locally with concurrent execution for independent ready
tasks and optional local concurrency limiting. Workflow definitions and run/task
state can be persisted in PostgreSQL through SQLAlchemy repositories and Alembic
migrations. The local executor can persist task and workflow state transitions
durably as it runs. Recovery can detect stale local `RUNNING` task state after a
restart and reconcile readiness without executing task callables. Safe resume
can continue `PENDING` or `RUNNING` durable runs that contain no `RUNNING`,
`INTERRUPTED`, `FAILED`, or individually `CANCELLED` tasks. Previously
`SUCCEEDED` tasks are not rerun; execution continues from persisted `READY`
tasks. Ordinary callable failures can be retried according to each task's
policy, with durable attempt history and `next_retry_at` state so retry timing
survives process restart. Each task run has a durable deterministic
`idempotency_key`, and each attempt exposes a deterministic `attempt_key` to
context-aware task callables. Phase 10 adds a Redis-backed dispatch boundary:
a scheduler can persist `DISPATCHED` task attempts and publish versioned JSON
dispatch messages, while workers consume messages and resolve task
implementations locally. Phase 11 adds worker lease ownership, heartbeat
renewal, lease-token fencing, and explicit expired-lease reclaim. PostgreSQL
remains the source of truth; Redis is only transport. Empty workflows are
rejected because a workflow with zero executable tasks is not meaningful.

The original direct executor remains single-process and local. Redis dispatch
and worker services are currently a foundation, not a full distributed runtime.
Interrupted tasks are not retried automatically, and recovery does not
guarantee exactly-once effects for external side effects performed before a
crash. The idempotency key is an identity primitive only; tasks are responsible
for using it with external systems. Concurrent multi-process resume is not
supported; Fluxion does not yet provide distributed run ownership or resume
coordination.
Fluxion still does not provide a transactional outbox or exactly-once execution.
Database and Redis publishing are not atomic; a failed publish may leave a
stranded `DISPATCHED` task for a future reconciliation phase. Duplicate Redis
delivery may occur, and workers reject messages that do not match durable
PostgreSQL state. Expired leases are treated conservatively: the attempt and
task become `INTERRUPTED` and the workflow becomes `FAILED`; Fluxion does not
automatically retry ambiguous work. Stale workers cannot commit after lease loss
because terminal attempt updates require the current lease token. There is no
automatic scheduler or lease-reaper daemon yet.

For Phase 2, a failed task or individually cancelled task marks the workflow run
as failed because successful completion is no longer possible. Explicit workflow
cancellation marks remaining non-terminal tasks as cancelled and sets the run to
cancelled.
