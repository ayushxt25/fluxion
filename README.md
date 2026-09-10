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
task idempotency identities. Redis dispatch now uses a PostgreSQL transactional
outbox so dispatch intent is durable before transport publication. Long-running
scheduler, outbox publisher, and lease reaper loops are available as explicit
engine services. Fluxion also exposes a versioned REST control plane for
workflow definitions, durable run inspection, cancellation, recovery,
continuation checks, and selected operational one-shot actions.
Phase 17 adds structured request and engine logging, Prometheus-compatible
process metrics at `/metrics`, and a readiness probe at `/ready`.
Phase 18 adds process entrypoints and Docker Compose wiring for a local
multi-process cluster with separate API, scheduler, publisher, reaper, worker,
PostgreSQL, Redis, and migration roles.
Phase 19 adds a built-in deterministic demo task pack and a `fluxion demo`
smoke command that drives the public API and proves the distributed execution
path through scheduler, transactional outbox, Redis, worker leasing, task
execution, downstream unlock, and durable workflow success.
Phase 20 adds durable JSON task results and direct dependency data passing
through `TaskExecutionContext.dependency_results`.

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

## Run The Processes

After installing the package, Fluxion exposes console scripts:

- `fluxion-api`
- `fluxion-scheduler`
- `fluxion-publisher`
- `fluxion-reaper`
- `fluxion-worker`
- `fluxion-demo`

The equivalent grouped command is
`fluxion api|scheduler|publisher|reaper|worker|demo`.
Each process loads settings once, configures logging once, opens shared
PostgreSQL/Redis resources for its role, and shuts down on `SIGINT`/`SIGTERM`.

`fluxion-api` runs Uvicorn with `API_HOST` and `API_PORT`:

```bash
fluxion-api
```

Health check:

```bash
curl http://127.0.0.1:8000/health
```

Readiness checks PostgreSQL and Redis without exposing connection details:

```bash
curl http://127.0.0.1:8000/ready
```

Prometheus-compatible process metrics are exposed at `/metrics`. The endpoint
uses low-cardinality labels such as HTTP method, route template, and status; it
does not label metrics by run ID, workflow ID, task ID, request ID, or user.

Worker task implementations are not uploaded through the API. Deployments
register task callables by editing the application-side hook:
`app.tasks.registry.build_task_registry()`. The default hook registers only the
safe built-in demo tasks `demo.prepare`, `demo.process`, and `demo.finalize` so
local smoke tests can run without arbitrary code upload.

## Demo Smoke Test

With the local cluster running, the demo command creates a unique workflow and
run through the public API, then polls until the distributed worker completes
all demo tasks:

```bash
export FLUXION_API_URL=http://localhost:8000
export FLUXION_API_TOKEN=<operator-or-admin-jwt>
fluxion demo
```

On Windows PowerShell:

```powershell
$env:FLUXION_API_URL="http://localhost:8001"
$env:FLUXION_API_TOKEN="<operator-or-admin-jwt>"
fluxion demo
```

`FLUXION_API_URL` is configurable because local Docker port mappings may vary.
If `AUTH_ENABLED=false`, the token can be omitted. The command also accepts
`--api-url`, `--token`, `--timeout`, and `--poll`.

Expected output is concise:

```text
Created workflow: demo-workflow-...
Created run: demo-run-...
Run status: RUNNING
demo.prepare: SUCCEEDED
demo.process: SUCCEEDED
demo.finalize: SUCCEEDED
Workflow: SUCCEEDED
```

The demo proves the API-to-worker path: workflow persistence, run persistence,
scheduler dispatch intent, outbox publication, Redis transport, worker claim,
lease/heartbeat infrastructure, task execution, dependency result passing,
dependent unlock, and durable workflow completion. The built-in demo tasks are
deterministic verification tasks for local smoke testing, not production
business logic. `demo.prepare` returns `{"value": 21}`, `demo.process` reads
that direct dependency result and returns `{"value": 42}`, and `demo.finalize`
returns a final summary.

## Task Results

Successful task callables may return `None` or a JSON-serializable value:
objects, arrays, strings, numbers, booleans, or null. Fluxion never pickles
task outputs and does not support binary results. Before a task can become
durably `SUCCEEDED`, its canonical result is JSON-normalized and persisted on
the `task_runs` row in the same transaction as the success transition.

Context-aware downstream tasks receive an immutable
`dependency_results` mapping in `TaskExecutionContext`. It contains only direct
dependencies, and each dependency must already be `SUCCEEDED`; root tasks
receive an empty mapping. A task that returned `None` is stored as a present
JSON null result, distinct internally from a task that has never succeeded.

`MAX_TASK_RESULT_BYTES` limits the UTF-8 encoded JSON result size and defaults
to 262144 bytes. Unsupported or oversized results are treated like ordinary
task failures and follow existing retry policy. Failed attempts do not write a
canonical task result; the final successful retry writes the task result.
Recovery, resume, and cancellation preserve already successful task results.
Task inspection API responses include `result` and `has_result`.

Fluxion does not yet provide artifact storage, blob storage, result streaming,
schema registries, cross-workflow data sharing, or secret/result templating.

## Docker Compose

For a local multi-process cluster:

```bash
docker compose build
docker compose up -d postgres redis
docker compose run --rm migrate
docker compose up -d api scheduler publisher reaper worker
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

Compose defines:

- `postgres`
- `redis`
- `migrate`
- `api`
- `scheduler`
- `publisher`
- `reaper`
- `worker`

All Fluxion application services reuse the same image with different commands.
The `migrate` service runs `alembic upgrade head` once; the long-running
services depend on its successful completion so every role does not race schema
migrations. Local Compose defaults use development PostgreSQL credentials, but
`JWT_SECRET` must be supplied through the environment and is not baked into the
image.

Control-plane endpoints live under `/api/v1`:

- `POST /api/v1/workflows`
- `GET /api/v1/workflows`
- `GET /api/v1/workflows/{workflow_id}`
- `POST /api/v1/workflows/{workflow_id}/runs`
- `GET /api/v1/runs`
- `GET /api/v1/runs/{run_id}`
- `GET /api/v1/runs/{run_id}/tasks`
- `GET /api/v1/runs/{run_id}/tasks/{task_id}/attempts`
- `POST /api/v1/runs/{run_id}/cancel`
- `POST /api/v1/runs/{run_id}/recover`
- `POST /api/v1/runs/{run_id}/resume`
- `POST /api/v1/ops/scheduler/tick`
- `POST /api/v1/ops/outbox/publish`
- `POST /api/v1/ops/leases/reap`

Example workflow definition:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{"id":"order-pipeline","name":"Order Pipeline","tasks":[{"id":"validate","depends_on":[]}]}'
```

The generated OpenAPI documentation is available from FastAPI at `/docs` and
`/openapi.json`.

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
context-aware task callables. Redis-backed dispatch is split across a scheduler,
transactional dispatch outbox, explicit publisher, and worker. The scheduler
persists `DISPATCHED` task attempts and matching outbox rows atomically; the
publisher later claims outbox rows and sends versioned JSON messages to Redis.
Outbox claim tokens and expiry reduce duplicate concurrent publication while
remaining retryable after publisher crashes. Phase 11 adds worker lease
ownership, heartbeat renewal, lease-token fencing, and explicit expired-lease
reclaim. The REST API is a control plane only: it persists and inspects
workflow state, but it does not upload task code or execute arbitrary task
callables inside the API process. PostgreSQL remains the source of truth; Redis
is only transport. Empty workflows are rejected because a workflow with zero
executable tasks is not meaningful.

The original direct executor remains single-process and local. Redis dispatch
and worker services are currently a foundation, not a full distributed runtime.
Interrupted tasks are not retried automatically, and recovery does not
guarantee exactly-once effects for external side effects performed before a
crash. The idempotency key is an identity primitive only; tasks are responsible
for using it with external systems. Concurrent multi-process resume is not
supported; Fluxion does not yet provide distributed run ownership or resume
coordination. The outbox provides at-least-once publication intent, not
exactly-once delivery or exactly-once execution. Redis message loss after an
outbox row is marked published is not automatically detected, and concurrent
outbox publisher claims do not remove the publish/crash duplicate window.
Duplicate Redis delivery may occur, and workers reject messages that do not
match durable PostgreSQL state. Expired leases are treated conservatively: the
attempt and task become `INTERRUPTED` and the workflow becomes `FAILED`;
Fluxion does not automatically retry ambiguous work. Stale workers cannot
commit after lease loss because terminal attempt updates require the current
lease token and a still-running task row. Service loops support graceful
shutdown, but there is no Kubernetes/process supervisor or exactly-once
execution guarantee yet. The Docker Compose file is for local development and
smoke testing, not production orchestration. The REST API uses bearer JWT authentication and RBAC
when `AUTH_ENABLED=true`. Roles are `viewer` for read-only workflow/run
inspection, `operator` for workflow/run creation and safe control actions, and
`admin` for operational endpoints under `/api/v1/ops`. Required JWT settings
include `JWT_SECRET`, `JWT_ALGORITHM=HS256`, `JWT_ISSUER`, `JWT_AUDIENCE`, and
`JWT_ACCESS_TOKEN_MINUTES`. Development tests can mint tokens with
`app.security.auth.create_access_token(subject, role)`. Setting
`AUTH_ENABLED=false` treats requests as an internal admin principal and is only
appropriate for local development, never exposed deployments. Authenticated API
traffic is rate-limited through Redis by principal subject, with default
per-minute limits of 120 for viewers, 90 for operators, 60 for admins, and 20
for operational endpoints. Rate-limit denials return `429` with `Retry-After`;
Redis rate-limit failures return `503` rather than silently disabling
protection. Security-sensitive actions and denials are written to a durable
`audit_events` table and can be inspected by admins at `/api/v1/ops/audit`.
Audit events include request ID, principal subject/role, action, resource,
outcome, and sanitized metadata; they do not store JWTs, secrets, or lease
tokens. API responses include lightweight security headers, and oversized
request bodies are rejected according to `MAX_REQUEST_BODY_BYTES`.
Structured logs are configurable with `LOG_LEVEL` and `LOG_FORMAT=json|text`.
Request completion logs include request ID, method, route template, status,
duration, and authenticated principal when available. Logs and metrics
intentionally exclude JWTs, authorization headers, lease tokens, database
credentials, Redis credentials, and other secrets. `/health` is liveness only
and does not check dependencies; `/ready` verifies PostgreSQL and Redis with
`READINESS_TIMEOUT_SECONDS`. Metrics are in-process only; multiprocess
Prometheus registry support is not implemented yet, and `/metrics` should be
protected by network controls in production.

There is no public login, user database, OAuth/OIDC provider, refresh token
flow, per-workflow ACL, SIEM export, OpenTelemetry tracing, external log
service, alerting system, object/blob result store, or rate-limit fallback store yet. Worker
implementations must already be deployed and registered in worker processes.
The API does not expose lease tokens, and unpublished outbox dispatches whose
task is later cancelled are discarded instead of being published as stale Redis
messages.
The built-in demo task pack is intentionally tiny and side-effect free. Real
deployments should replace or extend the registry hook with their own audited
task implementations.

For Phase 2, a failed task or individually cancelled task marks the workflow run
as failed because successful completion is no longer possible. Explicit workflow
cancellation marks remaining non-terminal tasks as cancelled and sets the run to
cancelled.
