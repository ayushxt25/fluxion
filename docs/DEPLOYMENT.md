# Deployment

Fluxion is designed for a multi-process deployment with PostgreSQL as canonical
state and Redis as dispatch transport. This document describes a production-style
starting point, not a universal production certification.

## Prerequisites and migration order

Provide managed or otherwise externally operated PostgreSQL and Redis. Run the
dedicated migration job first:

```bash
alembic upgrade head
```

Each runtime verifies PostgreSQL reachability and the Alembic head revision at
startup. Roles that use dispatch also verify Redis. They do not run migrations
themselves. A revision mismatch or unavailable dependency is a fatal startup
failure and exits non-zero without printing connection credentials.

## Required configuration

Set `APP_ENV=production`. Production validation requires `AUTH_ENABLED=true`,
`DEBUG=false`, JSON logs, a non-default JWT secret of at least 32 characters,
and non-development PostgreSQL/Redis URLs. Inject `JWT_SECRET`, database, Redis,
and webhook secrets with the deployment platform's secret mechanism; do not put
them in images, source control, or command lines.

| Area | Important settings |
| --- | --- |
| Core | `APP_ENV`, `DATABASE_URL`, `REDIS_URL`, `READINESS_TIMEOUT_SECONDS` |
| API/auth | `AUTH_ENABLED`, `JWT_SECRET`, `JWT_ISSUER`, `JWT_AUDIENCE`, request/rate limits |
| Worker | `WORKER_CONCURRENCY`, `WORKER_LEASE_SECONDS`, `WORKER_HEARTBEAT_SECONDS`, `WORKER_SHUTDOWN_GRACE_SECONDS` |
| Scheduler/publisher | scheduler dispatch limits, `OUTBOX_BATCH_SIZE`, `OUTBOX_CLAIM_SECONDS` |
| Retention | `RETENTION_ENABLED`, retention days, batch size, poll interval |
| Webhooks | allow-private/insecure flags, timeout, retry and claim settings |
| Observability | `LOG_LEVEL`, `LOG_FORMAT`, readiness timeout |

Use TLS termination at the ingress or proxy. Restrict PostgreSQL, Redis, and the
metrics endpoint to trusted networks. Workers expose no public port. Apply
egress policy appropriate for webhook destinations, restrict administrative API
access, and maintain database backup/restore procedures outside Fluxion.

## Roles and scaling

Run the API, scheduler, publisher, reaper, coordinator, and one or more workers.
Enable webhooks, retention, schedule runner, and reconciler when their features
are used. Scale worker replicas and tune `WORKER_CONCURRENCY` from observed
load; more workers are not automatically faster.

The supplied Compose file is a production-like local topology, not an
orchestrator. It runs a one-shot migration service before application roles and
uses restart policies for long-running roles. Start it with a strong
`JWT_SECRET`; scale workers with `docker compose up --scale worker=4`.

## Northflank Sandbox portfolio deployment

For a free-tier public portfolio deployment, use two Northflank services backed
by the PostgreSQL addon and an external Upstash Redis instance:

| Location | Role | Command / exposure |
| --- | --- | --- |
| Northflank service | API | `fluxion-api`; public port `8000` |
| Northflank service | Demo runtime | `sh scripts/start_demo_runtime.sh`; no public port |
| Northflank addon | PostgreSQL | private service dependency |
| Upstash | Redis transport | external `REDIS_URL` (`redis://` or `rediss://`) |
| Vercel | Next.js frontend | public frontend only |

The demo-runtime wrapper starts scheduler, publisher, worker, and reaper as
separate child processes and stops all of them if one exits unexpectedly. The
public portfolio deployment co-locates background runtimes in one container to
fit free-tier hosting limits. Fluxion’s normal deployment model keeps these
runtimes independently deployable.

Set these service environment variables through Northflank secrets/configuration
rather than repository files:

```dotenv
APP_ENV=production
DATABASE_URL=<postgres asyncpg URL>
REDIS_URL=<redis/rediss URL>
AUTH_ENABLED=true
JWT_SECRET=<secret>
JWT_ALGORITHM=HS256
JWT_ISSUER=fluxion
JWT_AUDIENCE=fluxion-api
LOG_LEVEL=INFO
LOG_FORMAT=json
READINESS_TIMEOUT_SECONDS=10
```

Configure `WORKER_CONCURRENCY` and the scheduler/outbox batch and polling
settings for the small demo workload if needed; do not use benchmark-only
settings as a production default. `rediss://` URLs work with Fluxion's
redis-py clients and are appropriate for Upstash TLS endpoints.

Run migrations before either service starts. Use a Northflank deployment or
pre-start job when available, with the command:

```sh
alembic upgrade head
```

Do not add automatic migrations to the API or demo-runtime start commands.

For the Vercel frontend, configure server-only environment variables:

```dotenv
FLUXION_API_URL=https://<public-api-domain>
FLUXION_API_TOKEN=<restricted demo operator token>
```

Never prefix `FLUXION_API_TOKEN` with `NEXT_PUBLIC_`, and do not use an admin
token. Keep the demo-runtime service private, leave authentication and rate
limits enabled, restrict PostgreSQL/Redis network access, and do not expose
database, Redis, or task-upload controls through the frontend.

## Health, readiness, and shutdown

`/health` is process liveness only. `/ready` performs bounded PostgreSQL and
Redis checks and returns 503 when either dependency is unavailable, without
including connection details. Kubernetes or another supervisor should use
`/health` for liveness and `/ready` for traffic admission.

SIGINT/SIGTERM stops runtime loops, allows the worker grace period for active
bounded work, and closes Redis clients and SQLAlchemy engines. Correctness does
not depend on graceful shutdown: worker leases and fencing protect task
ownership, PostgreSQL outbox state preserves dispatch intent, and Redis delivery
remains at least once.

## Rolling restarts and failure modes

Scheduler, publisher, reaper, and coordinator replicas may restart. The
coordinator lease fences stale run coordination; worker leases fence stale task
owners. A crashed publisher leaves durable outbox work for a later publisher.
If a worker disappears after execution may have begun, Fluxion conservatively
records an interrupted attempt; an administrator may resolve its durable
intervention with RETRY or FAIL. RETRY can repeat external side effects.

PostgreSQL loss prevents ready roles from starting and makes `/ready` fail.
Redis loss prevents dispatch-dependent roles from starting and makes `/ready`
fail, while PostgreSQL remains the source of truth. Webhook target failures use
their existing durable delivery retry policy. A failed migration blocks dependent
roles in Compose. Plan backward-compatible migrations for rolling deployments;
Fluxion does not guarantee universal zero-downtime migrations.

## Checklist

- [ ] `APP_ENV=production`, `AUTH_ENABLED=true`, and `DEBUG=false`
- [ ] Strong JWT secret injected through the platform
- [ ] Migration job completed before service roles start
- [ ] PostgreSQL and Redis are reachable from required roles
- [ ] API `/ready` is green and metrics are network protected
- [ ] Required task implementations are present in worker images
- [ ] Scheduler, publisher, reaper, coordinator, and workers are running
- [ ] Worker concurrency and retention policy are reviewed
- [ ] Webhook egress policy and API administrative access are restricted
- [ ] Backup/restore, log collection, and alerting are configured externally
