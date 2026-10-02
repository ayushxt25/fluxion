# Fluxion 1.0

Fluxion 1.0 is a PostgreSQL-backed distributed DAG execution engine. PostgreSQL
is canonical state; Redis is dispatch transport. Workflows use durable attempts
and results, transactional outbox dispatch, worker and coordinator fencing,
retry/recovery, immutable revisions, schedules, event triggers, signed
webhooks, retention, and a typed Python SDK.

The project includes metrics, structured logs, readiness checks, deployment
guidance, and a reproducible benchmark harness that exercises the real
scheduler/outbox/Redis/worker path.

## Important limits

- Delivery and execution are at least once, not exactly once.
- External side effects require task-level idempotency.
- `INTERRUPTED` work is not retried automatically; an administrator explicitly
  chooses RETRY or FAIL through a durable intervention.
- Task implementations must be deployed with workers; Fluxion does not upload
  arbitrary code.
- Compose is a local production-style topology, not a production orchestrator.

After CI is green, a maintainer may create the release tag:

```bash
git tag -a v1.0.0 -m "Fluxion 1.0.0"
git push origin v1.0.0
```
