# Changelog

## 1.0.0

### Distributed execution

- DAG workflow execution with immutable revisions, durable task attempts and
  results, retries, worker leases, and fencing.
- PostgreSQL-backed scheduling with Redis transport and at-least-once delivery.

### Durability and recovery

- Transactional dispatch outbox, stale-dispatch reconciliation, coordinator
  leases, safe recovery, and explicit resolution of ambiguous execution.

### Control plane and SDK

- FastAPI control plane with JWT/RBAC, audit records, event streaming, and
  typed synchronous and asynchronous Python SDKs.

### Scheduling and triggers

- Durable schedules, event triggers, signed webhook delivery, and retention.

### Observability and operations

- Structured logs, Prometheus-compatible metrics, health/readiness endpoints,
  operational runtimes, and production-style startup preflight checks.

### Reliability validation and benchmarking

- PostgreSQL/Redis integration, chaos and stress validation, migration checks,
  package/Docker checks, and reproducible end-to-end benchmark tooling.
