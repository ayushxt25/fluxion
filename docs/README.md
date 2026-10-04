# Fluxion documentation

Fluxion is a PostgreSQL-backed distributed DAG execution engine. This index is
the shortest path to the operational and design material behind the README.

| Document | Covers |
| --- | --- |
| [Architecture](ARCHITECTURE.md) | Canonical state, dispatch, recovery, ownership, failure behavior, and repository layout. |
| [Deployment](DEPLOYMENT.md) | Configuration, runtime roles, probes, migration order, security boundaries, and public-demo hosting. |
| [Benchmark results](BENCHMARK_RESULTS.md) | Captured local 10k/100k measurements, methodology, and reproduction commands. |
| [Fluxion 1.0 release notes](RELEASE_1_0.md) | Release scope and important semantic limits. |
| [Release checklist](RELEASE.md) | Maintainer checks before publishing a release. |
| [Screenshot guide](SCREENSHOTS.md) | Checked-in dashboard captures and safe capture standards. |

The public README describes the runtime at a high level. It does not override
the delivery and failure semantics documented in the architecture guide:
PostgreSQL is canonical, Redis transport is at least once, and external side
effects require idempotency.
