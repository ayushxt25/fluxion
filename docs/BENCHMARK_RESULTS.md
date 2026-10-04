# Benchmark Results

Fluxion's benchmark harness measures the full distributed execution path:
workflow submission, scheduler dispatch, PostgreSQL outbox persistence, Redis
transport, worker claim/execution, and durable completion. PostgreSQL
correctness checks run after each benchmark; an invalid result is not reported
as a performance result.

These figures are local measurements, not a hosted-production capacity claim.
They vary with machine, PostgreSQL and Redis configuration, worker capacity,
database history, and workload shape.

## Verified 100k single-task run

| Field | Result |
| --- | --- |
| Benchmark commit | [`bb9cee9`](https://github.com/ayushxt25/fluxion/commit/bb9cee9f4b29d698f0dc9f76e5e5273fe6ec628a) |
| Environment | Windows, Python 3.11.9 |
| Workload | 100,000 independent single-task workflow runs |
| Workers | 16 workers × 1 concurrency (16 effective loops) |
| Successful / failed / interrupted / cancelled | 100,000 / 0 / 0 / 0 |
| Submission duration | 739.993 s |
| Execution duration | 4,341.131 s |
| Submission-to-completion duration | 5,081.124 s |
| Execution throughput | 23.035 tasks/s |
| Submission-to-completion throughput | 19.681 tasks/s |
| Latency p50 / p95 / p99 | 2.338 s / 5.474 s / 5.990 s |
| Maximum queue depth | 82 |
| Scheduler ticks / total / average tick | 1,012 / 3,672.321 s / 3.629 s |
| Publisher active time | 306.562 s (about 326 publications/s while active) |
| Correctness | PASS |

The scheduler was the dominant bottleneck in this run. The publisher rate is a
component measurement and must not be read as end-to-end throughput.

## Earlier 10k comparison run

| Field | Result |
| --- | --- |
| Workload | 10,000 independent single-task workflow runs |
| Workers | 16 workers × 1 concurrency |
| Submission / execution / end-to-end | 73.845 s / 371.380 s / 445.225 s |
| Execution / end-to-end throughput | 26.927 / 22.461 tasks/s |
| Latency p50 / p95 / p99 | 1.928 s / 5.115 s / 5.372 s |
| Correctness | PASS |

## Reproducing a measurement

The harness refuses a database unless its name ends in `_bench`. It creates a
benchmark-specific Redis namespace and never calls `FLUSHDB` or `FLUSHALL`.
Use an isolated PostgreSQL database and retain the generated JSON artifact with
the result's environment details.

```bash
DATABASE_URL=postgresql+asyncpg://localhost/fluxion_bench fluxion benchmark \
  --workload single --runs 1000 --workers 2 \
  --output benchmark-results/local.benchmark.json

DATABASE_URL=postgresql+asyncpg://localhost/fluxion_bench fluxion benchmark \
  --workload single --runs 100000 --workers 16 \
  --output benchmark-results/100k.benchmark.json
```

The supported workload shapes are `single`, `linear`, and `fanout`. A valid
JSON result records configuration, execution count, throughput, latency
percentiles, backlog observations, and correctness violations. Do not compare
runs without recording their environment and workload shape.
