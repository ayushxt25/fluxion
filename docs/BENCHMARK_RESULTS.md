# Benchmark Results

Fluxion does not publish a throughput claim without a captured result artifact.
Use the benchmark harness against an isolated PostgreSQL database whose name
ends in `_bench` and record the generated JSON path with the environment below.

| Field | Record |
| --- | --- |
| Command | Pending measured run |
| Machine / OS | Pending measured run |
| PostgreSQL / Redis configuration | Pending measured run |
| Worker count and concurrency | Pending measured run |
| Workload shape and executions | Pending measured run |
| JSON artifact path | Pending measured run |

Example command:

```bash
DATABASE_URL=postgresql+asyncpg://localhost/fluxion_bench fluxion benchmark \
  --workload single --runs 100000 --workers 16 \
  --output benchmark-results/100k.benchmark.json
```

Results vary by machine, database, Redis configuration, worker capacity, and
workload shape. A valid result includes correctness checks; invalid runs are not
performance results.
