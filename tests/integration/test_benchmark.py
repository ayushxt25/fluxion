import os
from urllib.parse import urlparse

import pytest

BENCHMARK_DATABASE_URL = os.getenv("BENCHMARK_DATABASE_URL")
BENCHMARK_REDIS_URL = os.getenv("BENCHMARK_REDIS_URL")
if not BENCHMARK_DATABASE_URL or not BENCHMARK_REDIS_URL:
    pytest.skip(
        "BENCHMARK_DATABASE_URL and BENCHMARK_REDIS_URL are required",
        allow_module_level=True,
    )

benchmark_database_name = urlparse(BENCHMARK_DATABASE_URL).path.rsplit("/", maxsplit=1)[
    -1
]
if not benchmark_database_name.endswith("_bench"):
    pytest.skip(
        "BENCHMARK_DATABASE_URL must point to a *_bench database",
        allow_module_level=True,
    )

# ruff: noqa: E402
import asyncio

from app.benchmarking import BenchmarkConfig, run_benchmark
from app.core.config import Settings

pytestmark = pytest.mark.integration


def test_small_benchmark_uses_real_distributed_path() -> None:
    async def scenario() -> None:
        result = await run_benchmark(
            BenchmarkConfig(
                workload="linear",
                runs=10,
                tasks_per_run=3,
                workers=2,
                timeout_seconds=30,
            ),
            Settings(
                database_url=BENCHMARK_DATABASE_URL,
                redis_url=BENCHMARK_REDIS_URL,
            ),
        )

        assert result.correctness == {"valid": True, "violations": []}
        assert result.results["total_runs"] == 10
        assert result.results["total_task_executions"] == 30
        assert result.results["successful_executions"] == 30
        assert result.results["outbox_publications"] == 30

    asyncio.run(scenario())
