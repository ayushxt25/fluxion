import asyncio
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

from app.benchmarking import BenchmarkConfig, run_benchmark  # noqa: E402
from app.core.config import Settings  # noqa: E402

pytestmark = [pytest.mark.integration, pytest.mark.stress]


def test_benchmark_stress_uses_bounded_real_distributed_workload() -> None:
    async def scenario() -> None:
        result = await run_benchmark(
            BenchmarkConfig(
                workload="fanout",
                runs=100,
                fanout=8,
                workers=4,
                timeout_seconds=120,
            ),
            Settings(
                database_url=BENCHMARK_DATABASE_URL,
                redis_url=BENCHMARK_REDIS_URL,
            ),
        )
        assert result.workload["expected_executions"] == 1_000
        assert result.correctness == {"valid": True, "violations": []}

    asyncio.run(scenario())
