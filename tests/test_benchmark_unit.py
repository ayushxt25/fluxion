import json

import pytest

from app.benchmarking import (
    BenchmarkConfig,
    BenchmarkResult,
    build_workflow,
    correctness_violations,
    percentile,
    require_benchmark_database,
)
from app.runtime.benchmark import _parse


def test_benchmark_config_counts_workload_executions() -> None:
    assert BenchmarkConfig(workload="single", runs=3).expected_executions == 3
    assert (
        BenchmarkConfig(workload="linear", runs=3, tasks_per_run=4).expected_executions
        == 12
    )
    assert (
        BenchmarkConfig(workload="fanout", runs=2, fanout=4).expected_executions == 12
    )


def test_benchmark_configuration_and_database_guard_are_validated() -> None:
    with pytest.raises(ValueError):
        BenchmarkConfig(workers=0)
    with pytest.raises(ValueError):
        require_benchmark_database("postgresql+asyncpg://localhost/fluxion")
    require_benchmark_database("postgresql+asyncpg://localhost/fluxion_bench")


def test_percentiles_and_result_serialization_are_deterministic() -> None:
    assert percentile([1, 2, 3, 4], 0.5) == 2.5
    assert percentile([], 0.95) is None
    result = BenchmarkResult(
        benchmark_version=1,
        git_commit=None,
        python_version="3.11",
        platform="test",
        started_at="2026-10-02T00:00:00+00:00",
        execution_started_at="2026-10-02T00:00:00.250000+00:00",
        completed_at="2026-10-02T00:00:01+00:00",
        submission_duration_seconds=0.25,
        execution_duration_seconds=0.75,
        duration_seconds=1,
        workload={
            "type": "single",
            "runs": 1,
            "tasks_per_run": 1,
            "expected_executions": 1,
        },
        configuration={"workers": 1},
        results={"successful_executions": 1},
        correctness={"valid": True, "violations": []},
    )
    assert json.loads(json.dumps(result.as_dict()))["correctness"]["valid"]


def test_correctness_validation_rejects_duplicate_or_unfinished_work() -> None:
    violations = correctness_violations(
        BenchmarkConfig(runs=2),
        run_count=2,
        attempt_identities={("run-1", "bench.noop")},
        attempt_count=2,
        run_statuses=("SUCCEEDED", "SUCCEEDED"),
        attempt_statuses=("SUCCEEDED", "RUNNING"),
        actionable_outbox_count=1,
        pending_intervention_count=0,
    )
    assert "duplicate canonical task attempt" in violations
    assert "non-successful task attempt" in violations
    assert "actionable outbox remains" in violations


def test_workload_builder_uses_registered_benchmark_task_ids() -> None:
    assert [task.id for task in build_workflow("bench", BenchmarkConfig()).tasks] == [
        "bench.noop"
    ]
    assert (
        build_workflow("bench", BenchmarkConfig(workload="fanout")).tasks[-1].id
        == "bench.final"
    )


def test_benchmark_cli_parses_large_scale_configuration() -> None:
    _, config = _parse(["--workload", "single", "--runs", "100000", "--workers", "16"])
    assert config.expected_executions == 100_000
    assert config.workers == 16
