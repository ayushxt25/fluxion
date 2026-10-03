"""Reproducible end-to-end distributed benchmark support."""

from __future__ import annotations

import asyncio
import math
import platform
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.db.models.execution import (
    DispatchOutboxRecord,
    TaskAttemptRecord,
    WorkflowRunRecord,
)
from app.db.models.interventions import TaskInterventionRecord
from app.dispatch.transport import RedisTaskDispatcher
from app.engine.execution import WorkflowRun
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.loops import DispatchOutboxPublisherLoop, SchedulerLoop
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from app.services.worker import TaskWorker
from app.tasks.registry import build_task_registry

WORKLOADS = ("single", "linear", "fanout")


@dataclass(frozen=True)
class BenchmarkConfig:
    workload: str = "single"
    runs: int = 1_000
    tasks_per_run: int = 4
    fanout: int = 4
    workers: int = 1
    worker_concurrency: int = 1
    warmup_runs: int = 0
    timeout_seconds: float = 300.0
    poll_seconds: float = 0.05

    def __post_init__(self) -> None:
        if self.workload not in WORKLOADS:
            raise ValueError(f"workload must be one of {WORKLOADS}.")
        if self.runs < 1 or self.tasks_per_run < 1 or self.fanout < 1:
            raise ValueError("runs, tasks_per_run, and fanout must be positive.")
        if self.tasks_per_run > 64 or self.fanout > 64:
            raise ValueError("tasks_per_run and fanout must not exceed 64.")
        if self.workers < 1 or self.worker_concurrency < 1:
            raise ValueError("workers and worker_concurrency must be positive.")
        if self.warmup_runs < 0 or self.timeout_seconds <= 0 or self.poll_seconds <= 0:
            raise ValueError("warmup, timeout, and poll values must be valid.")

    @property
    def task_count_per_run(self) -> int:
        if self.workload == "single":
            return 1
        if self.workload == "linear":
            return self.tasks_per_run
        return self.fanout + 2

    @property
    def expected_executions(self) -> int:
        return self.runs * self.task_count_per_run


@dataclass(frozen=True)
class BenchmarkResult:
    benchmark_version: int
    git_commit: str | None
    python_version: str
    platform: str
    started_at: str
    execution_started_at: str
    completed_at: str
    submission_duration_seconds: float
    execution_duration_seconds: float
    duration_seconds: float
    workload: dict[str, int | str]
    configuration: dict[str, int | float]
    results: dict[str, int | float]
    correctness: dict[str, bool | list[str]]

    def as_dict(self) -> dict:
        return asdict(self)


def require_benchmark_database(database_url: str) -> None:
    name = urlparse(database_url).path.rstrip("/").rsplit("/", 1)[-1]
    if not name.endswith("_bench"):
        raise ValueError(
            "DATABASE_URL must target a benchmark database ending in _bench."
        )


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    if not 0 <= fraction <= 1:
        raise ValueError("percentile fraction must be between zero and one.")
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower, upper = math.floor(index), math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def correctness_violations(
    config: BenchmarkConfig,
    *,
    run_count: int,
    attempt_identities: set[tuple[str, str]],
    attempt_count: int,
    run_statuses: tuple[str, ...],
    attempt_statuses: tuple[str, ...],
    actionable_outbox_count: int,
    pending_intervention_count: int,
) -> list[str]:
    """Return canonical-state violations that invalidate a benchmark result."""
    violations: list[str] = []
    if run_count != config.runs:
        violations.append("unexpected durable run count")
    if attempt_count != config.expected_executions:
        violations.append("unexpected durable attempt count")
    if len(attempt_identities) != attempt_count:
        violations.append("duplicate canonical task attempt")
    if any(status != "SUCCEEDED" for status in run_statuses):
        violations.append("non-successful workflow run")
    if any(status != "SUCCEEDED" for status in attempt_statuses):
        violations.append("non-successful task attempt")
    if actionable_outbox_count:
        violations.append("actionable outbox remains")
    if pending_intervention_count:
        violations.append("pending intervention remains")
    return violations


def build_workflow(workflow_id: str, config: BenchmarkConfig) -> WorkflowDefinition:
    if config.workload == "single":
        tasks = (TaskDefinition(id="bench.noop"),)
    elif config.workload == "linear":
        tasks = tuple(
            TaskDefinition(
                id=f"bench.step.{index}",
                depends_on=() if index == 0 else (f"bench.step.{index - 1}",),
            )
            for index in range(config.tasks_per_run)
        )
    else:
        tasks = (
            TaskDefinition(id="bench.root"),
            *(
                TaskDefinition(id=f"bench.branch.{index}", depends_on=("bench.root",))
                for index in range(config.fanout)
            ),
            TaskDefinition(
                id="bench.final",
                depends_on=tuple(
                    f"bench.branch.{index}" for index in range(config.fanout)
                ),
            ),
        )
    return WorkflowDefinition(id=workflow_id, name="Fluxion benchmark", tasks=tasks)


async def run_benchmark(config: BenchmarkConfig, settings: Settings) -> BenchmarkResult:
    """Run scheduler, publisher, Redis transport, and real worker services locally."""
    database_url = settings.database_url
    require_benchmark_database(database_url)
    suffix = uuid4().hex
    workflow_id = f"bench-{suffix}"
    queue_name = f"{settings.dispatch_queue_name}:bench:{suffix}"
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    dispatcher = RedisTaskDispatcher(settings.redis_url, queue_name)
    registry = build_task_registry()
    stop_workers = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    max_queue_depth = 0
    scheduler_ticks = publisher_passes = dispatched = published = 0
    try:
        # Verify both required services before generating benchmark state. This
        # keeps a missing dependency from being reported as a workload timeout.
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        await dispatcher.ping()
        worker_tasks = [
            asyncio.create_task(
                _worker_loop(
                    factory,
                    dispatcher,
                    registry,
                    stop_workers,
                    f"bench-{index}",
                )
            )
            for index in range(config.workers * config.worker_concurrency)
        ]
        workflow = build_workflow(workflow_id, config)
        async with factory() as session:
            await WorkflowRepository(session).save(workflow)
            for index in range(config.warmup_runs):
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create(f"{workflow_id}-run-{index}", workflow)
                )
        if config.warmup_runs:
            await _drive_until_complete(
                factory,
                dispatcher,
                workflow_id,
                config.warmup_runs,
                config,
                worker_tasks,
            )
        submission_started = datetime.now(UTC)

        async with factory() as session:
            for index in range(config.runs):
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create(
                        f"{workflow_id}-measurement-{index}",
                        workflow,
                    )
                )

        execution_started = datetime.now(UTC)

        (
            scheduler_ticks,
            publisher_passes,
            max_queue_depth,
            dispatched,
            published,
            scheduler_total_seconds,
            publisher_total_seconds,
            publisher_claim_seconds,
            publisher_validity_check_seconds,
            publisher_dispatch_seconds,
            publisher_mark_published_seconds,
            publisher_discard_seconds,
            publisher_failure_record_seconds,
        ) = await _drive_until_complete(
            factory,
            dispatcher,
            workflow_id,
            config.warmup_runs + config.runs,
            config,
            worker_tasks,
        )

        completed = datetime.now(UTC)

        return await _result(
            factory,
            workflow_id,
            config,
            submission_started,
            execution_started,
            completed,
            scheduler_ticks,
            publisher_passes,
            max_queue_depth,
            dispatched,
            published,
            scheduler_total_seconds,
            publisher_total_seconds,
            publisher_claim_seconds,
            publisher_validity_check_seconds,
            publisher_dispatch_seconds,
            publisher_mark_published_seconds,
            publisher_discard_seconds,
            publisher_failure_record_seconds,
        )
    finally:
        stop_workers.set()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        await dispatcher.aclose()
        await engine.dispose()


async def _worker_loop(factory, dispatcher, registry, stop, worker_id: str) -> None:
    while not stop.is_set():
        message = await dispatcher.receive(timeout=0.1)
        if message is None:
            continue
        async with factory() as session:
            await TaskWorker(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
                TaskAttemptRepository(session),
                dispatcher,
                registry,
                worker_id=worker_id,
            ).process_message(message)


async def _drive_until_complete(
    factory, dispatcher, workflow_id, run_count, config, worker_tasks=()
):
    deadline = asyncio.get_running_loop().time() + config.timeout_seconds
    scheduler_ticks = publisher_passes = max_depth = dispatched = published = 0
    scheduler_total_seconds = 0.0
    publisher_total_seconds = 0.0
    publisher_claim_seconds = 0.0
    publisher_validity_check_seconds = 0.0
    publisher_dispatch_seconds = 0.0
    publisher_mark_published_seconds = 0.0
    publisher_discard_seconds = 0.0
    publisher_failure_record_seconds = 0.0
    while True:
        worker_errors = [
            task.exception()
            for task in worker_tasks
            if task.done() and not task.cancelled() and task.exception() is not None
        ]
        if worker_errors:
            raise RuntimeError(
                "Benchmark worker exited unexpectedly."
            ) from worker_errors[0]
        async with factory() as session:
            scheduler = SchedulerLoop(
                WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                ),
                WorkflowRunRepository(session),
                poll_seconds=config.poll_seconds,
            )
            publisher = DispatchOutboxPublisherLoop(
                DispatchOutboxPublisher(DispatchOutboxRepository(session), dispatcher),
                poll_seconds=config.poll_seconds,
            )
            scheduler_started = time.perf_counter()
            scheduled = await scheduler.tick()
            scheduler_total_seconds += time.perf_counter() - scheduler_started

            publisher_started = time.perf_counter()
            publication = await publisher.tick()
            publisher_total_seconds += time.perf_counter() - publisher_started
            publisher_claim_seconds += publication.claim_seconds
            publisher_validity_check_seconds += publication.validity_check_seconds
            publisher_dispatch_seconds += publication.dispatch_seconds
            publisher_mark_published_seconds += publication.mark_published_seconds
            publisher_discard_seconds += publication.discard_seconds
            publisher_failure_record_seconds += publication.failure_record_seconds
            scheduler_ticks += 1
            publisher_passes += 1
            dispatched += sum(
                len(summary.dispatched_task_ids) for summary in scheduled.scheduled
            )
            published += publication.published
            statuses = tuple(
                (
                    await session.execute(
                        select(WorkflowRunRecord.status).where(
                            WorkflowRunRecord.workflow_id == workflow_id
                        )
                    )
                ).scalars()
            )
        max_depth = max(max_depth, await dispatcher.queue_depth())
        if len(statuses) == run_count and all(
            status == "SUCCEEDED" for status in statuses
        ):
            return (
                scheduler_ticks,
                publisher_passes,
                max_depth,
                dispatched,
                published,
                scheduler_total_seconds,
                publisher_total_seconds,
                publisher_claim_seconds,
                publisher_validity_check_seconds,
                publisher_dispatch_seconds,
                publisher_mark_published_seconds,
                publisher_discard_seconds,
                publisher_failure_record_seconds,
            )
        if any(status in {"FAILED", "CANCELLED"} for status in statuses):
            raise RuntimeError("Benchmark workload reached a terminal failure.")
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(
                "Benchmark timed out before all workflow runs completed."
            )
        await asyncio.sleep(config.poll_seconds)


async def _result(
    factory,
    workflow_id,
    config,
    submission_started,
    execution_started,
    completed,
    ticks,
    passes,
    depth,
    dispatched,
    published,
    scheduler_total_seconds,
    publisher_total_seconds,
    publisher_claim_seconds,
    publisher_validity_check_seconds,
    publisher_dispatch_seconds,
    publisher_mark_published_seconds,
    publisher_discard_seconds,
    publisher_failure_record_seconds,
):
    measurement_runs = WorkflowRunRecord.run_id.like(f"{workflow_id}-measurement-%")
    async with factory() as session:
        attempts = tuple(
            (
                await session.execute(
                    select(TaskAttemptRecord).where(
                        TaskAttemptRecord.workflow_id == workflow_id,
                        TaskAttemptRecord.run_id.like(f"{workflow_id}-measurement-%"),
                    )
                )
            ).scalars()
        )
        runs = tuple(
            (
                await session.execute(
                    select(WorkflowRunRecord).where(
                        WorkflowRunRecord.workflow_id == workflow_id,
                        measurement_runs,
                    )
                )
            ).scalars()
        )
        actionable = await session.scalar(
            select(func.count())
            .select_from(DispatchOutboxRecord)
            .where(
                DispatchOutboxRecord.workflow_id == workflow_id,
                DispatchOutboxRecord.published_at.is_(None),
                DispatchOutboxRecord.discarded_at.is_(None),
            )
        )
        interventions = await session.scalar(
            select(func.count())
            .select_from(TaskInterventionRecord)
            .where(
                TaskInterventionRecord.workflow_id == workflow_id,
                TaskInterventionRecord.resolution == "PENDING",
            )
        )
    submission_duration = max(
        (execution_started - submission_started).total_seconds(),
        0.000001,
    )
    execution_duration = max(
        (completed - execution_started).total_seconds(),
        0.000001,
    )
    duration = max(
        (completed - submission_started).total_seconds(),
        0.000001,
    )
    latencies = [
        (attempt.finished_at - attempt.created_at).total_seconds()
        for attempt in attempts
        if attempt.finished_at is not None
    ]
    violations = correctness_violations(
        config,
        run_count=len(runs),
        attempt_identities={(attempt.run_id, attempt.task_id) for attempt in attempts},
        attempt_count=len(attempts),
        run_statuses=tuple(run.status for run in runs),
        attempt_statuses=tuple(attempt.status for attempt in attempts),
        actionable_outbox_count=actionable or 0,
        pending_intervention_count=interventions or 0,
    )
    return BenchmarkResult(
        benchmark_version=3,
        git_commit=_git_commit(),
        python_version=sys.version.split()[0],
        platform=platform.platform(),
        started_at=submission_started.isoformat(),
        execution_started_at=execution_started.isoformat(),
        completed_at=completed.isoformat(),
        submission_duration_seconds=submission_duration,
        execution_duration_seconds=execution_duration,
        duration_seconds=duration,
        workload={
            "type": config.workload,
            "runs": config.runs,
            "tasks_per_run": config.task_count_per_run,
            "expected_executions": config.expected_executions,
        },
        configuration={
            "workers": config.workers,
            "worker_concurrency": config.worker_concurrency,
            "effective_worker_loops": (
                config.workers * config.worker_concurrency
            ),
            "scheduler_ticks": ticks,
            "publisher_passes": passes,
            "max_queue_depth": depth,
            "scheduler_total_seconds": scheduler_total_seconds,
            "scheduler_avg_tick_seconds": (
                scheduler_total_seconds / ticks if ticks else 0.0
            ),
            "publisher_total_seconds": publisher_total_seconds,
            "publisher_avg_pass_seconds": (
                publisher_total_seconds / passes if passes else 0.0
            ),
            "publisher_claim_seconds": publisher_claim_seconds,
            "publisher_validity_check_seconds": publisher_validity_check_seconds,
            "publisher_dispatch_seconds": publisher_dispatch_seconds,
            "publisher_mark_published_seconds": publisher_mark_published_seconds,
            "publisher_discard_seconds": publisher_discard_seconds,
            "publisher_failure_record_seconds": publisher_failure_record_seconds,
        },
        results={
            "total_runs": len(runs),
            "total_task_executions": len(attempts),
            "successful_executions": sum(
                a.status == "SUCCEEDED" for a in attempts
            ),
            "failed_executions": sum(
                a.status == "FAILED" for a in attempts
            ),
            "interrupted_executions": sum(
                a.status == "INTERRUPTED" for a in attempts
            ),
            "cancelled_executions": sum(
                a.status == "CANCELLED" for a in attempts
            ),
            "tasks_per_second": len(attempts) / duration,
            "runs_per_second": len(runs) / duration,
            "execution_tasks_per_second": (
                len(attempts) / execution_duration
            ),
            "execution_runs_per_second": (
                len(runs) / execution_duration
            ),
            "scheduler_dispatches": dispatched,
            "scheduler_dispatches_per_second": (
                dispatched / execution_duration
            ),
            "scheduler_dispatches_per_scheduler_second": (
                dispatched / scheduler_total_seconds
                if scheduler_total_seconds > 0
                else 0.0
            ),
            "outbox_publications": published,
            "outbox_publications_per_second": (
                published / execution_duration
            ),
            "outbox_publications_per_publisher_second": (
                published / publisher_total_seconds
                if publisher_total_seconds > 0
                else 0.0
            ),
            "worker_executions_per_second": (
                len(attempts) / execution_duration
            ),
            "latency_p50_seconds": percentile(latencies, 0.5) or 0,
            "latency_p95_seconds": percentile(latencies, 0.95) or 0,
            "latency_p99_seconds": percentile(latencies, 0.99) or 0,
            "latency_max_seconds": max(latencies, default=0),
        },
        correctness={"valid": not violations, "violations": violations},
    )



def _git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None
