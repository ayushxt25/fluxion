import asyncio
from dataclasses import dataclass

from app.engine.execution import TaskAttempt, WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.loops import SchedulerLoop
from app.services.scheduler import WorkflowScheduler


@dataclass(frozen=True)
class _RunRef:
    run_id: str


class _LoopRunRepository:
    async def list_incomplete(self):
        return (_RunRef("run-a"), _RunRef("run-b"), _RunRef("run-c"))


class _FairScheduler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None]] = []

    async def dispatch_ready(self, run_id: str, *, max_dispatch: int | None = None):
        self.calls.append((run_id, max_dispatch))
        count = min(2, max_dispatch or 0)
        return type(
            "Summary",
            (),
            {
                "dispatched_task_ids": tuple(
                    f"{run_id}-{index}" for index in range(count)
                )
            },
        )()


def test_scheduler_loop_caps_global_work_and_visits_runs_in_order() -> None:
    async def scenario():
        scheduler = _FairScheduler()
        result = await SchedulerLoop(
            scheduler,
            _LoopRunRepository(),
            poll_seconds=0.01,
            max_dispatch_per_tick=3,
        ).tick()
        return scheduler.calls, result

    calls, result = asyncio.run(scenario())

    assert calls == [("run-a", 3), ("run-b", 1)]
    assert sum(len(summary.dispatched_task_ids) for summary in result.scheduled) == 3


def test_scheduler_enforces_per_run_cap_and_queue_backpressure() -> None:
    definition = WorkflowDefinition(
        id="workflow",
        name="Workflow",
        tasks=(TaskDefinition(id="a"), TaskDefinition(id="b"), TaskDefinition(id="c")),
    )
    workflow_run = WorkflowRun.create("run-1", definition)

    class WorkflowRepository:
        async def get(self, workflow_id: str):
            assert workflow_id == "workflow"
            return definition

    class RunRepository:
        async def get_workflow_id(self, run_id: str):
            assert run_id == "run-1"
            return "workflow"

        async def get(self, run_id: str, workflow):
            return workflow_run

        async def save_state(self, run):
            return None

    class AttemptRepository:
        async def next_attempt_number(self, run_id: str, task_id: str):
            return 1

    class OutboxRepository:
        def __init__(self) -> None:
            self.created: list[str] = []

        async def create_dispatch_intent(self, run, task_id, attempt_number, message):
            self.created.append(task_id)
            return (
                TaskAttempt(
                    run_id=run.run_id,
                    workflow_id=run.workflow_id,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    status=AttemptStatus.DISPATCHED,
                ),
                type("Event", (), {"id": f"event-{task_id}"})(),
            )

    class Dispatcher:
        def __init__(self, depth: int) -> None:
            self.depth = depth

        async def queue_depth(self) -> int:
            return self.depth

    async def scenario():
        outbox = OutboxRepository()
        scheduler = WorkflowScheduler(
            WorkflowRepository(),
            RunRepository(),
            AttemptRepository(),
            Dispatcher(depth=0),
            outbox,
            max_dispatch_per_run=2,
            queue_high_watermark=5,
        )
        summary = await scheduler.dispatch_ready("run-1")

        blocked_scheduler = WorkflowScheduler(
            WorkflowRepository(),
            RunRepository(),
            AttemptRepository(),
            Dispatcher(depth=5),
            outbox,
            max_dispatch_per_run=2,
            queue_high_watermark=5,
        )
        blocked = await blocked_scheduler.dispatch_ready("run-1")
        return summary, blocked, outbox.created

    summary, blocked, created = asyncio.run(scenario())

    assert summary.dispatched_task_ids == ("a", "b")
    assert created == ["a", "b"]
    assert workflow_run.get_task_status("a") == TaskStatus.DISPATCHED
    assert workflow_run.get_task_status("b") == TaskStatus.DISPATCHED
    assert workflow_run.get_task_status("c") == TaskStatus.READY
    assert blocked.dispatched_task_ids == ()
