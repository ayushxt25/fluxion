import asyncio

from app.services.loops import (
    DispatchOutboxPublisherLoop,
    LeaseReaperLoop,
    SchedulerLoop,
)
from app.services.outbox import OutboxPublishResult


class FakeScheduler:
    def __init__(self) -> None:
        self.calls = []

    async def dispatch_ready(self, run_id: str):
        self.calls.append(run_id)

        class Summary:
            dispatched_task_ids = ("a",)

        return Summary()


class FakeRunRepository:
    async def list_incomplete(self):
        return (type("RunRef", (), {"run_id": "run-1"})(),)


class FakePublisher:
    def __init__(self) -> None:
        self.calls = 0

    async def publish_pending(self, limit: int):
        self.calls += 1
        return OutboxPublishResult(
            attempted=0,
            published=0,
            failed=0,
            published_event_ids=(),
            failed_event_ids=(),
        )


class FakeReaper:
    def __init__(self) -> None:
        self.calls = 0

    async def reclaim_expired(self):
        self.calls += 1
        return ()


def test_scheduler_loop_tick_dispatches_incomplete_runs() -> None:
    async def scenario():
        scheduler = FakeScheduler()
        result = await SchedulerLoop(
            scheduler,
            FakeRunRepository(),
            poll_seconds=0.01,
        ).tick()
        return scheduler.calls, len(result.scheduled)

    assert asyncio.run(scenario()) == (["run-1"], 1)


def test_loop_run_stops_cleanly() -> None:
    async def scenario():
        stop_event = asyncio.Event()
        stop_event.set()
        publisher = FakePublisher()
        await DispatchOutboxPublisherLoop(
            publisher,
            poll_seconds=0.01,
            batch_size=1,
        ).run(stop_event)
        reaper = FakeReaper()
        await LeaseReaperLoop(reaper, interval_seconds=0.01).run(stop_event)
        return publisher.calls, reaper.calls

    assert asyncio.run(scenario()) == (0, 0)
