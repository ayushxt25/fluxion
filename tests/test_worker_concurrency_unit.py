import asyncio

from app.dispatch.messages import TaskDispatchMessage
from app.runtime import worker as worker_runtime


def _message(task_id: str) -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        run_id="run-1",
        task_id=task_id,
        attempt_number=1,
        attempt_key=f"run-1:{task_id}:1",
        idempotency_key=f"run-1:{task_id}",
    )


def test_worker_admits_no_more_than_configured_concurrency(monkeypatch) -> None:
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            return None

    class FakeDispatcher:
        def __init__(self) -> None:
            self.messages = [_message("a"), _message("b"), _message("c")]
            self.receive_calls = 0

        async def receive(self, timeout: float | None = None):
            self.receive_calls += 1
            if self.messages:
                return self.messages.pop(0)
            await asyncio.sleep(0)
            return None

    class FakeTaskWorker:
        active_count = 0
        max_active_count = 0
        started = asyncio.Event()
        release = asyncio.Event()
        worker_ids: list[str] = []

        def __init__(self, *args, worker_id: str, **kwargs) -> None:
            self.worker_id = worker_id
            self.worker_ids.append(worker_id)

        async def process_message(self, message: TaskDispatchMessage) -> None:
            self.__class__.active_count += 1
            self.__class__.max_active_count = max(
                self.__class__.max_active_count,
                self.__class__.active_count,
            )
            if self.__class__.active_count == 2:
                self.__class__.started.set()
            await self.__class__.release.wait()
            self.__class__.active_count -= 1

    dispatcher = FakeDispatcher()
    stop_event = asyncio.Event()

    monkeypatch.setattr(worker_runtime, "WorkflowRepository", lambda session: object())
    monkeypatch.setattr(
        worker_runtime,
        "WorkflowRunRepository",
        lambda session: object(),
    )
    monkeypatch.setattr(
        worker_runtime,
        "TaskAttemptRepository",
        lambda session: object(),
    )
    monkeypatch.setattr(worker_runtime, "TaskWorker", FakeTaskWorker)

    async def scenario() -> None:
        loop_task = asyncio.create_task(
            worker_runtime.run_worker_loop(
                FakeSession,
                dispatcher,
                object(),
                stop_event,
                concurrency=2,
                shutdown_grace_seconds=1,
                worker_id="worker-test",
            )
        )
        await asyncio.wait_for(FakeTaskWorker.started.wait(), timeout=1)

        assert FakeTaskWorker.max_active_count == 2
        assert dispatcher.receive_calls == 2

        stop_event.set()
        FakeTaskWorker.release.set()
        assert await asyncio.wait_for(loop_task, timeout=1) == "worker-test"

    asyncio.run(scenario())

    assert FakeTaskWorker.worker_ids == ["worker-test", "worker-test"]
