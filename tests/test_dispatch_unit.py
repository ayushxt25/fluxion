import asyncio

import pytest

from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.exceptions import InvalidDispatchMessageError


def message(task_id: str, attempt_number: int = 1) -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        run_id="run-1",
        task_id=task_id,
        attempt_number=attempt_number,
        attempt_key=f"run-1:{task_id}:{attempt_number}",
        idempotency_key=f"run-1:{task_id}",
    )


def test_dispatch_message_json_roundtrip() -> None:
    original = message("a")

    restored = TaskDispatchMessage.from_json(original.to_json())

    assert restored == original


def test_unsupported_version_rejected() -> None:
    payload = message("a").model_copy(update={"version": 2}).to_json()

    with pytest.raises(InvalidDispatchMessageError):
        TaskDispatchMessage.from_json(payload)


def test_malformed_message_rejected() -> None:
    with pytest.raises(InvalidDispatchMessageError):
        TaskDispatchMessage.from_json("{")


def test_in_memory_dispatcher_fifo() -> None:
    async def scenario():
        dispatcher = InMemoryTaskDispatcher()
        await dispatcher.dispatch(message("a"))
        await dispatcher.dispatch(message("b"))
        first = await dispatcher.receive(timeout=0.1)
        second = await dispatcher.receive(timeout=0.1)
        return first.task_id, second.task_id

    assert asyncio.run(scenario()) == ("a", "b")
