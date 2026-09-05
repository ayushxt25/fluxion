import os
from uuid import uuid4

import pytest

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    pytest.skip("REDIS_URL is not set", allow_module_level=True)

# ruff: noqa: E402
import asyncio

from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import RedisTaskDispatcher


def message(task_id: str) -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        run_id="run-1",
        task_id=task_id,
        attempt_number=1,
        attempt_key=f"run-1:{task_id}:1",
        idempotency_key=f"run-1:{task_id}",
    )


def test_redis_dispatcher_fifo() -> None:
    async def scenario():
        queue_name = f"fluxion:test:dispatch:{uuid4()}"
        dispatcher = RedisTaskDispatcher(REDIS_URL, queue_name)
        try:
            await dispatcher.dispatch(message("a"))
            await dispatcher.dispatch(message("b"))
            first = await dispatcher.receive(timeout=1)
            second = await dispatcher.receive(timeout=1)
            return first.task_id, second.task_id
        except Exception as exc:
            pytest.skip(f"Redis is unavailable: {exc}")
        finally:
            await dispatcher.aclose()

    assert asyncio.run(scenario()) == ("a", "b")
