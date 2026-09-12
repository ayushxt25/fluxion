import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from app.dispatch.messages import TaskDispatchMessage
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import DispatchOutboxEvent
from tests.support.faults import InjectedFault

pytestmark = pytest.mark.chaos


def _message() -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        run_id="run-1",
        task_id="a",
        attempt_number=1,
        attempt_key="run-1:a:1",
        idempotency_key="run-1:a",
    )


def test_publisher_crash_window_allows_duplicate_transport_on_retry() -> None:
    class Repository:
        def __init__(self) -> None:
            self.event = DispatchOutboxEvent(
                id="event-1",
                event_type="TASK_DISPATCH",
                message=_message(),
                run_id="run-1",
                workflow_id="workflow",
                task_id="a",
                attempt_number=1,
                created_at=datetime.now(UTC),
                published_at=None,
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                claim_expires_at=None,
                publish_attempts=0,
                last_error=None,
            )
            self.crash_before_mark = True

        async def claim_unpublished(
            self,
            publisher_id,
            claim_token,
            now,
            seconds,
            limit,
        ):
            if self.event.published_at is not None:
                return ()
            return (self.event,)

        async def is_dispatch_still_valid(self, event):
            return True

        async def mark_published(self, event_id, published_at, **kwargs):
            if self.crash_before_mark:
                self.crash_before_mark = False
                raise InjectedFault("after.redis.publish.before.outbox.mark")
            self.event = replace(self.event, published_at=published_at)

        async def record_publish_failure(self, *args, **kwargs):
            raise AssertionError("A simulated process crash is not a publish failure.")

    class Dispatcher:
        def __init__(self) -> None:
            self.messages: list[TaskDispatchMessage] = []

        async def dispatch(self, message):
            self.messages.append(message)

    async def scenario() -> None:
        repository = Repository()
        dispatcher = Dispatcher()
        with pytest.raises(InjectedFault):
            await DispatchOutboxPublisher(repository, dispatcher).publish_pending()

        result = await DispatchOutboxPublisher(repository, dispatcher).publish_pending()

        assert dispatcher.messages == [_message(), _message()]
        assert result.published == 1
        assert repository.event.published_at is not None

    asyncio.run(scenario())
