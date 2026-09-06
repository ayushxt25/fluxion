import asyncio
from datetime import UTC, datetime, timedelta

from app.dispatch.messages import TaskDispatchMessage
from app.engine.exceptions import DispatchError
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import DispatchOutboxEvent


def message(task_id: str = "a") -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        run_id="run-1",
        task_id=task_id,
        attempt_number=1,
        attempt_key=f"run-1:{task_id}:1",
        idempotency_key=f"run-1:{task_id}",
    )


class FakeOutboxRepository:
    def __init__(self) -> None:
        self.events = [
            DispatchOutboxEvent(
                id="event-1",
                event_type="TASK_DISPATCH",
                message=message(),
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
        ]
        self.published = []
        self.failures = []

    async def is_dispatch_still_valid(self, event: DispatchOutboxEvent) -> bool:
        return True

    async def list_unpublished(self, limit: int = 100):
        return tuple(event for event in self.events if event.published_at is None)

    async def claim_unpublished(
        self,
        publisher_id: str,
        claim_token: str,
        now: datetime,
        claim_seconds: float,
        limit: int = 100,
    ):
        claimed = []
        for event in self.events:
            if event.published_at is not None:
                continue
            if event.claim_token is not None and event.claim_expires_at > now:
                continue
            claimed_event = DispatchOutboxEvent(
                id=event.id,
                event_type=event.event_type,
                message=event.message,
                run_id=event.run_id,
                workflow_id=event.workflow_id,
                task_id=event.task_id,
                attempt_number=event.attempt_number,
                created_at=event.created_at,
                published_at=event.published_at,
                claimed_by=publisher_id,
                claim_token=claim_token,
                claimed_at=now,
                claim_expires_at=now + timedelta(seconds=claim_seconds),
                publish_attempts=event.publish_attempts,
                last_error=event.last_error,
            )
            self.events[0] = claimed_event
            claimed.append(claimed_event)
        return tuple(claimed[:limit])

    async def mark_published(
        self,
        event_id: str,
        published_at: datetime,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        self.published.append((event_id, published_at))
        event = self.events[0]
        assert event.claimed_by == publisher_id
        assert event.claim_token == claim_token
        self.events[0] = DispatchOutboxEvent(
            id=event.id,
            event_type=event.event_type,
            message=event.message,
            run_id=event.run_id,
            workflow_id=event.workflow_id,
            task_id=event.task_id,
            attempt_number=event.attempt_number,
            created_at=event.created_at,
            published_at=published_at,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            claim_expires_at=None,
            publish_attempts=event.publish_attempts + 1,
            last_error=None,
        )

    async def record_publish_failure(
        self,
        event_id: str,
        error: str,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        self.failures.append((event_id, error))
        event = self.events[0]
        assert event.claimed_by == publisher_id
        assert event.claim_token == claim_token
        self.events[0] = DispatchOutboxEvent(
            id=event.id,
            event_type=event.event_type,
            message=event.message,
            run_id=event.run_id,
            workflow_id=event.workflow_id,
            task_id=event.task_id,
            attempt_number=event.attempt_number,
            created_at=event.created_at,
            published_at=None,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            claim_expires_at=None,
            publish_attempts=event.publish_attempts + 1,
            last_error=error,
        )


class FlakyDispatcher:
    def __init__(self) -> None:
        self.calls = 0
        self.messages = []

    async def dispatch(self, dispatch_message: TaskDispatchMessage) -> None:
        self.calls += 1
        if self.calls == 1:
            raise DispatchError("redis down")
        self.messages.append(dispatch_message)


def test_outbox_publish_failure_remains_retryable() -> None:
    async def scenario():
        repository = FakeOutboxRepository()
        dispatcher = FlakyDispatcher()
        publisher = DispatchOutboxPublisher(repository, dispatcher)

        first = await publisher.publish_pending()
        second = await publisher.publish_pending()

        assert first.failed == 1
        assert repository.failures[0][0] == "event-1"
        assert second.published == 1
        assert dispatcher.messages == [message()]

    asyncio.run(scenario())
