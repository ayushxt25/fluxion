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
    def __init__(self, event_count: int = 1) -> None:
        self.events = [
            DispatchOutboxEvent(
                id=f"event-{index}",
                event_type="TASK_DISPATCH",
                message=message(chr(ord("a") + index - 1)),
                run_id="run-1",
                workflow_id="workflow",
                task_id=chr(ord("a") + index - 1),
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
            for index in range(1, event_count + 1)
        ]
        self.published = []
        self.published_batches = []
        self.discarded = []
        self.failures = []

    async def find_valid_dispatch_event_ids(
        self,
        event_ids: tuple[str, ...],
    ) -> frozenset[str]:
        return frozenset(event_ids)

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
        for index, event in enumerate(self.events):
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
            self.events[index] = claimed_event
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
        index = next(
            index for index, event in enumerate(self.events) if event.id == event_id
        )
        event = self.events[index]
        assert event.claimed_by == publisher_id
        assert event.claim_token == claim_token
        self.events[index] = DispatchOutboxEvent(
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

    async def mark_published_batch(
        self,
        event_ids: tuple[str, ...],
        published_at: datetime,
        *,
        publisher_id: str,
        claim_token: str,
    ) -> None:
        self.published_batches.append(event_ids)
        for event_id in event_ids:
            await self.mark_published(
                event_id,
                published_at,
                publisher_id=publisher_id,
                claim_token=claim_token,
            )

    async def mark_discarded(
        self,
        event_id: str,
        discarded_at: datetime,
        reason: str,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        self.discarded.append((event_id, reason))
        index = next(
            index for index, event in enumerate(self.events) if event.id == event_id
        )
        event = self.events[index]
        assert event.claimed_by == publisher_id
        assert event.claim_token == claim_token
        self.events[index] = DispatchOutboxEvent(
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
            publish_attempts=event.publish_attempts,
            last_error=event.last_error,
            discarded_at=discarded_at,
            discard_reason=reason,
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
        index = next(
            index for index, event in enumerate(self.events) if event.id == event_id
        )
        event = self.events[index]
        assert event.claimed_by == publisher_id
        assert event.claim_token == claim_token
        self.events[index] = DispatchOutboxEvent(
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
        assert repository.published_batches == [("event-1",)]

    asyncio.run(scenario())


def test_outbox_publisher_acknowledges_successes_in_one_batch() -> None:
    class Dispatcher:
        async def dispatch(self, dispatch_message: TaskDispatchMessage) -> None:
            return None

    async def scenario() -> None:
        repository = FakeOutboxRepository(event_count=2)
        result = await DispatchOutboxPublisher(
            repository, Dispatcher()
        ).publish_pending()

        assert result.published_event_ids == ("event-1", "event-2")
        assert repository.published_batches == [("event-1", "event-2")]
        assert all(event.published_at is not None for event in repository.events)
        assert all(event.claimed_by is None for event in repository.events)
        assert all(event.claim_token is None for event in repository.events)
        assert all(event.publish_attempts == 1 for event in repository.events)

    asyncio.run(scenario())


def test_outbox_publisher_excludes_redis_failures_from_batch_acknowledgment() -> None:
    async def scenario() -> None:
        repository = FakeOutboxRepository(event_count=2)
        result = await DispatchOutboxPublisher(
            repository, FlakyDispatcher()
        ).publish_pending()

        assert result.failed_event_ids == ("event-1",)
        assert result.published_event_ids == ("event-2",)
        assert repository.published_batches == [("event-2",)]
        assert repository.events[0].published_at is None
        assert repository.events[1].published_at is not None

    asyncio.run(scenario())


def test_outbox_publisher_discards_batch_targets_without_canonical_rows() -> None:
    class Repository(FakeOutboxRepository):
        async def find_valid_dispatch_event_ids(
            self,
            event_ids: tuple[str, ...],
        ) -> frozenset[str]:
            # event-2 represents a missing task row and event-3 a missing attempt.
            return frozenset(
                event_id for event_id in event_ids if event_id == "event-1"
            )

    class Dispatcher:
        async def dispatch(self, dispatch_message: TaskDispatchMessage) -> None:
            return None

    async def scenario() -> None:
        repository = Repository(event_count=3)
        result = await DispatchOutboxPublisher(
            repository, Dispatcher()
        ).publish_pending()

        assert result.published_event_ids == ("event-1",)
        assert result.discarded_event_ids == ("event-2", "event-3")
        assert repository.published_batches == [("event-1",)]
        assert repository.events[0].published_at is not None
        assert repository.events[1].discarded_at is not None
        assert repository.events[2].discarded_at is not None

    asyncio.run(scenario())
