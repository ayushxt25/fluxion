import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from app.dispatch.transport import TaskDispatcher
from app.engine.exceptions import DispatchError
from app.observability.metrics import record_outbox_publish
from app.services.repositories import DispatchOutboxRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutboxPublishResult:
    attempted: int
    published: int
    failed: int
    published_event_ids: tuple[str, ...]
    failed_event_ids: tuple[str, ...]
    discarded_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class DispatchReconciliationResult:
    dispatched_attempts_missing_outbox: tuple[tuple[str, str, int], ...]
    unpublished_outbox_event_ids: tuple[str, ...]


class DispatchOutboxPublisher:
    def __init__(
        self,
        outbox_repository: DispatchOutboxRepository,
        dispatcher: TaskDispatcher,
        *,
        publisher_id: str | None = None,
        claim_seconds: float = 30,
    ) -> None:
        if claim_seconds <= 0:
            raise ValueError("claim_seconds must be positive.")
        self.publisher_id = publisher_id or str(uuid4())
        self._claim_seconds = claim_seconds
        self._outbox_repository = outbox_repository
        self._dispatcher = dispatcher

    async def publish_pending(self, limit: int = 100) -> OutboxPublishResult:
        claim_token = str(uuid4())
        events = await self._outbox_repository.claim_unpublished(
            self.publisher_id,
            claim_token,
            datetime.now(UTC),
            self._claim_seconds,
            limit,
        )
        published_event_ids = []
        failed_event_ids = []
        discarded_event_ids = []

        for event in events:
            started = time.perf_counter()
            if not await self._outbox_repository.is_dispatch_still_valid(event):
                await self._outbox_repository.mark_discarded(
                    event.id,
                    datetime.now(UTC),
                    "dispatch target is no longer DISPATCHED.",
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                discarded_event_ids.append(event.id)
                record_outbox_publish(
                    outcome="discarded",
                    duration_seconds=time.perf_counter() - started,
                )
                continue
            try:
                await self._dispatcher.dispatch(event.message)
            except DispatchError as exc:
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                failed_event_ids.append(event.id)
                record_outbox_publish(
                    outcome="failed",
                    duration_seconds=time.perf_counter() - started,
                )
                logger.warning(
                    "Outbox publish failed.",
                    extra={
                        "event": "outbox.publish",
                        "outcome": "failed",
                        "workflow_id": event.workflow_id,
                        "run_id": event.run_id,
                        "task_id": event.task_id,
                        "attempt_number": event.attempt_number,
                    },
                )
                continue
            except Exception as exc:
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                failed_event_ids.append(event.id)
                record_outbox_publish(
                    outcome="failed",
                    duration_seconds=time.perf_counter() - started,
                )
                logger.warning(
                    "Outbox publish failed.",
                    extra={
                        "event": "outbox.publish",
                        "outcome": "failed",
                        "workflow_id": event.workflow_id,
                        "run_id": event.run_id,
                        "task_id": event.task_id,
                        "attempt_number": event.attempt_number,
                    },
                )
                continue

            await self._outbox_repository.mark_published(
                event.id,
                datetime.now(UTC),
                publisher_id=self.publisher_id,
                claim_token=claim_token,
            )
            published_event_ids.append(event.id)
            record_outbox_publish(
                outcome="success",
                duration_seconds=time.perf_counter() - started,
            )
            logger.info(
                "Outbox event published.",
                extra={
                    "event": "outbox.publish",
                    "outcome": "success",
                    "workflow_id": event.workflow_id,
                    "run_id": event.run_id,
                    "task_id": event.task_id,
                    "attempt_number": event.attempt_number,
                },
            )

        return OutboxPublishResult(
            attempted=len(events),
            published=len(published_event_ids),
            failed=len(failed_event_ids),
            published_event_ids=tuple(published_event_ids),
            failed_event_ids=tuple(failed_event_ids),
            discarded_event_ids=tuple(discarded_event_ids),
        )


class DispatchReconciliationService:
    def __init__(self, outbox_repository: DispatchOutboxRepository) -> None:
        self._outbox_repository = outbox_repository

    async def inspect(self) -> DispatchReconciliationResult:
        missing = (
            await self._outbox_repository.find_dispatched_attempts_missing_outbox()
        )
        unpublished = await self._outbox_repository.list_unpublished()
        return DispatchReconciliationResult(
            dispatched_attempts_missing_outbox=missing,
            unpublished_outbox_event_ids=tuple(event.id for event in unpublished),
        )
