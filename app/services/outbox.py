from dataclasses import dataclass
from datetime import UTC, datetime

from app.dispatch.transport import TaskDispatcher
from app.engine.exceptions import DispatchError
from app.services.repositories import DispatchOutboxRepository


@dataclass(frozen=True)
class OutboxPublishResult:
    attempted: int
    published: int
    failed: int
    published_event_ids: tuple[str, ...]
    failed_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class DispatchReconciliationResult:
    dispatched_attempts_missing_outbox: tuple[tuple[str, str, int], ...]
    unpublished_outbox_event_ids: tuple[str, ...]


class DispatchOutboxPublisher:
    def __init__(
        self,
        outbox_repository: DispatchOutboxRepository,
        dispatcher: TaskDispatcher,
    ) -> None:
        self._outbox_repository = outbox_repository
        self._dispatcher = dispatcher

    async def publish_pending(self, limit: int = 100) -> OutboxPublishResult:
        events = await self._outbox_repository.list_unpublished(limit)
        published_event_ids = []
        failed_event_ids = []

        for event in events:
            try:
                await self._dispatcher.dispatch(event.message)
            except DispatchError as exc:
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                )
                failed_event_ids.append(event.id)
                continue
            except Exception as exc:
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                )
                failed_event_ids.append(event.id)
                continue

            await self._outbox_repository.mark_published(event.id, datetime.now(UTC))
            published_event_ids.append(event.id)

        return OutboxPublishResult(
            attempted=len(events),
            published=len(published_event_ids),
            failed=len(failed_event_ids),
            published_event_ids=tuple(published_event_ids),
            failed_event_ids=tuple(failed_event_ids),
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
