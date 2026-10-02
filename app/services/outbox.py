import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from app.dispatch.transport import TaskDispatcher
from app.engine.exceptions import DispatchError
from app.observability.metrics import (
    record_dispatch_reconciled,
    record_dispatch_reconciliation,
    record_outbox_publish,
)
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
    claim_seconds: float = 0.0
    validity_check_seconds: float = 0.0
    dispatch_seconds: float = 0.0
    mark_published_seconds: float = 0.0
    discard_seconds: float = 0.0
    failure_record_seconds: float = 0.0


@dataclass(frozen=True)
class DispatchReconciliationResult:
    dispatched_attempts_missing_outbox: tuple[tuple[str, str, int], ...]
    unpublished_outbox_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class DispatchReconcileResult:
    considered: int
    reconciled: int
    reconciled_event_ids: tuple[str, ...]


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

        claim_started = time.perf_counter()
        events = await self._outbox_repository.claim_unpublished(
            self.publisher_id,
            claim_token,
            datetime.now(UTC),
            self._claim_seconds,
            limit,
        )
        claim_seconds = time.perf_counter() - claim_started

        published_event_ids = []
        failed_event_ids = []
        discarded_event_ids = []
        successfully_dispatched = []

        validity_check_seconds = 0.0
        dispatch_seconds = 0.0
        mark_published_seconds = 0.0
        discard_seconds = 0.0
        failure_record_seconds = 0.0

        started_at = {event.id: time.perf_counter() for event in events}
        valid_event_ids = frozenset()
        if events:
            validity_started = time.perf_counter()
            valid_event_ids = (
                await self._outbox_repository.find_valid_dispatch_event_ids(
                    tuple(event.id for event in events)
                )
            )
            validity_check_seconds = time.perf_counter() - validity_started

        for event in events:
            started = started_at[event.id]

            if event.id not in valid_event_ids:
                discard_started = time.perf_counter()
                await self._outbox_repository.mark_discarded(
                    event.id,
                    datetime.now(UTC),
                    "dispatch target is no longer DISPATCHED.",
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                discard_seconds += time.perf_counter() - discard_started

                discarded_event_ids.append(event.id)
                record_outbox_publish(
                    outcome="discarded",
                    duration_seconds=time.perf_counter() - started,
                )
                continue

            try:
                dispatch_started = time.perf_counter()
                await self._dispatcher.dispatch(event.message)
                dispatch_seconds += time.perf_counter() - dispatch_started

            except DispatchError as exc:
                failure_started = time.perf_counter()
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                failure_record_seconds += time.perf_counter() - failure_started

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
                failure_started = time.perf_counter()
                await self._outbox_repository.record_publish_failure(
                    event.id,
                    str(exc),
                    publisher_id=self.publisher_id,
                    claim_token=claim_token,
                )
                failure_record_seconds += time.perf_counter() - failure_started

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

            successfully_dispatched.append((event, started))

        if successfully_dispatched:
            mark_started = time.perf_counter()
            await self._outbox_repository.mark_published_batch(
                tuple(event.id for event, _ in successfully_dispatched),
                datetime.now(UTC),
                publisher_id=self.publisher_id,
                claim_token=claim_token,
            )
            mark_published_seconds += time.perf_counter() - mark_started

        for event, started in successfully_dispatched:
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
            claim_seconds=claim_seconds,
            validity_check_seconds=validity_check_seconds,
            dispatch_seconds=dispatch_seconds,
            mark_published_seconds=mark_published_seconds,
            discard_seconds=discard_seconds,
            failure_record_seconds=failure_record_seconds,
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


class DispatchReconciler:
    """Repairs stale published transport intent without creating another attempt."""

    def __init__(
        self,
        outbox_repository: DispatchOutboxRepository,
        *,
        reconcile_after_seconds: float,
        batch_size: int,
    ) -> None:
        if reconcile_after_seconds <= 0 or batch_size <= 0:
            raise ValueError("Reconciliation settings must be positive.")
        self._outbox_repository = outbox_repository
        self._reconcile_after_seconds = reconcile_after_seconds
        self._batch_size = batch_size

    async def reconcile_once(
        self,
        now: datetime | None = None,
    ) -> DispatchReconcileResult:
        event_ids = await self._outbox_repository.reconcile_stale_published(
            now=now or datetime.now(UTC),
            reconcile_after_seconds=self._reconcile_after_seconds,
            limit=self._batch_size,
        )
        record_dispatch_reconciliation(outcome="reconciled", count=len(event_ids))
        record_dispatch_reconciled(len(event_ids))
        if event_ids:
            logger.info("Reconciled %s stale dispatches.", len(event_ids))
        return DispatchReconcileResult(
            considered=len(event_ids),
            reconciled=len(event_ids),
            reconciled_event_ids=event_ids,
        )
