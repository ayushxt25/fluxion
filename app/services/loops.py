import asyncio
import logging
from dataclasses import dataclass

from app.core.config import get_settings
from app.engine.exceptions import DispatchError, PersistenceError, WorkerLeaseError
from app.observability.metrics import record_scheduler_tick_dispatches
from app.services.leases import LeaseReaper, LeaseReclaimResult
from app.services.outbox import DispatchOutboxPublisher, OutboxPublishResult
from app.services.repositories import WorkflowRunRepository
from app.services.scheduler import DispatchSummary, WorkflowScheduler

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerTickResult:
    scheduled: tuple[DispatchSummary, ...]


@dataclass(frozen=True)
class LeaseReaperTickResult:
    reclaimed: tuple[LeaseReclaimResult, ...]


class SchedulerLoop:
    def __init__(
        self,
        scheduler: WorkflowScheduler,
        run_repository: WorkflowRunRepository,
        *,
        poll_seconds: float | None = None,
        max_dispatch_per_tick: int | None = None,
    ) -> None:
        settings = get_settings()
        self._scheduler = scheduler
        self._run_repository = run_repository
        self._poll_seconds = (
            poll_seconds
            if poll_seconds is not None
            else settings.scheduler_poll_seconds
        )
        self._max_dispatch_per_tick = (
            max_dispatch_per_tick
            if max_dispatch_per_tick is not None
            else settings.scheduler_max_dispatch_per_tick
        )
        if self._poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive.")
        if self._max_dispatch_per_tick <= 0:
            raise ValueError("max_dispatch_per_tick must be positive.")

    async def tick(self) -> SchedulerTickResult:
        scheduled = []
        remaining = self._max_dispatch_per_tick
        for run_ref in await self._run_repository.list_incomplete():
            if remaining <= 0:
                break
            try:
                summary = await self._scheduler.dispatch_ready(
                    run_ref.run_id,
                    max_dispatch=remaining,
                )
            except (DispatchError, PersistenceError):
                logger.exception("Scheduler tick failed for run %s.", run_ref.run_id)
                continue
            if summary.dispatched_task_ids:
                scheduled.append(summary)
                remaining -= len(summary.dispatched_task_ids)
        dispatched = sum(len(summary.dispatched_task_ids) for summary in scheduled)
        record_scheduler_tick_dispatches(dispatched)
        return SchedulerTickResult(scheduled=tuple(scheduled))

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("Scheduler loop started.")
        try:
            while not stop_event.is_set():
                await self.tick()
                await _sleep_or_stop(stop_event, self._poll_seconds)
        finally:
            logger.info("Scheduler loop stopped.")


class DispatchOutboxPublisherLoop:
    def __init__(
        self,
        publisher: DispatchOutboxPublisher,
        *,
        poll_seconds: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        settings = get_settings()
        self._publisher = publisher
        self._poll_seconds = (
            poll_seconds if poll_seconds is not None else settings.outbox_poll_seconds
        )
        self._batch_size = (
            batch_size if batch_size is not None else settings.outbox_batch_size
        )
        if self._poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive.")
        if self._batch_size <= 0:
            raise ValueError("batch_size must be positive.")

    async def tick(self) -> OutboxPublishResult:
        try:
            return await self._publisher.publish_pending(self._batch_size)
        except (DispatchError, PersistenceError):
            logger.exception("Outbox publisher tick failed.")
            return OutboxPublishResult(
                attempted=0,
                published=0,
                failed=0,
                published_event_ids=(),
                failed_event_ids=(),
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("Outbox publisher loop started.")
        try:
            while not stop_event.is_set():
                await self.tick()
                await _sleep_or_stop(stop_event, self._poll_seconds)
        finally:
            logger.info("Outbox publisher loop stopped.")


class LeaseReaperLoop:
    def __init__(
        self,
        reaper: LeaseReaper,
        *,
        interval_seconds: float | None = None,
    ) -> None:
        settings = get_settings()
        self._reaper = reaper
        self._interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else settings.lease_reaper_interval_seconds
        )
        if self._interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive.")

    async def tick(self) -> LeaseReaperTickResult:
        try:
            reclaimed = await self._reaper.reclaim_expired()
        except (PersistenceError, WorkerLeaseError):
            logger.exception("Lease reaper tick failed.")
            return LeaseReaperTickResult(reclaimed=())
        if reclaimed:
            logger.info("Lease reaper reclaimed %s expired attempts.", len(reclaimed))
        return LeaseReaperTickResult(reclaimed=reclaimed)

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("Lease reaper loop started.")
        try:
            while not stop_event.is_set():
                await self.tick()
                await _sleep_or_stop(stop_event, self._interval_seconds)
        finally:
            logger.info("Lease reaper loop stopped.")


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        return
