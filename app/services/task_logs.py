import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models.logs import TaskLogRecord
from app.engine.task_logging import FluxionTaskLogger, PendingTaskLog
from app.observability.metrics import (
    record_task_log_persistence_failed,
    record_task_logs_persisted,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskLog:
    sequence_number: int
    level: str
    message: str
    fields: dict | None
    created_at: datetime


class TaskLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        *,
        run_id: str,
        workflow_id: str,
        task_id: str,
        attempt_number: int,
        entries: tuple[PendingTaskLog, ...],
    ) -> None:
        if not entries:
            return
        async with self._session.begin():
            self._session.add_all(
                TaskLogRecord(
                    run_id=run_id,
                    workflow_id=workflow_id,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    sequence_number=entry.sequence_number,
                    level=entry.level,
                    message=entry.message,
                    fields=entry.fields,
                )
                for entry in entries
            )

    async def list_attempt(
        self,
        run_id: str,
        task_id: str,
        attempt_number: int,
        *,
        after_sequence: int | None = None,
        limit: int = 100,
    ) -> tuple[TaskLog, ...]:
        async with self._session.begin():
            query = (
                select(TaskLogRecord)
                .where(TaskLogRecord.run_id == run_id)
                .where(TaskLogRecord.task_id == task_id)
                .where(TaskLogRecord.attempt_number == attempt_number)
            )
            if after_sequence is not None:
                query = query.where(TaskLogRecord.sequence_number > after_sequence)
            result = await self._session.execute(
                query.order_by(TaskLogRecord.sequence_number).limit(limit)
            )
            return tuple(_log(record) for record in result.scalars())


def _log(record: TaskLogRecord) -> TaskLog:
    return TaskLog(
        sequence_number=record.sequence_number,
        level=record.level,
        message=record.message,
        fields=record.fields,
        created_at=record.created_at,
    )


class TaskLogFlusher:
    """Serializes best-effort threshold/final flushes for one attempt."""

    def __init__(
        self,
        task_logger: FluxionTaskLogger,
        repository: TaskLogRepository | None,
        *,
        run_id: str,
        workflow_id: str,
        task_id: str,
        attempt_number: int,
    ) -> None:
        self.logger = task_logger
        self._repository = repository
        self._identity = (run_id, workflow_id, task_id, attempt_number)
        self._lock = asyncio.Lock()
        self._pending: set[asyncio.Task[None]] = set()
        self._loop = asyncio.get_running_loop()

    def flush_soon(self) -> None:
        self._loop.call_soon_threadsafe(self._schedule_flush)

    def _schedule_flush(self) -> None:
        task = asyncio.create_task(self.flush())
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def flush(self) -> None:
        async with self._lock:
            entries = self.logger.drain()
            if not entries or self._repository is None:
                return
            try:
                run_id, workflow_id, task_id, attempt_number = self._identity
                await self._repository.append(
                    run_id=run_id,
                    workflow_id=workflow_id,
                    task_id=task_id,
                    attempt_number=attempt_number,
                    entries=entries,
                )
                record_task_logs_persisted(entries)
            except Exception:  # noqa: BLE001
                record_task_log_persistence_failed()
                logger.exception("Task diagnostic log persistence failed.")

    async def close(self) -> None:
        pending = tuple(self._pending)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await self.flush()


def create_attempt_log_flusher(
    *,
    repository: TaskLogRepository | None,
    run_id: str,
    workflow_id: str,
    task_id: str,
    attempt_number: int,
) -> TaskLogFlusher:
    settings = get_settings()
    task_logger = FluxionTaskLogger(
        max_message_bytes=settings.task_log_max_message_bytes,
        max_fields_bytes=settings.task_log_max_fields_bytes,
        max_entries=settings.task_log_max_entries_per_attempt,
        buffer_size=settings.task_log_buffer_size,
    )
    flusher = TaskLogFlusher(
        task_logger,
        repository,
        run_id=run_id,
        workflow_id=workflow_id,
        task_id=task_id,
        attempt_number=attempt_number,
    )
    task_logger.set_threshold_callback(flusher.flush_soon)
    return flusher
