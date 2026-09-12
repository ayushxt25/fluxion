from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.events import RunEventRecord


@dataclass(frozen=True)
class RunEvent:
    id: int
    version: int
    event_type: str
    workflow_id: str
    run_id: str
    task_id: str | None
    attempt_number: int | None
    created_at: datetime
    payload: dict | None


class RunEventRepository:
    """Durable event access; callers own the surrounding transaction."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def record(
        self,
        *,
        run_id: str,
        workflow_id: str,
        event_type: str,
        task_id: str | None = None,
        attempt_number: int | None = None,
        payload: dict | None = None,
    ) -> None:
        self._session.add(
            RunEventRecord(
                run_id=run_id,
                workflow_id=workflow_id,
                event_type=event_type,
                task_id=task_id,
                attempt_number=attempt_number,
                version=1,
                payload=payload,
            )
        )

    async def list_after(
        self, run_id: str, *, after: int | None = None, limit: int = 100
    ) -> tuple[RunEvent, ...]:
        async with self._session.begin():
            query = select(RunEventRecord).where(RunEventRecord.run_id == run_id)
            if after is not None:
                query = query.where(RunEventRecord.id > after)
            result = await self._session.execute(
                query.order_by(RunEventRecord.id).limit(limit)
            )
            return tuple(_event(record) for record in result.scalars())


def _event(record: RunEventRecord) -> RunEvent:
    return RunEvent(
        id=record.id,
        version=record.version,
        event_type=record.event_type,
        workflow_id=record.workflow_id,
        run_id=record.run_id,
        task_id=record.task_id,
        attempt_number=record.attempt_number,
        created_at=record.created_at,
        payload=record.payload,
    )
