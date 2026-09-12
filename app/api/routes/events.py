import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, require_rate_limit, require_role
from app.core.config import get_settings
from app.db.session import AsyncSessionLocal
from app.observability.metrics import (
    record_sse_closed,
    record_sse_event,
    record_sse_opened,
)
from app.schemas.api import RunEventListResponse, RunEventResponse
from app.security.models import Role
from app.services.events import RunEvent, RunEventRepository
from app.services.repositories import WorkflowRunRepository

router = APIRouter(prefix="/runs", tags=["run-events"])
_TERMINAL_EVENTS = {"run.succeeded", "run.failed", "run.cancelled"}


def _cursor(last_event_id: str | None, after: int | None) -> int | None:
    raw = last_event_id if last_event_id is not None else after
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            422, "Event cursor must be a non-negative integer."
        ) from exc
    if value < 0:
        raise HTTPException(422, "Event cursor must be a non-negative integer.")
    return value


def _payload(event: RunEvent) -> dict:
    return {
        "id": event.id,
        "version": event.version,
        "event_type": event.event_type,
        "workflow_id": event.workflow_id,
        "run_id": event.run_id,
        "task_id": event.task_id,
        "attempt_number": event.attempt_number,
        "created_at": event.created_at.isoformat(),
        "payload": event.payload,
    }


def _sse(event: RunEvent) -> str:
    data = json.dumps(_payload(event), separators=(",", ":"))
    return f"id: {event.id}\nevent: {event.event_type}\ndata: {data}\n\n"


@router.get(
    "/{run_id}/events/history",
    response_model=RunEventListResponse,
    summary="List durable run events",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def event_history(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    after: int | None = Query(default=None, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> RunEventListResponse:
    await WorkflowRunRepository(session).get_workflow_id(run_id)
    events = await RunEventRepository(session).list_after(
        run_id, after=after, limit=limit
    )
    return RunEventListResponse(
        items=tuple(RunEventResponse(**_payload(event)) for event in events),
        limit=limit,
        count=len(events),
    )


@router.get(
    "/{run_id}/events",
    summary="Stream durable run events",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def stream_events(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    last_event_id: Annotated[str | None, Header()] = None,
    after: int | None = Query(default=None, ge=0),
) -> StreamingResponse:
    cursor = _cursor(last_event_id, after)
    await WorkflowRunRepository(session).get_workflow_id(run_id)
    settings = get_settings()

    async def stream() -> AsyncIterator[str]:
        record_sse_opened()
        last_heartbeat = time.monotonic()
        current = cursor
        try:
            while True:
                async with AsyncSessionLocal() as poll_session:
                    events = await RunEventRepository(poll_session).list_after(
                        run_id, after=current, limit=settings.sse_event_batch_size
                    )
                for event in events:
                    current = event.id
                    record_sse_event()
                    yield _sse(event)
                    if event.event_type in _TERMINAL_EVENTS:
                        return
                now = time.monotonic()
                if now - last_heartbeat >= settings.sse_heartbeat_seconds:
                    yield ": heartbeat\n\n"
                    last_heartbeat = now
                await asyncio.sleep(settings.sse_poll_interval_seconds)
        finally:
            record_sse_closed()

    return StreamingResponse(stream(), media_type="text/event-stream")
