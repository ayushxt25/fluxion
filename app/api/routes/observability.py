import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, get_redis_dispatcher
from app.core.config import get_settings
from app.dispatch.transport import RedisTaskDispatcher
from app.observability.metrics import PROMETHEUS_CONTENT_TYPE, render_prometheus

router = APIRouter(tags=["observability"])


@router.get("/metrics", summary="Expose Prometheus metrics")
async def metrics() -> Response:
    return Response(
        content=render_prometheus(),
        media_type=PROMETHEUS_CONTENT_TYPE,
    )


@router.get("/ready", summary="Check service readiness")
async def ready(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    dispatcher: Annotated[RedisTaskDispatcher, Depends(get_redis_dispatcher)],
) -> JSONResponse:
    settings = get_settings()
    checks = {
        "postgres": "ok",
        "redis": "ok",
    }

    try:
        await asyncio.wait_for(
            session.execute(text("SELECT 1")),
            timeout=settings.readiness_timeout_seconds,
        )
    except Exception:
        checks["postgres"] = "failed"

    try:
        await asyncio.wait_for(
            dispatcher.ping(),
            timeout=settings.readiness_timeout_seconds,
        )
    except Exception:
        checks["redis"] = "failed"

    if all(value == "ok" for value in checks.values()):
        return JSONResponse(content={"status": "ready", "checks": checks})

    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "checks": checks},
    )
