from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import install_api_handlers
from app.api.middleware import body_size_middleware, security_headers_middleware
from app.api.routes.health import router as health_router
from app.api.routes.observability import router as observability_router
from app.api.routes.operations import router as operations_router
from app.api.routes.runs import router as runs_router
from app.api.routes.workflows import router as workflows_router
from app.core.config import get_settings
from app.dispatch.transport import RedisTaskDispatcher
from app.observability.logging import configure_logging
from app.security.rate_limit import RedisRateLimiter


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    redis_dispatcher = RedisTaskDispatcher(
        settings.redis_url,
        settings.dispatch_queue_name,
    )
    rate_limiter = RedisRateLimiter(settings.redis_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.redis_dispatcher = redis_dispatcher
        app.state.rate_limiter = rate_limiter
        try:
            yield
        finally:
            await redis_dispatcher.aclose()
            await rate_limiter.aclose()

    app = FastAPI(
        title=settings.app_name,
        debug=settings.debug,
        lifespan=lifespan,
    )
    app.middleware("http")(security_headers_middleware)
    app.middleware("http")(body_size_middleware)
    install_api_handlers(app)
    app.include_router(health_router)
    app.include_router(observability_router)
    app.include_router(workflows_router, prefix="/api/v1")
    app.include_router(runs_router, prefix="/api/v1")
    app.include_router(operations_router, prefix="/api/v1")
    return app


app = create_app()
