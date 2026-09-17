import asyncio
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.observability.logging import configure_logging
from app.runtime.bootstrap import (
    install_shutdown_handlers,
    parse_runtime_arguments,
    run_async,
)
from app.services.webhooks import WebhookDeliveryService, WebhookRepository


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        while not stop_event.is_set():
            async with sessions() as session:
                service = WebhookDeliveryService(
                    WebhookRepository(session),
                    claim_seconds=settings.webhook_claim_seconds,
                    max_attempts=settings.webhook_max_attempts,
                    initial_backoff_seconds=settings.webhook_initial_backoff_seconds,
                    backoff_multiplier=settings.webhook_backoff_multiplier,
                    max_backoff_seconds=settings.webhook_max_backoff_seconds,
                    allow_insecure_http=settings.webhook_allow_insecure_http,
                    allow_private_networks=settings.webhook_allow_private_networks,
                    timeout_seconds=settings.webhook_request_timeout_seconds,
                )
                if settings.webhook_enabled:
                    await service.deliver_pending(settings.webhook_batch_size)
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.webhook_poll_interval_seconds
                )
    finally:
        await engine.dispose()


def main() -> None:
    parse_runtime_arguments(
        "fluxion-webhooks", "Run the Fluxion webhook delivery service."
    )
    run_async(run)
