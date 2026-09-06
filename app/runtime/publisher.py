import asyncio

from app.runtime.bootstrap import (
    install_shutdown_handlers,
    run_async,
    runtime_resources,
)
from app.services.loops import DispatchOutboxPublisherLoop
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import DispatchOutboxRepository


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    async with (
        runtime_resources() as resources,
        resources.session_factory() as session,
    ):
        publisher = DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            resources.dispatcher,
            claim_seconds=resources.settings.outbox_claim_seconds,
        )
        await DispatchOutboxPublisherLoop(publisher).run(stop_event)


def main() -> None:
    run_async(run)
