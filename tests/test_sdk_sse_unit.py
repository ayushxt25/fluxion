import json

import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionClient
from app.sdk.errors import FluxionAPIError


def _sse(event_id: int, event_type: str = "task.succeeded") -> str:
    data = {
        "id": event_id,
        "version": 1,
        "event_type": event_type,
        "workflow_id": "workflow",
        "run_id": "run",
        "task_id": "task",
        "attempt_number": 1,
        "created_at": "2026-09-12T00:00:00+00:00",
        "payload": {"status": "SUCCEEDED"},
    }
    return (
        f": heartbeat\n\nid: {event_id}\nevent: {event_type}\n"
        f"data: {json.dumps(data)}\n\n"
    )


def test_sync_watch_parses_events_and_sends_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["last-event-id"] == "4"
        return httpx.Response(
            200, text=_sse(5), headers={"content-type": "text/event-stream"}
        )

    with FluxionClient("http://test", transport=httpx.MockTransport(handler)) as client:
        events = list(client.watch_run("run", after=4))

    assert [event.id for event in events] == [5]
    assert events[0].event_type == "task.succeeded"


@pytest.mark.asyncio
async def test_async_watch_parses_events() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["last-event-id"] == "8"
        return httpx.Response(
            200, text=_sse(9), headers={"content-type": "text/event-stream"}
        )

    async with AsyncFluxionClient(
        "http://test", transport=httpx.MockTransport(handler)
    ) as client:
        events = [event async for event in client.watch_run("run", after=8)]

    assert [event.id for event in events] == [9]


def test_sync_watch_rejects_malformed_event() -> None:
    with FluxionClient(
        "http://test",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, text="data: not-json\n\n")
        ),
    ) as client, pytest.raises(FluxionAPIError):
        list(client.watch_run("run"))
