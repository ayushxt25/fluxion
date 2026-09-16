from datetime import datetime

import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionClient


def _response(_: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "items": [
                {
                    "sequence": 1,
                    "level": "INFO",
                    "message": "Processed",
                    "fields": {"records": 2},
                    "created_at": datetime.now().isoformat(),
                }
            ],
            "limit": 100,
            "count": 1,
        },
    )


def test_sync_sdk_gets_typed_attempt_logs() -> None:
    with FluxionClient(
        "http://test", transport=httpx.MockTransport(_response)
    ) as client:
        logs = client.get_attempt_logs("run", "task", 1)
    assert logs.items[0].sequence == 1
    assert logs.items[0].fields == {"records": 2}


@pytest.mark.asyncio
async def test_async_sdk_gets_typed_attempt_logs() -> None:
    async with AsyncFluxionClient(
        "http://test", transport=httpx.MockTransport(_response)
    ) as client:
        logs = await client.get_attempt_logs("run", "task", 1, after_sequence=0)
    assert logs.items[0].level == "INFO"
