import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionClient


def test_sync_reconcile_dispatches_uses_typed_operation_response() -> None:
    request: httpx.Request | None = None

    def handler(value: httpx.Request) -> httpx.Response:
        nonlocal request
        request = value
        return httpx.Response(
            200,
            json={
                "considered": 2,
                "reconciled": 1,
                "reconciled_event_ids": ["outbox-1"],
            },
        )

    with FluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client:
        result = client.reconcile_dispatches()

    assert request is not None
    assert request.method == "POST"
    assert request.url.path == "/api/v1/ops/dispatch/reconcile"
    assert result.reconciled_event_ids == ("outbox-1",)


@pytest.mark.asyncio
async def test_async_reconcile_dispatches_uses_typed_operation_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/v1/ops/dispatch/reconcile"
        return httpx.Response(
            200,
            json={
                "considered": 0,
                "reconciled": 0,
                "reconciled_event_ids": [],
            },
        )

    async with AsyncFluxionClient(
        "https://example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await client.reconcile_dispatches()

    assert result.reconciled == 0
