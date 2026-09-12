import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionTimeoutError


@pytest.mark.asyncio
async def test_async_client_auth_context_and_wait() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["authorization"] == "Bearer async-token"
        return httpx.Response(
            200,
            json={
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "status": "SUCCEEDED",
                "created_at": "2026-09-01T00:00:00Z",
                "has_input": True,
                "input": None,
                "tasks": [],
            },
        )

    async with AsyncFluxionClient(
        "https://fluxion.example/",
        token="async-token",
        transport=httpx.MockTransport(handler),
    ) as client:
        run = await client.wait_for_run("run-1", timeout=1, poll_interval=0.01)
        assert run.has_input
    assert calls == 1
    assert client._client.is_closed


@pytest.mark.asyncio
async def test_async_wait_timeout() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "run_id": "run-1",
                "workflow_id": "workflow-1",
                "status": "RUNNING",
                "created_at": "2026-09-01T00:00:00Z",
                "has_input": False,
                "tasks": [],
            },
        )

    async with AsyncFluxionClient(
        "https://fluxion.example",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(FluxionTimeoutError):
            await client.wait_for_run("run-1", timeout=0.001, poll_interval=0.001)
