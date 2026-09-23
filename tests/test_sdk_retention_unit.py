import json

import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionClient, RetentionSummary, ValidationError


def payload(*, dry_run: bool) -> dict:
    return {
        "dry_run": dry_run,
        "categories": {"task_logs": {"examined": 2, "eligible": 2, "deleted": 1}},
        "total_deleted": 1,
        "started_at": "2026-09-23T00:00:00Z",
        "completed_at": "2026-09-23T00:00:01Z",
    }


def test_sync_retention_methods_parse_and_serialize_categories():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=payload(dry_run=request.method == "GET"))

    with FluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert isinstance(client.preview_retention(("task_logs",)), RetentionSummary)
        assert client.run_retention(("audit_events",)).total_deleted == 1
    assert seen[0].url.params.get_list("categories") == ["task_logs"]
    assert json.loads(seen[1].content) == {"categories": ["audit_events"]}


@pytest.mark.asyncio
async def test_async_retention_methods_parse_and_serialize_categories():
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=payload(dry_run=request.method == "GET"))

    async with AsyncFluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert (await client.preview_retention()).dry_run
        assert (await client.run_retention(("audit_events",))).total_deleted == 1
    assert not seen[0].url.params
    assert json.loads(seen[1].content) == {"categories": ["audit_events"]}


def test_retention_http_errors_are_typed():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"error": {"message": "invalid"}})

    with FluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client, pytest.raises(ValidationError):
        client.preview_retention()
