import json

import httpx
import pytest

from app.sdk import AsyncFluxionClient, ConflictError, FluxionClient, TaskIntervention


def _item(resolution: str = "PENDING") -> dict:
    return {
        "id": "intervention-1",
        "workflow_id": "workflow-1",
        "run_id": "run-1",
        "task_id": "task-1",
        "interrupted_attempt_number": 1,
        "resolution": resolution,
        "created_at": "2026-10-01T00:00:00Z",
        "resolved_at": None,
        "resolver_subject": None,
        "resolver_role": None,
        "reason": None,
        "resulting_retry_attempt_number": None,
    }


def test_sync_intervention_methods_serialize_and_parse_models() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/ops/interventions":
            return httpx.Response(
                200,
                json={"items": [_item()], "limit": 3, "offset": 2, "count": 1},
            )
        return httpx.Response(200, json=_item("RETRY"))

    with FluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert all(
            hasattr(client, name)
            for name in (
                "list_interventions",
                "get_intervention",
                "retry_intervention",
                "fail_intervention",
            )
        )
        listing = client.list_interventions(limit=3, offset=2)
        item = client.get_intervention("intervention-1")
        retry = client.retry_intervention("intervention-1", reason="safe")
        failed = client.fail_intervention("intervention-1")

    assert listing.items[0].id == item.id == retry.id == failed.id
    assert requests[0].url.params.get("limit") == "3"
    assert requests[0].url.params.get("offset") == "2"
    assert requests[1].method == "GET"
    assert requests[1].url.path.endswith("/intervention-1")
    assert requests[2].method == "POST"
    assert requests[2].url.path.endswith("/retry")
    assert json.loads(requests[2].content) == {"reason": "safe"}
    assert json.loads(requests[3].content) == {"reason": None}


@pytest.mark.asyncio
async def test_async_intervention_methods_serialize_and_parse_models() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/ops/interventions":
            return httpx.Response(
                200,
                json={"items": [_item()], "limit": 50, "offset": 0, "count": 1},
            )
        return httpx.Response(200, json=_item("FAIL"))

    async with AsyncFluxionClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert all(
            hasattr(client, name)
            for name in (
                "list_interventions",
                "get_intervention",
                "retry_intervention",
                "fail_intervention",
            )
        )
        listing = await client.list_interventions()
        item = await client.get_intervention("intervention-1")
        retry = await client.retry_intervention("intervention-1", reason="safe")
        failed = await client.fail_intervention("intervention-1")

    assert isinstance(listing.items[0], TaskIntervention)
    assert item.id == retry.id == failed.id
    assert json.loads(requests[2].content) == {"reason": "safe"}


def test_intervention_conflict_is_a_typed_sdk_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"error": {"message": "resolved"}})

    with (
        FluxionClient(
            "https://example.test", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ConflictError),
    ):
        client.retry_intervention("intervention-1")
