import json

import httpx
import pytest

from app.sdk import AsyncFluxionClient, FluxionClient


def _schedule(*, revision: int | None = None) -> dict:
    return {
        "id": "schedule-1",
        "workflow_id": "workflow-1",
        "workflow_revision": revision,
        "schedule_type": "INTERVAL",
        "cron_expression": None,
        "interval_seconds": 60,
        "timezone": "Asia/Kolkata",
        "misfire_policy": "FIRE_ONCE",
        "enabled": True,
        "next_fire_at": "2026-09-28T00:01:00Z",
        "last_fire_at": None,
        "created_at": "2026-09-28T00:00:00Z",
        "updated_at": "2026-09-28T00:00:00Z",
        "created_by_subject": "operator",
        "created_by_role": "OPERATOR",
    }


def test_sync_schedule_lifecycle_serializes_revision_and_parses_models() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/schedules" and request.method == "GET":
            return httpx.Response(
                200, json={"items": [_schedule()], "limit": 100, "offset": 0, "count": 1}
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json=_schedule(revision=1))

    with FluxionClient("https://example.test", transport=httpx.MockTransport(handler)) as client:
        cron = client.create_schedule(
            workflow_id="workflow-1", schedule_type="CRON", cron_expression="0 9 * * *", revision=1
        )
        latest = client.create_schedule(
            workflow_id="workflow-1", schedule_type="INTERVAL", interval_seconds=60, revision=None
        )
        assert cron.workflow_revision == 1
        assert latest.next_fire_at.tzinfo is not None
        assert client.get_schedule("schedule-1").timezone == "Asia/Kolkata"
        assert client.list_schedules().items[0].misfire_policy == "FIRE_ONCE"
        assert client.update_schedule("schedule-1", interval_seconds=120).interval_seconds == 60
        assert client.pause_schedule("schedule-1").enabled
        assert client.resume_schedule("schedule-1").enabled
        client.delete_schedule("schedule-1")

    assert json.loads(requests[0].content)["workflow_revision"] == 1
    assert "workflow_revision" not in json.loads(requests[1].content)


@pytest.mark.asyncio
async def test_async_schedule_lifecycle_serializes_revision_and_parses_models() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/schedules" and request.method == "GET":
            return httpx.Response(
                200, json={"items": [_schedule()], "limit": 100, "offset": 0, "count": 1}
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json=_schedule(revision=None))

    async with AsyncFluxionClient("https://example.test", transport=httpx.MockTransport(handler)) as client:
        created = await client.create_schedule(
            workflow_id="workflow-1", schedule_type="INTERVAL", interval_seconds=60, revision=None
        )
        assert created.workflow_revision is None
        assert (await client.get_schedule("schedule-1")).enabled
        assert (await client.list_schedules()).count == 1
        await client.update_schedule("schedule-1", timezone="UTC")
        await client.pause_schedule("schedule-1")
        await client.resume_schedule("schedule-1")
        await client.delete_schedule("schedule-1")

    assert "workflow_revision" not in json.loads(requests[0].content)
