import pytest

from app.security.auth import create_access_token
from app.security.models import Role
from tests.integration.test_api import api_client

pytestmark = pytest.mark.asyncio


def headers(role=Role.ADMIN):
    return {"Authorization": f"Bearer {create_access_token('webhook-test', role)}"}


async def test_admin_webhook_crud_never_serializes_secret():
    async with api_client() as (client, _):
        payload = {
            "name": "hook",
            "target_url": "https://example.com/h",
            "secret": "raw-secret",
            "event_types": ["run.succeeded"],
        }
        created = await client.post("/api/v1/webhooks", json=payload, headers=headers())
        assert created.status_code == 201 and "raw-secret" not in created.text
        webhook_id = created.json()["id"]
        assert created.json()["secret_configured"] is True
        assert (
            await client.get("/api/v1/webhooks", headers=headers())
        ).status_code == 200
        assert (
            await client.get(f"/api/v1/webhooks/{webhook_id}", headers=headers())
        ).status_code == 200
        updated = await client.patch(
            f"/api/v1/webhooks/{webhook_id}",
            json={"name": "new", "secret": "rotated"},
            headers=headers(),
        )
        assert updated.status_code == 200 and "rotated" not in updated.text
        assert (
            await client.post(
                f"/api/v1/webhooks/{webhook_id}/disable", headers=headers()
            )
        ).status_code == 204


async def test_webhook_api_rejects_non_admins_and_missing_resources():
    async with api_client() as (client, _):
        for role in (Role.VIEWER, Role.OPERATOR):
            assert (
                await client.get("/api/v1/webhooks", headers=headers(role))
            ).status_code == 403
        assert (
            await client.get("/api/v1/webhooks/missing", headers=headers())
        ).status_code == 404
        assert (
            await client.get("/api/v1/webhook-deliveries/missing", headers=headers())
        ).status_code == 404
