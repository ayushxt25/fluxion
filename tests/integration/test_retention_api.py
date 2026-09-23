import os

import pytest
from sqlalchemy import select

from app.core.config import get_settings
from app.db.models.audit import AuditEventRecord
from app.security.models import Role
from tests.integration.test_api import api_client, auth_headers

pytestmark = pytest.mark.asyncio


async def test_admin_can_preview_retention_and_roles_are_enforced():
    async with api_client() as (client, _):
        response = await client.get(
            "/api/v1/ops/retention/preview", headers=auth_headers(Role.ADMIN)
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dry_run"] is True
        assert set(body["categories"])

        for role in (Role.VIEWER, Role.OPERATOR):
            denied = await client.get(
                "/api/v1/ops/retention/preview", headers=auth_headers(role)
            )
            assert denied.status_code == 403
        assert (await client.get("/api/v1/ops/retention/preview")).status_code == 401


async def test_retention_run_is_gated_when_disabled():
    async with api_client() as (client, _):
        response = await client.post(
            "/api/v1/ops/retention/run", json={}, headers=auth_headers(Role.ADMIN)
        )
        assert response.status_code == 409


async def test_enabled_manual_run_is_audited_and_validates_categories(monkeypatch):
    monkeypatch.setenv("RETENTION_ENABLED", "true")
    get_settings.cache_clear()
    try:
        async with api_client() as (client, session_factory):
            response = await client.post(
                "/api/v1/ops/retention/run",
                json={"categories": ["audit_events"]},
                headers=auth_headers(Role.ADMIN),
            )
            assert response.status_code == 200
            assert response.json()["dry_run"] is False
            invalid = await client.post(
                "/api/v1/ops/retention/run",
                json={"categories": ["not-a-category"]},
                headers=auth_headers(Role.ADMIN),
            )
            assert invalid.status_code == 422
            async with session_factory() as session:
                event = (
                    await session.execute(
                        select(AuditEventRecord).where(
                            AuditEventRecord.action == "ops.retention.run"
                        )
                    )
                ).scalar_one()
                assert event.event_metadata == {
                    "categories": ["audit_events"],
                    "total_deleted": 0,
                    "deleted": {"audit_events": 0},
                }
    finally:
        os.environ.pop("RETENTION_ENABLED", None)
        get_settings.cache_clear()
