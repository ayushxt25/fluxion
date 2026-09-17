import pytest

from app.db.models.webhooks import WebhookDeliveryRecord
from app.services.webhooks import WebhookRepository
from tests.integration.test_webhooks import event, in_db


def test_claim_fencing_and_expiry_reclaim():
    async def body(session):
        repo = WebhookRepository(session)
        await repo.create_subscription(
            "hook", "https://example.com", "s", ("run.succeeded",)
        )
        async with session.begin():
            session.add(event())
        await repo.reconcile()
        first = (await repo.claim_due("worker-a", 30, 1))[0]
        assert await repo.claim_due("worker-b", 30, 1) == ()
        with pytest.raises(RuntimeError):
            await repo.complete(first.id, "worker-b", first.claim_token or "", 200)
        async with session.begin():
            row = await session.get(WebhookDeliveryRecord, first.id)
            row.claim_expires_at = row.claim_expires_at.replace(year=2000)
        second = (await repo.claim_due("worker-b", 30, 1))[0]
        await repo.complete(second.id, "worker-b", second.claim_token or "", 204)

    in_db(body)


def test_stale_claim_cannot_retry_or_dead_letter():
    async def body(session):
        repo = WebhookRepository(session)
        await repo.create_subscription(
            "hook", "https://example.com", "s", ("run.succeeded",)
        )
        async with session.begin():
            session.add(event())
        await repo.reconcile()
        claim = (await repo.claim_due("a", 30, 1))[0]
        with pytest.raises(RuntimeError):
            await repo.retry_or_dead(
                claim.id,
                "b",
                claim.claim_token or "",
                status_code=500,
                error_type="x",
                error_message="x",
                next_attempt_at=__import__("datetime").datetime.now(
                    __import__("datetime").UTC
                ),
                max_attempts=1,
            )

    in_db(body)
