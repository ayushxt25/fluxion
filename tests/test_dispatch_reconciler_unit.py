from datetime import UTC, datetime

import pytest

from app.services.outbox import DispatchReconciler


class _Outbox:
    def __init__(self, event_ids: tuple[str, ...]) -> None:
        self.event_ids = event_ids
        self.kwargs = None

    async def reconcile_stale_published(self, **kwargs):
        self.kwargs = kwargs
        return self.event_ids


@pytest.mark.asyncio
async def test_reconciler_returns_repaired_existing_dispatches() -> None:
    repository = _Outbox(("event-1", "event-2"))
    reconciler = DispatchReconciler(
        repository,  # type: ignore[arg-type]
        reconcile_after_seconds=60,
        batch_size=10,
    )
    now = datetime(2026, 9, 29, tzinfo=UTC)

    result = await reconciler.reconcile_once(now)

    assert result.reconciled == 2
    assert result.reconciled_event_ids == ("event-1", "event-2")
    assert repository.kwargs["now"] == now
    assert repository.kwargs["reconcile_after_seconds"] == 60


def test_reconciler_rejects_unbounded_settings() -> None:
    repository = _Outbox(())
    with pytest.raises(ValueError):
        DispatchReconciler(
            repository,  # type: ignore[arg-type]
            reconcile_after_seconds=0,
            batch_size=1,
        )
