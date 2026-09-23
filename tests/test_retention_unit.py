from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.observability.metrics import render_prometheus, reset_metrics_for_tests
from app.services.retention import (
    CATEGORIES,
    RetentionCategorySummary,
    RetentionService,
    is_actionable_outbox,
    is_terminal_run_status,
)


class RecordingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime, int, bool]] = []

    async def delete_category(self, category, cutoff, limit, dry_run):
        self.calls.append((category, cutoff, limit, dry_run))
        return RetentionCategorySummary(
            examined=2, eligible=2, deleted=0 if dry_run else 2
        )


def test_retention_config_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="Retention"):
        Settings(retention_task_log_days=0)
    with pytest.raises(ValueError, match="Retention"):
        Settings(retention_batch_size=0)


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "CANCELLED"])
def test_terminal_statuses_are_eligible(status: str) -> None:
    assert is_terminal_run_status(status)


@pytest.mark.parametrize("status", ["PENDING", "RUNNING", "INTERRUPTED"])
def test_incomplete_statuses_are_not_eligible(status: str) -> None:
    assert not is_terminal_run_status(status)


def test_outbox_actionability_requires_publication_or_discard() -> None:
    now = datetime.now(UTC)
    assert is_actionable_outbox(published_at=None, discarded_at=None)
    assert not is_actionable_outbox(published_at=now, discarded_at=None)
    assert not is_actionable_outbox(published_at=None, discarded_at=now)


@pytest.mark.asyncio
async def test_category_selection_and_dry_run_summary() -> None:
    repository = RecordingRepository()
    settings = Settings(retention_batch_size=7)
    summary = await RetentionService(repository, settings).preview_retention(
        categories=("task_logs", "audit_events")
    )
    assert summary.dry_run
    assert summary.total_deleted == 0
    assert tuple(summary.categories) == ("task_logs", "audit_events")
    assert all(call[2:] == (7, True) for call in repository.calls)


@pytest.mark.asyncio
async def test_cleanup_summary_totals_and_cutoffs() -> None:
    repository = RecordingRepository()
    settings = Settings(retention_task_log_days=3, retention_batch_size=4)
    before = datetime.now(UTC)
    summary = await RetentionService(repository, settings).run_retention(
        categories=("task_logs",)
    )
    assert summary.total_deleted == 2
    category, cutoff, limit, dry_run = repository.calls[0]
    assert category == "task_logs" and limit == 4 and not dry_run
    assert before - timedelta(days=3, seconds=1) < cutoff < datetime.now(UTC)


@pytest.mark.asyncio
async def test_retention_metrics_use_only_bounded_labels() -> None:
    reset_metrics_for_tests()
    await RetentionService(RecordingRepository(), Settings()).run_retention(
        categories=("task_logs",)
    )
    rendered = render_prometheus()
    assert 'fluxion_retention_runs_total{outcome="success"} 1' in rendered
    assert 'fluxion_retention_deleted_total{category="task_logs"} 2' in rendered
    assert "run_id" not in rendered and "workflow_id" not in rendered


@pytest.mark.asyncio
async def test_unknown_category_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown"):
        await RetentionService(RecordingRepository(), Settings()).preview_retention(
            categories=("nope",)
        )


def test_categories_are_bounded() -> None:
    assert set(CATEGORIES) == {
        "task_logs",
        "run_events",
        "webhook_deliveries",
        "dispatch_outbox",
        "audit_events",
        "workflow_runs",
    }
