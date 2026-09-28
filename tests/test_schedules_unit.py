from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas.schedules import MisfirePolicy, ScheduleCreate, ScheduleType
from app.services.schedules import _next_fire


def test_interval_and_cron_schedule_validation_and_next_fire() -> None:
    interval = ScheduleCreate(
        workflow_id="wf", schedule_type=ScheduleType.INTERVAL, interval_seconds=60
    )
    cron = ScheduleCreate(
        workflow_id="wf", schedule_type=ScheduleType.CRON, cron_expression="0 9 * * *"
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)

    assert _next_fire(interval, base).minute == 1
    assert _next_fire(cron, base).hour == 9
    assert cron.misfire_policy is MisfirePolicy.SKIP


@pytest.mark.parametrize(
    "payload",
    (
        {"schedule_type": "CRON", "interval_seconds": 5},
        {"schedule_type": "INTERVAL", "cron_expression": "* * * * *"},
        {"schedule_type": "CRON", "cron_expression": "invalid"},
        {"schedule_type": "INTERVAL", "interval_seconds": 0},
        {"schedule_type": "INTERVAL", "interval_seconds": 5, "timezone": "Nope/Zone"},
    ),
)
def test_invalid_schedule_shapes_are_rejected(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ScheduleCreate(workflow_id="wf", **payload)


def test_cron_uses_its_iana_timezone_and_returns_an_aware_utc_instant() -> None:
    schedule = ScheduleCreate(
        workflow_id="wf",
        schedule_type=ScheduleType.CRON,
        cron_expression="0 9 * * *",
        timezone="Asia/Kolkata",
    )
    fire_at = _next_fire(schedule, datetime(2026, 1, 1, tzinfo=UTC))
    assert fire_at == datetime(2026, 1, 1, 3, 30, tzinfo=UTC)
