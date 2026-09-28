from datetime import datetime
from enum import StrEnum
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, model_validator


class ScheduleType(StrEnum):
    CRON = "CRON"
    INTERVAL = "INTERVAL"


class MisfirePolicy(StrEnum):
    SKIP = "SKIP"
    FIRE_ONCE = "FIRE_ONCE"


class ScheduleCreate(BaseModel):
    workflow_id: str
    workflow_revision: int | None = Field(default=None, ge=1)
    schedule_type: ScheduleType
    cron_expression: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1)
    timezone: str = "UTC"
    misfire_policy: MisfirePolicy = MisfirePolicy.SKIP
    next_fire_at: datetime | None = None

    @model_validator(mode="after")
    def validate_schedule(self) -> "ScheduleCreate":
        try:
            ZoneInfo(self.timezone)
        except Exception as exc:
            raise ValueError("timezone must be a valid IANA timezone.") from exc
        if self.schedule_type is ScheduleType.CRON:
            if not self.cron_expression or self.interval_seconds is not None:
                raise ValueError("CRON schedules require cron_expression only.")
            _validate_cron(self.cron_expression)
        elif self.interval_seconds is None or self.cron_expression is not None:
            raise ValueError("INTERVAL schedules require interval_seconds only.")
        return self


class ScheduleUpdate(BaseModel):
    workflow_revision: int | None = Field(default=None, ge=1)
    cron_expression: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1)
    timezone: str | None = None
    misfire_policy: MisfirePolicy | None = None


class WorkflowSchedule(BaseModel):
    id: str
    workflow_id: str
    workflow_revision: int | None
    schedule_type: ScheduleType
    cron_expression: str | None
    interval_seconds: int | None
    timezone: str
    misfire_policy: MisfirePolicy
    enabled: bool
    next_fire_at: datetime
    last_fire_at: datetime | None
    created_at: datetime
    updated_at: datetime
    created_by_subject: str | None
    created_by_role: str | None


class WorkflowScheduleList(BaseModel):
    items: tuple[WorkflowSchedule, ...]
    limit: int
    offset: int
    count: int


def _validate_cron(expression: str) -> None:
    fields = expression.split()
    if len(fields) != 5 or any(not _valid_cron_field(field) for field in fields):
        raise ValueError("cron_expression must be a valid five-field cron expression.")


def _valid_cron_field(field: str) -> bool:
    import re

    return bool(re.fullmatch(r"[0-9*/,-]+", field))
