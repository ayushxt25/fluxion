from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventSubscriptionCreate(BaseModel):
    workflow_id: str
    workflow_revision: int | None = Field(default=None, ge=1)
    event_type: str = Field(min_length=1, max_length=255)
    filter_json: dict[str, Any] | None = None
    pass_event_payload_as_input: bool = False

    @field_validator("filter_json")
    @classmethod
    def validate_filter(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None and any(
            isinstance(item, (dict, list)) for item in value.values()
        ):
            raise ValueError("filter_json supports exact top-level scalar values only.")
        return value


class EventSubscriptionUpdate(BaseModel):
    workflow_revision: int | None = Field(default=None, ge=1)
    filter_json: dict[str, Any] | None = None
    pass_event_payload_as_input: bool | None = None


class EventSubscription(BaseModel):
    id: str
    workflow_id: str
    workflow_revision: int | None
    event_type: str
    filter_json: dict[str, Any] | None
    pass_event_payload_as_input: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime
    created_by_subject: str | None
    created_by_role: str | None


class EventSubscriptionList(BaseModel):
    items: tuple[EventSubscription, ...]
    limit: int
    offset: int
    count: int


class EventIngestRequest(BaseModel):
    source: str = Field(min_length=1, max_length=255)
    external_event_id: str = Field(min_length=1, max_length=255)
    event_type: str = Field(min_length=1, max_length=255)
    payload: Any = None


class EventIngestResponse(BaseModel):
    trigger_event_id: str
    created: bool
    matched_subscriptions: int
    run_ids: tuple[str, ...]
