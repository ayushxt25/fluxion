from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WorkflowEventSubscriptionRecord(Base):
    __tablename__ = "workflow_event_subscriptions"
    __table_args__ = (Index("ix_event_subscriptions_type_enabled", "event_type", "enabled"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), ForeignKey("workflow_definitions.id"), nullable=False)
    workflow_revision: Mapped[int | None] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    filter_json: Mapped[dict | None] = mapped_column(JSONB)
    pass_event_payload_as_input: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_by_subject: Mapped[str | None] = mapped_column(String(255))
    created_by_role: Mapped[str | None] = mapped_column(String(32))


class WorkflowTriggerEventRecord(Base):
    __tablename__ = "workflow_trigger_events"
    __table_args__ = (UniqueConstraint("source", "external_event_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_json: Mapped[dict | list | str | int | float | bool | None] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class WorkflowEventFiringRecord(Base):
    __tablename__ = "workflow_event_firings"
    __table_args__ = (UniqueConstraint("subscription_id", "trigger_event_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    subscription_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_event_subscriptions.id"), nullable=False)
    trigger_event_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_trigger_events.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
