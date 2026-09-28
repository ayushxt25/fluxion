# ruff: noqa: E501
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WorkflowScheduleRecord(Base):
    __tablename__ = "workflow_schedules"
    __table_args__ = (
        Index("ix_workflow_schedules_due", "enabled", "next_fire_at"),
        Index("ix_workflow_schedules_workflow", "workflow_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    workflow_revision: Mapped[int | None] = mapped_column(Integer)
    schedule_type: Mapped[str] = mapped_column(String(16), nullable=False)
    cron_expression: Mapped[str | None] = mapped_column(String(128))
    interval_seconds: Mapped[int | None] = mapped_column(Integer)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")
    misfire_policy: Mapped[str] = mapped_column(
        String(16), nullable=False, default="SKIP"
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    next_fire_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_fire_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    created_by_subject: Mapped[str | None] = mapped_column(String(255))
    created_by_role: Mapped[str | None] = mapped_column(String(32))


class WorkflowScheduleFiringRecord(Base):
    __tablename__ = "workflow_schedule_firings"
    __table_args__ = (UniqueConstraint("schedule_id", "scheduled_for"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    schedule_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_schedules.id", ondelete="RESTRICT"),
        nullable=False,
    )
    scheduled_for: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    run_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
