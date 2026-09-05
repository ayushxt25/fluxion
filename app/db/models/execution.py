from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class WorkflowRunRecord(Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        UniqueConstraint("run_id", "workflow_id", name="uq_workflow_runs_run_workflow"),
    )

    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    task_runs: Mapped[list["TaskRunRecord"]] = relationship(
        back_populates="workflow_run",
        cascade="save-update, merge",
        foreign_keys="[TaskRunRecord.run_id, TaskRunRecord.workflow_id]",
        lazy="selectin",
    )


class TaskRunRecord(Base):
    __tablename__ = "task_runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "workflow_id"],
            ["workflow_runs.run_id", "workflow_runs.workflow_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["workflow_id", "task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "run_id",
            "workflow_id",
            "task_id",
            name="uq_task_runs_run_workflow_task",
        ),
        UniqueConstraint(
            "idempotency_key",
            name="uq_task_runs_idempotency_key",
        ),
    )

    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    idempotency_key: Mapped[str] = mapped_column(String(768), nullable=False)

    workflow_run: Mapped[WorkflowRunRecord] = relationship(
        back_populates="task_runs",
        foreign_keys=[run_id, workflow_id],
    )


class TaskAttemptRecord(Base):
    __tablename__ = "task_attempts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "workflow_id", "task_id"],
            ["task_runs.run_id", "task_runs.workflow_id", "task_runs.task_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "run_id",
            "task_id",
            "attempt_number",
            name="uq_task_attempts_run_task_attempt",
        ),
        Index("ix_task_attempts_run_task", "run_id", "task_id"),
    )

    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    attempt_number: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(255))
    error_message: Mapped[str | None] = mapped_column(String(1024))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_token: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DispatchOutboxRecord(Base):
    __tablename__ = "dispatch_outbox"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "workflow_id", "task_id"],
            ["task_runs.run_id", "task_runs.workflow_id", "task_runs.task_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "run_id",
            "task_id",
            "attempt_number",
            name="uq_dispatch_outbox_run_task_attempt",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_number: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    last_error: Mapped[str | None] = mapped_column(Text)


Index(
    "ix_task_runs_status_next_retry_at",
    TaskRunRecord.status,
    TaskRunRecord.next_retry_at,
)

Index(
    "ix_task_attempts_status_lease_expires_at",
    TaskAttemptRecord.status,
    TaskAttemptRecord.lease_expires_at,
)

Index(
    "ix_dispatch_outbox_unpublished_created",
    DispatchOutboxRecord.created_at,
    DispatchOutboxRecord.id,
    postgresql_where=DispatchOutboxRecord.published_at.is_(None),
)
