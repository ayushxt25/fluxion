from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
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
    )

    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)

    workflow_run: Mapped[WorkflowRunRecord] = relationship(
        back_populates="task_runs",
        foreign_keys=[run_id, workflow_id],
    )
