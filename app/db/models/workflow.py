from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class WorkflowDefinitionRecord(Base):
    __tablename__ = "workflow_definitions"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    tasks: Mapped[list["TaskDefinitionRecord"]] = relationship(
        back_populates="workflow",
        cascade="save-update, merge",
        lazy="selectin",
    )


class TaskDefinitionRecord(Base):
    __tablename__ = "task_definitions"

    workflow_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    workflow_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retry_max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    retry_initial_backoff_seconds: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
    )
    retry_backoff_multiplier: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=2.0,
    )
    retry_max_backoff_seconds: Mapped[float | None] = mapped_column(Float)
    parameters: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    workflow: Mapped[WorkflowDefinitionRecord] = relationship(back_populates="tasks")


class TaskDependencyRecord(Base):
    __tablename__ = "task_dependencies"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_id", "task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["workflow_id", "depends_on_task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
    )

    workflow_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    depends_on_task_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class WorkflowRevisionRecord(Base):
    __tablename__ = "workflow_revisions"
    __table_args__ = (UniqueConstraint("workflow_id", "revision"),)

    workflow_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class WorkflowRevisionTaskRecord(Base):
    __tablename__ = "workflow_revision_tasks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_id", "revision"],
            ["workflow_revisions.workflow_id", "workflow_revisions.revision"],
            ondelete="RESTRICT",
        ),
    )
    workflow_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255))
    retry_max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_initial_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    retry_backoff_multiplier: Mapped[float] = mapped_column(Float, nullable=False)
    retry_max_backoff_seconds: Mapped[float | None] = mapped_column(Float)
    parameters: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class WorkflowRevisionDependencyRecord(Base):
    __tablename__ = "workflow_revision_dependencies"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_id", "revision", "task_id"],
            [
                "workflow_revision_tasks.workflow_id",
                "workflow_revision_tasks.revision",
                "workflow_revision_tasks.task_id",
            ],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["workflow_id", "revision", "depends_on_task_id"],
            [
                "workflow_revision_tasks.workflow_id",
                "workflow_revision_tasks.revision",
                "workflow_revision_tasks.task_id",
            ],
            ondelete="RESTRICT",
        ),
    )
    workflow_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    depends_on_task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
