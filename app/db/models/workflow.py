from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, ForeignKeyConstraint, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class WorkflowDefinitionRecord(Base):
    __tablename__ = "workflow_definitions"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
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
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)

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
    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    depends_on_task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
