"""add task attempts and retry state

Revision ID: 20260903_0002
Revises: 20260901_0001
Create Date: 2026-09-03 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260903_0002"
down_revision: str | None = "20260901_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "task_definitions",
        sa.Column(
            "retry_max_attempts",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    op.add_column(
        "task_definitions",
        sa.Column(
            "retry_initial_backoff_seconds",
            sa.Float(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "task_definitions",
        sa.Column(
            "retry_backoff_multiplier",
            sa.Float(),
            nullable=False,
            server_default="2",
        ),
    )
    op.add_column(
        "task_definitions",
        sa.Column("retry_max_backoff_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "task_runs",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_task_runs_status_next_retry_at",
        "task_runs",
        ["status", "next_retry_at"],
    )
    op.create_unique_constraint(
        "uq_task_runs_run_workflow_task",
        "task_runs",
        ["run_id", "workflow_id", "task_id"],
    )
    op.create_table(
        "task_attempts",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_type", sa.String(length=255), nullable=True),
        sa.Column("error_message", sa.String(length=1024), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id", "workflow_id", "task_id"],
            ["task_runs.run_id", "task_runs.workflow_id", "task_runs.task_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", "task_id", "attempt_number"),
        sa.UniqueConstraint(
            "run_id",
            "task_id",
            "attempt_number",
            name="uq_task_attempts_run_task_attempt",
        ),
    )
    op.create_index(
        "ix_task_attempts_run_task",
        "task_attempts",
        ["run_id", "task_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_task_attempts_run_task", table_name="task_attempts")
    op.drop_table("task_attempts")
    op.drop_constraint(
        "uq_task_runs_run_workflow_task",
        "task_runs",
        type_="unique",
    )
    op.drop_index("ix_task_runs_status_next_retry_at", table_name="task_runs")
    op.drop_column("task_runs", "next_retry_at")
    op.drop_column("task_definitions", "retry_max_backoff_seconds")
    op.drop_column("task_definitions", "retry_backoff_multiplier")
    op.drop_column("task_definitions", "retry_initial_backoff_seconds")
    op.drop_column("task_definitions", "retry_max_attempts")
