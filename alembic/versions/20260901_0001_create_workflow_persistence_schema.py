"""create workflow persistence schema

Revision ID: 20260901_0001
Revises:
Create Date: 2026-09-01
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260901_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_definitions",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "task_definitions",
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflow_definitions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("workflow_id", "task_id"),
    )
    op.create_table(
        "workflow_runs",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflow_definitions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id"),
        sa.UniqueConstraint(
            "run_id",
            "workflow_id",
            name="uq_workflow_runs_run_workflow",
        ),
    )
    op.create_table(
        "task_dependencies",
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("depends_on_task_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_id", "depends_on_task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id", "task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("workflow_id", "task_id", "depends_on_task_id"),
    )
    op.create_table(
        "task_runs",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id", "workflow_id"],
            ["workflow_runs.run_id", "workflow_runs.workflow_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id", "task_id"],
            ["task_definitions.workflow_id", "task_definitions.task_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", "task_id"),
    )


def downgrade() -> None:
    op.drop_table("task_runs")
    op.drop_table("task_dependencies")
    op.drop_table("workflow_runs")
    op.drop_table("task_definitions")
    op.drop_table("workflow_definitions")
