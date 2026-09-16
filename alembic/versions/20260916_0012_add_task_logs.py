"""Add attempt-scoped task logs.

Revision ID: 20260916_0012
Revises: 20260912_0011
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260916_0012"
down_revision: str | None = "20260912_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "task_logs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(255), nullable=False),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("task_id", sa.String(255), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("level", sa.String(16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("fields", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "task_id", "attempt_number"],
            [
                "task_attempts.run_id",
                "task_attempts.task_id",
                "task_attempts.attempt_number",
            ],
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "uq_task_logs_attempt_sequence",
        "task_logs",
        ["run_id", "task_id", "attempt_number", "sequence_number"],
        unique=True,
    )
    op.create_index("ix_task_logs_run_id_id", "task_logs", ["run_id", "id"])


def downgrade() -> None:
    op.drop_index("ix_task_logs_run_id_id", table_name="task_logs")
    op.drop_index("uq_task_logs_attempt_sequence", table_name="task_logs")
    op.drop_table("task_logs")
