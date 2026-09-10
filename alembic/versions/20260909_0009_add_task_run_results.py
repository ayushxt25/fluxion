"""Add durable task run results.

Revision ID: 20260909_0009
Revises: 20260906_0008
Create Date: 2026-09-09 00:09:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260909_0009"
down_revision: str | None = "20260906_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "task_runs",
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "task_runs",
        sa.Column(
            "result_present",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.alter_column("task_runs", "result_present", server_default=None)


def downgrade() -> None:
    op.drop_column("task_runs", "result_present")
    op.drop_column("task_runs", "result")
