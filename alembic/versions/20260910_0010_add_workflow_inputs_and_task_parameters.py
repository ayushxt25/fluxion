"""Add workflow inputs and task parameter mappings.

Revision ID: 20260910_0010
Revises: 20260909_0009
Create Date: 2026-09-10 00:10:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260910_0010"
down_revision: str | None = "20260909_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "workflow_runs",
        sa.Column("input", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "workflow_runs",
        sa.Column(
            "input_present",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "task_definitions",
        sa.Column(
            "parameters",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("workflow_runs", "input_present", server_default=None)
    op.alter_column("task_definitions", "parameters", server_default=None)


def downgrade() -> None:
    op.drop_column("task_definitions", "parameters")
    op.drop_column("workflow_runs", "input_present")
    op.drop_column("workflow_runs", "input")
