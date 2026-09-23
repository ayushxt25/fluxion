"""Add terminal completion timestamp for retention."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260918_0014"
down_revision: str | None = "20260916_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "workflow_runs", sa.Column("completed_at", sa.DateTime(timezone=True))
    )
    op.create_index(
        "ix_workflow_runs_terminal_completed",
        "workflow_runs",
        ["status", "completed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_workflow_runs_terminal_completed", table_name="workflow_runs")
    op.drop_column("workflow_runs", "completed_at")
