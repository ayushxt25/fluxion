"""Add scheduled-run provenance."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260928_0019"
down_revision: str | None = "20260928_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("workflow_runs", sa.Column("schedule_id", sa.String(36)))
    op.add_column(
        "workflow_runs", sa.Column("scheduled_for", sa.DateTime(timezone=True))
    )


def downgrade() -> None:
    op.drop_column("workflow_runs", "scheduled_for")
    op.drop_column("workflow_runs", "schedule_id")
