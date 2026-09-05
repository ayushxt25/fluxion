"""add task run idempotency keys

Revision ID: 20260903_0003
Revises: 20260903_0002
Create Date: 2026-09-03 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260903_0003"
down_revision: str | None = "20260903_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "task_runs",
        sa.Column("idempotency_key", sa.String(length=768), nullable=True),
    )
    op.execute(
        "UPDATE task_runs SET idempotency_key = run_id || ':' || task_id "
        "WHERE idempotency_key IS NULL"
    )
    op.alter_column("task_runs", "idempotency_key", nullable=False)
    op.create_unique_constraint(
        "uq_task_runs_idempotency_key",
        "task_runs",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_task_runs_idempotency_key",
        "task_runs",
        type_="unique",
    )
    op.drop_column("task_runs", "idempotency_key")
