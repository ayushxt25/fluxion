"""Add compatibility revision columns for immutable workflow revisions."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260924_0015"
down_revision: str | None = "20260918_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for table in (
        "workflow_definitions",
        "task_definitions",
        "task_dependencies",
        "workflow_runs",
    ):
        column = "revision" if table == "workflow_definitions" else "workflow_revision"
        op.add_column(table, sa.Column(column, sa.Integer(), nullable=True))
        op.execute(sa.text(f"UPDATE {table} SET {column} = 1 WHERE {column} IS NULL"))
        op.alter_column(table, column, nullable=False)
        op.create_check_constraint(
            f"ck_{table}_{column}_positive", table, f"{column} >= 1"
        )
    op.create_index(
        "ix_workflow_definitions_id_revision",
        "workflow_definitions",
        ["id", "revision"],
    )
    op.create_index(
        "ix_workflow_runs_id_revision",
        "workflow_runs",
        ["workflow_id", "workflow_revision"],
    )


def downgrade() -> None:
    op.drop_index("ix_workflow_runs_id_revision", table_name="workflow_runs")
    op.drop_index(
        "ix_workflow_definitions_id_revision", table_name="workflow_definitions"
    )
    for table in (
        "workflow_runs",
        "task_dependencies",
        "task_definitions",
        "workflow_definitions",
    ):
        column = "revision" if table == "workflow_definitions" else "workflow_revision"
        op.drop_constraint(f"ck_{table}_{column}_positive", table, type_="check")
        op.drop_column(table, column)
