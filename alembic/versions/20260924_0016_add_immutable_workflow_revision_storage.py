"""Add immutable parallel workflow revision storage."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260924_0016"
down_revision: str | None = "20260924_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_revisions",
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("revision >= 1"),
        sa.PrimaryKeyConstraint("workflow_id", "revision"),
    )
    op.create_table(
        "workflow_revision_tasks",
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(255), nullable=False),
        sa.Column("name", sa.String(255)),
        sa.Column("retry_max_attempts", sa.Integer(), nullable=False),
        sa.Column("retry_initial_backoff_seconds", sa.Float(), nullable=False),
        sa.Column("retry_backoff_multiplier", sa.Float(), nullable=False),
        sa.Column("retry_max_backoff_seconds", sa.Float()),
        sa.Column("parameters", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_id", "revision"],
            ["workflow_revisions.workflow_id", "workflow_revisions.revision"],
        ),
        sa.PrimaryKeyConstraint("workflow_id", "revision", "task_id"),
    )
    op.create_table(
        "workflow_revision_dependencies",
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(255), nullable=False),
        sa.Column("depends_on_task_id", sa.String(255), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_id", "revision", "task_id"],
            [
                "workflow_revision_tasks.workflow_id",
                "workflow_revision_tasks.revision",
                "workflow_revision_tasks.task_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id", "revision", "depends_on_task_id"],
            [
                "workflow_revision_tasks.workflow_id",
                "workflow_revision_tasks.revision",
                "workflow_revision_tasks.task_id",
            ],
        ),
        sa.PrimaryKeyConstraint(
            "workflow_id", "revision", "task_id", "depends_on_task_id"
        ),
    )
    op.execute(
        "INSERT INTO workflow_revisions (workflow_id, revision, name, created_at) "
        "SELECT id, 1, name, created_at FROM workflow_definitions"
    )
    op.execute(
        "INSERT INTO workflow_revision_tasks SELECT workflow_id, 1, task_id, name, "
        "retry_max_attempts, retry_initial_backoff_seconds, retry_backoff_multiplier, "
        "retry_max_backoff_seconds, parameters FROM task_definitions"
    )
    op.execute(
        "INSERT INTO workflow_revision_dependencies SELECT workflow_id, 1, task_id, "
        "depends_on_task_id FROM task_dependencies"
    )


def downgrade() -> None:
    count = (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workflow_revisions WHERE revision > 1"))
        .scalar_one()
    )
    if count:
        raise RuntimeError(
            "Cannot downgrade workflow revisions while revision > 1 exists."
        )
    op.drop_table("workflow_revision_dependencies")
    op.drop_table("workflow_revision_tasks")
    op.drop_table("workflow_revisions")
