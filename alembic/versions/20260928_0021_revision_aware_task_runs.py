"""Make durable task runs reference immutable revision tasks."""

import sqlalchemy as sa
from alembic import op

revision = "20260928_0021"
down_revision = "20260928_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("task_runs", sa.Column("workflow_revision", sa.Integer()))
    op.execute(
        "UPDATE task_runs AS tr SET workflow_revision = wr.workflow_revision "
        "FROM workflow_runs AS wr WHERE tr.run_id = wr.run_id "
        "AND tr.workflow_id = wr.workflow_id"
    )
    op.alter_column("task_runs", "workflow_revision", nullable=False)
    op.create_check_constraint(
        "ck_task_runs_workflow_revision", "task_runs", "workflow_revision >= 1"
    )
    op.create_unique_constraint(
        "uq_workflow_runs_run_workflow_revision",
        "workflow_runs",
        ["run_id", "workflow_id", "workflow_revision"],
    )
    op.drop_constraint(
        "task_runs_run_id_workflow_id_fkey", "task_runs", type_="foreignkey"
    )
    op.drop_constraint(
        "task_runs_workflow_id_task_id_fkey", "task_runs", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_task_runs_parent_revision",
        "task_runs",
        "workflow_runs",
        ["run_id", "workflow_id", "workflow_revision"],
        ["run_id", "workflow_id", "workflow_revision"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_task_runs_revision_task",
        "task_runs",
        "workflow_revision_tasks",
        ["workflow_id", "workflow_revision", "task_id"],
        ["workflow_id", "revision", "task_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    unsafe = op.get_bind().execute(
        sa.text("SELECT 1 FROM task_runs WHERE workflow_revision > 1 LIMIT 1")
    ).first()
    if unsafe:
        raise RuntimeError(
            "Cannot downgrade revision-aware task runs with revision > 1 data."
        )
    op.drop_constraint("fk_task_runs_revision_task", "task_runs", type_="foreignkey")
    op.drop_constraint("fk_task_runs_parent_revision", "task_runs", type_="foreignkey")
    op.drop_constraint(
        "uq_workflow_runs_run_workflow_revision", "workflow_runs", type_="unique"
    )
    op.drop_constraint("ck_task_runs_workflow_revision", "task_runs", type_="check")
    op.create_foreign_key(
        "task_runs_run_id_workflow_id_fkey",
        "task_runs",
        "workflow_runs",
        ["run_id", "workflow_id"],
        ["run_id", "workflow_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "task_runs_workflow_id_task_id_fkey",
        "task_runs",
        "task_definitions",
        ["workflow_id", "task_id"],
        ["workflow_id", "task_id"],
        ondelete="RESTRICT",
    )
    op.drop_column("task_runs", "workflow_revision")
