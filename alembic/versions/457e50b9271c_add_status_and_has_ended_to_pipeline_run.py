"""add status and has_ended to pipeline_run

Revision ID: 457e50b9271c
Revises: b86b67add848
Create Date: 2026-02-21 11:22:56.149127

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '457e50b9271c'
down_revision: Union[str, Sequence[str], None] = 'b86b67add848'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_STATUS_PRIORITY = {
    "SYSTEM_ERROR": 1,
    "FAILED": 2,
    "INVALID": 3,
    "CANCELLING": 4,
    "CANCELLED": 5,
    "RUNNING": 6,
    "PENDING": 7,
    "WAITING_FOR_UPSTREAM": 8,
    "QUEUED": 9,
    "UNINITIALIZED": 10,
    "SKIPPED": 11,
    "SUCCEEDED": 12,
}

_PRIORITY_TO_STATUS = {v: k for k, v in _STATUS_PRIORITY.items()}

_ENDED_STATUSES = {"INVALID", "SUCCEEDED", "FAILED", "SYSTEM_ERROR", "CANCELLED", "SKIPPED"}


def upgrade() -> None:
    with op.batch_alter_table('pipeline_run', schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            'status',
            sa.Enum(
                'INVALID', 'UNINITIALIZED', 'QUEUED', 'WAITING_FOR_UPSTREAM',
                'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SYSTEM_ERROR',
                'CANCELLING', 'CANCELLED', 'SKIPPED',
                name='containerexecutionstatus',
            ),
            nullable=True,
        ))
        batch_op.add_column(sa.Column(
            'has_ended', sa.Boolean(), nullable=False, server_default=sa.text('0'),
        ))
        batch_op.create_index(
            batch_op.f('ix_pipeline_run_status'), ['status'], unique=False,
        )

    op.create_index(
        'ix_pipeline_run_status_created_at_desc',
        'pipeline_run',
        [sa.text('status'), sa.text('created_at DESC')],
        unique=False,
    )

    _backfill_status()


def _backfill_status() -> None:
    """Compute status and has_ended for all existing pipeline runs."""
    conn = op.get_bind()

    pipeline_run = sa.table(
        'pipeline_run',
        sa.column('id', sa.String),
        sa.column('root_execution_id', sa.String),
        sa.column('status', sa.String),
        sa.column('has_ended', sa.Boolean),
    )
    execution_ancestor = sa.table(
        'execution_ancestor',
        sa.column('ancestor_execution_id', sa.String),
        sa.column('execution_id', sa.String),
    )
    execution_node = sa.table(
        'execution_node',
        sa.column('id', sa.String),
        sa.column('container_execution_status', sa.String),
    )

    rows = conn.execute(
        sa.select(pipeline_run.c.id, pipeline_run.c.root_execution_id)
        .where(pipeline_run.c.status == None)
    ).fetchall()

    for run_id, root_execution_id in rows:
        status_counts = conn.execute(
            sa.select(
                execution_node.c.container_execution_status,
                sa.func.count().label('cnt'),
            )
            .select_from(execution_node)
            .join(
                execution_ancestor,
                execution_ancestor.c.execution_id == execution_node.c.id,
            )
            .where(execution_ancestor.c.ancestor_execution_id == root_execution_id)
            .where(execution_node.c.container_execution_status != None)
            .group_by(execution_node.c.container_execution_status)
        ).fetchall()

        if not status_counts:
            continue

        best_priority = 99
        total = 0
        ended = 0
        for status_val, count in status_counts:
            priority = _STATUS_PRIORITY.get(status_val, 99)
            if priority < best_priority:
                best_priority = priority
            total += count
            if status_val in _ENDED_STATUSES:
                ended += count

        computed_status = _PRIORITY_TO_STATUS.get(best_priority)
        computed_has_ended = total > 0 and ended == total

        conn.execute(
            pipeline_run.update()
            .where(pipeline_run.c.id == run_id)
            .values(status=computed_status, has_ended=computed_has_ended)
        )


def downgrade() -> None:
    op.drop_index('ix_pipeline_run_status_created_at_desc', table_name='pipeline_run')
    with op.batch_alter_table('pipeline_run', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_pipeline_run_status'))
        batch_op.drop_column('has_ended')
        batch_op.drop_column('status')
