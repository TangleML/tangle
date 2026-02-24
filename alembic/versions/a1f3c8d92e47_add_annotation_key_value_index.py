"""Add annotation key+value index for filter queries

Revision ID: a1f3c8d92e47
Revises: 457e50b9271c
Create Date: 2026-02-21
"""

from typing import Sequence, Union

from alembic import op

revision: str = "a1f3c8d92e47"
down_revision: Union[str, None] = "457e50b9271c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_pipeline_run_annotation_key_value",
        "pipeline_run_annotation",
        ["key", "value"],
        unique=False,
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pipeline_run_annotation_key_value",
        table_name="pipeline_run_annotation",
    )
