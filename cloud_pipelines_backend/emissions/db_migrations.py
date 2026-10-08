"""Schema changes to the emission tables that `create_all` cannot make.

`create_all` skips a table that already exists, indexes included, so a changed
`__table_args__` is a no-op on staging and production. Anything that has to alter a live
emission table goes here and is called from `app.py`.
"""

import logging

import sqlalchemy
from alembic import migration as alembic_migration
from alembic import operations as alembic_operations

from cloud_pipelines_backend.emissions import db_models

logger = logging.getLogger(__name__)


def _live_indexes(*, db_engine: sqlalchemy.Engine, table_name: str) -> set[str]:
    """Returns the names of the indexes the database currently has on a table."""
    return {
        index["name"] for index in sqlalchemy.inspect(db_engine).get_indexes(table_name)
    }


def migrate(*, db_engine: sqlalchemy.Engine) -> None:
    """Replace the 2-column dedupe key with the 3-column one that adds the node's status.

    Idempotent, and cheap once settled: a migrated database costs one inspection and issues no
    DDL, which is what every pod pays at startup. The exception is the first run after a deploy,
    which does build an index over whatever rows already exist -- measured at 319 rows and 0 MB
    on production before this feature went live, so it is instant and no algorithm MySQL might
    pick for it can block a write for a noticeable time. The new index is created first, because the
    old key is the stricter of the two -- no existing row can violate the new one, so the
    overlap in which both are installed rejects nothing.

    Args:
        db_engine: the engine to migrate.
    """
    table = db_models.EmissionEvent.__table__
    live = _live_indexes(db_engine=db_engine, table_name=table.name)

    new_ix = db_models.EmissionEvent.IX_NODE_TYPE_STATUS_NEW
    if new_ix not in live:
        logger.info(f"Emission Event Migration: new index `{new_ix}` not found in DB")

        # Read off the table, never constructed: the Index constructor registers itself on
        # the table definition and would leave a duplicate behind (sqlalchemy#12965).
        index = next((i for i in table.indexes if i.name == new_ix), None)
        if index is None:
            raise RuntimeError(
                f"{table.name} does not declare the index {new_ix!r};"
                " EmissionEvent.__table_args__ and IX_NODE_TYPE_STATUS_NEW disagree"
            )
        try:
            # checkfirst re-checks in the DB, which narrows the window between the inspection
            # above and the CREATE but does not close it.
            index.create(db_engine, checkfirst=True)
        except sqlalchemy.exc.DatabaseError:
            # As with the drop below, the postcondition decides rather than an errno: losing
            # this race leaves exactly the index this call wanted.
            if new_ix not in _live_indexes(db_engine=db_engine, table_name=table.name):
                raise
            logger.info(
                f"Emission Event Migration: new index `{new_ix}` already"
                " created by another process"
            )
        else:
            logger.info(f"Emission Event Migration: created new index: {new_ix}")

    old_ix = db_models.EmissionEvent.IX_NODE_TYPE_STATUS_OLD
    if old_ix not in live:
        return
    with db_engine.connect() as conn:
        ctx = alembic_migration.MigrationContext.configure(conn)
        try:
            alembic_operations.Operations(ctx).drop_index(old_ix, table_name=table.name)
            conn.commit()
        except sqlalchemy.exc.DatabaseError:
            # `live` above is a snapshot, and this process is not the only one migrating: the
            # app and the orchestrator both do. Losing the race is success, so the postcondition
            # decides rather than an errno -- MySQL has no `DROP INDEX IF EXISTS` to ask with.
            conn.rollback()
            if old_ix in _live_indexes(db_engine=db_engine, table_name=table.name):
                raise
            logger.info(
                f"Emission Event Migration: old index `{old_ix}` already"
                " dropped by another process"
            )
            return
    logger.info(f"Emission Event Migration: dropped old index: {old_ix}")
