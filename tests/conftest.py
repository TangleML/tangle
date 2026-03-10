"""Shared pytest fixtures for all tests in this directory.

pytest automatically discovers files named ``conftest.py`` and makes any
fixtures defined here available to every test module in the same directory
(and subdirectories) -- no import required.  Tests request fixtures simply
by naming them as function parameters.
"""

import pytest
from sqlalchemy import orm
from sqlalchemy import sql

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops


@pytest.fixture()
def mysql_varchar_limit_session_factory() -> orm.sessionmaker:
    """SQLite engine with TRIGGER-based VARCHAR length enforcement.

    Mimics MySQL's DataError 1406 by rejecting INSERT/UPDATE on
    pipeline_run_annotation when key or value exceeds the VARCHAR
    limit from type_annotation_map.
    """
    max_len = bts._TableBase.type_annotation_map[str].length
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    with db_engine.connect() as conn:
        conn.execute(sql.text(f"""
            CREATE TRIGGER enforce_annotation_value_length
            BEFORE INSERT ON pipeline_run_annotation
            FOR EACH ROW
            WHEN length(NEW.value) > {max_len}
            BEGIN
                SELECT RAISE(ABORT, 'Data too long for column value');
            END
        """))
        conn.execute(sql.text(f"""
            CREATE TRIGGER enforce_annotation_key_length
            BEFORE INSERT ON pipeline_run_annotation
            FOR EACH ROW
            WHEN length(NEW.key) > {max_len}
            BEGIN
                SELECT RAISE(ABORT, 'Data too long for column key');
            END
        """))
        conn.commit()
    return orm.sessionmaker(db_engine)
