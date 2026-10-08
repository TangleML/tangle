"""Shared fixtures for the quota group tests."""

import collections.abc

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.quota import (
    db_models,  # noqa: F401  # registers the tables on the metadata
)


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")

    # SQLite ignores foreign keys unless asked, so ON DELETE CASCADE is a no-op
    # without this. MySQL/InnoDB enforces them always.
    @sqlalchemy.event.listens_for(engine, "connect")
    def _enable_sqlite_foreign_keys(
        dbapi_connection: object, connection_record: object
    ) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    # autoflush=False mirrors production: every session in this service is built that way
    # (orchestrator_main.py:92, app.py:287 and four more). The default is True, and the
    # difference is not cosmetic -- a pending status change is invisible to a subsequent query
    # under autoflush=False, so code that reads its own uncommitted writes passes here and
    # silently does nothing in production. That exact bug shipped to review once already.
    with orm.Session(bind=db_engine, autoflush=False) as sess:
        yield sess
