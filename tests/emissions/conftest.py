"""Shared fixtures for the emissions tests.

Importing emissions.db_models registers the emission tables on _TableBase.metadata
(the mapped classes register themselves when the module is imported), so create_all
below builds them alongside the core Tangle tables.
"""

import collections.abc

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.emissions import db_models

# Registering the tables is what makes them part of the schema built below; the reference
# also stops linters from flagging the import as unused.
db_models.register_db_tables()


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    with orm.Session(bind=db_engine) as sess:
        yield sess


@pytest.fixture()
def session_factory(
    db_engine: sqlalchemy.Engine,
) -> orm.sessionmaker:
    """A session factory on the test engine, for code that opens its own sessions."""
    return orm.sessionmaker(bind=db_engine)
