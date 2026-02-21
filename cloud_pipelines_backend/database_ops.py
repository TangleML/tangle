import logging
import os
from pathlib import Path

import sqlalchemy

_logger = logging.getLogger(__name__)

_ALEMBIC_INI_PATH = str(Path(__file__).resolve().parent.parent / "alembic.ini")


def create_db_engine_and_migrate_db(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    db_engine = create_db_engine(database_uri=database_uri, **kwargs)
    run_migrations(db_engine)
    return db_engine


def initialize_and_migrate_db(db_engine: sqlalchemy.Engine):
    run_migrations(db_engine)


def run_migrations(db_engine: sqlalchemy.Engine) -> None:
    """Run all pending Alembic migrations against the given engine."""
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config(_ALEMBIC_INI_PATH)
    alembic_cfg.attributes["connection"] = db_engine

    _logger.info("Running database migrations")
    with db_engine.connect() as connection:
        alembic_cfg.attributes["connection"] = connection
        command.upgrade(alembic_cfg, "head")
    _logger.info("Database migrations complete")


def create_db_engine(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    if database_uri.startswith("mysql://"):
        try:
            import MySQLdb  # noqa: F401
        except ImportError:
            database_uri = database_uri.replace("mysql://", "mysql+pymysql://")

    create_engine_kwargs = {}
    if database_uri == "sqlite://":
        create_engine_kwargs["poolclass"] = sqlalchemy.pool.StaticPool

    if database_uri.startswith("sqlite://"):
        # https://fastapi.tiangolo.com/tutorial/sql-databases/#create-an-engine
        create_engine_kwargs.setdefault("connect_args", {})["check_same_thread"] = False

    if create_engine_kwargs.get("poolclass") != sqlalchemy.pool.StaticPool:
        # Preventing the "MySQL server has gone away" error:
        # https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
        create_engine_kwargs["pool_recycle"] = 3600
        create_engine_kwargs["pool_pre_ping"] = True

    if kwargs:
        create_engine_kwargs.update(kwargs)

    db_engine = sqlalchemy.create_engine(
        url=database_uri,
        **create_engine_kwargs,
    )
    return db_engine
