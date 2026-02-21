import logging
import os
from pathlib import Path

import sqlalchemy

from . import backend_types_sql as bts

_logger = logging.getLogger(__name__)

_ALEMBIC_INI_PATH = str(Path(__file__).resolve().parent.parent / "alembic.ini")


def create_db_engine_and_migrate_db(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    db_engine = create_db_engine(database_uri=database_uri, **kwargs)
    run_migrations(db_engine)
    _create_unmanaged_tables(db_engine)
    return db_engine


def initialize_and_migrate_db(db_engine: sqlalchemy.Engine):
    run_migrations(db_engine)
    _create_unmanaged_tables(db_engine)


def _create_unmanaged_tables(db_engine: sqlalchemy.Engine) -> None:
    """Create tables not yet tracked by Alembic migrations (e.g. ComponentLibraryRow)."""
    bts._TableBase.metadata.create_all(db_engine, checkfirst=True)


def run_migrations(db_engine: sqlalchemy.Engine) -> None:
    """Run all pending Alembic migrations against the given engine."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    alembic_cfg = Config(_ALEMBIC_INI_PATH)
    script = ScriptDirectory.from_config(alembic_cfg)
    head_rev = script.get_current_head()

    _logger.info("Running database migrations")

    with db_engine.connect() as conn:
        has_table = conn.dialect.has_table(conn, "alembic_version")
        if has_table:
            result = conn.execute(sqlalchemy.text("SELECT version_num FROM alembic_version"))
            current_rev = result.scalar()
        else:
            current_rev = None

    if current_rev == head_rev:
        _logger.info("Database already at head revision")
        return

    _logger.info(f"Migrating from {current_rev} to {head_rev}")
    from alembic import command

    alembic_cfg.attributes["connection"] = db_engine
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
        connect_args = create_engine_kwargs.setdefault("connect_args", {})
        connect_args["check_same_thread"] = False
        connect_args.setdefault("timeout", 10)

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

    if database_uri.startswith("sqlite:///"):
        with db_engine.connect() as conn:
            conn.execute(sqlalchemy.text("PRAGMA journal_mode=WAL"))
            conn.commit()

    return db_engine
