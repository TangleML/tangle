"""Alembic environment for the Cloud Pipelines backend database.

This module is executed by Alembic for every migration command. It is used in
two ways:

* From the command line (``alembic upgrade head``), configured by
  ``alembic.ini`` in the repository root. The database URL comes from the
  ``DATABASE_URI``/``DATABASE_URL`` environment variables.
* Programmatically from :func:`cloud_pipelines_backend.database_ops.upgrade_db_schema`,
  which passes an already-open :class:`sqlalchemy.Connection` through
  ``config.attributes``. Passing a live connection rather than a URL is
  required for in-memory SQLite (``sqlite://``), where opening a second
  connection would produce a second, empty database.
"""

import logging.config
import os

import sqlalchemy
from alembic import context

from cloud_pipelines_backend import backend_types_sql as bts

# Imported for its side effect: `component_library_api_server` declares four
# more tables ("component", "component_library", "published_component" and
# "user") on the same `_TableBase`, so they are only part of
# `_TableBase.metadata` once that module has been imported. Without this,
# `--autogenerate` would think those tables were dropped.
#
# Any future module that declares tables has to be imported here too.
from cloud_pipelines_backend import component_library_api_server  # noqa: F401

config = context.config

# The application configures its own logging, so the ini file's logging config
# is only applied when Alembic is driven from the command line. Programmatic
# callers pass `configure_logging=False` to keep their own setup.
if config.config_file_name is not None and config.attributes.get(
    "configure_logging", True
):
    logging.config.fileConfig(config.config_file_name, disable_existing_loggers=False)

# The schema that `--autogenerate` compares the database against.
target_metadata = bts._TableBase.metadata


def _get_database_url() -> str:
    """Return the database URL for command-line invocations.

    Mirrors the lookup done by `api_server_main` so that
    `alembic upgrade head` targets the same database as the running server.
    """
    url = (
        config.get_main_option("sqlalchemy.url")
        or os.environ.get("DATABASE_URI")
        or os.environ.get("DATABASE_URL")
    )
    if not url:
        raise ValueError(
            "No database URL configured. Set the DATABASE_URI environment"
            " variable, or pass `-x db_uri=...` to the alembic command."
        )
    return url


def _render_item(type_, obj, autogen_context) -> str | bool:
    """Render `bts.UtcDateTime` as the plain SQLAlchemy type it compiles to.

    `UtcDateTime` is a `TypeDecorator` over `DateTime(timezone=True)`; it only
    changes how values are converted in Python, and the emitted DDL is
    identical. Rendering it as `sa.DateTime(timezone=True)` keeps revision
    scripts free of imports from the application models, so that already-applied
    revisions keep working when those models are later refactored or renamed.
    """
    if type_ == "type" and isinstance(obj, bts.UtcDateTime):
        return "sa.DateTime(timezone=True)"
    # False means "fall back to Alembic's default rendering".
    return False


_CONTEXT_OPTS = dict(
    target_metadata=target_metadata,
    render_item=_render_item,
    # Detect column type changes (e.g. VARCHAR -> TEXT) during autogenerate.
    compare_type=True,
    # SQLite cannot ALTER most things in place, so Alembic recreates the table
    # instead. Ignored by dialects that support a real ALTER TABLE.
    render_as_batch=True,
)


def _run_migrations(connection: sqlalchemy.Connection) -> None:
    context.configure(connection=connection, **_CONTEXT_OPTS)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    """Emit the migration SQL to stdout instead of running it (``--sql``)."""
    context.configure(
        url=_get_database_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_CONTEXT_OPTS,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run the migrations against a live database."""
    connection = config.attributes.get("connection")
    if connection is not None:
        # Caller-supplied connection: the caller owns the transaction and is
        # responsible for committing it.
        _run_migrations(connection)
        return

    db_engine = sqlalchemy.create_engine(
        _get_database_url(),
        # A migration run is short-lived, so there is nothing to pool.
        poolclass=sqlalchemy.pool.NullPool,
    )
    try:
        with db_engine.begin() as connection:
            _run_migrations(connection)
    finally:
        db_engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
