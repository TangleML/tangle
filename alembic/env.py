import os
from logging.config import fileConfig

import sqlalchemy as sa
from sqlalchemy import engine_from_config, pool

from alembic import context

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.database_ops import create_db_engine

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = bts._TableBase.metadata

DEFAULT_DATABASE_URI = "sqlite:///db.sqlite"


def _render_item(type_: str, obj: object, autogen_context) -> str | bool:
    """Render UtcDateTime as sa.DateTime(timezone=True) in migrations."""
    if type_ == "type" and isinstance(obj, bts.UtcDateTime):
        return "sa.DateTime(timezone=True)"
    return False


def _get_database_url() -> str:
    """Resolve database URL from environment, falling back to alembic.ini."""
    url = (
        os.environ.get("DATABASE_URI")
        or os.environ.get("DATABASE_URL")
        or config.get_main_option("sqlalchemy.url")
        or DEFAULT_DATABASE_URI
    )
    if url.startswith("mysql://"):
        try:
            import MySQLdb  # noqa: F401
        except ImportError:
            url = url.replace("mysql://", "mysql+pymysql://")
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    Emits SQL to stdout rather than executing it.
    """
    url = _get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
        render_item=_render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode with a live database connection."""
    connectable = config.attributes.get("connection")

    if connectable is None:
        url = _get_database_url()
        connectable = create_db_engine(database_uri=url)

    def _run_with_connection(connection):
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
            compare_type=True,
            render_item=_render_item,
        )
        with context.begin_transaction():
            context.run_migrations()

    try:
        connectable.connect
        is_engine = hasattr(connectable, "pool")
    except AttributeError:
        is_engine = False

    if is_engine:
        with connectable.connect() as connection:
            _run_with_connection(connection)
    else:
        _run_with_connection(connectable)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
