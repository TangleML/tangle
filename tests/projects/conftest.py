"""Fixtures for the project tests: engines, a session, and one client per kind of caller."""

import collections.abc
import pathlib
import sys

import fastapi.testclient
import pytest
import sqlalchemy
from sqlalchemy import orm

# `--import-mode=importlib` keeps test directories off `sys.path`, so a test module cannot import
# a sibling. The helpers these tests share live beside them and are made importable here, which
# runs before any test module in this directory is collected.
sys.path.insert(0, str(pathlib.Path(__file__).parent))
pytest.register_assert_rewrite("project_test_helpers")

import project_test_helpers as helpers  # noqa: E402


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    return helpers.make_engine(enforce_foreign_keys=False)


@pytest.fixture()
def fk_db_engine() -> sqlalchemy.Engine:
    """An engine with SQLite's foreign-key enforcement switched on."""
    return helpers.make_engine(enforce_foreign_keys=True)


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Iterator[orm.Session]:
    with orm.Session(bind=db_engine) as sess:
        yield sess


@pytest.fixture()
def client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(helpers.build_app(db_engine=db_engine))


@pytest.fixture()
def fk_client(fk_db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The same caller on the engine that actually enforces foreign keys."""
    return fastapi.testclient.TestClient(helpers.build_app(db_engine=fk_db_engine))


@pytest.fixture()
def other_user_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """A second caller. Reads everything the first can -- there is no per-user scope here."""
    return fastapi.testclient.TestClient(
        helpers.build_app(db_engine=db_engine, user_name=helpers.OTHER_USER)
    )


@pytest.fixture()
def admin_client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The only caller that may write a workspace. Ordinary project writes work too."""
    return fastapi.testclient.TestClient(
        helpers.build_app(
            db_engine=db_engine, user_name=helpers.ADMIN_USER, is_admin=True
        )
    )


@pytest.fixture()
def read_only_admin_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """An admin on a read-only deployment: 503 on a workspace write, not 201."""
    return fastapi.testclient.TestClient(
        helpers.build_app(
            db_engine=db_engine,
            user_name=helpers.ADMIN_USER,
            is_admin=True,
            is_read_only=True,
        )
    )


@pytest.fixture()
def read_only_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        helpers.build_app(db_engine=db_engine, is_read_only=True)
    )


@pytest.fixture()
def no_write_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        helpers.build_app(db_engine=db_engine, can_write=False)
    )
