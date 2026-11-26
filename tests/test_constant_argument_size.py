"""Inline constant arguments over the storage limit are rejected at submit time.

Constant arguments are stored in ``artifact_data.value``, a ``TEXT`` column that
holds at most 65,535 bytes on MySQL. A longer value used to fail the insert with
"Data too long for column 'value'" and surface as an HTTP 500 with no run created.
"""

import fastapi
import pytest
import sqlalchemy
from fastapi import testclient
from sqlalchemy import orm

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors

_LIMIT = api_server_sql.MAX_CONSTANT_ARGUMENT_SIZE_BYTES
_70_KB = 70 * 1024


def _container_component(input_name: str) -> structures.ComponentSpec:
    return structures.ComponentSpec(
        name="consumer",
        inputs=[structures.InputSpec(name=input_name)],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )


def _root_task(value: str) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=_container_component("sql_fixture")
        ),
        arguments={"sql_fixture": value},
    )


def _graph_task_with_child_constant(value: str) -> structures.TaskSpec:
    child_task = structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=_container_component("sql_fixture")
        ),
        arguments={"sql_fixture": value},
    )
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name="pipeline",
                implementation=structures.GraphImplementation(
                    graph=structures.GraphSpec(tasks={"load-fixture": child_task})
                ),
            )
        ),
    )


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return engine


def _count_pipeline_runs(db_engine: sqlalchemy.Engine) -> int:
    with orm.Session(db_engine) as session:
        return session.scalar(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(bts.PipelineRun)
        )


def _create_run(db_engine: sqlalchemy.Engine, root_task: structures.TaskSpec):
    with orm.Session(db_engine) as session:
        return api_server_sql.PipelineRunsApiService_Sql().create(
            session, root_task=root_task
        )


def test_submitting_a_70_kb_constant_returns_a_client_error_and_creates_no_run(
    db_engine: sqlalchemy.Engine,
) -> None:
    app = fastapi.FastAPI()
    api_router.setup_routes(
        app=app,
        db_engine=db_engine,
        user_details_getter=lambda: api_router.UserDetails(
            name="user1",
            permissions=api_router.Permissions(read=True, write=True, admin=False),
        ),
    )
    with testclient.TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/api/pipeline_runs/",
            json={"root_task": _root_task("x" * _70_KB).to_json_dict()},
        )

    assert response.status_code == 422, response.text
    message = response.json()["detail"]
    assert "'sql_fixture'" in message
    assert f"{_70_KB} bytes" in message
    assert f"{_LIMIT}-byte limit" in message
    assert "artifact/file" in message
    assert _count_pipeline_runs(db_engine) == 0


def test_a_too_large_child_task_constant_names_the_task(
    db_engine: sqlalchemy.Engine,
) -> None:
    with pytest.raises(errors.ConstantArgumentTooLargeError) as exc_info:
        _create_run(db_engine, _graph_task_with_child_constant("x" * _70_KB))

    assert "input 'sql_fixture' of task 'load-fixture'" in str(exc_info.value)
    assert _count_pipeline_runs(db_engine) == 0


@pytest.mark.parametrize(
    ("value", "is_accepted"),
    [
        pytest.param("x" * _LIMIT, True, id="ascii-at-limit"),
        pytest.param("x" * (_LIMIT + 1), False, id="ascii-one-byte-over"),
        # MySQL limits TEXT by bytes: 40,000 two-byte characters are 80,000 bytes.
        pytest.param("é" * 40_000, False, id="multibyte-over-in-bytes"),
    ],
)
def test_the_limit_is_counted_in_utf8_bytes(
    db_engine: sqlalchemy.Engine, value: str, is_accepted: bool
) -> None:
    if is_accepted:
        _create_run(db_engine, _root_task(value))
        assert _count_pipeline_runs(db_engine) == 1
    else:
        with pytest.raises(errors.ConstantArgumentTooLargeError):
            _create_run(db_engine, _root_task(value))
        assert _count_pipeline_runs(db_engine) == 0
