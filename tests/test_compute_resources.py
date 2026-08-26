import fastapi
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import orm

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import compute_resources
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors

_GPU_ANNOTATION_KEY = "cloud-pipelines.net/launchers/generic/resources.accelerators"


def _make_container_task(
    *, gpu_id: str | None = None, name: str = "test-task"
) -> structures.TaskSpec:
    annotations = {_GPU_ANNOTATION_KEY: f'{{"{gpu_id}": "1"}}'} if gpu_id else None
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name=name,
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image="test-image:latest")
                ),
            )
        ),
        annotations=annotations,
    )


def _make_graph_task(*children: structures.TaskSpec) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name="test-pipeline",
                implementation=structures.GraphImplementation(
                    graph=structures.GraphSpec(
                        tasks={
                            f"task-{index}": task for index, task in enumerate(children)
                        }
                    )
                ),
            )
        )
    )


def _make_test_client() -> tuple[TestClient, orm.sessionmaker]:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    session_factory = orm.sessionmaker(engine)

    def get_session():
        with session_factory() as session:
            yield session

    def get_user_details() -> api_router.UserDetails:
        return api_router.UserDetails(
            name="test-user",
            permissions=api_router.Permissions(read=True, write=True, admin=False),
        )

    app = fastapi.FastAPI()
    api_router._setup_routes_internal(
        app=app,
        get_session=get_session,
        user_details_getter=get_user_details,
    )
    return TestClient(app), session_factory


def test_get_pipeline_run_capabilities_returns_supported_and_deprecated_gpus() -> None:
    response = compute_resources.get_pipeline_run_capabilities()

    assert response.gpus == (
        compute_resources.GpuResource(
            id="NVIDIA-B300", display_name="NVIDIA B300", status="supported"
        ),
        compute_resources.GpuResource(
            id="NVIDIA-H200", display_name="NVIDIA H200", status="deprecated"
        ),
    )


def test_deprecated_gpu_is_valid() -> None:
    compute_resources.validate_pipeline_gpu_resources(
        _make_container_task(gpu_id="NVIDIA-H200")
    )


def test_deprecated_gpu_in_skypilot_string_format_is_valid() -> None:
    task = _make_container_task()
    task.annotations = {_GPU_ANNOTATION_KEY: "NVIDIA-H200:1"}

    compute_resources.validate_pipeline_gpu_resources(task)


def test_unsupported_gpu_in_skypilot_string_format_is_rejected() -> None:
    task = _make_container_task()
    task.annotations = {_GPU_ANNOTATION_KEY: "NVIDIA-A100:1"}

    with pytest.raises(errors.UnsupportedGpuError) as exc_info:
        compute_resources.validate_pipeline_gpu_resources(task)

    assert exc_info.value.unsupported_gpus == ["NVIDIA-A100"]


def test_malformed_skypilot_string_format_is_rejected() -> None:
    task = _make_container_task()
    task.annotations = {_GPU_ANNOTATION_KEY: "NVIDIA-H200"}

    with pytest.raises(errors.ApiValidationError, match="SkyPilot"):
        compute_resources.validate_pipeline_gpu_resources(task)


def test_supported_gpu_is_valid_in_nested_task() -> None:
    compute_resources.validate_pipeline_gpu_resources(
        _make_graph_task(_make_container_task(gpu_id="NVIDIA-B300"))
    )


def test_unsupported_gpu_in_nested_task_is_rejected() -> None:
    with pytest.raises(errors.UnsupportedGpuError) as exc_info:
        compute_resources.validate_pipeline_gpu_resources(
            _make_graph_task(_make_container_task(gpu_id="NVIDIA-A100"))
        )

    assert exc_info.value.unsupported_gpus == ["NVIDIA-A100"]


def test_rejected_gpu_does_not_create_pipeline_run() -> None:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)

    with orm.Session(engine) as session:
        with pytest.raises(errors.UnsupportedGpuError):
            api_server_sql.PipelineRunsApiService_Sql().create(
                session=session,
                root_task=_make_container_task(gpu_id="NVIDIA-A100"),
            )

        assert session.query(bts.PipelineRun).count() == 0
        assert session.query(bts.ExecutionNode).count() == 0


def test_pipeline_run_capabilities_api_response() -> None:
    client, _ = _make_test_client()

    response = client.get("/api/pipeline_runs/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "gpus": [
            {
                "id": "NVIDIA-B300",
                "display_name": "NVIDIA B300",
                "status": "supported",
            },
            {
                "id": "NVIDIA-H200",
                "display_name": "NVIDIA H200",
                "status": "deprecated",
            },
        ]
    }


def test_pipeline_run_api_returns_structured_unsupported_gpu_error() -> None:
    client, session_factory = _make_test_client()
    root_task = _make_container_task(gpu_id="NVIDIA-A100")

    response = client.post(
        "/api/pipeline_runs/",
        json={"root_task": root_task.to_json_dict()},
    )

    assert response.status_code == 422
    assert response.json() == {
        "reason": "unsupported_gpu",
        "detail": "Unsupported GPU resource(s): 'NVIDIA-A100'.",
        "unsupported_gpus": ["NVIDIA-A100"],
    }
    with session_factory() as session:
        assert session.query(bts.PipelineRun).count() == 0
        assert session.query(bts.ExecutionNode).count() == 0
