from typing import Callable
from unittest import mock

from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


def _initialize_db_and_get_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def test_running_pipeline_with_secrets():
    user = "user1"
    secret_name = "SECRET_1"
    secret_value = "SECRET_1_VALUE"

    secret_input_name = "secret_input"

    component_spec = component_structures.ComponentSpec(
        inputs=[
            component_structures.InputSpec(name=secret_input_name),
        ],
        implementation=component_structures.ContainerImplementation(
            container=component_structures.ContainerSpec(image="python")
        ),
    )

    task_spec1 = component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(spec=component_spec),
        arguments={
            secret_input_name: component_structures.SecretArgument(
                secret=component_structures.SecretReference(
                    id=secret_id,
                )
            )
        },
    )

    graph_input_name = "graph_input_1"
    task_spec2 = component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(spec=component_spec),
        arguments={
            secret_input_name: component_structures.GraphInputArgument(
                graph_input=component_structures.GraphInputReference(
                    input_name=graph_input_name
                )
            )
        },
    )

    for task_spec in [task_spec1, task_spec2]:
        pipeline_spec = component_structures.ComponentSpec(
            inputs=[
                component_structures.InputSpec(name=graph_input_name),
            ],
            implementation=component_structures.GraphImplementation(
                graph=component_structures.GraphSpec(
                    tasks={
                        "task": task_spec,
                    }
                )
            ),
        )

        root_pipeline_task = component_structures.TaskSpec(
            component_ref=component_structures.ComponentReference(spec=pipeline_spec),
            arguments={
                graph_input_name: component_structures.SecretArgument(
                    secret=component_structures.SecretReference(id=secret_id)
                )
            },
        )

        session_factory = _initialize_db_and_get_session_factory()
        secrets_service = api_server_sql.SecretsApiService()
        pipeline_runs_service = api_server_sql.PipelineRunsApiService_Sql()

        secrets_service.create_secret(
            session=session_factory(),
            user_id=user,
            secret_name=secret_name,
            secret_value=secret_value,
        )

        list_secrets_response = secrets_service.list_secrets(
            session=session_factory(),
            user_id=user,
        )
        assert list_secrets_response.secrets
        assert list_secrets_response.secrets[0].secret_name == secret_name

        pipeline_runs_service.create(
            session=session_factory(),
            root_task=root_pipeline_task,
            created_by=user,
        )

        storage_provider_mock = mock.MagicMock()
        launched_container_mock = mock.MagicMock(
            status=launcher_interfaces.ContainerStatus.PENDING,
            to_dict=lambda: {"foo": "bar"},
        )
        launch_container_task_mock = mock.MagicMock(
            return_value=launched_container_mock
        )
        launcher_mock = mock.MagicMock(launch_container_task=launch_container_task_mock)
        data_root_uri = "file:///tmp/artifacts"
        logs_root_uri = "file:///tmp/logs"

        from cloud_pipelines_backend import orchestrator_sql

        orchestrator = orchestrator_sql.OrchestratorService_Sql(
            session_factory=session_factory,
            launcher=launcher_mock,
            storage_provider=storage_provider_mock,
            data_root_uri=data_root_uri,
            logs_root_uri=logs_root_uri,
        )
        orchestrator.process_each_queue_once()

        launch_container_task_mock.assert_called_once()
        input_arguments: dict[str, launcher_interfaces.InputArgument] | None = (
            launch_container_task_mock.call_args.kwargs.get("input_arguments")
        )
        assert input_arguments
        secret_argument = input_arguments.get(secret_input_name)
        assert secret_argument
        assert secret_argument.value == secret_value
        assert secret_argument.is_secret
