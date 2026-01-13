import copy
import json
import datetime
import logging
import time
import traceback
import typing
from typing import Any


import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_provider_interfaces,
)
from cloud_pipelines.orchestration.launchers import naming_utils

from . import backend_types_sql as bts
from . import component_structures as structures
from .launchers import common_annotations
from .launchers import interfaces as launcher_interfaces


_logger = logging.getLogger(__name__)

_T = typing.TypeVar("_T")


class OrchestratorError(RuntimeError):
    pass


class OrchestratorService_Sql:
    def __init__(
        self,
        session_factory: typing.Callable[[], orm.Session],
        launcher: launcher_interfaces.ContainerTaskLauncher[
            launcher_interfaces.LaunchedContainer
        ],
        storage_provider: storage_provider_interfaces.StorageProvider,
        data_root_uri: str,
        logs_root_uri: str,
        default_task_annotations: dict[str, Any] | None = None,
        sleep_seconds_between_queue_sweeps: float = 1.0,
        output_data_purge_duration: datetime.timedelta = None,
    ):
        self._session_factory = session_factory
        self._launcher = launcher
        self._storage_provider = storage_provider
        self._data_root_uri = data_root_uri
        self._logs_root_uri = logs_root_uri
        self._default_task_annotations = default_task_annotations
        self._sleep_seconds_between_queue_sweeps = sleep_seconds_between_queue_sweeps
        self._queued_executions_queue_idle = False
        self._running_executions_queue_idle = False
        self._output_data_purge_duration = output_data_purge_duration

    def run_loop(self):
        while True:
            try:
                self.process_each_queue_once()
                time.sleep(self._sleep_seconds_between_queue_sweeps)
            except:
                _logger.exception("Error while calling `process_each_queue_once`")

    def process_each_queue_once(self):
        queue_handlers = [
            self.internal_process_queued_executions_queue,
            self.internal_process_running_executions_queue,
        ]
        for queue_handler in queue_handlers:
            try:
                with self._session_factory() as session:
                    queue_handler(session=session)
            except:
                _logger.exception(f"Error while executing {queue_handler=}")

    def internal_process_queued_executions_queue(self, session: orm.Session):
        query = (
            sql.select(bts.ExecutionNode).where(
                bts.ExecutionNode.container_execution_status.in_(
                    (
                        bts.ContainerExecutionStatus.UNINITIALIZED,
                        bts.ContainerExecutionStatus.QUEUED,
                    )
                )
            )
            # TODO: Maybe add last_processed_at
            # .order_by(bts.ExecutionNode.last_processed_at)
            # .order_by(bts.ExecutionNode.id)
            .limit(1)
        )
        queued_execution = session.scalar(query)
        if queued_execution:
            self._queued_executions_queue_idle = False
            start_timestamp = time.monotonic_ns()
            _logger.info(f"Before processing {queued_execution.id=}")
            try:
                self.internal_process_one_queued_execution(
                    session=session, execution=queued_execution
                )
            except Exception as ex:
                _logger.exception(f"Error processing {queued_execution.id=}")
                session.rollback()
                queued_execution.container_execution_status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                record_system_error_exception(execution=queued_execution, exception=ex)
                session.commit()
            finally:
                duration_ms = int((time.monotonic_ns() - start_timestamp) / 1_000_000)
                _logger.info(
                    f"After processing {queued_execution.id=}. Duration={duration_ms}ms"
                )
            return True
        else:
            if not self._queued_executions_queue_idle:
                self._queued_executions_queue_idle = True
                _logger.debug(f"No queued executions found")
            return False

    def internal_process_running_executions_queue(self, session: orm.Session):
        query = (
            sql.select(bts.ContainerExecution)
            .where(
                bts.ContainerExecution.status.in_(
                    (
                        bts.ContainerExecutionStatus.PENDING,
                        bts.ContainerExecutionStatus.RUNNING,
                    )
                )
            )
            .order_by(bts.ContainerExecution.last_processed_at)
            .limit(1)
        )
        running_container_execution = session.scalar(query)
        if running_container_execution:
            self._running_executions_queue_idle = False
            start_timestamp = time.monotonic_ns()
            _logger.info(f"Before processing {running_container_execution.id=}")
            try:
                self.internal_process_one_running_execution(
                    session=session, container_execution=running_container_execution
                )
            except Exception as ex:
                _logger.exception(f"Error processing {running_container_execution.id=}")
                session.rollback()
                running_container_execution.status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                # Doing an intermediate commit here because it's most important to mark the problematic execution as SYSTEM_ERROR.
                session.commit()
                # Mark our ExecutionNode as SYSTEM_ERROR
                execution_nodes = running_container_execution.execution_nodes
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.SYSTEM_ERROR
                    )
                    record_system_error_exception(
                        execution=execution_node, exception=ex
                    )
                # Doing an intermediate commit here because it's most important to mark the problematic node as SYSTEM_ERROR.
                session.commit()
                # Skip downstream executions
                for execution_node in execution_nodes:
                    _mark_all_downstream_executions_as_skipped(
                        session=session, execution=execution_node
                    )
                session.commit()
            finally:
                duration_ms = int((time.monotonic_ns() - start_timestamp) / 1_000_000)
                _logger.info(
                    f"After processing {running_container_execution.id=}. Duration={duration_ms}ms"
                )
            return True
        else:
            if not self._running_executions_queue_idle:
                _logger.debug(f"No running container executions found")
                self._running_executions_queue_idle = True
            return False

    def internal_process_one_queued_execution(
        self, session: orm.Session, execution: bts.ExecutionNode
    ):
        task_spec = structures.TaskSpec.from_json_dict(execution.task_spec)
        component_spec = task_spec.component_ref.spec
        if component_spec is None:
            raise OrchestratorError(
                f"Missing ComponentSpec. {task_spec.component_ref=}"
            )
        implementation = component_spec.implementation
        if not implementation:
            raise OrchestratorError(f"Missing implementation. {component_spec=}")
        if implementation is None or not isinstance(
            implementation, structures.ContainerImplementation
        ):
            raise OrchestratorError(
                f"Implementation must be container. {implementation=}"
            )
        container_spec = implementation.container
        input_artifact_data = dict(
            session.execute(
                sql.select(bts.InputArtifactLink.input_name, bts.ArtifactData)
                .where(bts.InputArtifactLink.execution == execution)
                .join(bts.InputArtifactLink.artifact)
                .join(bts.ArtifactNode.artifact_data, isouter=True)
            )
            .tuples()
            .all()
        )
        if None in input_artifact_data.values():
            _logger.info(
                f"Execution did not have all input artifact data present. Waiting for upstream. {execution.id=}"
            )
            execution.container_execution_status = (
                bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
            )
            session.commit()
            return

        cache_key = _calculate_container_execution_cache_key(
            container_spec=container_spec, input_artifact_data=input_artifact_data
        )

        # Trying to reuse an older execution from cache.
        max_cache_staleness_str: str | None = None
        if task_spec.execution_options and task_spec.execution_options.caching_strategy:
            max_cache_staleness_str = (
                task_spec.execution_options.caching_strategy.max_cache_staleness
            )

        # TODO: Support other ways to express 0-length time period.
        if max_cache_staleness_str == "P0D":
            execution_candidates: typing.Sequence[bts.ExecutionNode] = []
        else:
            # TODO: Support cache reuse options (max cached data staleness etc).
            # TODO: It might be better to search for ContainerExecutions rather than ExecutionNodes.
            execution_candidates = session.scalars(
                sql.select(bts.ExecutionNode)
                .where(bts.ExecutionNode.container_execution_cache_key == cache_key)
                .where(
                    bts.ExecutionNode.container_execution_status.in_(
                        [
                            # We can reuse both succeeded executions and also non yet finished ones.
                            # Reusing still running executions is important since it allows cache reuse
                            # when multiple versions of a pipeline are submitted in parallel.
                            # bts.ContainerExecutionStatus.STARTING,  # Doesn't exist yet
                            bts.ContainerExecutionStatus.PENDING,
                            bts.ContainerExecutionStatus.RUNNING,
                            bts.ContainerExecutionStatus.SUCCEEDED,
                        ]
                    )
                )
                # TODO: Filter by purged==False. Or `expires_at`.
                .join(bts.ContainerExecution)
                .order_by(bts.ContainerExecution.created_at.desc())
            ).all()
        non_purged_candidates = [
            execution_candidate
            for execution_candidate in execution_candidates
            if not (execution_candidate.extra_data or {}).get("is_purged", False)
        ]
        if self._output_data_purge_duration:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            data_purge_threshold_time = current_time - self._output_data_purge_duration
            non_purged_candidates = [
                execution_candidate
                for execution_candidate in execution_candidates
                # The `execution_candidate.container_execution` attribute access accesses the DB.
                # TODO: Move the time filtering to the DB side.
                # TODO: Filter by `ended_at`, not `created_at`.
                # Note: Need `.astimezone` to avoid `TypeError: can't compare offset-naive and offset-aware datetimes`
                if execution_candidate.container_execution.created_at.astimezone(
                    datetime.timezone.utc
                )
                > data_purge_threshold_time
            ]

        if non_purged_candidates:
            # Re-using the oldest candidate
            # TODO: Use better cached execution selection strategy:
            # 1. Try to reuse the execution with `max_cache_staleness in extra_data["used_for_max_cache_staleness"]`. There should be 0 or 1 of those.
            # 2. Else: Try to reuse the latest SUCCEEDED execution. Mark it `extra_data["used_for_max_cache_staleness"][max_cache_staleness] = True`.
            # 3. Else: Try to reuse the latest RUNNING/PENDING execution. Mark it `extra_data["used_for_max_cache_staleness"][max_cache_staleness] = True`.
            # There must be at least one SUCCEEDED/RUNNING/PENDING since non_purged_candidates is non-empty.
            old_execution = non_purged_candidates[-1]
            _logger.info(
                f"Execution {execution.id=} will reuse the {old_execution.id=} with "
                f"{old_execution.container_execution_id=}, {old_execution.container_execution_status=}"
            )
            # Reusing the execution:
            if not execution.extra_data:
                execution.extra_data = {}
            execution.extra_data["reused_from_execution_node_id"] = old_execution.id

            execution.container_execution_status = (
                old_execution.container_execution_status
            )

            # If the execution is still running, it's enough to just link execution node to it.
            # The container execution will set the outputs itself when it succeeds.
            execution.container_execution_id = old_execution.container_execution_id
            # However if the container execution has already ended, we need to copy the outputs ourselves.
            if (
                old_execution.container_execution_status
                == bts.ContainerExecutionStatus.SUCCEEDED
            ):
                # Copying the output artifact data (if the execution already succeeded).
                reused_execution_output_artifact_data_ids = {
                    output_name: artifact_data_id
                    for output_name, artifact_data_id in session.execute(
                        sql.select(
                            bts.OutputArtifactLink.output_name,
                            bts.ArtifactNode.artifact_data_id,
                        )
                        .join(bts.OutputArtifactLink.artifact)
                        .where(bts.OutputArtifactLink.execution_id == old_execution.id)
                    ).tuples()
                }
                # Setting artifact data
                for output_name, artifact_node in session.execute(
                    sql.select(bts.OutputArtifactLink.output_name, bts.ArtifactNode)
                    # TODO: Verify that this improves the performance
                    .with_for_update()
                    .join(bts.OutputArtifactLink.artifact)
                    .where(bts.OutputArtifactLink.execution_id == execution.id)
                ).tuples():
                    artifact_node.artifact_data_id = (
                        reused_execution_output_artifact_data_ids[output_name]
                    )
                # Waking up all direct downstream executions.
                # Many of them will get processed and go back to WAITING_FOR_UPSTREAM state.
                for downstream_execution in _get_direct_downstream_executions(
                    session=session, execution=execution
                ):
                    if (
                        downstream_execution.container_execution_status
                        == bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
                    ):
                        downstream_execution.container_execution_status = (
                            bts.ContainerExecutionStatus.QUEUED
                        )
            session.commit()
            return

        # Launching the container execution

        pipeline_run = session.scalar(
            sql.select(bts.PipelineRun)
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == bts.PipelineRun.root_execution_id,
            )
            .where(bts.ExecutionToAncestorExecutionLink.execution_id == execution.id)
        )
        session.rollback()

        if pipeline_run is None:
            raise OrchestratorError(
                f"Execution {execution} is not associated with a PipelineRun."
            )

        # Handling cancellation.
        # In case of cancellation, we do not create container execution and we skip all downstream executions.
        should_terminate_execution = (execution.extra_data or {}).get(
            "desired_state"
        ) == "TERMINATED"
        should_terminate_run = (pipeline_run.extra_data or {}).get(
            "desired_state"
        ) == "TERMINATED"
        should_terminate = should_terminate_execution or should_terminate_run
        if should_terminate:
            _logger.info(
                f"Cancelling execution {execution.id} and skipping all downstream executions."
            )
            execution.container_execution_status = (
                bts.ContainerExecutionStatus.CANCELLED
            )
            _mark_all_downstream_executions_as_skipped(
                session=session, execution=execution
            )
            session.commit()
            return

        # Creating new container execution
        container_execution_uuid = _generate_random_id()

        _ARTIFACT_PATH_LAST_PART = "data"
        _LOG_FILE_NAME = "log.txt"
        # _TIMESTAMPED_LOG_FILE_NAME = "timestamped.log.txt"
        # _STDOUT_LOG_FILE_NAME = "stdout.log.txt"
        # _STDERR_LOG_FILE_NAME = "stderr.log.txt"

        # Input URIs might be needed to write constant input data.
        # Or to prevent the data from being purged during the execution.
        # TODO: Ensure unique artifact URIs (they can become non-unique due to sanitization)
        def generate_input_artifact_uri(
            root_dir: str,
            execution_id: str,
            input_name: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            input_name_str = naming_utils.sanitize_file_name(input_name)
            return f"{root_dir}/by_execution/{execution_id_str}/inputs/{input_name_str}/{_ARTIFACT_PATH_LAST_PART}"

        def generate_output_artifact_uri(
            root_dir: str,
            execution_id: str,
            output_name: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            output_name_str = naming_utils.sanitize_file_name(output_name)
            return f"{root_dir}/by_execution/{execution_id_str}/outputs/{output_name_str}/{_ARTIFACT_PATH_LAST_PART}"

        def generate_execution_log_uri(
            root_dir: str,
            execution_id: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            return f"{root_dir}/by_execution/{execution_id_str}/{_LOG_FILE_NAME}"

        log_uri = generate_execution_log_uri(
            root_dir=self._logs_root_uri, execution_id=container_execution_uuid
        )
        output_artifact_uris = {
            output_spec.name: generate_output_artifact_uri(
                root_dir=self._data_root_uri,
                execution_id=container_execution_uuid,
                output_name=output_spec.name,
            )
            for output_spec in component_spec.outputs or []
        }

        # Handling secrets.
        # We read secrets from execution_node.extra_data rather than from task_spec.arguments,
        # because some secrets might have been passed from upstream graph inputs.
        secret_reference_arguments = (execution.extra_data or {}).get(
            bts.EXECUTION_NODE_EXTRA_DATA_SECRET_REFERENCE_ARGUMENTS_KEY, {}
        )
        secret_hash = "<DUMMY_HASH_FOR_SECRET>"
        for input_name, secret_reference_dict in secret_reference_arguments.items():
            user_id = pipeline_run.created_by
            secret_id = secret_reference_dict["id"]
            secret = session.get(bts.Secret, (user_id, secret_id))
            if not secret:
                raise OrchestratorError(
                    f"{execution.id=}: User error: Error resolving a secret argument for {input_name=}: User {user_id} does not have secret {secret_id}."
                )
            secret_value = secret.secret_value
            input_artifact_data[input_name] = bts.ArtifactData(
                total_size=len(secret_value.encode("utf-8")),
                is_dir=False,
                value=secret_value,
                uri=None,
                # This hash is not used, so we're using a dummy value here that makes it possible to identify the secret arguments in the following code.
                hash=secret_hash,
            )
        session.rollback()

        # Preparing the launcher input arguments

        input_arguments = {
            input_name: launcher_interfaces.InputArgument(
                total_size=artifact_data.total_size,
                is_dir=artifact_data.is_dir,
                value=artifact_data.value,
                uri=artifact_data.uri,
                staging_uri=generate_input_artifact_uri(
                    root_dir=self._data_root_uri,
                    execution_id=container_execution_uuid,
                    input_name=input_name,
                ),
                is_secret=(artifact_data.hash == secret_hash),
            )
            for input_name, artifact_data in input_artifact_data.items()
        }

        # Handling annotations

        full_annotations = {}
        _update_dict_recursive(
            full_annotations, copy.deepcopy(self._default_task_annotations or {})
        )
        _update_dict_recursive(
            full_annotations, copy.deepcopy(pipeline_run.annotations or {})
        )
        _update_dict_recursive(
            full_annotations, copy.deepcopy(task_spec.annotations or {})
        )

        full_annotations[common_annotations.PIPELINE_RUN_CREATED_BY_ANNOTATION_KEY] = (
            pipeline_run.created_by
        )
        full_annotations[common_annotations.PIPELINE_RUN_ID_ANNOTATION_KEY] = (
            pipeline_run.id
        )
        full_annotations[common_annotations.EXECUTION_NODE_ID_ANNOTATION_KEY] = (
            execution.id
        )
        full_annotations[common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY] = (
            container_execution_uuid
        )

        try:
            launched_container: launcher_interfaces.LaunchedContainer = (
                self._launcher.launch_container_task(
                    component_spec=component_spec,
                    # The value and uri might be updated during launching.
                    input_arguments=input_arguments,
                    output_uris=output_artifact_uris,
                    log_uri=log_uri,
                    annotations=full_annotations,
                )
            )
            if launched_container.status not in (
                launcher_interfaces.ContainerStatus.PENDING,
                launcher_interfaces.ContainerStatus.RUNNING,
            ):
                raise OrchestratorError(
                    f"Unexpected status of just launched container: {launched_container.status=}, {launched_container=}"
                )
        except Exception as ex:
            session.rollback()
            with session.begin():
                # Logs whole exception
                _logger.exception(f"Error launching container for {execution.id=}")
                execution.container_execution_status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                record_system_error_exception(execution=execution, exception=ex)
                _mark_all_downstream_executions_as_skipped(
                    session=session, execution=execution
                )
            return

        current_time = _get_current_time()

        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus(launched_container.status),
            last_processed_at=current_time,
            created_at=current_time,
            launcher_data=launched_container.to_dict(),
            # Updating the map with uploaded value URIs and downloaded values.
            input_artifact_data_map={
                input_name: dict(
                    id=artifact_data.id,
                    is_dir=artifact_data.is_dir,
                    total_size=artifact_data.total_size,
                    hash=artifact_data.hash,
                    # The value and URI might have been updated during launching.
                    value=input_arguments[input_name].value,
                    uri=input_arguments[input_name].uri,
                )
                for input_name, artifact_data in input_artifact_data.items()
            },
            output_artifact_data_map={
                output_name: dict(
                    uri=uri,
                )
                for output_name, uri in output_artifact_uris.items()
            },
            log_uri=log_uri,
        )
        container_execution.id = container_execution_uuid

        # Need to commit/rollback before explicitly beginning non-nested transaction.
        session.rollback()
        with session.begin():
            session.add(container_execution)
            execution.container_execution = container_execution
            execution.container_execution_cache_key = cache_key
            execution.container_execution_status = container_execution.status
            # TODO: Maybe add artifact value and URI to input ArtifactData.

    def internal_process_one_running_execution(
        self, session: orm.Session, container_execution: bts.ContainerExecution
    ):
        # Should we update last_processed_at early? Should we do it always (even when there are errors).
        # ALways updating the last_processed_at such that a problematic container does not get queue stuck.
        session.expire_on_commit = False
        session.rollback()
        with session.begin():
            container_execution.last_processed_at = _get_current_time()
        launcher_data = _assert_not_none(container_execution.launcher_data)
        launched_container: launcher_interfaces.LaunchedContainer = (
            self._launcher.deserialize_launched_container_from_dict(launcher_data)
        )
        previous_status = launched_container.status
        if previous_status not in (
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
        ):
            raise OrchestratorError(
                f"Unexpected running container status: {previous_status=}, {launched_container=}"
            )

        # Handling cancellation
        votes_to_terminate = []
        votes_to_not_terminate = []
        # TODO: Get the desired state from the pipeline runs, not execution nodes
        execution_nodes = container_execution.execution_nodes
        for execution_node in execution_nodes:
            should_terminate = (execution_node.extra_data or {}).get(
                "desired_state"
            ) == "TERMINATED"
            if should_terminate:
                votes_to_terminate.append(execution_node)
            else:
                votes_to_not_terminate.append(execution_node)

        if votes_to_terminate:
            # Terminate the container execution only when all execution nodes pointing to it are asked to terminate.
            terminated = False
            if votes_to_not_terminate:
                _logger.info(
                    f"Not terminating container execution {container_execution.id=} since some other executions ({[execution_node.id for execution_node in votes_to_not_terminate]}) are still using it."
                )
            else:
                _logger.info(
                    f"Terminating container execution {container_execution.id}."
                )
                # We should preserve the logs before terminating/deleting the container
                try:
                    _retry(lambda: launched_container.upload_log())
                except:
                    _logger.exception(
                        f"Error uploading logs for {container_execution.id=} before termination."
                    )
                # Requesting container termination.
                # Termination might not happen immediately (e.g. Kubernetes has grace period).
                launched_container.terminate()
                container_execution.ended_at = _get_current_time()
                # We need to mark the execution as CANCELLED otherwise orchestrator will continue polling it.
                container_execution.status = bts.ContainerExecutionStatus.CANCELLED
                terminated = True

            # Mark the execution nodes as cancelled only after the launched container is successfully terminated (if needed)
            for execution_node in votes_to_terminate:
                _logger.info(
                    f"Cancelling execution {execution_node.id} ({container_execution.id=}) and skipping all downstream executions."
                )
                execution_node.container_execution_status = (
                    bts.ContainerExecutionStatus.CANCELLED
                )
                _mark_all_downstream_executions_as_skipped(
                    session=session, execution=execution_node
                )
            session.commit()
            if terminated:
                return

        # Asking the launcher to refresh the container state.
        reloaded_launched_container: launcher_interfaces.LaunchedContainer = (
            self._launcher.get_refreshed_launched_container_from_dict(launcher_data)
        )
        current_time = _get_current_time()
        # Saving the updated launcher data
        reloaded_launcher_data = reloaded_launched_container.to_dict()
        if reloaded_launcher_data != launcher_data:
            session.rollback()
            with session.begin():
                container_execution.launcher_data = reloaded_launcher_data
                container_execution.updated_at = current_time
        new_status: launcher_interfaces.ContainerStatus = (
            reloaded_launched_container.status
        )
        if new_status == previous_status:
            _logger.info(
                f"Container execution {container_execution.id} remains in {new_status} state."
            )
            return
        _logger.info(
            f"Container execution {container_execution.id} is now in state {new_status} (was {previous_status})."
        )
        session.rollback()
        container_execution.updated_at = current_time
        execution_nodes = container_execution.execution_nodes
        if not execution_nodes:
            raise OrchestratorError(
                f"Could not find ExecutionNode associated with ContainerExecution. {container_execution.id=}"
            )
        if len(execution_nodes) > 1:
            execution_node_ids = [execution.id for execution in execution_nodes]
            _logger.warning(
                f"ContainerExecution is associated with multiple ExecutionNodes: {container_execution.id=}, {execution_node_ids=}"
            )

        if new_status == launcher_interfaces.ContainerStatus.RUNNING:
            container_execution.status = bts.ContainerExecutionStatus.RUNNING
            container_execution.started_at = reloaded_launched_container.started_at
            for execution_node in execution_nodes:
                execution_node.container_execution_status = (
                    bts.ContainerExecutionStatus.RUNNING
                )
        elif new_status == launcher_interfaces.ContainerStatus.SUCCEEDED:
            container_execution.status = bts.ContainerExecutionStatus.SUCCEEDED
            container_execution.exit_code = reloaded_launched_container.exit_code
            container_execution.started_at = reloaded_launched_container.started_at
            container_execution.ended_at = reloaded_launched_container.ended_at

            # Don't fail the execution if log upload fails.
            # Logs are important, but not so important that we should fail a successfully completed container execution.
            try:
                _retry(reloaded_launched_container.upload_log)
            except Exception as ex:
                _logger.exception(
                    f"! Error during `LaunchedContainer.upload_log` call: {ex}."
                )

            _MAX_PRELOAD_VALUE_SIZE = 255

            def _maybe_preload_value(
                uri_reader: storage_provider_interfaces.UriReader,
                data_info: storage_provider_interfaces.DataInfo,
            ) -> str | None:
                """Preloads artifact value is it's small enough (e.g. <=255 bytes)"""
                if (
                    not data_info.is_dir
                    and data_info.total_size < _MAX_PRELOAD_VALUE_SIZE
                ):
                    # Don't fail the execution if small value preloading fails.
                    # Those values may be useful for preservation, but not so important that we should fail a successfully completed container execution.
                    try:
                        data = uri_reader.download_as_bytes()
                    except Exception as ex:
                        _logger.exception(
                            f"Error during preloading small artifact values."
                        )
                        return None
                    try:
                        text = data.decode("utf-8")
                        return text
                    except:
                        pass

            output_artifact_uris: dict[str, str] = {
                output_name: output_artifact_info_dict["uri"]
                for output_name, output_artifact_info_dict in container_execution.output_artifact_data_map.items()
            }

            # We need to first check that the output data exists. Otherwise `uri_reader.get_info()`` throws `IndexError`
            # in cloud_pipelines/orchestration/storage_providers/interfaces.py", line 172, in _make_data_info_for_dir
            missing_output_names = [
                output_name
                for output_name, uri in output_artifact_uris.items()
                if not _retry(
                    lambda: self._storage_provider.make_uri(uri).get_reader().exists()
                )
            ]

            if missing_output_names:
                # Marking the container execution as FAILED (even though the program itself has completed successfully)
                container_execution.status = bts.ContainerExecutionStatus.FAILED
                orchestration_error_message = f"Container execution {container_execution.id} is marked as FAILED due to missing outputs: {missing_output_names}."
                _logger.error(orchestration_error_message)
                _record_orchestration_error_message(
                    container_execution=container_execution,
                    execution_nodes=execution_nodes,
                    message=orchestration_error_message,
                )
                # Skip downstream executions
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.FAILED
                    )
                    _mark_all_downstream_executions_as_skipped(
                        session=session, execution=execution_node
                    )
            else:
                output_artifact_data_info_map = {
                    output_name: _retry(
                        lambda: self._storage_provider.make_uri(uri)
                        .get_reader()
                        .get_info()
                    )
                    for output_name, uri in output_artifact_uris.items()
                }
                new_output_artifact_data_map = {
                    output_name: bts.ArtifactData(
                        total_size=data_info.total_size,
                        is_dir=data_info.is_dir,
                        # Using 1st hash only.
                        # TODO: Support multiple hashes.
                        hash=[
                            f"{hash_name}={hash_value}"
                            for hash_name, hash_value in data_info.hashes.items()
                        ][0],
                        uri=output_artifact_uris[output_name],
                        # Preloading artifact value is it's small enough (e.g. <=255 bytes)
                        value=_retry(
                            lambda: _maybe_get_small_artifact_value(
                                uri_reader=self._storage_provider.make_uri(
                                    output_artifact_uris[output_name]
                                ).get_reader(),
                                data_info=data_info,
                            )
                        ),
                        created_at=current_time,
                    )
                    for output_name, data_info in output_artifact_data_info_map.items()
                }

                session.add_all(new_output_artifact_data_map.values())
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.SUCCEEDED
                    )
                    # TODO: Optimize
                    for output_name, artifact_node in session.execute(
                        sql.select(bts.OutputArtifactLink.output_name, bts.ArtifactNode)
                        .join(bts.OutputArtifactLink.artifact)
                        .where(bts.OutputArtifactLink.execution_id == execution_node.id)
                    ).tuples():
                        artifact_node.artifact_data = new_output_artifact_data_map[
                            output_name
                        ]
                        artifact_node.had_data_in_past = True
                    # Waking up all direct downstream executions.
                    # Many of them will get processed and go back to WAITING_FOR_UPSTREAM state.
                    for downstream_execution in _get_direct_downstream_executions(
                        session=session, execution=execution_node
                    ):
                        if (
                            downstream_execution.container_execution_status
                            == bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
                        ):
                            downstream_execution.container_execution_status = (
                                bts.ContainerExecutionStatus.QUEUED
                            )
        elif new_status == launcher_interfaces.ContainerStatus.FAILED:
            container_execution.status = bts.ContainerExecutionStatus.FAILED
            container_execution.exit_code = reloaded_launched_container.exit_code
            container_execution.started_at = reloaded_launched_container.started_at
            container_execution.ended_at = reloaded_launched_container.ended_at
            launcher_error = reloaded_launched_container.launcher_error_message
            if launcher_error:
                orchestration_error_message = f"Launcher error: {launcher_error}"
                _record_orchestration_error_message(
                    container_execution=container_execution,
                    execution_nodes=execution_nodes,
                    message=orchestration_error_message,
                )

            _retry(reloaded_launched_container.upload_log)
            # Skip downstream executions
            for execution_node in execution_nodes:
                execution_node.container_execution_status = (
                    bts.ContainerExecutionStatus.FAILED
                )
                _mark_all_downstream_executions_as_skipped(
                    session=session, execution=execution_node
                )
        else:
            _logger.error(
                f"Container execution {container_execution.id} is now in unexpected state {new_status}. System error. {container_execution=}"
            )
            # This SYSTEM_ERROR will be handled by the outer exception handler
            raise OrchestratorError(
                f"Unexpected running container status: {new_status=}, {launched_container=}"
            )
        session.commit()


def _get_direct_downstream_executions(
    session: orm.Session, execution: bts.ExecutionNode
):
    return session.scalars(
        sql.select(bts.ExecutionNode)
        .select_from(bts.OutputArtifactLink)
        .where(bts.OutputArtifactLink.execution_id == execution.id)
        .join(bts.ArtifactNode)
        .join(bts.InputArtifactLink)
        .join(bts.ExecutionNode)
    ).all()


# TODO: Cover with tests
def _mark_all_downstream_executions_as_skipped(
    session: orm.Session,
    execution: bts.ExecutionNode,
    seen_execution_ids: set[bts.IdType] | None = None,
):
    if seen_execution_ids is None:
        seen_execution_ids = set()
    if execution.id in seen_execution_ids:
        return
    seen_execution_ids.add(execution.id)
    if execution.container_execution_status in {
        bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM,
        # A downstream ExecutionNode can be in "Queued" state when it's been "woken up" by one of its upstreams.
        bts.ContainerExecutionStatus.QUEUED,
    }:
        execution.container_execution_status = bts.ContainerExecutionStatus.SKIPPED

    # for artifact_node in execution.output_artifact_nodes:
    #     for downstream_execution in artifact_node.downstream_executions:
    for downstream_execution in _get_direct_downstream_executions(session, execution):
        _mark_all_downstream_executions_as_skipped(
            session=session,
            execution=downstream_execution,
            seen_execution_ids=seen_execution_ids,
        )


def _assert_type(value: typing.Any, typ: typing.Type[_T]) -> _T:
    if not isinstance(value, typ):
        raise TypeError(f"Expected type {typ}, but got {type(value)}: {value}")
    return value


def _assert_not_none(value: _T | None) -> _T:
    if value is None:
        raise TypeError(f"Expected value to be not None, but got {value}.")
    return value


def _calculate_hash(s: str) -> str:
    import hashlib

    return "md5=" + hashlib.md5(s.encode("utf-8")).hexdigest()


def _calculate_container_execution_cache_key(
    container_spec: structures.ContainerSpec,
    input_artifact_data: dict[str, bts.ArtifactData],
):
    # Using ContainerSPec instead of the whole ComponentSpec.
    input_hashes = {
        input_name: _assert_not_none(artifact_data).hash
        for input_name, artifact_data in (input_artifact_data or {}).items()
    }
    cache_key_struct = {
        "container_spec": container_spec.to_json_dict(),
        "input_hashes": input_hashes,
    }
    # Need to sort keys to ensure consistency
    cache_key_str = json.dumps(cache_key_struct, separators=(",", ":"), sort_keys=True)
    # We could use a different hash function, but there is no reason to.
    cache_key = _calculate_hash(cache_key_str)
    return cache_key


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _generate_random_id() -> str:
    import os
    import time

    random_bytes = os.urandom(4)
    nanoseconds = time.time_ns()
    milliseconds = nanoseconds // 1_000_000

    return ("%012x" % milliseconds) + random_bytes.hex()


def _update_dict_recursive(d1: dict, d2: dict):
    for k, v2 in d2.items():
        if k in d1:
            v1 = d1[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                _update_dict_recursive(v1, v2)
                continue
            # elif isinstance(v1, list) and isinstance(v2, list):
            # # Merging lists is not supported yet
        d1[k] = v2


def _retry(
    func: typing.Callable[[], _T], max_retries: int = 5, wait_seconds: float = 1.0
) -> _T:
    for i in range(max_retries):
        try:
            return func()
        except:
            _logger.exception(f"Exception calling {func}.")
            time.sleep(wait_seconds)
            if i == max_retries - 1:
                raise
    raise


def record_system_error_exception(execution: bts.ExecutionNode, exception: Exception):
    if execution.extra_data is None:
        execution.extra_data = {}
    execution.extra_data[
        bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY
    ] = "".join(traceback.format_exception_only(type(exception), exception))
    execution.extra_data[
        bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY
    ] = traceback.format_exc()


def _record_orchestration_error_message(
    container_execution: bts.ContainerExecution,
    execution_nodes: list[bts.ExecutionNode],
    message: str,
):
    if container_execution.extra_data is None:
        container_execution.extra_data = {}
    container_execution.extra_data[
        bts.CONTAINER_EXECUTION_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
    ] = message

    for execution_node in execution_nodes:
        if execution_node.extra_data is None:
            execution_node.extra_data = {}
        execution_node.extra_data[
            bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
        ] = message


_MAX_PRELOAD_VALUE_SIZE = 255


def _maybe_get_small_artifact_value(
    uri_reader: storage_provider_interfaces.UriReader,
    data_info: storage_provider_interfaces.DataInfo,
) -> str | None:
    """Preloads artifact value is it's small enough (e.g. <=255 bytes)"""
    if not data_info.is_dir and data_info.total_size < _MAX_PRELOAD_VALUE_SIZE:
        # Don't fail the execution if small value preloading fails.
        # Those values may be useful for preservation, but not so important that we should fail a successfully completed container execution.
        try:
            data = uri_reader.download_as_bytes()
        except Exception as ex:
            _logger.exception(f"Error during preloading small artifact values.")
            return None
        try:
            text = data.decode("utf-8")
            return text
        except:
            pass
