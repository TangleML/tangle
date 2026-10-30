"""Tests for SQLAlchemy event listeners in orchestrator_sql and instrumentation.metrics."""

import datetime
import unittest.mock

import pytest
from sqlalchemy import event as sql_event
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import (
    orchestrator_sql,
)  # noqa: F401 — registers set listener
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.instrumentation import execution_annotations
from cloud_pipelines_backend.instrumentation import (
    metrics,
)  # noqa: F401 — registers before_commit listener


@pytest.fixture()
def session() -> orm.Session:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    with orm.Session(db_engine) as s:
        yield s


class TestStatusHistoryListeners:
    def test_status_change_appends_history_to_extra_data(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        history = node.extra_data[bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY]
        assert len(history) == 1
        assert history[0]["status"] == bts.ContainerExecutionStatus.QUEUED

    def test_duplicate_status_is_not_appended_to_history(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        history = node.extra_data[bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY]
        assert len(history) == 1

    def test_second_status_change_records_duration_metric(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()

        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["attributes"] == {
            "execution.status.from": bts.ContainerExecutionStatus.QUEUED,
            "execution.status.to": bts.ContainerExecutionStatus.RUNNING,
            "execution.run_attempt": "first",
            "execution.cloud_provider": "none",
            "execution.accelerator_type": "none",
            "execution.accelerator_count": "0",
            "execution.cluster": "none",
            "k8s.namespace.name": "none",
            "execution.pending_reason": "none",
        }


class TestCloudProviderLabel:
    """The platform label is resolved through the registered map, never labelled raw."""

    @staticmethod
    def _targets() -> dict[str, execution_annotations.Target]:
        return {
            "lt3": execution_annotations.Target(provider="google", cluster="lt3"),
            "gke-lt3": execution_annotations.Target(provider="google", cluster="lt3"),
            "h200": execution_annotations.Target(provider="nebius", cluster="h200"),
        }

    @pytest.fixture(autouse=True)
    def _registered(self):
        execution_annotations.register_targets(targets=self._targets())
        yield
        execution_annotations.register_targets(targets={})

    @staticmethod
    def _record(session: orm.Session, **annotations: str) -> dict[str, str]:
        node = bts.ExecutionNode(task_spec={"annotations": annotations})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()
        return mock_record.call_args.kwargs["attributes"]

    @pytest.mark.parametrize(
        "annotation_value,expected",
        [
            ("lt3", "google"),
            ("gke-lt3", "google"),
            ("h200", "nebius"),
            ("retired-cluster", "none"),
        ],
        ids=["canonical", "alias", "other-provider", "unrouted"],
    )
    def test_the_map_decides_the_provider(
        self, session: orm.Session, annotation_value: str, expected: str
    ) -> None:
        attributes = self._record(
            session,
            **{execution_annotations.CLOUD_PROVIDER_KEY: annotation_value},
        )
        assert attributes["execution.cloud_provider"] == expected

    def test_an_alias_is_not_counted_as_its_own_platform(
        self, session: orm.Session
    ) -> None:
        """Two spellings of one cluster must not split the series."""
        by_key = self._record(
            session, **{execution_annotations.CLOUD_PROVIDER_KEY: "lt3"}
        )
        by_alias = self._record(
            session, **{execution_annotations.CLOUD_PROVIDER_KEY: "gke-lt3"}
        )
        assert (
            by_key["execution.cloud_provider"] == by_alias["execution.cloud_provider"]
        )

    def test_a_task_naming_no_platform_is_labelled_not_omitted(
        self, session: orm.Session
    ) -> None:
        """An absent attribute forms a second series no filter on the first one covers."""
        attributes = self._record(session)
        assert attributes["execution.cloud_provider"] == "none"


class TestNamespaceLabel:
    """Two origins: a task's own service account, else the cluster's namespace."""

    PROVIDER = execution_annotations.CLOUD_PROVIDER_KEY
    GSA = execution_annotations.GOOGLE_SERVICE_ACCOUNT_KEY
    CREATED_BY = common_annotations.PIPELINE_RUN_CREATED_BY_ANNOTATION_KEY
    SIDEKICK = "sidekick-oasis-runner@s5y-sidekick-52ee.iam.gserviceaccount.com"

    @staticmethod
    def _record(session: orm.Session, **annotations: str) -> dict[str, str]:
        node = bts.ExecutionNode(task_spec={"annotations": annotations})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()
        return mock_record.call_args.kwargs["attributes"]

    @pytest.fixture(autouse=True)
    def _registered(self):
        execution_annotations.register_targets(
            targets={
                "lt3": execution_annotations.Target(
                    "google", "us-ce1-lt3", "kueue-jobs-staging"
                ),
                "eo9": execution_annotations.Target(
                    "google", "us-ce1-eo9", "tangle-jobs-production"
                ),
            }
        )
        yield
        execution_annotations.register_targets(targets={})

    def test_a_service_account_gets_a_namespace_of_its_own(
        self, session: orm.Session
    ) -> None:
        attributes = self._record(
            session,
            **{
                self.PROVIDER: "lt3",
                self.GSA: self.SIDEKICK,
                self.CREATED_BY: "someone@shopify.com",
            },
        )
        assert attributes["k8s.namespace.name"] == (
            "sidekick-oasis-runner-at-s5y-sidekick-52ee"
        )

    @pytest.mark.parametrize(
        "target,expected",
        [("lt3", "kueue-jobs-staging"), ("eo9", "tangle-jobs-production")],
        ids=["default-cluster", "second-cluster"],
    )
    def test_without_one_the_cluster_namespace_is_used(
        self, session: orm.Session, target: str, expected: str
    ) -> None:
        attributes = self._record(session, **{self.PROVIDER: target})
        assert attributes["k8s.namespace.name"] == expected

    def test_a_service_account_without_a_creator_falls_back(
        self, session: orm.Session
    ) -> None:
        """The pod builder only makes a per-account namespace when it has both."""
        attributes = self._record(
            session, **{self.PROVIDER: "lt3", self.GSA: self.SIDEKICK}
        )
        assert attributes["k8s.namespace.name"] == "kueue-jobs-staging"

    def test_an_unrouted_target_is_labelled_not_omitted(
        self, session: orm.Session
    ) -> None:
        attributes = self._record(session, **{self.PROVIDER: "retired-tpu"})
        assert attributes["k8s.namespace.name"] == "none"

    def test_no_annotation_at_all_is_labelled_not_omitted(
        self, session: orm.Session
    ) -> None:
        assert self._record(session)["k8s.namespace.name"] == "none"


class TestClusterLabel:
    """The cluster comes from the registered map, never off the annotation."""

    PROVIDER = execution_annotations.CLOUD_PROVIDER_KEY
    DAVIES_CLUSTER = "example.test/byo/cluster"

    @staticmethod
    def _record(session: orm.Session, **annotations: str) -> dict[str, str]:
        node = bts.ExecutionNode(task_spec={"annotations": annotations})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()
        return mock_record.call_args.kwargs["attributes"]

    @pytest.fixture(autouse=True)
    def _registered(self):
        execution_annotations.register_targets(
            targets={
                "lt3": execution_annotations.Target("google", "us-ce1-lt3"),
                "gke-lt3": execution_annotations.Target("google", "us-ce1-lt3"),
                "eo9": execution_annotations.Target("google", "us-ce1-eo9"),
                "h200": execution_annotations.Target("nebius", "b300-cluster"),
                "byo": execution_annotations.Target(
                    "byo", "byo", cluster_from_annotation=self.DAVIES_CLUSTER
                ),
            }
        )
        yield
        execution_annotations.register_targets(targets={})

    @pytest.mark.parametrize(
        "annotation_value,expected",
        [
            ("lt3", "us-ce1-lt3"),
            ("gke-lt3", "us-ce1-lt3"),
            ("eo9", "us-ce1-eo9"),
            ("h200", "b300-cluster"),
            ("retired-tpu", "none"),
        ],
        ids=["default", "alias", "second-gke", "nebius", "unrouted"],
    )
    def test_the_map_decides_the_cluster(
        self, session: orm.Session, annotation_value: str, expected: str
    ) -> None:
        attributes = self._record(session, **{self.PROVIDER: annotation_value})
        assert attributes["execution.cluster"] == expected

    def test_the_annotation_is_a_target_not_a_cluster(
        self, session: orm.Session
    ) -> None:
        """ "lt3" names a target; the cluster it resolves to is spelled differently."""
        attributes = self._record(session, **{self.PROVIDER: "lt3"})
        assert attributes["execution.cluster"] != "lt3"
        assert attributes["execution.cluster"] == "us-ce1-lt3"

    def test_a_provider_with_no_clusters_takes_the_one_the_task_named(
        self, session: orm.Session
    ) -> None:
        attributes = self._record(
            session, **{self.PROVIDER: "byo", self.DAVIES_CLUSTER: "ml-control-plane"}
        )
        assert attributes["execution.cluster"] == "ml-control-plane"

    def test_that_provider_naming_no_cluster_is_labelled_not_omitted(
        self, session: orm.Session
    ) -> None:
        attributes = self._record(session, **{self.PROVIDER: "byo"})
        assert attributes["execution.cluster"] == "none"

    def test_no_annotation_is_labelled_not_omitted(self, session: orm.Session) -> None:
        assert self._record(session)["execution.cluster"] == "none"


class TestAcceleratorLabels:
    """One GPU written several ways must be one series, not several."""

    ACCEL = execution_annotations.RESOURCES_ACCELERATORS_KEY

    @staticmethod
    def _record(session: orm.Session, **annotations: str) -> dict[str, str]:
        node = bts.ExecutionNode(task_spec={"annotations": annotations})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()
        return mock_record.call_args.kwargs["attributes"]

    @pytest.mark.parametrize(
        "raw,expected_type,expected_count",
        [
            ('{"NVIDIA-B300-SXM6-PC":"8"}', "NVIDIA-B300-SXM6-PC", "8"),
            ('{"NVIDIA-B300-SXM6-PC": "8"}', "NVIDIA-B300-SXM6-PC", "8"),
            ('{"B300-SXM6-PC":"1"}', "B300-SXM6-PC", "1"),
            ('{"NVIDIA-B300-SXM6-PC":"0"}', "NVIDIA-B300-SXM6-PC", "0"),
            ("{}", "none", "0"),
            ('{"A":"1","B":"1"}', "unparseable", "0"),
            ("not json", "unparseable", "0"),
        ],
        ids=[
            "compact",
            "spaced",
            "second-spelling",
            "explicit-zero",
            "empty-object",
            "two-models",
            "malformed",
        ],
    )
    def test_the_annotation_is_parsed_not_copied(
        self,
        session: orm.Session,
        raw: str,
        expected_type: str,
        expected_count: str,
    ) -> None:
        attributes = self._record(session, **{self.ACCEL: raw})
        assert attributes["execution.accelerator_type"] == expected_type
        assert attributes["execution.accelerator_count"] == expected_count

    def test_one_space_does_not_make_a_second_series(
        self, session: orm.Session
    ) -> None:
        """json.dumps emits the space and a hand-typed annotation does not."""
        compact = self._record(session, **{self.ACCEL: '{"NVIDIA-H200":"4"}'})
        spaced = self._record(session, **{self.ACCEL: '{"NVIDIA-H200": "4"}'})
        assert compact["execution.accelerator_type"] == (
            spaced["execution.accelerator_type"]
        )
        assert compact["execution.accelerator_count"] == (
            spaced["execution.accelerator_count"]
        )

    def test_a_task_asking_for_no_gpu_is_labelled_not_omitted(
        self, session: orm.Session
    ) -> None:
        attributes = self._record(session)
        assert attributes["execution.accelerator_type"] == "none"
        assert attributes["execution.accelerator_count"] == "0"

    def test_a_malformed_annotation_does_not_lose_the_measurement(
        self, session: orm.Session
    ) -> None:
        """The duration still records; only the accelerator labels degrade."""
        attributes = self._record(session, **{self.ACCEL: "not json"})
        assert attributes["execution.status.to"] == (
            bts.ContainerExecutionStatus.RUNNING
        )
        assert attributes["execution.accelerator_type"] == "unparseable"


class TestRunAttempt:
    """A preempted execution reaches RUNNING more than once; only the first is a start."""

    @staticmethod
    def _history(*statuses: str) -> list[dict[str, str]]:
        return [
            {"status": s, "first_observed_at": "2024-01-01T00:00:00+00:00"}
            for s in statuses
        ]

    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ((), "none"),
            (("QUEUED",), "none"),
            (("QUEUED", "PENDING"), "none"),
            (("QUEUED", "PENDING", "RUNNING"), "first"),
            (("QUEUED", "PENDING", "RUNNING", "PENDING"), "none"),
            (("QUEUED", "PENDING", "RUNNING", "PENDING", "RUNNING"), "repeat"),
            (
                (
                    "QUEUED",
                    "PENDING",
                    "RUNNING",
                    "PENDING",
                    "RUNNING",
                    "PENDING",
                    "RUNNING",
                ),
                "repeat",
            ),
            (("QUEUED", "PENDING", "RUNNING", "SUCCEEDED"), "none"),
        ],
    )
    def test_run_attempt_for_history(
        self, statuses: tuple[str, ...], expected: str
    ) -> None:
        assert metrics._run_attempt(history=self._history(*statuses)) == expected

    def test_resumed_execution_is_labelled_repeat_end_to_end(
        self, session: orm.Session
    ) -> None:
        """The preemption walk: three PENDING->RUNNING samples, first then repeat twice."""
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        attempts = []
        for status in [
            bts.ContainerExecutionStatus.QUEUED,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
        ]:
            node.container_execution_status = status
            with unittest.mock.patch.object(
                metrics.execution_status_transition_duration, "record"
            ) as mock_record:
                session.commit()
            for call in mock_record.call_args_list:
                attrs = call.kwargs["attributes"]
                if attrs["execution.status.to"] == bts.ContainerExecutionStatus.RUNNING:
                    attempts.append(attrs["execution.run_attempt"])

        assert attempts == ["first", "repeat", "repeat"]


class TestSlowTransitionCounter:
    """The slow-path counter, and the rule that the commit path issues no query."""

    SLOW = datetime.timedelta(seconds=metrics._SLOW_TRANSITION_SECONDS + 1)
    FAST = datetime.timedelta(seconds=metrics._SLOW_TRANSITION_SECONDS - 1)

    @staticmethod
    def _transition(
        session: orm.Session,
        *,
        gap: datetime.timedelta,
        status_from: bts.ContainerExecutionStatus = (
            bts.ContainerExecutionStatus.QUEUED
        ),
        status_to: bts.ContainerExecutionStatus = (
            bts.ContainerExecutionStatus.RUNNING
        ),
    ) -> tuple[bts.ExecutionNode, list]:
        """Move a node between two statuses with a chosen gap, returning the counter calls."""
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = status_from
        session.commit()

        history = node.extra_data[metrics._HISTORY_KEY]
        earlier = (
            datetime.datetime.fromisoformat(history[-1]["first_observed_at"]) - gap
        )
        history[-1]["first_observed_at"] = earlier.isoformat()
        orm.attributes.flag_modified(node, "extra_data")
        session.commit()

        node.container_execution_status = status_to
        with unittest.mock.patch.object(
            metrics.execution_slow_transitions, "add"
        ) as mock_add:
            session.commit()
        return node, mock_add.call_args_list

    def test_a_slow_transition_is_counted(self, session: orm.Session) -> None:
        _, calls = self._transition(session, gap=self.SLOW)
        assert len(calls) == 1
        assert calls[0].kwargs["attributes"]["execution.status.from"] == "QUEUED"
        assert calls[0].kwargs["attributes"]["execution.status.to"] == "RUNNING"

    def test_a_fast_transition_is_not(self, session: orm.Session) -> None:
        assert self._transition(session, gap=self.FAST)[1] == []

    @pytest.mark.parametrize(
        "status_to",
        [
            bts.ContainerExecutionStatus.SUCCEEDED,
            bts.ContainerExecutionStatus.FAILED,
        ],
        ids=["succeeded", "failed"],
    )
    def test_a_long_run_is_not_a_long_wait(
        self, session: orm.Session, status_to: bts.ContainerExecutionStatus
    ) -> None:
        """Once a task is RUNNING the clock measures work, not queueing."""
        _, calls = self._transition(
            session,
            gap=self.SLOW,
            status_from=bts.ContainerExecutionStatus.RUNNING,
            status_to=status_to,
        )
        assert calls == []

    def test_a_long_wait_that_never_ran_is_counted(self, session: orm.Session) -> None:
        """Leaving PENDING for FAILED still means the submitter waited."""
        _, calls = self._transition(
            session,
            gap=self.SLOW,
            status_from=bts.ContainerExecutionStatus.PENDING,
            status_to=bts.ContainerExecutionStatus.FAILED,
        )
        assert len(calls) == 1
        assert calls[0].kwargs["attributes"]["execution.status.from"] == "PENDING"

    def test_the_owner_is_the_runs_creator(self, session: orm.Session) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(
            bts.PipelineRun(root_execution=node, created_by="alice@example.com")
        )
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()
        history = node.extra_data[metrics._HISTORY_KEY]
        history[-1]["first_observed_at"] = (
            datetime.datetime.fromisoformat(history[-1]["first_observed_at"])
            - self.SLOW
        ).isoformat()
        orm.attributes.flag_modified(node, "extra_data")
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_slow_transitions, "add"
        ) as mock_add:
            session.commit()
        assert mock_add.call_args.kwargs["attributes"]["owner"] == "alice@example.com"

    def test_a_node_under_a_run_resolves_through_the_ancestor_link(
        self, session: orm.Session
    ) -> None:
        """An arbitrary node has no foreign key to its run; only the root does."""
        root = bts.ExecutionNode(task_spec={})
        child = bts.ExecutionNode(task_spec={})
        session.add(bts.PipelineRun(root_execution=root, created_by="bob@example.com"))
        session.add(
            bts.ExecutionToAncestorExecutionLink(
                ancestor_execution=root, execution=child
            )
        )
        session.commit()

        child.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()
        history = child.extra_data[metrics._HISTORY_KEY]
        history[-1]["first_observed_at"] = (
            datetime.datetime.fromisoformat(history[-1]["first_observed_at"])
            - self.SLOW
        ).isoformat()
        orm.attributes.flag_modified(child, "extra_data")
        session.commit()

        child.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_slow_transitions, "add"
        ) as mock_add:
            session.commit()
        assert mock_add.call_args.kwargs["attributes"]["owner"] == "bob@example.com"

    def test_a_node_with_no_run_is_labelled_not_dropped(
        self, session: orm.Session
    ) -> None:
        _, calls = self._transition(session, gap=self.SLOW)
        assert calls[0].kwargs["attributes"]["owner"] == "none"

    def test_a_failed_owner_read_still_counts_the_point(
        self, session: orm.Session
    ) -> None:
        """An alert fires on this counter, so a point missing a label beats no point."""
        with unittest.mock.patch.object(
            metrics, "_owners_of", side_effect=RuntimeError("database is gone")
        ):
            _, calls = self._transition(session, gap=self.SLOW)
        assert len(calls) == 1
        assert calls[0].kwargs["attributes"]["owner"] == "none"

    def test_the_commit_path_issues_no_query(self, session: orm.Session) -> None:
        """before_commit runs inside the flush; a SELECT there re-enters it."""
        statements: list[str] = []

        @sql_event.listens_for(session.get_bind(), "before_cursor_execute")
        def _record(conn, cursor, statement, *args):  # noqa: ANN001, ANN202
            statements.append(statement)

        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()
        history = node.extra_data[metrics._HISTORY_KEY]
        history[-1]["first_observed_at"] = (
            datetime.datetime.fromisoformat(history[-1]["first_observed_at"])
            - self.SLOW
        ).isoformat()
        orm.attributes.flag_modified(node, "extra_data")
        session.commit()

        # Everything the commit path ran before the owner lookup, which is the first
        # thing that happens once the transaction has closed.
        issued_before_the_owner_read: list[str] = []

        def _snapshot(**_kwargs: object) -> dict:
            issued_before_the_owner_read.extend(statements)
            return {}

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        statements.clear()
        with unittest.mock.patch.object(
            metrics, "_owners_of", side_effect=_snapshot
        ) as mock_owners:
            session.commit()

        mock_owners.assert_called_once()
        assert (
            issued_before_the_owner_read
        ), "the status change itself should be a write"
        assert not [
            s
            for s in issued_before_the_owner_read
            if s.lstrip().upper().startswith("SELECT")
        ]


class TestPendingReason:
    """Why a task is waiting, stamped while it waits and read when it stops."""

    @staticmethod
    def _launcher_data(*, phase: str, reason: str | None, has_container: bool = True):
        """The shape the Kubernetes launcher serialises onto launcher_data."""
        status: dict = {"phase": phase}
        if has_container:
            state = {"waiting": {"reason": reason}} if reason else {}
            status["containerStatuses"] = [{"state": state}]
        return {"kubernetes_job": {"debug_pods": {"0": {"status": status}}}}

    @pytest.mark.parametrize(
        "reason, expected",
        [
            ("ContainerCreating", "starting_container"),
            ("PodInitializing", "starting_container"),
            ("ImagePullBackOff", "image_unavailable"),
            ("ErrImagePull", "image_unavailable"),
            ("CreateContainerConfigError", "bad_config"),
            ("InvalidImageName", "bad_config"),
        ],
    )
    def test_a_kubernetes_reason_becomes_one_of_ours(self, reason, expected):
        data = self._launcher_data(phase="Pending", reason=reason)
        assert execution_annotations.pending_reason(launcher_data=data) == expected

    def test_an_unknown_kubernetes_reason_does_not_add_a_label_value(self):
        data = self._launcher_data(phase="Pending", reason="SomeFutureK8sReason")
        assert execution_annotations.pending_reason(launcher_data=data) == "other"

    def test_a_pod_with_no_containers_yet_is_waiting_for_a_node(self):
        data = self._launcher_data(phase="Pending", reason=None, has_container=False)
        assert execution_annotations.pending_reason(launcher_data=data) == (
            "waiting_for_node"
        )

    def test_a_running_pod_is_not_waiting_for_anything(self):
        data = self._launcher_data(phase="Running", reason=None)
        assert execution_annotations.pending_reason(launcher_data=data) is None

    def test_a_launcher_that_is_not_kubernetes_yields_no_reason(self):
        assert execution_annotations.pending_reason(launcher_data={"skypilot": {}}) is (
            None
        )

    def test_a_poll_stamps_the_reason_onto_the_container_execution(
        self, session: orm.Session
    ):
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        session.add(container_execution)
        container_execution.launcher_data = self._launcher_data(
            phase="Pending", reason="ImagePullBackOff"
        )
        session.commit()
        assert container_execution.extra_data == {
            execution_annotations.PENDING_REASON_KEY: "image_unavailable"
        }

    def test_what_lands_in_the_json_column_is_a_plain_string(
        self, session: orm.Session
    ):
        """A str-valued enum passes == either way, so pin the stored type itself."""
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        session.add(container_execution)
        container_execution.launcher_data = self._launcher_data(
            phase="Pending", reason="ImagePullBackOff"
        )
        # Before the commit: afterwards SQLite round-trips the JSON and hands back a
        # plain string whatever was put in, which would hide the difference.
        stored = container_execution.extra_data[
            execution_annotations.PENDING_REASON_KEY
        ]
        assert type(stored) is str

    def test_the_reason_survives_the_pod_starting_to_run(self, session: orm.Session):
        """The flip is exactly when the pod no longer says why it was waiting."""
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        session.add(container_execution)
        container_execution.launcher_data = self._launcher_data(
            phase="Pending", reason="ImagePullBackOff"
        )
        session.commit()
        container_execution.launcher_data = self._launcher_data(
            phase="Running", reason=None
        )
        session.commit()
        assert container_execution.extra_data == {
            execution_annotations.PENDING_REASON_KEY: "image_unavailable"
        }

    def test_a_node_reads_the_reason_off_its_container_execution(
        self, session: orm.Session
    ):
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        node = bts.ExecutionNode(task_spec={})
        node.container_execution = container_execution
        session.add(node)
        container_execution.launcher_data = self._launcher_data(
            phase="Pending", reason="CreateContainerConfigError"
        )
        session.commit()
        # The orchestrator holds both objects when it flips a status, which is the only
        # time this reader runs; commit expires them, so reload as that caller would.
        node.container_execution
        assert metrics._pending_reason(execution=node) == "bad_config"

    def test_a_reason_behind_a_lazy_load_is_skipped_rather_than_fetched(
        self, session: orm.Session
    ):
        """A SELECT here would land inside the flush that before_commit runs in."""
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        node = bts.ExecutionNode(task_spec={})
        node.container_execution = container_execution
        session.add(node)
        container_execution.launcher_data = self._launcher_data(
            phase="Pending", reason="ImagePullBackOff"
        )
        session.commit()
        session.expire(node)
        assert metrics._pending_reason(execution=node) == "none"

    def test_a_node_with_no_container_execution_is_unlabelled(
        self, session: orm.Session
    ):
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        session.commit()
        assert metrics._pending_reason(execution=node) == "none"


class TestPendingReasonOnTheInstruments:
    """The reason rides both instruments, so an alert and its link agree."""

    @staticmethod
    def _waiting(reason: str) -> dict:
        return {
            "kubernetes_job": {
                "debug_pods": {
                    "0": {
                        "status": {
                            "phase": "Pending",
                            "containerStatuses": [
                                {"state": {"waiting": {"reason": reason}}}
                            ],
                        }
                    }
                }
            }
        }

    def _wait_then_run(self, session: orm.Session, *, gap: datetime.timedelta):
        """A task that waits on an image pull, then starts -- the case T5 exists for."""
        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING
        )
        node = bts.ExecutionNode(task_spec={})
        node.container_execution = container_execution
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.PENDING
        container_execution.launcher_data = self._waiting("ImagePullBackOff")
        session.commit()
        node.container_execution
        history = node.extra_data[bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY]
        history[-1]["first_observed_at"] = (
            datetime.datetime.now(datetime.timezone.utc) - gap
        ).isoformat()
        node.extra_data = {**node.extra_data}
        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        return node

    def test_the_histogram_carries_why_the_task_was_waiting(self, session: orm.Session):
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as record:
            self._wait_then_run(session, gap=datetime.timedelta(minutes=3))
            session.commit()
        attributes = record.call_args.kwargs["attributes"]
        assert attributes["execution.pending_reason"] == "image_unavailable"

    def test_the_slow_counter_carries_why_the_task_was_waiting(
        self, session: orm.Session
    ):
        with unittest.mock.patch.object(
            metrics.execution_slow_transitions, "add"
        ) as add:
            self._wait_then_run(session, gap=datetime.timedelta(hours=4))
            session.commit()
        attributes = add.call_args.kwargs["attributes"]
        assert attributes["execution.pending_reason"] == "image_unavailable"
        assert attributes["execution.status.to"] == "RUNNING"


class TestPendingReasonWhenNoPodExists:
    """A Job whose pods were refused at admission, and the first write of any task."""

    @staticmethod
    def _no_pods() -> dict:
        """What the launcher serialises for a Job that has produced no pod."""
        return {"kubernetes_job": {"debug_pods": {}}}

    def test_a_job_with_no_pods_is_not_silent(self):
        assert (
            execution_annotations.pending_reason(launcher_data=self._no_pods())
            == "no_pod_created"
        )

    def test_a_launcher_that_is_not_kubernetes_says_nothing(self):
        # Without the explicit None check this would fall through to an empty pod map
        # and label every local-process task as a refused Kubernetes pod.
        for data in ({}, {"local_process": {"pid": 42}}):
            assert execution_annotations.pending_reason(launcher_data=data) is None

    def test_a_pod_that_exists_but_is_not_pending_is_still_none(self):
        data = {
            "kubernetes_job": {"debug_pods": {"0": {"status": {"phase": "Running"}}}}
        }
        assert execution_annotations.pending_reason(launcher_data=data) is None

    def test_the_set_listener_alone_would_lose_the_constructor_reason(self):
        # Pins why there are two listeners. ContainerExecution declares extra_data
        # after launcher_data, so __init__ assigns the reason and then overwrites it
        # with extra_data's own default of None. Nothing is flushed here, so only the
        # attribute listener has run. Move extra_data above launcher_data and this
        # fails -- which is the warning that the insert hook has become dead code.
        execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING,
            launcher_data=self._no_pods(),
        )
        assert execution.launcher_data is not None
        assert execution.extra_data is None

    def test_the_reason_supplied_to_the_constructor_is_persisted(
        self, session: orm.Session
    ):
        # The orchestrator passes the first launcher_data as a constructor argument,
        # which is not an attribute assignment and never reaches the "set" listener.
        execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING,
            launcher_data=self._no_pods(),
        )
        session.add(execution)
        session.commit()
        execution_id = execution.id
        session.expunge_all()

        reloaded = session.get(bts.ContainerExecution, execution_id)
        assert reloaded is not None
        assert reloaded.extra_data == {
            execution_annotations.PENDING_REASON_KEY: "no_pod_created"
        }

    def test_a_later_poll_overwrites_the_reason_from_the_first_write(
        self, session: orm.Session
    ):
        # Every task is launched with an empty pod map, so a healthy one must not stay
        # labelled as refused once its pod shows up.
        execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING,
            launcher_data=self._no_pods(),
        )
        session.add(execution)
        session.commit()
        assert execution.extra_data == {
            execution_annotations.PENDING_REASON_KEY: "no_pod_created"
        }

        execution.launcher_data = {
            "kubernetes_job": {
                "debug_pods": {
                    "0": {
                        "status": {
                            "phase": "Pending",
                            "containerStatuses": [
                                {"state": {"waiting": {"reason": "ContainerCreating"}}}
                            ],
                        }
                    }
                }
            }
        }
        session.commit()
        assert execution.extra_data == {
            execution_annotations.PENDING_REASON_KEY: "starting_container"
        }
