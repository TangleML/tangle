"""Tests for SQLAlchemy event listeners in orchestrator_sql and instrumentation.metrics."""

import unittest.mock

import pytest
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
