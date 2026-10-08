import datetime

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.access_control import checks
from cloud_pipelines_backend.access_control import edits
from cloud_pipelines_backend.access_control import runs
from cloud_pipelines_backend.access_control import scopes

_NOW = datetime.datetime(2026, 10, 7, tzinfo=datetime.timezone.utc)

_PIPELINE = scopes.ResourceType(
    name="pipeline",
    operate=frozenset(runs.RunScope),
    edit=frozenset({"pipeline:update"}),
    manage=frozenset({"pipeline:delete", "pipeline:share"}),
)


def _granted(*permissions: str) -> dict:
    return {"users": {"sam@example.com": {"permissions": list(permissions)}}}


class TestEffectiveScopes:
    @pytest.mark.parametrize(
        ("permissions", "expected"),
        [
            (["operate"], set(runs.RunScope)),
            (["edit"], set(runs.RunScope) | {"pipeline:update"}),
            (["manage"], _PIPELINE.fine_scopes),
            (["run:cancel"], {"run:cancel"}),
            (["made:up"], set()),
        ],
    )
    def test_expands_permissions(self, permissions, expected) -> None:
        granted = checks.effective_scopes(
            resource_type=_PIPELINE,
            owner="ada@example.com",
            access_control=_granted(*permissions),
            caller=checks.Caller(email=" Sam@Example.com "),
        )
        assert granted == expected

    @pytest.mark.parametrize(
        "caller",
        [
            checks.Caller(email="ADA@example.com"),
            checks.Caller(email="other@example.com", is_admin=True),
        ],
    )
    def test_owner_and_admin_hold_manage(self, caller) -> None:
        granted = checks.effective_scopes(
            resource_type=_PIPELINE,
            owner="ada@example.com",
            access_control=None,
            caller=caller,
        )
        assert granted == _PIPELINE.fine_scopes

    def test_require_raises_permission_error(self) -> None:
        with pytest.raises(errors.PermissionError):
            checks.require(
                scope="pipeline:update",
                resource_type=_PIPELINE,
                resource_id="p1",
                owner="ada@example.com",
                access_control=_granted("operate"),
                caller=checks.Caller(email="sam@example.com"),
            )


class TestApplyUsersEdit:
    def _edit(self, access_control, users, *, expected_revision=None) -> dict:
        return edits.apply_users_edit(
            resource_type=_PIPELINE,
            owner="ada@example.com",
            access_control=access_control,
            users=users,
            changed_by="Ada@example.com",
            changed_at=_NOW,
            expected_revision=(
                edits.latest_revision(access_control)
                if expected_revision is None
                else expected_revision
            ),
        )

    def test_history_replays_to_users(self) -> None:
        access_control = self._edit(
            None, {"Sam@example.com": {"permissions": ["operate"]}}
        )
        access_control = self._edit(
            access_control,
            {
                "sam@example.com": {"permissions": ["edit"]},
                "kai@example.com": {"permissions": ["run:cancel"]},
            },
        )
        access_control = self._edit(
            access_control, {"kai@example.com": {"permissions": ["run:cancel"]}}
        )
        history = access_control["history"]
        assert [entry["revision"] for entry in history] == [1, 2, 3]
        assert history[0]["changes"] == {
            "sam@example.com": {"from": [], "to": ["operate"]}
        }
        assert history[2]["changes"] == {
            "sam@example.com": {"from": ["edit"], "to": []}
        }
        assert history[0]["changed_by"] == "ada@example.com"
        assert edits.replay(history) == access_control["users"]

    def test_no_change_appends_nothing(self) -> None:
        access_control = self._edit(
            None, {"sam@example.com": {"permissions": ["edit"]}}
        )
        again = self._edit(
            access_control, {"sam@example.com": {"permissions": ["edit"]}}
        )
        assert again["history"] == access_control["history"]

    def test_stale_revision_is_rejected(self) -> None:
        access_control = self._edit(
            None, {"sam@example.com": {"permissions": ["edit"]}}
        )
        with pytest.raises(edits.StaleRevisionError):
            self._edit(access_control, {}, expected_revision=0)

    @pytest.mark.parametrize(
        "users",
        [
            {"sam@example.com": {"permissions": ["made:up"]}},
            {"ada@example.com": {"permissions": ["edit"]}},
            {"Sam@example.com": {"permissions": ["edit"]}, "sam@example.com ": {}},
            {" ": {"permissions": ["edit"]}},
        ],
    )
    def test_invalid_users_are_rejected(self, users) -> None:
        with pytest.raises(errors.ApiValidationError):
            self._edit(None, users)


@pytest.fixture()
def session_factory():
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


@pytest.fixture()
def shared_with_sam(monkeypatch):
    def _resolver(session, pipeline_run):
        yield runs.RunParent(
            resource_type=_PIPELINE,
            owner="ada@example.com",
            access_control=_granted("run:cancel"),
        )

    monkeypatch.setattr(runs, "_parent_resolvers", [_resolver])


class TestRunScopes:
    def _run(self, session_factory) -> api_server_sql.PipelineRunResponse:
        root_task = structures.TaskSpec(
            component_ref=structures.ComponentReference(
                spec=structures.ComponentSpec(
                    name="p",
                    implementation=structures.GraphImplementation(
                        graph=structures.GraphSpec(tasks={})
                    ),
                )
            )
        )
        with session_factory() as session:
            return api_server_sql.PipelineRunsApiService_Sql().create(
                session, root_task=root_task, created_by="ada@example.com"
            )

    def test_inherited_cancel(self, session_factory, shared_with_sam) -> None:
        run = self._run(session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            service.terminate(session, run.id, terminated_by="sam@example.com")
        with session_factory() as session:
            assert (
                session.get(bts.PipelineRun, run.id).extra_data["desired_state"]
                == "TERMINATED"
            )

    def test_cancel_does_not_grant_annotate(
        self, session_factory, shared_with_sam
    ) -> None:
        run = self._run(session_factory)
        with session_factory() as session, pytest.raises(errors.PermissionError):
            api_server_sql.PipelineRunsApiService_Sql().set_annotation(
                session=session,
                id=run.id,
                key="k",
                value="v",
                user_name="sam@example.com",
            )

    def test_stranger_cannot_cancel(self, session_factory) -> None:
        run = self._run(session_factory)
        with session_factory() as session, pytest.raises(errors.PermissionError):
            api_server_sql.PipelineRunsApiService_Sql().terminate(
                session, run.id, terminated_by="kai@example.com"
            )
