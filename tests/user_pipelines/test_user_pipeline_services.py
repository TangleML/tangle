import ast
import concurrent.futures
import datetime
import inspect
import textwrap
import threading
from unittest import mock

import pytest
import sqlalchemy
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.user_pipelines import (
    database_ops as pipeline_database_ops,
)
from cloud_pipelines_backend.user_pipelines import db_models, services
from cloud_pipelines_backend.user_pipelines import errors as pipeline_errors
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

from tests import sql_capture
from tests.user_pipelines.conftest import pipeline_task


def test_insert_fallback_preserves_unrelated_integrity_error(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session, session.begin():
        error = pipeline_database_ops.insert_with_integrity_fallback(
            session=session,
            table=db_models.UserPipelineVersion.__table__,
            values={
                "pipeline_id": "00000000-0000-0000-0000-000000000000",
                "version_key": "d" * 64,
                "content_digest": "d" * 64,
                "root_pipeline_task": {},
            },
        )

        assert isinstance(error, sqlalchemy.exc.IntegrityError)
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 0
        )


def test_first_write_uses_transient_model_candidate_for_id(
    db_engine: sqlalchemy.Engine,
    monkeypatch,
) -> None:
    candidates: list[db_models.UserPipeline] = []
    original_init = db_models.UserPipeline.__init__

    def track_candidate(self, *args, **kwargs) -> None:
        original_init(self, *args, **kwargs)
        candidates.append(self)

    monkeypatch.setattr(db_models.UserPipeline, "__init__", track_candidate)

    with orm.Session(db_engine) as session:
        result = services.UserPipelineService().set_pipeline(
            session=session,
            user_id="owner@example.com",
            file_path="pipelines/model-id.yaml",
            root_pipeline_task=pipeline_task(name="model-id"),
            pipeline_run_annotations=None,
        )
        persisted_id = result.pipeline.id

    assert len(candidates) == 1
    assert sqlalchemy.inspect(candidates[0]).transient
    assert persisted_id == candidates[0].id


def test_concurrent_first_writes_share_stable_identity(tmp_path) -> None:
    engine = database_ops.create_db_engine(
        database_uri=f"sqlite:///{tmp_path / 'pipelines.db'}"
    )
    bts._TableBase.metadata.create_all(engine)
    barrier = threading.Barrier(2)

    def write_pipeline() -> tuple[str, str]:
        with orm.Session(engine) as session:
            barrier.wait()
            result = services.UserPipelineService().set_pipeline(
                session=session,
                user_id="concurrent@example.com",
                file_path="pipelines/concurrent.yaml",
                root_pipeline_task=pipeline_task(name="concurrent"),
                pipeline_run_annotations={"source": "test"},
            )
            return result.pipeline.id, result.version.content_digest

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: write_pipeline(), range(2)))

    assert len({pipeline_id for pipeline_id, _ in results}) == 1
    assert len({digest for _, digest in results}) == 1
    with orm.Session(engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 1
        )
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 1
        )


def test_concurrent_omitted_mode_preserves_full_mode_winner(
    tmp_path,
    monkeypatch,
) -> None:
    engine = database_ops.create_db_engine(
        database_uri=f"sqlite:///{tmp_path / 'mode-race.db'}"
    )
    bts._TableBase.metadata.create_all(engine)
    omitted_at_insert = threading.Event()
    full_inserted = threading.Event()
    thread_context = threading.local()
    original_insert = pipeline_database_ops.insert_with_integrity_fallback

    def order_stable_inserts(*, session, table, values):
        if table is db_models.UserPipeline.__table__:
            if thread_context.mode is db_models.PipelineVersioningMode.FULL:
                assert omitted_at_insert.wait(timeout=5)
                result = original_insert(
                    session=session,
                    table=table,
                    values=values,
                )
                full_inserted.set()
                return result
            omitted_at_insert.set()
            assert full_inserted.wait(timeout=5)
        return original_insert(session=session, table=table, values=values)

    monkeypatch.setattr(
        pipeline_database_ops,
        "insert_with_integrity_fallback",
        order_stable_inserts,
    )

    def write_pipeline(
        mode: db_models.PipelineVersioningMode | None,
    ) -> services.PipelineWriteResult:
        thread_context.mode = mode
        with orm.Session(engine) as session:
            return services.UserPipelineService().set_pipeline(
                session=session,
                user_id="mode-race@example.com",
                file_path="pipelines/mode-race.yaml",
                root_pipeline_task=pipeline_task(name="mode-race"),
                pipeline_run_annotations=None,
                versioning_mode=mode,
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        full_future = executor.submit(
            write_pipeline,
            db_models.PipelineVersioningMode.FULL,
        )
        omitted_future = executor.submit(write_pipeline, None)
        full_future.result()
        omitted_future.result()

    with orm.Session(engine) as session:
        pipeline = session.scalar(sqlalchemy.select(db_models.UserPipeline))
        assert pipeline is not None
        assert pipeline.versioning_mode is db_models.PipelineVersioningMode.FULL
        assert pipeline.current_version_key != db_models.CURRENT_VERSION_KEY
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 1
        )


def test_property_patch_locks_owned_active_pipeline() -> None:
    session = mock.MagicMock(spec=orm.Session)
    session.scalar.return_value = None

    with pytest.raises(pipeline_errors.PipelineNotFoundError):
        services.UserPipelineService().patch_pipeline_properties(
            session=session,
            user_id="owner@example.com",
            pipeline_id="00000000-0000-0000-0000-000000000000",
            versioning_mode=db_models.PipelineVersioningMode.FULL,
        )

    query = session.scalar.call_args.args[0]
    assert query._for_update_arg is not None
    # Exclusive, not shared. Now that the pin-resolving reader takes `FOR SHARE`
    # on this same row, the writer being `FOR UPDATE` is what still makes the two
    # serialize -- two shared locks would not.
    assert query._for_update_arg.read is False
    query_text = str(query)
    assert "pipeline.user_id" in query_text
    assert "pipeline.deleted_at IS NULL" in query_text


# The pin resolver and the guard it depends on. Both exist so that another domain -- triggers,
# today -- can store a durable reference to a pipeline version without reimplementing either
# "is this pipeline mine and alive" or "which version_key does this digest mean".

_ALIVE_ID = "11111111-1111-4111-8111-111111111111"
_DELETED_ID = "22222222-2222-4222-8222-222222222222"
_VERSION_A = "a" * db_models.DIGEST_LENGTH
_VERSION_B = "b" * db_models.DIGEST_LENGTH


def _pipeline(
    *,
    session: orm.Session,
    pipeline_id: str,
    user_id: str,
    file_path: str,
    deleted_at=None,
) -> db_models.UserPipeline:
    pipeline = db_models.UserPipeline(
        user_id=user_id, file_path=file_path, deleted_at=deleted_at
    )
    pipeline.id = pipeline_id
    session.add(pipeline)
    session.flush()
    return pipeline


def _version(
    *,
    session: orm.Session,
    pipeline_id: str,
    version_key: str,
    content_digest: str,
) -> None:
    session.add(
        db_models.UserPipelineVersion(
            pipeline_id=pipeline_id,
            version_key=version_key,
            content_digest=content_digest,
            root_pipeline_task={},
        )
    )
    session.flush()


class TestLivePipelineIds:
    """The read-side counterpart of `get_live_owned_pipeline`.

    Different question, on purpose: a read route reporting whether a stored reference still
    resolves asks only "is it there", where the write guard also asks "is it yours". Batched,
    because the caller is a listing route holding a whole page of ids.
    """

    def test_a_live_pipeline_is_returned(self, db_engine: sqlalchemy.Engine) -> None:
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )

            assert services.live_pipeline_ids(
                session=session, pipeline_ids=[_ALIVE_ID]
            ) == {_ALIVE_ID}

    def test_a_soft_deleted_pipeline_is_absent(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The tombstone still satisfies every foreign key, which is why this cannot be
        inferred from the reference existing."""
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_DELETED_ID,
                user_id="alice",
                file_path="gone.py",
                deleted_at=db_utils.utc_now(),
            )

            assert (
                services.live_pipeline_ids(session=session, pipeline_ids=[_DELETED_ID])
                == set()
            )

    def test_a_mixed_batch_is_separated(self, db_engine: sqlalchemy.Engine) -> None:
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _pipeline(
                session=session,
                pipeline_id=_DELETED_ID,
                user_id="alice",
                file_path="gone.py",
                deleted_at=db_utils.utc_now(),
            )

            assert services.live_pipeline_ids(
                session=session, pipeline_ids=[_ALIVE_ID, _DELETED_ID]
            ) == {_ALIVE_ID}

    def test_somebody_elses_live_pipeline_is_still_live(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Ownership is not asked. Answering False for another tenant's pipeline would
        misreport a row whose target id is already in the same response."""
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="bob",
                file_path="b.py",
            )

            assert services.live_pipeline_ids(
                session=session, pipeline_ids=[_ALIVE_ID]
            ) == {_ALIVE_ID}

    def test_an_unknown_id_is_absent_rather_than_an_error(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(db_engine) as session, session.begin():
            assert (
                services.live_pipeline_ids(session=session, pipeline_ids=[_ALIVE_ID])
                == set()
            )

    def test_a_malformed_id_is_absent_rather_than_an_error(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A read route must not raise over a value a write route could never have stored;
        `normalize_pipeline_id` raises, so the id is dropped instead."""
        with orm.Session(db_engine) as session, session.begin():
            assert (
                services.live_pipeline_ids(session=session, pipeline_ids=["not-a-uuid"])
                == set()
            )

    def test_the_answer_is_keyed_by_the_spelling_that_was_asked(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Ids are normalized to compare, then spoken back as they arrived, so a caller can
        test membership against the value it already holds."""
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            shouted = _ALIVE_ID.replace("a", "A").upper()

            assert services.live_pipeline_ids(
                session=session, pipeline_ids=[shouted]
            ) == {shouted}

    def test_an_empty_batch_asks_the_database_nothing(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """An empty `IN ()` is a SQL error on some backends and a full scan on others; the
        listing route hits this on its last page."""
        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(  # type: ignore[no-untyped-def]
            conn, cursor, statement, parameters, context, executemany
        ) -> None:
            statements.append(statement)

        try:
            with orm.Session(db_engine) as session, session.begin():
                assert (
                    services.live_pipeline_ids(session=session, pipeline_ids=[])
                    == set()
                )
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        assert statements == []


class TestGetLiveOwnedPipeline:
    def test_returns_the_pipeline_when_it_is_alive_and_theirs(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            found = services.get_live_owned_pipeline(
                session=session, pipeline_id=_ALIVE_ID, user_id="alice"
            )
            assert found.id == _ALIVE_ID

    def test_a_soft_deleted_pipeline_is_not_found(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The guard's whole reason to exist: the foreign key cannot see this.

        A soft-deleted pipeline's row is physically present, so every REFERENCES pipeline(id)
        in the schema is satisfied by it. Without this check a subscription can be parked on a
        tombstone -- and `save` clears `deleted_at`, so whoever recreates that file path
        silently inherits it.
        """
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_DELETED_ID,
                user_id="alice",
                file_path="gone.py",
                deleted_at=db_utils.utc_now(),
            )
            with pytest.raises(pipeline_errors.PipelineNotFoundError):
                services.get_live_owned_pipeline(
                    session=session, pipeline_id=_DELETED_ID, user_id="alice"
                )

    def test_someone_elses_pipeline_is_not_found_rather_than_forbidden(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Not-found, not not-allowed: the distinction is an existence oracle."""
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            with pytest.raises(pipeline_errors.PipelineNotFoundError):
                services.get_live_owned_pipeline(
                    session=session, pipeline_id=_ALIVE_ID, user_id="bob"
                )

    def test_a_null_user_id_skips_the_ownership_filter(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The admin door, and deliberately not the soft-delete door -- see the next test."""
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            found = services.get_live_owned_pipeline(
                session=session, pipeline_id=_ALIVE_ID, user_id=None
            )
            assert found.id == _ALIVE_ID

    def test_a_null_user_id_still_refuses_a_soft_deleted_pipeline(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(db_engine) as session, session.begin():
            _pipeline(
                session=session,
                pipeline_id=_DELETED_ID,
                user_id="alice",
                file_path="gone.py",
                deleted_at=db_utils.utc_now(),
            )
            with pytest.raises(pipeline_errors.PipelineNotFoundError):
                services.get_live_owned_pipeline(
                    session=session, pipeline_id=_DELETED_ID, user_id=None
                )

    def test_an_id_that_is_not_a_uuid_is_a_validation_error(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(db_engine) as session, session.begin():
            with pytest.raises(pipeline_errors.PipelineValidationError):
                services.get_live_owned_pipeline(
                    session=session, pipeline_id="not-a-uuid", user_id="alice"
                )


class TestResolvePinnableVersionKey:
    def test_a_full_mode_version_resolves_to_its_key(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(db_engine) as session, session.begin():
            pipeline = _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=_VERSION_A,
                content_digest=_VERSION_A,
            )
            assert (
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_A,
                )
                == _VERSION_A
            )

    def test_a_disabled_mode_pipeline_offers_nothing_to_pin(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The mutable head is excluded even though it carries the digest asked for."""
        with orm.Session(db_engine) as session, session.begin():
            pipeline = _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=db_models.CURRENT_VERSION_KEY,
                content_digest=_VERSION_A,
            )
            with pytest.raises(pipeline_errors.VersionNotPinnableError):
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_A,
                )

    def test_an_unknown_version_is_distinguished_from_a_pipeline_without_history(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Two failures the caller can act on differently, so they are two error classes."""
        with orm.Session(db_engine) as session, session.begin():
            pipeline = _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=_VERSION_A,
                content_digest=_VERSION_A,
            )
            with pytest.raises(pipeline_errors.VersionNotFoundError) as caught:
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_B,
                )
            assert _VERSION_B in str(caught.value)

    def test_a_row_whose_key_is_not_its_digest_is_not_pinnable(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The resolver keys on `version_key`, which is a primary-key seek rather than a walk
        over one pipeline's history. No writer produces a row where the two differ; that
        invariant is guarded through the real API by
        `test_every_stored_version_keys_itself_by_its_digest`. This only documents which
        column the lookup trusts."""
        with orm.Session(db_engine) as session, session.begin():
            pipeline = _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=_VERSION_B,
                content_digest=_VERSION_A,
            )
            with pytest.raises(pipeline_errors.VersionNotFoundError):
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_A,
                )
            assert (
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_B,
                )
                == _VERSION_B
            )

    def test_the_immutable_row_wins_while_a_switch_is_mid_flight(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A DISABLED -> FULL switch leaves both rows carrying the digest until the delete
        lands, and the immutable one is the answer in that window."""
        with orm.Session(db_engine) as session, session.begin():
            pipeline = _pipeline(
                session=session,
                pipeline_id=_ALIVE_ID,
                user_id="alice",
                file_path="a.py",
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=db_models.CURRENT_VERSION_KEY,
                content_digest=_VERSION_A,
            )
            _version(
                session=session,
                pipeline_id=_ALIVE_ID,
                version_key=_VERSION_A,
                content_digest=_VERSION_A,
            )
            assert (
                services.resolve_pinnable_version_key(
                    session=session,
                    pipeline=pipeline,
                    content_digest=_VERSION_A,
                )
                == _VERSION_A
            )


def test_resolving_a_pin_takes_a_shared_lock_not_an_exclusive_one() -> None:
    """Thread 3898128857: concurrent pin resolutions should not block each other.

    The mode writer still holds `FOR UPDATE`, and `FOR SHARE` blocks it, so the
    serialization this lock exists for is unchanged. What changes is that two
    schedules resolving pins against the same pipeline no longer queue behind
    one another for a row neither of them writes.
    """
    session = mock.MagicMock(spec=orm.Session)
    session.scalar.return_value = None

    with pytest.raises(pipeline_errors.PipelineNotFoundError):
        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id="00000000-0000-0000-0000-000000000000",
            user_id="owner@example.com",
            file_path=None,
            version="some-pinned-digest",
            require_pinnable=True,
        )

    query = session.scalar.call_args.args[0]
    assert query._for_update_arg is not None
    assert query._for_update_arg.read is True


def test_an_unpinned_resolve_takes_no_lock_at_all() -> None:
    """Only a pin needs the mode held still; tracking current does not."""
    session = mock.MagicMock(spec=orm.Session)
    session.scalar.return_value = None

    with pytest.raises(pipeline_errors.PipelineNotFoundError):
        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id="00000000-0000-0000-0000-000000000000",
            user_id="owner@example.com",
            file_path=None,
            version=None,
            require_pinnable=True,
        )

    assert session.scalar.call_args.args[0]._for_update_arg is None


def _write_versioned_pipeline(
    *,
    db_engine: sqlalchemy.Engine,
    user_id: str = "owner@example.com",
    file_path: str = "pipelines/projected.yaml",
) -> tuple[str, str]:
    """A FULL-mode pipeline with two versions. Returns (pipeline_id, old_key)."""
    service = services.UserPipelineService()

    def write(name: str) -> tuple[str, str]:
        with orm.Session(db_engine) as session:
            written = service.set_pipeline(
                session=session,
                user_id=user_id,
                file_path=file_path,
                root_pipeline_task=pipeline_task(name=name),
                pipeline_run_annotations=None,
                versioning_mode=db_models.PipelineVersioningMode.FULL,
            )
            return written.pipeline.id, written.version.version_key

    pipeline_id, old_key = write("v1")
    write("v2")
    return pipeline_id, old_key


#: The columns a validation has no business reading. `root_pipeline_task` is the
#: pipeline body; the other two are the rest of the payload that rides with it.
_PAYLOAD_COLUMNS = (
    "root_pipeline_task",
    "pipeline_run_annotations",
    "extra_data",
)


class TestValidationOnlyResolutionSelectsNoPayload:
    """Binks 3906749295: validating a reference should not load the pipeline body.

    The rule itself is not allowed to move -- same service, same queries, same
    lock, same errors -- so these tests pin the SQL shape and the query count
    rather than the outcome, which is identical either way.
    """

    @pytest.mark.parametrize("pin_an_older_version", [False, True])
    def test_a_validating_resolve_reads_no_payload_column(
        self,
        db_engine: sqlalchemy.Engine,
        pin_an_older_version: bool,
    ) -> None:
        """Both version lookups, not just the current-following one.

        The requested-version SELECT is a separate statement with its own
        options, so a projection removed from only that branch would otherwise
        slip past every payload assertion here.
        """
        pipeline_id, old_key = _write_versioned_pipeline(db_engine=db_engine)

        with (
            orm.Session(db_engine) as session,
            sql_capture.capture_sql(db_engine) as statements,
        ):
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version=old_key if pin_an_older_version else None,
                require_pinnable=True,
                validation_only=True,
            )

        sql_capture.asserted_absent(
            sql_capture.selects(statements), columns=_PAYLOAD_COLUMNS
        )

    def test_the_owner_column_is_projected_even_though_the_predicate_filters_it(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """It is read back to be re-compared exactly; see `_get_pipeline`.

        Without this the projection would make the loose-collation ownership bug
        unfixable, which is the one column omission that would be a security
        regression rather than a saving.
        """
        pipeline_id, _ = _write_versioned_pipeline(db_engine=db_engine)

        with (
            orm.Session(db_engine) as session,
            sql_capture.capture_sql(db_engine) as statements,
        ):
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version=None,
                require_pinnable=True,
                validation_only=True,
            )

        pipeline_selects = sql_capture.selects_from(statements, table="pipeline")
        assert pipeline_selects
        assert all(
            sql_capture.mentions(statement, column="user_id")
            for statement in pipeline_selects
        )

    def test_a_submitting_resolve_still_loads_the_payload(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The executor's default. Projecting this would break the fire path."""
        pipeline_id, _ = _write_versioned_pipeline(db_engine=db_engine)

        with (
            orm.Session(db_engine) as session,
            sql_capture.capture_sql(db_engine) as statements,
        ):
            _, version_row, _ = services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version=None,
                require_pinnable=True,
            )
            assert (
                version_row.root_pipeline_task["componentRef"]["spec"]["name"] == "v2"
            )

        assert any(
            sql_capture.mentions(statement, column="root_pipeline_task")
            for statement in sql_capture.selects(statements)
        )

    @pytest.mark.parametrize(
        ("pin", "expected_selects"),
        [
            pytest.param(None, 2, id="following-current"),
            pytest.param("current-digest", 2, id="pinned-to-the-current-digest"),
            pytest.param("older", 3, id="pinned-to-an-older-version"),
        ],
    )
    def test_the_query_count_is_unchanged(
        self,
        db_engine: sqlalchemy.Engine,
        pin: str | None,
        expected_selects: int,
    ) -> None:
        """A projection narrows columns; it must not add or remove statements."""
        pipeline_id, old_key = _write_versioned_pipeline(db_engine=db_engine)
        with orm.Session(db_engine) as session:
            pipeline = session.get(db_models.UserPipeline, pipeline_id)
            assert pipeline is not None
            current_key = pipeline.current_version_key
        assert current_key is not None
        version = {None: None, "current-digest": current_key, "older": old_key}[pin]

        with (
            orm.Session(db_engine) as session,
            sql_capture.capture_sql(db_engine) as statements,
        ):
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version=version,
                require_pinnable=True,
                validation_only=True,
            )

        assert len(sql_capture.selects(statements)) == expected_selects

    def test_touching_a_projected_away_column_raises_instead_of_querying(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`raiseload=True`, not a plain defer.

        A deferred column would emit a silent second SELECT on first access --
        the exact cost this removes, reintroduced somewhere no test would look.
        """
        pipeline_id, _ = _write_versioned_pipeline(db_engine=db_engine)

        with orm.Session(db_engine) as session:
            _, version_row, _ = services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version=None,
                require_pinnable=True,
                validation_only=True,
            )
            with pytest.raises(sqlalchemy.exc.InvalidRequestError):
                _ = version_row.root_pipeline_task

    def test_a_pin_still_takes_the_shared_lock_before_the_version_query(
        self,
    ) -> None:
        """The projection is applied to the same locked statement, not after it."""
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = None

        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id="00000000-0000-0000-0000-000000000000",
                user_id="owner@example.com",
                file_path=None,
                version="some-pinned-digest",
                require_pinnable=True,
                validation_only=True,
            )

        query = session.scalar.call_args.args[0]
        assert query._for_update_arg is not None
        assert query._for_update_arg.read is True
        # Nothing was resolved, so nothing after the lock ran.
        assert session.get.call_count == 0

    @pytest.mark.parametrize("validation_only", [False, True])
    def test_every_refusal_is_the_same_with_or_without_the_projection(
        self,
        db_engine: sqlalchemy.Engine,
        validation_only: bool,
    ) -> None:
        """Missing, foreign, unknown version, the reserved key, soft-deleted."""
        pipeline_id, _ = _write_versioned_pipeline(db_engine=db_engine)
        service = services.UserPipelineService()

        def resolve(**overrides) -> None:
            defaults = {
                "pipeline_id": pipeline_id,
                "user_id": "owner@example.com",
                "file_path": None,
                "version": None,
                "require_pinnable": True,
                "validation_only": validation_only,
            }
            with orm.Session(db_engine) as session:
                service.get_pipeline_and_version(
                    session=session, **(defaults | overrides)
                )

        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            resolve(pipeline_id="00000000-0000-0000-0000-000000000000")
        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            resolve(user_id="stranger@example.com")
        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            resolve(version="0" * 64)
        # The reserved key is excluded from the version query, so pinning it is
        # a miss rather than a validation error.
        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            resolve(version=db_models.CURRENT_VERSION_KEY)

        with orm.Session(db_engine) as session:
            pipeline = session.get(db_models.UserPipeline, pipeline_id)
            assert pipeline is not None
            pipeline.deleted_at = datetime.datetime.now(datetime.timezone.utc)
            session.commit()
        with pytest.raises(pipeline_errors.PipelineNotFoundError):
            resolve()


class TestUnpinnableModeIsRefusedIdentically:
    """`require_pinnable_versioning` runs on the projected row, not a full one."""

    @pytest.mark.parametrize("validation_only", [False, True])
    def test_pinning_a_disabled_pipeline_is_refused(
        self,
        db_engine: sqlalchemy.Engine,
        validation_only: bool,
    ) -> None:
        with orm.Session(db_engine) as session:
            written = services.UserPipelineService().set_pipeline(
                session=session,
                user_id="owner@example.com",
                file_path="pipelines/disabled.yaml",
                root_pipeline_task=pipeline_task(name="disabled"),
                pipeline_run_annotations=None,
                versioning_mode=db_models.PipelineVersioningMode.DISABLED,
            )
            pipeline_id = written.pipeline.id

        with (
            orm.Session(db_engine) as session,
            pytest.raises(pipeline_errors.PipelineValidationError),
        ):
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id=pipeline_id,
                user_id="owner@example.com",
                file_path=None,
                version="d" * 64,
                require_pinnable=True,
                validation_only=validation_only,
            )


class TestTheOwnerComparatorIsTheDatabases:
    """This resolver holds no opinion about owner identity of its own.

    It used to. An earlier revision of this branch re-compared `user_id`
    byte-exactly in Python after the query, on the premise that a caller whose
    name differed from the stored spelling was a different principal. That
    premise was withdrawn: owner identity is decided by the deployment's column
    collation, and a second comparator in application code competes with it.

    These tests are the inversion of the ones that pinned the old behaviour.
    They are kept rather than deleted because "we used to refuse this caller"
    is what a reader needs in order to understand the rows that exist today.

    Scope note, so nothing broader is read into the fixtures below: the decided
    contract concerns CASE. Accent and normalization semantics are deliberately
    not asserted here in either direction -- they are out of scope for this
    change, and a test that pinned them would be inventing a policy no one has
    ruled on. What IS asserted is narrower and sufficient: whatever row the
    database's comparator returns, this resolver does not overrule it.
    """

    @staticmethod
    def _pipeline_owned_by(*, owner: str) -> db_models.UserPipeline:
        return db_models.UserPipeline(
            user_id=owner,
            file_path="pipelines/mine.yaml",
            current_version_key="d" * 64,
            versioning_mode=db_models.PipelineVersioningMode.FULL,
        )

    @pytest.mark.parametrize("validation_only", [False, True])
    @pytest.mark.parametrize(
        ("stored_owner", "requesting_as"),
        [
            pytest.param(
                "Jose@example.com", "jose@example.com", id="stored-capitalized"
            ),
            pytest.param(
                "jose@example.com", "Jose@example.com", id="caller-capitalized"
            ),
            pytest.param("JOSE@example.com", "jose@example.com", id="stored-upper"),
        ],
    )
    def test_a_case_variant_owner_resolves_rather_than_404ing(
        self,
        stored_owner: str,
        requesting_as: str,
        validation_only: bool,
    ) -> None:
        """The regression that matters: this used to fail CLOSED.

        A caller whose authenticated identity differs from the stored spelling
        only in case IS the owner. The old Python recheck refused them and
        reported not-found, locking a user out of their own pipeline. The
        database returned the row; nothing here may discard it.

        Both caller shapes, deliberately: the executor resolves at full width and
        the schedule writer resolves projected, so a refusal reinstated in only
        one of them would still be caught.
        """
        row = self._pipeline_owned_by(owner=stored_owner)
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = row

        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id="00000000-0000-0000-0000-000000000000",
            user_id=requesting_as,
            file_path=None,
            version=None,
            require_pinnable=True,
            validation_only=validation_only,
        )

        # The spellings really do differ, or this test proves nothing.
        assert row.user_id != requesting_as
        # Reached the version lookup, which is what the old refusal short-circuited.
        assert session.get.call_count == 1

    def test_the_file_path_branch_also_stops_second_guessing(self) -> None:
        """The other query, which had its own copy of the removed recheck."""
        row = self._pipeline_owned_by(owner="Jose@example.com")
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = row

        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id=None,
            user_id="jose@example.com",
            file_path="pipelines/mine.yaml",
            version=None,
            require_pinnable=True,
        )

        assert session.get.call_count == 1

    def test_an_exactly_matching_owner_still_resolves(self) -> None:
        """The control: removing a refusal must not disturb the ordinary case."""
        owner = "jose@example.com"
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = self._pipeline_owned_by(owner=owner)

        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id="00000000-0000-0000-0000-000000000000",
            user_id=owner,
            file_path=None,
            version=None,
            require_pinnable=True,
        )

        assert session.get.call_count == 1

    def test_an_unscoped_lookup_is_unaffected(self) -> None:
        """`user_id=None` means "any owner"; there is nothing to compare."""
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = self._pipeline_owned_by(
            owner="someone@example.com"
        )

        services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id="00000000-0000-0000-0000-000000000000",
            user_id=None,
            file_path=None,
            version=None,
            require_pinnable=True,
        )

        assert session.get.call_count == 1

    def test_a_missing_row_is_still_not_found(self) -> None:
        """Removing the recheck must not remove the refusal it was folded into.

        The database returning nothing is still not-found, and the message must
        still not distinguish absent from someone-else's -- that distinction
        would be an existence oracle over other tenants' pipeline ids.
        """
        session = mock.MagicMock(spec=orm.Session)
        session.scalar.return_value = None

        with pytest.raises(pipeline_errors.PipelineNotFoundError) as refusal:
            services.UserPipelineService().get_pipeline_and_version(
                session=session,
                pipeline_id="00000000-0000-0000-0000-000000000000",
                user_id="jose@example.com",
                file_path=None,
                version=None,
                require_pinnable=True,
            )

        assert "was not found" in str(refusal.value)
        assert session.get.call_count == 0

    @pytest.mark.parametrize(
        "resolver",
        [
            services.get_live_owned_pipeline,
            services.UserPipelineService._get_pipeline,
        ],
        ids=["shared-door", "file-path-branch"],
    )
    def test_no_owner_comparison_is_reinstated_in_python(
        self, resolver: object
    ) -> None:
        """Structural, because a behavioural test cannot see a dormant comparison.

        Flags any comparison involving a loaded ROW's `.user_id` -- which is the
        competing comparator, and has no legitimate form in these resolvers. A
        reviewer restoring `pipeline.user_id != user_id`, the exact line this
        change removed, fails here even with every behavioural test above green.

        Two shapes are deliberately not offences, and the distinction is the
        point of the matcher:

        * `db_models.UserPipeline.user_id == user_id` -- the SQLAlchemy column
          expression. That IS the owner rule; flagging it would forbid the thing
          this change standardizes on. Excluded by looking at what the attribute
          hangs off: the mapped class, not a row.
        * `user_id is None` / `is not None` -- a bare name, never an attribute, so
          it is not matched in the first place. It decides whether to scope the
          query at all and compares nothing to an owner.

        Parsed rather than grepped so the docstrings and comments recounting the
        removed rule, which necessarily quote it, do not trip the check.

        Honest limit: this sees the syntactic shape, not the dataflow. Binding the
        owner to a local first (`stored = pipeline.user_id; if stored != ...`)
        would evade it. That is accepted -- the behavioural tests above are what
        catch the regression itself, and this exists to catch the obvious restore.
        """

        def _is_the_column_expression(node: ast.expr) -> bool:
            base = node.value if isinstance(node, ast.Attribute) else None
            return getattr(base, "attr", getattr(base, "id", None)) == "UserPipeline"

        def _names_a_row_owner(node: ast.expr) -> bool:
            return (
                isinstance(node, ast.Attribute)
                and node.attr == "user_id"
                and not _is_the_column_expression(node)
            )

        tree = ast.parse(textwrap.dedent(inspect.getsource(resolver)))
        offenders = [
            ast.dump(node)
            for node in ast.walk(tree)
            if isinstance(node, ast.Compare)
            and any(
                _names_a_row_owner(operand)
                for operand in [node.left, *node.comparators]
            )
        ]

        assert offenders == [], offenders
