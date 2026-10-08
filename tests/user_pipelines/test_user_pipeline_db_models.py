import uuid

import pytest
import sqlalchemy
from cloud_pipelines_backend.user_pipelines import db_models, services
from sqlalchemy import orm
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.schema import CreateTable

from tests.user_pipelines.conftest import pipeline_task


def test_schema_has_expected_primary_keys_uniqueness_and_foreign_keys(
    db_engine: sqlalchemy.Engine,
) -> None:
    inspector = sqlalchemy.inspect(db_engine)

    pipeline_pk = inspector.get_pk_constraint("pipeline")
    version_pk = inspector.get_pk_constraint("pipeline_version")
    pipeline_uniques = inspector.get_unique_constraints("pipeline")
    pipeline_fks = inspector.get_foreign_keys("pipeline")
    version_fks = inspector.get_foreign_keys("pipeline_version")
    pipeline_indexes = inspector.get_indexes("pipeline")
    pipeline_checks = inspector.get_check_constraints("pipeline")
    pipeline_columns = {
        column["name"]: column for column in inspector.get_columns("pipeline")
    }
    version_columns = {
        column["name"]: column for column in inspector.get_columns("pipeline_version")
    }

    assert pipeline_pk["constrained_columns"] == ["id"]
    assert "extra_data" in pipeline_columns
    assert pipeline_columns["deleted_at"]["nullable"] is True
    assert pipeline_columns["versioning_mode"]["type"].length == 255
    assert any(
        constraint["name"] == "ck_pipeline_versioning_mode"
        and "'disabled'" in constraint["sqltext"]
        and "'full'" in constraint["sqltext"]
        for constraint in pipeline_checks
    )
    assert "content_digest" in version_columns
    assert version_pk["constrained_columns"] == ["pipeline_id", "version_key"]
    assert any(
        constraint["column_names"] == ["user_id", "file_path"]
        for constraint in pipeline_uniques
    )
    assert any(
        fk["constrained_columns"] == ["id", "current_version_key"]
        and fk["referred_columns"] == ["pipeline_id", "version_key"]
        for fk in pipeline_fks
    )
    assert any(
        fk["constrained_columns"] == ["pipeline_id"]
        and fk["referred_table"] == "pipeline"
        for fk in version_fks
    )
    assert any(
        index["column_names"] == ["user_id", "updated_at", "id"]
        for index in pipeline_indexes
    )
    assert all(
        index["column_names"]
        not in (
            ["user_id"],
            ["user_id", "file_path", "updated_at", "id"],
        )
        for index in pipeline_indexes
    )


def test_pipeline_id_is_uuid_and_owner_path_is_unique(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        pipeline = db_models.UserPipeline(user_id="owner", file_path="pipeline.yaml")
        pipeline_id = pipeline.id
        assert str(uuid.UUID(pipeline_id)) == pipeline_id
        assert pipeline.versioning_mode is db_models.PipelineVersioningMode.DISABLED

        session.add(pipeline)
        session.commit()
        assert pipeline.id == pipeline_id

        session.add(db_models.UserPipeline(user_id="owner", file_path="pipeline.yaml"))
        with pytest.raises(IntegrityError):
            session.commit()


def test_versioning_mode_round_trips_as_typed_enum_and_lowercase_storage(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        pipeline = db_models.UserPipeline(
            user_id="owner",
            file_path="typed-mode.yaml",
            versioning_mode=db_models.PipelineVersioningMode.FULL,
        )
        session.add(pipeline)
        session.commit()
        pipeline_id = pipeline.id

        assert pipeline.versioning_mode is db_models.PipelineVersioningMode.FULL
        assert (
            session.execute(
                sqlalchemy.text(
                    "SELECT versioning_mode FROM pipeline WHERE id = :pipeline_id"
                ),
                {"pipeline_id": pipeline_id},
            ).scalar_one()
            == "full"
        )
        session.expire_all()
        loaded_pipeline = session.get(db_models.UserPipeline, pipeline_id)
        assert loaded_pipeline is not None
        assert loaded_pipeline.versioning_mode is db_models.PipelineVersioningMode.FULL


def test_versioning_mode_rejects_invalid_raw_and_bound_strings(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        pipeline = db_models.UserPipeline(
            user_id="owner",
            file_path="invalid-mode.yaml",
        )
        session.add(pipeline)
        session.commit()

        with pytest.raises(IntegrityError):
            session.execute(
                sqlalchemy.text(
                    "UPDATE pipeline SET versioning_mode = 'partial' WHERE id = :pipeline_id"
                ),
                {"pipeline_id": pipeline.id},
            )
            session.commit()
        session.rollback()

        with pytest.raises(StatementError):
            session.execute(
                sqlalchemy.update(db_models.UserPipeline)
                .where(db_models.UserPipeline.id == pipeline.id)
                .values(versioning_mode="partial")
            )


def test_mysql_versioning_mode_is_portable_varchar_check() -> None:
    ddl = str(
        CreateTable(db_models.UserPipeline.__table__).compile(dialect=mysql.dialect())
    )

    assert "versioning_mode VARCHAR(255) NOT NULL" in ddl
    assert "CONSTRAINT ck_pipeline_versioning_mode CHECK" in ddl
    assert "versioning_mode IN ('disabled', 'full')" in ddl
    assert "ENUM(" not in ddl


def test_pipeline_extra_data_persists_in_place_updates(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        pipeline = db_models.UserPipeline(
            user_id="owner",
            file_path="pipeline.yaml",
            extra_data={"future": "initial"},
        )
        session.add(pipeline)
        session.commit()
        pipeline.extra_data["future"] = "updated"  # type: ignore[index]
        session.commit()
        pipeline_id = pipeline.id

    with orm.Session(db_engine) as session:
        loaded_pipeline = session.get(db_models.UserPipeline, pipeline_id)
        assert loaded_pipeline is not None
        assert loaded_pipeline.extra_data == {"future": "updated"}


def test_version_composite_key_is_per_pipeline(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        first_pipeline = db_models.UserPipeline(user_id="one", file_path="same.yaml")
        second_pipeline = db_models.UserPipeline(user_id="two", file_path="same.yaml")
        session.add_all([first_pipeline, second_pipeline])
        session.flush()

        digest = "d" * 64
        session.add_all(
            [
                db_models.UserPipelineVersion(
                    pipeline_id=first_pipeline.id,
                    version_key=digest,
                    content_digest=digest,
                    root_pipeline_task={},
                ),
                db_models.UserPipelineVersion(
                    pipeline_id=second_pipeline.id,
                    version_key=digest,
                    content_digest=digest,
                    root_pipeline_task={},
                ),
            ]
        )
        session.commit()

        session.add(
            db_models.UserPipelineVersion(
                pipeline_id=first_pipeline.id,
                version_key=digest,
                content_digest=digest,
                root_pipeline_task={},
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_version_requires_existing_pipeline(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        session.add(
            db_models.UserPipelineVersion(
                pipeline_id=str(uuid.uuid4()),
                version_key="f" * 64,
                content_digest="f" * 64,
                root_pipeline_task={},
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_current_version_key_requires_version_owned_by_pipeline(
    db_engine: sqlalchemy.Engine,
) -> None:
    with orm.Session(db_engine) as session:
        pipeline = db_models.UserPipeline(user_id="owner", file_path="pipeline.yaml")
        session.add(pipeline)
        session.flush()
        pipeline.current_version_key = "f" * 64

        with pytest.raises(IntegrityError):
            session.commit()


def test_digest_is_deterministic_and_includes_annotations() -> None:
    first = services.prepare_pipeline_content(
        root_pipeline_task=pipeline_task(name="stable"),
        pipeline_run_annotations={"b": "two", "a": "one"},
    )
    reordered = services.prepare_pipeline_content(
        root_pipeline_task=pipeline_task(name="stable"),
        pipeline_run_annotations={"a": "one", "b": "two"},
    )
    no_annotations = services.prepare_pipeline_content(
        root_pipeline_task=pipeline_task(name="stable"),
        pipeline_run_annotations=None,
    )
    empty_annotations = services.prepare_pipeline_content(
        root_pipeline_task=pipeline_task(name="stable"),
        pipeline_run_annotations={},
    )

    assert first.digest == reordered.digest
    assert first.digest != no_annotations.digest
    assert no_annotations.digest == empty_annotations.digest
    assert no_annotations.pipeline_run_annotations == {}
    assert empty_annotations.pipeline_run_annotations == {}
