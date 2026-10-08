"""Unit tests for emissions.db_models.

Named test_emission_db_models (not test_db_models) so the module basename stays unique
across the suite: the repo has no __init__.py in tests and pytest runs in the default
prepend import mode, which keys modules by basename, so a duplicate basename
(scheduling/pipelines/test_db_models.py) would collide on collection.
"""

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.emissions import db_models


class TestDedupe:
    def test_duplicate_natural_key_rejected(self, db_engine: sqlalchemy.Engine) -> None:
        # A double-fire is a second, independent producer write of the same natural key
        # (not two rows batched into one commit), so commit the first row, then attempt a
        # separate write and expect it rejected.
        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-1",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-1",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_duplicate_natural_key_rejected_without_a_container_execution(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The same double-fire for a node that never had a container (SKIPPED, QUEUED, and
        # friends), which leaves container_execution_id NULL. The natural key covers only NOT
        # NULL columns, so such a row is constrained like any other.
        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-skipped",
                    container_execution_id=None,
                    container_execution_status="SKIPPED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-skipped",
                    container_execution_id=None,
                    container_execution_status="SKIPPED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_same_node_different_container_execution_rejected(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The container execution is not in the key: the same node, kind and status collide
        # whichever container produced them.
        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-1",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-2",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_different_nodes_sharing_a_container_execution_allowed(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The cache-hit shape: the orchestrator points a new node at an existing container
        # execution, so one container execution can back several nodes. Each node is its own
        # emission.
        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-shared",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-2",
                    container_execution_id="ce-shared",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.commit()

            count = session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.EmissionEvent
                )
            )
            assert count == 2

    def test_same_node_different_emission_type_allowed(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-1",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.READINESS.value,
                )
            )
            session.add(
                db_models.EmissionEvent(
                    execution_node_id="node-1",
                    container_execution_id="ce-1",
                    container_execution_status="SUCCEEDED",
                    emission_type=db_models.EmissionType.METADATA.value,
                )
            )
            session.commit()

            count = session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.EmissionEvent
                )
            )
            assert count == 2


class TestDefaultsAndRoundTrip:
    def test_timestamps_set_and_claim_and_handling_defaults(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            event = db_models.EmissionEvent(
                execution_node_id="node-1",
                container_execution_status="SUCCEEDED",
                emission_type=db_models.EmissionType.READINESS.value,
            )
            session.add(event)
            session.commit()
            session.refresh(event)

            assert len(event.id) == 20
            assert event.created_at is not None
            assert event.updated_at is not None
            assert event.container_execution_id is None
            assert event.pipeline_run_id is None
            assert event.claimed_status == db_models.ClaimStatus.PENDING.value
            assert event.claimed_at is None
            assert event.claimed_by is None
            assert event.handle_status is None
            assert event.handle_detail is None
            assert event.extra_data is None


class TestEmissionEventAnnotation:
    def _make_event(self, session: orm.Session) -> str:
        event = db_models.EmissionEvent(
            execution_node_id="node-ann",
            container_execution_status="SUCCEEDED",
            emission_type=db_models.EmissionType.READINESS.value,
        )
        session.add(event)
        session.commit()
        return event.id

    def test_annotation_round_trip_and_search(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            session.add(
                db_models.EmissionEventAnnotation(
                    emission_event_id=eid,
                    key="tangleml.com/emission/readiness/event",
                    value="dw.source.orders.ready",
                )
            )
            session.commit()

            found = session.scalars(
                sqlalchemy.select(db_models.EmissionEventAnnotation).where(
                    db_models.EmissionEventAnnotation.key
                    == "tangleml.com/emission/readiness/event",
                    db_models.EmissionEventAnnotation.value == "dw.source.orders.ready",
                )
            ).all()
            assert len(found) == 1
            assert found[0].emission_event_id == eid

    def test_duplicate_key_per_event_rejected(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            session.add(
                db_models.EmissionEventAnnotation(
                    emission_event_id=eid,
                    key="tangleml.com/emission/readiness/event",
                    value="v1",
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEventAnnotation(
                    emission_event_id=eid,
                    key="tangleml.com/emission/readiness/event",
                    value="v2",
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def _mysql_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        # SQLite enforces neither the column's length nor an index prefix, so only the DDL the
        # MySQL dialect compiles can tell whether either is in place. From the registry rather
        # than an import, since nothing else in this module needs it.
        return sqlalchemy.dialects.registry.load("mysql")()

    def test_value_column_is_text_on_mysql(self) -> None:
        # Asserts sql.Text won over type_annotation_map, which maps a bare `str` to VARCHAR(255).
        compiled = db_models.EmissionEventAnnotation.__table__.c.value.type.compile(
            dialect=self._mysql_dialect()
        )
        assert compiled == "TEXT"

    def test_key_value_index_names_a_prefix_on_mysql(self) -> None:
        # Without the prefix MySQL refuses to create the index at all (error 1170), so this is
        # about the DDL being accepted, not about the index being fast.
        index = next(
            idx
            for idx in db_models.EmissionEventAnnotation.__table__.indexes
            if idx.name == "ix_emission_event_annotation_key_value"
        )
        compiled = str(
            sqlalchemy.schema.CreateIndex(index).compile(dialect=self._mysql_dialect())
        )
        assert "(`key`, value(512))" in compiled


class TestEmissionEventOutcome:
    _SINK_KEY = "tangleml.com/emission/metadata/sink/example-collector"

    def _make_event(self, session: orm.Session) -> str:
        event = db_models.EmissionEvent(
            execution_node_id="node-outcome",
            container_execution_status="SUCCEEDED",
            emission_type=db_models.EmissionType.METADATA.value,
        )
        session.add(event)
        session.commit()
        return event.id

    def test_outcome_round_trip_and_defaults(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            outcome = db_models.EmissionEventOutcome(
                emission_event_id=eid,
                annotation_key=self._SINK_KEY,
                status="success",
                detail={"http_status": 200},
                extra_data={"timings": {"sink_s": 0.5}},
            )
            session.add(outcome)
            session.commit()
            session.refresh(outcome)

            assert outcome.created_at is not None
            assert outcome.updated_at is not None
            assert outcome.detail == {"http_status": 200}
            assert outcome.extra_data == {"timings": {"sink_s": 0.5}}

    def test_detail_and_extra_data_default_to_null(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # A sink that reported nothing beyond its status leaves both JSON columns unset.
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            outcome = db_models.EmissionEventOutcome(
                emission_event_id=eid,
                annotation_key=self._SINK_KEY,
                status="ignore",
            )
            session.add(outcome)
            session.commit()
            session.refresh(outcome)

            assert outcome.detail is None
            assert outcome.extra_data is None

    def test_duplicate_pair_rejected(self, db_engine: sqlalchemy.Engine) -> None:
        # The dedupe the ledger runs on: a delivery already recorded cannot be recorded again,
        # so a reclaiming consumer that races the original writer loses on the primary key
        # rather than overwriting what the winner recorded.
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            session.add(
                db_models.EmissionEventOutcome(
                    emission_event_id=eid,
                    annotation_key=self._SINK_KEY,
                    status="success",
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.EmissionEventOutcome(
                    emission_event_id=eid,
                    annotation_key=self._SINK_KEY,
                    status="fail",
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_one_row_per_sink_read_back_by_event(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The only read this table serves: every outcome of one event, which is what tells a
        # handler which deliveries are already done.
        external_key = "tangleml.com/emission/metadata/sink/external"
        with orm.Session(bind=db_engine) as session:
            eid = self._make_event(session)
            session.add(
                db_models.EmissionEventOutcome(
                    emission_event_id=eid,
                    annotation_key=self._SINK_KEY,
                    status="success",
                )
            )
            session.add(
                db_models.EmissionEventOutcome(
                    emission_event_id=eid,
                    annotation_key=external_key,
                    status="fail",
                )
            )
            session.commit()

            found = session.scalars(
                sqlalchemy.select(db_models.EmissionEventOutcome).where(
                    db_models.EmissionEventOutcome.emission_event_id == eid
                )
            ).all()
            assert {row.annotation_key: row.status for row in found} == {
                self._SINK_KEY: "success",
                external_key: "fail",
            }
