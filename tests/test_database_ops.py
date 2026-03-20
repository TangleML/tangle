"""Tests for database_ops: migrate_db and index creation."""

import sqlalchemy

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops


def _get_index_names(
    *,
    engine: sqlalchemy.Engine,
    table_name: str,
) -> set[str]:
    inspector = sqlalchemy.inspect(engine)
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


class TestMigrateDb:
    """Verify migrate_db creates indexes on a pre-existing DB missing them."""

    def _bare_engine(self) -> sqlalchemy.Engine:
        """Create tables then drop all indexes so migrate_db can recreate them."""
        db_engine = database_ops.create_db_engine(database_uri="sqlite://")
        bts._TableBase.metadata.create_all(db_engine)

        with db_engine.connect() as conn:
            for table_name in ("execution_node", "pipeline_run_annotation"):
                for idx_name in _get_index_names(
                    engine=db_engine, table_name=table_name
                ):
                    conn.execute(sqlalchemy.text(f"DROP INDEX IF EXISTS {idx_name}"))
            conn.commit()
        return db_engine

    def test_creates_execution_node_cache_key_index(self) -> None:
        bare_engine = self._bare_engine()
        assert bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY not in _get_index_names(
            engine=bare_engine,
            table_name="execution_node",
        )
        database_ops.migrate_db(db_engine=bare_engine, do_skip_backfill=False)
        assert bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY in _get_index_names(
            engine=bare_engine,
            table_name="execution_node",
        )

    def test_creates_annotation_run_id_key_value_index(self) -> None:
        bare_engine = self._bare_engine()
        assert (
            bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE
            not in _get_index_names(
                engine=bare_engine, table_name="pipeline_run_annotation"
            )
        )
        database_ops.migrate_db(db_engine=bare_engine, do_skip_backfill=False)
        assert (
            bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE
            in _get_index_names(
                engine=bare_engine, table_name="pipeline_run_annotation"
            )
        )

    def test_idempotent(self) -> None:
        bare_engine = self._bare_engine()
        database_ops.migrate_db(db_engine=bare_engine, do_skip_backfill=False)
        indexes_after_first = {
            "execution_node": _get_index_names(
                engine=bare_engine, table_name="execution_node"
            ),
            "pipeline_run_annotation": _get_index_names(
                engine=bare_engine,
                table_name="pipeline_run_annotation",
            ),
        }
        database_ops.migrate_db(db_engine=bare_engine, do_skip_backfill=False)
        indexes_after_second = {
            "execution_node": _get_index_names(
                engine=bare_engine, table_name="execution_node"
            ),
            "pipeline_run_annotation": _get_index_names(
                engine=bare_engine,
                table_name="pipeline_run_annotation",
            ),
        }
        assert indexes_after_first == indexes_after_second
