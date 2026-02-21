import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.api_server_sql import (
    ExecutionNodesApiService_Sql,
    GetGraphExecutionStateResponse,
)


def _initialize_db_and_get_session_factory():
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _make_execution_node(
    task_spec: dict | None = None,
    task_id_in_parent: str | None = None,
    container_execution_status: bts.ContainerExecutionStatus | None = None,
) -> bts.ExecutionNode:
    return bts.ExecutionNode(
        task_spec=task_spec or {},
        task_id_in_parent_execution=task_id_in_parent,
        container_execution_status=container_execution_status,
    )


def _add_child_node(
    session: orm.Session,
    parent_id: str,
    task_spec: dict | None = None,
    task_id_in_parent: str | None = None,
    container_execution_status: bts.ContainerExecutionStatus | None = None,
) -> bts.ExecutionNode:
    """Create a child execution node, add to session, and set parent_execution_id.

    parent_execution_id is init=False in the MappedAsDataclass model,
    so it must be set after the object is added and flushed.
    """
    node = _make_execution_node(
        task_spec=task_spec,
        task_id_in_parent=task_id_in_parent,
        container_execution_status=container_execution_status,
    )
    session.add(node)
    session.flush()
    node.parent_execution_id = parent_id
    session.flush()
    return node


class TestGetGraphExecutionState:
    """Tests for ExecutionNodesApiService_Sql.get_graph_execution_state"""

    def setup_method(self):
        self.session_factory = _initialize_db_and_get_session_factory()
        self.service = ExecutionNodesApiService_Sql()

    def test_no_children_returns_empty_stats(self):
        """A graph node with no children returns empty stats."""
        with self.session_factory() as session:
            parent = _make_execution_node(task_spec={"name": "parent_graph"})
            session.add(parent)
            session.flush()

            result = self.service.get_graph_execution_state(session, parent.id)

        assert isinstance(result, GetGraphExecutionStateResponse)
        assert result.child_execution_status_stats == {}

    def test_children_with_no_status_are_excluded(self):
        """Children whose container_execution_status is None are not counted."""
        with self.session_factory() as session:
            parent = _make_execution_node(task_spec={"name": "graph"})
            session.add(parent)
            session.flush()

            _add_child_node(
                session,
                parent.id,
                task_spec={"name": "task_pending"},
                task_id_in_parent="pending_task",
                container_execution_status=None,
            )

            result = self.service.get_graph_execution_state(session, parent.id)

        assert result.child_execution_status_stats == {}

    def test_direct_container_children(self):
        """Children that are direct container nodes (no descendants via ancestor links)."""
        with self.session_factory() as session:
            parent = _make_execution_node(task_spec={"name": "graph"})
            session.add(parent)
            session.flush()

            child1 = _add_child_node(
                session,
                parent.id,
                task_spec={"name": "task1"},
                task_id_in_parent="task1",
                container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            child2 = _add_child_node(
                session,
                parent.id,
                task_spec={"name": "task2"},
                task_id_in_parent="task2",
                container_execution_status=bts.ContainerExecutionStatus.RUNNING,
            )

            result = self.service.get_graph_execution_state(session, parent.id)

        stats = result.child_execution_status_stats
        assert child1.id in stats
        assert stats[child1.id] == {"SUCCEEDED": 1}
        assert child2.id in stats
        assert stats[child2.id] == {"RUNNING": 1}

    def test_three_level_mixed_stats(self):
        """3-level deep graph with direct tasks and nested sub-graphs.

        Tree structure:
            root (graph)
            ├── task_1 (SUCCEEDED)
            ├── task_2 (FAILED)
            ├── sub_graph_a (graph)
            │   ├── task_sg_a_1 (CANCELLED)
            │   ├── task_sg_a_2 (SKIPPED)
            │   ├── task_sg_a_3 (RUNNING)
            │   └── sub_graph_a_b (graph)
            │       ├── task_sg_a_b_1 (INVALID)
            │       ├── task_sg_a_b_2 (SYSTEM_ERROR)
            │       └── task_sg_a_b_3 (SUCCEEDED)
            └── task_3 (QUEUED)

        Ended statuses used across the 3 levels:
            Level 1 (root children):   SUCCEEDED, FAILED
            Level 2 (sg_a children):   CANCELLED, SKIPPED
            Level 3 (sg_a_b children): INVALID, SYSTEM_ERROR, SUCCEEDED
        """
        with self.session_factory() as session:
            # -- Level 0: root --
            root = _make_execution_node(task_spec={"name": "root"})
            session.add(root)
            session.flush()

            # -- Level 1: direct children of root --
            task_1 = _add_child_node(
                session,
                root.id,
                task_spec={"name": "task_1"},
                task_id_in_parent="task_1",
                container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            task_2 = _add_child_node(
                session,
                root.id,
                task_spec={"name": "task_2"},
                task_id_in_parent="task_2",
                container_execution_status=bts.ContainerExecutionStatus.FAILED,
            )
            sub_graph_a = _add_child_node(
                session,
                root.id,
                task_spec={"name": "sub_graph_a"},
                task_id_in_parent="sub_graph_a",
                container_execution_status=None,
            )
            task_3 = _add_child_node(
                session,
                root.id,
                task_spec={"name": "task_3"},
                task_id_in_parent="task_3",
                container_execution_status=bts.ContainerExecutionStatus.QUEUED,
            )

            # -- Level 2: children of sub_graph_a --
            task_sg_a_1 = _add_child_node(
                session,
                sub_graph_a.id,
                task_spec={"name": "task_sg_a_1"},
                task_id_in_parent="task_sg_a_1",
                container_execution_status=bts.ContainerExecutionStatus.CANCELLED,
            )
            task_sg_a_2 = _add_child_node(
                session,
                sub_graph_a.id,
                task_spec={"name": "task_sg_a_2"},
                task_id_in_parent="task_sg_a_2",
                container_execution_status=bts.ContainerExecutionStatus.SKIPPED,
            )
            task_sg_a_3 = _add_child_node(
                session,
                sub_graph_a.id,
                task_spec={"name": "task_sg_a_3"},
                task_id_in_parent="task_sg_a_3",
                container_execution_status=bts.ContainerExecutionStatus.RUNNING,
            )
            sub_graph_a_b = _add_child_node(
                session,
                sub_graph_a.id,
                task_spec={"name": "sub_graph_a_b"},
                task_id_in_parent="sub_graph_a_b",
                container_execution_status=None,
            )

            # -- Level 3: children of sub_graph_a_b --
            task_sg_a_b_1 = _add_child_node(
                session,
                sub_graph_a_b.id,
                task_spec={"name": "task_sg_a_b_1"},
                task_id_in_parent="task_sg_a_b_1",
                container_execution_status=bts.ContainerExecutionStatus.INVALID,
            )
            task_sg_a_b_2 = _add_child_node(
                session,
                sub_graph_a_b.id,
                task_spec={"name": "task_sg_a_b_2"},
                task_id_in_parent="task_sg_a_b_2",
                container_execution_status=bts.ContainerExecutionStatus.SYSTEM_ERROR,
            )
            task_sg_a_b_3 = _add_child_node(
                session,
                sub_graph_a_b.id,
                task_spec={"name": "task_sg_a_b_3"},
                task_id_in_parent="task_sg_a_b_3",
                container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED,
            )

            # -- Ancestor links (closure table) --
            # sub_graph_a is ancestor of: all sg_a children + all sg_a_b children
            for node in [
                task_sg_a_1,
                task_sg_a_2,
                task_sg_a_3,
                sub_graph_a_b,
                task_sg_a_b_1,
                task_sg_a_b_2,
                task_sg_a_b_3,
            ]:
                session.add(
                    bts.ExecutionToAncestorExecutionLink(
                        ancestor_execution=sub_graph_a,
                        execution=node,
                    )
                )
            # sub_graph_a_b is ancestor of: all sg_a_b children
            for node in [task_sg_a_b_1, task_sg_a_b_2, task_sg_a_b_3]:
                session.add(
                    bts.ExecutionToAncestorExecutionLink(
                        ancestor_execution=sub_graph_a_b,
                        execution=node,
                    )
                )
            # root is ancestor of: all nodes below root
            for node in [
                task_1,
                task_2,
                sub_graph_a,
                task_3,
                task_sg_a_1,
                task_sg_a_2,
                task_sg_a_3,
                sub_graph_a_b,
                task_sg_a_b_1,
                task_sg_a_b_2,
                task_sg_a_b_3,
            ]:
                session.add(
                    bts.ExecutionToAncestorExecutionLink(
                        ancestor_execution=root,
                        execution=node,
                    )
                )
            session.flush()

            result = self.service.get_graph_execution_state(session, root.id)

        stats = result.child_execution_status_stats

        # -- Direct container children of root (Query 2) --
        assert stats[task_1.id] == {"SUCCEEDED": 1}
        assert stats[task_2.id] == {"FAILED": 1}
        assert stats[task_3.id] == {"QUEUED": 1}

        # -- sub_graph_a: aggregates ALL descendants via ancestor links (Query 1) --
        # Level 2: CANCELLED(1), SKIPPED(1), RUNNING(1)
        # Level 3: INVALID(1), SYSTEM_ERROR(1), SUCCEEDED(1)
        # sub_graph_a_b has NULL status so it is excluded from counts
        assert stats[sub_graph_a.id] == {
            "CANCELLED": 1,
            "SKIPPED": 1,
            "RUNNING": 1,
            "INVALID": 1,
            "SYSTEM_ERROR": 1,
            "SUCCEEDED": 1,
        }

        # sub_graph_a_b is NOT a direct child of root, so it does not
        # appear as a key in the stats
        assert sub_graph_a_b.id not in stats


if __name__ == "__main__":
    pytest.main()
