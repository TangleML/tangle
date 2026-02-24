"""AST-based detectors for N+1 query patterns in SQLAlchemy code."""

import ast
import dataclasses
from pathlib import Path


SUPPRESSION_COMMENT = "n1-ok"

SESSION_QUERY_METHODS = frozenset({"get", "execute", "scalars", "scalar"})

EAGER_LOADING_FUNCTIONS = frozenset({"joinedload", "selectinload", "subqueryload"})

LOOP_NODE_TYPES = (ast.For, ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)


@dataclasses.dataclass(frozen=True)
class Violation:
    filepath: str
    line: int
    category: str
    code_snippet: str
    suggestion: str
    func_name: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_suppressed(source_lines: list[str], line: int) -> bool:
    if 0 < line <= len(source_lines):
        return SUPPRESSION_COMMENT in source_lines[line - 1]
    return False


def _get_source_line(source_lines: list[str], line: int) -> str:
    if 0 < line <= len(source_lines):
        return source_lines[line - 1].strip()
    return "<unknown>"


def _is_session_query_call(node: ast.expr) -> bool:
    """Return True if *node* is ``session.<query_method>(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr in SESSION_QUERY_METHODS
        and isinstance(func.value, ast.Name)
        and func.value.id == "session"
    )


def _iter_loop_nodes(tree: ast.AST) -> list[ast.AST]:
    return [node for node in ast.walk(tree) if isinstance(node, LOOP_NODE_TYPES)]


def _node_contains(parent: ast.AST, child: ast.AST) -> bool:
    for node in ast.walk(parent):
        if node is child:
            return True
    return False


def _build_parent_map(tree: ast.Module) -> dict[int, ast.AST]:
    parents: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node
    return parents


def _find_enclosing_funcdef(
    parent_map: dict[int, ast.AST], target: ast.AST
) -> ast.FunctionDef | None:
    current: ast.AST | None = target
    while current is not None:
        if isinstance(current, ast.FunctionDef):
            return current
        current = parent_map.get(id(current))
    return None


def _get_func_qualname(
    parent_map: dict[int, ast.AST], func_node: ast.FunctionDef
) -> str:
    """Build a qualified name like ``ClassName.method_name``."""
    parts = [func_node.name]
    current: ast.AST | None = parent_map.get(id(func_node))
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.ClassDef)):
            parts.append(current.name)
        current = parent_map.get(id(current))
    return ".".join(reversed(parts))


def _collect_eager_loaded_attrs(func_node: ast.AST) -> set[str]:
    """Collect relationship attrs that are eagerly loaded in a function scope."""
    attrs: set[str] = set()
    for node in ast.walk(func_node):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_eager = False
        if isinstance(func, ast.Name) and func.id in EAGER_LOADING_FUNCTIONS:
            is_eager = True
        elif isinstance(func, ast.Attribute) and func.attr in EAGER_LOADING_FUNCTIONS:
            is_eager = True
        if is_eager and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Attribute):
                attrs.add(arg.attr)
    return attrs


# ---------------------------------------------------------------------------
# DB-source tracking — determines which variables hold query results
# ---------------------------------------------------------------------------


def _expr_involves_db(expr: ast.expr) -> bool:
    """Return True if *expr* is or wraps a session query call."""
    if _is_session_query_call(expr):
        return True
    if isinstance(expr, ast.Call):
        func = expr.func
        if isinstance(func, ast.Attribute) and func.attr in (
            "all", "unique", "first", "one", "tuples", "fetchall", "fetchone",
        ):
            return _expr_involves_db(func.value)
        if isinstance(func, ast.Name) and func.id in ("list", "tuple", "set"):
            return any(_expr_involves_db(arg) for arg in expr.args)
    return False


def _collect_db_variables(func_node: ast.AST) -> set[str]:
    """Return the set of variable names assigned from session query results
    within *func_node*.
    """
    db_vars: set[str] = set()
    for node in ast.walk(func_node):
        value: ast.expr | None = None
        targets: list[ast.expr] = []

        if isinstance(node, ast.Assign):
            value = node.value
            targets = node.targets
        elif isinstance(node, ast.AnnAssign) and node.value:
            value = node.value
            targets = [node.target]

        if value is None or not _expr_involves_db(value):
            continue

        for target in targets:
            if isinstance(target, ast.Name):
                db_vars.add(target.id)
            elif isinstance(target, ast.Tuple):
                for elt in target.elts:
                    if isinstance(elt, ast.Name):
                        db_vars.add(elt.id)
    return db_vars


def _is_db_iterable(
    expr: ast.expr | None,
    db_vars: set[str],
) -> bool:
    """Classify whether a loop iterable comes from a DB source.

    Returns True (potential N+1) when the data is DB-sourced or unknown.
    Returns False when the iterable is clearly local/in-memory.
    """
    if expr is None:
        return True

    if isinstance(expr, ast.Name):
        return expr.id in db_vars

    if _is_session_query_call(expr):
        return True

    if isinstance(expr, ast.Call):
        func = expr.func
        if isinstance(func, ast.Attribute):
            if func.attr in ("all", "unique", "first", "one", "tuples"):
                return _is_db_iterable(func.value, db_vars)
            if func.attr in ("values", "items", "keys"):
                return False
        if isinstance(func, ast.Name):
            if func.id in ("list", "tuple", "set"):
                return _is_db_iterable(expr.args[0], db_vars) if expr.args else False
            if func.id in ("range", "enumerate", "zip", "sorted", "reversed"):
                return False

    if isinstance(expr, ast.Attribute) and isinstance(expr.value, ast.Name):
        return expr.value.id in db_vars

    return True


# ---------------------------------------------------------------------------
# Shared: loop body extraction (excludes first generator iterable)
# ---------------------------------------------------------------------------


def _get_loop_body_nodes(loop_node: ast.AST) -> list[ast.AST]:
    """Return the AST subtrees that represent the loop *body*.

    The first generator's iterable is excluded — it is evaluated once,
    so accessing it is a single lazy load, not N+1.  Inner generators'
    iterables ARE included since they run per-item of the outer generator.
    """
    if isinstance(loop_node, ast.For):
        return loop_node.body

    if isinstance(loop_node, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
        nodes: list[ast.AST] = [loop_node.elt]
        for i, gen in enumerate(loop_node.generators):
            if i > 0:
                nodes.append(gen.iter)
            nodes.extend(gen.ifs)
        return nodes

    if isinstance(loop_node, ast.DictComp):
        nodes: list[ast.AST] = [loop_node.key, loop_node.value]
        for i, gen in enumerate(loop_node.generators):
            if i > 0:
                nodes.append(gen.iter)
            nodes.extend(gen.ifs)
        return nodes

    return []


def _get_first_iterable(loop_node: ast.AST) -> ast.expr | None:
    if isinstance(loop_node, ast.For):
        return loop_node.iter
    if isinstance(
        loop_node, (ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)
    ):
        return loop_node.generators[0].iter if loop_node.generators else None
    return None


# ---------------------------------------------------------------------------
# QueryInLoopDetector
# ---------------------------------------------------------------------------


class QueryInLoopDetector:
    """Detect ``session.get/execute/scalars/scalar`` calls inside loops.

    Handles two cases:
    1. Direct: the call is literally inside a for-loop or comprehension body.
    2. Indirect: a local function containing the call is invoked from a loop.

    Does NOT flag session calls used as the iterable of a loop (iterating
    over one query's result set is normal, not N+1).
    """

    def detect(
        self,
        filepath: Path,
        tree: ast.Module,
        parent_map: dict[int, ast.AST],
    ) -> list[Violation]:
        source_lines = filepath.read_text().splitlines()
        violations: list[Violation] = []

        local_func_session_calls = self._collect_local_func_session_calls(tree)

        for loop_node in _iter_loop_nodes(tree):
            body_nodes = _get_loop_body_nodes(loop_node)
            func_node = _find_enclosing_funcdef(parent_map, loop_node)
            func_name = (
                _get_func_qualname(parent_map, func_node) if func_node else "<module>"
            )

            for body_node in body_nodes:
                for node in ast.walk(body_node):
                    if _is_session_query_call(node):
                        if _is_suppressed(source_lines, node.lineno):
                            continue
                        if self._is_inside_nested_funcdef(loop_node, node):
                            continue
                        violations.append(
                            Violation(
                                filepath=str(filepath),
                                line=node.lineno,
                                category="session query inside loop",
                                code_snippet=_get_source_line(
                                    source_lines, node.lineno
                                ),
                                suggestion="Batch-load with a single WHERE IN query before the loop",
                                func_name=func_name,
                            )
                        )

                    if isinstance(node, ast.Call) and isinstance(
                        node.func, ast.Name
                    ):
                        func_id = node.func.id
                        if func_id in local_func_session_calls:
                            call_lines = local_func_session_calls[func_id]
                            for call_line in call_lines:
                                if _is_suppressed(source_lines, call_line):
                                    continue
                                violations.append(
                                    Violation(
                                        filepath=str(filepath),
                                        line=call_line,
                                        category="session query inside loop (via function call)",
                                        code_snippet=_get_source_line(
                                            source_lines, call_line
                                        ),
                                        suggestion=(
                                            f"Function `{func_id}` is called in a loop "
                                            f"and contains a session query — batch-load before the loop"
                                        ),
                                        func_name=func_name,
                                    )
                                )

        return _deduplicate(violations)

    def _is_inside_nested_funcdef(
        self, loop_node: ast.AST, target: ast.AST
    ) -> bool:
        for node in ast.walk(loop_node):
            if isinstance(node, ast.FunctionDef) and _node_contains(node, target):
                return True
        return False

    def _collect_local_func_session_calls(
        self, tree: ast.Module
    ) -> dict[str, list[int]]:
        result: dict[str, list[int]] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for child in ast.walk(node):
                if isinstance(child, ast.FunctionDef) and child is not node:
                    lines: list[int] = []
                    for inner in ast.walk(child):
                        if _is_session_query_call(inner):
                            lines.append(inner.lineno)
                    if lines:
                        result[child.name] = lines
        return result


# ---------------------------------------------------------------------------
# LazyRelationshipDetector
# ---------------------------------------------------------------------------


class LazyRelationshipDetector:
    """Detect lazy relationship attribute access inside loops over DB data.

    Only flags when the loop iterates over data that originates from a
    database query (session call result or variable assigned from one).
    Loops over local/in-memory collections are skipped.

    Also skips:
    - Attributes in Store context (assignments like ``obj.rel = value``)
    - Attributes eagerly loaded (``joinedload``/``selectinload``) in scope
    - First generator iterable (evaluated once, not N+1)
    """

    def __init__(self, relationships: dict[str, list[str]]) -> None:
        self._all_relationship_attrs: set[str] = set()
        self._attr_to_models: dict[str, list[str]] = {}
        for model, attrs in relationships.items():
            for attr in attrs:
                self._all_relationship_attrs.add(attr)
                self._attr_to_models.setdefault(attr, []).append(model)

    def detect(
        self,
        filepath: Path,
        tree: ast.Module,
        parent_map: dict[int, ast.AST],
    ) -> list[Violation]:
        source_lines = filepath.read_text().splitlines()
        violations: list[Violation] = []
        func_cache: dict[int, tuple[set[str], set[str], str]] = {}

        for loop_node in _iter_loop_nodes(tree):
            func_node = _find_enclosing_funcdef(parent_map, loop_node)

            if func_node:
                fid = id(func_node)
                if fid not in func_cache:
                    func_cache[fid] = (
                        _collect_db_variables(func_node),
                        _collect_eager_loaded_attrs(func_node),
                        _get_func_qualname(parent_map, func_node),
                    )
                db_vars, eager_attrs, func_name = func_cache[fid]
            else:
                db_vars, eager_attrs, func_name = set(), set(), "<module>"

            iterable = _get_first_iterable(loop_node)
            if not _is_db_iterable(iterable, db_vars):
                continue

            body_nodes = _get_loop_body_nodes(loop_node)

            for body_node in body_nodes:
                for node in ast.walk(body_node):
                    if not isinstance(node, ast.Attribute):
                        continue
                    if node.attr not in self._all_relationship_attrs:
                        continue
                    if isinstance(node.ctx, ast.Store):
                        continue
                    if node.attr in eager_attrs:
                        continue
                    if _is_suppressed(source_lines, node.lineno):
                        continue

                    models = self._attr_to_models.get(node.attr, [])
                    model_hint = f" ({', '.join(models)})" if models else ""

                    violations.append(
                        Violation(
                            filepath=str(filepath),
                            line=node.lineno,
                            category="lazy relationship access in loop",
                            code_snippet=_get_source_line(
                                source_lines, node.lineno
                            ),
                            suggestion=(
                                f"`.{node.attr}`{model_hint} is lazy-loaded — "
                                f"add selectinload/joinedload to the query, "
                                f"or batch-load before the loop"
                            ),
                            func_name=func_name,
                        )
                    )

        return _deduplicate(violations)


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


def _deduplicate(violations: list[Violation]) -> list[Violation]:
    seen: set[tuple[str, int, str]] = set()
    result: list[Violation] = []
    for v in violations:
        key = (v.filepath, v.line, v.category)
        if key not in seen:
            seen.add(key)
            result.append(v)
    return result
