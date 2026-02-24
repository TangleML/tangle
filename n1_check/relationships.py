"""AST parser to extract SQLAlchemy relationship metadata from backend_types_sql.py.

Parses the source file without importing it, so it works without a running DB
or any of the project's dependencies installed.
"""

import ast
from pathlib import Path


def _is_relationship_call(node: ast.expr) -> bool:
    """Return True if *node* is a call to ``orm.relationship(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr == "relationship":
        if isinstance(func.value, ast.Name) and func.value.id == "orm":
            return True
    return False


def _get_lazy_kwarg(call: ast.Call) -> str | None:
    """Extract the ``lazy=`` keyword argument value from a relationship call.

    Returns None when the keyword is absent (SQLAlchemy defaults to ``"select"``).
    """
    for kw in call.keywords:
        if kw.arg == "lazy" and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def extract_relationships(
    source_path: Path,
) -> dict[str, list[str]]:
    """Parse *source_path* and return ``{ClassName: [relationship_attr, ...]}``
    for every lazy-loaded relationship (``lazy="select"`` or default).
    """
    source = source_path.read_text()
    tree = ast.parse(source, filename=str(source_path))

    result: dict[str, list[str]] = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        lazy_attrs: list[str] = []

        for item in node.body:
            if isinstance(item, ast.AnnAssign) and item.value is not None:
                rel_call = _find_relationship_in_expr(item.value)
                if rel_call is None:
                    continue

                lazy = _get_lazy_kwarg(rel_call)
                if lazy is None or lazy == "select":
                    if isinstance(item.target, ast.Name):
                        lazy_attrs.append(item.target.id)

        if lazy_attrs:
            result[node.name] = lazy_attrs

    return result


def _find_relationship_in_expr(expr: ast.expr) -> ast.Call | None:
    """Find an ``orm.relationship(...)`` call in an expression.

    Handles plain calls and wrapped forms like
    ``orm.relationship(...)`` inside ``orm.Mapped[...] = orm.relationship(...)``.
    """
    if _is_relationship_call(expr):
        return expr
    if isinstance(expr, ast.Call):
        for arg in expr.args:
            found = _find_relationship_in_expr(arg)
            if found:
                return found
    return None
