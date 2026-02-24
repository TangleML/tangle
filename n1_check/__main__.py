"""CLI entry point for the N+1 static analyzer.

Usage:
    uv run python -m n1_check [FILES...]
    uv run n1-check [FILES...]

If no files are given, defaults to cloud_pipelines_backend/api_server_sql.py.
"""

import ast
import sys
from pathlib import Path

from n1_check.detectors import (
    LazyRelationshipDetector,
    QueryInLoopDetector,
    Violation,
    _build_parent_map,
)
from n1_check.relationships import extract_relationships

DEFAULT_TARGETS = ["cloud_pipelines_backend/api_server_sql.py"]
MODELS_FILE = "cloud_pipelines_backend/backend_types_sql.py"

RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def _print_header() -> None:
    print(f"\n{BOLD}N+1 Query Analysis{RESET}")
    print("=" * 40)
    print()


def _print_violation(v: Violation) -> None:
    filepath = Path(v.filepath).name
    location = f"{filepath}:{v.line}"
    if v.func_name:
        location += f"  in {BOLD}{v.func_name}{RESET}"
    print(f"  {YELLOW}WARN{RESET}  {location}")
    print(f"    {v.category}")
    print(f"    --> {v.code_snippet}")
    print(f"    {DIM}Fix: {v.suggestion}{RESET}")
    print()


def _print_summary(count: int, files_scanned: int) -> None:
    if count == 0:
        print(f"  {GREEN}No N+1 patterns detected.{RESET}  ({files_scanned} file{'s' if files_scanned != 1 else ''} scanned)\n")
    else:
        label = "pattern" if count == 1 else "patterns"
        print(f"  {RED}Found {count} potential N+1 {label}.{RESET}  ({files_scanned} file{'s' if files_scanned != 1 else ''} scanned)")
        print(f"  {DIM}Suppress false positives with an inline  # n1-ok  comment.{RESET}\n")


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    target_files = args if args else DEFAULT_TARGETS

    models_path = Path(MODELS_FILE)
    if not models_path.exists():
        print(f"Error: models file not found: {MODELS_FILE}", file=sys.stderr)
        print("Run this command from the backend/ directory.", file=sys.stderr)
        return 2

    relationships = extract_relationships(models_path)
    query_detector = QueryInLoopDetector()
    rel_detector = LazyRelationshipDetector(relationships)

    all_violations: list[Violation] = []
    files_scanned = 0

    for target in target_files:
        target_path = Path(target)
        if not target_path.exists():
            print(f"Warning: file not found, skipping: {target}", file=sys.stderr)
            continue

        source = target_path.read_text()
        tree = ast.parse(source, filename=target)
        parent_map = _build_parent_map(tree)
        files_scanned += 1

        all_violations.extend(query_detector.detect(target_path, tree, parent_map))
        all_violations.extend(rel_detector.detect(target_path, tree, parent_map))

    all_violations.sort(key=lambda v: (v.filepath, v.line))

    _print_header()
    for v in all_violations:
        _print_violation(v)
    _print_summary(len(all_violations), files_scanned)

    return 1 if all_violations else 0


if __name__ == "__main__":
    sys.exit(main())
