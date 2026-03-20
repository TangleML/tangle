# Claude Code conventions for this repo

## Python style

- **f-strings for all logging.** Use `_logger.info(f"...")` — never `%s` interpolation.
- **Keyword-only arguments.** Add `*` to function definitions to make all parameters keyword-only, even when there is only one parameter.
- **Unused parameters.** Prefix unused function parameters with `_` (e.g. `_old_value`, `_initiator`) and add type hints to every parameter, used or not.
- **Type hints on all function parameters and return types.** Every function signature must be fully annotated.
- **StrEnum for known string sets.** When a parameter or constant is drawn from a fixed set of strings, define a `StrEnum` for it rather than using bare string literals.
- **Imports: modules only.** Import modules, not individual names — except for `from typing import ...`. Example: `import sqlalchemy` not `from sqlalchemy import text`.
- **File naming: plural or uncountable nouns.** Name Python files as plural or uncountable nouns. Example: `database_migrations.py` not `database_migrate.py`.

## SQLAlchemy

- **Derive schema metadata from models.** Never hardcode table names, column names, or column types as strings. Reference them from the SQLAlchemy model (e.g. `Model.__table__.c.column_name`) and compile types with `col.type.compile(dialect=engine.dialect)`.
- **Single transaction per logical operation.** Wrap all related DDL or DML mutations in one `with engine.connect() as conn` / `conn.commit()` block.
- **Log migrations and catch errors.** Every migration step (ALTER TABLE, backfill, index creation) should log before it runs, catch exceptions, log them, and handle `OperationalError` for concurrent deployments gracefully.

## Data safety

- **Never delete or overwrite data files without explicit confirmation.** Use `sqlite://` (in-memory) or a `tempfile` for smoke tests and migration verification — never reference or delete files under `data/`.
