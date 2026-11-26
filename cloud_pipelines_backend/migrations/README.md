# Database migrations

Schema changes to `cloud_pipelines_backend/backend_types_sql.py` are applied to
existing databases by [Alembic](https://alembic.sqlalchemy.org/) revisions in
[`versions/`](versions/).

Migrations run automatically on application startup, via
`database_ops.upgrade_db_schema` (called from `initialize_and_migrate_db` and
`create_db_engine_and_migrate_db`). Nothing extra needs to be done at deploy
time.

## Adding a migration

1. Edit the models in `backend_types_sql.py`.
2. Point `DATABASE_URI` at a database that is already at `head` and
   autogenerate the revision. This project uses sequential revision ids, so
   pass the next one explicitly:

   ```sh
   alembic revision --autogenerate -m "Add the foo column" --rev-id 0003
   ```

3. **Review and edit the generated script.** Autogenerate is a starting point,
   not an oracle: it does not detect renames (it emits a drop plus an add,
   which loses data), and it cannot know what to backfill into a new column.
4. Check the SQL that will run against production without applying it:

   ```sh
   alembic upgrade head --sql
   ```

5. Add a test to `tests/test_migrations.py` if the revision does anything
   beyond plain DDL.

Keep identifiers (table, column and especially index names) at **63 characters
or fewer** — that is PostgreSQL's limit; MySQL's is 64. MySQL raises
`ERROR 1059: Identifier name is too long`, while PostgreSQL silently truncates,
which makes a later `DROP INDEX` fail to match.

## Databases created before Alembic was introduced

Revision `0001` is a baseline that reproduces the schema as it stood just
before Alembic was added. Databases that predate Alembic have no
`alembic_version` table, so `upgrade_db_schema` detects them, brings them up to
the baseline through the pre-existing idempotent fixups in `database_ops` and
`database_migrations`, then stamps them as `0001` before upgrading to `head`.
Fresh databases simply run every revision from scratch.
