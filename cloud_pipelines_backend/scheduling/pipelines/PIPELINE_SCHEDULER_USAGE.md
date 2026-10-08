# Pipeline Scheduler — API Usage Guide

All endpoints are under `/api/schedules/pipelines` and tagged `pipeline-scheduling` in Swagger UI.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/schedules/pipelines` | Create a pipeline schedule |
| GET | `/api/schedules/pipelines` | List schedules (paginated) |
| GET | `/api/schedules/pipelines/{id}` | Get schedule details |
| PATCH | `/api/schedules/pipelines/{id}` | Update a schedule |
| DELETE | `/api/schedules/pipelines/{id}` | Delete a schedule |
| POST | `/api/schedules/pipelines/{id}/trigger` | Trigger immediately |

## Create a Schedule

```http
POST /api/schedules/pipelines
```

```jsonc
{
  "name": "Daily ETL",                        // required
  "pipeline_task_spec": { ... },              // required — fully hydrated pipeline YAML as JSON dict
  "cron_expression": "0 9 * * MON-FRI",       // required — 5-field or 6-field
  "timezone": "UTC",                           // optional, default "UTC"
  "pipeline_templates": {                      // optional — argument templates
    "arguments": {
      "as_of_date": "{{ schedule_time | shift('-1d') | date }}"
    }
  }
}
```

Writes to the `scheduled_pipeline_run` DB table and registers the job with APScheduler. The schedule starts firing immediately at the specified cron times.

### Argument Templates

`pipeline_templates.arguments` maps a **declared root input** of the pipeline to a template
rendered afresh at every fire, so one schedule can pass a different `as_of_date` each morning
without an edit. A cron schedule is the only kind that can use `schedule_time`; the grammar, the
full source and operator list, the error messages and the worked examples are in
[Argument Templates — API Usage Guide](../../templating/arguments/ARGUMENT_TEMPLATES_USAGE.md).

Two things about this envelope, both easy to get wrong:

| you send | what happens to the stored templates |
| --- | --- |
| `"pipeline_templates": {"arguments": {"a": "…", "b": "…"}}` | replaced by exactly those two keys |
| `"pipeline_templates": {"arguments": {"a": "…"}}` | **`b` is gone** — this is an assignment, not a merge |
| `"pipeline_templates": {"arguments": {}}` | all templates cleared |
| an envelope with no `arguments` key | left alone |

A `PATCH` that drops a template is not an error and nothing fails: the input falls back to its
stored value, so a run that lost a template and a run that never had one look identical. Send the
whole map every time you change one key.


## Update a Schedule

```http
PATCH /api/schedules/pipelines/{id}
```

```jsonc
{
  "name": "Nightly ETL",                // optional
  "cron_expression": "0 3 * * *",       // optional — updates cron trigger
  "timezone": "America/Toronto",        // optional
  "pipeline_task_spec": { ... },        // optional — fully hydrated pipeline YAML; replaces stored spec
  "paused": true                        // optional — true → pause, false → resume
}
```

All fields optional. Updates both the DB row and the APScheduler job. Changing `cron_expression` or `timezone` reschedules the job.

**Pause/resume behavior**: Unpausing a schedule will fire **exactly one** coalesced run if any triggers were missed while paused. Missed runs are never skipped, but they are collapsed into a single execution regardless of how long the schedule was paused.

## Get a Schedule

```http
GET /api/schedules/pipelines/{id}?include_spec=false
```

`include_spec` defaults to `false`. When `true`, the full `pipeline_task_spec` is included.

**Owner-scoped.** Reading someone else's schedule is `403`, the same answer `PATCH`, `DELETE` and
`POST .../trigger` give for that id; a missing schedule is `404`. Admins may read any schedule.

Addressing by `?schedule_path=` instead resolves only within the caller's own namespace, so a path
that exists for someone else is `404` rather than `403` — telling one caller that another owns a
given path would itself be a disclosure. Admins get no cross-user path resolver: a path names at most
one schedule *per owner*, so resolving one globally would be ambiguous.

```jsonc
{
  "id": "018f3a...",
  "name": "Daily ETL",
  "cron_expression": "0 9 * * MON-FRI",
  "timezone": "UTC",
  "paused": false,
  "created_by": "alice@example.com",
  "created_at": "2026-06-08T00:00:00Z",
  "updated_at": "2026-06-08T00:00:00Z",
  "last_run_at": null,
  "last_run_submission_result": null,
  "pipeline_task_spec_from_pipeline_run_id": null,
  "pipeline_task_spec_from_user_pipeline_id": null,
  "next_run_at": "2026-06-09T09:00:00Z"
}
```

When `include_spec=false` (default), the `pipeline_task_spec` field is omitted entirely from the response (not returned as `null`).

## List Schedules (Paginated)

```http
GET /api/schedules/pipelines?page_size=10&page_token=...
```

| Param | Type | Default | Constraints |
|-------|------|---------|-------------|
| `page_size` | int | 10 | min=1, max=100 |
| `page_token` | string | — | Cursor from previous `next_page_token` |

Uses cursor-based pagination ordered by `updated_at DESC, id DESC`. The cursor format is `updated_at~id` (e.g., `2026-06-09T09:00:00+00:00~018f3a...`).

`pipeline_task_spec` is always omitted in list responses (specs can be MBs).

**Owner-scoped**, including `total_count` and the cursor: the list returns only the caller's own
schedules, and pages through only those. Unlike the single-schedule read, admins are not exempt here
— an admin lists their own schedules and opens anyone else's by id.

```jsonc
{
  "schedules": [ ... ],
  "total_count": 5,
  "next_page_token": "2026-06-08T00:00:00+00:00~018f3a..."  // null on last page
}
```

To iterate all pages:

```python
page_token = None
while True:
    resp = client.get("/api/schedules/pipelines", params={
        "page_size": 20,
        "page_token": page_token,
    })
    data = resp.json()
    process(data["schedules"])
    page_token = data["next_page_token"]
    if page_token is None:
        break
```

## Delete a Schedule

```http
DELETE /api/schedules/pipelines/{id}
```

Returns `204 No Content`. Removes from both `scheduled_pipeline_run` and APScheduler.

## Manual Trigger

```http
POST /api/schedules/pipelines/{id}/trigger
```

Fires the schedule immediately regardless of cron timing. Executes with the same logic as a scheduled trigger (annotations, `created_by`).

⚠️ **Argument templates are the one exception.** A manual fire has no scheduled time, so
`schedule_time` is unavailable and every template naming it fails — per key, while the other keys
still render and the run still starts. Write `coalesce(schedule_time, trigger_time)` for a
template that must survive both paths; see
[Surviving a manual fire](../../templating/arguments/ARGUMENT_TEMPLATES_USAGE.md#surviving-a-manual-fire).

## Scheduling Annotations

When a schedule fires, the created pipeline run includes these annotations:

| Annotation | Value |
|-----------|-------|
| `tangleml.com/source/scheduler` | `"true"` — marks this run as scheduler-initiated |
| `tangleml.com/scheduling/id` | Schedule ID |
| `tangleml.com/scheduling/name` | Schedule name |
| `tangleml.com/scheduling/cron` | Cron expression with timezone, e.g. `"0 9 * * MON-FRI (UTC)"` |
| `tangleml.com/scheduling/updated_at` | ISO 8601 datetime |

## Timestamp Storage

All timestamps (`created_at`, `updated_at`, `last_run_at`, `next_run_at`) are stored and returned as **UTC**. `updated_at` reflects the last user-initiated change (PATCH API) and is **not** updated when a schedule fires via trigger API or cron.

- **PostgreSQL**: timestamps are stored with timezone metadata (`TIMESTAMP WITH TIME ZONE`).
- **MySQL / SQLite**: timezone metadata is not natively supported for datetime columns. The values are stored as correct UTC, and the application layer (`UtcDateTime` type) re-attaches `tzinfo=UTC` on read.

The `timezone` field on a schedule (e.g., `"America/Toronto"`) controls **when the cron fires**, not how timestamps are stored. All stored and returned timestamps are always UTC.

## Permissions

| Operation | Owner (creator) | Other user | Admin |
|-----------|:---:|:---:|:---:|
| **Create** | Yes | Yes | Yes |
| **Get** (by id) | Yes | 403 | Yes |
| **List** | Yes | not listed | own only |
| **Update** (PATCH) | Yes | 403 | Yes |
| **Delete** | Yes | 403 | Yes |
| **Trigger** | Yes | 403 | Yes |

Ownership is determined by matching the authenticated user's email against the schedule's `created_by` field. Admin users (defined in `ADMIN_USERS` in `app.py`) bypass the ownership check and can operate on any schedule *by id*; the list is owner-scoped for everyone, admins included.

**Owner identity is not case-sensitive.** `Jose@example.com` and `jose@example.com` are the same
person and address the same schedules. The comparison is the database's, not the application's:
nothing here lowercases or normalizes an owner name, so the exact rule is the one the deployment's
`created_by` column collation applies. On MySQL that folds case; note that a local SQLite database
compares byte-for-byte, so ownership looks case-sensitive in development and is not in staging.

This holds for **every** operation and both addressing modes. By-id routes previously compared the
owner in Python and so answered differently from by-path routes for the same caller — that is fixed;
they now authorize with an owner-scoped SQL predicate against the same column. It also covers the
`pipeline_task_spec_from_pipeline_run_id` reference check, so a schedule can reference a run the same
person created under a differently-cased spelling.

By-id routes still answer **403** for another user's schedule and **404** for one that does not
exist; by-path routes answer 404 for both, because a path only ever resolves inside the caller's own
namespace. That distinction is unchanged.

Paths are the opposite and deliberately so: `schedule_path` is case-**sensitive**, so `Foo/nightly`
and `foo/nightly` are two different schedules belonging to the same owner. See `SCHEDULER_DESIGN.md`
("Collation is verified, not assumed") for why the composite key is mixed.

## Cron Expression Format

- **5-field** (standard): `minute hour day month day_of_week` — e.g., `"0 9 * * MON-FRI"`
- **6-field** (with seconds): `second minute hour day month day_of_week` — e.g., `"0 0 * * * *"` (every hour)
- **Frequency limit**: Maximum **50 triggers per day**. Expressions that fire more frequently (e.g., `* * * * *` = every minute = 1440/day) are rejected with HTTP 422.
