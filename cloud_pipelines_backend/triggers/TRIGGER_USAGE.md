# Trigger subscriptions

Subscriptions start a saved pipeline when their event condition is satisfied. The API accepts
`all` and `any` branches containing named event leaves. A leaf may set `expire_seconds` to
require a recent emission. The stored definition is validated before a write.

## Create and inspect

`POST /api/triggers/subscriptions` creates a subscription owned by the authenticated caller:

```json
{
  "name": "nightly-report",
  "condition": {"op": "all", "children": [{"event": "orders-ready"}, {"event": "refunds-ready"}]},
  "pipeline_task_spec_from_user_pipeline_id": "0123456789abcdef0123"
}
```

The target must be a live saved pipeline owned by the caller. An optional
`pipeline_task_spec_from_user_pipeline_version_key` pins a version; otherwise execution
follows the current version. `pipeline_templates` supplies an argument-template envelope.

`GET /api/triggers/subscriptions` lists subscriptions with keyset pagination through
`page_size` and `page_token`, and supports filtering by enabled state and event name.
`GET /api/triggers/subscriptions/{subscription_id}` returns the subscription, live event
emissions, missing events and the most recent triggered cycle and time.

## Edit and delete

`PATCH /api/triggers/subscriptions/{subscription_id}` edits name, condition, enabled state,
target/version pin or templates. Only the creator or an administrator may edit or delete.
Changing the condition, re-enabling or changing the target re-evaluates events already received
and may start a run immediately. The response states whether it triggered, why, and the run ID.

`DELETE /api/triggers/subscriptions/{subscription_id}` removes the subscription and pending
event states; prior trigger history remains.

The same detail, edit and delete operations are available at
`/api/triggers/subscriptions/lookup?name=nightly-report`. An optional `created_by` query
parameter selects another principal's namespace; when absent, the caller is used.

## Runtime integration

The emission producer records opted-in execution status changes in the same transaction as
the status change. The consumer's readiness handler records arrivals through
`StartPipelineRunSink` and evaluates subscribed conditions. Configure the producer, consumer
and their sessions against the same database. Inject a configured `UserPipelineService` into
the trigger routes and readiness sink when the application supplies saved-run hooks.

Trigger history and its pipeline run are written atomically. Arrival processing is idempotent,
and a bounded fan-out that cannot finish remains eligible for redelivery. Disabled subscriptions
still receive events but do not start runs. See [the architecture](docs/TRIGGER_ARCHITECTURE.md)
for the condition evaluation, cycle fence and transaction boundaries.
