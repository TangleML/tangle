# Quota groups

Cap how many of your pipeline's nodes run at once. Put one annotation on a node, create the
group, and the gate admits up to `capacity` members and parks the rest in FIFO order.

Opt-in only: no annotation, no gating. The states the gate and sink move a node through are in
[STATES.md](STATES.md) — this page is how to use them.

## The annotation

One key, on the node's `TaskSpec.annotations`. Defined at `emissions/handlers/quota/annotations.py:51`.

| Key | Value | Effect |
|---|---|---|
| `tangleml.com/orchestration/quota-group` | the group name | the node joins that group |

```python
TaskSpec(
    annotations={"tangleml.com/orchestration/quota-group": "ml-training-shared"},
    ...
)
```

Blank, whitespace-only, or absent means ungated. **A name that matches no existing group is
also ungated** — the node launches and writes no claim (`quota/interceptor.py:78`). Create the
group before submitting, or the cap silently does not apply.

## The group

Create it once; it outlives any run.

```bash
curl -X POST https://api.example.com/api/quota_groups \
  -H 'Content-Type: application/json' \
  -d '{"name": "ml-training-shared", "capacity": 4}'
```

| Field | Rule |
|---|---|
| `name` | 1–63 chars, `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, unique |
| `capacity` | integer `>= 0`. `0` pauses the group: nothing new is admitted |

## Endpoints

Base `/api/quota_groups`; instance `/api/quota_groups/{key_kind}/{key}` where `key_kind` is
`name` or `id` (`quota/api_routes.py:49`). The paths below use `name`.

| Method | Path | Does |
|---|---|---|
| `POST` | `/api/quota_groups` | create · **201** |
| `GET` | `/api/quota_groups` | list · `page_size` 1–100 (default 10), `page_token` |
| `GET` | `/api/quota_groups/name/{name}` | one group, live claims inlined |
| `PATCH` | `/api/quota_groups/name/{name}` | change `capacity` |
| `DELETE` | `/api/quota_groups/name/{name}` | delete · `?force=true` |
| `GET` | `/api/quota_groups/name/{name}/claims` | the queue, oldest first |
| `DELETE` | `/api/quota_groups/name/{name}/claims/{execution_node_id}` | release one slot by hand |
| `POST` | `/api/quota_groups/name/{name}/promote` | advisory promotion pass |

## Errors

| Status | When |
|---|---|
| **403** | you are not the group's `created_by` |
| **404** | no such group, or the node holds no claim in it |
| **409** | the name already exists; or `DELETE` while a member is still active — retry with `?force=true` |
| **422** | bad `name`/`capacity`, or a malformed `page_token` |

## Capacity changes

```
  raise 2 -> 4    admits immediately, waiters drain up to the new cap
  lower 4 -> 2    binds the NEXT admission only
  lower to 0      pauses the group
```

**A lower capacity never kills anything already running.** A group at 4 members lowered to 2
stays legitimately over its cap until two members finish.

## Deleting a group

`DELETE` un-parks every waiter first, then cascades the claims — one transaction. Waiters are
released to run **ungated**, not cancelled.

```
  DELETE without force, a member still active   ->  409, nothing changed
  DELETE ?force=true                            ->  200 {"released": N}
```

## Promotion is advisory

`POST .../promote` makes parked nodes visible again; **it grants nothing**. The gate decides
again when the orchestrator next sees them, so promoting into a full group promotes zero.

```json
{ "capacity": 4, "occupancy_before": 4, "promoted": 0,
  "waiters_examined": 2, "waiters_unexamined": 0, "nodes": [ ... ] }
```

`waiters_unexamined: 0` means the pass saw the whole queue. Non-zero means the report was
truncated, not that the queue is stuck.

Routine operation needs none of this — a member finishing triggers the sink automatically. Reach
for `promote` when a group looks stuck and you want the reason, waiter by waiter.

## Ordering

FIFO by `claim.created_at`, which survives re-parking — a node that is un-parked and re-parked
keeps its place. Occupancy is derived from the nodes themselves on every read, so there is no
counter to drift and nothing to reconcile after a crash.

## Gotchas

| | |
|---|---|
| group named but not created | node runs **ungated**, no claim row |
| capacity lowered below occupancy | legal; nothing is killed |
| deleted group with waiters | waiters run ungated |
| chained node upstream of a member | takes no claim until its upstream finishes |
| claim rows accumulate | the table is a ledger: one terminal row per node that ever used the group |
