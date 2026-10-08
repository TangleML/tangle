# `start_pipeline_run` sink — behaviour reference

The emission path's one write into the trigger tables. A readiness signal arrives, the sink
records it against every subscription waiting on that event name, and asks whether any of
their conditions now hold.

It decides almost nothing itself. `triggers.service.record_event_and_maybe_start_runs` writes
the event state and per subscription calls `maybe_trigger` — the same call a `PATCH` makes,
so an arrival and an edit can never disagree about what "satisfied" means.

```
emission  ──▶  StartPipelineRunSink.emit(intent, emission_event_id)
                    │  one session per emit, up to 3 attempts
                    ▼
              record_event_and_maybe_start_runs(event_name, emission_event_id)
                    │  per subscription, in its own transaction
                    ├── fill(event_state)          ← always, enabled or not
                    └── maybe_trigger(subscription) ← withholds the run if disabled
```

## What starts a run

Three routes, and only the first involves an emission. The other two are `PATCH` calls that
re-evaluate the condition against arrivals this sink banked earlier.

| Route | Decided in | Needs a new event? |
| --- | --- | --- |
| An arrival completes the condition | `record_event_and_maybe_start_runs` -> `maybe_trigger` | Yes |
| A subscription is switched back on | `rename_subscription`/`update_subscription`, off->on | No |
| An edit leaves a condition that already holds | `update_subscription` | No |

## Verdicts

| Status | When | `detail` |
| --- | --- | --- |
| `SUCCESS` | At least one subscription was found and written to | `subscriptions: [{subscription_id, triggered, cycle, reason}]` |
| `IGNORE` | No subscription waits on this event name | `reason: no_subscription` |
| `FAIL` | The seek, or the commit before the retry loop, kept failing after 3 attempts — nothing was recorded | `error: repr(...)` |
| `FAIL` | Part of the fan-out never recorded: some subscriptions were written, some stayed contended | `reason: fan_out_incomplete`, `recorded: [...]`, `deferred: [...]` |
| *no verdict* | The fan-out ran out of its time budget | raises `DeliveryIncomplete`; see below |

The two `FAIL` details are disjoint, and which one a delivery row carries says how much of the
fan-out landed. The first has no `reason` and nothing was written; the second has no `error` and
names both halves, because recording the `FAIL` settles the emission — there is no redelivery
behind it, so that detail is the only trace of which subscriptions missed the signal. The last
row is not a verdict at all: nothing is written, the emission stays unsettled, and the rest of
the fan-out runs on the next delivery.

`SUCCESS` is the verdict whenever the arrival was recorded, even though usually nothing
triggers — recording it *is* the work. `IGNORE` means the sink found nothing to write to at
all. Why a given subscription did not start a run rides in its own `reason`, never in the
status.

## Per-subscription reasons

| `reason` | Meaning |
| --- | --- |
| `null`, with `triggered: true` | The condition held: a cycle was claimed and the event states cleared |
| `awaiting_events` | Recorded; the condition still needs other events |
| `subscription_disabled` | Recorded; the run is withheld because the subscription is off |
| `arrival_already_recorded` | This exact `emission_event_id` already landed on this event state, and it started nothing |
| `run_already_started`, with `triggered: true` | Same, except it *did* start a run — reported with that run's `pipeline_run_id` and the cycle it fired on |
| `arrival_superseded` | A *different*, older emission, arriving after the row had moved past it — dropped by the monotonic guard, so nothing it announced will run |
| `cycle_already_triggered` | Another writer claimed the same cycle first |
| `user_pipeline_deleted` | The condition held; the target pipeline is soft-deleted, or its pinned version is gone |
| `target_unbuildable` | The condition held; the target is alive but its stored task spec no longer parses |
| `run_start_failed` | The condition held; the run build raised something unanticipated — read as a bug |

The last three are the FAIL verdict below, not a quiet SUCCESS: the arrival committed and no
run exists anywhere. They are `triggers.service.RUN_NOT_STARTED_REASONS`, and both this sink's
post-retry recompute of `failed` and the fan-out's own classification read that set.

| Reason | Recovered by |
| --- | --- |
| `user_pipeline_deleted` | The subscriber repointing the subscription at a live pipeline |
| `target_unbuildable` | The pipeline's owner re-saving it against the current schema |
| `run_start_failed` | On-call, from the traceback the trigger logged |

In each case the banked arrival and the unspent cycle are what make recovery possible: the
next write that re-evaluates finds the condition still satisfied and starts the run then.

## The fan-out's time budget

One emission's fan-out may keep taking on new subscriptions for `_FAN_OUT_BUDGET_SECONDS`
(30s), shared across the sink's three attempts. Past that it stops and raises
`DeliveryIncomplete`, which is *not* a verdict: the emission row is left claimed and unsettled,
its claim lease runs out, and another consumer picks it up and carries on.

```
  pass 1   s1 ✓  s2 ✓  s3 ✓   [30s gone]  s4 ✗  s5 ✗   ── raise, nothing settled
           │
           └─ each committed its own transaction: s1..s3 are durable

  pass 2   s1 ·  s2 ·  s3 ·               s4 ✓  s5 ✓   ── settle COMPLETE
           │                                │
           └─ two reads each: `fill` knows   └─ the budget went to the ones still waiting
              the emission, so the answer
              is `arrival_already_recorded`
              or `run_already_started`
```

Three properties make this safe rather than merely tidy:

| Property | Why it holds |
| --- | --- |
| Nothing is half-written | The check sits *between* subscriptions; the one in flight always finishes |
| No infinite redelivery | At least one subscription is served per pass (`index > 0`), so N subscriptions take at most N passes |
| No double run | The list is re-derived each pass, and a subscription already served answers off `last_emission_event_id` |

The cost is latency for the subscriptions not reached — one claim lease each pass, currently
300s. The budget is deliberately well under that lease, so the subscription in flight when it
expires *and* the settle after it both land while this consumer still holds the row.

## Disabled subscriptions

`enabled: false` **withholds the run — it does not stop the listening, and it never clears.**

| Question | Answer |
| --- | --- |
| Does a disabled subscription still record arrivals? | **Yes.** `fill` runs exactly as it would for a live one |
| Does disabling clear what was already recorded? | **No.** Only a successful trigger clears event states |
| Does the delivery report `IGNORE`? | **No** — `SUCCESS`, with `subscription_disabled` per subscription |
| Can re-enabling start a run immediately? | **Yes**, if the condition is satisfied by then |

Re-enabling is evaluated by `rename_subscription`/`update_subscription` on the off→on
transition. That evaluation is load-bearing: once every event is filled, nothing further will
arrive to prompt a re-check, so without it a re-enabled subscription would sit satisfied and
dormant forever.

Expiry keeps running while a subscription is off, so events that aged out during the off
period do not count towards the condition when it comes back.

## Edits are judged against the state they leave

An edit is evaluated against the condition it produces and the arrivals already on the rows —
never against the condition it found. Several edits therefore start a run with no emission
anywhere near them.

| Edit | Effect on arrivals the sink recorded |
| --- | --- |
| Drop an event still waiting — `all(A, B)` -> `all(A)` with A filled | The condition now holds: triggers on the spot |
| Add an event | Nothing has filled it, so the trigger is held back |
| Remove an event | Its event state is deleted with it, arrival and all |
| Lengthen `expire_seconds` | `expires_at` is recomputed from the arrival's own `filled_at`, so a lapsed arrival comes back and can trigger |
| Shorten `expire_seconds` | The same recompute the other way: `expires_at` can land in the past, and the banked arrival stops counting |

Expiry is a `WHERE` clause on the read, not a delete, and nothing sweeps the row — which is
why a window can be widened after the fact and the arrival is still there to revive.

## Idempotency

Two independent mechanisms, because they cover different windows:

| Mechanism | Covers |
| --- | --- |
| `last_emission_event_id` on the event state | A redelivery of the *same* emission: `fill` is a no-op. Survives `clear` on purpose, so it still holds once a trigger has advanced the cycle |
| `UNIQUE (subscription_id, cycle)` on `trigger_history` | Two writers racing the same cycle: the loser reports `cycle_already_triggered` |

A *new* emission id on an already-filled event state is not a duplicate — but "new" is decided
by the id's millisecond prefix, not by arrival order. The later emission wins and both
`filled_at` and `expires_at` move with it; one from an *older* millisecond is refused, and
reported as `arrival_superseded`. Refusing it is what stops a straggler from dragging the
expiry window backwards and aging out an arrival that is still live. Two emissions minted in
the same millisecond are both accepted: the prefix cannot order them, and dropping one of two
genuine signals is worse than the duplicate.

### Refusing a replay, and still telling the truth about it

A redelivery is not a random event — it is what happens when the *outcome write itself* failed.
The recorder raises `RecorderUnavailable`, the emission is left unsettled, and it comes back.
So the delivery answering for an already-started run is precisely the one whose answer becomes
the permanent ledger row.

```
  delivery 1   fill -> True   condition holds   RUN R started, committed
               recorder.record(...)  ->  BOOM, emission left unsettled

  delivery 2   fill -> False
                 |
                 +-- did this emission start a run?  (trigger_history, by emission id)
                        |
                        +-- yes -> triggered: true,  run_already_started, run R, cycle N
                        +-- no  -> triggered: false, arrival_already_recorded
                 |
               recorder.record(...)  ->  commits the truth
```

The lookup is `triggers.service._run_started_by`: an indexed prefix seek on the fence's
`UNIQUE (subscription_id, cycle)`, newest cycle first, bounded by `_NUM_PAST_CYCLES_TO_SCAN`,
then the `triggered_by` match in Python — that column is JSON, with no index and no portable
containment operator across SQLite, MySQL and Postgres. Out of range it degrades to
`arrival_already_recorded`, the answer it would have given anyway.

## Failure handling

Each subscription is its own transaction, committed or rolled back whole. Rows are taken
`FOR UPDATE` in one order everywhere — subscription, then event state, then history — and
subscriptions are visited in id order, so an arrival and a `PATCH` cannot deadlock by
approaching the same two rows from opposite ends.

Retries are in-process — 3 attempts, a backoff of 50 ms × the attempt number (so 50 ms then
100 ms), a fresh session each time — because
nothing above the sink will retry: a raise out of a sink becomes a failed verdict and the
emission row is settled, so there is no redelivery to fall back on. Only contention-shaped
failures are retried (`OperationalError`, `InterfaceError`, `InternalError`, `TimeoutError`);
a value the column rejects would fail identically every time.

## The permanent failures

Every other verdict this sink reports either retries or is an ordinary outcome. The three
`RUN_NOT_STARTED_REASONS` are neither: the arrivals commit, the run is refused, and the
delivery is `fail` with `runs_not_started`. Recording a `fail` settles the emission, so there
is no redelivery — the subscription is stopped until the owner named in the recovery table
above acts.

```
  arrival ---> committed, and kept          <-- not rolled back with the run
  run     ---> refused: user_pipeline_deleted | target_unbuildable | run_start_failed
  emission --> FAIL, settled, no redelivery
                    |
  the repair -------+--> the still-satisfied condition re-evaluates, the run starts
```

The soft-deleted target is the walked-through case below; the other two differ only in who
performs the repair.

The detail names the subscription in `recorded` *and* in `failed`, which is the distinction it
exists to carry: the arrival landed, the run did not. Keeping the arrival is what makes the
`PATCH` a recovery rather than a request for a fresh emission. `trigger.run_not_started`
counts it, and `T22` in `tests/e2e/emissions/run_tests.py` walks the whole loop.

## See also

| Doc | For |
| --- | --- |
| `triggers/TRIGGER_USAGE.md` | Writing a subscription and its condition |
| `triggers/docs/TRIGGER_ARCHITECTURE.md` | The trigger tables and the evaluation model |
| `emissions/docs/EMISSION_ARCHITECTURE.md` | How an emission reaches a sink |
