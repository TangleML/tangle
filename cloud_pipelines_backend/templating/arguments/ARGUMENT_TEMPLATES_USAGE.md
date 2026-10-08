# Argument Templates — API Usage Guide

A template computes a pipeline argument at fire time from the clock. Templates are stored on a
pipeline schedule or a trigger subscription and rendered every time that row starts a run.

## Endpoints

Templates ride on the create, update and get bodies of both carriers; there is no separate
template endpoint.

| Carrier | Endpoint prefix |
|---------|-----------------|
| Pipeline schedule | `/api/schedules/pipelines` |
| Trigger subscription | `/api/triggers/subscriptions` |

## Request shape

```jsonc
{
  "name": "Daily ETL",
  "pipeline_task_spec": { },
  "cron_expression": "0 2 * * *",
  "pipeline_templates": {                              // optional
    "arguments": {                                     // required inside the envelope
      "as_of_date": "{{ schedule_time | date }}",      // key = pipeline input name
      "region": "ca-central-1"                         // a constant is a valid template
    }
  }
}
```

The `arguments` wrapper is required. A `pipeline_templates` body without it is accepted and
stores nothing — unknown envelope keys are ignored so the envelope can grow.

```jsonc
{ "pipeline_templates": { "as_of_date": "..." } }                    // 201, stores NOTHING
{ "pipeline_templates": { "arguments": { "as_of_date": "..." } } }   // 201, stored
{ "pipeline_templates": { "arguments": {} } }                        // 201, clears them
```

⚠️ A `GET` cannot tell you which of the first and third happened: both read back as
`{"arguments": {}}`. So a forgotten wrapper looks exactly like templates you meant to clear,
and the only evidence that a fire used no templates is the absence of
`tangleml.com/templating/argument/...` annotations on the run. Check the readback names your
keys before assuming a schedule is templated.

PATCH replaces the whole map. Send the full set to change one entry; omit the field to leave the
stored templates alone.

## Time sources

| Source | Is | Cron fire | Subscription | Manual trigger |
|--------|----|-----------|--------------|----------------|
| `schedule_time` | the cron time the run was due — not when it ran | yes | no | no |
| `trigger_time` | when the work was accepted; shared by every key in one render | yes | yes | yes |
| `now` | read at render, so it drifts within a fan-out | yes | yes | yes |

A cron schedule is *saved* as a cron schedule but *renders* as a manual one when fired by hand.
`POST /{id}/trigger` bypasses the scheduler, so `schedule_time` is absent there even though the
row has a cron expression.

Naming a source the fire does not supply fails that key. `coalesce(a, b)` takes the first source
the fire does supply, which is how a `schedule_time` template survives a manual trigger — see
[Surviving a manual fire](#surviving-a-manual-fire).

## Operators and formatters

A template is a source, then zero or more operators, then at most one formatter. An operator
after a formatter is rejected, and so is a second formatter.

| Operator | Argument | Does |
|----------|----------|------|
| `shift` | `'-1d'`, `'+2h'` | move the instant by an offset |
| `truncate_time` | `'1d'`, `'4h'` | go back that far, then floor to the unit |
| `timezone` | `'America/Toronto'` | reinterpret in a zone; the instant is unchanged |

Units for both: `s` `m` `h` `d` `w`.

`truncate_time(N<unit>)` is "go back N, then floor", not "floor to the last N-boundary":

```
{{ schedule_time | truncate_time('4h') | rfc3339 }}   on 02:00  ->  22:00 the previous day
```

The multiple moves the instant; the unit letter decides the floor. `truncate_time('90m')` floors
to the minute, not to a 90-minute boundary. More cases in the appendix.

A formatter turns the instant into the string the pipeline receives.

| Formatter | Produces |
|-----------|----------|
| *(none)* | the datetime's own text form — rarely what a pipeline input wants |
| `date` | the calendar date, `YYYY-MM-DD` |
| `rfc3339` | the full timestamp with offset, always six fractional digits |
| `epoch_seconds` | whole seconds since the epoch |

## Worked examples

Rendered against a cron fire due at `2026-03-06T02:00:00Z`, accepted at `02:17:43Z`:

```
{{ schedule_time }}                                         2026-03-06 02:00:00+00:00
{{ schedule_time | date }}                                  2026-03-06
{{ schedule_time | rfc3339 }}                               2026-03-06T02:00:00.000000+00:00
{{ schedule_time | epoch_seconds }}                         1772762400
{{ schedule_time | shift('-1d') | date }}                   2026-03-05
{{ schedule_time | truncate_time('1d') | date }}            2026-03-05
{{ schedule_time | truncate_time('4h') | rfc3339 }}         2026-03-05T22:00:00.000000+00:00
{{ schedule_time | timezone('America/Toronto') | rfc3339 }}  2026-03-05T21:00:00.000000-05:00
{{ trigger_time | rfc3339 }}                                2026-03-06T02:17:43.000000+00:00
{{ now | date }}                                            2026-03-06
ca-central-1                                                ca-central-1
```

A value is either a plain string or one `{{ }}` expression — never both. `run-{{ schedule_time |
epoch_seconds }}` is refused at save time.

## Surviving a manual fire

`coalesce` takes the first available source. It is the only way a `schedule_time` template keeps
working when the schedule is fired by hand.

```
                         {{ coalesce(schedule_time, trigger_time) | rfc3339 }}
cron fire, due 02:00  ->  2026-03-06T02:00:00.000000+00:00     schedule_time won
hand fire at 09:42    ->  2026-03-06T09:42:00.000000+00:00     fell through
```

Without it the key is left alone entirely:

```
                         {{ schedule_time | date }}
hand fire             ->  the argument keeps whatever the pipeline spec had
```

Every arm is checked at save time, so `coalesce(schedule_time, ...)` is rejected on a
subscription — no fire of that kind supplies one.

## What a failure does

**A template that fails to render never stops the run.** The pipeline runs regardless: the failed
argument keeps the value the pipeline spec gave it, the run is submitted and reported as
successful, and a schedule's cycle advances as usual. A render failure is a data-quality event,
not an error path.

Failure is also per key. Three good templates still render when a fourth fails.

| Case | Argument the pipeline receives |
|------|--------------------------------|
| Key renders | the rendered value |
| Key renders, argument already present | the rendered value — the template wins |
| Key renders empty | the empty value; this is not a failure |
| Key fails to render | unchanged, exactly as the spec had it |

The only signal is a warning log carrying the row id and the failed keys. A run can therefore
succeed against a stale default without anyone noticing.

## Errors

Every rejection is `422` with `detail` set to the message below. Validation is pure: no database
is read and no pipeline is loaded, so a malformed template is refused before anything is written.

| Cause | Template | `detail` |
|-------|----------|----------|
| Text mixed with an expression | `run-{{ schedule_time \| date }}` | `Invalid template for 'as_of_date': a value is either a plain string or one {{ }} expression, not a mix` |
| Unbalanced braces | `{{ schedule_time \| date` | `Invalid template for 'as_of_date': unexpected end of template, expected 'end of print statement'.` |
| Unknown source | `{{ yesterday \| date }}` | `Invalid template for 'as_of_date': unknown source 'yesterday'` |
| Unknown filter | `{{ now \| nope }}` | `Invalid template for 'as_of_date': unknown filter 'nope'` |
| Two formatters | `{{ now \| date \| rfc3339 }}` | `Invalid template for 'as_of_date': two formatters, 'date' and 'rfc3339'` |
| Operator after a formatter | `{{ now \| date \| shift('-1d') }}` | `Invalid template for 'as_of_date': operator 'shift' after formatter 'date'` |
| Formatter given an argument | `{{ now \| date('x') }}` | `Invalid template for 'as_of_date': date takes no argument` |
| Bad operator argument | `{{ now \| shift('banana') }}` | `Invalid template for 'as_of_date': shift('banana') is not an offset; expected e.g. '-2d'` |
| Unknown timezone | `{{ now \| timezone('Mars/Olympus') }}` | `Invalid template for 'as_of_date': unknown timezone 'Mars/Olympus'` |
| Source not available for the kind | `{{ schedule_time \| date }}` on a subscription | `Invalid template for 'as_of_date': 'schedule_time' is not available for a subscription; available sources are now, trigger_time` |

## Not validated

A template key is never checked against the pipeline it belongs to. Both consequences below are
accepted, and neither is caught when the template is saved.

**The key must name an input the pipeline declares.** A rendered argument is passed to run
submission like any other; if the pipeline has no input by that name, submission rejects it and
**the whole run fails to start**. Unlike a render failure, this is not per key and not survivable
— one template naming `region` on a pipeline without a `region` input stops every run from that
schedule. Check the spelling against the pipeline's inputs.

**A key may target a secret-backed input.** The rendered value replaces it, silently downgrading
the secret to a plain argument.

**The input must be a *root* input, and a root input is not ambient.** A template can only set
what the pipeline declares at its top level; declaring one there does not make it visible to a
task nested inside a subgraph. Reaching a nested consumer costs one declaration and one
`graphInput` wire *per level of nesting* between the root and that consumer, and until the chain
is wired the whole way the schedule has nothing to override. A pipeline three subgraphs deep
needs the input threaded through all three. It behaves like a function parameter, not a global:
there is no dynamic scoping and nothing is reachable by default.

## Appendix — `truncate_time` in detail

The multiple decides how far back; the unit letter decides the floor. Two specs sharing a unit
floor identically and differ only in distance, so `4h` and `5h` both land on an hour boundary.

Applied to `Wed 2026-09-02 09:41:37.123456` in `America/Toronto`:

| Spec | Result | Floors to |
|------|--------|-----------|
| `30s` | `Wed 2026-09-02 09:41:07.000000` | the second |
| `5m` | `Wed 2026-09-02 09:36:00.000000` | the minute |
| `90m` | `Wed 2026-09-02 08:11:00.000000` | the minute |
| `1h` | `Wed 2026-09-02 08:00:00.000000` | the hour |
| `4h` | `Wed 2026-09-02 05:00:00.000000` | the hour |
| `5h` | `Wed 2026-09-02 04:00:00.000000` | the hour |
| `1d` | `Tue 2026-09-01 00:00:00.000000` | midnight |
| `2d` | `Mon 2026-08-31 00:00:00.000000` | midnight |
| `1w` | `Mon 2026-08-24 00:00:00.000000` | Monday midnight |
| `3w` | `Mon 2026-08-10 00:00:00.000000` | Monday midnight |

`w` floors to **Monday**, not to the same weekday. `1w` from a Wednesday goes back seven days to
the previous Wednesday, then floors to the Monday before it — nine days, not seven. The week
start is not configurable.

Rejected: `0h`, `-1h`, `2mo`, and the empty string. Zero, negative, and units other than
`s` `m` `h` `d` `w`.
