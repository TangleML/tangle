# Quota states — reference card

## Two sides

```
  PRODUCER SIDE                                CONSUMER SIDE
  (orchestrator, inline, synchronous)          (emissions, async, after the fact)

        GATE                                         SINK
   admission control                            promotion pass
   ─────────────────                            ──────────────
   park / CAS / admit                           release, then un-park waiters
   writes WAITING and ACTIVE                    writes DONE, and node status
   runs per node, before launch                 runs per member completion
```

Occupancy is counted on **both** sides — the gate counts it to decide whether to admit, the sink
counts it to decide how many waiters to wake. Same definition, two readers.

## Vocabulary

| term | what it is | side |
| --- | --- | --- |
| **gate** | the admission check a node passes through before it is allowed to launch. Also called the interceptor. The only thing that grants a slot. | producer |
| **park** | the gate's answer when the group is full: take the node off the launch path and record its place in line. | producer |
| **CAS** | compare-and-set. Two nodes can both read "3 of 4 slots used" and both admit; a CAS makes them fight over a single row so one has to lose and retry. This, not the count, is what keeps a group inside its cap. | producer |
| **admit** | the gate's answer when there is room and the CAS was won: the node holds a slot. | producer |
| **sink** | the reaction to a member finishing: work out how many slots freed up and wake that many waiters. Also reachable by hand, and from an API capacity change. | consumer |
| **promote** | the sink's action: put a parked node back on the launch path. **Advisory** — it grants nothing, it only makes the node visible so the gate can decide again. | consumer |
| **occupancy** | how many slots are in use right now. Derived from the nodes themselves on every read — no counter, nothing to decrement, nothing to reconcile after a crash. | both |
| **capacity** | the cap. Gates admission only; it never kills anything already running, so a lowered capacity leaves the group legitimately over its limit until members finish. | — |
| **claim** | one row per node recording which group it wants and whether it holds a slot. Its creation time is the node's place in the queue. | both |
| **release** | the sink's other action, taken before promoting: mark the finished node's claim terminal so it stops describing the present. Frees nothing by itself — occupancy never read that row. | consumer |
| **ledger** | what the claim table becomes. Rows are never deleted, so a group accumulates one terminal row per node that ever used it. One row per **node**, not per event: a node re-entering the gate updates the row it already has. | both |

## State pairs

Node status and claim state, read together. Neither means anything alone.

| # | node status | claim | means | put here by | side | occupies? |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `UNINITIALIZED` | `WAITING` | parked — in line, invisible to the orchestrator | gate, group was full | producer | ❌ |
| 2 | `QUEUED` | `WAITING` | un-parked — visible again, gate has not run yet | sink, promotion | consumer | ❌ |
| 3 | `QUEUED` | `ACTIVE` | admitted — won the CAS, about to launch | gate, CAS won | producer | ✅ |
| 4 | `PENDING` / `RUNNING` / `CANCELLING` | `ACTIVE` | member — real load on the protected system | orchestrator launch | producer | ✅ |
| 5 | terminal | `ACTIVE` | ending — the node is done, the sink has not caught up | orchestrator | producer | ❌ |
| 6 | terminal | `DONE` | released — the ledger entry, and where a claim stays forever | sink | consumer | ❌ |
| — | `QUEUED` | `WAITING` | contended — gate saw room but lost the CAS every retry; nothing changed, next pass tries again | gate, gave up | producer | ❌ |

Rows 2 and 3 are why the claim has a state at all: identical node status, opposite meaning.
Counting row 2 as occupied would let a promotion wave re-fill the group it was meant to drain,
and every woken waiter would be turned away by the occupancy its own promotion created.

Rows 5 and 6 are the same node before and after the sink reaches it, and **neither occupies**.
That is the point: the sink is best-effort, so row 5 can last forever if the emission is never
delivered, and nothing breaks. `DONE` is not how a slot is freed — the node's terminal status
already did that — it is how the row is told apart from a live one without deleting it.

## Gotchas

- **`active_count` is not `occupancy`.** It counts row 5 until the sink arrives; occupancy never
  did. Neither counts row 6.
- **`DONE` is a filter, not a fact about slots.** Every hot query names it so the ledger stays
  out of the way; no decision is made from it. Believe the node's status.
- **The ledger is only as durable as its nodes.** Both foreign keys are `ON DELETE CASCADE`, so
  purging `execution_node` rows would take the history with them. Nothing purges them today.
- **A node keeps its queue position when re-parked** — the claim is updated in place, never
  deleted and re-inserted, or a busy group would starve its oldest waiter.
- **Losing every CAS retry is not "full"** — the node is left queued, never parked. Parking would
  assert a fullness nothing established.
- **A node that already holds a slot skips the gate.** Otherwise its own claim counts against it,
  and at capacity 1 it would park itself with nothing left to wake it.
