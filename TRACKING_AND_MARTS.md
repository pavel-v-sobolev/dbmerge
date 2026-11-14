# Tracking and Marts

DBMerge synchronizes data into a table: it inserts missing rows, updates changed ones, and marks or deletes rows that are gone. This document describes two **optional** layers on top of a merge:

- **Tracking** — a journal of merges in the database, telling you when a table last changed and whether a merge is running right now.
- **Mart** — a loop that rebuilds a derived table when its sources change, and does nothing when they don't.

If you don't need them, dbmerge works exactly as before — both are opt-in.

## The big picture

```
a merge ──► a row in the tracking table ──► MartLoop sees the change ──► rebuilds the mart
             (started/finished/changed/watermark)
```

- Every merge wrapped in `tracker.exec_merge(...)` leaves a row in the tracking table.
- While the merge runs, the row is "alive" (its heartbeat is refreshed) and holds the safe boundary.
- When the merge finishes, the row gets `finished_at` and a `changed` flag ("did rows actually change").
- `MartLoop` polls this table and rebuilds the mart only when the sources really changed.

## Tracking

### Why

Two programs run independently: one writes into tables (a replicator, a loader), the other builds reports from them (a mart). The second one needs to know:

1. "Did table X change after moment Y?"
2. "Is someone writing into X right now?" — so it doesn't read a half-written row.

Operation rows and durable last-change rows in one tracking table answer these questions.

### Where the journal lives

The journal lives in a **transactional** database. `MergeTracker` refuses ClickHouse and DuckDB — they have no cross-process transactional store. A ClickHouse mart is tracked through PostgreSQL:

```python
tracker = MergeTracker(postgres_engine)              # the journal in PostgreSQL
with dbmerge(clickhouse_engine, table_name="mart",   # the merge itself in ClickHouse
             data=rows, key=["id"]) as merge:
    tracker.exec_merge(merge)
```

Every journal row records the schema of the merged table together with its name — `schema` + `table_name` is the identity. The read methods take a `schema=...` filter; two databases with a conflicting name need separate tracking tables.

### Minimal example

```python
from dbmerge import dbmerge
from dbmerge.tracking import MergeTracker

tracker = MergeTracker(engine)          # the dbmerge_tracking table is created lazily; the tracker is long-lived
with dbmerge(engine, table_name="orders", data=orders_rows,
             key=["id"], merged_on_field="merged_on") as merge:
    record, result = tracker.exec_merge(merge)
```

`tracker.exec_merge(merge)` runs `merge.exec()` itself and returns `(record, result)`:

- `record` — what the journal recorded about this merge (`MergeRecord`);
- `result` — the usual `exec()` result (`mergeResult`).

Conditions built on the merge's own tables (`source_condition`, `delete_condition`, `update_condition`, `insert_condition`) are passed through `tracker.exec_merge`:

```python
with dbmerge(engine, table_name="orders",
             source_table_name="orders_source", source_schema="source",
             key=["id"], merged_on_field="merged_on") as merge:
    record, result = tracker.exec_merge(
        merge,
        source_condition=merge.source_table.c["Period"] == period,
    )
```

The table name is taken from the merge itself; the `changed` flag comes from its result.

### Tracking other committed writes

```python
def operation():
    with engine.begin() as conn:
        return conn.execute(statement).rowcount

record, result = tracker.exec(
    operation, destination=("target", "orders"), scope="full_load",
)
record, result = tracker.exec_merge(merge, scope="changes")
```

`exec` calls a zero-argument callable and returns its original result. Only `False` or a
numeric zero means no changes; `None`, empty containers and empty strings mean potentially
changed. The callable **must commit its data writes before returning**. It is registered
durably before invocation, heartbeated while running, and finished afterwards. Exceptions
propagate and leave the operation available for conservative recovery as changed.

`exec_merge` uses the same executor, detects changes from inserted/updated/deleted counts,
and preserves the original merge result. Neither method accepts a `changed` override;
there is no public tracking context manager. Migrate existing `tracker.exec(merge)` calls
to `tracker.exec_merge(merge)`.

`scope` is an optional operation category, persisted in the journal and returned record.
It is not a source table reference. Existing journals gain a nullable `scope` column on
startup. `is_changed_since(..., scope="changes")` filters durable change marks;
omitting scope reads all scopes. MartLoop accepts the same optional `scope` filter. Safe boundaries always
include active operations regardless of scope. Watermarks are committed by `MartLoop` after the whole handler succeeds.

### What you can ask

Every read method takes a `schema=` filter — the schema of the merged table. Together with the
table name it is the identity; omit it (or pass `None`) to read across all schemas.

```python
# Up to which moment is the data settled (in-progress merges hold it back)?
safe_boundary = tracker.safe_boundary(["orders", "order_items"], schema="target")   # datetime

# What was the latest finished operation (which may be a no-op)?
last = tracker.last_merge("orders", schema="target")                       # MergeRecord | None
last.started_at, last.finished_at, last.changed

# The watermark of the mart's last successful update?
tracker.watermark("orders_by_customer", schema="target")                   # datetime | None

# Did any of the tables really change after `since`?
tracker.is_changed_since(["orders", "order_items"], since=watermark, schema="target")  # bool
```

- `safe_boundary(...)` — the safe boundary: everything committed up to this moment is visible and settled. It caps the **watermark**, not the read (you may read further).
- `is_changed_since(...)` — whether there was a real change after `since`. A no-op merge (nothing changed) does not count.

### How it is stored

The journal, `dbmerge_tracking` (the name is configurable); its full column list is in
[Table schema](#table-schema). Storage rule: **at most one finished row per table (the latest), plus
any number of unfinished rows** — history does not grow.

The same `dbmerge_tracking` table stores a stable `row_kind="change"` row per
destination/schema/scope, with a deterministic `change:`-prefixed `id` and nullable `changed_at`.
Operation rows have `row_kind="operation"` and random IDs; lease rows have `row_kind="lease"`.
Legacy NULL kinds are interpreted as operations, except reserved `lease:` table names.
No second tracking table is created. The change row is prepared before registering an operation. Changed operations and recovery
atomically finish the journal entry and publish a monotonic database-clock change timestamp.
No-ops and watermark writes never clear it, including when operations finish out of start order.
`is_changed_since` reads change rows; `last_merge` continues to describe the latest operation.
Operation readers, safe boundaries, recovery and operation pruning explicitly exclude change rows.
Storage grows with distinct destination/scope identities, not the number of operations.
`reset` clears the selected destinations' marks together with their finished journal rows,
but keeps the stable rows so active operations can still publish on completion.

On startup, missing nullable `row_kind`/`changed_at` columns are added and existing finished
changed operation rows are backfilled into change rows in the same table.
Changes already erased by the old no-op behavior cannot be reconstructed from the journal;
rebuild affected marts if necessary. Deployment still requires coordinated schema creation.

### When a merge crashes

A crashed merge leaves a row with an empty `finished_at`; its heartbeat eventually goes stale. Such a row stops holding the safe boundary, and `recover_abandoned()` marks it as changed so that marts which missed it wake up:

```python
tracker.recover_abandoned()   # call it at the start of your loop
```

## Marts

### Why

A mart is a table built from one or more source tables. It must be recalculated when the sources change, and left alone when they don't. `MartLoop` owns the "when to run" machinery and leaves only the calculation to you.

### Minimal example

```python
from dbmerge import dbmerge
from dbmerge.tracking import MergeTracker
from dbmerge.mart import MartHandler

class OrdersByCustomer(MartHandler):
    destination = ("target", "orders_by_customer")          # the result table: (schema, table)
    sources = [("target", "orders"), ("target", "order_items")]  # what it reads: (schema, table)
    min_interval = 5.0

    def setup(self, ctx):
        # One-time DDL: create the mart table. dbmerge can not create an empty table, so the
        # DDL goes here and process_changes() merges into it. Anything the mart's SELECT needs (views,
        # indexes) is created here too.
        self.execute(ctx, """
            CREATE TABLE IF NOT EXISTS target.orders_by_customer (
                customer_id INTEGER PRIMARY KEY,
                total NUMERIC
            )
        """)

    def process_changes(self, ctx):
        # Read everything changed since the watermark; the read is unbounded — rows to the right of
        # ctx.next_watermark are processed now and again in the next window.
        rows = build_rows(since=ctx.watermark)   # your SELECT, e.g. ... WHERE merged_on > :watermark
        with dbmerge(ctx.engine, schema=ctx.destination[0], table_name=ctx.destination[1],
                     data=rows, key=["customer_id"]) as merge:
            ctx.tracker.exec_merge(merge)

    def rebuild(self, ctx):
        # a full rebuild; by default the same as process_changes with full_rebuild=True
        self.process_changes(ctx)
        yield "all"

tracker = MergeTracker(engine)             # one tracker per process
OrdersByCustomer(engine).run_forever(tracker)
```

The low-level alternative without a class is `MartLoop(engine, tracker, destination=..., sources=..., process_changes=process_changes)`, where `process_changes` is a plain function. `MartHandler` is a convenience wrapper over it.

Important: for `MartLoop` to see source changes, the source merges must also be wrapped in `tracker.exec_merge(...)` — as in the Tracking section.

### What MartLoop does

Each pass (every `poll_interval` seconds):

1. Recovers abandoned merges (`recover_abandoned`).
2. Claims the destination-derived lease to coordinate competing workers (not a fencing guarantee).
3. Reads its watermark: `watermark = tracker.watermark(destination)` (the first run starts from the beginning).
4. Computes the next watermark: `next_watermark = tracker.safe_boundary(sources)`.
5. Asks: `tracker.is_changed_since(sources, since=watermark)`.
   - no change → skip;
   - a change, but `next_watermark <= watermark` → wait (a source is still being written).
6. Otherwise calls `process_changes(ctx)`.

The key point: **"should I update" is decided from timestamps, not from a signal**. An event that was "lost" (a crash, a switched-off mart) is not lost — the next pass sees it through `is_changed_since`. Therefore `process_changes` must be **idempotent**: it may run again.

### The watermark

`MartLoop` commits `ctx.next_watermark` only after the whole `process_changes(ctx)` returns successfully. It checks and renews the lease in the same progress transaction. A failed multi-write handler leaves the previous watermark intact. Writes remain independently committed and must be idempotent for replay. Progress is a stable `row_kind="watermark"` row in the same `dbmerge_tracking` table; ordinary operations cannot overwrite it.

The read in the handler is **unbounded**: select rows by `> ctx.watermark`. Rows to the right of `ctx.next_watermark` are processed now and again in the next window (at-least-once). Only the **watermark** is capped: it always equals `ctx.next_watermark`, otherwise the cursor would jump over rows of an unfinished merge that aren't committed yet.

"Rebuild the mart from scratch" = delete its finished row:

```python
tracker.reset("orders_by_customer")   # the next run starts from the beginning
```

### The handler context

```python
@dataclass
class MartContext:
    engine: Engine
    tracker: MergeTracker
    destination: tuple[str | None, str]
    sources: frozenset[tuple[str | None, str]]
    watermark: datetime        # where to read changes from
    next_watermark: datetime   # where to move the watermark on success (not a read cap)
    full_rebuild: bool         # True on the first run / after reset
    lease_is_mine: Any         # SQL condition "the mart name is still ours"
```

## Reliability tests and CI

Transactional-engine workflows and the Python matrix run merge and tracking/marts as
independent jobs (`suite: merge` / `tracking`), each with a 30-minute job timeout.
A stalled merge suite cannot prevent the tracking/marts suite from starting.
Tracking/marts retain the workflow's existing engine, driver and SQLAlchemy matrix.

Regression tests exercise committed writes followed by abrupt process exit,
recovery without losing a change to a later no-op, rebuild cursor/boundary resumption
in a new process, a concurrent request at rebuild start, legacy `dbmerge_marts`
column migration, heartbeat retry after an exception, and bounded close while a
heartbeat is blocked. Crash tests expire the dead owner's timestamp explicitly
instead of sleeping for the production TTL.

The mixed ClickHouse/PostgreSQL workflow runs `tests/test_mart_clickhouse.py` in an
independent job on each configured ClickHouse/SQLAlchemy combination. It provisions
a PostgreSQL source exposed through ClickHouse's PostgreSQL database engine,
merges aggregate rows into ClickHouse, and stores tracking, leases, watermarks and
rebuild state in PostgreSQL. It checks initial load, repeated updates, soft-delete,
idle passes, progress publication and an explicit rebuild. Local invocation:

```sh
DBMERGE_TEST_ENGINES=clickhouse \
DBMERGE_TEST_CLICKHOUSE_POSTGRES=postgres:5432 \
uv run --no-sync pytest tests/test_mart_clickhouse.py -v
```

The address is how **ClickHouse** reaches PostgreSQL, not necessarily the address
used by the test process. PostgreSQL connection parameters can be overridden with
`DBMERGE_TEST_URL_POSTGRES`.

ReplacingMergeTree background compaction is asynchronous; the integration test
reads destination rows with `FINAL` rather than waiting for compaction. Soft-delete
keeps a zero aggregate for a customer with no remaining active orders; this test
does not establish physical-delete or arbitrary stale-writer fencing guarantees.
Crash/resume is tested separately against the transactional-engine matrix, not by
killing a ClickHouse merge in flight. Local results are not remote CI results.

## Table schema

```
dbmerge_tracking
    id            String(64)  PK
    row_kind      String(16) NULL       -- operation / change / watermark / lease; NULL for legacy rows
    owner         String(255) NULL      -- NULL for lease rows ("free")
    table_name    String(255) NOT NULL
    schema        String(255) NULL      -- the schema of the merged table ("" when none)
    scope         String(255) NULL      -- operation category
    changed_at    DateTime(tz) NULL     -- durable mark on change rows only
    started_at    DateTime(tz) NOT NULL
    finished_at   DateTime(tz) NULL
    heartbeat_at  DateTime(tz) NOT NULL
    changed       Boolean NULL
    watermark     DateTime(tz) NULL

dbmerge_marts (per-mart rebuild state, created on first use too)
    table_name                String(255) PK    -- the mart table name
    schema                    String(255) PK
    full_rebuild_is_required  Boolean NOT NULL  -- the rebuild request flag
    rebuild_cursor            String(255) NULL  -- label of the last completed rebuild block
    rebuild_request           String(36) NULL   -- token for the latest explicit request
    rebuild_boundary          DateTime(tz) NULL -- source boundary when this rebuild started
```

## Reference

### MergeTracker

| member | what it does |
|---|---|
| `MergeTracker(engine, table_name=..., schema=..., ...)` | a long-lived journal; one tracker serves many merges. Optionally `with MergeTracker(engine) as tracker:` stops the heartbeat on exit |
| `exec(operation, *, destination, scope=None, with_lease=False, wait_lease=False, wait_timeout=None, wait_interval=30.0)` | journals a zero-argument callable; returns `(MergeRecord, original_result)`, or `(None, None)` when a requested lease is busy |
| `exec_merge(merge, *, scope=None, with_lease=False, wait_lease=False, wait_timeout=None, wait_interval=30.0, **exec_kwargs)` | adapts a merge to `exec`, inferring destination and changes from row counters; preserves the merge result |
| `safe_boundary(table_names, schema=None)` | the safe upper boundary over these tables (`table_names` is one name or a list) |
| `last_merge(table_name, schema=None)` | the last finished row of one table, or `None` |
| `watermark(table_names, schema=None)` | the watermark of the last successful merge over these tables, or `None` (`table_names` is one name or a list) |
| `is_changed_since(table_names, since, schema=None, *, scope=None)` | whether there was a real change after `since`, optionally filtered by scope (`None` reads all scopes) |
| `recover_abandoned()` | marks stale unfinished rows as changed |
| `reset(table_name, schema=None)` | deletes the finished row (rebuild from scratch) |
| `mark_watermark(table_name, watermark, schema=None)` | persists a monotonic consumer watermark independently of operation rows; MartLoop calls it with a transactionally checked lease |

The table name and schema together are the identity; pass the `schema=` to the read methods. Two databases with a conflicting name need separate tracking tables. The destination-derived lease is a `Lease` row in the same tracking table.

### MartLoop

| member | what it does |
|---|---|
| `run_forever()` | a blocking poll loop |
| `run_if_pending()` | one pass (for cron or your own loop) |
| `request_stop()` / `close()` | stop |

`run_mart(mart, engine=None, tracker=None, *, poll_interval=2.0, schema=None, lease=None, run_forever=True)`
runs one mart until `request_stop()` is called: it wraps the handler into a `MartLoop`, creates the
`MergeTracker` when none is passed, and runs the loop. `request_stop()` asks every loop started by
`run_mart` to stop after its current pass — wire it to SIGTERM/SIGINT in a runner (see
[examples/runner_marts.py](examples/runner_marts.py)).

`request_rebuild(tracker.engine, destination, schema=tracker.schema)` sets the rebuild flag in the per-mart `dbmerge_marts`
table in the transactional tracking engine (`full_rebuild_is_required` + `rebuild_cursor`); the next pass performs the rebuild. An
interrupted rebuild resumes from the saved cursor — `ctx.rebuild_from` tells the handler which
blocks are already done.

### MartHandler

The base class of a mart: declare the required `destination` and non-empty `sources` and implement the methods.
`destination` and each source are `(schema, table)` tuples (a bare `"table"` string means no schema).

| member | what it does |
|---|---|
| `setup(ctx)` | one-time preparation before the first run (DDL) |
| `process_changes(ctx)` | the incremental work; the loop commits progress after the whole method succeeds |
| `rebuild(ctx)` | a full rebuild, block by block — a generator that yields a label per committed block; by default `process_changes` + `yield "all"` |
| `changed_since(ctx, *columns)` | an OR of `column > ctx.watermark` |
| `execute(ctx, sql, **params)` / `query(ctx, sql, **params)` | run SQL in its own transaction / return rows |
| `loop(tracker, ...)` | build a `MartLoop` |
| `run_forever(tracker=None, ...)` | run the loop |

`as_handler(obj, engine=None)` normalizes a `MartHandler` instance, a module, or a callable with attributes into a `MartHandler`.

## Things to remember

- **Idempotency.** `process_changes` may run again — don't double-count.
- **Latency.** The mart updates with a delay of up to `poll_interval` (this is polling, not push).
- **`merged_on_field`.** Source tables are merged with a change-timestamp column, and the handler selects rows by `> ctx.watermark` — otherwise there is nothing to "catch up" over the window.
- **One process per mart.** The destination identity is claimed by a lease; a second process skips the pass.



## Required configuration and migration

`MartHandler(engine, *, destination=None, sources=None)` requires an explicit SQLAlchemy engine.
`destination` and a non-empty iterable `sources` must be provided either as class attributes or as
constructor arguments. Constructor arguments override the class declarations. Invalid or missing
references raise `IncorrectParameter` immediately. Sources are normalized to an immutable frozenset;
references from different schemas are tracked separately. No-schema references match only the empty
schema, not all schemas.

`destination` is a logical identifier, not a requirement to write to a SQL table. It accepts a bare
string, a schema-qualified string, or `(schema, identifier)`. For table and non-table destinations alike, return only after successful,
committed, idempotent delivery. `MartLoop` persists the destination's watermark automatically.
Do not pass `next_watermark` to `exec`/`exec_merge`, or call `mark_watermark` inside a handler.
Destination identities must be unique within a journal; server/database identity is not included.

This is a breaking API change:

- Remove `name`; replace `mart_table` / `ctx.mart_table` with `destination` / `ctx.destination`.
- Replace `MyMart().loop(engine, tracker)` with `MyMart(engine).loop(tracker)`.
- Replace `MyMart().run_forever(engine, tracker)` with `MyMart(engine).run_forever(tracker)`.
- Low-level construction: `MartLoop(engine, tracker, destination, sources, process_changes, ...)`.
- Module/function handlers require `as_handler(obj, engine)` or `run_mart(obj, engine=engine)`.
- Rebuild helpers now accept `(tracking_engine, destination, *, schema=None)`; `schema` selects
  the service-table schema, while the destination tuple carries its business schema.
- Stop old workers before deployment: the lease key is now derived from destination, not old name.
  Existing watermark and cursor identities remain `(schema, identifier)`.

Every loop creates its own lease by default. A supplied lease remains caller-owned; closing one loop
releases only its own key and does not stop a shared lease's heartbeat. `run_mart` closes trackers it
creates, but not a caller-supplied tracker. For a separate destination engine (e.g. ClickHouse),
pass a transactional tracker explicitly. Rebuild state is always stored through `tracker.engine`.

## Debugging examples

Both example files call `run_mart(..., run_forever=False)` directly under
`if __name__ == "__main__"`: no separate main function, arguments, signals, or polling.
Edit the connection strings there and run the file or place a breakpoint in the handler:

```bash
python examples/orders_by_customer.py
python examples/orders_by_customer_clickhouse.py
```

`run_mart` creates and closes a tracker automatically when none is passed. The ClickHouse example
passes a PostgreSQL tracker explicitly because ClickHouse cannot store the journal.
A single pass returns `True` if work ran, otherwise `False`, and propagates errors to the debugger.
The loop releases its lease on exit. Engines remain caller-owned. A pass without source changes
does nothing; sources and ClickHouse's external PostgreSQL mapping must already exist.
Continuous operation remains the default (`run_forever=True`).

## Reliability limits

The current implementation is at-least-once scheduling, not an atomic snapshot or exactly-once
pipeline. Durable change marks now survive later no-ops; the audit's no-op-loss finding is fixed.
Lease expiration alone cannot stop a stalled writer; rebuilds do not automatically remove obsolete destination keys. See
`audit/tracking-and-marts-2026-11-14.md` for the detailed audit, reproductions and prioritized work.


## Consumer, lease and shutdown contracts

- Set `scope = "changes"` on a handler (or pass it to `MartLoop`) to filter wakeup signals.
  Safe boundaries still include **all** operations on the sources, regardless of scope.
- Set `loop.enabled = False` to pause a consumer without advancing its durable watermark.
  Re-enabling or recreating it with the same destination catches up from that watermark; later
  no-ops cannot erase pending changes. `None` scope reads all categories.
- Rebuild requests carry a token. Starting a rebuild consumes only the observed request;
  a request arriving while it runs remains pending for the next rebuild. An empty cursor means
  an active rebuild before its first block. A durable start boundary replaces early watermark
  seeding; incremental handlers between blocks commit progress only after success. Completion
  publishes at least the start boundary, without moving a newer watermark backwards.
- Lease acquisition is **nonreentrant**, including two objects using the same owner. A pass
  acquires/releases its lease in `try/finally`. `still_mine(name)` only checks;
  `renew(name)` explicitly extends a fresh hold and returns False after expiry or ownership loss.
  Neither method fences arbitrary destination writes; handlers must remain idempotent.
- Tracker and lease heartbeats use a recreated independent connection pool by default, preserving
  the original pool connection creator. `heartbeat_engine=` may supply an explicitly configured
  engine pointing to the same transactional store; it remains caller-owned. Configure driver/query
  timeouts for bounded database shutdown. In-memory SQLite is unsuitable for background tracking.
- `request_stop()` reaches direct and runner-created loops and is remembered for future loops.
  `reset_stop()` allows *new* loops to start again; existing stopped loops stay stopped. Signal
  handlers are not installed automatically: the host can wire request_stop to SIGTERM/SIGINT.
- Closing a loop requests stop and waits for its current pass before releasing its lease.
  Closing a tracker rejects new writes, waits for active writes with heartbeat alive, then joins
  heartbeat and closes owned resources only. Do not close a tracker from inside its operation.
  Lease close stops renewal and releases held keys. Shared caller engines are never disposed.
  A heartbeat join timeout raises rather than silently declaring shutdown complete.
