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

- Every merge wrapped in `tracker.exec(...)` leaves a row in the tracking table.
- While the merge runs, the row is "alive" (its heartbeat is refreshed) and holds the safe boundary.
- When the merge finishes, the row gets `finished_at` and a `changed` flag ("did rows actually change").
- `MartLoop` polls this table and rebuilds the mart only when the sources really changed.

## Tracking

### Why

Two programs run independently: one writes into tables (a replicator, a loader), the other builds reports from them (a mart). The second one needs to know:

1. "Did table X change after moment Y?"
2. "Is someone writing into X right now?" — so it doesn't read a half-written row.

One tracking table answers both questions.

### Where the journal lives

The journal lives in a **transactional** database. `MergeTracker` refuses ClickHouse and DuckDB — they have no cross-process transactional store. A ClickHouse mart is tracked through PostgreSQL:

```python
tracker = MergeTracker(postgres_engine)              # the journal in PostgreSQL
with dbmerge(clickhouse_engine, table_name="mart",   # the merge itself in ClickHouse
             data=rows, key=["id"]) as merge:
    tracker.exec(merge)
```

Every journal row records the schema of the merged table together with its name — `schema` + `table_name` is the identity. The read methods take a `schema=...` filter; two databases with a conflicting name need separate tracking tables.

### Minimal example

```python
from dbmerge import dbmerge
from dbmerge.tracking import MergeTracker

tracker = MergeTracker(engine)          # the dbmerge_tracking table is created lazily; the tracker is long-lived
with dbmerge(engine, table_name="orders", data=orders_rows,
             key=["id"], merged_on_field="merged_on") as merge:
    record, result = tracker.exec(merge)
```

`tracker.exec(merge)` runs `merge.exec()` itself and returns `(record, result)`:

- `record` — what the journal recorded about this merge (`MergeRecord`);
- `result` — the usual `exec()` result (`mergeResult`).

Execution options (`commit_all_steps`, `chunk_size`, `bulk_load`) are set when the merge is created — the `dbmerge` constructor accepts them; `exec()` keeps them too, for backward compatibility. Conditions built on the merge's own tables (`source_condition`, `delete_condition`, `update_condition`, `insert_condition`) are passed through `tracker.exec`:

```python
with dbmerge(engine, table_name="orders",
             source_table_name="orders_source", source_schema="source",
             key=["id"], merged_on_field="merged_on") as merge:
    record, result = tracker.exec(
        merge,
        source_condition=merge.source_table.c["Period"] == period,
    )
```

The table name is taken from the merge itself; the `changed` flag comes from its result.

### What you can ask

Every read method takes a `schema=` filter — the schema of the merged table. Together with the
table name it is the identity; omit it (or pass `None`) to read across all schemas.

```python
# Up to which moment is the data settled (in-progress merges hold it back)?
safe_boundary = tracker.safe_boundary(["orders", "order_items"], schema="target")   # datetime

# When did the table last change, and did rows actually change?
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

One table, `dbmerge_tracking` (the name is configurable); its full column list is in
[Table schema](#table-schema). Storage rule: **at most one finished row per table (the latest), plus
any number of unfinished rows** — history does not grow.

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
    name = "orders_by_customer"             # unique mart name (for the lease)
    mart_table = ("target", "orders_by_customer")          # the result table: (schema, table)
    sources = [("target", "orders"), ("target", "order_items")]  # what it reads: (schema, table)
    min_interval = 5.0

    def setup(self, ctx):
        # One-time DDL: create the mart table. dbmerge can not create an empty table, so the
        # DDL goes here and handle() merges into it. Anything the mart's SELECT needs (views,
        # indexes) is created here too.
        self.execute(ctx, """
            CREATE TABLE IF NOT EXISTS orders_by_customer (
                customer_id INTEGER PRIMARY KEY,
                total NUMERIC
            )
        """)

    def handle(self, ctx):
        # Read everything changed since the watermark; the read is unbounded — rows to the right of
        # ctx.next_watermark are processed now and again in the next window.
        rows = build_rows(since=ctx.watermark)   # your SELECT, e.g. ... WHERE merged_on > :watermark
        with dbmerge(ctx.engine, table_name=self.mart_table,
                     data=rows, key=["customer_id"]) as merge:
            ctx.tracker.exec(merge, next_watermark=ctx.next_watermark)

    def rebuild(self, ctx):
        # a full rebuild; by default the same as handle with full_rebuild=True
        self.handle(ctx)

tracker = MergeTracker(engine)             # one tracker per process
OrdersByCustomer().run_forever(engine, tracker)
```

The low-level alternative without a class is `MartLoop(engine, tracker, name=..., mart_table=..., sources=..., handle=handle)`, where `handle` is a plain function. `MartHandler` is a convenience wrapper over it.

Important: for `MartLoop` to see source changes, the source merges must also be wrapped in `tracker.exec(...)` — as in the Tracking section.

### What MartLoop does

Each pass (every `poll_interval` seconds):

1. Recovers abandoned merges (`recover_abandoned`).
2. Claims the mart name — so two processes never build the same mart.
3. Reads its watermark: `watermark = tracker.watermark(mart_table)` (the first run starts from the beginning).
4. Computes the next watermark: `next_watermark = tracker.safe_boundary(sources)`.
5. Asks: `tracker.is_changed_since(sources, since=watermark)`.
   - no change → skip;
   - a change, but `next_watermark <= watermark` → wait (a source is still being written).
6. Otherwise calls `handle(ctx)`.

The key point: **"should I update" is decided from timestamps, not from a signal**. An event that was "lost" (a crash, a switched-off mart) is not lost — the next pass sees it through `is_changed_since`. Therefore `handle` must be **idempotent**: it may run again.

### The watermark

You don't store the watermark separately. When `handle` finishes the mart merge with `next_watermark=ctx.next_watermark`, that value is written to the mart's tracking row as its new watermark — the next run starts exactly from it.

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
    name: str
    mart_table: str
    sources: frozenset[str]
    watermark: datetime        # where to read changes from
    next_watermark: datetime   # where to move the watermark on success (not a read cap)
    full_rebuild: bool         # True on the first run / after reset
    lease_is_mine: Any         # SQL condition "the mart name is still ours"
```

## Table schema

```
dbmerge_tracking
    id            String(64)  PK
    owner         String(255) NULL      -- NULL for lease rows ("free")
    table_name    String(255) NOT NULL
    schema        String(255) NULL      -- the schema of the merged table ("" when none)
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
```

## Reference

### MergeTracker

| member | what it does |
|---|---|
| `MergeTracker(engine, table_name=..., schema=..., ...)` | a long-lived journal; one tracker serves many merges. Optionally `with MergeTracker(engine) as tracker:` stops the heartbeat on exit |
| `exec(merge, *, next_watermark=None, changed=None, with_lease=False, wait_lease=False, wait_timeout=None, wait_interval=30.0, **exec_kwargs)` | writes a tracking row and runs `merge.exec(**exec_kwargs)`; `with_lease` makes the merge exclusive per table and `wait_lease` waits for the holder; returns `(MergeRecord, mergeResult)`, or `(None, None)` when skipped |
| `safe_boundary(table_names, schema=None)` | the safe upper boundary over these tables (`table_names` is one name or a list) |
| `last_merge(table_name, schema=None)` | the last finished row of one table, or `None` |
| `watermark(table_names, schema=None)` | the watermark of the last successful merge over these tables, or `None` (`table_names` is one name or a list) |
| `is_changed_since(table_names, since, schema=None)` | whether there was a real change after `since` (`table_names` is one name or a list) |
| `recover_abandoned()` | marks stale unfinished rows as changed |
| `reset(table_name)` | deletes the finished row (rebuild from scratch) |
| `mark_watermark(table_name, watermark, schema=None)` | records a finished, unchanged merge carrying a watermark — seals a mart's watermark after its first rebuild |

The table name and schema together are the identity; pass the `schema=` to the read methods. Two databases with a conflicting name need separate tracking tables. The mart-name lease is the `Lease` class in the same table (key `lease:<name>`).

### MartLoop

| member | what it does |
|---|---|
| `run_forever()` | a blocking poll loop |
| `run_if_pending()` | one pass (for cron or your own loop) |
| `request_stop()` / `close()` | stop |

`run_mart(mart, engine, tracker=None, *, poll_interval=2.0, schema=None, lease=None)`
runs one mart until `request_stop()` is called: it wraps the handler into a `MartLoop`, creates the
`MergeTracker` when none is passed, and runs the loop. `request_stop()` asks every loop started by
`run_mart` to stop after its current pass — wire it to SIGTERM/SIGINT in a runner (see
[examples/runner_marts.py](examples/runner_marts.py)).

`request_rebuild(engine, schema, table_name)` sets the rebuild flag in the per-mart `dbmerge_marts`
table (`full_rebuild_is_required` + `rebuild_cursor`); the next pass performs the rebuild. An
interrupted rebuild resumes from the saved cursor — `ctx.rebuild_from` tells the handler which
blocks are already done.

### MartHandler

The base class of a mart: declare `name`, `mart_table`, `sources` and implement the methods.
`mart_table` and each source are `(schema, table)` tuples (a bare `"table"` string means no schema).

| member | what it does |
|---|---|
| `setup(ctx)` | one-time preparation before the first run (DDL) |
| `handle(ctx)` | the incremental work; records the watermark via `tracker.exec(..., next_watermark=ctx.next_watermark)` |
| `rebuild(ctx)` | a full rebuild, block by block — a generator that yields a label per committed block; by default `handle` + `yield "all"` |
| `changed_since(ctx, *columns)` | an OR of `column > ctx.watermark` |
| `execute(ctx, sql, **params)` / `query(ctx, sql, **params)` | run SQL in its own transaction / return rows |
| `loop(engine, tracker, ...)` | build a `MartLoop` |
| `run_forever(engine, tracker, ...)` | run the loop |

`as_handler(obj)` normalizes a `MartHandler` instance, a module, or a callable with attributes into a `MartHandler`.

## Things to remember

- **Idempotency.** `handle` may run again — don't double-count.
- **Latency.** The mart updates with a delay of up to `poll_interval` (this is polling, not push).
- **`merged_on_field`.** Source tables are merged with a change-timestamp column, and the handler selects rows by `> ctx.watermark` — otherwise there is nothing to "catch up" over the window.
- **One process per mart.** The mart name is claimed by a lease; a second process skips the pass.


