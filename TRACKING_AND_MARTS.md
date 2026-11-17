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

- Every merge executed through `tracker.exec_merge(...)` leaves a row in the tracking table.
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
with DBMerge(clickhouse_engine, table_name="mart",   # the merge itself in ClickHouse
             data=rows, key=["id"]) as merge:
    tracker.exec_merge(merge)
```

Every journal row records the schema of the merged table together with its name — `schema` + `table_name` is the identity. The read methods take a `schema=...` filter; two databases with a conflicting name need separate tracking tables.

### Minimal example

```python
from dbmerge import DBMerge
from dbmerge.tracking import MergeTracker

with MergeTracker(engine) as tracker:  # prepares dbmerge_tracking immediately
    with DBMerge(engine, table_name="orders", data=orders_rows,
                 key=["id"], merged_on_field="merged_on") as merge:
        record, result = tracker.exec_merge(merge)
```

`tracker.exec_merge(merge)` runs the merge and returns `(MergeRecord, mergeResult)`.
Plain `merge.exec()` remains untracked unless `tracker=` is supplied.
The tracker is caller-owned. Journaling starts when `exec_merge` is called, not
while the merge constructor prepares DDL/staging tables. Data and journal commits
remain separate.

The snippet above is a complete lifetime: reuse one open tracker across writes in
an application, then close it only after all writers and mart workers have stopped.
Later snippets assume such an open tracker. Construction connects to the journal
database and creates/migrates its table; an import alone does not. Use finite
positive `heartbeat_period` and `heartbeat_ttl` values in seconds. Set the TTL
comfortably above the refresh period and expected database/network pauses; a
heartbeat interval greater than the TTL can make a healthy writer appear abandoned.

Pass conditions and execution options through `tracker.exec_merge`:

```python
from dbmerge import LeaseOptions

with DBMerge(engine, table_name="orders", source_table_name="orders_source",
             source_schema="source", key=["id"], merged_on_field="merged_on") as merge:
    record, result = tracker.exec_merge(
        merge, source_condition=merge.source_table.c["Period"] == period,
        lease_options=LeaseOptions(wait_lease=True, wait_timeout=300, wait_interval=2),
    )
```

A busy lease raises `LeaseBusy` if waiting is disabled or the wait times out.
Heartbeat renews a held lease, and it is released on success or failure. Constructor
DDL runs before this lease is acquired; this is not a fence around constructor work.

The table name is taken from the merge itself; the `changed` flag comes from its result.

### Constructor tracking and lease options

```python
from dbmerge import DBMerge, MergeTracker, LeaseOptions

tracker = MergeTracker(engine)
with DBMerge(engine, table_name="orders", data=orders_rows, key=["id"],
             tracker=tracker,
             lease_options=LeaseOptions(wait_lease=True, wait_timeout=300, wait_interval=2)) as merge:
    result = merge.exec()
    record = merge.tracking_record
```

`tracker=` enables journaling; `lease_options=None` does not acquire a lease.
The presence of `LeaseOptions` enables a lease automatically and requires a tracker.

`lease_options=LeaseOptions(scope="full")` and `scope="changes"` separate destination
leases. Lease scope and journal scope are independent. Use `lease_handle=ctx.lease_handle`
in mart writes: it explicitly borrows the pass lease, which stays held until progress
is saved. No lease is implicitly inherited through tracker. A supplied handle is
never reacquired or released by a borrower; its acquisition token prevents reuse
following release/reacquisition. Both parameters require tracker and are mutually
exclusive. A lost/stale handle raises `LeaseLost`; incompatible storage is rejected.
Marts coordinate by default; `lease_options=None` disables coordination. A borrowed
pass handle can instead be supplied using `lease_handle=...` (omit lease_options).
Mart lease scopes are assigned internally (`rebuild`/`changes`); a non-None
`LeaseOptions.scope` on a mart is rejected rather than silently ignored.
`DBMerge.exec()`, `MergeTracker.exec()` and `exec_merge()` raise `LeaseBusy` when
acquisition fails or times out. MartLoop catches it and returns False without
advancing that unprocessed block or watermark. No result-is-None check is needed
inside handlers. Low-level `Lease.acquire()` still returns a boolean.
Same-database writes include SQL ownership guards; cross-engine checks are external,
not distributed transactional fencing.

Tracked merges use the tracker engine's clock for `merged_on_field` and
`inserted_on_field`, sampled before each write phase. Destination timestamp types
must preserve compatible precision and timezone semantics.

For stale-rebuild protection, set `use_safe_border=True`, `merged_on_field=...`,
and `delete_mode='no'` or `'mark'`. dbmerge captures the destination's safe boundary
before staging, then restricts updates and marks to NULL timestamps or timestamps
strictly below it. Inserts remain allowed. The boundary considers all operation
scopes, not only the current lease scope. This does not make source reads consistent,
protect arbitrarily old preloaded snapshots, or resolve ClickHouse version ties.
Defaults are `wait_lease=False`, `wait_timeout=None`, `wait_interval=30.0`.
Timeouts must be finite and non-negative (or None); intervals must be finite and positive.
A busy lease raises `LeaseBusy` from `merge.exec()` and leaves `tracking_record=None`;
the merge may be retried while its context remains open. Other failures propagate.
The tracker remains caller-owned. Neither journaling nor the lease covers constructor
DDL/staging preparation, and data and journal commits are not atomic.

`tracker.exec_merge(merge, ...)` remains supported: bound merges are journaled once,
conflicting trackers are rejected. Configure lease_options/lease_handle on the merge
or adapter, not both; omitted adapter arguments use the merge configuration.

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
omitting scope reads all scopes. Marts consume all source scopes. Safe boundaries always
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
tracker.watermark("product_daily_sales", schema="target")                   # datetime | None

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
from sqlalchemy import create_engine
from dbmerge import MergeTracker
from examples.seed_product_daily_sales import seed
from examples.product_daily_sales import ProductDailySales

engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
try:
    with MergeTracker(engine) as tracker:
        seed(engine, tracker)  # Demo source only; production loaders journal their own writes.
        ProductDailySales(engine, tracker=tracker).run_once()
finally:
    engine.dispose()
```

Run from the repository root with a disposable PostgreSQL database. The source
`target.orders` has business columns `Order`, `Date`, `Product`, `Shop`, `Amount`:

| Order | Date | Product | Shop | Amount |
|---|---|---|---|---|
| 1 | 2026-11-16 | 10 | 1 | 100.00 |
| 2 | 2026-11-16 | 10 | 2 | 50.00 |
| 3 | 2026-11-16 | 20 | 1 | 30.00 |
| 4 | 2026-11-17 | 10 | 1 | 70.00 |

The mart collapses **all shops** into `(Date, Product, SUM(Amount))`. `Order`
identifies a source row; neither `Order` nor `Shop` is part of the aggregate key.

```sql
SELECT "Date", "Product", "Amount"
FROM target.product_daily_sales
WHERE NOT is_deleted
ORDER BY "Date", "Product";
```

| Date | Product | Amount |
|---|---|---|
| 2026-11-16 | 10 | 150.00 |
| 2026-11-16 | 20 | 30.00 |
| 2026-11-17 | 10 | 70.00 |

`Amount` in the result is the sum, not a single order's amount. The production-style
handlers also store technical timestamps, retained order IDs and tombstones to
support incremental updates, moved/deleted orders and resumable monthly rebuilds.
The source additionally has `merged_on` and `is_deleted`; every source write must
refresh its change timestamp and use the shared tracker. For ClickHouse, use
[product_daily_sales_clickhouse.py](examples/product_daily_sales_clickhouse.py),
attach the PostgreSQL source as described there, and read the mart with `FINAL`
and `WHERE NOT is_deleted`.

The low-level alternative without a class is `MartLoop(engine, tracker, destination=..., sources=..., process_changes=process_changes)`, where `process_changes` is a plain function. `MartHandler` is a convenience wrapper over it.

Important: for `MartLoop` to see source changes, source merges must also be
journaled using `DBMerge(..., tracker=tracker).exec()` or the alternative
`tracker.exec_merge(merge)`. Do not run both on the same merge. For the lease
defaults and the choice between acquiring and borrowing, see the compact
[configuration table](DOCUMENTATION.md#optional-merge-tracking-and-leases).

### What MartLoop does

Creating a `MartLoop` registers its logical destination in `dbmerge_marts` through
the tracker engine and service schema, even when disabled. Registration does not
call `setup` or the calculation, require a physical destination table, reset an
existing watermark, or overwrite a rebuild request/cursor/boundary. Creating a
`MartHandler` alone does not register it; `loop()`, `run_once()` and `run_forever()` do.

**First run:** no saved watermark means a full rebuild, even if sources were loaded
outside the tracker and the change journal is empty. Empty sources can initialize
successfully too. Sources must be ready before enabling the loop: registration,
source readiness and successful initialization are separate states. An error does
not publish a successful watermark. This differs from 2.5.0, which could wait for
a source change; plan the initial rebuild load when deploying the new behavior.

Each pass (every `poll_interval` seconds):

1. Recovers abandoned merges (`recover_abandoned`).
2. Claims the destination-derived lease to coordinate competing workers (not a fencing guarantee).
3. Reads its watermark: `watermark = tracker.watermark(destination)` (the first run starts from the beginning).
4. Computes the next watermark: `next_watermark = tracker.safe_boundary(sources)`.
5. For initialized, incremental passes, asks: `tracker.is_changed_since(sources, since=watermark)`.
   - no change → skip (not applicable to initial/explicit/resumed rebuilds);
   - a change, but `next_watermark <= watermark` → wait (a source is still being written).
6. Otherwise calls `process_changes(ctx)`.

The key point: **"should I update" is decided from timestamps, not from a signal**. An event that was "lost" (a crash, a switched-off mart) is not lost — the next pass sees it through `is_changed_since`. Therefore `process_changes` must be **idempotent**: it may run again.

### The watermark

`MartLoop` commits `ctx.next_watermark` only after the whole `process_changes(ctx)` returns successfully. It checks and renews the lease in the same progress transaction. A failed multi-write handler leaves the previous watermark intact. Writes remain independently committed and must be idempotent for replay. Progress is a stable `row_kind="watermark"` row in the same `dbmerge_tracking` table; ordinary operations cannot overwrite it.

The read in the handler is **unbounded**: select rows by `> ctx.watermark`. Rows to the right of `ctx.next_watermark` are processed now and again in the next window (at-least-once). Only the **watermark** is capped: it always equals `ctx.next_watermark`, otherwise the cursor would jump over rows of an unfinished merge that aren't committed yet.

To request a rebuild, use the durable public API, not manual deletion of a
`dbmerge_marts` row (which does not reset a saved watermark):

```python
from dbmerge import request_rebuild
request_rebuild(tracker.engine, ("target", "product_daily_sales"), schema=tracker.schema)
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
    lease_handle: LeaseHandle | None  # borrowed pass acquisition
```

## Reliability tests and CI

Transactional-engine workflows and the Python matrix run merge and tracking/marts as
independent jobs (`suite: merge` / `tracking`). Tracking jobs have a 30-minute
timeout; merge jobs use 30 minutes except the CockroachDB and Python-version
workflows, which allow 90 minutes for their larger/slower merge matrix. These two
workflows report the 20 slowest tests and dump a traceback after 180 seconds in a
test (diagnostic only, not a per-test cancellation).
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

Both tables live in the transactional tracking engine and its configured service
schema. The `schema` **column** identifies the destination's schema, not the schema
containing these service tables. Types below are SQLAlchemy types; their physical
SQL representation depends on the database. `PK` means primary key; `NULL` means
the column is nullable.

### `dbmerge_tracking`

This table combines operation status, durable source-change signals, consumer
progress and leases. It is not an append-only history: finishing an operation
replaces earlier finished operation rows for the same destination, while durable
change and watermark rows are kept independently.

| Field | Type / constraint | Meaning |
|---|---|---|
| `id` | `String(64)`, PK | Internal unique row identifier. Operation rows receive individual IDs; change, watermark and lease rows use stable IDs derived from their identity. Do not use it as a time or ordering key. |
| `row_kind` | `String(16)`, NULL | Row purpose: `operation`, `change`, `watermark` or `lease` (see below). `NULL` is supported for legacy rows. |
| `owner` | `String(255)`, NULL | Tracker/process owner identifier for operation and newly created watermark rows; current holder for leases. A lease with `owner IS NULL` is free. Change rows do not need an owner. |
| `table_name` | `String(255)`, NOT NULL | Source/destination table name or logical destination identifier. On lease rows this is the internal lock key prefixed with `lease:`, not a physical table name. |
| `schema` | `String(255)`, NULL | Schema of the tracked source/destination. Together with `table_name`, identifies it within the tracker. No schema is normally stored as `""` (Oracle stores this as `NULL`); lease rows do not use this column. |
| `scope` | `String(255)`, NULL | Operation category, such as `changes` or `rebuild`; change signals are also separated by scope. `NULL` means no category. On lease rows, instead holds a unique token for the current acquisition, preventing an old holder from renewing or releasing a newer acquisition. |
| `changed_at` | `DateTime(timezone=True)`, NULL | Durable, monotonically advancing change timestamp on `change` rows. Published when an operation changes data or is recovered as abandoned. Remains available even after the corresponding operation row is replaced. `NULL` means no published signal (or a reset signal). |
| `started_at` | `DateTime(timezone=True)`, NOT NULL | Operation start time. For change, watermark and lease rows, the time the service row was created, not the start of every subsequent update/acquisition. |
| `finished_at` | `DateTime(timezone=True)`, NULL | Operation completion or abandoned-operation recovery time. `NULL` on an operation means it has not finished. Watermark rows set this on creation; change and lease rows leave it unset. |
| `heartbeat_at` | `DateTime(timezone=True)`, NOT NULL | Last liveness mark for an active operation or held lease; used with the relevant TTL to detect abandonment/expiry. On change and watermark rows it is initialized at creation and is not a live-worker heartbeat. For a free lease, the retained timestamp does not indicate ownership. |
| `changed` | `Boolean`, NULL | Operation result: `True` when rows were inserted, updated or deleted; `False` when no data changed. Recovery conservatively sets it to `True`. Unfinished operations normally have `NULL`. Watermark rows use `False`; change and lease rows do not use it. This is not the durable change timestamp—see `changed_at`. |
| `watermark` | `DateTime(timezone=True)`, NULL | Successfully processed source boundary for a consumer/destination: where its next incremental window starts. `MartLoop` persists progress in a separate `watermark` row and never moves it backwards. `NULL` means no stored boundary; a mart without a watermark performs an initial full rebuild. It is not the wall-clock finish time. |

| `row_kind` | Purpose and lifecycle |
|---|---|
| `operation` | Tracks a running/completed operation, its heartbeat and result. Several unfinished operations may coexist; only the latest finished operation row is retained per destination. |
| `change` | One durable change signal per `(schema, table_name, scope)`. Updating `changed_at` preserves real changes across later no-op operations. |
| `watermark` | Independent consumer progress per `(schema, table_name)`. Updated only after successful processing, separately from operation-row retention. |
| `lease` | Reusable coordination row for a named lock. Ownership is determined by `owner`, the acquisition token in `scope`, and a fresh `heartbeat_at`. |
| `NULL` | Legacy format: treated as an operation unless its reserved `lease:` name identifies a legacy lease. |

### `dbmerge_marts`

One row per logical mart destination, registered when `MartLoop` is constructed
(or when a rebuild is explicitly requested). Registration alone does not run the
handler or overwrite existing progress. The primary key is the pair
`(table_name, schema)`; the destination need not be a physical SQL table.

| Field | Type / constraint | Meaning |
|---|---|---|
| `table_name` | `String(255)`, composite PK | Mart destination table name or logical identifier. |
| `schema` | `String(255)`, composite PK | Destination schema. A single space (`" "`) represents no schema so the composite key also works on Oracle, where an empty string becomes `NULL`. This is not the service-table schema. |
| `full_rebuild_is_required` | `Boolean`, NOT NULL, default `False` | Pending explicit rebuild request. `request_rebuild()` sets it to `True`; starting the matching request clears it without discarding a newer request. `False` does not imply that no rebuild is running: resume state is stored in `rebuild_cursor`. An initial rebuild also runs without this flag when no watermark exists. |
| `rebuild_cursor` | `String(255)`, NULL | Label of the last successfully completed rebuild block, exposed to the handler as `ctx.rebuild_from`. `NULL` means no active resumable rebuild; `""` means a rebuild has started but no block has finished yet (stored as `" "` on Oracle). Each yielded block label replaces it; successful completion clears it. |
| `rebuild_request` | `String(36)`, NULL | UUID of the latest explicit rebuild request. Distinguishes a request being started from a newer request arriving concurrently, so the newer one is not lost. `NULL` before any explicit request; a retained token is not itself a pending-request flag. |
| `rebuild_boundary` | `DateTime(timezone=True)`, NULL | Safe source boundary captured when a rebuild starts. Retained across interruptions so resuming does not move that rebuild's boundary; used to commit progress on completion. This is a source-processing boundary, not a completion timestamp. |

## Reference

### MergeTracker

| member | what it does |
|---|---|
| `MergeTracker(engine, table_name=..., schema=..., ...)` | a long-lived journal; one tracker serves many merges. Optionally `with MergeTracker(engine) as tracker:` stops the heartbeat on exit |
| `exec(operation, *, destination, scope=None, lease_options=None, lease_handle=None)` | journals a zero-argument callable; returns `(MergeRecord, original_result)`; raises `LeaseBusy` when a requested lease is busy or acquisition times out |
| `exec_merge(merge, *, scope=None, lease_options=None, lease_handle=None, **exec_kwargs)` | adapts a merge to `exec`, inferring destination and changes from row counters; preserves the merge result |
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

`mart = MyMart(engine, tracker=tracker)` requires an explicit, open `MergeTracker`.
Omitting it raises `TypeError`; passing `None` or a non-tracker raises `IncorrectParameter`.
Configure the journal engine, table, schema, heartbeat and other options on the tracker itself.
`mart.run_forever(poll_interval=2.0)` creates a loop and polls until `request_stop()`.
`mart.run_once(*, rebuild=False)` performs one pending pass and propagates errors.
With `rebuild=True`, it records a request even without source changes and runs all
remaining blocks. An interrupted rebuild resumes its saved cursor. A busy lease
returns `False` and leaves the request pending; another worker can claim it.

One tracker may be shared by multiple mart threads. Each run closes only its loop
and its owned lease, never the tracker or engines. Stop and join all workers before
calling `tracker.close()` (or exiting its context manager), then dispose the engines.
Use separate handler instances per concurrent worker and sufficient connection-pool
headroom for writes and lease heartbeats. Do not share SQLAlchemy Connection objects.
Scopes are context-local; heartbeat state and lazy lease creation are lock-protected.
A tracker is not a global lock: distinct destinations may run in parallel, while
workers for the same destination coordinate through operation leases.

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
| `loop(...)` | build a `MartLoop` |
| `run_forever(poll_interval=2.0)` | run the loop |

`run_once(..., rebuild=True)` requests a full rebuild; `rebuild(ctx)` is the handler hook,
not a standalone entry point. The separate `run_mart` function has been removed.

## Things to remember

- **Idempotency.** `process_changes` may run again — don't double-count.
- **Latency.** The mart updates with a delay of up to `poll_interval` (this is polling, not push).
- **`merged_on_field`.** Source tables are merged with a change-timestamp column, and the handler selects rows by `> ctx.watermark` — otherwise there is nothing to "catch up" over the window.
- **One process per mart.** The destination identity is claimed by a lease; a second process skips the pass.



## Required configuration and migration

`MartHandler(engine, *, tracker, destination=None, sources=None)` requires an explicit SQLAlchemy engine
and an open, caller-owned MergeTracker.
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
- Replace `MyMart().loop(engine, tracker)` with `MyMart(engine, tracker=tracker).loop()`.
- Replace `MyMart().run_forever(engine, tracker)` with `MyMart(engine, tracker=tracker).run_forever()`.
- Low-level construction: `MartLoop(engine, tracker, destination, sources, process_changes, ...)`.
- Module/function handler adapters are not supported; define a `MartHandler` subclass.
- Rebuild helpers now accept `(tracking_engine, destination, *, schema=None)`; `schema` selects
  the service-table schema, while the destination tuple carries its business schema.
- Stop old workers before deployment: the lease key is now derived from destination, not old name.
  Existing watermark and cursor identities remain `(schema, identifier)`.

Every loop creates its own lease by default. A borrowed handle remains caller-owned:
closing the loop neither releases that acquisition nor stops its heartbeat.
The handler run methods close their loop, not the supplied tracker. For a ClickHouse
destination, construct the tracker on PostgreSQL. To supply a lease, use
`MyMart(engine, tracker=tracker, lease_handle=handle)`. Rebuild state is stored through `handler.tracker.engine`.

## Debugging examples

Both example files call `mart.run_once()` directly under
`if __name__ == "__main__"`: no separate main function, arguments, signals, or polling.
Edit the connection strings there and run the file or place a breakpoint in the handler:

```bash
python examples/product_daily_sales.py
python examples/product_daily_sales_clickhouse.py
```

For a one-off full rebuild, configure and run `python examples/rebuild_mart.py`.
It calls `mart.run_once(rebuild=True)` without source-change checks. A busy lease
leaves the request pending; an interrupted rebuild resumes its cursor rather than
starting again from the first block. It closes the tracker and disposes the engine
even when a pass fails.

Both one-shot examples construct their tracker explicitly and close it before
disposing their engines. The continuous `examples/runner_marts.py` stops and joins workers,
closes the tracker, then disposes the engine. The ClickHouse example
uses a PostgreSQL tracker because ClickHouse cannot store the journal.
A single pass returns `True` if work ran, otherwise `False`, and propagates errors to the debugger.
The loop releases its lease on exit. Engines remain caller-owned. An initialized
incremental pass without source changes does nothing; the first pass rebuilds.
Sources and ClickHouse's external PostgreSQL mapping must already exist.
Use `mart.run_forever()` for continuous operation.

## Concurrent full and changes example (PostgreSQL, experimental)

The two examples are `examples/product_daily_sales.py` (PostgreSQL) and
`examples/product_daily_sales_clickhouse.py` (ClickHouse with PostgreSQL tracking). One `ProductDailySales`
class declares one logical destination and implements `process_changes(ctx)` and
`rebuild(ctx)`. No constructor scope or separate full/changes destinations are needed.
The parent assigns `scope="changes"` while processing changes and `scope="rebuild"`
while iterating rebuild blocks, including writes through `ctx.tracker.exec_merge(...)`.
Between rebuild blocks, incremental writes are labelled `changes`, then the scope
returns to `rebuild`. Scopes are execution-context-local even with a shared tracker.

The examples use literal table names, raw SQL DDL and an aggregate view.
PostgreSQL reads orders directly; ClickHouse uses a rows view to normalize the
external PostgreSQL column types once at the source boundary.
It stores retained order IDs in a `BIGINT[]` column with a GIN index to find previous
and current groups after updates, moves and deletions. Both methods merge directly
from the aggregate view using `source_table_name`; no Python aggregate records,
external snapshot tables, placeholder INSERTs or advisory locks are needed.
The ClickHouse variant reads affected keys into Python once before the merge:
inlining that nested query can exhaust the external PostgreSQL connection pool
on ClickHouse 23.8. Aggregate rows are still loaded directly by ClickHouse.
Rebuild iterates monthly blocks; changes selects only affected composite keys.

The aggregate view contains active groups only. Both refresh paths use
`delete_mode="mark"`, `delete_mark_field="is_deleted"` and
`delete_mark_values={"Amount": None}` (plus a new tombstone version in ClickHouse).
Missing groups retain their keys and order IDs, but their `Amount` becomes SQL
`NULL`, not zero. Deletion is limited to affected keys, or the current rebuild
month, so unrelated groups remain untouched. The destination `Amount` is nullable.
Rebuild guards existing rows by timestamps and lease ownership; changes uses the
lease guard. These guards do not fence concurrent missing-row insertion races.
Hard deletion before a group's first publication leaves no retained ID; use soft
deletes for durable evidence. A single worker processes changes between rebuild
blocks, not in a separate background thread. Separate operation leases do not by
themselves create parallel execution.

The PostgreSQL aggregate view contains no random write token: unchanged totals,
retained IDs and tombstone state do not rewrite a mart row or advance its
`merged_on`. Timestamp guards therefore do not fence every stale staged snapshot,
including an intermediate value staged before a later equal-total refresh. Use
one worker per mart as shown; separate leases are not a general concurrent-writer
correctness guarantee.

## Reliability limits

The current implementation is at-least-once scheduling, not an atomic snapshot or exactly-once
pipeline. Durable change marks now survive later no-ops; the audit's no-op-loss finding is fixed.
Lease expiration alone cannot stop a stalled writer; rebuilds do not automatically remove obsolete destination keys. See
`audit/tracking-and-marts-2026-11-14.md` for the detailed audit, reproductions and prioritized work.


## Consumer, lease and shutdown contracts

- Scope is assigned internally by the callback: `rebuild` or `changes`. Neither
  `MartHandler` nor `MartLoop` accepts a scope parameter. Source notifications and safe
  boundaries include all scopes; the standalone tracker API still supports custom categories.
- Set `loop.enabled = False` to pause a consumer without advancing its durable watermark.
  Re-enabling or recreating it with the same destination catches up from that watermark; later
  no-ops cannot erase pending changes.
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

### ClickHouse example concurrency limits

The ClickHouse example follows the same rows-view/aggregate-view, retained-ID,
tombstone and monthly-block pattern, with `ReplacingMergeTree(version)` and `FINAL`
reads instead of PostgreSQL updates. Staging captures a microsecond wall-clock version;
a later insertion of an older staged version does not replace a newer one. Equal
versions can tie; synchronize clocks. This is not an atomic source snapshot.
PostgreSQL lease predicates cannot be evaluated inside ClickHouse SQL; parent-loop
leases coordinate workers but do not fence ClickHouse writes after lease loss.
A single worker still processes changes between rebuild blocks, not in parallel.
