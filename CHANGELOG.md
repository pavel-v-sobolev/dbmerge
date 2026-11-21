# Changelog

All notable changes to DBMerge. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). Releases before 2.0.0 are described in [GitHub Releases](https://github.com/pavel-v-sobolev/dbmerge/releases).

## [Unreleased]

## [2.7.0] - 2026-11-21

### Added
- `source_query` accepts a SQL string or SQLAlchemy SELECT and loads staging with `INSERT … SELECT`. A `source_condition` can filter the query output. When types cannot be determined for new columns, provide typed SQLAlchemy output or `data_types`.
- PostgreSQL's built-in planar `point` supports reflection, binding, COPY and NULL-safe change detection. PostGIS types are not covered.

### Fixed
- ClickHouse staging loads containing a native `Point` use the native client instead of Arrow.

### Tests
- Exercise numeric latitude/longitude in the shared merge suite and native points on PostgreSQL and ClickHouse. Add SQL-query input and documentation regression checks.

### Documentation
- Add full and scoped snapshot recipes, SQL-source examples, and a bilingual PostgreSQL-to-ClickHouse guide. Clarify data commit boundaries, staging cleanup, and mart watermarks.

## [2.6.0] - 2026-11-17

Tracking, leases and marts remain **beta**. Stop existing workers before rollout;
ensure sources are ready and plan capacity for initial rebuilds on destinations
without a saved watermark. Existing untracked merge calls retain their behavior.

### Added
- `DBMerge` is the canonical class name, exported from both `dbmerge` and
  `dbmerge.dbmerge`. The legacy `dbmerge` name remains an alias of the same class
  for backwards compatibility. Examples and documentation use `DBMerge`.

### Changed
- **Tracking/lease API:** `DBMerge.exec()`, `MergeTracker.exec()` and `exec_merge()`
  now raise `LeaseBusy` when lease acquisition fails or times out, instead of
  returning `None` / `(None, None)`. Catch `LeaseBusy` for standalone retry/skip
  handling. MartLoop already catches it without advancing progress; callbacks
  simply call `merge.exec()`. Successful no-ops still return normal results.

### Fixed
- Reject disconnected outer source SELECTs before staging INSERT, including the
  ClickHouse path; preserve linked joins and scoped subqueries.
- Initialize marts without a source-change journal signal when no watermark exists.
- Register logical mart destinations when creating loops without resetting durable
  progress or requests; close owned resources on constructor failure.
- Avoid truncated primary-key collisions for long lease names with a common prefix.
  Existing short lease IDs and lease lookup names remain unchanged.
- Tolerate concurrent creation of tracking/mart service tables and schemas, while
  preserving unrelated database errors.
- Reject non-finite, boolean and invalid heartbeat/polling intervals before starting workers.
- Restore previous signal handlers when the continuous mart runner exits.
- Include both ClickHouse guides in the source distribution.

### Documentation
- Replace the customer-total mart examples with `product_daily_sales`: source
  `Order`, `Date`, `Product`, `Shop`, `Amount`; result `Date`, `Product`, `SUM(Amount)`
  across shops. Include a journaled source seeder, PostgreSQL/ClickHouse handlers,
  and updated runner/rebuild entry points.
- Describe every tracking and mart-state field, row kinds and nullable-state semantics.
- Use absolute GitHub documentation/example links in the README for PyPI rendering.
- Explain initial mart load/readiness, registration, source-filter validation and
  ClickHouse materialized-view row-count limitations.
- Add a version-aware migration note, observable SQLite quick start, guide navigation,
  and the missing tracker/lease/safe-border constructor arguments.
- Correct journal creation timing and execution defaults; show explicit tracker cleanup.
- Add CI checks for API documentation coverage, Python snippet syntax, local file links,
  quick-start execution and startup/lease regressions.

## [2.5.0] - 2026-11-16

Tracking, leases and marts remain **beta**. Existing untracked merge calls retain their behavior.

### Added

- Unified `lease_options` / `lease_handle` arguments, exported `LeaseHandle`, `LeaseLost` and `LeaseBusy`. Handles refer to a single acquisition and are never released by borrowers.
- `use_safe_border=True` protects destination rows at or after the captured safe boundary, including tombstone writes. Requires a tracker, a timestamp `merged_on_field` and `delete_mode='no'` or `'mark'`.
- `LeaseOptions(scope=...)` separates destination leases by operation category.
- Optional `dbmerge(..., tracker=tracker, lease_options=LeaseOptions(...))`: `exec()` journals the merge and acquires a destination lease when options are supplied. Returns `mergeResult` or `None` for a busy lease; journal records are available as `merge.tracking_record`. A lease requires a tracker.
- `mart.run_once(rebuild=True)` requests a full rebuild and executes one pass through its remaining blocks, resuming a saved cursor. A busy lease returns `False` and leaves the request pending.
- One-off full rebuild example in `examples/rebuild_mart.py` and a continuous runner with signal handling and ordered worker/tracker/engine shutdown.

### Changed

- Merge writes explicitly borrow mart pass acquisitions using `lease_handle=ctx.lease_handle`. Explicit merge leases guard update, insert and delete/mark phases without caller-supplied ownership predicates; cross-engine checks are explicitly non-atomic.
- Document the primary bound-tracker execution style, the distinct merge/mart lease defaults, and caller responsibility for the key of a borrowed acquisition. Centralize mart lease validation and remove redundant ownership/release calls.
- Tracked merges fill audit timestamps from the tracker engine's clock, including ClickHouse destinations and the explicit `exec_merge()` adapter.

- **Beta mart API:** require an explicit SQLAlchemy data engine and an open, caller-owned `tracker=` in `MartHandler`. Use `mart.run_forever(poll_interval=...)`, `mart.run_once()` or `mart.loop()`; the separate `run_mart` entry point and module/function adapters are removed.
- Assign mart operation scopes internally: `rebuild` and `changes`. Remove the public mart scope filter; source notifications include all categories.
- Coordinate setup, rebuild and changes with separate parent-managed leases. `lease_options=None` disables coordination. Stop old workers before deploying the changed lease-key format.
- Consolidate mart implementations into PostgreSQL and ClickHouse examples using rows/aggregate views, retained order IDs, tombstones and monthly rebuild blocks. ClickHouse uses `ReplacingMergeTree(version)`; concurrency and fencing limitations are documented.

### Fixed

- Permit a tracker to be shared by mart threads without mixing operation scopes or closing another worker's resources; protect lazy internal lease creation with a lock.

### Migration notes

- Construct and configure `MergeTracker` externally, then pass `tracker=tracker` to marts and merges. Inside mart callbacks explicitly pass `lease_handle=ctx.lease_handle` to merge writes. A missing, invalid or closed mart tracker is rejected. Close shared trackers only after all worker threads have stopped.
- Replace merge `lease=` and tracker `with_lease=` / waiting arguments with `lease_options=LeaseOptions(...)`. Replace mart `with_lease=False` with `lease_options=None`. Borrow an acquired lease using `lease_handle=...`; do not supply options and a handle together. Old ownership predicates and implicit lease inheritance are removed.
- `tracker.exec_merge(merge, ...)` remains supported without double tracking for bound merges. Without a tracker, plain `merge.exec()` stays untracked.
- One worker applies changes **between** rebuild blocks, not in an automatically created background thread. Separate leases do not create parallel execution or fence all concurrent writes; see `TRACKING_AND_MARTS.md`.

## [2.4.1] - 2026-11-14

Tracking, leases and marts remain **beta**. The core merge API is unchanged.

### Fixed

- **MSSQL / Oracle:** compile the mart-state boolean default with the database dialect instead of emitting unsupported `DEFAULT false` SQL.
- **Oracle:** retain no-schema mart identities and the active rebuild cursor before its first block; empty strings are stored with nonempty internal markers because Oracle treats them as NULL.
- **CockroachDB:** explicitly type the monotonic change-timestamp and watermark `CASE` updates so naive datetime parameters do not cause TIMESTAMP/TIMESTAMPTZ conflicts.
- **Regression tests:** keep CockroachDB cleanup DDL separate from reflection, avoid unintended heartbeat expiry in the scope/boundary test, and compare boundary timestamps at the database's stored precision. Add MSSQL/Oracle boolean-default compilation checks.

## [2.4.0] - 2026-11-14

Tracking, leases and marts are **beta**; their APIs may change. The core `dbmerge` merge API is unchanged.

### Changed

- **Breaking mart callback rename:** `MartHandler.process_changes(ctx)` and `MartLoop(..., process_changes=...)` replace `handle`; module/callable adapters and examples use the new name, with no `handle` alias.
- **Consumer progress:** `exec`/`exec_merge` no longer accept `next_watermark`. MartLoop commits a stable `row_kind="watermark"` in the same journal after the whole handler succeeds, guarded by its live lease in the progress transaction. Rebuilds retain a start boundary instead of publishing progress before work.
- **Lease/lifecycle:** lease acquisition is nonreentrant; `still_mine` checks only and `renew` explicitly extends a fresh hold. Tracker/lease heartbeats use an independent pool (or caller-supplied `heartbeat_engine`). Each mart pass releases its lease in `finally`; tracker close drains active writes before stopping heartbeats. Process-wide `request_stop` reaches direct and future loops; `reset_stop` permits new loops after a stop.
- **Breaking tracker API:** use `exec_merge(merge, scope=...)` for merges and `exec(operation, destination=..., scope=...)` for other committed writes. The common executor treats only `False` or numeric zero as no change, preserves original results, and removes the `changed` override. Journals gain a nullable `scope` column; MartLoop can filter wakeups by scope without filtering safe boundaries.
- **Breaking mart API:** `MartHandler(engine, ...)` now requires an explicit engine; `destination` replaces `mart_table`, and `destination` plus non-empty `sources` are validated at construction (class declarations or constructor overrides). The separate mart `name` is removed; a destination-derived lease is created automatically. Use `handler.loop(tracker)` / `handler.run_forever(tracker)` and `ctx.destination`.
- Rebuild helpers now take `(tracking_engine, destination, *, schema=None)`; service-table schema and destination identity are separate. Rebuild state lives in the transactional tracking engine, including for ClickHouse destinations.

### Fixed

- Retain rebuild requests received during an active rebuild, including before its first block; add scope-filtered mart wakeups and disabled-consumer catch-up without filtering safe boundaries. Recovery now prunes previous finished operation rows too.
- Preserve durable last-change timestamps per destination/schema/scope in dedicated `row_kind="change"` rows of `dbmerge_tracking`, without a second tracking table; no-ops and watermark writes no longer erase prior change signals. Completion/recovery publish atomically with journal updates, using monotonic timestamps for out-of-order finishes. Backfill surviving legacy change rows at startup; `is_changed_since(..., scope=...)` can filter change categories without filtering safe boundaries.
- Preserve finished tracking rows and watermarks of same-named tables in different schemas; allow schema-scoped `reset`.
- Watch sources from different schemas independently; match no-schema references exactly.
- Repair rebuild helpers referencing removed identity functions; do not recover expired lease rows as abandoned merges, and recheck heartbeat expiration before recovery writes.
- Close automatically created trackers on runner exit; retain caller-owned shared leases; retain stop requests made before entering the poll loop.

### Added

- Independent, time-bounded tracking/marts CI jobs across the transactional engine and Python matrices; a dedicated ClickHouse/PostgreSQL mart job. Regression coverage includes process crashes/restarts, heartbeat failure/retry and shutdown timeout, rebuild-start request races, and legacy mart-state migration.
- `run_mart(..., run_forever=False)` for one pending pass with error propagation and automatic tracker/lease cleanup; both mart examples call it directly under `if __name__ == "__main__"`, without a main function, arguments, or signals.
- Tracking/marts audit with prioritized reliability recommendations and usage documentation in `TRACKING_AND_MARTS.md`.
- Include the tracking/marts guide and runnable examples in the source distribution.

- **Resumable rebuilds** (`dbmerge.mart`): `MartHandler.rebuild` yields one label per committed block; `MartLoop` saves the label in a per-mart `dbmerge_marts` table so an interrupted rebuild resumes from the next block, and it applies incremental changes between blocks. `request_rebuild(tracking_engine, destination, schema=...)` / `rebuild_requested(...)` manage requests; MartLoop publishes the stored rebuild boundary only after successful completion.
- **Simplified table identity in the service tables** (`dbmerge.tracking`, `dbmerge.mart`): the journal and `dbmerge_marts` identify a table by `schema` + `table_name` only (the `database_type`/`instance_name`/`database_name` columns are gone, and `Database`/`database_of` are removed). Read methods take `schema=`; two databases with a conflicting name need separate tracking tables.

## [2.3.1] - 2026-11-13

### Added

- **Per-table merge lease** (`MergeTracker.exec`): `with_lease=True` makes a merge exclusive per table, `wait_lease`/`wait_timeout`/`wait_interval` control waiting for a held lease, and the wait is logged at INFO with the current holder. `run_mart` no longer creates a `Lease` automatically — pass a shared one when cross-process exclusivity is needed.

## [2.3.0] - 2026-11-12

### Added

- **Change tracking** (`dbmerge.tracking`): a `MergeTracker` journals every merge into one table (`dbmerge_tracking` by default) — when it started/finished, whether rows really changed, and the identity of the database the merged table lives in (`database_type`/`instance_name`/`database_name`). Readers answer the two questions a downstream consumer asks: `safe_boundary()` — up to which moment the data is settled (in-progress merges hold it back), and `is_changed_since()`/`last_merge()`/`watermark()` — when and whether a table changed. A crashed merge stops holding the boundary once its heartbeat expires and is picked up by `recover_abandoned()`. Tracking requires a transactional shared store: ClickHouse and DuckDB are rejected — track a ClickHouse mart through PostgreSQL.
- **Marts** (`dbmerge.mart`): a `MartLoop` polls the tracking table and runs a `MartHandler` only when the declared sources really changed since the mart's watermark. `MartHandler` declares `name`/`mart_table`/`sources` and implements `handle`, optionally `setup` (one-time DDL) and `rebuild` (full rebuild; no resume — an interrupted rebuild starts over). The watermark is recorded on the mart's own tracking row via `tracker.exec(merge, next_watermark=...)`.
- **Lease** (`dbmerge.lease`): named role ownership with a heartbeat, stored in the same tracking table under a reserved `lease:<name>` key, so two processes never build the same mart.
- **`dbmerge` execution defaults**: the constructor now accepts `commit_all_steps`, `chunk_size` and `bulk_load`; `exec()` keeps them for backward compatibility and an explicit argument overrides the constructor value.

## [2.2.0] - 2026-11-11

### Added

- **Oracle**, through python-oracledb (`pip install dbmerge[oracle]`), tested on Oracle XE 21c in Thin mode. Updates run as correlated scalar `SELECT`s guarded by `EXISTS` (Oracle's SQLAlchemy dialect has no `UPDATE … FROM`); the staging table is a `GLOBAL TEMPORARY` table with `ON COMMIT PRESERVE ROWS`; CLOB/NCLOB/BLOB values are compared with `DBMS_LOB.COMPARE`. Inferred strings become `CLOB` (a string key needs an explicit `String(length)`), inferred datetimes become `TIMESTAMP` to keep microseconds, and inferred floats become `BINARY_DOUBLE`. Native JSON and `LONG` columns are not supported, and schemas are existing Oracle users — dbmerge never creates them. See [Oracle support](ORACLE.md).
- **Oracle bulk load:** `bulk_load=True` loads the staging table with python-oracledb's array DML (`cursor.executemany`), with `setinputsizes` taken from the same column types SQLAlchemy uses, so a `TIMESTAMP` keeps its microseconds. Each chunk runs in a savepoint and falls back to the row by row `INSERT` when the array DML refuses it, reporting the same error.
- **Oracle CI:** a `test_oracle.yml` workflow runs the shared matrix and the Oracle-specific tests against Oracle XE 21c.
- **Per-database test hooks** in `tests/db_hooks`: a database can prepare its engine once per URL, skip scenarios it does not support, and adapt parity input and computed-column options. Oracle uses them for its schema owners, native-JSON skips and virtual computed columns.

### Notes

- Oracle runs on SQLAlchemy 2.0 and 2.1. Native JSON and `LONG` columns are not supported on 2.0; from 2.1 the dialect grows a native `JSON` type, but the shared suite still skips the native-JSON scenario.

## [2.1.2] - 2026-11-06

### Fixed

- **CockroachDB:** retry the complete merge up to five times after a transaction serialization failure (`SQLSTATE 40001`). Each retry uses a fresh connection and staging table, with a short exponential backoff. Other database errors are raised without retrying.

### Notes

- With `commit_all_steps=True`, stages committed before a retry remain committed; retrying the merge does not make those stages atomic. Schema changes are also outside the merge transaction, as before.

## [2.1.1] - 2026-10-29

### Fixed

- **A view of a PostgreSQL or MySQL database attached to ClickHouse** works as `source_table_name`. It was not found: `system.tables` lists only the tables of such a database.
- **`source_condition` with an `IN` or `NOT IN` of one column over a subquery, on a source of such a database**, reads only the rows it picks. ClickHouse sends to the other server only the filters that compare columns with constants, so it read the whole source and filtered it itself. DBMerge now runs the subquery first and writes its rows into the condition. An `IN` of several columns and a subquery correlated with the source are still left to ClickHouse.
- **A `datetime` in a condition on ClickHouse is compared as the moment it names.** clickhouse-connect bound it as text with whole seconds, in the time zone of the server, which moved the bound of a condition such as `merged_on > last_run` to the start of its second, and missed the moment in a column of a time zone of its own, such as `DateTime64(6, 'UTC')`, on a server outside UTC.
- **A long `in_()` list in a condition on ClickHouse** no longer fails with `Max query size exceeded`: `max_query_size` is raised for the statement.

## [2.1.0] - 2026-10-28

### Added

- **`source_table_name` on ClickHouse.** The source can be any table or view ClickHouse reads, including a PostgreSQL or MySQL database attached to ClickHouse with its database engine; the server copies it into the staging table itself. A `ReplacingMergeTree` source is read with `FINAL`. A target created from a source takes its column types, with the key columns made not nullable. See [Source tables on ClickHouse](DOCUMENTATION.md#source-tables-on-clickhouse).

### Fixed

- **`in_()` with a list of values in a condition.** On ClickHouse every condition with it failed, and so did `insert_condition` and `source_condition` with DuckDB, psycopg 3 and mysql-connector-python: the list reached the database as an unexpanded `__[POSTCOMPILE_…]` placeholder.

## [2.0.1] - 2026-10-26

### Fixed

- **PostgreSQL and CockroachDB, bulk load:** a value that COPY refuses is reported as the same SQLAlchemy exception that the row by row `INSERT` raises (`DataError` and so on). It used to be hidden behind `current transaction is aborted` from the cleanup; with a naive `datetime` column in the data, it arrived as the driver's own exception, which `except sqlalchemy.exc.DataError` does not catch.
- **psycopg 3 with SQLAlchemy 2.1:** JSON columns go through COPY. With pyarrow, their chunks used to fall back to the row by row `INSERT` silently; without pyarrow, the merge failed.
- **PostgreSQL and CockroachDB without pyarrow (Python 3.12+):** a pandas integer column with a missing value, which pandas holds as floats, no longer fails COPY on values such as `1.0`.
- **MS SQL Server with pyodbc:** a bad value in a chunk is reported as itself, not as a violation of the primary key of the staging table.
- **DuckDB:** a chunk that Arrow can not hold, such as a column of mixed types, goes row by row, so a bad value is reported by DuckDB rather than as a pyarrow error.
- **ClickHouse:** a value that does not fit its column raises `IncorrectDataError` naming the column and the row, rather than a bare pyarrow error.
- **A failure to drop the staging table** no longer hides the error of the merge, and the connection is always closed. After a merge that succeeded, the failure is logged as a warning with the name of the table.
- **`source_table_name` with a computed target column** (`GENERATED ALWAYS AS …`): the column is left out and computed by the target, instead of failing the update.

### Changed

- **Every row of a list of dicts must have the same keys, and a pandas DataFrame must not have two columns of one name.** Both raise `IncorrectDataError`. Before, the values of a key missing from the first row were dropped, a key missing from a later row failed the row by row `INSERT` but became `NULL` through COPY, and of two columns of one name only the last one was stored.
- **A computed target column in `data`** raises `IncorrectParameter` before anything is written.
- **mssql-python** loads the staging table in chunks of 100,000 rows unless `chunk_size` is given. Every bulk copy opens a connection of its own; a million rows load in about half the time.

### Added

- The exceptions can be imported from the package root: `from dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists`. The old path `dbmerge.dbmerge` still works.

### Known issues

- **CockroachDB:** its COPY, which loads the staging table with `bulk_load=True`, does not apply the limits of column types. A string longer than `VARCHAR(n)` or `CHAR(n)` is cut, a number beyond `DECIMAL(p,s)` is stored as it is, and extra decimal places are not rounded, where the row by row `INSERT` refuses or rounds. Pass `exec(bulk_load=False)` if your data may not fit the column types. This was so in 2.0.0 as well.
- **pg8000 1.31.5** does not connect with scramp 1.5.0, released on 2026-10-25. Install `scramp<1.5` until pg8000 fixes it.

## [2.0.0] - 2026-10-25

### Added

- **ClickHouse**, through clickhouse-connect (`pip install dbmerge[clickhouse]`): merges into `ReplacingMergeTree` tables, from a `clickhousedb://` engine or a clickhouse-connect client.
- **DuckDB**, under SQLAlchemy 2.0 and 2.1.
- **Bulk load of the staging table**, `exec(bulk_load=True)` by default: `COPY FROM STDIN` on PostgreSQL and CockroachDB, bulk copy with mssql-python, `fast_executemany` with pyodbc, Arrow on DuckDB and ClickHouse.
- **Tested drivers:** psycopg 3 and pg8000 on PostgreSQL, psycopg 3 on CockroachDB, PyMySQL, mysql-connector-python and mysqlclient on MySQL, mssql-python (SQLAlchemy 2.1 only) and pymssql on MS SQL Server. **SQLAlchemy 2.1** and **Python 3.15** are tested alongside SQLAlchemy 2.0 and earlier Pythons.
- [COMPATIBILITY.md](COMPATIBILITY.md): the tested databases, drivers and versions, the bulk load of each driver, and where drivers store the same data differently.

### Changed

- `exec(chunk_size=None)` by default: 10,000 rows, 1,000,000 on ClickHouse.
- **MySQL:** the staging table is a regular table instead of a `TEMPORARY` one, since MySQL can not refer to one temporary table twice in a statement. MariaDB keeps the `TEMPORARY` table.
- **Without pyarrow:** PostgreSQL and CockroachDB write the CSV for COPY with the `csv` module on Python 3.12+, and load row by row with a warning on older Pythons; DuckDB and ClickHouse warn and load without Arrow.
- **PostgreSQL:** a float in a `NUMERIC` column without precision may be stored with another scale (`1.0` against `1`); the values are equal as numbers.
- **DuckDB:** a column for numbers of unknown precision is created as `DOUBLE`. As a generic `NUMERIC` it would be `DECIMAL(18,3)` there, rounded to three decimal places.

### Fixed

- Correct `inserted_row_count` with psycopg 3, mysql-connector-python and DuckDB, which reported no count for `INSERT … SELECT`.
- DuckDB: the table definition is read from DuckDB's own catalog. SQLAlchemy reflection fails on DuckDB under SQLAlchemy 2.1, and under 2.0 read `DOUBLE` as a 4-byte float (cutting a staging table to single precision) and missed the primary key.

### Upgrading from 1.x

- **Bulk load is on by default.** Pass `exec(bulk_load=False)` to load the staging table row by row as before.
- **MySQL:** the staging table is a regular table now, and a process killed mid-merge can leave it behind; see [The Staging Table](DOCUMENTATION.md#the-staging-table).
- **CockroachDB:** with the bulk load, values that do not fit their column types are not refused (see Known issues of 2.0.1).
- **DuckDB:** a float column created by an earlier version is `DECIMAL(18,3)` and keeps rounding. Change it to `DOUBLE` and reload the data if the decimals matter.
- **pymssql** keeps milliseconds only, a limitation of the driver, and has no bulk load. Prefer pyodbc or mssql-python on MS SQL Server.

[2.2.0]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.1.2...2.2.0
[2.1.2]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.1.1...2.1.2
[2.1.1]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.0.1...2.1.0
[2.0.1]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.0.0...2.0.1
[2.0.0]: https://github.com/pavel-v-sobolev/dbmerge/compare/1.0.24...2.0.0
