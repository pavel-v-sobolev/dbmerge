# DBMerge API Documentation

The `dbmerge` module provides a simple, robust interface for merging data into SQL database tables. The core functionality is encapsulated within the `dbmerge` class, which is designed to be used as a context manager to ensure safe resource handling and connection closure.

---

## Class: `dbmerge`

### Initialization: `__init__`

The initialization method prepares the database and internal structures before the actual merge operation occurs. 

**Key Preparation Steps:**
1. Verifies the existence of the target table. If it does not exist, it is automatically created.
2. Inspects existing table fields. Missing columns are automatically created based on the provided or auto-detected data types.
3. Creates a temporary table to ensure the merge operation is executed with optimal performance. The temporary table is safely dropped when the context manager exits.

**Recommended Usage (Context Manager):**
```python
from dbmerge import dbmerge

with dbmerge(engine=engine, data=data, table_name="YourTable") as merge:
    result = merge.exec()
```

#### Arguments

- **`engine`** *(sqlalchemy.engine.Engine | clickhouse_connect client)*: The SQLAlchemy engine connected to your database. Tested with PostgreSQL, MariaDB/MySQL, SQLite, MS SQL, CockroachDB, DuckDB and ClickHouse — [COMPATIBILITY.md](COMPATIBILITY.md) lists the drivers and versions. For ClickHouse it may also be a clickhouse-connect client — see [ClickHouse](#clickhouse).
- **`table_name`** *(str)*: The name of the target table where data will be merged.
- **`data`** *(list[dict] | dict[str, list] | pd.DataFrame | pl.DataFrame | None, optional)*: The source data to merge. Accepts a list of dictionaries (e.g., `[{'col1': 'val1'}, ...]`), a dict of lists (e.g., `{'col1': ['val1', ...], ...}`) or a Pandas/Polars DataFrame. For a list of dictionaries, every row must have the same keys as the first one, and a DataFrame must not have two columns of one name: otherwise the values of a column would be lost, so either raises `IncorrectDataError`. A column the target computes itself (`GENERATED ALWAYS AS …`) can not be written, and raises `IncorrectParameter` when it comes in `data` (on DuckDB the database refuses it itself).
- **`delete_mode`** *(Literal['no', 'delete', 'mark'], optional)*: Defines how to handle records that exist in the target table but are missing from the source data. 
  - `'no'` (default): Retain existing target rows (do nothing).
  - `'delete'`: Hard delete rows from the target table.
  - `'mark'`: Soft delete rows by setting a flag in `delete_mark_field`.
  
  > ⚠️ With `'delete'` or `'mark'`, **every** target row missing from the source is affected. If the source covers only part of the table (or is empty), restrict the scope with the `delete_condition` argument of `exec()` — otherwise an empty source with `delete_mode='delete'` wipes the entire table.
- **`delete_mark_field`** *(str, optional)*: The column used to flag a record as deleted. Must be a **Boolean** or **Integer** column. A row missing from the source is set to `True`/`1`; inserted or resurrected (reappeared) rows are set to `False`/`0`, so a flag DBMerge manages is never `NULL`. If this column is present in the incoming `data`, the supplied value is used as-is, `NULL` included. It must be a column of its own: it cannot be part of `key`, nor the same column as `merged_on_field` or `inserted_on_field`. On MySQL/MariaDB a `BOOLEAN` column is really `TINYINT(1)` and is read back as an integer, so the flag there is `0`/`1` rather than `False`/`True`.
- **`delete_mark_values`** *(dict[str, Any] | None, optional)*: Extra columns to write when a row is marked as deleted, as `{column name: value}`. Requires `delete_mode='mark'`. Without it, a marked row keeps the values of the last load that still contained it; with it, you can also record something about the marking itself — for example `delete_mark_values={'load_id': 42}` writes `42` into the `load_id` column of every row that disappeared from the source. The columns must exist in the target table. They cannot be part of `key` (that would rewrite the identity of the row being marked), and `delete_mark_field`, `merged_on_field` and `inserted_on_field` are managed automatically and cannot be listed here either.
- **`merged_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically updated to the current datetime whenever a row is inserted, updated, or marked as deleted. This column is always managed automatically: if present in the incoming `data`, the supplied values are ignored.
- **`inserted_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically set to the current datetime when a new row is initially inserted. It is ignored during updates or deletions. This column is always managed automatically: if present in the incoming `data`, the supplied values are ignored.
- **`skip_update_fields`** *(list, optional)*: A list of column names to exclude from the `UPDATE` operation. These fields will only be written during the initial `INSERT`. Such a column is also never compared, so a difference in it alone does not update the row — a column that is never written cannot be a reason to update it.
- **`skip_compare_fields`** *(list, optional)*: A list of column names that are written, but never compared. The row is updated only when some **other** column differs; then these columns are written together with the rest. A row whose only difference is in these columns is not updated at all, so `merged_on_field` is not bumped and the row does not look changed.

  Use it for a column that gets a new value on every load — a load id, an import timestamp — which would otherwise make every row look modified:

  ```python
  skip_compare_fields=['load_id']   # written on update, but never causes one
  ```

  The difference from `skip_update_fields` is what happens when the row does change: skipped-update columns are still not written and keep their inserted value, skipped-compare columns are written.
- **`key`** *(list | None, optional)*: A list of column names serving as the unique key to compare source and target tables. If omitted, the module attempts to use the target table's Primary Key. *Note: If the table does not exist yet, this parameter is required to create the Primary Key.*

  The key is always supplied by your data, so a key column DBMerge creates is never a generated one: no `AUTO_INCREMENT`, `SERIAL` or `IDENTITY`. If you want the database to generate surrogate ids, add that column to your schema yourself — it is not part of the merge key.
- **`data_types`** *(dict[str, types.TypeEngine] | None, optional)*: A dictionary mapping column names to SQLAlchemy data types (e.g., `{'Name': String(100)}`). Used when creating missing tables or columns. If omitted, data types are auto-detected from the source data.

  A type given here is authoritative: it is used exactly as written and is never substituted, even when the engine then refuses to create a column from it. Types that were auto-detected instead are adapted to the target engine where the generic type would not work — on MySQL/MariaDB a string of unknown length becomes `LONGTEXT`, on MySQL/MariaDB/MS SQL/DuckDB a number of unknown precision becomes a double (DuckDB would round it to three decimal places), and on MySQL/MariaDB/MS SQL a datetime keeps its microseconds.

  On MySQL/MariaDB a **string column of the merge key** must be given an explicit length here: InnoDB indexes at most 3072 bytes per key, shared by all of its columns, so no default length is safe.
- **`schema`** *(str | None, optional)*: The database schema of the target table. Defaults to `None` (uses the database default schema, e.g., `public` in PostgreSQL). Ignored by SQLite. **Required** for MariaDB/MySQL (must be set to your database name). On ClickHouse it is a database, and defaults to the current database of the connection.
- **`temp_schema`** *(str | None, optional)*: The schema where the temporary staging table will be created. Defaults to `schema`. The table is named `tmp_<yymmddHHMMSS>_<table>_<hex8>` and dropped when the context manager exits — see [The Staging Table](#the-staging-table) for the naming rules and for how it is created on each engine. Quietly ignored on PostgreSQL and DuckDB, which keep temporary tables in a schema of their own, and on ClickHouse whenever the staging table is a temporary one there.
- **`source_table_name`** *(str | None, optional)*: If provided, data will be sourced directly from another existing database table or view instead of Python memory. Mutually exclusive with `data` — passing both raises `IncorrectParameter`. A column the target computes itself (`GENERATED ALWAYS AS …`) is left out: the target computes it anyway. Not supported on ClickHouse.
- **`source_schema`** *(str | None, optional)*: The database schema of the source table or view.
- **`can_create_table`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target table if it does not exist.
- **`can_create_columns`** *(bool, optional)*: Defaults to `True`. Allows the module to append missing columns to the target table.
- **`can_create_schemas`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target schema in the database if it does not exist.

#### Exceptions

All exceptions subclass `RuntimeError`, and can be imported from the package root (`from dbmerge.dbmerge import …` works as well):

```python
from dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists
```

- **`IncorrectParameter`**: arguments are wrong, missing, or conflict with each other (two roles on one column, `delete_mark_values` pointing at a key or managed column, a non-boolean `commit_all_steps`, `source_schema` without `source_table_name`, a column the target computes itself brought in `data`).
- **`NoKeyError`**: no usable merge key could be determined, or a key column is also used for another role.
- **`IncorrectDataError`**: the input data has an unsupported shape (rows of a list of dicts with different keys, two DataFrame columns of one name), a column type could not be resolved, or on ClickHouse a value does not fit its column (the error names the column and the row).
- **`TableNotFoundError`**: the target table does not exist and `can_create_table=False`.
- **`TempTableAlreadyExists`**: a staging table with the generated name already exists.

#### Instance Attributes (Available after initialization)
- **`table`**: The SQLAlchemy `Table` object representing your target table.
- **`temp_table`**: The SQLAlchemy `Table` object representing the temporary staging table.
- **`source_table`**: The SQLAlchemy `Table` object representing the source table (only populated if `source_table_name` was provided).

---

### Execution: `exec()`

This method triggers the actual merge operation based on the configurations passed during initialization.

**Execution Workflow:**
1. Inserts the source data into the temporary staging table.
2. **Update:** Updates existing rows in the target table where field values differ from the temporary table.
3. **Insert:** Copies rows from the temporary table to the target table that do not currently exist.
4. **Delete / Mark:** Deletes or marks rows in the target table that are entirely missing from the temporary staging data.

Because updates run before deletes, a `UNIQUE` constraint on columns other than the key can stop a valid snapshot. When a value moves to another row while the row that holds it now leaves the source — code `'B'` goes from row 2, which is deleted, to row 1 — the update meets the old row 2 still in place and fails with an `IntegrityError`. Such a change takes two merges, or the constraint has to be checked differently.

**Scoped Deletion Example:**
If you load data in chunks (e.g., monthly snapshots), you can restrict the deletion scope using the `delete_condition` argument so that it only affects the relevant timeframe.

```python
from datetime import date

with dbmerge(data=data, engine=engine, table_name="YourTable", delete_mode='delete') as merge:
    # Restrict deletion scope to January 2025
    condition = merge.table.c['Date'].between(date(2025, 1, 1), date(2025, 1, 31))
    result = merge.exec(delete_condition=condition)
```

**Conditional Update Example:**
By default the update phase overwrites a target row whenever any field differs from the source. Pass an `update_condition` to restrict which rows may be overwritten. For example, keep rows a user has marked as protected and refresh everything else:

```python
with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
    # Never overwrite rows flagged as protected in the target table.
    result = merge.exec(update_condition=merge.table.c['is_protected'] == False)
```

Rows that fail the condition are simply filtered out of the set of rows to update — they are left untouched (never deleted). The condition applies to the update phase only; the insert phase still inserts rows that are missing from the target. You can also compare the incoming value against the stored one (e.g. only overwrite when `merge.temp_table.c['updated_at'] >= merge.table.c['updated_at']`) to keep the freshest write.

**Conditional Insert Example:**
By default the insert phase adds every source row that is missing from the target. Pass an `insert_condition` to restrict which of those rows are inserted:

```python
with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
    # Insert only rows with a positive amount.
    result = merge.exec(insert_condition=merge.temp_table.c['amount'] > 0)
```

Build the condition on `merge.temp_table` (the row being inserted). Do **not** reference `merge.table` directly here — during insert the target row is absent (`NULL`) by definition. To look at *other* target rows, use a correlated `EXISTS` over a separate alias of the target:

```python
from sqlalchemy import exists

with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
    # Skip inserting into a category that already has a locked row.
    g = merge.table.alias()
    guard = ~exists().where((g.c['category'] == merge.temp_table.c['category']) & (g.c['locked'] == True))
    result = merge.exec(insert_condition=guard)
```

#### Arguments
- **`delete_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression used in the `WHERE` clause during the delete/mark phase. Essential for chunked or partitioned data syncs.
- **`source_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression used to filter the `SELECT` statement when loading data from a `source_table_name`.
- **`update_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression added to the `WHERE` clause of the update phase. A target row is updated only when it differs from the source **and** satisfies this condition. Build it on `merge.table` (target) and `merge.temp_table` (staging).
- **`insert_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression added to the `WHERE` clause of the insert phase. A source row missing from the target is inserted only when it also satisfies this condition. Build it on `merge.temp_table` (the row being inserted); to inspect other target rows use a correlated `EXISTS` over `merge.table.alias()`.
- **`commit_all_steps`** *(bool, optional)*: Defaults to `True`. If `True`, every step (temp insert, target insert, update, delete) is committed immediately. If `False`, a single commit is issued after all steps complete successfully. Must be a real boolean — a string such as `'false'` is rejected rather than silently treated as truthy.

  It applies to the **data only**. Schema changes — creating the target table, adding columns, creating and dropping the staging table — are always committed on their own and are never part of the merge transaction, so `commit_all_steps=False` does not make them roll back with the data.

  It has no effect on ClickHouse, which has no transactions: every statement there is final as soon as it runs.
- **`chunk_size`** *(int, optional)*: Defaults to `10000`; to `100000` for the bulk copy of mssql-python, where every chunk opens a connection of its own; and to `1000000` on ClickHouse, where every insert creates a new part of the table and small ones are costly. Defines the batch size when inserting raw data (from lists of dicts, dicts of lists or Pandas/Polars DataFrames) into the temporary table to avoid memory/query-size limits. Must be a positive integer.
- **`bulk_load`** *(bool, optional)*: Defaults to `True`. Loads the staging table in bulk where the database and its driver allow it — `COPY FROM STDIN` on PostgreSQL and CockroachDB, bulk copy with mssql-python, `fast_executemany` with pyodbc, Arrow on DuckDB — instead of SQLAlchemy's row by row `INSERT`. The values stored are the same either way — except on CockroachDB, whose COPY does not apply the limits of column types (see [CockroachDB](COMPATIBILITY.md#cockroachdb)); `False` turns it off. ClickHouse always loads in bulk. See [Bulk Staging Load](COMPATIBILITY.md#bulk-staging-load) for each driver, the speed-ups, and the data that still goes row by row.

#### Execution Results & Statistics
`exec()` returns a `mergeResult` dataclass. The same statistics are also available as attributes on the `dbmerge` instance after `exec()` completes.

Fields of the returned `mergeResult` (and matching instance attributes):

- **`total_row_count`**: Total number of rows processed from the source data.
- **`inserted_row_count`**: Number of new rows inserted into the target table.
- **`updated_row_count`**: Number of existing rows successfully updated.
- **`deleted_row_count`**: Number of rows deleted (or flagged as deleted).
- **`total_time`**: The sum of `temp_insert_time`, `insert_time`, `update_time` and `delete_time`, in seconds. Schema changes, commits and the cleanup of the staging table are not part of it.
- **`temp_insert_time`**: Time taken (in seconds) to load data into the temporary table.
- **`insert_time`**: Time taken (in seconds) to perform the target `INSERT` step.
- **`update_time`**: Time taken (in seconds) to perform the target `UPDATE` step.
- **`delete_time`**: Time taken (in seconds) to perform the `DELETE` or `MARK` step.
- **`table_created`**: `True` when this merge created the target table. Its columns are **not** listed in `added_fields` — there was no previous version of the table to add them to.
- **`added_fields`**: A `{column name: SQLAlchemy type}` mapping of the columns this merge added to an already existing target table. The type is the one the column was really created with, after any adjustment made for the target engine — so on MySQL/MariaDB a string of unknown length appears as `LONGTEXT`, not as the generic `String`. The automatically managed columns (`merged_on_field`, `inserted_on_field`, and an auto-managed `delete_mark_field`) are left out — they carry no source data. A delete flag supplied in `data` is a data column and is reported. With `can_create_columns=False` the mapping stays empty: the missing columns are dropped from the merge instead of being created.

`table_created` and `added_fields` are known once the context manager is entered, so they can also be read off the `dbmerge` instance before `exec()`.

They exist for consumers that maintain a derived dataset incrementally from a `merged_on_field` watermark. Such a consumer can not notice a new column on its own: the watermark only moves on rows whose **values** changed, and adding a column changes no values — every row already in the table keeps its old timestamp and stays outside the increment. `added_fields` is that missing signal, and is typically used to flag the derived dataset for a full recalculation:

```python
with dbmerge(engine=engine, data=data, table_name="Facts", merged_on_field='merged_on') as merge:
    result = merge.exec()

if result.added_fields:
    logger.info(f"Facts gained columns {result.added_fields} - marts built on it need a full rebuild")
```

The executed SQL statements are exposed only on the `dbmerge` instance (not in `mergeResult`):

- **`insert_sql`**: The `INSERT` statement of the insert phase, as SQLAlchemy renders it, with placeholders in place of the values.
- **`update_sql`**: The `UPDATE` statement of the update phase, rendered the same way.
- **`delete_sql`**: The `DELETE` (or mark) statement of the delete phase, rendered the same way.

---

## The Staging Table

Every merge loads its data into a staging table first, and drops it when the context manager exits. The name follows a fixed shape:

```
tmp_260823224826_Facts_0472030e
│   │            │     └─ random hex, keeps parallel merges of one table apart
│   │            └─ the target table name
│   └─ creation time, yymmddHHMMSS
└─ fixed prefix marking the table as disposable
```

Each part earns its place. The prefix says the table may be deleted; the timestamp says how old it is; the hex suffix means two processes merging into the same table never collide. The timestamp comes **before** the table name deliberately, so that an ordinary listing of the schema sorts by age — which is what anyone cleaning up by hand goes by.

**Why the name carries a timestamp at all:** PostgreSQL records no creation time for a table, so the name is the only place it can live. That is what makes a leftover recognizable — see below.

The timestamp is read from the **database** clock, the same one that fills `merged_on_field`, because the processes reading these names need not share a clock with each other. It therefore follows the same timezone rules as the audit timestamps in the next section: UTC on SQLite, server-local elsewhere.

**Length.** The whole name is kept within 58 bytes, leaving room under PostgreSQL's 63-byte identifier limit for the `_pkey` suffix it adds to the primary key. Only the middle part — the target table name — is shortened, so the prefix, timestamp and suffix always stay intact and the name stays parsable. Truncation counts bytes, not characters, and never splits a multibyte character:

```
tmp_260823224826_Fact1Fact2Fact3Fact4Fact5Fact6Fa_7c7f55e5     58 bytes
tmp_260823194756_Продажи_по_магази_d15633ca                    58 bytes
```

**Lifetime.** On PostgreSQL, SQLite, MariaDB and DuckDB the staging table is a real `TEMPORARY` table: it belongs to the session that created it, is invisible to every other connection, and the database removes it when that session ends — so even a process killed mid-merge leaves nothing behind. On ClickHouse it is a `TEMPORARY` table too, except in the cases listed under [ClickHouse](#the-staging-table-on-clickhouse).

On PostgreSQL this also keeps the staging data out of `shared_buffers` entirely; it is cached in the session's own `temp_buffers` instead. Two things follow on a busy server: the staging data never evicts the real working set, and dropping the table does not make PostgreSQL scan the whole buffer pool (a cost that grows with `shared_buffers` and is paid again on every standby replaying the drop). Measured on a 14 MB staging table: an `UNLOGGED` table occupied 1277 pages of `shared_buffers`, a `TEMPORARY` one occupied none, with no difference in merge time.

Because PostgreSQL keeps temporary tables in its own per-session schema, `temp_schema` does not apply there and is quietly ignored. The same holds for DuckDB, which keeps them in its `temp` catalog.

**Leftovers** are therefore only possible where the staging table is a regular one — MS SQL Server, CockroachDB, MySQL, and ClickHouse in the cases listed under its section. MySQL gets a regular table because it can not refer to one temporary table twice in a statement (`Can't reopen table`), which an `update_condition` or `insert_condition` built on `merge.temp_table` needs. There the name is what lets you find them:

```sql
-- staging tables older than a day, in creation order
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'tmp'
  AND table_name LIKE 'tmp\_%'
  AND substring(table_name, 5, 12) < to_char(now() - interval '1 day', 'YYMMDDHH24MISS')
ORDER BY table_name;
```

Do not drop a staging table just because it looks old: a long-running merge is still using it. Compare against the age of the merges you actually run.

Should the staging table fail to drop when the merge ends, DBMerge logs a warning with its name rather than raising: the data of a merge that succeeded is committed already, and the error of a merge that failed must not be replaced.

---

## Audit Timestamps

`merged_on_field` and `inserted_on_field` are filled by the **database**, not by the Python process: DBMerge emits the engine's own "current time" function as part of the `INSERT` and `UPDATE` statements. Nothing is normalized on the way — the timezone and the resolution of the stored value are whatever that engine's clock gives.

This is deliberate. The timestamp then reflects the moment the database applied the change, is immune to clock skew between application hosts, and stays consistent with any other column your schema fills with a database default. The cost is that the exact meaning of the value is engine-specific:

When DBMerge creates the audit column itself, it creates a **naive** timestamp (`DATETIME`, or `timestamp without time zone` on PostgreSQL), so the offset the clock function carried is dropped at write time. The table below describes that default.

You can change it by giving the column a timezone-aware type instead — either pre-create it in your schema, or pass `data_types={'Merged On': DateTime(timezone=True)}`. On PostgreSQL and CockroachDB the column then becomes `timestamptz`, `now()` supplies a real offset, and the instant is preserved:

```python
from sqlalchemy import DateTime

with dbmerge(engine=engine, data=data, table_name="Facts", key=['id'],
             merged_on_field='Merged On',
             data_types={'Merged On': DateTime(timezone=True)}) as merge:
    result = merge.exec()
# stored: 2026-08-12 22:26:34.416479+03:00
```

This does not help everywhere: MySQL/MariaDB have no type that stores an offset, and on MS SQL `CURRENT_TIMESTAMP` itself returns a value without one, so a `datetimeoffset` column would merely label server-local time as UTC.

| Engine | Emitted SQL | Timezone of the stored value | Resolution |
|---|---|---|---|
| PostgreSQL | `now()` | session `TimeZone` — `now()` returns a `timestamptz`, which the naive column converts to local time and strips | microseconds |
| CockroachDB | `now()` | session `TimeZone`, which defaults to UTC | microseconds |
| MySQL / MariaDB | `NOW(6)` | server/session `time_zone` | microseconds |
| SQLite | `CURRENT_TIMESTAMP` | **always UTC** | whole seconds |
| MS SQL Server | `CURRENT_TIMESTAMP` | server timezone | ~3.3 ms (it returns a `datetime`) |
| DuckDB | `now()` | session `TimeZone`, which defaults to the local timezone of the machine | microseconds |
| ClickHouse | `now64(6)` | stores the instant; shown in the column's timezone, or else the server's | microseconds |

Two consequences worth planning around:

- **With the default naive column, the same code stores a different wall-clock value on different engines.** SQLite records UTC; every other engine records its server's local time, and nothing in the column says which. Moving a pipeline between engines shifts these columns by the server's UTC offset, silently. On PostgreSQL/CockroachDB use a `timezone=True` column as shown above; elsewhere, run the servers in UTC.
- **Resolution limits incremental reads.** A consumer polling `WHERE merged_on > :watermark` cannot distinguish rows written within the same tick — one second on SQLite, about 3 ms on MS SQL. Either accept re-reading the boundary tick (make the consumer idempotent) or advance the watermark by row key as well as by timestamp.

On PostgreSQL and CockroachDB, `now()` returns the *transaction* start time. With the default `commit_all_steps=True` each phase is its own transaction, so rows touched by the update phase and rows added by the insert phase get slightly different timestamps; with `commit_all_steps=False` the whole merge shares one.

If you need a single timestamp with guaranteed semantics across every engine, do not use these parameters — pass your own column in `data` with a value you computed (e.g. `datetime.now(timezone.utc)`), and it will be stored as an ordinary field.

---

## ClickHouse

ClickHouse has no `UPDATE` suited to merging and no transactions, so DBMerge merges into it differently: a changed row is written as a new version of itself, and a `ReplacingMergeTree` table keeps only the last version of every key. The interface stays the same.

```python
import clickhouse_connect
from sqlalchemy import create_engine
from dbmerge import dbmerge

engine = create_engine("clickhousedb://default:@localhost:8123/default")   # a SQLAlchemy engine,
engine = clickhouse_connect.get_client(host="localhost")                     # or a clickhouse-connect client

with dbmerge(engine=engine, data=data, table_name="Facts", key=['Shop', 'Product', 'Date']) as merge:
    result = merge.exec()
```

Install it with `pip install dbmerge[clickhouse]`, which adds clickhouse-connect and pyarrow. A client passed in stays yours: DBMerge does not close it. `schema` is a ClickHouse database and defaults to the current database of the connection. The server must be 23.8 or newer.

### The target table

It has to be a `ReplacingMergeTree` — plain, `Replicated` or `Shared`. Before anything is written, DBMerge checks that:

- `ORDER BY` lists exactly the merge key columns, in any order. `ReplacingMergeTree` keeps one row per value of `ORDER BY`: an extra column there splits one key into several rows, a missing one merges different keys. Without the `key` argument, the key is taken from `ORDER BY`.
- `PARTITION BY` uses only key columns. Rows are replaced only within a partition, so the versions of a key spread over two partitions would never be merged.
- With a version column, `ReplacingMergeTree(ver)`, the data carries `ver`, or `ver` is the `merged_on_field`. Otherwise every new version would get the default `0` and lose to the row it replaces.
- The data does not try to write `MATERIALIZED`, `ALIAS` or `EPHEMERAL` columns.

A table that fails a check raises `NoKeyError` or `IncorrectParameter`.

A table DBMerge creates is `ENGINE = ReplacingMergeTree ORDER BY (<key>)`, with no version column and no partitioning. The key columns are not nullable, since `ORDER BY` refuses nullable columns; every other column is `Nullable`. Types are chosen so that no value changes on the way in:

| SQLAlchemy type | Detected from the data | Given in `data_types` |
|---|---|---|
| `Boolean` | `Bool` | `Bool` |
| `SmallInteger` / `Integer` / `BigInteger` | `Int16` / `Int32` / `Int64` | the same |
| `Float`, `Numeric` without precision | `Float64` | `Float64` |
| `Numeric(p, s)` | `Decimal(p, s)` | `Decimal(p, s)` |
| `String`, `Text` (any length) | `String` | `String` |
| `DateTime` | `DateTime64(6)` | `DateTime` (whole seconds) |
| `DateTime(timezone=True)` | `DateTime64(6, 'UTC')` | `DateTime('UTC')` |
| `Date` | `Date32` (1900–2299) | `Date` (1970–2149) |
| `Uuid` | `UUID` | `UUID` |
| `JSON`, `LargeBinary` | `String`; dicts and lists are written as JSON text | `String` |

Any ClickHouse type can be given in `data_types` as it is, and is then used exactly as written, nullability included:

```python
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name

data_types={'Shop': sqla_type_from_name('LowCardinality(String)')}
```

A `NULL` arriving for a column that is not `Nullable` fails the load. It is not stored as `0`, `''` or `1970-01-01`, as ClickHouse would otherwise do.

### How the merge runs

1. The data goes into the staging table in Arrow format, in chunks of 1,000,000 rows by default. Without pyarrow it goes through the native protocol of clickhouse-connect, a few times slower, and DBMerge logs a warning once.
2. **Changed rows** are compared with the current version of the target rows and written anew as `INSERT … SELECT`. Columns the merge does not write keep their current value in the new version: `skip_update_fields`, `inserted_on_field`, and columns the data does not have.
3. **New rows** are inserted when the target has no version of their key at all.
4. **Mark** writes a new version of each row missing from the source, with `delete_mark_field` set.
5. **Delete** is a lightweight `DELETE` of every version of the keys missing from the source.

The conditions of `exec()` are built on `merge.table` and `merge.temp_table` as everywhere else. In `update_condition` and `delete_condition`, `merge.table` stands for the current version of the row.

DBMerge itself needs nothing beyond ClickHouse 23.8: its own statements use only joins and `IN` / `NOT IN` over subqueries that do not refer to the outer row. A condition you write yourself may need a newer server, though — when it holds a **correlated subquery**, one that refers to the row being checked, like the `EXISTS` guard in [Conditional Insert Example](#execution-exec): `g.c['category'] == merge.temp_table.c['category']` inside it refers to the staging row. ClickHouse runs such a condition out of the box from 25.8 on; on 25.4–25.7 only with the setting `allow_experimental_correlated_subqueries = 1`, which can go into the engine URL (`clickhousedb://…/default?allow_experimental_correlated_subqueries=1`); 25.3 and older can not run it at all. On an older server, write the same condition with `NOT IN` over a subquery that stands on its own:

```python
from sqlalchemy import select

with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
    # The guard above, without a correlated subquery: ClickHouse 23.8 runs it.
    locked = select(merge.table.c['category']).where(merge.table.c['locked'] == True)
    result = merge.exec(insert_condition=merge.temp_table.c['category'].not_in(locked))
```

The statistics differ slightly from the other engines:

- `updated_row_count`, `inserted_row_count` and, with `delete_mode='mark'`, `deleted_row_count` are the rows each step actually wrote.
- With `delete_mode='delete'`, `deleted_row_count` is counted right before the `DELETE`, which reports no count of its own. If other processes write to the table at the same time, it is an estimate.
- Changed rows are written by an `INSERT`, so their time is part of `insert_time`, and `update_time` is `0`. `update_sql` holds that `INSERT … SELECT`.

### Reading the table: FINAL

`ReplacingMergeTree` removes older versions in background merges, at a time of its choosing. Until then, the table holds a changed row twice, the old version next to the new one. Read it with `FINAL` to see one row per key:

```sql
SELECT * FROM Facts FINAL WHERE Date = '2025-01-01'
```

`SETTINGS final = 1` does the same for every table in a query. Rows removed by `delete_mode='delete'` disappear from every read at once.

### The staging table on ClickHouse

It is a `TEMPORARY` table in the session of the clickhouse-connect client: no other connection sees it, and the server drops it with the session. `temp_schema` is then ignored. It is a regular table in `temp_schema` (which defaults to `schema`) in two cases:

- **`delete_mode='delete'`**: a `DELETE` runs as a mutation, which can not read a temporary table.
- **a client without a session**, created with `autogenerate_session_id=False` (for example to share it between threads): a temporary table would be gone right after it was created.

A regular staging table can be left over by a process killed mid-merge; its name tells how old it is — see [The Staging Table](#the-staging-table).

### Duplicate keys on ClickHouse

The staging table on ClickHouse has no primary key, so duplicate keys in the source are not refused. All of them are written and counted, and `ReplacingMergeTree` keeps one — in practice the last, but this is not guaranteed. Remove duplicates from the source if it matters which one stays. A `NULL` in a key column still fails the load, as on the other engines, since `ORDER BY` columns are not nullable.

### Not supported

- `source_table_name`.
- `commit_all_steps`: there are no transactions, every statement is final as soon as it runs.
- clickhouse-sqlalchemy (`clickhouse://` engines): its driver opens a new session for every statement, so the temporary staging table would not survive. Use clickhouse-connect.

---

## Data Loss Risks

DBMerge rewrites your target table with bulk `UPDATE`/`INSERT`/`DELETE` statements. Three properties of that design can destroy data if you rely on the defaults without thinking about them. They are deliberate trade-offs, not defects — none of them will be "fixed" in a later release, so plan around them.

### 1. The source is treated as a complete snapshot

With `delete_mode='delete'` or `'mark'`, **every** target row that is not present in the source is deleted or flagged. That is the point of the feature, and it makes accidents easy:

- An **empty** source with `delete_mode='delete'` wipes the entire table.
- A source covering only part of the table (one shop, one month, one partition) removes everything else.

Scope the deletion whenever the source is partial:

```python
with dbmerge(data=january_data, engine=engine, table_name="Facts", delete_mode='delete') as merge:
    # Only January rows may be deleted, whatever else the table contains.
    result = merge.exec(delete_condition=merge.table.c['Date'].between(date(2025, 1, 1), date(2025, 1, 31)))
```

DBMerge logs a warning for empty input but still performs the delete — if an empty source is possible in your pipeline, guard the call yourself.

#### The merge key must be unique in the target table

Duplicates **in the source** are caught for you: the staging table is built with a primary key over `key`, so duplicate or `NULL` key values fail on load before the target is touched. ClickHouse is the exception — see [Duplicate keys](#duplicate-keys-on-clickhouse).

The **target** is the database's responsibility. If it has no primary key or unique index on the key columns and already contains duplicate key values, every phase treats them as one logical row and rewrites all of them:

```python
# target (no primary key): (1,'a'), (1,'b'), (2,'c'), (2,'d')
# source: [{'id': 1, 'v': 'NEW'}] with delete_mode='delete'
# result: (1,'NEW'), (1,'NEW')     <- 'b' silently replaced by a copy of 'NEW'
```

The source is valid here; only the target is at fault, and no error is raised. Put a primary key or unique index on the key columns. If you cannot, check the target yourself before merging:

```sql
SELECT id, COUNT(*) FROM your_table GROUP BY id HAVING COUNT(*) > 1;
```

### 2. Automatic schema creation infers types from a sample of your data

With `can_create_table` or `can_create_columns` enabled and no `data_types`, DBMerge picks a column type by looking at the first non-null Python value it finds. Inference from values is a guess, and it is structurally unable to recover the things that matter most:

- **Precision and scale** of a decimal — a sample of `1.75` says nothing about whether the column must hold four decimal places.
- **Length** of a string — the widest value in the sample is not the widest value the column will ever see.
- **Timezone policy** — whether naive values are local or UTC, and whether the offset must survive a round trip.
- **Key semantics** — whether an integer key is externally assigned or should be generated by the database.

The resulting generic type is then resolved by each backend's own defaults, so the same data produces a different physical column on PostgreSQL, MySQL and MS SQL. Where the column matters, say what you mean:

```python
from sqlalchemy import String, Numeric, Double, DateTime

with dbmerge(engine=engine, data=data, table_name="Facts", key=['id'],
             data_types={'Name': String(100),
                         'Price': Numeric(12, 2),
                         'Ratio': Double(),
                         'Ts': DateTime(timezone=True)}) as merge:
    result = merge.exec()
```

Automatic DDL is a convenience for development and ad-hoc loads. For production, manage the schema with your own migrations and run with `can_create_table=False, can_create_columns=False` or revoke DDL privileges from your merge user.

### 3. Per-step commits mean the merge is not atomic

`exec()` defaults to `commit_all_steps=True`, which commits after **each** phase: staging load, update, insert, delete/mark. This keeps transactions, locks and WAL/undo small on large datasets, and it is a deliberate trade-off.

It covers the data and nothing else. Every schema change is committed as it happens, whichever value you pass: the target table, any column added to it, and the staging table are already permanent by the time the first row is written. `commit_all_steps=False` therefore makes the *rows* all-or-nothing, not the schema.

The cost is that a failure leaves everything committed before it in place. Because the order is update → insert → delete:

`exec()` rolls back only the phase that failed; it cannot undo earlier commits, and there is no compensating logic. Treat a raised exception as "the target is in an unknown intermediate state", then re-run the full merge once the cause is fixed — repeating the same complete snapshot converges to the correct result.

For an all-or-nothing merge, use a single transaction:

```python
with dbmerge(engine=engine, data=data, table_name="Facts", delete_mode='delete') as merge:
    result = merge.exec(commit_all_steps=False)
```

Any failure then rolls the whole merge back. The cost is one large transaction holding locks longer and producing a bigger WAL/undo segment. ClickHouse has no transactions, so a merge there is never all-or-nothing: plan to re-run it. Choose per dataset size rather than by default — and note that even with per-step commits, a single bulk `UPDATE` or `DELETE` over millions of rows is still one large statement.

### Checklist before a production merge

1. The key columns have a primary key or unique index in the target table.
2. `delete_mode` matches whether your source is a complete snapshot; if it is partial, `delete_condition` is set.
3. `data_types` is explicit for every decimal, float, datetime and string column — or the schema is pre-created with `can_create_table=False` and `can_create_columns=False`.
4. `commit_all_steps` is chosen deliberately for the size of the dataset — remembering that it never covers schema changes.
5. The merge is safe to re-run, so a partial failure can be resolved by repeating it.
