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

- **`engine`** *(sqlalchemy.engine.Engine)*: The SQLAlchemy engine connected to your database. Tested with PostgreSQL, MariaDB/MySQL, SQLite, MS SQL and CockroachDB.
- **`table_name`** *(str)*: The name of the target table where data will be merged.
- **`data`** *(list[dict] | dict[str, list] | pd.DataFrame | pl.DataFrame | None, optional)*: The source data to merge. Accepts a list of dictionaries (e.g., `[{'col1': 'val1'}, ...]`), a dict of lists (e.g., `{'col1': ['val1', ...], ...}`) or a Pandas/Polars DataFrame. For a list of dictionaries, the set of columns is taken from the first row: all rows must have the same keys, and keys that appear only in later rows are ignored.
- **`delete_mode`** *(Literal['no', 'delete', 'mark'], optional)*: Defines how to handle records that exist in the target table but are missing from the source data. 
  - `'no'` (default): Retain existing target rows (do nothing).
  - `'delete'`: Hard delete rows from the target table.
  - `'mark'`: Soft delete rows by setting a flag in `delete_mark_field`.
  
  > ⚠️ With `'delete'` or `'mark'`, **every** target row missing from the source is affected. If the source covers only part of the table (or is empty), restrict the scope with the `delete_condition` argument of `exec()` — otherwise an empty source with `delete_mode='delete'` wipes the entire table.
- **`delete_mark_field`** *(str, optional)*: The column used to flag a record as deleted. Must be a **Boolean** or **Integer** column. A row missing from the source is set to `True`/`1`; inserted or resurrected (reappeared) rows are set to `False`/`0` (an active row is `False`/`0`, never `NULL`). If this column is present in the incoming `data`, the supplied value is used as-is. It must be a column of its own: it cannot be part of `key`, nor the same column as `merged_on_field` or `inserted_on_field`. On MySQL/MariaDB a `BOOLEAN` column is really `TINYINT(1)` and is read back as an integer, so the flag there is `0`/`1` rather than `False`/`True`.
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

  A type given here is authoritative: it is used exactly as written and is never substituted, even when the engine then refuses to create a column from it. Types that were auto-detected instead are adapted to the target engine where the generic type would not work — on MySQL/MariaDB a string of unknown length becomes `LONGTEXT`, and on MySQL/MariaDB/MS SQL a number of unknown precision becomes a double and a datetime keeps its microseconds.

  On MySQL/MariaDB a **string column of the merge key** must be given an explicit length here: InnoDB indexes at most 3072 bytes per key, shared by all of its columns, so no default length is safe.
- **`schema`** *(str | None, optional)*: The database schema of the target table. Defaults to `None` (uses the database default schema, e.g., `public` in PostgreSQL). Ignored by SQLite. **Required** for MariaDB/MySQL (must be set to your database name).
- **`temp_schema`** *(str | None, optional)*: The schema where the temporary staging table will be created.
- **`source_table_name`** *(str | None, optional)*: If provided, data will be sourced directly from another existing database table or view instead of Python memory. Mutually exclusive with `data` — passing both raises `IncorrectParameter`.
- **`source_schema`** *(str | None, optional)*: The database schema of the source table or view.
- **`can_create_table`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target table if it does not exist.
- **`can_create_columns`** *(bool, optional)*: Defaults to `True`. Allows the module to append missing columns to the target table.
- **`can_create_schemas`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target schema in the database if it does not exist.

#### Exceptions

All exceptions subclass `RuntimeError`. They are **not** re-exported from the package root — import them from `dbmerge.dbmerge`:

```python
from dbmerge.dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists
```

- **`IncorrectParameter`**: arguments are wrong, missing, or conflict with each other (two roles on one column, `delete_mark_values` pointing at a key or managed column, a non-boolean `commit_all_steps`, `source_schema` without `source_table_name`).
- **`NoKeyError`**: no usable merge key could be determined, or a key column is also used for another role.
- **`IncorrectDataError`**: the input data has an unsupported shape, or a column type could not be resolved.
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
- **`chunk_size`** *(int, optional)*: Defaults to `10000`. Defines the batch size when inserting raw data (from lists of dicts, dicts of lists or Pandas/Polars DataFrames) into the temporary table to avoid memory/query-size limits. Must be a positive integer.

#### Execution Results & Statistics
`exec()` returns a `mergeResult` dataclass. The same statistics are also available as attributes on the `dbmerge` instance after `exec()` completes.

Fields of the returned `mergeResult` (and matching instance attributes):

- **`total_row_count`**: Total number of rows processed from the source data.
- **`inserted_row_count`**: Number of new rows inserted into the target table.
- **`updated_row_count`**: Number of existing rows successfully updated.
- **`deleted_row_count`**: Number of rows deleted (or flagged as deleted).
- **`total_time`**: Total execution time (in seconds) for the entire database operation.
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

- **`insert_sql`**: The exact SQL `INSERT` statement executed against the database.
- **`update_sql`**: The exact SQL `UPDATE` statement executed against the database.
- **`delete_sql`**: The exact SQL `DELETE` (or mark) statement executed against the database.

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

Two consequences worth planning around:

- **With the default naive column, the same code stores a different wall-clock value on different engines.** SQLite records UTC; every other engine records its server's local time, and nothing in the column says which. Moving a pipeline between engines shifts these columns by the server's UTC offset, silently. On PostgreSQL/CockroachDB use a `timezone=True` column as shown above; elsewhere, run the servers in UTC.
- **Resolution limits incremental reads.** A consumer polling `WHERE merged_on > :watermark` cannot distinguish rows written within the same tick — one second on SQLite, about 3 ms on MS SQL. Either accept re-reading the boundary tick (make the consumer idempotent) or advance the watermark by row key as well as by timestamp.

On PostgreSQL and CockroachDB, `now()` returns the *transaction* start time. With the default `commit_all_steps=True` each phase is its own transaction, so rows touched by the update phase and rows added by the insert phase get slightly different timestamps; with `commit_all_steps=False` the whole merge shares one.

If you need a single timestamp with guaranteed semantics across every engine, do not use these parameters — pass your own column in `data` with a value you computed (e.g. `datetime.now(timezone.utc)`), and it will be stored as an ordinary field.

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

Duplicates **in the source** are caught for you: the staging table is built with a primary key over `key`, so duplicate or `NULL` key values fail on load before the target is touched.

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

Any failure then rolls the whole merge back. The cost is one large transaction holding locks longer and producing a bigger WAL/undo segment. Choose per dataset size rather than by default — and note that even with per-step commits, a single bulk `UPDATE` or `DELETE` over millions of rows is still one large statement.

### Checklist before a production merge

1. The key columns have a primary key or unique index in the target table.
2. `delete_mode` matches whether your source is a complete snapshot; if it is partial, `delete_condition` is set.
3. `data_types` is explicit for every decimal, float, datetime and string column — or the schema is pre-created with `can_create_table=False` and `can_create_columns=False`.
4. `commit_all_steps` is chosen deliberately for the size of the dataset — remembering that it never covers schema changes.
5. The merge is safe to re-run, so a partial failure can be resolved by repeating it.
