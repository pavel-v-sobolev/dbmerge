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
- **`delete_mark_field`** *(str, optional)*: The column used to flag a record as deleted. Must be a **Boolean** or **Integer** column. A row missing from the source is set to `True`/`1`; inserted or resurrected (reappeared) rows are set to `False`/`0` (an active row is `False`/`0`, never `NULL`). If this column is present in the incoming `data`, the supplied value is used as-is.
- **`merged_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically updated to the current datetime whenever a row is inserted, updated, or marked as deleted. This column is always managed automatically: if present in the incoming `data`, the supplied values are ignored.
- **`inserted_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically set to the current datetime when a new row is initially inserted. It is ignored during updates or deletions. This column is always managed automatically: if present in the incoming `data`, the supplied values are ignored.
- **`skip_update_fields`** *(list, optional)*: A list of column names to exclude from the `UPDATE` operation. These fields will only be written during the initial `INSERT`.
- **`key`** *(list | None, optional)*: A list of column names serving as the unique key to compare source and target tables. If omitted, the module attempts to use the target table's Primary Key. *Note: If the table does not exist yet, this parameter is required to create the Primary Key.*
- **`data_types`** *(dict[str, types.TypeEngine] | None, optional)*: A dictionary mapping column names to SQLAlchemy data types (e.g., `{'Name': String(100)}`). Used when creating missing tables or columns. If omitted, data types are auto-detected from the source data.
- **`schema`** *(str | None, optional)*: The database schema of the target table. Defaults to `None` (uses the database default schema, e.g., `public` in PostgreSQL). Ignored by SQLite. **Required** for MariaDB/MySQL (must be set to your database name).
- **`temp_schema`** *(str | None, optional)*: The schema where the temporary staging table will be created.
- **`source_table_name`** *(str | None, optional)*: If provided, data will be sourced directly from another existing database table or view instead of Python memory. Mutually exclusive with `data` — passing both raises `IncorrectParameter`.
- **`source_schema`** *(str | None, optional)*: The database schema of the source table or view.
- **`can_create_table`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target table if it does not exist.
- **`can_create_columns`** *(bool, optional)*: Defaults to `True`. Allows the module to append missing columns to the target table.
- **`can_create_schemas`** *(bool, optional)*: Defaults to `True`. Allows the module to automatically create the target schema in the database if it does not exist.

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
- **`commit_all_steps`** *(bool, optional)*: Defaults to `True`. If `True`, every step (temp insert, target insert, update, delete) is committed immediately. If `False`, a single commit is issued after all steps complete successfully.
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

The executed SQL statements are exposed only on the `dbmerge` instance (not in `mergeResult`):

- **`insert_sql`**: The exact SQL `INSERT` statement executed against the database.
- **`update_sql`**: The exact SQL `UPDATE` statement executed against the database.
- **`delete_sql`**: The exact SQL `DELETE` (or mark) statement executed against the database.
