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
    merge.exec()
```

#### Arguments

- **`engine`** *(sqlalchemy.engine.Engine)*: The SQLAlchemy engine connected to your database. Tested with PostgreSQL, MariaDB/MySQL, SQLite, and MS SQL.
- **`table_name`** *(str)*: The name of the target table where data will be merged.
- **`data`** *(list[dict] | pd.DataFrame | None, optional)*: The source data to merge. Accepts a list of dictionaries (e.g., `[{'col1': 'val1'}, ...]`) or a Pandas DataFrame.
- **`delete_mode`** *(Literal['no', 'delete', 'mark'], optional)*: Defines how to handle records that exist in the target table but are missing from the source data. 
  - `'no'` (default): Retain existing target rows (do nothing).
  - `'delete'`: Hard delete rows from the target table.
  - `'mark'`: Soft delete rows by setting a boolean/integer flag in `delete_mark_field`.
- **`delete_mark_field`** *(str, optional)*: The column name used to flag a record as deleted (must be boolean or integer). Set to `True` or `1` when `delete_mode='mark'`.
- **`merged_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically updated to the current datetime whenever a row is inserted, updated, or marked as deleted.
- **`inserted_on_field`** *(str | None, optional)*: The name of a timestamp column. Automatically set to the current datetime when a new row is initially inserted. It is ignored during updates or deletions.
- **`skip_update_fields`** *(list, optional)*: A list of column names to exclude from the `UPDATE` operation. These fields will only be written during the initial `INSERT`.
- **`key`** *(list | None, optional)*: A list of column names serving as the unique key to compare source and target tables. If omitted, the module attempts to use the target table's Primary Key. *Note: If the table does not exist yet, this parameter is required to create the Primary Key.*
- **`data_types`** *(dict[str, types.TypeEngine] | None, optional)*: A dictionary mapping column names to SQLAlchemy data types (e.g., `{'Name': String(100)}`). Used when creating missing tables or columns. If omitted, data types are auto-detected from the source data.
- **`schema`** *(str | None, optional)*: The database schema of the target table. Defaults to `None` (uses the database default schema, e.g., `public` in PostgreSQL). Ignored by SQLite.
- **`temp_schema`** *(str | None, optional)*: The schema where the temporary staging table will be created.
- **`source_table_name`** *(str | None, optional)*: If provided, data will be sourced directly from another existing database table or view instead of Python memory (`data` parameter is ignored).
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
2. **Insert:** Copies rows from the temporary table to the target table that do not currently exist.
3. **Update:** Updates existing rows in the target table where field values differ from the temporary table.
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

#### Arguments
- **`delete_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression used in the `WHERE` clause during the delete/mark phase. Essential for chunked or partitioned data syncs.
- **`source_condition`** *(ColumnElement, optional)*: An SQLAlchemy binary expression used to filter the `SELECT` statement when loading data from a `source_table_name`.
- **`commit_all_steps`** *(bool, optional)*: Defaults to `True`. If `True`, every step (temp insert, target insert, update, delete) is committed immediately. If `False`, a single commit is issued after all steps complete successfully.
- **`chunk_size`** *(int, optional)*: Defaults to `10000`. Defines the batch size when inserting raw data (from Lists or Pandas DataFrames) into the temporary table to avoid memory/query-size limits.

#### Execution Results & Statistics
After `exec()` completes, the returned result object (or the `dbmerge` instance) exposes the following statistical attributes:

- **`count_data`**: Total number of rows processed from the source data.
- **`inserted_row_count`**: Number of new rows inserted into the target table.
- **`updated_row_count`**: Number of existing rows successfully updated.
- **`deleted_row_count`**: Number of rows deleted (or flagged as deleted).
- **`total_time`**: Total execution time (in seconds) for the entire database operation.
- **`data_insert_time`**: Time taken (in seconds) to load data into the temporary table.
- **`insert_time`**: Time taken (in seconds) to perform the target `INSERT` step.
- **`update_time`**: Time taken (in seconds) to perform the target `UPDATE` step.
- **`delete_time`**: Time taken (in seconds) to perform the `DELETE` or `MARK` step.
- **`insert_sql`**: The exact SQL `INSERT` statement executed against the database.
- **`update_sql`**: The exact SQL `UPDATE` statement executed against the database.
- **`delete_sql`**: The exact SQL `DELETE` (or mark) statement executed against the database.
