# DBMerge Python Module Documentation

# dbmerge class 
## init method
Init function performs preparation steps before merge.
- Check that target table is existing and create table if it does not exist.
- Check existing table fields and create missing fields according to given or detected data types.
- To make effecient merge the module creates a temporary table, which will be used in exec() method.
\
Preferable way to do this is to use context:\
E.g.:

```python
with dbmerge(engine=engine, data=data, table_name="YourTable") as merge:
    merge.exec()

```

### Arguments

- **engine** (Engine): Database sqlalchemy engine. Module was tested with postgres, mariadb, sqlite. It should work with most of DBs, which support insert, update from select syntax and delete with where not exists syntax.
- **table_name** (str): Target table name. This is where the data is merged.
- **data** (list[dict] | pd.DataFrame | None, optional): Data to merge into the table. It can be list of dict e.g. [{'column1':'value1','column2':'value2'},] or a pandas DataFrame.
- **delete_mode** (Literal['no', 'delete', 'mark'], optional): Defines how to handle values, which exist in target table, but does not exist in data or source table.
    - no - do nothing (default)
    - delete - delete rows from target table, that are missing in the source data
    - mark - set deletion mark to True or 1 in the delete_mark_field.
- **delete_mark_field** (str, optional): Field used for setting deletion status for record. The field should be boolean or integer. When record is missing in the data or source table, it is set to True or 1.
- **merged_on_field** (str | None, optional): Timestamp field name which is set to current datetime when the data is inserted/updated/marked.
- **inserted_on_field** (str | None, optional): Timestamp field when the record was inserted. This field is not changed when data is updated or marked for deletion. 
- **skip_update_fields** (list, optional): List of fields, that will be excluded from update. These fields will be written only when data is inserted.
- **key** (list | None, optional): List of key fields which will be used to compare source and target tables. If key is not set, then table primary key will be used (recommended). This field will be required if the table does not exist and it will be created automatically with this primary key.                 
- **data_types** (dict[str,types.TypeEngine] | None, optional): A dictionary of data types. If the table or field is not existing in the database, then it will be used to assign a data type. If data type for new field is not given here, then the module will try to auto detect the data type.
- **schema** (str | None, optional): Database schema of target table. If it is None, then default schema will be used. E.g. public schema for postgres database is default in this case. Sqlite database does not support schamas, so this parameter will be ignored.
- **temp_schema** (str | None, optional): Database schema where temporary tabe will be created. 
- **source_table_name** (str | None, optional): If this parameter is set, then data will be taken from other database table or view.
- **source_schema** (str | None, optional): Database schema of source table or view.
- **can_create_table** (bool, optional): If True (default), then table and view will be created automatically.
- **can_create_columns** (bool, optional): If True (default), then module will create missing columns in the database table.

### Attributes
These attributes will be available after init dbmerge object is initialized.
- **table** - SQLAlchemy Table object for your target table. If it does not exist, then it will be automatically created during initialization.
- **temp_table** - SQLAlchemy Table object for temporary table. This table is created during init and will we deleted after exec method will be finished (or if leaving the with block).
- **source_table** - SQLAlchemy Table object for source table, if you are doing merge operation from source table.



## exec method
This method executes the merge operation, which contains the following steps:
1) Insert source data to temporary table.
Further steps are done base on data in the temp table to merge data to the target table (table). 
2) Insert rows, missing in the target table.
3) Update rows, which exist in target table and which have different values (fields are compared).
4) Delete or mark as deleted (update deletion mark) for the fields, which dont exist in source data

If your data comes in portions then you can set a delete_condition argument to define your portion of data.
E.g. if you load monthly data you can call the method like this:

```python
with dbmerge(data=data, engine=engine, table_name="YourTable",delete_mode='delete') as merge:
    merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,1,1),date(2025,1,31)))

```

### Arguments
- **delete_condition** (ColumnElement, optional) - If delete mode is 'delete' or 'mark', then you can set a condition to filter the target table. It should be an SQL alchemy binary exporession, which will be used in the where condition of delete or mark deleted.
- **source_condition** (ColumnElement, optional): If the data is loaded from source table or view, then you can set this parameter to use in the where() statement when selecting the data and inserting to temp table. 
- **commit_all_steps** (bool, optional): If set to True (default), then all steps will be commited (insert to temp, insert to target, update, delete). If False, then commit will be done only after all is finished.
- **chunk_size** (int, optional): When data (from list or dataframe) is inserted to temp table, it will be split in chunks. Defaults to 10000 rows per chunk.

### Attributes
These attributes will be available after exec method is finished
- **count_data** - number of rows in your data
- **inserted_row_count** - number of rows that were inserted into the target table 
- **updated_row_count** - number of rows that were updated in the target table
- **deleted_row_count** - number of rows that were deleted or marked as deleted with using delete_mark_field.
- **total_time** - total time in seconds for all database operations. insert to temp, insert to target, update, delete (or mark)
- **data_insert_time** - time in seconds for inserting data to temporary table
- **insert_time** - time in seconds for inserting data to target table
- **update_time** - time in seconds for updating data, which dont match the data in the target table
- **delete_time** - time in seconds for deleting data or marking as deleted.
- **insert_sql** - SQL statement that was issued to the database when inserting data to target table.
- **update_sql** - SQL statement that was issued to the database when updating data.
- **delete_sql** - SQL statement that was issued to the database when deleting data or marking data as deleted.

