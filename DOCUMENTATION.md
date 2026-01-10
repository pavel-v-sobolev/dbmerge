# DBMerge python module documentation

# dbmerge class 
## Init 
Init function performs preparation steps before merge.
- Check that target table is existing and create table if it does not exist.
- Check existing table fields and create missing fields according to given or detected data types.
- To make effecient merge the module creates a temporary table, which will be used in exec() method.

Preferable way to do this is to use context:
E.g.:
    with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
        merge.exec()


## Parameters

- **engine** (Engine) - Database sqlalchemy engine. Module was tested with postgres, mariadb, sqlite. It should work with most of DBs, which support insert, update from select syntax and delete with where not exists syntax.
- **table_name** (str) - Target table name. This is where the data is merged.
- **data** (list[dict] | pd.DataFrame | None, optional): Data to merge into the table. It can be list of dict e.g. [{'column1':'value1','column2':'value2'},] or a pandas DataFrame.
- **missing_mode** (Literal['keep', 'delete', 'mark'], optional) - Defines how to handle values, which exist in target table, but does not exist in data or source table.
    - keep - do nothing (default)
    - delete - delete missing rows from target table
    - mark - set deletion mark to True or 1 in the missing_mark_field.
- **missing_mark_field** (str, optional): Field used for setting deletion status for record. The field should be boolean or integer. When record is missing in the data or source table, it is set to True or 1.
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

