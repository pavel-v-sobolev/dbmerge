DBMerge is a python library for automatic merge of table data to an SQL database with insert/update/delete(or mark).

# Introduction

This library is designed as a simple interface for Insert/Update/Delete operation with SQL database table.
Merge is done with optimal speed via putting your data first to temporary table and then doing data modification in the target table.
This module is based on SQLAlchemy library and using its abstraction layer to support multiple database engines.
DBMerge requires a non-null unique key (preferable primary key) to compare data and deside which operation is required.

It was tested with PostgreSQL, MariaDB and SQLite.


# Main features:
- Insert rows new, which dont exist in the target table.
- Update rows, which exist in target table, but have different values. It is skipping update of rows which dont change the values.
- It has various options for deletion of rows, which were not found in the source table, but exist in the target table. (Either keep, delete or mark with special boolean flag field).
- It can handle data filtering in deletion part. E.g. handle subsets or period in the source data to check and delete/mark only inside this scope.
- It handles creation of table or table columns, if they dont exist in the database.
- It can use different data inputs: pandas DataFrame, list of dict or other database table or view.
- It can put merged_on timestamp and inserted_on timestamp to indicate when your data was modified or first inserted.


# Installation
```
pip install dbmerge
```

# Usage
```python

from sqlalchemy import create_engine, String
from datetime import date
from dbmerge import dbmerge

engine = create_engine("""postgresql+psycopg2://postgres:@localhost:5432/dbmerge""")

data=[# some data for 2025-01
      {'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
      {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50},
      {'Shop':'125','Product':'124','Date':date(2025,1,1),'Qty':1,'Price':120.20},
      # some data for 2025-02
      {'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'124','Product':'123','Date':date(2025,2,1),'Qty':1,'Price':110.50},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':1,'Price':90.20}]

# key is required if your table does not exist in the database. Otherwise module will read this info from the database.
# data_types will be needed for creating table in mariadb, because it requires setting string length.
key = ['Shop','Product','Date']
data_types = {'Shop':String(100),'Product':String(100)}

# Object is created using the with expression (context) to make sure that all resources are freed and 
# connection to db is closed when exiting the with block.
with dbmerge(engine=engine, data=data, table_name="Facts", 
                  key=key, data_types=data_types) as merge:
    merge.exec()

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 6 rows (3ms), 
# Inserted: 6 rows (5ms), Updated: 0 rows (6ms), Deleted: no, Total time: 13ms


# Now lets assume you want to update data in 2025-02, including deletion.
data=[{'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':3,'Price':90.20}]

# Pass the delete_condition as SQLAlchemy logical expression, 
# to delete missing data in 2025-02.
# (If you dont pass it, then whole target table will be checked vs your data for missing rows.)
with dbmerge(engine=engine, data=data, table_name="Facts", 
             delete_mode='delete') as merge:
    # Use the table attribute to access our target table as SQLAlchemy object.
    merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,2,1),date(2025,2,28)))

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 2 rows (3ms), 
# Inserted: 0 rows (5ms), Updated: 1 rows (5ms), Deleted: 1 rows (5ms), Total time: 19ms


```

# Documentation
[Module Documentation](https://github.com/pavel-v-sobolev/dbmerge/blob/main/DOCUMENTATION.md)

# More examples
[Examples Python File](https://github.com/pavel-v-sobolev/dbmerge/blob/main/user_guide.py)


# Database specific details:
- In PostgreSQL only JSONB type is supported by this library, not JSON. The reason is that for JSON not possible to compare and check if something was changed.
- MariaDB / MySQL is not detecting changes in uppercase and space padding. E.g. 'test' = ' Test' will be true. If it is important for your project, you need to change collation settings in your database.
- For MariaDB / MySQL schema is same as database, but still schema settings are supported by this library.
- MariaDB / MySQL does not allow strings with unlimited size. You need to set data_types, if you want to create a table automatically. E.g. data_types = {'Your Field':String(100)}
- SQLite does not support schema, so if schema setting provided, they are automatically reset to None with warning.
