from sqlalchemy import create_engine, String, select
from datetime import date
from dbmerge import dbmerge, drop_table_if_exists

engine = create_engine("""postgresql+psycopg2://postgres:@localhost:5432/dbmerge""")

# This is a additional routine to drop table in database (just to run a fresh test each time)
# Be careful if you use it!
drop_table_if_exists(engine, "Facts")

# Lets create some initial data
data=[# some data for 2025-01
      {'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
      {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50},
      {'Shop':'125','Product':'124','Date':date(2025,1,1),'Qty':1,'Price':120.20},
      # some data for 2025-02
      {'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'124','Product':'123','Date':date(2025,2,1),'Qty':1,'Price':110.50},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':1,'Price':90.20}]

# key is required if your table does not exist in the database.
key = ['Shop','Product','Date']
# data_types will be needed for creating table in mariadb, because it requires setting string length.
data_types = {'Shop':String(100),'Product':String(100)}

# Object is created with context to make sure that all resources are freed and connection to db is closed
with dbmerge(engine=engine, data=data, table_name="Facts", 
                  key=key, data_types=data_types, merged_on_field='Merged On') as merge:
    merge.exec()

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 6 rows (3ms), 
# Inserted: 6 rows (5ms), Updated: 0 rows (6ms), Deleted: no, Total time: 13ms


# CASE 1 - Loading monthly data
# Now lets assume you want to update data in 2025-02, including deletion.
data=[{'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':3,'Price':90.20}]

# Pass the delete_condition as SQLAlchemy logical expression, 
# to delete data only in 2025-02. 
# (If you dont pass it, then whole target table will be checked vs your data for missing rows.)
with dbmerge(engine=engine, data=data, table_name="Facts", 
             delete_mode='delete', merged_on_field='Merged On') as merge:
    # Use the table attribute to access our target table as SQLAlchemy object.
    merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,2,1),date(2025,2,28)))

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 2 rows (3ms), 
# Inserted: 0 rows (5ms), Updated: 1 rows (5ms), Deleted: 1 rows (5ms), Total time: 19ms

# CASE 2 - Loading some objects data. (e.g. shops)
# Assume you received a dataset for one or several shops and you want to check missing rows
# only for these shops.
data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
      {'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'123','Product':'124','Date':date(2025,2,1),'Qty':1,'Price':80.20},
      {'Shop':'123','Product':'125','Date':date(2025,2,1),'Qty':13,'Price':70.10}]
with dbmerge(engine=engine, data=data, table_name="Facts", 
             delete_mode='delete', merged_on_field='Merged On') as merge:
      # Lets create missing condition so that in checks that value of Shop in the target table
      # is in values in the temp_table.
      # This means, that deletion will be only done for shops, loaded in the data.
      merge.exec(delete_condition=merge.table.c['Shop'].in_(select(merge.temp_table.c['Shop'])))

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 4 rows (3ms), 
# Inserted: 2 rows (7ms), Updated: 0 rows (6ms), Deleted: 0 rows (5ms), Total time: 20ms

# CASE 3 - Load data from other table with condition
# Lets merge 2025-02 data to other table 
# This case e.g. can be useful if you have some heavy calculation source view, 
# that you want to update in persistent target table.

with dbmerge(engine=engine, table_name="Facts_latest", source_table_name='Facts',
             delete_mode='delete', key=key, data_types=data_types, merged_on_field='Merged On') as merge:
    # Select only 2025-02 data froum your source table
    # Apply missing condition to target table, just in case some data need to be deleted.
    merge.exec(source_condition=merge.source_table.c['Date'].between(date(2025,2,1),date(2025,2,28)),
               delete_condition=merge.table.c['Date'].between(date(2025,2,1),date(2025,2,28)))

# OUTPUT:
# INFO - Merged data into table "Facts_latest". Temp data: 4 rows (5ms), 
# Inserted: 4 rows (9ms), Updated: 0 rows (10ms), Deleted: 0 rows (5ms), Total time: 29ms
