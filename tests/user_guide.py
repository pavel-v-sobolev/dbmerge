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

# key and data_types are only required if your table does not exist in the database.
key = ['Shop','Product','Date']
data_types = {'Shop':String(100),'Product':String(100)}

# object is created with context to make sure that all resources are freed and connection to db is closed
with dbmerge(engine=engine, data=data, table_name="Facts", 
                  key=key, data_types=data_types) as merge:
    merge.exec()

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 3 rows (4ms), 
# Inserted: 0 rows (8ms), Updated: 0 rows (9ms), Deleted: no, Total time: 21ms

# Now lets assume you want to update data in 2025-02, including deletion.
data=[{'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':3,'Price':90.20}]

# Pass the missing_condition as SQLAlchemy logical expression, 
# to delete data only in 2025-02. 
# Use the table attribute to access our target table as SQLAlchemy object.
with dbmerge(engine=engine, data=data, table_name="Facts", 
             missing_mode='delete') as merge:
    merge.exec(missing_condition=merge.table.c['Date'].between(date(2025,2,1),date(2025,2,28)))

# OUTPUT:
# INFO - Merged data into table "Facts". Temp data: 2 rows (3ms), 
# Inserted: 0 rows (5ms), Updated: 1 rows (5ms), Deleted: 1 rows (5ms), Total time: 19ms

