from sqlalchemy import create_engine, String, select
from datetime import date
from dbmerge import dbmerge, drop_table_if_exists

engine = create_engine("""postgresql+psycopg2://postgres:postgres@localhost:5432/dbmerge""")

drop_table_if_exists(engine, "Facts")

data=[
      {'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
      {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50},
      {'Shop':'125','Product':'124','Date':date(2025,1,1),'Qty':1,'Price':120.20},
      {'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10},
      {'Shop':'124','Product':'123','Date':date(2025,2,1),'Qty':1,'Price':110.50},
      {'Shop':'125','Product':'124','Date':date(2025,2,1),'Qty':1,'Price':90.20}]

key = ['Shop','Product','Date']

data_types = {'Shop':String(100),'Product':String(100)}


with dbmerge(engine=engine, data=data, table_name="Facts", 
                  key=key, data_types=data_types, merged_on_field='Merged On') as merge:
    merge.exec()

data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10,'New Field':'a'},
      {'Shop':'123','Product':'123','Date':date(2025,2,1),'Qty':2,'Price':52.10,'New Field':'b'},
      {'Shop':'123','Product':'124','Date':date(2025,2,1),'Qty':1,'Price':80.20,'New Field':'c'},
      {'Shop':'123','Product':'125','Date':date(2025,2,1),'Qty':13,'Price':70.10,'New Field':'d'}]
with dbmerge(engine=engine, data=data, table_name="Facts", 
             delete_mode='delete', merged_on_field='Merged On') as merge:
      merge.exec(delete_condition=merge.table.c['Shop'].in_(select(merge.temp_table.c['Shop'])))

