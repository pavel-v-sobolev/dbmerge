import pandas as pd
import numpy as np
from datetime import date

# from sqlalchemy import text, select, schema
from sqlalchemy.engine import create_engine

from dbmerge import dbmerge
import logging
logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)


data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None,'Price':1.1},
    {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':None},
    {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':1.2}]

# engine = create_engine('duckdb:///data/data.ddb')
engine = create_engine('duckdb:///data/data.ddb')

with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', 
              temp_schema='tmp', key= ['Shop','Product','Date']) as merge:
    merge.exec()

with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', 
              temp_schema='tmp', key= ['Shop','Product','Date']) as merge:
    merge.exec()