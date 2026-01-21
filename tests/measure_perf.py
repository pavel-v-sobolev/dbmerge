import pandas as pd
import numpy as np
from datetime import date
import time
import uuid
import pytest
import logging

from sqlalchemy import create_engine,text,select
from sqlalchemy import Table, MetaData, Column, String, Date, Integer, Numeric, JSON, Uuid

from sample_data_in_pg import get_data, get_modified_data
import urllib
from dbmerge import dbmerge, drop_table_if_exists, format_ms

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)


mssql_settings = urllib.parse.quote_plus(
                                        "DRIVER={ODBC Driver 18 for SQL Server};"
                                        "SERVER=localhost;"
                                        "DATABASE=dbmerge;"
                                        "UID=sa;"
                                        "PWD=MSSQL.test_pass;"
                                        "Encrypt=yes;"
                                        "TrustServerCertificate=yes;"
                                        )

engines = {'sqlite':create_engine("""sqlite:///data/data.sqlite"""),
           'postgres':create_engine("""postgresql+psycopg2://postgres:postgres@localhost:5432/dbmerge"""),
           'mariadb':create_engine("""mariadb+mariadbconnector://root:root@localhost:3306"""),
           #'mssql':create_engine(f"mssql+pyodbc:///?odbc_connect={mssql_settings}",connect_args={"autocommit": False,"fast_executemany": True})
         }





def clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_source',schema='source')


def measure_performance(engine_name,size):
    logger.debug(f'MEASURE PERFORMANCE {engine_name} {size}')
    engine = engines[engine_name]
    key = ['Shop','Product','Date']
    data_types = {'Shop':String(100),'Product':String(100)}
    clean_data(engine)

    logger.debug('Create source table')
    data = get_data(limit = size/2)
    
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  inserted_on_field='Inserted On',merged_on_field='Merged On',key=key, data_types=data_types) as merge:
        merge.exec()

    logger.debug('Now modify some date and load to Facts table')
    data = get_modified_data(limit = size)

    #data = data.fillna(0)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                 delete_mode='delete', inserted_on_field='Inserted On',merged_on_field='Merged On',
                 key=key, data_types=data_types) as merge:
        result = merge.exec()
        total_time = result.total_time
        logger.info('SHARES: '
        f'inserted  {round(result.inserted_row_count/merge.total_row_count*100)}% '
        f'updated  {round(result.updated_row_count/merge.total_row_count*100)}% '
        f'deleted  {round(result.deleted_row_count/merge.total_row_count*100)}%')
    
    return total_time


if __name__ == '__main__':

    result = {}
    for engine_name in ['postgres','mariadb','sqlite']:
        total_time = measure_performance(engine_name,1000000)
        result[engine_name] = round(total_time,1)

logger.info(str(result))

# 
# size = 100000 {'postgres': 2.0, 'mariadb': 1.0, 'sqlite': 0.7, 'mssql': 22.4}
# size = 1000000 {'postgres': 19.8, 'mariadb': 11.1, 'sqlite': 7.6, 'mssql': 263.7}
# inserted  65% updated  7% deleted  15%
