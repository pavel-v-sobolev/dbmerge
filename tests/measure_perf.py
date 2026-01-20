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
           'mssql':create_engine(f"mssql+pyodbc:///?odbc_connect={mssql_settings}",connect_args={"autocommit": False,"fast_executemany": True})
         }





def clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_source',schema='source')


def measure_performance(engine_name,):
    logger.debug(f'MEASURE PERFORMANCE {engine_name}')
    engine = engines[engine_name]
    key = ['Shop','Product','Date']
    data_types = {'Shop':String(100),'Product':String(100)}
    clean_data(engine)

    logger.debug('Create source table')
    data = get_data(limit = 500000)
    
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  inserted_on_field='Inserted On',merged_on_field='Merged On',key=key, data_types=data_types) as merge:
        merge.exec()

    logger.debug('Now modify some date and load to Facts table')
    data = get_modified_data(limit = 1000000)

    #data = data.fillna(0)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                 delete_mode='delete', inserted_on_field='Inserted On',merged_on_field='Merged On',
                 key=key, data_types=data_types) as merge:
        merge.exec()
        total_time = merge.total_time
    
    return total_time


if __name__ == '__main__':

    result = {}
    for engine_name in ['postgres']:
        total_time = measure_performance(engine_name)
        result[engine_name] = round(total_time,2)

logger.info(str(result))

