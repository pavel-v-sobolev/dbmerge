import pandas as pd
import polars as pl
import numpy as np
from datetime import date
import time
import uuid
import pytest
import logging

from sqlalchemy import create_engine, text, select, schema, func
from sqlalchemy import Table, MetaData, Column, String, Date, Integer, Numeric, JSON, Uuid, StaticPool

from sample_data_in_sqlite import get_data, get_modified_data
import urllib
from dbmerge import dbmerge, drop_table_if_exists, format_ms

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

mssql_settings = urllib.parse.quote_plus(
                                        "DRIVER={ODBC Driver 18 for SQL Server};"
                                        "SERVER=localhost;"
                                        "DATABASE=master;"
                                        "UID=sa;"
                                        "PWD=MSSQL.test_pass;"
                                        "Encrypt=yes;"
                                        "TrustServerCertificate=yes;"
                                        )

engines = {'sqlite':"""sqlite:///data/data.sqlite""",
           'postgres':"""postgresql+psycopg2://postgres:postgres@localhost:5432/dbmerge""",
           'mariadb':"""mariadb+mariadbconnector://root:root@127.0.0.1:3306""",
           'mssql':f"mssql+pyodbc:///?odbc_connect={mssql_settings}",
           'cockroachdb':f"cockroachdb://root@localhost:26257/defaultdb?sslmode=disable",
           #'duckdb':'duckdb:///:memory:',
           #'duckdb':'duckdb:///data/data.ddb'
           #'oracle':"oracle+oracledb://system:oracle@localhost/?service_name=XEPDB1"
         }





key = ['Shop','Product','Date']
data_types = {'Shop':String(100),'Product':String(100)}


def _reflect_facts(engine):
    # sqlite does not support schemas - dbmerge stores the table without one there.
    schema = None if engine.dialect.name=='sqlite' else 'target'
    return Table('Facts', MetaData(), autoload_with=engine, schema=schema)


def count_all_rows(engine, date_from=None, date_to=None):
    """Count rows in target.Facts, optionally restricted to a date range."""
    tbl = _reflect_facts(engine)
    stmt = select(func.count()).select_from(tbl)
    if date_from is not None and date_to is not None:
        stmt = stmt.where(tbl.c['Date'].between(date_from, date_to))
    with engine.connect() as conn:
        return conn.execute(stmt).scalar()


def count_deleted_rows(engine, date_from=None, date_to=None):
    """Count rows whose 'Deleted' flag is set (True/1), optionally within a date range."""
    tbl = _reflect_facts(engine)
    stmt = select(func.count()).select_from(tbl).where(tbl.c['Deleted'] == True)
    if date_from is not None and date_to is not None:
        stmt = stmt.where(tbl.c['Date'].between(date_from, date_to))
    with engine.connect() as conn:
        return conn.execute(stmt).scalar()


def prepare_and_clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_source',schema='source')
    drop_table_if_exists(engine,'Fact1Fact2Fact3Fact4Fact5Fact6Fact7Fact8Fact9Fact10Fact11Fact12',schema='target')


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data) 
                                                     for engine_name in engines 
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_table_create_from_data_with_various_types(engine_name,type_of_data):
    logger.debug(f'TEST TABLE CREATE FROM DATA WITH VARIOUS TYPES {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None,'Price':1.1,'Data':{'a':1},'uuid':uuid.uuid4()},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':None,'Data':{'b':[1,2]},'uuid':uuid.uuid4()},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':1.2,'Data':{'c':[]},'uuid':uuid.uuid4()}]
    data_types = {'Shop':String(100),'Product':String(100),'uuid':Uuid()}

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data) 
                                                     for engine_name in engines 
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_table_with_long_name(engine_name,type_of_data):
    logger.debug(f'TEST TABLE CREATE FROM DATA WITH VARIOUS TYPES {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':2}]
    
    data_types = {'Shop':String(100),'Product':String(100),'Qty':Integer()}

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Fact1Fact2Fact3Fact4Fact5Fact6Fact7Fact8Fact9Fact10Fact11Fact12", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'



@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_empty_data_updates(engine_name,type_of_data):
    logger.debug(f'TEST TABLE CREATE FROM DATA WITH VARIOUS TYPES {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)


    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':120.20}]

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec()

    if type_of_data=='pandas':
        data = pd.DataFrame({'Shop':[],'Product':[],'Date':[]})
    elif type_of_data=='polars':
        data = pl.DataFrame({'Shop':[],'Product':[],'Date':[]})
    elif type_of_data=='dict of list':
        data = {'Shop':[],'Product':[],'Date':[]}
    else:
        data = []

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',delete_mode='delete') as merge:
        merge.exec()
        assert merge.deleted_row_count==3, f'Incorrect row count from delete {merge.deleted_row_count}, should be 3'


    with dbmerge(engine=engine, table_name="Facts_empty", schema='target', temp_schema='tmp',delete_mode='delete',
                 source_table_name = 'Facts', source_schema = 'target', key=key) as merge:
        merge.exec()
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.deleted_row_count}, should be 0'    

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_case_sensitive_and_spaces(engine_name,type_of_data):
    logger.debug(f'TEST CASE SENSITIVE AND SPACES {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data_types = {'Shop':String(100),'Product':String(100),'Test Field':String(100)}

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10,'Test Field':'test'},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50,'Test Field':'test'},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':120.20,'Test Field':'test'}]

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec()

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10,'Test Field':'test'},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50,'Test Field':'Test'},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':120.20,'Test Field':' tEst'}]

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec()


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_table_only_key_no_other_fields(engine_name,type_of_data):
    logger.debug(f'TEST ONLY KEY NO OTHER FIELDS {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data=[{'Shop':'123 ','Product':'123','Date':date(2025,1,1)},
        {'Shop':'124','Product':' 1223','Date':date(2025,1,1)}]

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts",key=key, schema='target', temp_schema='tmp',
                  delete_mode='delete', data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==2, f'Incorrect row count from insert {merge.inserted_row_count}, should be 2'

    data=[{'Shop':'123 ','Product':'123','Date':date(2025,1,1)}]
    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)
    with dbmerge(engine=engine, data=data, table_name="Facts",key=key, schema='target', temp_schema='tmp',
                  delete_mode='delete') as merge:
        merge.exec()
        assert merge.deleted_row_count==1, f'Incorrect row count from delete {merge.deleted_row_count}, should be 1'

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_insert_to_existing_table_and_test_new_field(engine_name,type_of_data):
    logger.debug(f'TEST INSERT TO EXISTING TABLE AND TEST NEW FIELD {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    logger.debug('Create table from first merge')
    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None,'Price':1.1},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':None},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':1.2}]

    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'

    data = get_data(limit=10000)
    data['Test Field']=1
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  merged_on_field='Merged On',inserted_on_field='Inserted On') as merge:
        merge.exec(chunk_size = 10000)
        assert merge.inserted_row_count==10000, f'Incorrect row count from insert {merge.inserted_row_count}, should be 10000'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be =0'

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_change_data_and_mark_deleted_data(engine_name,type_of_data):
    logger.debug(f"TEST CHANGE DATA AND DELETE DATA with delete_mode='mark' {engine_name} {type_of_data}")
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data = get_data(limit=10001)
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec(chunk_size = 10000)
        assert merge.inserted_row_count==10001, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==10001'

    data = get_modified_data(limit=10000)
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='mark',merged_on_field='Merged On',inserted_on_field='Inserted On',
                  delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_date_range_with_deletion(engine_name,type_of_data):
    logger.debug(f'TEST DATE RANGE WITH DELETION {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data = get_data(start_date=date(2025,1,1),end_date=date(2025,7,10))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(engine=engine, data=data,  table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)
    
    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='delete') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'
        

@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_date_range_with_delete_mark(engine_name,type_of_data):
    logger.debug(f'TEST DATE RANGE WITH MISSING MARK {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data = get_data(start_date=date(2025,1,1),end_date=date(2025,7,10))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key, delete_mark_field='Deleted') as merge:
        merge.exec()
        total_rows = merge.inserted_row_count

    # freshly inserted rows must default to active (Deleted=False), never NULL or True
    assert count_all_rows(engine)==total_rows, f'Expected {total_rows} rows in the table'
    assert count_deleted_rows(engine)==0, 'Inserted rows must default to Deleted=False, none should be marked deleted'

    data = get_modified_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='mark',delete_mark_field='Deleted') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'
        deleted_count = merge.deleted_row_count

    # the flag must actually be persisted as True, and scoped to the delete_condition range only
    assert count_all_rows(engine)==total_rows, 'Mark mode must not insert or physically delete rows'
    assert count_deleted_rows(engine)==deleted_count, 'Number of rows marked Deleted=True must match deleted_row_count'
    assert count_deleted_rows(engine,date(2025,3,1),date(2025,4,15))==deleted_count, 'All marked rows must fall inside the delete_condition range'
    assert count_deleted_rows(engine,date(2025,1,1),date(2025,2,28))==0, 'Rows outside the delete_condition range must stay active'

    logger.debug('Now test how missing mark is recovered')
    data = get_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)
    
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='mark',delete_mark_field='Deleted') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>=deleted_count,\
            f'Incorrect row count from update {merge.updated_row_count}, should be >={deleted_count}'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be ==0'

    # recovered (reappeared) rows must be reset back to active (Deleted=False)
    assert count_deleted_rows(engine,date(2025,3,1),date(2025,4,15))==0, 'Recovered rows must be reset to Deleted=False'
    assert count_deleted_rows(engine)==0, 'No row should remain marked deleted after full recovery'
    assert count_all_rows(engine)==total_rows, 'Recovery must not insert or physically delete rows'


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_delete_mark_field_with_delete_mode_no(engine_name,type_of_data):
    logger.debug(f"TEST delete_mark_field is always populated with delete_mode='no' {engine_name} {type_of_data}")
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    # Part A: 'Deleted' is NOT in the data -> every row must default to False
    data=[{'Shop':'1','Product':'A','Date':date(2025,1,1),'Qty':10},
          {'Shop':'1','Product':'B','Date':date(2025,1,1),'Qty':20},
          {'Shop':'2','Product':'A','Date':date(2025,1,1),'Qty':30}]
    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types, delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'

    assert count_all_rows(engine)==3
    assert count_deleted_rows(engine)==0, "delete_mode='no': rows without 'Deleted' in data must default to False, not NULL/True"

    # Part B: 'Deleted' IS in the data -> the supplied value must be stored as-is
    data=[{'Shop':'1','Product':'A','Date':date(2025,1,1),'Qty':10,'Deleted':True},
          {'Shop':'1','Product':'B','Date':date(2025,1,1),'Qty':20,'Deleted':False},
          {'Shop':'2','Product':'A','Date':date(2025,1,1),'Qty':30,'Deleted':True}]
    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types, delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be 0'

    assert count_all_rows(engine)==3
    assert count_deleted_rows(engine)==2, "delete_mode='no': 'Deleted' supplied in data must be used as-is (2 True expected)"


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_a_set_from_temp_with_deletion(engine_name,type_of_data):
    logger.debug(f'TEST A SET FROM TEMP WITH DELETION {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    data = get_data(limit=10000)
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(shops = ['Shop16','Shop18','Shop3'], limit=10000)
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)
    
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='delete') as merge:
        merge.exec(delete_condition=merge.table.c['Shop'].in_(select(merge.temp_table.c['Shop'])))
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'

  
@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_update_from_source_table_with_delete_in_a_period(engine_name,type_of_data):
    logger.debug(f'TEST UPDATE FROM SOURCE TABLE WITH DELETE/UPDATE OF IN A SET {engine_name} {type_of_data}')
    engine = create_engine(engines[engine_name])
    prepare_and_clean_data(engine)

    logger.debug('Create source table')
    data = get_data()
    data['Test field']=1.1
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(engine=engine, data=data, table_name="Facts_source", schema='source', temp_schema='tmp',
                  inserted_on_field='Inserted On', key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count==0, f'Incorrect row count from update {merge.updated_row_count}, should be 0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'

    logger.debug('Now modify some date and load to Facts table')
    data = get_modified_data()

    data['Test field']=1.1
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = pl.from_pandas(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp', 
                  key=key, data_types=data_types,
                  delete_mode='mark',merged_on_field='Merged On',inserted_on_field='Inserted On',
                  delete_mark_field='Deleted'
                  ) as merge:
        merge.exec()
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count==0, f'Incorrect row count from update {merge.updated_row_count}, should be 0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'

    logger.debug('Now take data from source table in defined period')
    with dbmerge(engine=engine, source_table_name='Facts_source', temp_schema='tmp', 
                  source_schema='source',
                  table_name="Facts", schema='target',
                  delete_mode='delete') as merge:
        merge.exec(source_condition=merge.source_table.c['Date'].between(date(2025,1,1),date(2025,1,15)),
                   delete_condition=merge.table.c['Date'].between(date(2025,1,1),date(2025,1,15)))
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'
        


if __name__ == '__main__':

    test_date_range_with_delete_mark('cockroachdb','list of dict')
