import pandas as pd
import polars as pl
import numpy as np
from datetime import date, datetime
import time
import uuid
import pytest
import logging

from sqlalchemy import create_engine, text, select, schema, func, exists, and_, or_
from sqlalchemy import Table, MetaData, Column, String, Date, DateTime, Integer, Numeric, JSON, Uuid, StaticPool
from sqlalchemy import types

from sample_data_in_sqlite import get_data, get_modified_data
import urllib
from dbmerge import dbmerge, drop_table_if_exists, format_ms
# The exceptions are not re-exported from the package root.
from dbmerge.dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError

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


def make_engine(engine_name):
    # CockroachDB runs SERIALIZABLE by default. A merge step here reads the staging table and
    # updates tens of thousands of target rows, so a single step can stay open for tens of seconds -
    # long enough for an unrelated commit (e.g. the background GC of a dropped temp table) to break
    # the transaction's read-timestamp refresh and fail COMMIT with a transient RETRY_SERIALIZABLE.
    # CockroachDB expects the client to retry such transactions; dbmerge does not, so the tests ask
    # for READ COMMITTED, where CockroachDB retries statements internally.
    if engine_name == 'cockroachdb':
        return create_engine(engines[engine_name], isolation_level='READ COMMITTED')
    return create_engine(engines[engine_name])



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


def get_qty(engine, shop, product):
    """Return the 'Qty' value of a single row identified by Shop/Product."""
    tbl = _reflect_facts(engine)
    stmt = select(tbl.c['Qty']).where((tbl.c['Shop'] == shop) & (tbl.c['Product'] == product))
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
def test_update_condition(engine_name,type_of_data):
    logger.debug(f'TEST UPDATE_CONDITION {engine_name} {type_of_data}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)

    # seed three rows with distinct Qty values
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
                  key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'

    # every Qty changes, but update_condition restricts the UPDATE to Shop='1' rows only
    data=[{'Shop':'1','Product':'A','Date':date(2025,1,1),'Qty':100},
          {'Shop':'1','Product':'B','Date':date(2025,1,1),'Qty':200},
          {'Shop':'2','Product':'A','Date':date(2025,1,1),'Qty':300}]
    if type_of_data=='dict of list':
        data = {k:[d[k] for d in data] for k in data[0].keys()}
    elif type_of_data=='pandas':
        data = pd.DataFrame(data)
    elif type_of_data=='polars':
        data = pl.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types) as merge:
        merge.exec(update_condition=merge.table.c['Shop'] == '1')
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be 0'
        assert merge.updated_row_count==2, f'Incorrect row count from update {merge.updated_row_count}, should be 2 (only Shop=1)'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'

    # rows satisfying update_condition got the new values ...
    assert get_qty(engine,'1','A')==100, 'Shop=1 row must be updated'
    assert get_qty(engine,'1','B')==200, 'Shop=1 row must be updated'
    # ... while the row failing the condition keeps its original value untouched
    assert get_qty(engine,'2','A')==30, 'Shop=2 row must be left untouched by update_condition'


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_mark_resurrection_key_only_table(engine_name,type_of_data):
    # A key-only table (no value columns) must still reset the auto-managed delete flag back to
    # active when a previously marked-deleted row reappears in the source.
    logger.debug(f'TEST MARK RESURRECTION ON KEY-ONLY TABLE {engine_name} {type_of_data}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)

    def as_type(rows):
        if type_of_data=='dict of list':
            return {k:[d[k] for d in rows] for k in rows[0].keys()}
        elif type_of_data=='pandas':
            return pd.DataFrame(rows)
        elif type_of_data=='polars':
            return pl.DataFrame(rows)
        return rows

    both=[{'Shop':'1','Product':'A','Date':date(2025,1,1)},
          {'Shop':'1','Product':'B','Date':date(2025,1,1)}]
    with dbmerge(engine=engine, data=as_type(both), table_name="Facts", schema='target', temp_schema='tmp',
                  key=key, data_types=data_types, delete_mark_field='Deleted') as merge:
        merge.exec()
    assert count_deleted_rows(engine)==0, 'Freshly inserted rows must be active'

    # drop 'B' -> it must be marked deleted
    one=[{'Shop':'1','Product':'A','Date':date(2025,1,1)}]
    with dbmerge(engine=engine, data=as_type(one), table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='mark', delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.deleted_row_count==1, f'Expected 1 marked deleted, got {merge.deleted_row_count}'
    assert count_deleted_rows(engine)==1, "'B' must be marked deleted"

    # 'B' reappears -> the flag must be reset back to active despite there being no value columns
    with dbmerge(engine=engine, data=as_type(both), table_name="Facts", schema='target', temp_schema='tmp',
                  delete_mode='mark', delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.updated_row_count==1, f'Reappeared row must be updated (reset), got {merge.updated_row_count}'
        assert merge.deleted_row_count==0, f'Nothing should be marked deleted, got {merge.deleted_row_count}'
    assert count_deleted_rows(engine)==0, 'Resurrected row must be reset to Deleted=False'


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_a_set_from_temp_with_deletion(engine_name,type_of_data):
    logger.debug(f'TEST A SET FROM TEMP WITH DELETION {engine_name} {type_of_data}')
    engine = make_engine(engine_name)
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
    engine = make_engine(engine_name)
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
        


@pytest.mark.parametrize("engine_name", list(engines))
def test_update_condition_version_guard(engine_name):
    # update_condition restricts the update phase: a row is overwritten only when it also satisfies
    # the condition. Here: overwrite only if the incoming 'version' is not lower than the stored one.
    logger.debug(f'TEST UPDATE_CONDITION_VERSION_GUARD {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    seed = [{'Shop': 'S', 'Product': 'A', 'Qty': 1, 'version': 5}]
    with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k) as merge:
        merge.exec()

    def conditional_merge(qty, version):
        data = [{'Shop': 'S', 'Product': 'A', 'Qty': qty, 'version': version}]
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k) as merge:
            cond = or_(merge.table.c['version'].is_(None),
                       merge.temp_table.c['version'] >= merge.table.c['version'])
            merge.exec(update_condition=cond)
            return merge.updated_row_count

    # incoming version lower than stored -> update_condition fails -> row left untouched
    assert conditional_merge(qty=99, version=3) == 0, 'row failing the condition must not be updated'
    assert get_qty(engine, 'S', 'A') == 1

    # incoming version not lower -> updated
    assert conditional_merge(qty=42, version=9) == 1, 'row satisfying the condition must be updated'
    assert get_qty(engine, 'S', 'A') == 42


@pytest.mark.parametrize("engine_name", list(engines))
def test_insert_condition(engine_name):
    # insert_condition restricts the insert phase. With a correlated EXISTS it can inspect other
    # target rows: here a missing row is inserted only if its group ('Shop') has no row with a
    # greater 'version'. Combined with matching delete/update conditions, a group that already has a
    # greater version is left fully untouched, while the rest is refreshed from the source.
    logger.debug(f'TEST INSERT_CONDITION {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    seed = [{'Shop': 'A', 'Product': '1', 'Qty': 10, 'version': 5},   # group A at version 5
            {'Shop': 'A', 'Product': '2', 'Qty': 20, 'version': 5},
            {'Shop': 'B', 'Product': '1', 'Qty': 30, 'version': 1}]   # group B at version 1
    with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k) as merge:
        merge.exec()

    threshold = 3
    incoming = [{'Shop': 'A', 'Product': '1', 'Qty': 10, 'version': threshold},
                {'Shop': 'A', 'Product': '3', 'Qty': 999, 'version': threshold},  # new row in group A (version 5 > 3)
                {'Shop': 'B', 'Product': '1', 'Qty': 31, 'version': threshold}]   # group B (version 1 <= 3)
    with dbmerge(engine=engine, data=incoming, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k, delete_mode='delete') as merge:
        g = merge.table.alias()
        upd = or_(merge.table.c['version'].is_(None),
                  merge.temp_table.c['version'] >= merge.table.c['version'])
        # delete only within groups present in the source AND not above the threshold
        dele = and_(merge.table.c['Shop'].in_(select(merge.temp_table.c['Shop'])),
                    merge.table.c['version'] <= threshold)
        # don't insert into a group that already has a greater version
        ins = ~exists().where(and_(g.c['Shop'] == merge.temp_table.c['Shop'],
                                   g.c['version'] > threshold))
        merge.exec(update_condition=upd, delete_condition=dele, insert_condition=ins)

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        rows = {(r.Shop, r.Product): (r.Qty, r.version) for r in conn.execute(
            select(tbl.c['Shop'], tbl.c['Product'], tbl.c['Qty'], tbl.c['version'])).all()}

    # group A has a greater version -> left untouched: new row (A,3) not inserted, existing rows kept
    assert ('A', '3') not in rows, 'insert_condition must block inserting into a group with a greater version'
    assert rows[('A', '1')] == (10, 5)
    assert rows[('A', '2')] == (20, 5), 'row missing from source must survive (version-scoped delete_condition)'
    # group B is at/below the threshold -> refreshed from the source
    assert rows[('B', '1')] == (31, threshold)


@pytest.mark.parametrize("engine_name", list(engines))
def test_skip_compare_fields(engine_name):
    # skip_compare_fields: a column that is written on update but never causes one. Here 'version'
    # changes on every load; without the parameter every row would look modified and 'Merged On'
    # would be bumped, making unchanged rows look fresh to downstream incremental consumers.
    logger.debug(f'TEST SKIP_COMPARE_FIELDS {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    def merge_row(qty, version):
        data = [{'Shop': 'S', 'Product': 'A', 'Qty': qty, 'version': version}]
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, merged_on_field='Merged On',
                     skip_compare_fields=['version']) as merge:
            merge.exec()
            return merge.updated_row_count

    def get_row():
        tbl = _reflect_facts(engine)
        with engine.connect() as conn:
            return conn.execute(select(tbl.c['Qty'], tbl.c['version'], tbl.c['Merged On'])
                                .where(tbl.c['Shop'] == 'S')).one()._mapping

    merge_row(qty=1, version=5)
    seeded = get_row()
    assert seeded['version'] == 5, 'insert writes the skipped column as usual'

    # same data, new version only -> not a change: row untouched, 'Merged On' not bumped
    assert merge_row(qty=1, version=6) == 0, 'a difference only in skip_compare_fields must not update'
    quiet = get_row()
    assert quiet['version'] == 5, 'the skipped column is not written when nothing else changed'
    assert quiet['Merged On'] == seeded['Merged On'], 'merged_on must not be bumped by a no-op load'

    # real change -> row updated and the skipped column is written along with it
    time.sleep(1)   # some engines keep merged_on at second resolution
    assert merge_row(qty=42, version=7) == 1, 'a real difference must still update'
    changed = get_row()
    assert changed['Qty'] == 42
    assert changed['version'] == 7, 'the skipped column is written when the row is updated'
    assert changed['Merged On'] > seeded['Merged On'], 'merged_on is bumped by a real change'


@pytest.mark.parametrize("engine_name", list(engines))
def test_delete_mark_values(engine_name):
    # delete_mark_values: extra columns stamped on a row when it is marked as deleted, so a row
    # leaving the source carries the same bookkeeping ('version') as a row that was updated.
    logger.debug(f'TEST DELETE_MARK_VALUES {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    seed = [{'Shop': 'S', 'Product': 'A', 'Qty': 1, 'version': 5},
            {'Shop': 'S', 'Product': 'B', 'Qty': 2, 'version': 5}]
    with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k, merged_on_field='Merged On',
                 delete_mark_field='Deleted') as merge:
        merge.exec()

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        seeded_merged_on = conn.execute(select(tbl.c['Merged On'])
                                        .where(tbl.c['Product'] == 'B')).scalar()

    # product B is gone from the source -> marked, and stamped with the new version
    time.sleep(1)   # some engines keep merged_on at second resolution
    with dbmerge(engine=engine, data=[{'Shop': 'S', 'Product': 'A', 'Qty': 1, 'version': 9}],
                 table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k, merged_on_field='Merged On',
                 delete_mode='mark', delete_mark_field='Deleted',
                 delete_mark_values={'version': 9}) as merge:
        merge.exec()

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        rows = {r.Product: r._mapping for r in conn.execute(
            select(tbl.c['Product'], tbl.c['Deleted'], tbl.c['version'], tbl.c['Merged On'])).all()}

    assert rows['B']['Deleted'], 'row missing from the source must be marked'
    assert rows['B']['version'] == 9, 'delete_mark_values must be stamped on the marked row'
    assert rows['B']['Merged On'] > seeded_merged_on, 'marking bumps merged_on'

    # delete_mark_values outside 'mark' mode is a mistake, not a silent no-op
    with pytest.raises(IncorrectParameter):
        with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, delete_mark_field='Deleted',
                     delete_mark_values={'version': 1}) as merge:
            merge.exec()

    # unknown column is a mistake too
    with pytest.raises(IncorrectParameter):
        with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, delete_mode='mark', delete_mark_field='Deleted',
                     delete_mark_values={'NoSuchColumn': 1}) as merge:
            merge.exec()

    # a key column identifies the row being marked, so stamping it would rewrite that identity
    with pytest.raises(IncorrectParameter):
        with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, delete_mode='mark', delete_mark_field='Deleted',
                     delete_mark_values={'Shop': 'X'}) as merge:
            merge.exec()

    # an automatically managed column would get two values in the same UPDATE
    with pytest.raises(IncorrectParameter):
        with dbmerge(engine=engine, data=seed, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, delete_mode='mark', delete_mark_field='Deleted',
                     merged_on_field='Merged On',
                     delete_mark_values={'Merged On': None}) as merge:
            merge.exec()


@pytest.mark.parametrize("engine_name", list(engines))
def test_managed_field_roles_must_be_distinct(engine_name):
    # delete_mark_field, merged_on_field and inserted_on_field are each written by a rule of their
    # own, so one column can not play two of these roles: the merge would have to give it two
    # different values in a single statement.
    logger.debug(f'TEST MANAGED FIELD ROLES MUST BE DISTINCT {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    data = [{'Shop': 'S', 'Product': 'A', 'Qty': 1}]
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    for clashing in ({'merged_on_field': 'Stamp', 'inserted_on_field': 'Stamp'},
                     {'delete_mode': 'mark', 'delete_mark_field': 'Stamp', 'merged_on_field': 'Stamp'},
                     {'delete_mode': 'mark', 'delete_mark_field': 'Stamp', 'inserted_on_field': 'Stamp'}):
        with pytest.raises(IncorrectParameter):
            dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                    data_types=dt, key=k, **clashing)


@pytest.mark.parametrize("engine_name", list(engines))
def test_data_types_for_managed_field(engine_name):
    # A type given for an automatically managed column is used to create it, and must not turn that
    # column into a data field: its value comes from the merge itself, not from the source row.
    logger.debug(f'TEST DATA TYPES FOR MANAGED FIELD {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    data = [{'Shop': 'S', 'Product': 'A', 'Qty': 1}]
    dt = {'Shop': String(100), 'Product': String(100),
          'Merged On': DateTime(), 'Inserted On': DateTime(), 'Deleted': Integer()}

    with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k, merged_on_field='Merged On',
                 inserted_on_field='Inserted On',
                 delete_mode='mark', delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count == 1, 'the row must be inserted, not rejected'

    tbl = _reflect_facts(engine)
    for col in ('Merged On', 'Inserted On', 'Deleted'):
        assert col in tbl.c, f'managed column "{col}" was not created from data_types'

    with engine.connect() as conn:
        row = conn.execute(select(tbl.c['Merged On'], tbl.c['Inserted On'], tbl.c['Deleted'])).one()
    assert row[0] is not None and row[1] is not None, 'the merge must fill the audit timestamps'
    assert not row[2], 'a freshly inserted row must not be marked as deleted'


@pytest.mark.parametrize("engine_name", list(engines))
def test_delete_mark_field_can_not_be_in_key(engine_name):
    # The mark phase would compile to "UPDATE ... SET <key column> = <deleted value>", overwriting
    # the key of every row missing from the source.
    logger.debug(f'TEST DELETE MARK FIELD CAN NOT BE IN KEY {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)

    with pytest.raises(NoKeyError):
        dbmerge(engine=engine, data=[{'Shop': 'S', 'Product': 'A', 'Qty': 1}], table_name='Facts',
                schema='target', temp_schema='tmp', key=['Shop', 'Product'],
                data_types={'Shop': String(100), 'Product': String(100)},
                delete_mode='mark', delete_mark_field='Shop')


@pytest.mark.parametrize("engine_name", list(engines))
def test_parameter_conflicts_raise_domain_errors(engine_name):
    # A wrong combination of arguments has to surface as a dbmerge error, not as a raw TypeError or
    # as a silently wrong setting.
    logger.debug(f'TEST PARAMETER CONFLICTS {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}
    data = [{'Shop': 'S', 'Product': 'A', 'Qty': 1}]

    # a schema for the source table says nothing without the source table itself
    with pytest.raises(IncorrectParameter):
        dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                data_types=dt, key=k, source_schema='source')

    # a non-empty string is truthy, so commit_all_steps='false' would switch step commits ON
    with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types=dt, key=k) as merge:
        with pytest.raises(IncorrectParameter):
            merge.exec(commit_all_steps='false')


@pytest.mark.parametrize("engine_name", list(engines))
def test_auto_types_keep_fraction_and_microseconds(engine_name):
    # A column created automatically must keep the value it was created from. The generic types are
    # resolved differently per engine, and some of them round unless the precision is stated.
    logger.debug(f'TEST AUTO TYPES KEEP PRECISION {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    stamp = datetime(2026, 8, 2, 12, 34, 56, 987654)
    data = [{'Shop': 'S', 'Product': 'A', 'Price': 1.75, 'Stamp': stamp}]

    with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                 data_types={'Shop': String(100), 'Product': String(100)},
                 key=['Shop', 'Product']) as merge:
        merge.exec()

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        row = conn.execute(select(tbl.c['Price'], tbl.c['Stamp'])).one()

    assert float(row[0]) == 1.75, f'the fraction must survive the round trip, got {row[0]!r}'
    assert row[1].microsecond == 987654, f'microseconds must survive the round trip, got {row[1]!r}'


@pytest.mark.parametrize("engine_name", list(engines))
def test_auto_string_length_policy(engine_name):
    # A string column created automatically must accept a value longer than any length the module
    # could have guessed. On MySQL/MariaDB a string key still needs an explicit length, because
    # InnoDB shares one index budget between all columns of the key.
    logger.debug(f'TEST AUTO STRING LENGTH POLICY {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)

    if engine.dialect.name in ('mysql', 'mariadb'):
        with pytest.raises(IncorrectParameter):
            dbmerge(engine=engine, data=[{'Shop': 'S', 'Qty': 1}], table_name='Facts',
                    schema='target', temp_schema='tmp', key=['Shop'])

    long_note = 'x' * 5000
    with dbmerge(engine=engine, data=[{'Shop': 'S', 'Product': 'A', 'Note': long_note}],
                 table_name='Facts', schema='target', temp_schema='tmp',
                 data_types={'Shop': String(100), 'Product': String(100)},
                 key=['Shop', 'Product']) as merge:
        merge.exec()

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        stored = conn.execute(select(tbl.c['Note'])).scalar()
    assert len(stored) == len(long_note), f'the string was truncated to {len(stored)} characters'


@pytest.mark.parametrize("engine_name", list(engines))
def test_skip_compare_fields_covering_every_column(engine_name):
    # When every comparable column is skipped, no row can qualify as changed and nothing is written.
    logger.debug(f'TEST SKIP COMPARE FIELDS COVERING EVERY COLUMN {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    def merge_row(version):
        with dbmerge(engine=engine, data=[{'Shop': 'S', 'Product': 'A', 'version': version}],
                     table_name='Facts', schema='target', temp_schema='tmp',
                     data_types=dt, key=k, skip_compare_fields=['version']) as merge:
            merge.exec()
            return merge.updated_row_count

    merge_row(5)
    assert merge_row(6) == 0, 'nothing is comparable, so no row may be updated'

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        assert conn.execute(select(tbl.c['version'])).scalar() == 5, 'the skipped column stays as inserted'


@pytest.mark.parametrize("engine_name", list(engines))
def test_skip_compare_fields_still_resurrects_marked_row(engine_name):
    # The delete flag is reset inside the update phase. Even when every comparable column is
    # skipped, a row that reappears in the source must come back to active.
    logger.debug(f'TEST SKIP COMPARE FIELDS RESURRECTION {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    def merge_rows(rows, version):
        data = [{'Shop': 'S', 'Product': p, 'version': version} for p in rows]
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target',
                     temp_schema='tmp', data_types=dt, key=k, skip_compare_fields=['version'],
                     delete_mode='mark', delete_mark_field='Deleted') as merge:
            merge.exec()

    def deleted_flags():
        tbl = _reflect_facts(engine)
        with engine.connect() as conn:
            return {r.Product: r.Deleted for r in
                    conn.execute(select(tbl.c['Product'], tbl.c['Deleted'])).all()}

    merge_rows(['A', 'B'], 5)
    merge_rows(['A'], 6)
    assert deleted_flags()['B'], 'a row missing from the source must be marked'

    merge_rows(['A', 'B'], 7)
    assert not deleted_flags()['B'], 'a row back in the source must be reset to active'


@pytest.mark.parametrize("engine_name", list(engines))
def test_skip_update_wins_over_skip_compare(engine_name):
    # A column named in both lists follows skip_update_fields: it is written by the insert and
    # never touched again, even when the row is updated for another reason.
    logger.debug(f'TEST SKIP UPDATE WINS OVER SKIP COMPARE {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100)}

    def merge_row(qty, note):
        with dbmerge(engine=engine, data=[{'Shop': 'S', 'Product': 'A', 'Qty': qty, 'Note': note}],
                     table_name='Facts', schema='target', temp_schema='tmp', data_types=dt, key=k,
                     skip_update_fields=['Note'], skip_compare_fields=['Note']) as merge:
            merge.exec()
            return merge.updated_row_count

    merge_row(1, 'first')
    assert merge_row(42, 'second') == 1, 'a real change in Qty must still update the row'

    tbl = _reflect_facts(engine)
    with engine.connect() as conn:
        row = conn.execute(select(tbl.c['Qty'], tbl.c['Note'])).one()
    assert row[0] == 42, 'the compared column is updated'
    assert row[1] == 'first', 'skip_update_fields wins: the column keeps its inserted value'


@pytest.mark.parametrize("engine_name", list(engines))
def test_reported_schema_changes(engine_name):
    # A merge reports what it changed in the target schema: a downstream consumer that rebuilds a
    # derived dataset from the merged_on watermark can not see a new column otherwise - adding a
    # column changes no values, so no watermark moves.
    logger.debug(f'TEST REPORTED SCHEMA CHANGES {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    k = ['Shop', 'Product']
    dt = {'Shop': String(100), 'Product': String(100), 'Note': String(100)}

    def merge_row(row, **kwargs):
        with dbmerge(engine=engine, data=[row], table_name='Facts', schema='target',
                     temp_schema='tmp', data_types=dt, key=k, **kwargs) as merge:
            return merge.exec()

    result = merge_row({'Shop': 'S', 'Product': 'A', 'Qty': 1},
                       merged_on_field='Merged On', inserted_on_field='Inserted On')
    assert result.table_created is True, 'the target table did not exist and was created'
    assert result.added_fields == {}, 'a new table reports no added columns - it has no previous version'

    result = merge_row({'Shop': 'S', 'Product': 'A', 'Qty': 2},
                       merged_on_field='Merged On', inserted_on_field='Inserted On')
    assert result.table_created is False, 'the table already exists'
    assert result.added_fields == {}, 'nothing was added to the schema'

    # A new data column, alongside a newly requested auto-managed one: only the data column is
    # reported - the delete flag carries no source data to recompute over.
    result = merge_row({'Shop': 'S', 'Product': 'A', 'Qty': 2, 'Note': 'n'},
                       merged_on_field='Merged On', inserted_on_field='Inserted On',
                       delete_mark_field='Deleted')
    assert result.table_created is False
    assert list(result.added_fields) == ['Note'], f'unexpected added_fields {result.added_fields}'
    # the value is the SQLAlchemy type the column was really created with
    added_type = result.added_fields['Note']
    assert isinstance(added_type, types.TypeEngine), f'expected a SQLAlchemy type, got {added_type!r}'
    tbl = _reflect_facts(engine)
    assert isinstance(tbl.c['Note'].type, String), f'"Note" is {tbl.c["Note"].type} in the database'

    # can_create_columns=False: the column is dropped from the merge instead of being created,
    # so there is no schema change to report.
    result = merge_row({'Shop': 'S', 'Product': 'A', 'Qty': 2, 'Note': 'n', 'Extra': 1},
                       can_create_columns=False)
    assert result.added_fields == {}, 'nothing may be reported when no DDL was allowed'

    tbl = _reflect_facts(engine)
    assert 'Extra' not in tbl.c, 'the column was really not created'


if __name__ == '__main__':

    test_date_range_with_delete_mark('cockroachdb','list of dict')
