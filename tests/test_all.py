import pandas as pd
import polars as pl
import numpy as np
from datetime import date, datetime, timezone, timedelta
import time
import os
import sys
import importlib.util
import re
import uuid
import decimal
import pytest
import logging

from sqlalchemy import create_engine, text, select, schema, func, exists, and_, or_, cast, type_coerce
from sqlalchemy import Table, MetaData, Column, String, Date, DateTime, Integer, Numeric, Double, JSON, Uuid, StaticPool, Computed
from sqlalchemy import types
from sqlalchemy.exc import NoSuchModuleError, DBAPIError

from sample_data_in_sqlite import get_data, get_modified_data
import urllib
from dbmerge import dbmerge, drop_table_if_exists, format_ms
from dbmerge.duckdb import read_table as read_duckdb_table
# Through the module: the constants live only there (the exceptions are in the package root as well).
from dbmerge.dbmerge import (IncorrectParameter, IncorrectDataError, NoKeyError,
                            MAX_TEMP_TABLE_NAME_LEN, TEMP_TABLE_TIMESTAMP_FORMAT)

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
           'postgres-psycopg':"postgresql+psycopg://postgres:postgres@localhost:5432/dbmerge",
           'postgres-pg8000':"postgresql+pg8000://postgres:postgres@localhost:5432/dbmerge",
           'mariadb':"""mariadb+mariadbconnector://root:root@127.0.0.1:3306""",
           'mysql-pymysql':"mysql+pymysql://root:root@127.0.0.1:3307",
           'mysql-connector':"mysql+mysqlconnector://root:root@127.0.0.1:3307",
           'mysql-mysqlclient':"mysql+mysqldb://root:root@127.0.0.1:3307",
           'mssql':f"mssql+pyodbc:///?odbc_connect={mssql_settings}",
           'mssql-pymssql':"mssql+pymssql://sa:MSSQL.test_pass@localhost:1433/master",
           # SQLAlchemy 2.1 and newer only
           'mssql-python':"mssql+mssqlpython://sa:MSSQL.test_pass@localhost:1433/master?TrustServerCertificate=yes",
           'cockroachdb':f"cockroachdb://root@localhost:26257/defaultdb?sslmode=disable",
           'cockroachdb-psycopg':"cockroachdb+psycopg://root@localhost:26257/defaultdb?sslmode=disable",
           'clickhouse':"clickhousedb://default:@localhost:8123/default",
           # a file, not :memory: - every connection to :memory: is a database of its own, so the
           # checks below would not see what the merge wrote
           'duckdb':'duckdb:///data/data.ddb',
           #'oracle':"oracle+oracledb://system:oracle@localhost/?service_name=XEPDB1"
         }

# Any URL above can be replaced from the environment, e.g. DBMERGE_TEST_URL_CLICKHOUSE for another server
# version, or DBMERGE_TEST_URL_MYSQL_PYMYSQL for another port.
for _name in engines:
    _override = os.environ.get('DBMERGE_TEST_URL_' + _name.upper().replace('-', '_'))
    if _override:
        engines[_name] = _override

# DBMERGE_TEST_ENGINES=postgres,postgres-pg8000 runs the tests of these engines only. A substring for -k
# would not do: "mssql" is also part of "mssql-pymssql" and "mssql-python".
# Every URL, before DBMERGE_TEST_ENGINES narrows engines down: a test of one engine may need another as a source.
all_engine_urls = dict(engines)

if os.environ.get('DBMERGE_TEST_ENGINES'):
    _selected = os.environ['DBMERGE_TEST_ENGINES'].split(',')
    _unknown = [name for name in _selected if name not in engines]
    if _unknown:
        raise ValueError(f'DBMERGE_TEST_ENGINES: unknown engines {_unknown}, known are {list(engines)}')
    engines = {name: url for name, url in engines.items() if name in _selected}


def _to_polars(frame):
    # pl.from_pandas needs pyarrow for columns of Python objects (strings, dates). Without it the frame is
    # built from plain lists, NaN - the missing value of pandas - turned into null, as from_pandas does.
    if importlib.util.find_spec('pyarrow') is not None:
        return pl.from_pandas(frame)
    return pl.DataFrame({c: [None if isinstance(v, float) and v != v else v for v in frame[c].tolist()]
                         for c in frame.columns})


def make_engine(engine_name):
    # CockroachDB runs SERIALIZABLE by default. A merge step here reads the staging table and
    # updates tens of thousands of target rows, so a single step can stay open for tens of seconds -
    # long enough for an unrelated commit (e.g. the background GC of a dropped temp table) to break
    # the transaction's read-timestamp refresh and fail COMMIT with a transient RETRY_SERIALIZABLE.
    # CockroachDB expects the client to retry such transactions; dbmerge does not, so the tests ask
    # for READ COMMITTED, where CockroachDB retries statements internally.
    # An engine whose dialect or driver is not installed here (mssql-python needs SQLAlchemy 2.1, for one)
    # is skipped rather than failed.
    try:
        if engine_name.startswith('cockroachdb'):
            engine = create_engine(engines[engine_name], isolation_level='READ COMMITTED')
        else:
            engine = create_engine(engines[engine_name])
    except (NoSuchModuleError, ImportError) as e:
        pytest.skip(f'{engine_name} is not available here: {e}')
    _made_engines.append(engine)
    return engine


# The engines make_engine made, disposed after every test: a pooled connection stays open until its engine
# is disposed or collected, and a whole run once used up the 151 connections of MariaDB.
_made_engines = []


@pytest.fixture(autouse=True)
def _dispose_engines():
    yield
    while _made_engines:
        _made_engines.pop().dispose()



key = ['Shop','Product','Date']
data_types = {'Shop':String(100),'Product':String(100)}


def _reflect_facts(engine, table_name='Facts'):
    # sqlite does not support schemas - dbmerge stores the table without one there.
    schema = None if engine.dialect.name=='sqlite' else 'target'
    if engine.dialect.name == 'duckdb':
        # SQLAlchemy can not reflect a DuckDB table correctly, so it is read the way dbmerge reads it.
        with engine.connect() as conn:
            return read_duckdb_table(conn, MetaData(), table_name, schema)
    return Table(table_name, MetaData(), autoload_with=engine, schema=schema)


def _final_on_clickhouse(engine, stmt, tbl):
    # ReplacingMergeTree keeps the replaced versions of a row until its parts are merged, so on
    # ClickHouse the rows are read as they are now: FINAL.
    if engine.dialect.name == 'clickhousedb':
        from clickhouse_connect.cc_sqlalchemy.sql import final
        return final(stmt, tbl)
    return stmt



def count_all_rows(engine, date_from=None, date_to=None):
    """Count rows in target.Facts, optionally restricted to a date range."""
    tbl = _reflect_facts(engine)
    stmt = select(func.count()).select_from(tbl)
    if date_from is not None and date_to is not None:
        stmt = stmt.where(tbl.c['Date'].between(date_from, date_to))
    with engine.connect() as conn:
        return conn.execute(_final_on_clickhouse(engine, stmt, tbl)).scalar()


def count_deleted_rows(engine, date_from=None, date_to=None):
    """Count rows whose 'Deleted' flag is set (True/1), optionally within a date range."""
    tbl = _reflect_facts(engine)
    stmt = select(func.count()).select_from(tbl).where(tbl.c['Deleted'] == True)
    if date_from is not None and date_to is not None:
        stmt = stmt.where(tbl.c['Date'].between(date_from, date_to))
    with engine.connect() as conn:
        return conn.execute(_final_on_clickhouse(engine, stmt, tbl)).scalar()


def get_qty(engine, shop, product):
    """Return the 'Qty' value of a single row identified by Shop/Product."""
    tbl = _reflect_facts(engine)
    stmt = select(tbl.c['Qty']).where((tbl.c['Shop'] == shop) & (tbl.c['Product'] == product))
    with engine.connect() as conn:
        return conn.execute(_final_on_clickhouse(engine, stmt, tbl)).scalar()


def prepare_and_clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_bulk',schema='target')
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

@pytest.mark.parametrize("engine_name", engines)
def test_temp_table_name_format(engine_name):
    # The staging table name is a contract, not cosmetics: on PostgreSQL it is UNLOGGED (persistent),
    # so a process that dies mid-merge leaves it behind, and a cleanup job has nothing but the name
    # to go by - PostgreSQL records no creation time for a table. Hence the fixed prefix, the
    # creation timestamp (leading, so a listing sorts by age) and the random tail, with only the
    # table name part ever truncated.
    logger.debug(f'TEST TEMP TABLE NAME FORMAT {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':1}]
    long_name = "Fact1Fact2Fact3Fact4Fact5Fact6Fact7Fact8Fact9Fact10Fact11Fact12"
    pattern = re.compile(r'^tmp_(\d{12})_(.+)_([0-9a-f]{8})$')

    # exec() drops the staging table and forgets it, so the name is captured as it is created.
    created_names = []
    original_create = dbmerge._create_temp_table

    def spy(self):
        original_create(self)
        created_names.append(self.temp_table.name)

    dbmerge._create_temp_table = spy
    try:
        for table_name in ("Facts", long_name):
            with dbmerge(engine=engine, data=data, table_name=table_name, schema='target',
                         temp_schema='tmp', key=key,
                         data_types={'Shop':String(100),'Product':String(100),'Qty':Integer()}) as merge:
                merge.exec()
                temp_name = created_names[-1]
                match = pattern.match(temp_name)
                assert match, f'Unexpected temp table name {temp_name}'
                assert len(temp_name) <= MAX_TEMP_TABLE_NAME_LEN
                # Only the table name part is truncated, and it keeps its beginning: that is what
                # tells a human which table the leftover belongs to.
                assert table_name.startswith(match.group(2))
                datetime.strptime(match.group(1), TEMP_TABLE_TIMESTAMP_FORMAT)
    finally:
        dbmerge._create_temp_table = original_create
        drop_table_if_exists(engine, long_name, schema='target')


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

    if engine_name == 'clickhouse':
        return  # source_table_name is not supported on ClickHouse

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
        data = _to_polars(data)

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
        data = _to_polars(data)

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
        data = _to_polars(data)

    with dbmerge(engine=engine, data=data,  table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = _to_polars(data)
    
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
        data = _to_polars(data)

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
        data = _to_polars(data)

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
        data = _to_polars(data)
    
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
        data = _to_polars(data)

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='tmp',
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(shops = ['Shop16','Shop18','Shop3'], limit=10000)
    if type_of_data=='dict of list':
        data = data.replace({np.nan: None}).to_dict(orient='list')
    elif type_of_data=='list of dict':
        data = data.replace({np.nan: None}).to_dict(orient='records')
    elif type_of_data=='polars':
        data = _to_polars(data)
    
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
        data = _to_polars(data)

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
        data = _to_polars(data)

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
    if engine_name == 'clickhouse':
        # ClickHouse has correlated subqueries by default since 25.8 (behind a setting in 25.4-25.7), and the
        # condition below is one.
        with engine.connect() as conn:
            try:
                conn.exec_driver_sql('SELECT 1 FROM (SELECT 1 AS a) AS x '
                                     'WHERE EXISTS (SELECT 1 FROM (SELECT 1 AS b) AS y WHERE y.b = x.a)')
            except Exception:
                pytest.skip('this ClickHouse server has no correlated subqueries, which the condition here uses')
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
        rows = {(r.Shop, r.Product): (r.Qty, r.version) for r in conn.execute(_final_on_clickhouse(engine,
            select(tbl.c['Shop'], tbl.c['Product'], tbl.c['Qty'], tbl.c['version']), tbl)).all()}

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
            return conn.execute(_final_on_clickhouse(engine, select(tbl.c['Qty'], tbl.c['version'], tbl.c['Merged On'])
                                .where(tbl.c['Shop'] == 'S'), tbl)).one()._mapping

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
        seeded_merged_on = conn.execute(_final_on_clickhouse(engine, select(tbl.c['Merged On'])
                                        .where(tbl.c['Product'] == 'B'), tbl)).scalar()

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
        rows = {r.Product: r._mapping for r in conn.execute(_final_on_clickhouse(engine,
            select(tbl.c['Product'], tbl.c['Deleted'], tbl.c['version'], tbl.c['Merged On']), tbl)).all()}

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
        row = conn.execute(_final_on_clickhouse(engine, select(tbl.c['Merged On'], tbl.c['Inserted On'], tbl.c['Deleted']), tbl)).one()
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
def test_auto_types_keep_fraction_and_microseconds(engine_name, request):
    # A column created automatically must keep the value it was created from. The generic types are
    # resolved differently per engine, and some of them round unless the precision is stated.
    logger.debug(f'TEST AUTO TYPES KEEP PRECISION {engine_name}')
    if engine_name == 'mssql-pymssql':
        # The driver itself sends a datetime parameter with milliseconds only (COMPATIBILITY.md).
        request.applymarker(pytest.mark.xfail(reason='pymssql truncates microseconds to milliseconds', strict=True))
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    stamp = datetime(2026, 8, 2, 12, 34, 56, 987654)
    # More digits than a decimal of a default scale or a 4-byte float keep.
    price = 0.1234567890123
    data = [{'Shop': 'S', 'Product': 'A', 'Price': price, 'Stamp': stamp}]

    # The second merge goes into the table the first one created: its staging table is typed after the
    # target as read back from the database, so the value has to survive that reading too.
    for round_no in (1, 2):
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     data_types={'Shop': String(100), 'Product': String(100)},
                     key=['Shop', 'Product']) as merge:
            result = merge.exec()
        if round_no == 2:
            assert result.updated_row_count == 0, 'the same data merged again must change nothing'

        tbl = _reflect_facts(engine)
        # Read as a float: SQLAlchemy turns a NUMERIC of SQLite into a Decimal of 10 digits on the way
        # back, whatever is stored.
        stmt = select(type_coerce(tbl.c['Price'], Double), tbl.c['Stamp'])
        with engine.connect() as conn:
            row = conn.execute(_final_on_clickhouse(engine, stmt, tbl)).one()

        assert row[0] == price, f'the fraction must survive merge {round_no}, got {row[0]!r}'
        assert row[1].microsecond == 987654, f'microseconds must survive merge {round_no}, got {row[1]!r}'


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
        stored = conn.execute(_final_on_clickhouse(engine, select(tbl.c['Note']), tbl)).scalar()
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
        assert conn.execute(_final_on_clickhouse(engine, select(tbl.c['version']), tbl)).scalar() == 5, 'the skipped column stays as inserted'


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
                    conn.execute(_final_on_clickhouse(engine, select(tbl.c['Product'], tbl.c['Deleted']), tbl)).all()}

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
        row = conn.execute(_final_on_clickhouse(engine, select(tbl.c['Qty'], tbl.c['Note']), tbl)).one()
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
    note_type = tbl.c['Note'].type
    # ClickHouse types reflect as clickhouse-connect's own classes, not as subclasses of String
    is_string = isinstance(note_type, String) or (engine_name == 'clickhouse' and 'String' in note_type.name)
    assert is_string, f'"Note" is {note_type} in the database'

    # can_create_columns=False: the column is dropped from the merge instead of being created,
    # so there is no schema change to report.
    result = merge_row({'Shop': 'S', 'Product': 'A', 'Qty': 2, 'Note': 'n', 'Extra': 1},
                       can_create_columns=False)
    assert result.added_fields == {}, 'nothing may be reported when no DDL was allowed'

    tbl = _reflect_facts(engine)
    assert 'Extra' not in tbl.c, 'the column was really not created'


def _parity_data(type_of_data, postgres_family=False):
    # Values that a loader can get wrong: an empty string next to NULL, quotes, separators and line breaks,
    # a large integer, microseconds, time zones, dates far from today, JSON, UUID, and NULL everywhere.
    # The UUIDs are proper version 4 ones: the native UUID type of MariaDB, which SQLAlchemy 2.1 creates for
    # Uuid, refuses others.
    utc = timezone.utc
    msk = timezone(timedelta(hours=3))
    data = [{'Shop': 'S1', 'Product': 'p1', 'Date': date(2025, 1, 1), 'Qty': 1, 'Price': 1.25, 'Flag': True,
             'Note': 'plain', 'Stamp': datetime(2025, 1, 1, 12, 0, 0, 123456),
             'StampTz': datetime(2025, 1, 1, 12, 0, tzinfo=utc), 'Old': date(1950, 5, 5),
             'Data': {'a': [1, 2], 'b': 'x"y'}, 'uuid': uuid.UUID('12345678-1234-4678-9234-567812345678')},
            {'Shop': 'S1', 'Product': 'p2', 'Date': date(2025, 1, 2), 'Qty': None, 'Price': None, 'Flag': None,
             'Note': '', 'Stamp': None, 'StampTz': None, 'Old': None, 'Data': None, 'uuid': None},
            {'Shop': 'S2', 'Product': 'p1', 'Date': date(2025, 1, 3), 'Qty': -2**40, 'Price': -0.5, 'Flag': False,
             'Note': 'quote " comma , newline \n tab \t backslash \\ unicode ёж', 'Stamp': datetime(1999, 12, 31, 23, 59, 59),
             'StampTz': datetime(2025, 6, 1, 12, 0, tzinfo=msk), 'Old': date(2100, 1, 1),
             'Data': {'a': [], 'b': ''}, 'uuid': uuid.UUID('87654321-4321-4765-8321-876543218765')}]
    data_types = {'Shop': String(100), 'Product': String(100), 'uuid': Uuid()}
    if postgres_family:
        # An aware datetime headed for a column without a time zone: each driver converts it its own way,
        # and COPY must repeat what the driver does. (MySQL drivers refuse or mangle such values outright.)
        for row, value in zip(data, (datetime(2025, 1, 1, 12, 0, tzinfo=utc), None,
                                     datetime(2025, 6, 1, 12, 0, tzinfo=msk))):
            row['StampTzToNaive'] = value
        data_types['StampTzToNaive'] = DateTime(timezone=False)
    if type_of_data == 'dict of list':
        data = {k: [d[k] for d in data] for k in data[0].keys()}
    elif type_of_data == 'pandas':
        data = pd.DataFrame(data)
    elif type_of_data == 'polars':
        data = pl.DataFrame(data)
    return data, data_types


def _stored_as_text(engine, engine_name, table_name):
    # Compared as the text the database itself renders the values to, so that no driver conversion on the
    # way back can hide a difference.
    tbl = _reflect_facts(engine, table_name)
    # ClickHouse casts to a non-nullable String and fails on NULL, toString keeps it nullable
    as_text = (lambda c: func.toString(c)) if engine_name == 'clickhouse' else (lambda c: cast(c, String(4000)))
    stmt = select(*[as_text(c).label(c.name) for c in tbl.c]).order_by(*[tbl.c[k] for k in key])
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(_final_on_clickhouse(engine, stmt, tbl)).all()]


def _assert_same_rows(engine, engine_name, first_label, second_label):
    first, second = _stored_as_text(engine, engine_name, 'Facts'), _stored_as_text(engine, engine_name, 'Facts_bulk')
    # A NUMERIC without precision keeps the scale a value came with, and drivers differ in it (1.0 or 1),
    # so numbers are compared as numbers.
    tbl = _reflect_facts(engine)
    numeric = {c.name for c in tbl.c if isinstance(c.type, Numeric)}

    def same(column, a, b):
        if a == b:
            return True
        return column in numeric and a is not None and b is not None and decimal.Decimal(a) == decimal.Decimal(b)

    # zip() would stop at the shorter side, so the row counts and the columns are compared first.
    assert len(first) == len(second) == 3, f'{first_label}: {len(first)} rows, {second_label}: {len(second)} rows'
    assert set(first[0]) == set(second[0]), f'{first_label} columns {sorted(first[0])}, {second_label} {sorted(second[0])}'
    for a, b in zip(first, second):
        differences = {c: (a[c], b.get(c)) for c in a if not same(c, a[c], b.get(c))}
        assert not differences, f'{a["Shop"]}/{a["Product"]}: {first_label} vs {second_label} {differences}'


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name,type_of_data)
                                                     for engine_name in engines
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas','polars')])
def test_bulk_load_stores_what_row_by_row_stores(engine_name, type_of_data):
    # Where an engine and its driver have a bulk loader, it must store exactly what the row by row
    # INSERT stores: the same data goes through both into two tables, which must hold the same values.
    logger.debug(f'TEST BULK LOAD STORES WHAT ROW BY ROW STORES {engine_name} {type_of_data}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    data, data_types = _parity_data(type_of_data, engine.dialect.name in ('postgresql', 'cockroachdb'))
    for table_name, bulk_load in (('Facts', False), ('Facts_bulk', True)):
        with dbmerge(engine=engine, data=data, table_name=table_name, schema='target', temp_schema='tmp',
                     key=key, data_types=data_types) as merge:
            merge.exec(bulk_load=bulk_load)
    _assert_same_rows(engine, engine_name, 'row by row', 'bulk')


@pytest.mark.parametrize("type_of_data", ('list of dict', 'dict of list', 'pandas', 'polars'))
def test_clickhouse_without_pyarrow_stores_what_arrow_stores(type_of_data, monkeypatch):
    # Without pyarrow ClickHouse loads the staging table through the native protocol of the client. It must
    # store exactly what the Arrow load stores.
    if 'clickhouse' not in engines:
        pytest.skip('ClickHouse is not among the tested engines')
    engine = make_engine('clickhouse')
    prepare_and_clean_data(engine)
    data, data_types = _parity_data(type_of_data)
    with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', key=key, data_types=data_types) as merge:
        merge.exec()
    import sys
    monkeypatch.setattr(sys.modules['dbmerge.clickhouse'], 'pa', None)
    with dbmerge(engine=engine, data=data, table_name='Facts_bulk', schema='target', key=key, data_types=data_types) as merge:
        merge.exec()
    _assert_same_rows(engine, 'clickhouse', 'Arrow', 'native protocol')


@pytest.mark.parametrize("engine_name", list(engines))
def test_list_of_dict_rows_with_other_keys_are_refused(engine_name):
    # The columns of a list of dicts are those of its first row. A later row with an extra key would lose its
    # value, and one with a missing key would fail row by row but store NULL in a bulk load (DB-08).
    logger.debug(f'TEST LIST OF DICT ROWS WITH OTHER KEYS ARE REFUSED {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    first = {'Shop': '1', 'Product': 'a', 'Date': date(2025, 1, 1), 'Qty': 1}
    for second in ({**first, 'Shop': '2', 'Extra': 'lost'}, {'Shop': '2', 'Product': 'a', 'Date': date(2025, 1, 1)}):
        with pytest.raises(IncorrectDataError, match='Row 1'):
            with dbmerge(engine=engine, data=[first, second], table_name='Facts', schema='target', temp_schema='tmp',
                         key=key, data_types=data_types) as merge:
                merge.exec()


@pytest.mark.parametrize("engine_name", list(engines))
def test_pandas_duplicate_columns_are_refused(engine_name):
    # Two DataFrame columns of one name would end up as one column, losing the values of the other (DB-09).
    logger.debug(f'TEST PANDAS DUPLICATE COLUMNS ARE REFUSED {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    data = pd.DataFrame([['1', 'a', date(2025, 1, 1), 1, 2]], columns=['Shop', 'Product', 'Date', 'Qty', 'Qty'])
    with pytest.raises(IncorrectDataError, match='Qty'):
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     key=key, data_types=data_types) as merge:
            merge.exec()


@pytest.mark.parametrize("engine_name", list(engines))
def test_computed_target_column_is_not_written(engine_name):
    # A column the target computes itself (DB-07). Merged from a source table built like the target, it is
    # left out and computed by the target; brought in "data", it is refused before anything is written.
    if engine_name == 'clickhouse':
        pytest.skip('ClickHouse computes MATERIALIZED and ALIAS columns, which SQLAlchemy Computed does not create')
    if engine_name == 'duckdb':
        pytest.skip('DuckDB does not tell a generated column from a default in its catalog')
    logger.debug(f'TEST COMPUTED TARGET COLUMN IS NOT WRITTEN {engine_name}')
    engine = make_engine(engine_name)
    schema = None if engine.dialect.name == 'sqlite' else 'target'
    metadata = MetaData()
    source, target = [Table(name, metadata, Column('id', Integer, primary_key=True, autoincrement=False),
                            Column('a', Integer), Column('doubled', Integer, Computed('a * 2', persisted=True)),
                            schema=schema)
                      for name in ('Generated_source', 'Generated')]
    # One table per transaction: CockroachDB refuses two schema changes in one under READ COMMITTED.
    for table in (source, target):
        drop_table_if_exists(engine, table.name, schema=schema)
        table.create(engine)
    try:
        with engine.begin() as conn:
            conn.execute(source.insert(), [{'id': 1, 'a': 3}])
        with dbmerge(engine=engine, source_table_name='Generated_source', source_schema=schema,
                     table_name='Generated', schema=schema, temp_schema='tmp') as merge:
            result = merge.exec()
        assert result.inserted_row_count == 1
        with engine.connect() as conn:
            assert tuple(conn.execute(select(target.c['a'], target.c['doubled'])).one()) == (3, 6)
        with pytest.raises(IncorrectParameter, match='doubled'):
            with dbmerge(engine=engine, data=[{'id': 1, 'a': 4, 'doubled': 8}], table_name='Generated',
                         schema=schema, temp_schema='tmp') as merge:
                merge.exec()
    finally:
        for table in (source, target):
            drop_table_if_exists(engine, table.name, schema=schema)


@pytest.mark.parametrize("engine_name", list(engines))
def test_failed_cleanup_keeps_the_error_and_closes(engine_name, caplog):
    # A staging table that can not be dropped must neither hide the error of the merge nor leave the
    # connection open (DB-12). After a merge that succeeded, it is only a warning: the data is committed.
    logger.debug(f'TEST FAILED CLEANUP KEEPS THE ERROR AND CLOSES {engine_name}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    data = [{'Shop': '1', 'Product': 'a', 'Date': date(2025, 1, 1), 'Qty': 1}]

    def failing_drop(merge):
        real_drop = merge._drop_temp_table
        def drop():
            real_drop()     # the table really goes, so no test leaves one behind
            raise RuntimeError('cleanup failed')
        merge._drop_temp_table = drop

    with pytest.raises(RuntimeError, match='merge failed'):
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     key=key, data_types=data_types) as merge:
            failing_drop(merge)
            def failing_load():
                raise RuntimeError('merge failed')
            merge._insert_data_to_temp = failing_load
            merge.exec()
    assert merge.conn.closed and merge.merge_finished

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger='dbmerge'):
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', temp_schema='tmp',
                     key=key, data_types=data_types) as merge:
            failing_drop(merge)
            result = merge.exec()
    assert result.inserted_row_count == 1
    assert merge.conn.closed and merge.merge_finished
    assert any('Could not drop the staging table' in r.getMessage() for r in caplog.records)


def test_exceptions_are_exported_from_the_package_root():
    import dbmerge as package
    from dbmerge.dbmerge import IncorrectParameter as old_path
    assert package.IncorrectParameter is old_path
    exported = {'IncorrectParameter', 'IncorrectDataError', 'NoKeyError', 'TableNotFoundError', 'TempTableAlreadyExists'}
    assert exported <= set(package.__all__) and all(hasattr(package, name) for name in exported)


@pytest.mark.parametrize("type_of_data", ('list of dict', 'dict of list', 'pandas'))
def test_clickhouse_reports_column_and_row_of_a_bad_value(type_of_data):
    # ClickHouse loads in bulk only, through Arrow. Arrow names neither the column nor the row of a value it
    # can not take, so dbmerge finds them for the error.
    if 'clickhouse' not in engines:
        pytest.skip('ClickHouse is not among the tested engines')
    engine = make_engine('clickhouse')
    prepare_and_clean_data(engine)
    data = [{'id': i, 'amount_x': i} for i in range(999)] + [{'id': 999, 'amount_x': 'bad text'}]
    if type_of_data == 'dict of list':
        data = {k: [d[k] for d in data] for k in data[0]}
    elif type_of_data == 'pandas':
        data = pd.DataFrame(data)
    with pytest.raises(IncorrectDataError, match=r'Column "amount_x", row 999: .*bad text'):
        with dbmerge(engine=engine, data=data, table_name='Facts', schema='target', key=['id'],
                     data_types={'amount_x': types.BigInteger()}) as merge:
            merge.exec(chunk_size=600)      # the bad row lands in the second chunk: the row number is not per chunk


def _attach_postgresql():
    """
    The PostgreSQL of the tests attached to ClickHouse as the database dbmerge_pg (its schema "source"), the
    likely source of source_table_name on ClickHouse. ClickHouse reaches PostgreSQL itself, at an address that
    may differ from the one the tests use - in CI they are separate containers - so
    DBMERGE_TEST_CLICKHOUSE_POSTGRES gives it as host:port. Returns the engines of ClickHouse and of that
    PostgreSQL.
    """
    from sqlalchemy.engine import make_url
    address = os.environ.get('DBMERGE_TEST_CLICKHOUSE_POSTGRES')
    if 'clickhouse' not in engines or not address:
        pytest.skip('needs ClickHouse and DBMERGE_TEST_CLICKHOUSE_POSTGRES, the address of PostgreSQL for ClickHouse')
    engine = make_engine('clickhouse')
    url = make_url(all_engine_urls['postgres'])
    pg = create_engine(url)
    _made_engines.append(pg)
    try:
        with pg.begin() as conn:
            conn.exec_driver_sql('CREATE SCHEMA IF NOT EXISTS source')
    except Exception as e:
        pytest.skip(f'PostgreSQL is not available: {e}')
    with engine.connect() as conn:
        conn.exec_driver_sql('DROP DATABASE IF EXISTS dbmerge_pg')
        conn.exec_driver_sql(f"CREATE DATABASE dbmerge_pg ENGINE = PostgreSQL('{address}', '{url.database}', "
                             f"'{url.username}', '{url.password}', 'source')")
    return engine, pg


def _detach_postgresql(engine, pg, table_name):
    with engine.connect() as conn:
        conn.exec_driver_sql('DROP DATABASE IF EXISTS dbmerge_pg')
    with pg.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE IF EXISTS source.{table_name}')


def _same_as_source(engine, table_name):
    """The target table, read as it is now (FINAL), holds what ClickHouse reads from the PostgreSQL table."""
    with engine.connect() as conn:
        source = conn.exec_driver_sql(f'SELECT * FROM dbmerge_pg.{table_name} ORDER BY 1, 2, 3').all()
        target = conn.exec_driver_sql('SELECT * FROM target.Facts FINAL ORDER BY 1, 2, 3').all()
    return [tuple(r) for r in source] == [tuple(r) for r in target]


def test_clickhouse_linked_postgresql_types():
    # A ClickHouse table filled from a PostgreSQL table of many types through the PostgreSQL database engine.
    # The target takes the types ClickHouse reads them as - but a key column that is nullable in PostgreSQL is
    # made not nullable, as ORDER BY wants - and a second merge of the same data must find nothing to change.
    engine, pg = _attach_postgresql()
    prepare_and_clean_data(engine)
    with pg.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS source.ch_types')
        conn.exec_driver_sql("""CREATE TABLE source.ch_types (id integer, s text, v varchar(10), c char(3), i2 smallint,
            i8 bigint, n numeric(12,2), nn numeric, d double precision, r real, f boolean, dt date, ts timestamp(6),
            tz timestamptz, t time, u uuid, j jsonb, by bytea, arr integer[], tarr text[])""")
        conn.exec_driver_sql("""INSERT INTO source.ch_types VALUES
            (1, 'text "quoted", with a comma', 'ünïcödé', 'ab', -32768, 9223372036854775807, 1234567890.12,
             0.1234567890123, 1.5e300, 1.25, true, '1900-01-01', '2025-06-01 12:34:56.123456', '2025-06-01 12:00+03',
             '13:45:00', '12345678-1234-4678-9234-567812345678', '{"a": [1, null]}', '\\x00ff', '{1,2,NULL}', '{"x","y"}'),
            (2, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (3, 'gone soon', NULL, NULL, 0, 0, 0, 0, 0, 0, false, '2299-12-31', NULL, NULL, NULL, NULL, 'null', NULL, '{}', '{}')""")
    try:
        def merge(**kw):
            with dbmerge(engine=engine, source_table_name='ch_types', source_schema='dbmerge_pg',
                         table_name='Facts', schema='target', **kw) as merge:
                result = merge.exec()
            return result.inserted_row_count, result.updated_row_count, result.deleted_row_count

        assert merge(key=['id']) == (3, 0, 0)
        with engine.connect() as conn:
            types = dict(conn.exec_driver_sql("SELECT name, type FROM system.columns WHERE database = 'dbmerge_pg' "
                                              "AND table = 'ch_types'").all())
            created = dict(conn.exec_driver_sql("SELECT name, type FROM system.columns WHERE database = 'target' "
                                                "AND table = 'Facts'").all())
        assert types['id'] == 'Nullable(Int32)' and created['id'] == 'Int32', (types['id'], created['id'])
        assert {c: t for c, t in created.items() if c != 'id'} == {c: t for c, t in types.items() if c != 'id'}

        assert merge(delete_mode='delete') == (0, 0, 0), 'the same data merged again must change nothing'

        with pg.begin() as conn:
            conn.exec_driver_sql("""UPDATE source.ch_types SET s = 'changed', v = NULL, i2 = 1, n = 2.5, nn = NULL,
                d = -0.0001, r = 2.5, f = false, dt = '2299-12-31', ts = '2025-06-01 12:34:56.123457', tz = NULL,
                u = '87654321-4321-4765-8321-876543218765', j = '{"a": [1, 2]}', by = '\\x01', arr = '{1,2}',
                tarr = '{}' WHERE id = 1""")
            conn.exec_driver_sql("UPDATE source.ch_types SET s = NULL, arr = '{7}' WHERE id = 2")
            conn.exec_driver_sql("DELETE FROM source.ch_types WHERE id = 3")
            conn.exec_driver_sql("INSERT INTO source.ch_types (id, s) VALUES (4, 'new')")
        assert merge(delete_mode='delete') == (1, 2, 1)
        assert _same_as_source(engine, 'ch_types')
        assert merge(delete_mode='delete') == (0, 0, 0), 'the same data merged again must change nothing'
    finally:
        _detach_postgresql(engine, pg, 'ch_types')


def test_clickhouse_linked_postgresql_sample_data():
    # A ClickHouse table kept in step with a PostgreSQL table through the PostgreSQL database engine: every merge
    # of ClickHouse must count what the merge of the same change into PostgreSQL counted, and leave the same rows.
    engine, pg = _attach_postgresql()
    prepare_and_clean_data(engine)
    drop_table_if_exists(pg, 'ch_facts', schema='source')

    def into_postgresql(data, **kw):
        with dbmerge(engine=pg, data=data, table_name='ch_facts', schema='source', key=key,
                     data_types=data_types, **kw) as merge:
            result = merge.exec()
        return result.inserted_row_count, result.updated_row_count, result.deleted_row_count

    try:
        assert into_postgresql(get_data(limit=50000)) == (50000, 0, 0)
        with dbmerge(engine=engine, source_table_name='ch_facts', source_schema='dbmerge_pg',
                     table_name='Facts', schema='target', key=key) as merge:
            assert merge.exec().inserted_row_count == 50000
        assert _same_as_source(engine, 'ch_facts')

        # inserts, updates and deletes at once: get_modified_data changes prices, adds rows and leaves some out
        changed = into_postgresql(get_modified_data(limit=50000), delete_mode='delete')
        assert all(changed), changed
        with dbmerge(engine=engine, source_table_name='ch_facts', source_schema='dbmerge_pg',
                     table_name='Facts', schema='target', delete_mode='delete') as merge:
            result = merge.exec()
        assert (result.inserted_row_count, result.updated_row_count, result.deleted_row_count) == changed
        assert _same_as_source(engine, 'ch_facts')

        # a merge of a part only: one shop leaves PostgreSQL, and the scoped merge removes just that shop
        with pg.begin() as conn:
            removed = conn.exec_driver_sql('DELETE FROM source.ch_facts WHERE "Shop" = \'Shop1\'').rowcount
        with dbmerge(engine=engine, source_table_name='ch_facts', source_schema='dbmerge_pg',
                     table_name='Facts', schema='target', delete_mode='delete') as merge:
            result = merge.exec(source_condition=merge.source_table.c['Shop'].in_(['Shop1', 'Shop2']),
                                delete_condition=merge.table.c['Shop'].in_(['Shop1', 'Shop2']))
        assert (result.inserted_row_count, result.updated_row_count, result.deleted_row_count) == (0, 0, removed)
        assert removed > 0 and _same_as_source(engine, 'ch_facts')
    finally:
        _detach_postgresql(engine, pg, 'ch_facts')


# The engines that load the staging table with COPY.
POSTGRES_FAMILY = [e for e in engines if e.startswith(('postgres', 'cockroachdb'))]


def _spy_on_copy(monkeypatch):
    """The SQL of every COPY the bulk loader runs: no fall back message in the log does not prove COPY ran."""
    # imported lazily by dbmerge, so it may not be loaded yet. Not "import dbmerge._bulk_load": that would
    # bind the name dbmerge in the calling function to the package, not to the class.
    from dbmerge import _bulk_load
    calls = []
    real_copy = _bulk_load._copy

    def copy(merge, sql, data):
        calls.append(sql)
        return real_copy(merge, sql, data)
    monkeypatch.setattr(_bulk_load, '_copy', copy)
    return calls


@pytest.mark.parametrize("engine_name", [e for e in engines if e != 'clickhouse'])
def test_bulk_load_reports_the_original_error(engine_name):
    # A value the bulk load refuses must surface as the same SQLAlchemy exception the row by row INSERT
    # raises. COPY writes past SQLAlchemy, so its error could turn into "current transaction is aborted" from
    # the DROP of the staging table that runs after it. fast_executemany of pyodbc may have stored the rows
    # before the bad one, so the row by row retry could fail on the primary key of the staging table instead.
    # The bad value comes last in a chunk of 1000. A naive datetime column makes the COPY loader ask for the
    # session time zone first (except on pg8000), which opens the transaction another way, so both are tried.
    # (ClickHouse always loads in bulk, there is nothing to compare with. A value Arrow can not put in an
    # integer column goes row by row on DuckDB.)
    logger.debug(f'TEST BULK LOAD REPORTS THE ORIGINAL ERROR {engine_name}')
    engine = make_engine(engine_name)
    if engine.dialect.name == 'sqlite':
        pytest.skip('SQLite stores text in an integer column, so there is no error to compare')
    for with_naive_datetime in (False, True):
        for commit_all_steps in (True, False):
            rows = [{'id': i, 'value': i} for i in range(999)] + [{'id': 999, 'value': 'not a number'}]
            if with_naive_datetime:
                rows = [{**row, 'Stamp': datetime(2025, 1, 1, 12, 0)} for row in rows]
            errors = {}
            for bulk_load in (False, True):
                drop_table_if_exists(engine, 'BadData', schema='target')
                with pytest.raises(DBAPIError) as info:
                    with dbmerge(engine=engine, data=rows, table_name='BadData',
                                 schema='target', temp_schema='tmp', key=['id'],
                                 data_types={'value': types.BigInteger()}) as merge:
                        merge.exec(bulk_load=bulk_load, commit_all_steps=commit_all_steps)
                errors[bulk_load] = info.value
            case = f'naive datetime: {with_naive_datetime}, commit_all_steps: {commit_all_steps}'
            assert type(errors[True]) is type(errors[False]), (case, type(errors[False]), type(errors[True]))
            assert 'PRIMARY KEY' not in str(errors[True]).upper(), (case, str(errors[True]))
            if engine_name in POSTGRES_FAMILY:
                assert 'not a number' in str(errors[True]), (case, str(errors[True]))
    drop_table_if_exists(engine, 'BadData', schema='target')


@pytest.mark.parametrize("engine_name,type_of_data", [(engine_name, type_of_data)
                                                     for engine_name in POSTGRES_FAMILY
                                                     for type_of_data in ('list of dict', 'dict of list', 'pandas', 'polars')])
def test_bulk_load_without_pyarrow_stores_what_row_by_row_stores(engine_name, type_of_data, monkeypatch):
    # Without pyarrow the CSV for COPY is written by the csv module (Python 3.12+). CI always has pyarrow,
    # so this path is covered only here.
    if sys.version_info < (3, 12):
        pytest.skip('without pyarrow, COPY needs the csv module of Python 3.12+')
    from dbmerge import _bulk_load
    monkeypatch.setattr(_bulk_load, '_has_pyarrow', lambda: False)
    copies = _spy_on_copy(monkeypatch)
    logger.debug(f'TEST BULK LOAD WITHOUT PYARROW STORES WHAT ROW BY ROW STORES {engine_name} {type_of_data}')
    engine = make_engine(engine_name)
    prepare_and_clean_data(engine)
    data, data_types = _parity_data(type_of_data, True)
    for table_name, bulk_load in (('Facts', False), ('Facts_bulk', True)):
        with dbmerge(engine=engine, data=data, table_name=table_name, schema='target', temp_schema='tmp',
                     key=key, data_types=data_types) as merge:
            merge.exec(bulk_load=bulk_load)
    assert copies, 'the staging table was not loaded with COPY'
    _assert_same_rows(engine, engine_name, 'row by row', 'bulk without pyarrow')


@pytest.mark.parametrize("engine_name,with_pyarrow", [(engine_name, with_pyarrow)
                                                     for engine_name in POSTGRES_FAMILY
                                                     for with_pyarrow in (True, False)])
def test_json_goes_through_copy(engine_name, with_pyarrow, monkeypatch):
    # JSON must go through COPY rather than fall back to row by row, and be stored as the row by row INSERT
    # stores it, JSON null and SQL NULL apart. With SQLAlchemy 2.1 and psycopg 3, JSON has no bind processor
    # at all - the driver adapts it itself - so dbmerge has to serialize it for the CSV on its own.
    from dbmerge import _bulk_load
    from sqlalchemy import null
    from sqlalchemy.dialects.postgresql import JSONB
    if with_pyarrow and importlib.util.find_spec('pyarrow') is None:
        pytest.skip('pyarrow is not installed')
    if not with_pyarrow:
        if sys.version_info < (3, 12):
            pytest.skip('without pyarrow, COPY needs the csv module of Python 3.12+')
        monkeypatch.setattr(_bulk_load, '_has_pyarrow', lambda: False)
    copies = _spy_on_copy(monkeypatch)
    engine = make_engine(engine_name)
    values = [{'a': 1, 'list': [1, None], 'text': 'x"y'}, [1, 'two'], {}, 'text', 0, None, JSON.NULL, null()]
    data = [{'id': i, 'doc': value, 'doc_none_as_null': value} for i, value in enumerate(values)]
    data_types = {'doc': JSONB(), 'doc_none_as_null': JSONB(none_as_null=True)}
    stored = {}
    for table_name, bulk_load in (('JsonRows', False), ('JsonBulk', True)):
        drop_table_if_exists(engine, table_name, schema='target')
        copies.clear()
        with dbmerge(engine=engine, data=data, table_name=table_name, schema='target', temp_schema='tmp',
                     key=['id'], data_types=data_types) as merge:
            merge.exec(bulk_load=bulk_load)
        assert bool(copies) == bulk_load, f'COPY calls with bulk_load={bulk_load}: {len(copies)}'
        tbl = Table(table_name, MetaData(), autoload_with=engine, schema='target')
        with engine.connect() as conn:
            stored[bulk_load] = conn.execute(select(
                tbl.c['id'], cast(tbl.c['doc'], String), tbl.c['doc'].is_(None),
                cast(tbl.c['doc_none_as_null'], String), tbl.c['doc_none_as_null'].is_(None)).order_by(tbl.c['id'])).all()
        drop_table_if_exists(engine, table_name, schema='target')
    assert stored[True] == stored[False], (stored[False], stored[True])


if __name__ == '__main__':

    test_date_range_with_delete_mark('cockroachdb','list of dict')
