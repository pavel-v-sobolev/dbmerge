import pandas as pd
import numpy as np
from datetime import date
import uuid
import pytest
import logging

from sqlalchemy import create_engine,text,select
from sqlalchemy import Table, MetaData, Column, String, Date, Integer, Numeric, JSON, Uuid

from sample_data_in_pg import get_data, get_modified_data

from dbmerge import dbmerge, drop_table_if_exists


logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

engines = {'sqlite':create_engine("""sqlite:///data/data.sqlite"""),
           'postgres':create_engine("""postgresql+psycopg2://postgres:@localhost:5432/dbmerge"""),
           'mariadb':create_engine("""mariadb+mariadbconnector://root:root@localhost:3306""")
          }





key = ['Shop','Product','Date']
data_types = {'Shop':String(100),'Product':String(100)}



def clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_source',schema='source')


@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_table_create_from_data_with_various_types(engine_name,test_pandas):
    logger.debug('TEST TABLE CREATE FROM DATA WITH VARIOUS TYPES')
    engine = engines[engine_name]
    clean_data(engine)

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None,'Price':1.1,'Data':{'a':1},'uuid':uuid.uuid4()},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':None,'Data':{'b':[1,2]},'uuid':uuid.uuid4()},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':1.2,'Data':{'c':[]},'uuid':uuid.uuid4()}]
    data_types = {'Shop':String(100),'Product':String(100),'uuid':Uuid()}

    if test_pandas:
        data = pd.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'



@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,True) 
                                                     for engine_name in engines])
def test_empty_data_updates(engine_name,test_pandas):
    logger.debug('TEST TABLE CREATE FROM DATA WITH VARIOUS TYPES')
    engine = engines[engine_name]
    clean_data(engine)

    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':2,'Price':50.10},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':100.50},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':120.20}]

    if test_pandas:
        data = pd.DataFrame(data)
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  key=key, data_types=data_types) as merge:
        merge.exec()
    if test_pandas:
        data = pd.DataFrame(pd.DataFrame({'Shop':[],'Product':[],'Date':[]}))
    else:
        data = []

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',delete_mode='delete') as merge:
        merge.exec()
        assert merge.deleted_row_count==3, f'Incorrect row count from delete {merge.deleted_row_count}, should be 3'
    
    with dbmerge(engine=engine, table_name="Facts_empty", schema='target', temp_schema='temp',delete_mode='delete',
                 source_table_name = 'Facts', source_schema = 'target', key=key) as merge:
        merge.exec()
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.deleted_row_count}, should be 0'    



@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_table_only_key_no_other_fields(engine_name,test_pandas):
    logger.debug('TEST ONLY KEY NO OTHER FIELDS')
    engine = engines[engine_name]
    clean_data(engine)

    data=[{'Shop':'123 ','Product':'123','Date':date(2025,1,1)},
        {'Shop':'124','Product':' 1223','Date':date(2025,1,1)}]
    if test_pandas:
        data = pd.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts",key=key, schema='target', temp_schema='temp',
                  delete_mode='delete', data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count==2, f'Incorrect row count from insert {merge.inserted_row_count}, should be 2'
    
    data=[{'Shop':'123 ','Product':'123','Date':date(2025,1,1)}]
    if test_pandas:
        data = pd.DataFrame(data)
    with dbmerge(engine=engine, data=data, table_name="Facts",key=key, schema='target', temp_schema='temp',
                  delete_mode='delete') as merge:
        merge.exec()
        assert merge.deleted_row_count==1, f'Incorrect row count from delete {merge.deleted_row_count}, should be 1'

@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_insert_to_existing_table_and_test_new_field(engine_name,test_pandas):
    logger.debug('TEST INSERT TO EXISTING TABLE AND TEST NEW FIELD')
    engine = engines[engine_name]
    clean_data(engine)

    logger.debug('Create table from first merge')
    data=[{'Shop':'123','Product':'123','Date':date(2025,1,1),'Qty':None,'Price':1.1},
        {'Shop':'124','Product':'123','Date':date(2025,1,1),'Qty':1,'Price':None},
        {'Shop':'124','Product':'1223','Date':date(2025,1,1),'Qty':1,'Price':1.2}]
    if test_pandas:
        data = pd.DataFrame(data)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp', 
                  data_types=data_types, key=key) as merge:
        merge.exec()
        assert merge.inserted_row_count==3, f'Incorrect row count from insert {merge.inserted_row_count}, should be 3'

    data = get_data(limit=10000)
    data['Test Field']=1
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  merged_on_field='Merged On',inserted_on_field='Inserted On') as merge:
        merge.exec(chunk_size = 10000)
        assert merge.inserted_row_count==10000, f'Incorrect row count from insert {merge.inserted_row_count}, should be 10000'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be =0'

@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_change_data_and_mark_deleted_data(engine_name,test_pandas):
    logger.debug("TEST CHANGE DATA AND DELETE DATA with delete_mode='mark'")
    engine = engines[engine_name]
    clean_data(engine)

    data = get_data(limit=10001)
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='temp',
                  data_types=data_types, key=key) as merge:
        merge.exec(chunk_size = 10000)
        assert merge.inserted_row_count==10001, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==10001'

    data = get_modified_data(limit=10000)
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='temp',
                  delete_mode='mark',merged_on_field='Merged On',inserted_on_field='Inserted On',
                  delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be 0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'

@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_date_range_with_deletion(engine_name,test_pandas):
    logger.debug('TEST DATE RANGE WITH DELETION')
    engine = engines[engine_name]
    clean_data(engine)

    data = get_data(start_date=date(2025,1,1),end_date=date(2025,7,10))
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(engine=engine, data=data,  table_name="Facts", schema='target', temp_schema='temp',
                  data_types=data_types, key=key) as merge:
        merge.exec()
    
    data = get_modified_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if not test_pandas:
        data = data.to_dict(orient='records')
    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='temp',
                  delete_mode='delete') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'
        

@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_date_range_with_delete_mark(engine_name,test_pandas):
    logger.debug('TEST DATE RANGE WITH MISSING MARK')
    engine = engines[engine_name]
    clean_data(engine)

    data = get_data(start_date=date(2025,1,1),end_date=date(2025,7,10))
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='temp', 
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if not test_pandas:
        data = data.replace({np.nan: None}).to_dict(orient='records')
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  delete_mode='mark',delete_mark_field='Deleted') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'
        deleted_count = merge.deleted_row_count

    logger.debug('Now test how missing mark is recovered')
    data = get_data(start_date=date(2025,3,1),end_date=date(2025,4,15))
    if not test_pandas:
        data = data.to_dict(orient='records')
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  delete_mode='mark',delete_mark_field='Deleted') as merge:
        merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,3,1),date(2025,4,15)))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>=deleted_count,\
            f'Incorrect row count from update {merge.updated_row_count}, should be >={deleted_count}'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be ==0'
        

@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])
def test_a_set_from_temp_with_deletion(engine_name,test_pandas):
    logger.debug('TEST A SET FROM TEMP WITH DELETION')
    engine = engines[engine_name]
    clean_data(engine)

    data = get_data(limit=10000)
    if not test_pandas:
        data = data.to_dict(orient='records')

    with dbmerge(data=data, engine=engine, table_name="Facts", schema='target', temp_schema='temp', 
                  data_types=data_types, key=key) as merge:
        merge.exec()

    data = get_modified_data(shops = ['Shop16','Shop18','Shop3'], limit=10000)
    if not test_pandas:
        data = data.replace({np.nan: None}).to_dict(orient='records')

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp',
                  delete_mode='delete') as merge:
        merge.exec(delete_condition=merge.table.c['Shop'].in_(select(merge.temp_table.c['Shop'])))
        assert merge.inserted_row_count==0, f'Incorrect row count from insert {merge.inserted_row_count}, should be ==0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count>0, f'Incorrect row count from delete {merge.deleted_row_count}, should be >0'

  
@pytest.mark.parametrize("engine_name,test_pandas", [(engine_name,test_pandas) 
                                                     for engine_name in engines for test_pandas in (True, False)])               
def test_update_from_source_table_with_delete_in_a_period(engine_name,test_pandas):
    logger.debug('TEST UPDATE FROM SOURCE TABLE WITH DELETE/UPDATE OF IN A SET')
    engine = engines[engine_name]
    clean_data(engine)

    logger.debug('Create source table')
    data = get_data()
    data['Test field']=1.1
    if not test_pandas:
        data = data.replace({np.nan: None}).to_dict(orient='records')
    
    with dbmerge(engine=engine, data=data, table_name="Facts_source", schema='source', temp_schema='temp',
                  inserted_on_field='Inserted On', key=key, data_types=data_types) as merge:
        merge.exec()
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count==0, f'Incorrect row count from update {merge.updated_row_count}, should be 0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'

    logger.debug('Now modify some date and load to Facts table')
    data = get_modified_data()

    if not test_pandas:
        data = data.replace({np.nan: None}).to_dict(orient='records')
    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='temp', 
                  key=key, data_types=data_types,
                  delete_mode='mark',merged_on_field='Merged On',inserted_on_field='Inserted On',
                  delete_mark_field='Deleted') as merge:
        merge.exec()
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count==0, f'Incorrect row count from update {merge.updated_row_count}, should be 0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'

    logger.debug('Now take data from source table in defined period')
    with dbmerge(engine=engine, source_table_name='Facts_source', temp_schema='temp', 
                  source_schema='source',
                  table_name="Facts", schema='target',
                  delete_mode='delete') as merge:
        merge.exec(source_condition=merge.source_table.c['Date'].between(date(2025,1,1),date(2025,1,15)),
                   delete_condition=merge.table.c['Date'].between(date(2025,1,1),date(2025,1,15)))
        assert merge.inserted_row_count>0, f'Incorrect row count from insert {merge.inserted_row_count}, should be >0'
        assert merge.updated_row_count>0, f'Incorrect row count from update {merge.updated_row_count}, should be >0'
        assert merge.deleted_row_count==0, f'Incorrect row count from delete {merge.deleted_row_count}, should be 0'
        


if __name__ == '__main__':

    # test_table_create_from_data_with_various_types('postgres',True)
    # test_case_sensitive_and_spaces()
    # test_table_only_key_no_other_fields()
    test_insert_to_existing_table_and_test_new_field('postgres',True)
    # test_change_data_and_mark_deleted_data() # stress test
    # test_date_range_with_deletion()
    # test_date_range_with_delete_mark()
    # test_a_set_from_temp_with_deletion()
    #test_duplicates_and_na('postgres',True)
    #test_empty_data_updates('postgres',True)
    # test_date_range_with_deletion('mariadb',True)
    #test_update_from_source_table_with_delete_in_a_period('postgres',True)