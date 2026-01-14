"""
This library is designed as a simple interface for Insert/Update/Delete operation with SQL database table.
Merge is done with optimal speed via putting your data first to temporary table and then doing data modification 
in the target table.
This module is based on SQLAlchemy library and using its abstraction layer to support multiple database engines.
DBMerge requires a non-null unique key (preferable primary key) to compare data and deside which operation is required.
"""


import uuid
import time
import pandas as pd
import numpy as np
from typing import Literal,Any
import logging
from datetime import datetime, date

from sqlalchemy import inspect, and_, or_, not_, insert, select, update, delete, exists
from sqlalchemy import Engine, Table, MetaData, Column, ColumnElement
from sqlalchemy import String, BigInteger, Numeric, Boolean, DateTime, Date, JSON, Uuid
from sqlalchemy import types, dialects, func
from alembic.migration import MigrationContext
from alembic.operations import Operations



logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class TableNotFoundError(RuntimeError):
    pass    

class NoKeyError(RuntimeError):
    pass

class IncorrectDataError(RuntimeError):
    pass

class IncorrectParameter(RuntimeError):
    pass

class TempTableAlreadyExists(RuntimeError):
    pass

# Maximum rows to check when deteting column type until non null value is found
MAX_TYPE_DETECTION_ROWS = 10000 



class dbmerge:
    def __init__(self,
                 engine: Engine, 
                 table_name: str, 
                 data: list[dict[str,Any]] | pd.DataFrame | None = None,
                 delete_mode: Literal['no', 'delete', 'mark']='no',
                 delete_mark_field: str = None,
                 merged_on_field: str | None = None,
                 inserted_on_field: str | None = None,
                 skip_update_fields: list = [],
                 key: list | None = None,
                 data_types: dict[str,types.TypeEngine] | None = None,
                 schema: str | None = None, 
                 temp_schema: str | None = None,
                 source_table_name: str | None = None, 
                 source_schema: str | None = None, 
                 can_create_table: bool = True,
                 can_create_columns: bool = True):
        """
        Init function performs preparation steps before merge.
        - Check that target table is existing and create table if it does not exist.
        - Check existing table fields and create missing fields according to given or detected data types.
        - To make effecient merge the module creates a temporary table, which will be used in exec() method.

        Preferable way to do this is to use context:
        E.g.:
            with dbmerge(data=data, engine=engine, table_name="YourTable") as merge:
                merge.exec()

        Args:
            engine (Engine): Database sqlalchemy engine. 
                Module was tested with postgres, mariadb, sqlite. It should work with most of DBs, 
                which support insert, update from select syntax and delete with where not exists syntax.
            table_name (str): Target table name. This is where the data is merged.
            data (list[dict] | pd.DataFrame | None, optional): Data to merge into the table.
                It can be list of dict e.g. [{'column1':'value1','column2':'value2'},] 
                or a pandas DataFrame.
            delete_mode (Literal['no', 'delete', 'mark'], optional): 
                Defines how to handle values, which exist in target table, 
                but does not exist in data or source table.
                - no - do nothing (default)
                - delete - delete missing rows from target table
                - mark - set deletion mark to True or 1 in the delete_mark_field.
            delete_mark_field (str, optional): Field used for setting deletion status for record.
                The field should be boolean or integer. 
                When record is missing in the data or source table, it is set to True or 1.
            merged_on_field (str | None, optional): Timestamp field name which is set to current datetime when the 
                data is inserted/updated/marked.
            inserted_on_field (str | None, optional): Timestamp field when the record was inserted. 
                This field is not changed when data is updated or marked for deletion. 
            skip_update_fields (list, optional): List of fields, that will be excluded from update. 
                These fields will be written only when data is inserted.  
            key (list | None, optional): List of key fields which will be used to compare source and target tables.
                If key is not set, then table primary key will be used (recommended).
                This field will be required if the table does not exist and it will be created automatically with this primary key.                 
            data_types (dict[str,types.TypeEngine] | None, optional): A dictionary of data types. 
                If the table or field is not existing in the database, then it will be used to assign a data type.
                If data type for new field is not given here, then the module will try to auto detect the data type.
            schema (str | None, optional): Database schema of target table. If it is None, then default schema will be used. 
                E.g. public schema for postgres database is default in this case. 
                Sqlite database does not support schamas, so this parameter will not work.
            temp_schema (str | None, optional): Database schema where temporary tabe will be created. 
            source_table_name (str | None, optional): If this parameter is set, 
                then data will be taken from other database table or view.
            source_schema (str | None, optional): Database schema of source table or view.
            can_create_table (bool, optional): If True (default), then table and view will be created automatically.
            can_create_columns (bool, optional): If True (default), then module will create missing columns in the database table.
            

        Raises:
            IncorrectParameter: Raised in several cases when arguments are not correct or missing required arguments.
            TempTableAlreadyExists: Not realistic case when temp table already exist due to hash collision with parrelel process.
        """
        

        try:
            self.data = data
            self.engine = engine
            self.table_name = table_name

            dialect_name = self.engine.dialect.name

            self.schema = schema
            self.temp_schema = temp_schema
            self.source_schema = source_schema
            self.source_table = None

            if dialect_name in ['sqlite']:
                if schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. Omitting parameter schema = "{schema}"')
                    self.schema = None

                if temp_schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. Omitting parameter temp_schema = "{temp_schema}"')
                    self.temp_schema = None

                if source_schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. Omitting parameter source_schema = "{source_schema}"')
                    self.source_schema = None
            
            self.count_data = 0
            self.inserted_row_count =0 
            self.updated_row_count = 0
            self.deleted_row_count = 0
            self.total_time = 0
            self.data_insert_time = 0
            self.insert_time = 0
            self.update_time = 0
            self.delete_time = 0
            self.insert_sql = ''
            self.update_sql = ''
            self.delete_sql = ''

            self.source_table_name = source_table_name
            
            self.skip_update_fields = skip_update_fields

            if self.schema is None:
                self.table_full_name = table_name
            else:
                self.table_full_name = self.schema+'.'+table_name

            if self.source_schema is None:
                self.source_table_full_name = source_table_name
            else:
                self.source_table_full_name = self.source_schema+'.'+source_table_name


            self.key = key

            if data_types is None:
                self.given_data_types={}
            else:   
                self.given_data_types = data_types

            self.table = None
            self.data_fields = {}
            self.new_fields = {}

            self.conn = engine.connect()
            self.inspector = inspect(self.engine)
            self.metadata = MetaData()

            self.delete_mode = delete_mode

            self.delete_mark_field = delete_mark_field
            self.merged_on_field = merged_on_field
            self.inserted_on_field = inserted_on_field
            
            self.special_fields = [f for f in [self.delete_mark_field,self.merged_on_field,self.inserted_on_field]
                                   if f is not None]

            if self.delete_mode=='mark': 
                if self.delete_mark_field is None:
                    raise IncorrectParameter(f"delete_mode='mark', but delete_mark_field is not set.")
                

            self.max_type_detection_rows = MAX_TYPE_DETECTION_ROWS

            self.unique_id=str(uuid.uuid4().hex[:8])

            self.table = self._load_table_metadata_from_db(self.table_name,self.schema)
            


            if self.source_table_name is not None:
                self.type_of_data = 'table'
                if self.source_table_full_name==self.table_full_name:
                    raise IncorrectParameter(f'Source table "{self.source_table_full_name}" can not be same as target table')

                self.source_table = self._load_table_metadata_from_db(self.source_table_name,
                                                                      self.source_schema)
                self.count_data = 0
                if self.source_table is None:
                    raise IncorrectParameter(f'Table "{self.source_table_full_name}" not found in the database')
                self._get_fields_from_source_table()

            elif isinstance(self.data,list):
                self.type_of_data = 'list of dict'
                self.count_data = len(self.data)
                if self.count_data==0:
                    raise IncorrectDataError(f'Input list is empty.')
                self._get_fields_from_list_of_dict()               

            elif isinstance(self.data,pd.DataFrame):
                self.type_of_data = 'pandas'
                self.count_data = len(self.data)
                if self.count_data==0:
                    logger.warning('No data, empty dataframe')
                self._get_fields_from_pandas()

            else:
                raise IncorrectDataError(f'Input "data" should be pandas DataFrame or list of dict')

            self._check_existing_and_new_fields()
            self._check_key()

            if self.table is None:
                if can_create_table:
                    logger.info(f'Table "{self.table_full_name}" does not exist. Creating.')
                    self._check_given_types()    
                    if self.type_of_data in ['list of dict','pandas']: #data types from source table are already known
                        self._detect_delete_data_types()
                    self._create_table()
                else:
                    raise TableNotFoundError("Table not found {self.table_full_name} and can_create_table=False")
            else:
                if len(self.new_fields)>0:
                    if can_create_columns:
                        self._check_given_types()
                        if self.type_of_data in ['list of dict','pandas']:
                            self._detect_delete_data_types()
                        self._create_new_fields()
                    else:
                        self._remove_new_fields()
                
            self._create_temp_table()


        except:
            if hasattr(self, 'conn'):
                self.conn.rollback()
            raise



    def exec(self, delete_condition: ColumnElement=None, source_condition: ColumnElement=None,
             commit_all_steps=True, chunk_size: int = 10000):
        """
        This method executes the merge operation, which contains the following steps:
        1) Insert source data to temporary table.
        Further steps are done base on data in the temp table to merge data to the target table (table). 
        2) Insert rows, missing in the target table.
        3) Update rows, which exist in target table and which have different values (fields are compared).
        4) Delete or mark as deleted (update deletion mark) for the fields, which dont exist in source data

        If your data comes in portions then you can set a delete_condition argument to define your portion of data.
        E.g. if you load monthly data you can call the method like this:
            with dbmerge(data=data, engine=engine, table_name="YourTable",delete_mode='delete') as merge:
                merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,1,1),date(2025,1,31)))

        Args:
            delete_condition (ColumnElement, optional): 
                If missing mode is 'delete' or 'mark', then you can set a condition to filter the target table. 
                It should be an SQL alchemy binary exporession, which will be used in the where condition of delete or mark deleted.
            source_condition (ColumnElement, optional): If the data is loaded from source table or view, 
                then you can set this parameter to use in the where() statement when selecting the data 
                and inserting to temp table. 
            commit_all_steps (bool, optional): If set to True (default), then all steps will be commited 
                (insert to temp, insert to target, update, delete).     
                If False, then commit will be done only after all is finished.
            chunk_size (int, optional): When data (from list or dataframe) is inserted to temp table, 
                it will be split in chunks. Defaults to 10000 rows per chunk.

        """
        
        if delete_condition is not None and not isinstance(delete_condition,ColumnElement):
            raise IncorrectParameter('delete_condition argument should be sqlalchemy logical expression (ColumnElement type)')
        self.delete_condition = delete_condition

        if source_condition is not None and not isinstance(source_condition,ColumnElement):
            raise IncorrectParameter('source_condition argument should be sqlalchemy logical expression (ColumnElement type)')
        self.source_condition = source_condition

        self.chunk_size = chunk_size
        
        try:
            
            if self.source_table_name is None:
                self._insert_data_to_temp()
            else:
                self._insert_source_table_to_temp()
            data_msg = f'Temp data: {self.count_data} rows ({format_ms(self.data_insert_time)})'
            if commit_all_steps:
                self.conn.commit()
            
            
            self._update_not_matching_data()
            updated_msg = f'Updated: {self.updated_row_count} rows ({format_ms(self.update_time)})'
            if commit_all_steps:
                self.conn.commit()

            self._insert_missing_data()
            inserted_msg = f'Inserted: {self.inserted_row_count} rows ({format_ms(self.insert_time)})'
            if commit_all_steps:
                self.conn.commit()

            if self.delete_mode=='delete':
                self._delete_rows_missing_in_source()
                delete_msg = f'Deleted: {self.deleted_row_count} rows ({format_ms(self.delete_time)})'

            elif self.delete_mode=='mark':
                self._mark_rows_missing_in_source()
                delete_msg = f'Marked deleted: {self.deleted_row_count} rows ({format_ms(self.delete_time)})'

            else:                  #self.delete_mode=='no':
                delete_msg = 'Deleted: no'
                self.delete_time=0


            self.conn.commit()           

            self.total_time = self.data_insert_time+self.insert_time+\
                              self.update_time+self.delete_time
            
            logger.info(f'Merged data into table "{self.table_full_name}". '+\
                        ', '.join([data_msg,inserted_msg,updated_msg,delete_msg])+', '+\
                        f'Total time: {format_ms(self.total_time)}')
        
        except:
            if hasattr(self, 'conn'):
                self.conn.rollback()
            raise

        finally:
            self._drop_temp_table()
            self.conn.close()



    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._drop_temp_table()
        self.conn.close()

    def __del__(self):
        self._drop_temp_table()
        self.conn.close()


    def _get_fields_from_source_table(self):
        self.data_fields = {c.name:c.type for c in self.source_table.c if c.name not in self.special_fields}

    def _get_fields_from_list_of_dict(self):
        test_row = self.data[0]
        self.type_of_data = 'list of dict'
        if isinstance(test_row,dict):
            self.data_fields = {c:None for c in test_row if c not in self.special_fields}
        else:
            raise IncorrectDataError(f'Input "data" is list, but no dict inside')

    def _get_fields_from_pandas(self):
        self.data_fields = {c:None for c in self.data.columns if c not in self.special_fields}


    def _check_type_is_supported(self,field_type):
        if isinstance(field_type,JSON) and self.engine.dialect.name == 'postgresql':
            if not isinstance(field_type,dialects.postgresql.JSONB):
                raise IncorrectDataError(f'JSON type is not supported for postgres. '+\
                                        'Use JSONB instead (sqlalchemy.dialects.postgresql.JSONB)')

    def _check_existing_and_new_fields(self):
        if self.table is not None:
            existing_fields = {c.name:c.type for c in self.table.c}
        else:
            existing_fields = {}

        for f in self.data_fields:
            if f in existing_fields:
                self.data_fields[f]=existing_fields[f]
                self._check_type_is_supported(existing_fields[f])
            else:
                self.new_fields[f]=self.data_fields[f]

        delete_mark_field = self.delete_mark_field
        if delete_mark_field is not None and delete_mark_field not in existing_fields:
            self.new_fields[delete_mark_field]=Boolean()

        merged_on_field = self.merged_on_field
        if merged_on_field is not None and merged_on_field not in existing_fields:
            self.new_fields[merged_on_field]=DateTime()

        inserted_on_field = self.inserted_on_field
        if inserted_on_field is not None and inserted_on_field not in existing_fields:
            self.new_fields[inserted_on_field]=DateTime()        


    def _check_given_types(self):

        for f in self.new_fields:
            if f in self.given_data_types:
                given_type = self.given_data_types[f]
                if isinstance(given_type, types.TypeEngine):
                    self.new_fields[f]=given_type
                    self.data_fields[f]=given_type
                    self._check_type_is_supported(given_type)
                else:
                    raise IncorrectDataError(f'Incorrect type {given_type} given for field {f}. '+\
                                              'Should be sqlalchemy data type')

    def _detect_delete_data_types(self):

        not_given_data_types = [f for f in self.new_fields if self.new_fields[f] is None]

        if self.type_of_data == 'list of dict':
            test_data = self.data[:self.max_type_detection_rows]
        elif self.type_of_data == 'pandas':
            test_data = self.data.loc[:self.max_type_detection_rows].to_dict(orient='records')
        else:
            return

        for i,test_row in enumerate(test_data):
            
            if isinstance(test_row,dict):
                
                detection_failed = False                   
                
                for f in not_given_data_types:
                    #Check if this field was already detected
                    if self.new_fields.get(f) is None:
                        if isinstance(test_row[f],int):
                            self.new_fields[f] = BigInteger()
                        elif isinstance(test_row[f],float):
                            self.new_fields[f] = Numeric()
                        elif isinstance(test_row[f],str):
                            self.new_fields[f] = String()
                        elif isinstance(test_row[f],bool):
                            self.new_fields[f] = Boolean()
                        elif isinstance(test_row[f],date):
                            self.new_fields[f] = Date()
                        elif isinstance(test_row[f],datetime):
                            if test_row[f].tzinfo is None:
                                self.new_fields[f] = DateTime()
                            else:
                                self.new_fields[f] = DateTime(timezone=True)
                        elif isinstance(test_row[f],list) or isinstance(test_row[f],dict) or \
                            isinstance(test_row[f],tuple):
                            if self.engine.dialect.name == 'postgresql':
                                self.new_fields[f] = dialects.postgresql.JSONB()
                            else:
                                self.new_fields[f] = JSON()

                        elif isinstance(test_row[f],uuid.UUID):
                            self.new_fields[f] = Uuid()
                        else:
                            detection_failed = True
                            self.new_fields[f] = None

                if not detection_failed:
                    break

            else:
                raise IncorrectDataError(f'Incorrect Data Format. '+\
                                        f'Expected list of dict, but list of {type(test_row)} detected')

        for f in not_given_data_types:
            detected_type = self.new_fields[f]
            if isinstance(detected_type, types.TypeEngine):
                self.data_fields[f] = detected_type
            else:
                raise IncorrectDataError(f'Could not detect data type for column {f}')


    def _remove_new_fields(self):

        new_data_fields={}
        for f in self.data_fields:
            if f not in self.new_fields:
                new_data_fields[f]=self.data_fields[f]
            else:
                logger.warning(f'Skipping field "{f}", because it does not exist in table "{self.table_full_name}"')

        for f in self.special_fields:
            if f in self.new_fields:
                raise IncorrectParameter(f'Field "{f}", is required, but does not exist in table "{self.table_full_name}""')

        self.data_fields = new_data_fields



    def _create_new_fields(self):

        if len(self.new_fields)>0:
            op=Operations(MigrationContext.configure(self.conn))
            for field_name in self.new_fields:
                logger.info(f'Creating new field "{field_name}" - {self.new_fields[field_name]}')       
                primary_key = field_name in self.key
                op.add_column(
                        self.table_name,
                        Column(field_name, self.new_fields[field_name], primary_key=primary_key),
                        schema=self.schema
                    )
            self.conn.commit()
            self.table = self._load_table_metadata_from_db(self.table_name,self.schema)
               

    def _check_key(self):
        if self.key is None or len(self.key)==0:
            if self.table is not None:
                self.key = [col.name for col in self.table.primary_key.columns]

        if self.key is None or len(self.key)==0:
            raise NoKeyError("No primary key: provide 'key' argument or set primary key in DB")
        else:
            for c in self.key:
                if c not in self.data_fields:
                    raise NoKeyError(f'Key field "{c}" not found in data')


    def _insert_missing_data(self):
        
        start_time = time.perf_counter()
        first_pk_col = self.table.c[self.key[0]]
        
        source_fields = [self.temp_table.c[f] for f in self.data_fields]
        target_fields = [self.table.c[f] for f in self.data_fields]

        if self.merged_on_field is not None:
            source_fields.append(func.now().label(self.merged_on_field))
            target_fields.append(self.table.c[self.merged_on_field])

        if self.inserted_on_field is not None:
            source_fields.append(func.now().label(self.inserted_on_field))
            target_fields.append(self.table.c[self.inserted_on_field])

        join_conditions = []
        for key_col in self.key:
            join_conditions.append(self.table.c[key_col]==self.temp_table.c[key_col])
        on_clause = and_(*join_conditions)

        select_stmt = select(*source_fields).join(self.table, on_clause, isouter=True).where(first_pk_col.is_(None))  

        #alternative version of select, which does not work on postgres
        #select_stmt = select(*source_fields).where(not_(exists(on_clause))) 

        insert_stmt = insert(self.table).from_select(target_fields, select_stmt) #.returning(*pk_cols)

        self.insert_sql = str(insert_stmt)

        result = self.conn.execute(insert_stmt)
        self.inserted_row_count = result.rowcount #if use returning, then rowcount will not work.

        end_time = time.perf_counter()
        self.insert_time = end_time - start_time  


    def _delete_rows_missing_in_source(self):

        start_time = time.perf_counter()
        delete_join_conditions = []
        for c in self.key:
            delete_join_conditions.append(self.table.c[c]==self.temp_table.c[c])
        delete_where_clause = and_(*delete_join_conditions)
        
        if self.delete_condition is None:
            delete_stmt = delete(self.table).where(not_(exists().where(delete_where_clause)))
        else:
            delete_stmt = delete(self.table).where(and_(self.delete_condition,
                                                        not_(exists().where(delete_where_clause))))
                
        self.delete_sql = str(delete_stmt)

        result = self.conn.execute(delete_stmt)
        self.deleted_row_count = result.rowcount

        end_time = time.perf_counter()
        self.delete_time = end_time - start_time  


    def _mark_rows_missing_in_source(self):

        start_time = time.perf_counter()
        update_join_conditions = []
        for c in self.key:
            update_join_conditions.append(self.table.c[c]==self.temp_table.c[c])
        update_where_clause = and_(*update_join_conditions)

        update_values = {}
        mark_field = self.table.c[self.delete_mark_field]
        update_values[mark_field] = 1

        if self.merged_on_field is not None:
            merged_on_field = self.table.c[self.merged_on_field]
            update_values[merged_on_field]=func.now()

        if self.delete_condition is None:
            update_stmt = update(self.table).values(update_values).where(not_(exists().where(update_where_clause)))
        else:
            update_stmt = update(self.table).values(update_values).\
                                where(and_(self.delete_condition,
                                           not_(exists().where(update_where_clause)) ))
        
        self.delete_sql = str(update_stmt)
        
        result = self.conn.execute(update_stmt)
        self.deleted_row_count = result.rowcount

        end_time = time.perf_counter()
        self.delete_time = end_time - start_time  


    def _update_not_matching_data(self):
        
        start_time = time.perf_counter()
        non_key_cols = [c for c in self.data_fields if c not in self.key and 
                                                       c not in self.skip_update_fields]

        if len(non_key_cols)==0:
            # nothing to update
            self.updated_row_count = 0
            self.update_time = 0
            return

        join_conditions = []
        for c in self.key:
            join_conditions.append(self.table.c[c]==self.temp_table.c[c])
        on_clause = and_(*join_conditions)

        where_conditions = []
        for c in non_key_cols:
            col = self.table.c[c]
            temp_col = self.temp_table.c[c]
            where_conditions.append(col.is_distinct_from(temp_col))

        if self.delete_mark_field is not None:
            mark_field = self.table.c[self.delete_mark_field]
            where_conditions.append(mark_field.is_not(None))

        where_clause = or_(*where_conditions)        

        select_stmt = select(self.temp_table).join(self.table, on_clause, isouter=False).where(where_clause)
        select_stmt = select_stmt.subquery()

        update_values = {}
        for c in non_key_cols:
            update_values[self.table.c[c]] = select_stmt.c[c]

        if self.delete_mark_field is not None:
            mark_field = self.table.c[self.delete_mark_field]
            update_values[mark_field]=None

        if self.merged_on_field is not None:
            merged_on_field = self.table.c[self.merged_on_field]
            update_values[merged_on_field]=func.now()

        update_join_conditions = []
        for c in self.key:
            update_join_conditions.append(self.table.c[c]==select_stmt.c[c])
        update_where_clause = and_(*update_join_conditions)


        update_stmt = update(self.table).values(update_values).where(update_where_clause)

        self.update_sql = str(update_stmt)

        result = self.conn.execute(update_stmt)

        self.updated_row_count = result.rowcount
        
        end_time = time.perf_counter()
        self.update_time = end_time - start_time  


    def _load_table_metadata_from_db(self,table_name,schema):
        table_exists = self.inspector.has_table(table_name, schema)
        if table_exists:
            table = Table(table_name, self.metadata, autoload_with=self.engine, schema=schema,
                               extend_existing=True)
            return table

            
    
    def _create_table(self):
        cols = [Column(c, self.data_fields[c], primary_key = c in self.key) for c in self.data_fields]

        special_cols = [Column(c, self.new_fields[c]) for c in self.new_fields if c not in self.data_fields]

        all_cols = cols+special_cols

        for f in all_cols:
            key = f in self.key
            if key:
                logger.info(f'Table field "{f.name}" - {f.type}, primary key')
            else:
                logger.info(f'Table field "{f.name}" - {f.type}')

        self.table = Table(self.table_name, self.metadata, *all_cols, schema = self.schema)
        self.table.create(self.engine, checkfirst=True)
        self.conn.commit() 

 
    def _create_temp_table(self):
        
        temp_table_name = self.table_name + '_' + self.unique_id
        table_exists = self.inspector.has_table(temp_table_name, self.temp_schema)
        if table_exists:
            raise TempTableAlreadyExists(f'Temp table "{temp_table_name}" already exists in schema "{self.temp_schema}"')

        cols = [Column(c.name, c.type, primary_key = c.name in self.key) for c in self.table.c]

        self.temp_table = Table(temp_table_name, self.metadata, *cols, schema = self.temp_schema)
        self.temp_table.create(self.engine, checkfirst=True)
        self.conn.commit()        
 

    def _insert_data_to_temp(self):

        start_time = time.perf_counter()
        
        chunks_num = (self.count_data // self.chunk_size)
        if self.count_data % self.chunk_size > 0:
            chunks_num = chunks_num + 1
        
        if self.count_data>0:
            for i in range(chunks_num):
                begin = i * self.chunk_size
                end = min((i + 1) * self.chunk_size, self.count_data)
                if self.type_of_data == 'list of dict':
                    data_slice = self.data[begin:end]
                elif self.type_of_data == 'pandas':
                    data_slice = self.data.loc[begin:end-1]
                    if self.engine.dialect.name in ('mysql','mariadb'):
                        data_slice = data_slice.replace({np.nan: None})    
                    data_slice = data_slice.to_dict(orient='records')
                else:
                    return
                
                result = self.conn.execute(insert(self.temp_table), data_slice)
        
        end_time = time.perf_counter()
        self.data_insert_time = end_time - start_time       
        
    def _insert_source_table_to_temp(self):

        start_time = time.perf_counter()

        source_fields = [self.source_table.c[f] for f in self.data_fields]
        target_fields = [self.temp_table.c[f] for f in self.data_fields]

        if self.source_condition is None:
            select_stmt = select(*source_fields)
        else:
            select_stmt = select(*source_fields).where(self.source_condition)

        insert_stmt = insert(self.temp_table).from_select(target_fields,select_stmt)
        result = self.conn.execute(insert_stmt)
        self.count_data = result.rowcount 

        end_time = time.perf_counter()
        self.data_insert_time = end_time - start_time  


    def _drop_temp_table(self):
        if hasattr(self, 'temp_table') and self.temp_table is not None:
            self.temp_table.drop(self.engine, checkfirst=True)
            self.temp_table = None


def drop_table_if_exists(engine,table_name,schema=None):

    """
    Additional routine to drop table in DB if it exists. Use carefully.
    """

    if schema is not None and engine.dialect.name=='sqlite':
        logger.warning('sqlite engine does not support schemas. Omitting parameter schema = "{schema}"')
        schema=None

    if schema is None:
        table_full_name = table_name
    else:
        table_full_name = schema+'.'+table_name

    inspector = inspect(engine)
    metadata = MetaData()
    table_exists = inspector.has_table(table_name, schema)
    if table_exists:
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        logger.debug(f'Deleting table "{table_full_name}"')
        table.drop(engine, checkfirst=True)

  
def format_ms(seconds):

    milliseconds = round(seconds * 1000)
    if milliseconds < 0:
        return "-"
    
    seconds, ms = divmod(milliseconds, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    if ms and milliseconds<1000:  
        parts.append(f"{round(ms)}ms")
    
    return " ".join(parts[:2])