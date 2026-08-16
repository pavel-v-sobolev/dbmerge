"""
This library is designed as a simple interface for Insert/Update/Delete operation with SQL database table.
Merge is done with optimal speed via putting your data first to temporary table and then doing data modification 
in the target table.
This module is based on SQLAlchemy library and using its abstraction layer to support multiple database engines.
DBMerge requires a non-null unique key (preferable primary key) to compare data and decide which operation is required.
"""

from __future__ import annotations
from typing import Literal, Any, TYPE_CHECKING

import sys
import uuid
import time

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame

import logging
from datetime import datetime, date
from dataclasses import dataclass, field

from sqlalchemy import inspect, and_, or_, not_, insert, select, update, delete, exists, literal, literal_column
from sqlalchemy import Engine, Table, MetaData, Column, ColumnElement
from sqlalchemy import String, Text, Integer, BigInteger, Numeric, Double, Boolean, DateTime, Date, Time, JSON, Uuid, LargeBinary
from sqlalchemy import types, dialects, func, text, schema

POLARS_TO_SQLALCHEMY_TYPE_MAP = {
    'Int8': BigInteger(),
    'Int16': BigInteger(),
    'Int32': BigInteger(),
    'Int64': BigInteger(),
    'Int128': BigInteger(),
    'UInt8': BigInteger(),
    'UInt16': BigInteger(),
    'UInt32': BigInteger(),
    'UInt64': BigInteger(),
    'UInt128': BigInteger(),
    'Float16': Numeric(),
    'Float32': Numeric(),
    'Float64': Numeric(),
    'Decimal': Numeric(),
    'Boolean': Boolean(),
    'Utf8': String(),
    'String': String(),
    'Categorical': String(),
    'Categories': String(),
    'Enum': String(),
    'Binary': LargeBinary(),
    'Date': Date(),
    'Datetime': DateTime(),
    'Time': Time(),
    'Duration': String(),
    'List': JSON(),
    'Struct': JSON(),
    'Array': JSON(),
    'Object': JSON(),
    'Null': String(),
    'Unknown': String(),
    'Extension': JSON(),
}


def _is_pandas_dataframe(data) -> bool:
    # If pandas was never imported by the calling process, "data" cannot be a pandas DataFrame,
    # so we only look up sys.modules and never import pandas ourselves.
    pd = sys.modules.get('pandas')
    return pd is not None and isinstance(data, pd.DataFrame)

def _is_polars_dataframe(data) -> bool:
    pl = sys.modules.get('polars')
    return pl is not None and isinstance(data, pl.DataFrame)

def _is_string_without_length(field_type) -> bool:
    # Text and its dialect variants (TEXT, LONGTEXT, ...) are lengthless by nature rather than by
    # omission, so they are not treated as a string whose length is still unknown.
    return isinstance(field_type, String) and not isinstance(field_type, Text) \
           and field_type.length is None

def _is_numeric_without_precision(field_type) -> bool:
    # Integer, BigInteger and Boolean are not Numeric subclasses, so only the decimal/float
    # family is matched here.
    return isinstance(field_type, Numeric) and field_type.precision is None


logger = logging.getLogger('dbmerge')

def _ensure_logger_handler():
    # If the user has not set up logging, then we set up a default logger to stdout with INFO level. 
    # If the user has already set up logging, we do nothing and let the user's configuration work.
    # This function is called from dbmerge class __init__.
    if not logger.hasHandlers():
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)


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

# Maximum rows to check when detecting column type until non null value is found
MAX_TYPE_DETECTION_ROWS = 10000 

# Maximum length of postgres table name is 63, postgres might need some symbols for "_pkey" suffix.
MAX_TEMP_TABLE_NAME_LEN = 58

# Dialects that require JSONB (not plain JSON) so values can be compared with IS DISTINCT FROM.
# CockroachDB speaks the postgres wire protocol and natively supports JSONB.
JSONB_DIALECTS = ('postgresql', 'cockroachdb')

# Dialects that need generic types adapted before a column can be created from them.
MYSQL_DIALECTS = ('mysql', 'mariadb')

# Dialects that resolve a NUMERIC with no precision to a whole-number decimal:
# DECIMAL(10,0) on MySQL/MariaDB and NUMERIC(18,0) on MS SQL. Both round every value.
ROUNDING_NUMERIC_DIALECTS = ('mysql', 'mariadb', 'mssql')

# InnoDB indexes a key of at most this many bytes, shared by all columns of the key.
# With utf8mb4 a character takes up to 4 bytes, so a single-column key fits VARCHAR(768).
MYSQL_MAX_KEY_BYTES = 3072

@dataclass
class mergeResult:
    total_row_count: int
    inserted_row_count: int 
    updated_row_count: int 
    deleted_row_count: int 
    total_time: float
    temp_insert_time: float
    insert_time: float
    update_time: float
    delete_time: float
    # Schema changes this merge made to the target table. A downstream consumer that keeps a
    # derived dataset (a mart built incrementally from a "merged on" watermark) can not notice a
    # new column on its own: the watermark only moves on rows whose values changed, and adding a
    # column changes no values. These two fields are that missing signal.
    table_created: bool = False
    added_fields: dict[str, types.TypeEngine] = field(default_factory=dict)


class dbmerge:
    def __init__(self,
                 engine: Engine, 
                 table_name: str, 
                 data: list[dict[str,Any]] | dict[str,list] | PandasDataFrame | PolarsDataFrame | None = None,
                 delete_mode: Literal['no', 'delete', 'mark']='no',
                 delete_mark_field: str | None = None,
                 delete_mark_values: dict[str,Any] | None = None,
                 merged_on_field: str | None = None,
                 inserted_on_field: str | None = None,
                 skip_update_fields: list | None = None,
                 skip_compare_fields: list | None = None,
                 key: list | None = None,
                 data_types: dict[str,types.TypeEngine] | None = None,
                 schema: str | None = None, 
                 temp_schema: str | None = None,
                 source_table_name: str | None = None, 
                 source_schema: str | None = None, 
                 can_create_table: bool = True,
                 can_create_columns: bool = True,
                 can_create_schemas: bool = True) -> None:
        """
        Init function prepares the database and internal structures before the merge operation.
        
        Key Preparation Steps:
        - Verifies the existence of the target table and creates it if it does not exist.
        - Inspects existing table fields and automatically creates missing columns based on the provided or auto-detected data types.
        - Creates a temporary staging table to ensure the merge operation executes with optimal performance. The temporary table is safely dropped upon exiting the context.

        Preferable usage (Context Manager):
        ```python
        with dbmerge(engine=engine, data=data, table_name="YourTable") as merge:
            result = merge.exec()
        ```

        Args:
            engine (Engine): The SQLAlchemy engine connected to your database. Tested with PostgreSQL, MariaDB/MySQL, SQLite, MS SQL and CockroachDB.
            table_name (str): The name of the target table where data will be merged.
            data (list[dict] | dict[str,list] | pd.DataFrame | pl.DataFrame | None, optional): The source data to merge. Accepts a list of dictionaries (e.g. [{'col1': 'val1'}, ...]), a dict of lists (e.g. {'col1': ['val1', ...], ...}) or a Pandas/Polars DataFrame.
                For a list of dictionaries, the set of columns is taken from the first row: all rows must have the same keys, keys that appear only in later rows are ignored.
            delete_mode (Literal['no', 'delete', 'mark'], optional): Defines how to handle records that exist in the target table but are missing from the source data.
                - 'no' (default): Retain existing target rows.
                - 'delete': Hard delete rows from the target table.
                - 'mark': Soft delete rows by setting a flag in `delete_mark_field`.
                Warning: with 'delete' or 'mark', every target row missing from the source is affected. If the source covers only part
                of the table (or is empty), restrict the scope with the delete_condition argument of exec(), otherwise
                an empty source with delete_mode='delete' wipes the entire table.
            delete_mark_field (str, optional): The column used to flag a record as deleted. Must be a Boolean or Integer column.
                A row missing from the source is set to True/1; inserted or resurrected (reappeared) rows are set to False/0.
                If this column is present in the incoming data, the supplied value is used as-is.
                It must be a column of its own: it can not be part of "key", nor the same column as
                merged_on_field or inserted_on_field.
            delete_mark_values (dict[str, Any] | None, optional): Extra columns to write when a row is marked as deleted,
                as {column name: value}. Requires delete_mode='mark'. Without it, a marked row keeps the values of the
                last load that still contained it. The columns must exist in the target table, can not be part of
                "key", and can not be the automatically managed delete_mark_field/merged_on_field/inserted_on_field.
            merged_on_field (str | None, optional): Timestamp column automatically updated to current datetime when a row is inserted, updated, or marked.
                This column is always managed automatically: if present in the incoming data, the supplied values are ignored.
            inserted_on_field (str | None, optional): Timestamp column automatically set to current datetime when a new row is initially inserted.
                This column is always managed automatically: if present in the incoming data, the supplied values are ignored.
            skip_update_fields (list, optional): List of column names to exclude from the UPDATE operation: they are
                written by the initial INSERT and never touched again. Such a column is also never compared, so a
                difference in it alone does not update the row - a column that is never written can not be a reason
                to update it.
            skip_compare_fields (list, optional): List of column names that are written, but never compared. The row is
                updated only when some other column differs; then these columns are written together with the rest.
                Use it for a column that gets a new value on every load (a load id, an import timestamp) and 
                would otherwise make every row look modified.
            key (list | None, optional): List of column names serving as the unique key to compare source and target tables. If omitted, uses the target table's Primary Key.
            data_types (dict[str, types.TypeEngine] | None, optional): Dictionary mapping column names to SQLAlchemy data types. Used when creating missing tables or columns.
                A type given here is authoritative: it is used as written and never substituted, even if the engine then
                refuses to create a column from it. Auto-detected types are adapted to the target engine instead, where
                the generic type would not work (on MySQL/MariaDB a string of unknown length becomes LONGTEXT; on
                MySQL/MariaDB/MS SQL a number of unknown precision becomes a double and a datetime keeps microseconds).
                On MySQL/MariaDB a string column of the merge key must be given an explicit length here, because InnoDB
                indexes at most 3072 bytes per key, shared by all of its columns.
            schema (str | None, optional): The database schema of the target table.
            temp_schema (str | None, optional): The database schema where the temporary staging table will be created.
            source_table_name (str | None, optional): If provided, data will be sourced directly from another existing database table or view. Mutually exclusive with the "data" argument (passing both raises IncorrectParameter).
            source_schema (str | None, optional): The database schema of the source table or view.
            can_create_table (bool, optional): Allows the module to automatically create the target table if it does not exist (default is True).
            can_create_columns (bool, optional): Allows the module to append missing columns to the target table (default is True).
            can_create_schemas (bool, optional): Allows the module to automatically create target and temp schema if they don't exist (default is True).

        Raises:
            IncorrectParameter: Raised when arguments are not correct, missing, or conflict with each other.
            NoKeyError: Raised when no usable merge key can be determined, or when a key column is also used for another role.
            IncorrectDataError: Raised when the input data has an unsupported shape or a type that can not be resolved.
            TableNotFoundError: Raised when the target table does not exist and can_create_table=False.
            TempTableAlreadyExists: Raised if a temporary table with the generated name already exists (a very unlikely case, since the name includes a random unique id).

        All of these subclass RuntimeError. They live in dbmerge.dbmerge and are not re-exported from
        the package root, so import them as: from dbmerge.dbmerge import IncorrectParameter
        """
        

        try:
            _ensure_logger_handler()

            self._validate_init_params(engine=engine, table_name=table_name, data=data,
                                       delete_mode=delete_mode, delete_mark_field=delete_mark_field,
                                       delete_mark_values=delete_mark_values,
                                       merged_on_field=merged_on_field, inserted_on_field=inserted_on_field,
                                       skip_update_fields=skip_update_fields,
                                       skip_compare_fields=skip_compare_fields, key=key, data_types=data_types,
                                       schema=schema, temp_schema=temp_schema,
                                       source_table_name=source_table_name, source_schema=source_schema,
                                       can_create_table=can_create_table, can_create_columns=can_create_columns,
                                       can_create_schemas=can_create_schemas)

            self.data = data
            self.engine = engine
            self.table_name = table_name
            self.merge_finished = False
            dialect_name = self.engine.dialect.name

            self.schema = schema
            self.temp_schema = temp_schema

            self.source_table_name = source_table_name
            self.source_schema = source_schema
            self.source_table = None
            self.can_create_columns = can_create_columns
            self.can_create_table = can_create_table
            self.can_create_schemas = can_create_schemas

            self.inspector = inspect(self.engine)
            self.metadata = MetaData()

            if dialect_name in ('mysql','mariadb'):
                if self.schema is None:
                    raise IncorrectParameter(f"""MariaDB/MySQL require "schema" argument to be set to your database name.""")
                if self.temp_schema is None:
                    self.temp_schema = self.schema
                if self.source_schema is None and self.source_table_name is not None:
                    raise IncorrectParameter(f"""MariaDB/MySQL require "source_schema" argument to be set 
                                             to your database name, corresponding to your "source_table".""")

            if dialect_name in ['sqlite']:
                if schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. '
                                   f'Omitting parameter schema = "{schema}"')
                    self.schema = None

                if temp_schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. '
                                   f'Omitting parameter temp_schema = "{temp_schema}"')
                    self.temp_schema = None

                if source_schema is not None:
                    logger.warning(f'"{dialect_name}" engine does not support schemas. '
                                   f'Omitting parameter source_schema = "{source_schema}"')
                    self.source_schema = None
            
            self.total_row_count = 0
            self.inserted_row_count =0 
            self.updated_row_count = 0
            self.deleted_row_count = 0
            self.total_time = 0
            self.temp_insert_time = 0
            self.insert_time = 0
            self.update_time = 0
            self.delete_time = 0
            self.insert_sql = ''
            self.update_sql = ''
            self.delete_sql = ''
            
            self.skip_update_fields = skip_update_fields if skip_update_fields is not None else []
            self.skip_compare_fields = skip_compare_fields if skip_compare_fields is not None else []

            self.conn = engine.connect()

            if self.schema is not None:
                self.table_full_name = self.schema+'.'+table_name
                self._create_schema_if_not_exists(self.schema)
            else:
                self.table_full_name = table_name

            if self.source_schema is not None:
                self.source_table_full_name = self.source_schema+'.'+source_table_name                
            else:
                self.source_table_full_name = source_table_name

            if self.temp_schema is not None:
                self._create_schema_if_not_exists(self.temp_schema)

            self.key = key

            if data_types is None:
                self.given_data_types={}
            else:   
                self.given_data_types = data_types

            self.table = None
            self.data_fields = {}
            self.new_fields = {}

            # Reported schema changes (see mergeResult). Both are filled by the fact of the DDL,
            # not by what the incoming data asked for: with can_create_columns=False the missing
            # columns are dropped from the merge instead of being created, and nothing is reported.
            self.table_created = False
            self.added_fields = {}

            self.delete_mode = delete_mode

            self.delete_mark_field = delete_mark_field
            self.delete_mark_values = delete_mark_values if delete_mark_values is not None else {}
            self.merged_on_field = merged_on_field
            self.inserted_on_field = inserted_on_field
            
            self.special_fields = [f for f in [self.merged_on_field,self.inserted_on_field]
                                   if f is not None]

            if self.delete_mode=='mark':
                if self.delete_mark_field is None:
                    raise IncorrectParameter(f"delete_mode='mark', but delete_mark_field is not set.")
            elif self.delete_mark_values:
                # Nothing gets marked outside 'mark' mode, so the values would be silently ignored.
                raise IncorrectParameter(f"delete_mark_values is set, but delete_mode is '{self.delete_mode}'. "
                                         f"It only applies to rows marked as deleted (delete_mode='mark').")


            self.max_type_detection_rows = MAX_TYPE_DETECTION_ROWS

            self.unique_id=str(uuid.uuid4().hex[:8])

            self.table = self._load_table_metadata_from_db(self.table_name,self.schema)
            


            if self.source_table_name is not None:
                self.type_of_data = 'table'
                if self.source_table_full_name==self.table_full_name:
                    raise IncorrectParameter(f'Source table "{self.source_table_full_name}" can not be same as target table')

                self.source_table = self._load_table_metadata_from_db(self.source_table_name,
                                                                      self.source_schema)
                self.total_row_count = 0
                if self.source_table is None:
                    raise IncorrectParameter(f'Table "{self.source_table_full_name}" not found in the database')
                self._get_fields_from_source_table()

            # LIST OF DICT
            elif isinstance(self.data,list):
                self.type_of_data = 'list of dict'
                self.total_row_count = len(self.data)
                if self.total_row_count==0:
                    if self.table is None:
                        raise IncorrectDataError(f'Input list is empty and table "{self.table_full_name}" does not exist.')
                    else:
                        logger.warning('Input list is empty.')
                        self._get_fields_from_table()
                else:
                    self._get_fields_from_list_of_dict() 

            # DICT OF LIST
            elif isinstance(self.data,dict):
                self.type_of_data = 'dict of list'
                list_length = None
                
                if len(self.data.keys())==0:
                    raise IncorrectDataError(f'Input "data" is empty dict')
                
                for k,v in self.data.items():
                    if not isinstance(v,list):
                        raise IncorrectDataError(f'Input "data" is dict, but value for key "{k}" is not list')
                    elif list_length is None:
                        list_length = len(v)
                    elif len(v)!=list_length:
                        raise IncorrectDataError(f'Input "data" is dict of list, but lists have different length. '+\
                                                 f'Key "{k}" has list of length {len(v)}, but expected length is {list_length}')

                self.total_row_count = list_length
                if self.total_row_count==0:
                    if self.table is None:
                        raise IncorrectDataError(f'Input list is empty and table "{self.table_full_name}" does not exist.')
                    else:
                        logger.warning('Input list is empty.')
                        self._get_fields_from_table()
                else:
                    self._get_fields_from_dict_of_list()              

            # PANDAS
            elif _is_pandas_dataframe(self.data):
                self.type_of_data = 'pandas'
                self.total_row_count = len(self.data)
                if self.total_row_count==0:
                    if len(self.data.columns)==0:
                        if self.table is None:
                            raise IncorrectDataError(f'Input DataFrame is empty and table "{self.table_full_name}" does not exist.')
                        else:
                            logger.warning('No data, empty dataframe with empty columns')
                            self._get_fields_from_table()
                    else:
                        logger.warning('No data, empty dataframe')
                        self._get_fields_from_pandas()

                else:
                    self._get_fields_from_pandas()

            # POLARS
            elif _is_polars_dataframe(self.data):
                self.type_of_data = 'polars'
                self.total_row_count = len(self.data)
                if self.total_row_count==0:
                    if len(self.data.columns)==0:
                        if self.table is None:
                            raise IncorrectDataError(f'Input DataFrame is empty and table "{self.table_full_name}" does not exist.')
                        else:
                            logger.warning('No data, empty dataframe with empty columns')
                            self._get_fields_from_table()
                    else:
                        logger.warning('No data, empty dataframe')
                        self._get_fields_from_polars()

                else:
                    self._get_fields_from_polars()

            else:
                raise IncorrectDataError(f'Input "data" should be pandas/polars DataFrame, '
                                         f'list of dicts or dict of lists')

            self._check_existing_and_new_fields()

            # delete_mark_field may be supplied in the incoming data; if so we honor that value
            # instead of managing the flag automatically.
            self.delete_mark_from_data = (self.delete_mark_field is not None
                                          and self.delete_mark_field in self.data_fields)

            self._check_key()

            if self.table is None:
                if can_create_table:
                    logger.info(f'Table "{self.table_full_name}" does not exist. Creating.')
                    self._check_given_types()    
                    if self.type_of_data in ['list of dict','dict of list','pandas','polars']: #data types from source table are already known
                        self._detect_missing_data_types()
                    self._adapt_types_to_dialect()
                    self._create_table()
                    # The whole table is new, so its columns are not reported as added ones:
                    # there was no previous version of the table to add them to.
                    self.table_created = True
                else:
                    raise TableNotFoundError(f"Table not found {self.table_full_name} and can_create_table=False")
            else:
                if len(self.new_fields)>0:
                    if can_create_columns:
                        self._check_given_types()
                        if self.type_of_data in ['list of dict','dict of list','pandas','polars']:
                            self._detect_missing_data_types()
                        self._adapt_types_to_dialect()
                        self._create_new_fields()
                    else:
                        self._remove_new_fields()

            if self.delete_mark_field is not None:
                self._resolve_delete_mark_values()

            self._create_temp_table()


        except Exception:
            if hasattr(self, 'conn'):
                self.conn.rollback()
                self.conn.close()
            raise



    def exec(self, delete_condition: ColumnElement=None, source_condition: ColumnElement=None,
             update_condition: ColumnElement=None, insert_condition: ColumnElement=None,
             commit_all_steps=True, chunk_size: int = 10000) -> mergeResult:
        """
        Executes the merge operation. It returns a mergeResult class with statistical information.

        This method triggers the actual merge operation based on the configurations passed during initialization.
        
        Execution Workflow:
        1) Inserts the source data into the temporary staging table.
        2) Update: Updates existing rows in the target table where field values differ from the temporary table.
        3) Insert: Copies rows from the temporary table to the target table that do not currently exist.
        4) Delete / Mark: Deletes or marks rows in the target table that are entirely missing from the temporary staging data.

        If your data comes in portions (e.g., monthly snapshots), you can set a delete_condition argument 
        to restrict the deletion scope so that it only affects the relevant timeframe.
        E.g.:
            with dbmerge(data=data, engine=engine, table_name="YourTable", delete_mode='delete') as merge:
                merge.exec(delete_condition=merge.table.c['Date'].between(date(2025,1,1),date(2025,1,31)))

        Args:
            delete_condition (ColumnElement, optional): An SQLAlchemy binary expression used in the WHERE clause
                during the delete/mark phase. Essential for chunked or partitioned data syncs.
            source_condition (ColumnElement, optional): An SQLAlchemy binary expression used to filter the SELECT statement
                when loading data from a source_table_name.
            update_condition (ColumnElement, optional): An SQLAlchemy binary expression added to the WHERE clause of the
                UPDATE phase, so a target row is updated only when this condition also holds (on top of "some field differs").
                Build it on the target and staging tables exposed as `merge.table` and `merge.temp_table`. Rows that do not
                satisfy the condition are left untouched (they are filtered out of the set of rows to update, not deleted);
                the insert phase is unaffected. E.g. never overwrite rows flagged as protected:
                    merge.exec(update_condition=merge.table.c['is_protected'] == False)
            insert_condition (ColumnElement, optional): An SQLAlchemy binary expression added to the WHERE clause of the
                INSERT phase, so a source row missing from the target is inserted only when this condition also holds.
                Build it on the staging table `merge.temp_table` (the row being inserted), e.g. insert only positive rows:
                    merge.exec(insert_condition=merge.temp_table.c['amount'] > 0)
                Do NOT reference `merge.table` directly here: in the insert step the target row is absent (NULL) by
                definition. To look at other target rows, use a correlated EXISTS over a separate alias of the target,
                e.g. skip inserting into a category that already has a locked row:
                    g = merge.table.alias()
                    guard = ~exists().where((g.c['category'] == merge.temp_table.c['category']) & (g.c['locked'] == True))
                    merge.exec(insert_condition=guard)
            commit_all_steps (bool, optional): If set to True (default), then every step (temp insert, target insert, update, delete)
                is committed immediately. If False, a single commit is issued after all steps complete successfully.
                Must be a real boolean: a string such as 'false' is rejected instead of being treated as truthy.
                It applies to the data only - schema changes (creating the target table, adding columns, creating and
                dropping the staging table) are always committed on their own and never roll back with the data.
            chunk_size (int, optional): Defines the batch size when inserting raw data (from lists of dicts, dicts of lists
                or Pandas/Polars DataFrames) into the temporary table to avoid memory/query-size limits. Defaults to 10000.

        Returns:
            mergeResult: A dataclass object containing execution statistics (e.g., inserted_row_count, total_time)
                and the schema changes this merge made to the target table (table_created, added_fields:
                a {column name: SQLAlchemy type} mapping of the columns that were added).
        """
        
        if self.merge_finished:
            raise IncorrectParameter(f'Merge exec already finished on table {self.table_full_name}')

        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
            raise IncorrectParameter(f'chunk_size must be a positive integer, got {chunk_size!r}')

        # Anything non-empty passed here would be truthy, so commit_all_steps='false' would turn
        # step commits on - the opposite of what such a call means.
        if not isinstance(commit_all_steps, bool):
            raise IncorrectParameter(f'commit_all_steps must be True or False, got {commit_all_steps!r}.')

        if delete_condition is not None:
            if not isinstance(delete_condition,ColumnElement):
                raise IncorrectParameter('delete_condition argument should be sqlalchemy logical expression (ColumnElement type)')
            if self.delete_mode not in ['delete', 'mark']:
                logger.warning(f"""delete_condition is assigned, but delete_mode='{self.delete_mode}'. """
                                """delete_condition will be ignored.""")
            
        self.delete_condition = delete_condition

        if source_condition is not None:
            if not isinstance(source_condition,ColumnElement):
                raise IncorrectParameter('source_condition argument should be sqlalchemy logical expression (ColumnElement type)')
            if self.source_table is None:
                logger.warning('source_condition is assigned, but source_table is not assigned. '
                               'source_condition will be ignored.')
        self.source_condition = source_condition

        if update_condition is not None:
            if not isinstance(update_condition,ColumnElement):
                raise IncorrectParameter('update_condition argument should be sqlalchemy logical expression (ColumnElement type)')
        self.update_condition = update_condition

        if insert_condition is not None:
            if not isinstance(insert_condition,ColumnElement):
                raise IncorrectParameter('insert_condition argument should be sqlalchemy logical expression (ColumnElement type)')
        self.insert_condition = insert_condition

        self.chunk_size = chunk_size
        
        try:
            
            if self.source_table_name is None:
                self._insert_data_to_temp()
            else:
                self._insert_source_table_to_temp()
            data_msg = f'Temp data: {self.total_row_count} rows ({format_ms(self.temp_insert_time)})'
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

            self.total_time = self.temp_insert_time+self.insert_time+\
                              self.update_time+self.delete_time
            
            logger.info(f'Merged data into table "{self.table_full_name}". '+\
                        ', '.join([data_msg,inserted_msg,updated_msg,delete_msg])+', '+\
                        f'Total time: {format_ms(self.total_time)}')
            
            return mergeResult(total_row_count = self.total_row_count,
                               inserted_row_count = self.inserted_row_count,
                               updated_row_count = self.updated_row_count,
                               deleted_row_count = self.deleted_row_count,
                               total_time = self.total_time, 
                               temp_insert_time = self.temp_insert_time,
                               insert_time = self.insert_time,
                               update_time = self.update_time,
                               delete_time = self.delete_time,
                               table_created = self.table_created,
                               # copied, so that the result stays a snapshot: the same mapping is
                               # also readable on the instance, and the two should not share it
                               added_fields = dict(self.added_fields))
    
        
        except Exception:
            if hasattr(self, 'conn'):
                self.conn.rollback()
            raise

        finally:
            self._drop_temp_table()
            if hasattr(self, 'conn'):
                self.conn.close()
            self.merge_finished = True



    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._drop_temp_table()
        if hasattr(self, 'conn') and not self.conn.closed:
            self.conn.close()

    def __del__(self):
        # Best-effort safety net for objects used without a context manager and
        # without calling exec(). Must never raise: during interpreter shutdown
        # the engine/connection may already be finalized.
        try:
            self._drop_temp_table()
        except Exception:
            pass
        try:
            if hasattr(self, 'conn') and not self.conn.closed:
                self.conn.close()
        except Exception:
            pass

    def _create_schema_if_not_exists(self,schema_name):
        if self.conn.dialect.name not in ['sqlite']:
            schema_exists = self.conn.dialect.has_schema(self.conn, schema_name)
            # The check just opened a transaction on the connection. It is closed here whether or
            # not anything gets created, so that the DDL below - and any DDL later in __init__ -
            # stays alone in its own transaction. CockroachDB refuses a multi-statement transaction
            # that contains a schema change under a weak isolation level.
            self.conn.commit()
            if not schema_exists:
                if self.can_create_schemas:
                    logger.info(f"""Creating schema "{schema_name}".""")
                    self.conn.execute(schema.CreateSchema(schema_name))
                    self.conn.commit()
                else:
                    raise IncorrectParameter(f"""Schema "{schema_name}" does not exist and can_create_schemas=False""")


    def _get_fields_from_source_table(self):
        self.data_fields = {c.name:c.type for c in self.source_table.c if c.name not in self.special_fields}

    def _get_fields_from_list_of_dict(self):
        test_row = self.data[0]
        self.type_of_data = 'list of dict'
        if isinstance(test_row,dict):
            self.data_fields = {c:None for c in test_row if c not in self.special_fields}
        else:
            raise IncorrectDataError(f'Input "data" is list, but no dict inside')

    def _get_fields_from_dict_of_list(self):
        self.data_fields = {c:None for c in self.data.keys() if c not in self.special_fields}

    def _get_fields_from_pandas(self):
        self.data_fields = {c:None for c in self.data.columns if c not in self.special_fields}

    def _get_fields_from_polars(self):
        self.data_fields = {}
        for c,t in self.data.schema.items():
            if c not in self.special_fields:
                if str(t) in POLARS_TO_SQLALCHEMY_TYPE_MAP.keys():
                    self.data_fields[c] = POLARS_TO_SQLALCHEMY_TYPE_MAP[str(t)]
                else:
                    self.data_fields[c] = None
        

    def _get_fields_from_table(self):
        self.data_fields = {c.name:c.type for c in self.table.c if c.name not in self.special_fields}


    def _check_type_is_supported(self,field_type):
        if isinstance(field_type,JSON) and self.engine.dialect.name in JSONB_DIALECTS:
            if not isinstance(field_type,dialects.postgresql.JSONB):
                raise IncorrectDataError(f'JSON type is not supported for "{self.engine.dialect.name}". '+\
                                        'Use JSONB instead (sqlalchemy.dialects.postgresql.JSONB)')

    def _check_existing_and_new_fields(self):
        if self.table is not None:
            existing_fields = {c.name:c.type for c in self.table.c}
        else:
            existing_fields = {}

        for f in self.data_fields:
            if f in existing_fields:
                self._check_type_is_supported(existing_fields[f])
                self.data_fields[f]=existing_fields[f]
            else:
                self.new_fields[f]=self.data_fields[f]

        delete_mark_field = self.delete_mark_field
        if delete_mark_field is not None and delete_mark_field not in existing_fields \
                and delete_mark_field not in self.new_fields:
            # not present in data either - default the auto-managed flag to Boolean
            self.new_fields[delete_mark_field]=Boolean()

        merged_on_field = self.merged_on_field
        if merged_on_field is not None and merged_on_field not in existing_fields:
            self.new_fields[merged_on_field]=DateTime()

        inserted_on_field = self.inserted_on_field
        if inserted_on_field is not None and inserted_on_field not in existing_fields:
            self.new_fields[inserted_on_field]=DateTime()


    def _resolve_delete_mark_values(self):
        # delete_mark_field must be a Boolean (active=False / deleted=True) or an
        # Integer column (active=0 / deleted=1). Resolve the concrete values to use.
        if self.delete_mark_field not in self.table.c:
            raise IncorrectParameter(f'delete_mark_field "{self.delete_mark_field}" column does not exist '
                                     f'in table "{self.table_full_name}" and could not be created.')

        col_type = self.table.c[self.delete_mark_field].type
        if isinstance(col_type, Boolean):
            self.delete_mark_active_value = False
            self.delete_mark_deleted_value = True
        elif isinstance(col_type, (Integer, Numeric)):
            self.delete_mark_active_value = 0
            self.delete_mark_deleted_value = 1
        else:
            raise IncorrectParameter(f'delete_mark_field "{self.delete_mark_field}" must be a Boolean or Integer '
                                     f'column, but its type is {col_type}.')

        # Extra columns stamped on a marked row must exist, and must not collide with the columns
        # the mark itself writes (otherwise one column would get two values in the same UPDATE).
        managed = {f for f in (self.delete_mark_field, self.merged_on_field, self.inserted_on_field)
                   if f is not None}
        for col in self.delete_mark_values:
            if col not in self.table.c:
                raise IncorrectParameter(f'delete_mark_values column "{col}" does not exist in table '
                                         f'"{self.table_full_name}".')
            if col in managed:
                raise IncorrectParameter(f'delete_mark_values column "{col}" is managed automatically '
                                         f'and can not be set manually.')
            if col in self.key:
                # Stamping a key column would rewrite the identity of every row being marked,
                # silently when the new key value happens to be free.
                raise IncorrectParameter(f'delete_mark_values column "{col}" is part of the merge key '
                                         f'and can not be overwritten: it identifies the row being marked.')


    def _check_given_types(self):

        for f in self.new_fields:
            if f in self.given_data_types:
                given_type = self.given_data_types[f]
                if isinstance(given_type, types.TypeEngine):
                    self._check_type_is_supported(given_type)
                    # An automatically managed column (the audit timestamps, the auto-managed delete
                    # flag) is created from this type, but must not become a data field: its value
                    # comes from the merge itself, not from the source row.
                    self._set_new_field_type(f, given_type)
                else:
                    raise IncorrectDataError(f'Incorrect type {given_type} given for field {f}. '+\
                                              'Should be sqlalchemy data type')

    def _detect_missing_data_types(self):

        not_given_data_types = [f for f in self.new_fields if self.new_fields[f] is None]

        if len(not_given_data_types) == 0:
            return
        
        if self.type_of_data == 'list of dict':
            test_data = self.data[:self.max_type_detection_rows]
        elif self.type_of_data == 'dict of list':
            test_data = [dict(zip(self.data.keys(), vals)) 
                         for vals in zip(*[v[:self.max_type_detection_rows] for v in self.data.values()])]
        elif self.type_of_data == 'pandas':
            test_data = self.data.iloc[:self.max_type_detection_rows].to_dict(orient='records')
        elif self.type_of_data == 'polars':
            test_data = self.data.slice(0,self.max_type_detection_rows).to_dicts()
        else:
            return

        for i,test_row in enumerate(test_data):
            
            if isinstance(test_row,dict):
                
                detection_failed = False                   
                
                for f in not_given_data_types:
                    #Check if this field was already detected
                    if self.new_fields.get(f) is None:
                        # bool should be before int, 
                        # because boolean vars are also detected as int in python isinstance check 
                        if isinstance(test_row[f],bool):
                            self.new_fields[f] = Boolean()
                        elif isinstance(test_row[f],int):
                            self.new_fields[f] = BigInteger()
                        elif isinstance(test_row[f],float):
                            self.new_fields[f] = Numeric()
                        elif isinstance(test_row[f],str):
                            self.new_fields[f] = String()
                        # datetime should be before date, becuase datetime is also detected as date
                        elif isinstance(test_row[f],datetime):
                            if test_row[f].tzinfo is None:
                                self.new_fields[f] = DateTime()
                            else:
                                self.new_fields[f] = DateTime(timezone=True)
                        elif isinstance(test_row[f],date):
                            self.new_fields[f] = Date()
                        elif isinstance(test_row[f],list) or isinstance(test_row[f],dict) or \
                            isinstance(test_row[f],tuple):
                            if self.engine.dialect.name in JSONB_DIALECTS:
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


    def _adapt_types_to_dialect(self):
        # A type detected from the data or mapped from a dataframe is generic, and not every engine
        # can create a column from it as is: the DDL may fail to compile, or the engine may resolve
        # the generic type to something that silently changes the values. Here the types of the
        # fields we are about to create are adjusted to what the target engine actually supports.
        # Only new fields are touched - a column that already exists in the table keeps the type
        # the table gives it.
        dialect_name = self.engine.dialect.name

        for f in self.new_fields:
            # A type listed in data_types is the caller's own decision about how the column must be
            # stored, so it is never substituted here. If the engine can not create a column from
            # it, that is reported by the engine rather than silently worked around.
            if f in self.given_data_types:
                continue

            field_type = self.new_fields[f]

            # MySQL/MariaDB can not create a VARCHAR without a length, while on the other engines
            # String() already means "text of unlimited length". LONGTEXT is that same unlimited
            # text, so the column is switched to it instead of inventing a VARCHAR size: any size
            # would be a limit the other engines do not have, and a longer value arriving on a
            # later load fails on an already created table.
            # A key column can not be switched, because MySQL/MariaDB refuse to index a TEXT column
            # without a prefix length. No default size is safe there either, since InnoDB shares
            # MYSQL_MAX_KEY_BYTES between all columns of the key, so the caller has to state it.
            if dialect_name in MYSQL_DIALECTS and _is_string_without_length(field_type):
                if f in self.key:
                    raise IncorrectParameter(
                        f'Column "{f}" is part of the merge key, so "{dialect_name}" needs an '
                        f'explicit length for it: data_types={{"{f}": String(255)}}. '
                        f'InnoDB indexes at most {MYSQL_MAX_KEY_BYTES} bytes per key, shared by all '
                        f'{len(self.key)} column(s) of this key, and utf8mb4 takes up to 4 bytes per character.')

                logger.info(f'Field "{f}" - string of unknown length, '
                            f'creating it as LONGTEXT on "{dialect_name}"')
                self._set_new_field_type(f, dialects.mysql.LONGTEXT())

            # A NUMERIC with no precision is rounded to a whole number by these engines, so 1.75
            # would be stored as 2. A float carries no precision or scale to declare, and a double
            # is the type the value already has in Python, so it is used instead of inventing one.
            elif dialect_name in ROUNDING_NUMERIC_DIALECTS and _is_numeric_without_precision(field_type):
                logger.info(f'Field "{f}" - number of unknown precision, '
                            f'creating it as a double on "{dialect_name}" to keep the fraction')
                self._set_new_field_type(f, Double())

            # MySQL/MariaDB default DATETIME to whole seconds and drop microseconds without a
            # warning. The fractional precision has to be declared explicitly to keep them.
            elif dialect_name in MYSQL_DIALECTS and isinstance(field_type, DateTime):
                logger.info(f'Field "{f}" - datetime, '
                            f'creating it as DATETIME(6) on "{dialect_name}" to keep microseconds')
                self._set_new_field_type(f, dialects.mysql.DATETIME(fsp=6))

            # MS SQL DATETIME rounds to about 3 ms, while DATETIME2 keeps the full fraction.
            # A timezone-aware value already compiles to DATETIMEOFFSET, which keeps both the
            # offset and the fraction, so only the naive one is replaced here.
            elif dialect_name == 'mssql' and isinstance(field_type, DateTime) and not field_type.timezone:
                logger.info(f'Field "{f}" - datetime, '
                            f'creating it as DATETIME2 on "{dialect_name}" to keep microseconds')
                self._set_new_field_type(f, dialects.mssql.DATETIME2())


    def _now(self):
        # The audit timestamps are produced by the database, so their timezone and resolution are
        # the engine's, not ours. The only thing asked for here is the fractional precision that an
        # engine hides behind a default: MySQL/MariaDB NOW() drops the fraction unless NOW(fsp) is
        # used. The argument has to be a literal - a bind parameter is not accepted there.
        if self.engine.dialect.name in MYSQL_DIALECTS:
            return func.now(literal_column('6'))
        return func.now()


    def _set_new_field_type(self, field_name, field_type):
        # A field about to be created is listed in new_fields, and - unless it is one of the
        # automatically managed columns - also in data_fields, so both have to stay in sync.
        self.new_fields[field_name] = field_type
        if field_name in self.data_fields:
            self.data_fields[field_name] = field_type


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
            
            from alembic.migration import MigrationContext
            from alembic.operations import Operations

            # Each ALTER commits on its own here, so a failure part way through leaves the columns
            # added so far. They are exactly the columns the incoming data asked for, and a repeated
            # merge adds the remaining ones, so the target converges.
            self.conn.commit()
            op=Operations(MigrationContext.configure(self.conn))
            # The automatically managed columns are left out of the reported added_fields: they
            # carry no source data, so a consumer watching for a schema change has nothing to
            # recompute over them. A delete flag supplied in the data is a data column, and stays.
            managed = {f for f in (self.merged_on_field, self.inserted_on_field) if f is not None}
            if self.delete_mark_field is not None and not self.delete_mark_from_data:
                managed.add(self.delete_mark_field)
            for field_name in self.new_fields:
                logger.info(f'Creating new field "{field_name}" - {self.new_fields[field_name]}')
                primary_key = field_name in self.key
                op.add_column(
                        self.table_name,
                        Column(field_name, self.new_fields[field_name], primary_key=primary_key),
                        schema=self.schema
                    )
                # Committed one ALTER at a time, so each stays alone in its transaction. A failure
                # part way through therefore leaves the columns added so far - they are exactly the
                # ones the incoming data asked for, and a repeated merge adds the rest.
                self.conn.commit()
                # Recorded after the commit, so the mapping holds the columns that are really in
                # the table - a failed ALTER stops here and does not report the column it did not
                # add. The type stored is the one the column was created with, after any adjustment
                # made for the target engine, so it is what the database actually holds.
                if field_name not in managed:
                    self.added_fields[field_name] = self.new_fields[field_name]
            self.table = self._load_table_metadata_from_db(self.table_name,self.schema)
               

    def _validate_init_params(self, engine, table_name, data, delete_mode, delete_mark_field,
                              delete_mark_values, merged_on_field, inserted_on_field,
                              skip_update_fields, skip_compare_fields, key,
                              data_types, schema, temp_schema, source_table_name, source_schema,
                              can_create_table, can_create_columns, can_create_schemas):
        """Validate user-supplied arguments up front so that a wrong type raises a
        clear IncorrectParameter instead of an obscure crash deeper in the merge."""

        if not isinstance(engine, Engine):
            raise IncorrectParameter(f'"engine" must be a SQLAlchemy Engine (from create_engine()), '
                                     f'got {type(engine).__name__}.')

        if not isinstance(table_name, str) or table_name.strip() == '':
            raise IncorrectParameter(f'"table_name" must be a non-empty string, got {table_name!r}.')

        if delete_mode not in ('no', 'delete', 'mark'):
            raise IncorrectParameter(f'''"delete_mode" must be one of 'no', 'delete', 'mark', got {delete_mode!r}.''')

        # scalar string-or-None parameters
        for name, value in (('delete_mark_field', delete_mark_field),
                            ('merged_on_field', merged_on_field),
                            ('inserted_on_field', inserted_on_field),
                            ('schema', schema),
                            ('temp_schema', temp_schema),
                            ('source_table_name', source_table_name),
                            ('source_schema', source_schema)):
            if value is not None and not isinstance(value, str):
                raise IncorrectParameter(f'"{name}" must be a string or None, got {type(value).__name__}.')

        # list-of-strings-or-None parameters
        for name, value in (('key', key), ('skip_update_fields', skip_update_fields),
                            ('skip_compare_fields', skip_compare_fields)):
            if value is None:
                continue
            if not isinstance(value, list):
                raise IncorrectParameter(f'"{name}" must be a list of column names or None, '
                                         f'got {type(value).__name__}.')
            for item in value:
                if not isinstance(item, str):
                    raise IncorrectParameter(f'"{name}" must contain only column name strings, '
                                             f'got element {item!r} of type {type(item).__name__}.')

        if delete_mark_values is not None:
            if not isinstance(delete_mark_values, dict):
                raise IncorrectParameter(f'"delete_mark_values" must be a dict mapping column names to values '
                                         f'or None, got {type(delete_mark_values).__name__}.')
            for col in delete_mark_values:
                if not isinstance(col, str):
                    raise IncorrectParameter(f'"delete_mark_values" keys must be column name strings, '
                                             f'got key {col!r}.')

        if data_types is not None:
            if not isinstance(data_types, dict):
                raise IncorrectParameter(f'"data_types" must be a dict mapping column names to SQLAlchemy '
                                         f'types or None, got {type(data_types).__name__}.')
            for col, col_type in data_types.items():
                if not isinstance(col, str):
                    raise IncorrectParameter(f'"data_types" keys must be column name strings, got key {col!r}.')
                if not isinstance(col_type, types.TypeEngine):
                    raise IncorrectParameter(f'"data_types" value for column "{col}" must be a SQLAlchemy type '
                                             f'instance (e.g. String(100)), got {col_type!r}.')

        # boolean flags
        for name, value in (('can_create_table', can_create_table),
                            ('can_create_columns', can_create_columns),
                            ('can_create_schemas', can_create_schemas)):
            if not isinstance(value, bool):
                raise IncorrectParameter(f'"{name}" must be a boolean, got {type(value).__name__}.')

        if data is not None and source_table_name is not None:
            raise IncorrectParameter('Provide either "data" or "source_table_name", not both '
                                     '("data" is ignored when a source table is given).')

        if source_schema is not None and source_table_name is None:
            raise IncorrectParameter('"source_schema" is set, but "source_table_name" is not. '
                                     'The schema only says where the source table lives, so it has '
                                     'nothing to apply to on its own.')

        # Each of these columns is written by a rule of its own, so one column can not play two of
        # these roles at once: the merge would have to give it two different values in one statement.
        roles = (('delete_mark_field', delete_mark_field),
                 ('merged_on_field', merged_on_field),
                 ('inserted_on_field', inserted_on_field))
        for i, (name, value) in enumerate(roles):
            if value is None:
                continue
            for other_name, other_value in roles[i+1:]:
                if value == other_value:
                    raise IncorrectParameter(f'"{name}" and "{other_name}" are both set to column '
                                             f'"{value}". Each of them needs a column of its own.')


    def _check_key(self):
        if self.key is None or len(self.key)==0:
            if self.table is not None:
                self.key = [col.name for col in self.table.primary_key.columns]

        if self.key is None or len(self.key)==0:
            raise NoKeyError("No primary key: provide 'key' argument or set primary key in DB")
        else:
            for c in self.key:
                if c in self.special_fields:
                    raise NoKeyError(f'Key field "{c}" is a special field, which can not be used in the primary key.')
                elif c == self.delete_mark_field:
                    # The mark phase would run "UPDATE ... SET <key column> = <deleted value>",
                    # assigning the flag value to the key of every row missing from the source.
                    raise NoKeyError(f'Key field "{c}" is also the delete_mark_field, which can not be used '
                                     f'in the primary key: marking a row deleted would overwrite its key.')
                elif c not in self.data_fields:
                    raise NoKeyError(f'Key field "{c}" not found in data')
                elif c in self.new_fields and self.table is not None:
                    raise NoKeyError(f'Key field "{c}" is a new column, '+\
                                     f'but table "{self.table_full_name}" already exist, can not add primary key column.')


    def _insert_missing_data(self):
        
        start_time = time.perf_counter()
        first_pk_col = self.table.c[self.key[0]]
        
        source_fields = [self.temp_table.c[f] for f in self.data_fields]
        target_fields = [self.table.c[f] for f in self.data_fields]

        if self.merged_on_field is not None:
            source_fields.append(self._now().label(self.merged_on_field))
            target_fields.append(self.table.c[self.merged_on_field])

        if self.inserted_on_field is not None:
            source_fields.append(self._now().label(self.inserted_on_field))
            target_fields.append(self.table.c[self.inserted_on_field])

        if self.delete_mark_field is not None and not self.delete_mark_from_data:
            source_fields.append(literal(self.delete_mark_active_value).label(self.delete_mark_field))
            target_fields.append(self.table.c[self.delete_mark_field])

        join_conditions = []
        for key_col in self.key:
            join_conditions.append(self.table.c[key_col]==self.temp_table.c[key_col])
        on_clause = and_(*join_conditions)

        # A source row is "missing" when the outer-joined target row is absent (its PK is NULL).
        where_clause = first_pk_col.is_(None)

        # Optional caller-supplied guard: a missing source row is inserted only when it also
        # satisfies insert_condition. Reference merge.temp_table (and, for target lookups, a
        # separate alias of the target) — merge.table here is the NULL side of the outer join.
        if self.insert_condition is not None:
            where_clause = and_(where_clause, self.insert_condition)

        select_stmt = select(*source_fields).join(self.table, on_clause, isouter=True).where(where_clause)

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
        update_values[mark_field] = self.delete_mark_deleted_value

        if self.merged_on_field is not None:
            merged_on_field = self.table.c[self.merged_on_field]
            update_values[merged_on_field]=self._now()

        # Caller-supplied columns written together with the mark (e.g. a load id), so that the
        # marking itself can be recorded and not just the flag.
        for col, value in self.delete_mark_values.items():
            update_values[self.table.c[col]] = value

        # Skip rows that are already marked as deleted, so repeated merges do not
        # re-mark them (inflating deleted_row_count and overwriting merged_on_field).
        where_conditions = [not_(exists().where(update_where_clause)),
                            mark_field.is_distinct_from(self.delete_mark_deleted_value)]
        if self.delete_condition is not None:
            where_conditions.append(self.delete_condition)

        update_stmt = update(self.table).values(update_values).where(and_(*where_conditions))
        
        self.delete_sql = str(update_stmt)
        
        result = self.conn.execute(update_stmt)
        self.deleted_row_count = result.rowcount

        end_time = time.perf_counter()
        self.delete_time = end_time - start_time  


    def _update_not_matching_data(self):
        
        start_time = time.perf_counter()
        non_key_cols = [c for c in self.data_fields if c not in self.key and
                                                       c not in self.skip_update_fields]

        # The auto-managed delete flag still has to be reset to active when a marked-deleted row
        # reappears, even for key-only tables that have no value columns to compare/update.
        manage_mark_reset = self.delete_mark_field is not None and not self.delete_mark_from_data

        if len(non_key_cols)==0 and not manage_mark_reset:
            # nothing to update
            self.updated_row_count = 0
            self.update_time = 0
            return

        join_conditions = []
        for c in self.key:
            join_conditions.append(self.table.c[c]==self.temp_table.c[c])
        on_clause = and_(*join_conditions)

        # Columns listed in skip_compare_fields are still written (they stay in non_key_cols and in
        # update_values below), but are left out of the comparison, so a difference in them alone
        # does not make the row "changed" and does not get it updated.
        where_conditions = []
        for c in non_key_cols:
            if c in self.skip_compare_fields:
                continue
            col = self.table.c[c]
            temp_col = self.temp_table.c[c]
            where_conditions.append(col.is_distinct_from(temp_col))

        # When the flag is auto-managed (not supplied in the data), a row currently marked as
        # deleted must be picked up for update so it gets reset back to the active value.
        if self.delete_mark_field is not None and not self.delete_mark_from_data:
            mark_field = self.table.c[self.delete_mark_field]
            where_conditions.append(mark_field.is_distinct_from(self.delete_mark_active_value))

        if len(where_conditions)==0:
            # Every comparable column is skipped, so no row can ever qualify as changed.
            self.updated_row_count = 0
            self.update_time = 0
            return

        where_clause = or_(*where_conditions)

        # Optional caller-supplied guard: a target row is updated only when it also satisfies
        # update_condition (e.g. a version guard that keeps the newest write). Rows that fail the
        # guard drop out of this subquery, so the UPDATE below never touches them.
        if self.update_condition is not None:
            where_clause = and_(where_clause, self.update_condition)

        select_stmt = select(self.temp_table).join(self.table, on_clause, isouter=False).where(where_clause)
        select_stmt = select_stmt.subquery()

        update_values = {}
        for c in non_key_cols:
            update_values[self.table.c[c]] = select_stmt.c[c]

        if self.delete_mark_field is not None and not self.delete_mark_from_data:
            mark_field = self.table.c[self.delete_mark_field]
            update_values[mark_field]=self.delete_mark_active_value

        if self.merged_on_field is not None:
            merged_on_field = self.table.c[self.merged_on_field]
            update_values[merged_on_field]=self._now()

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
        # autoincrement=False: the key value always comes from the source data, so the database must
        # not generate it. Left at its default, a single-column integer key is created as
        # AUTO_INCREMENT / SERIAL / IDENTITY - which turns a key of 0 into 1 on MySQL, leaves the
        # PostgreSQL sequence behind the rows that were written, and makes MS SQL reject the
        # explicit insert altogether.
        cols = [Column(c, self.data_fields[c], primary_key = c in self.key, autoincrement=False)
                for c in self.data_fields]

        special_cols = [Column(c, self.new_fields[c]) for c in self.new_fields if c not in self.data_fields]

        all_cols = cols+special_cols

        for f in all_cols:
            key = f in self.key
            if key:
                logger.info(f'Table field "{f.name}" - {f.type}, primary key')
            else:
                logger.info(f'Table field "{f.name}" - {f.type}')

        self.table = Table(self.table_name, self.metadata, *all_cols, schema = self.schema)
        # Created on the merge connection rather than through the engine: with an in-memory SQLite
        # database a second connection is a different database, so the table has to be made here.
        # Created on the merge connection rather than through the engine: with an in-memory SQLite
        # database a second connection is a different database, so the table has to be made here.
        # A schema change is committed on its own, whatever commit_all_steps says, and is kept
        # alone in its transaction - the commit before matters as much as the one after.
        # checkfirst=False on purpose: it would put an existence query in front of the CREATE, and
        # the two together form the multi-statement transaction that CockroachDB rejects under a
        # weak isolation level. The table is known to be missing - that is why we are here.
        self.conn.commit()
        self.table.create(self.conn, checkfirst=False)
        self.conn.commit()

 
    @staticmethod
    def _truncate_to_bytes(s, max_bytes):
        # Postgres limits identifiers by bytes, not characters. Truncate the byte
        # representation and drop any incomplete trailing multibyte sequence.
        encoded = s.encode('utf-8')
        if len(encoded) <= max_bytes:
            return s
        return encoded[:max_bytes].decode('utf-8', errors='ignore')

    def _create_temp_table(self):

        # '_' and unique_id are ASCII (1 byte each), so we can subtract their char length as bytes
        max_bytes = MAX_TEMP_TABLE_NAME_LEN - len(self.unique_id) - 1

        temp_table_name = self._truncate_to_bytes(self.table_name, max_bytes) + '_' + self.unique_id

        # autoincrement=False for the same reason as in _create_table: the staging table is loaded
        # with the key values of the source, never with generated ones.
        cols = [Column(c.name, c.type, primary_key = c.name in self.key, autoincrement=False)
                for c in self.table.c]

        # As everywhere else, the schema change is kept alone in its own transaction.
        self.conn.commit()

        if self.engine.dialect.name =='postgresql':
            self.temp_table = Table(temp_table_name, self.metadata, *cols, schema = self.temp_schema, 
                                    prefixes=['UNLOGGED'])
            # postgresql_on_commit='PRESERVE ROWS' should be used for TEMP table,
            # but looks like UNLOGGED is performing better then TEMP.
            # Note: UNLOGGED is a persistent table (unlike TEMP), so if the process crashes before
            # _drop_temp_table() runs, the table is left behind in temp_schema and is not auto-cleaned.
            self.temp_table.create(bind=self.conn, checkfirst=False)

        elif self.engine.dialect.name in ('mariadb','mysql','sqlite'):
            self.temp_table = Table(temp_table_name, self.metadata, *cols, schema = self.temp_schema, 
                                    prefixes=['TEMPORARY'])
            self.temp_table.create(bind=self.conn, checkfirst=False)
        
        else:
            table_exists = self.inspector.has_table(temp_table_name, self.temp_schema)
            if table_exists:
                raise TempTableAlreadyExists(f'Temp table "{temp_table_name}" already exists in schema "{self.temp_schema}"')
            self.temp_table = Table(temp_table_name, self.metadata, *cols, schema = self.temp_schema)
            self.temp_table.create(bind=self.conn, checkfirst=False)

        self.conn.commit()        


    def _insert_data_to_temp(self):

        start_time = time.perf_counter()
        
        chunks_num = (self.total_row_count // self.chunk_size)
        if self.total_row_count % self.chunk_size > 0:
            chunks_num = chunks_num + 1
        
        if self.total_row_count>0:
            for i in range(chunks_num):
                begin = i * self.chunk_size
                end = min((i + 1) * self.chunk_size, self.total_row_count)
                
                if self.type_of_data == 'list of dict':
                    data_slice = self.data[begin:end]
                
                elif self.type_of_data == 'dict of list':
                    data_slice = [dict(zip(self.data.keys(), vals)) 
                                  for vals in zip(*[v[begin:end] for v in self.data.values()])]
                
                elif self.type_of_data == 'pandas':
                    import numpy as np  # numpy is a hard dependency of pandas, so it is already loaded
                    data_slice = self.data.iloc[begin:end]
                    data_slice = data_slice.replace({np.nan: None})
                    data_slice = data_slice.to_dict(orient='records')
                
                elif self.type_of_data == 'polars':
                    data_slice = self.data.slice(begin,self.chunk_size)   
                    data_slice = data_slice.to_dicts()
                
                else:
                    return
                    
                self.conn.execute(insert(self.temp_table), data_slice)

        
        end_time = time.perf_counter()
        self.temp_insert_time = end_time - start_time       
        
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
        self.total_row_count = result.rowcount 

        end_time = time.perf_counter()
        self.temp_insert_time = end_time - start_time  


    def _drop_temp_table(self):
        if hasattr(self, 'temp_table') and self.temp_table is not None:
            # TEMPORARY tables (sqlite/mysql/mariadb) are visible only on the connection
            # that created them: dropping via the engine runs on another pooled connection
            # where the table does not exist, leaving it alive until the pool recycles.
            if hasattr(self, 'conn') and not self.conn.closed:
                # checkfirst=False for the same reason as on creation, and there is nothing to
                # check anyway: the name carries a unique id, so this table is ours and exists.
                self.conn.commit()
                self.temp_table.drop(self.conn, checkfirst=False)
                self.conn.commit()
            elif inspect(self.engine).has_table(self.temp_table.name, self.temp_table.schema):
                # The connection is gone, so this runs on another one from the pool. A TEMPORARY
                # table would not be visible there at all - only a regular staging table can still
                # be dropped. Existence is checked separately, so the DROP stays alone in its
                # transaction.
                self.temp_table.drop(self.engine, checkfirst=False)
            self.temp_table = None


def drop_table_if_exists(engine,table_name,schema=None):

    """
    Additional routine to drop table in DB if it exists. Use carefully.
    """

    if schema is not None and engine.dialect.name=='sqlite':
        logger.warning('sqlite engine does not support schemas. ' 
                       f'Omitting parameter schema = "{schema}"')
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
        # checkfirst=False: has_table above has already answered that question, and asking again
        # would put a second statement in the transaction that carries the DROP - which CockroachDB
        # rejects under a weak isolation level.
        table.drop(engine, checkfirst=False)

  
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

    if not parts:
        return "0ms"

    return " ".join(parts[:2])