"""
Bulk loading of the staging table, where the engine and its driver offer a way to do it. The generic way -
an executemany of INSERT through SQLAlchemy - works everywhere, but goes row by row. A loader here puts the
same data into the staging table in bulk: it has to store exactly what the generic way stores, and
exec(bulk_load=False) turns every one of them off.

The module is imported only when a merge loads data with bulk_load on, and the drivers and pyarrow it uses
are imported only by the loader that needs them.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import importlib.util
import io
import json
import sys
import time

from .dbmerge import logger

if TYPE_CHECKING:
    from .dbmerge import dbmerge


# The PostgreSQL drivers that take COPY ... FROM STDIN, each in its own way (see _copy).
COPY_DRIVERS = ('psycopg2', 'psycopg', 'pg8000')

# Rows per bulk copy of mssql-python, unless exec() is given a chunk_size. Every call opens a connection of
# its own, some 40ms: a million rows load in 5.4s in chunks of 10,000 and in 1.5s in chunks of 100,000.
MSSQL_PYTHON_CHUNK_SIZE = 100_000


def loader_for(merge: dbmerge):
    """The bulk loader for the engine and the driver of this merge, or None to load row by row."""
    if merge.dialect_name == 'duckdb':
        if _has_pyarrow():
            return _duckdb_from_arrow
        warn_without_pyarrow(merge.dialect_name)

    elif merge.dialect_name in ('postgresql', 'cockroachdb') and merge.conn.dialect.driver in COPY_DRIVERS:
        if not _copy_takes_columns(merge):
            return None
        # The CSV for COPY is written by pyarrow, or else by the csv module, which tells NULL from an empty
        # string only since Python 3.12 (QUOTE_NOTNULL).
        if _has_pyarrow() or sys.version_info >= (3, 12):
            return _postgres_copy
        warn_without_pyarrow(merge.dialect_name)

    elif merge.dialect_name == 'mssql' and merge.conn.dialect.driver == 'mssqlpython':
        return _mssql_python_bulkcopy

    elif merge.dialect_name == 'mssql' and merge.conn.dialect.driver == 'pyodbc':
        return _pyodbc_fast_executemany

    return None


# The warnings already given, so that a loop of merges warns once rather than on every call.
_warned = set()


def _warn_once(topic, message):
    if topic not in _warned:
        _warned.add(topic)
        logger.warning(message)


def warn_without_pyarrow(dialect_name):
    _warn_once(f'pyarrow:{dialect_name}',
               f'pyarrow is not installed: the staging table on "{dialect_name}" is loaded without it, '
               f'several times slower. Install pyarrow to load it faster.')


def _has_pyarrow():
    return importlib.util.find_spec('pyarrow') is not None


def _duckdb_from_arrow(merge: dbmerge):
    # DuckDB reads an Arrow table registered on its connection where it lies, without copying it, so each
    # chunk goes into the staging table with a single INSERT ... SELECT.
    from ._chunks import arrow_table

    start_time = time.perf_counter()
    connection = merge.conn.connection.dbapi_connection   # duckdb_engine's wrapper of the DuckDB connection
    preparer = merge.conn.dialect.identifier_preparer
    fields = list(merge.data_fields)
    columns = ', '.join(preparer.quote(f) for f in fields)
    staging = preparer.format_table(merge.temp_table)
    processors = _json_processors(merge)
    view = f'dbmerge_chunk_{merge.unique_id}'

    for begin in range(0, merge.total_row_count, merge.chunk_size):
        end = min(begin + merge.chunk_size, merge.total_row_count)
        try:
            chunk = arrow_table(merge, begin, end, processors=processors)
        except Exception as e:
            # Values Arrow can not put in one column - a column of mixed types, say - go row by row, as they
            # would without bulk_load: a value the column refuses is then reported by DuckDB as it is there.
            logger.debug(f'Rows {begin}..{end} could not be put in an Arrow table ({e}), inserting them row by row')
            merge._insert_rows_to_temp(begin, end)
            continue
        connection.register(view, chunk)
        try:
            merge.conn.exec_driver_sql(f'INSERT INTO {staging} ({columns}) SELECT {columns} FROM {view}')
        finally:
            connection.unregister(view)

    merge.temp_insert_time = time.perf_counter() - start_time


def _postgres_copy(merge: dbmerge):
    # COPY ... FROM STDIN through the connection the merge already holds, so the staging table - a TEMPORARY
    # one, seen only by its own session - is there, and the load belongs to the same transaction.
    from sqlalchemy import DateTime, Integer, LargeBinary
    from zoneinfo import ZoneInfo

    start_time = time.perf_counter()
    # COPY goes through the driver's own cursor, past SQLAlchemy. Unless SQLAlchemy has a transaction open by
    # then, it does not know of the one COPY starts: a failed COPY would leave the connection aborted, the
    # rollback in exec() would not reach the driver, and the DROP of the staging table would then fail with
    # "current transaction is aborted", in place of the error that COPY raised.
    if not merge.conn.in_transaction():
        merge.conn.begin()
    preparer = merge.conn.dialect.identifier_preparer
    fields = list(merge.data_fields)
    sql = (f'COPY {preparer.format_table(merge.temp_table)} ({", ".join(preparer.quote(f) for f in fields)}) '
           f'FROM STDIN WITH (FORMAT csv)')

    processors = _json_processors(merge)
    for f in fields:
        if isinstance(merge.temp_table.c[f].type, LargeBinary):
            # bytea is read from text in its hex form
            processors[f] = lambda v: None if v is None else '\\x' + bytes(v).hex()

    # COPY reads a datetime with a time zone into a column without one by dropping the offset, while the row
    # by row INSERT converts it - to the time zone of the session with psycopg2 and psycopg, to UTC with
    # pg8000. The conversion is done here the same way.
    wall_clock = {}
    naive_columns = [f for f in fields if isinstance(merge.temp_table.c[f].type, DateTime)
                     and not merge.temp_table.c[f].type.timezone]
    if naive_columns:
        zone = 'UTC' if merge.conn.dialect.driver == 'pg8000' else merge.conn.exec_driver_sql('SHOW TimeZone').scalar()
        try:
            ZoneInfo(zone)
        except Exception:
            # A time zone that Python does not know: the conversion can not be repeated, so row by row.
            logger.debug(f'Session time zone "{zone}" is unknown to Python, loading the staging table row by row')
            zone = None
        if zone is None:
            for begin in range(0, merge.total_row_count, merge.chunk_size):
                merge._insert_rows_to_temp(begin, min(begin + merge.chunk_size, merge.total_row_count))
            merge.temp_insert_time = time.perf_counter() - start_time
            return
        wall_clock = {f: zone for f in naive_columns}

    if _has_pyarrow():
        to_csv = _csv_by_arrow
    else:
        to_csv = _csv_by_csv_module
        # pandas keeps an integer column with a missing value as floats. The csv module writes 1.0 for them,
        # which COPY does not take for an integer column, while Arrow writes 1 and the row by row INSERT sends
        # a number the server casts.
        for f in fields:
            if isinstance(merge.temp_table.c[f].type, Integer) and f not in processors:
                processors[f] = lambda v: int(v) if isinstance(v, float) and v.is_integer() else v
    for begin in range(0, merge.total_row_count, merge.chunk_size):
        end = min(begin + merge.chunk_size, merge.total_row_count)
        try:
            data = to_csv(merge, begin, end, processors, wall_clock)
        except Exception as e:
            # Values the CSV can not carry as the driver would send them - a column of mixed types, a
            # number too large for 64 bits - go row by row, as they would without bulk_load.
            logger.debug(f'Rows {begin}..{end} could not be written for COPY ({e}), inserting them row by row')
            merge._insert_rows_to_temp(begin, end)
            continue
        _copy(merge, sql, data)

    merge.temp_insert_time = time.perf_counter() - start_time


def _copy_takes_columns(merge: dbmerge):
    # Types whose text form for COPY the CSV here does not produce: arrays and intervals keep the row by
    # row INSERT.
    from sqlalchemy import ARRAY, Interval
    return not any(isinstance(merge.temp_table.c[f].type, (ARRAY, Interval)) for f in merge.data_fields)


def _csv_by_arrow(merge: dbmerge, begin, end, processors, wall_clock):
    import pyarrow.csv as pacsv
    from ._chunks import arrow_table
    buffer = io.BytesIO()
    # all_valid quotes every value that is there, so that an empty string ("") stays apart from NULL (an
    # empty field), as COPY reads them.
    pacsv.write_csv(arrow_table(merge, begin, end, processors=processors, wall_clock=wall_clock), buffer,
                    pacsv.WriteOptions(include_header=False, quoting_style='all_valid'))
    return buffer.getvalue()


def _csv_by_csv_module(merge: dbmerge, begin, end, processors, wall_clock):
    import csv
    from ._chunks import python_columns
    columns = python_columns(merge, begin, end, processors=processors, wall_clock=wall_clock)
    buffer = io.StringIO()
    # QUOTE_NOTNULL quotes every value but None, for the same reason as all_valid above.
    csv.writer(buffer, quoting=csv.QUOTE_NOTNULL, lineterminator='\n').writerows(zip(*columns.values()))
    return buffer.getvalue().encode()


def _copy(merge: dbmerge, sql, data):
    from sqlalchemy.exc import DBAPIError
    dialect = merge.conn.dialect
    driver = dialect.driver
    cursor = merge.conn.connection.dbapi_connection.cursor()
    try:
        if driver == 'psycopg2':
            cursor.copy_expert(sql, io.BytesIO(data))
        elif driver == 'psycopg':
            with cursor.copy(sql) as copy:
                copy.write(data)
        else:  # pg8000
            cursor.execute(sql, stream=io.BytesIO(data))
    except dialect.loaded_dbapi.Error as e:
        # The driver's own exception, wrapped as SQLAlchemy wraps it for the row by row INSERT, so that
        # the caller gets the same exception class (DataError, IntegrityError...) with bulk_load on or off.
        raise DBAPIError.instance(sql, None, e, dialect.loaded_dbapi.Error, dialect=dialect) from e
    finally:
        cursor.close()


def _json_processors(merge: dbmerge):
    # A JSON column gets its values through its own bind processor, as the row by row INSERT does: the
    # serializer of the engine, and None as the JSON literal null unless the type says none_as_null.
    from sqlalchemy import JSON
    from sqlalchemy.sql.elements import Null
    dialect = merge.conn.dialect
    processors = {}
    for f in merge.data_fields:
        column_type = merge.temp_table.c[f].type
        if not isinstance(column_type, JSON):
            continue
        processor = column_type.bind_processor(dialect)
        if processor is None:
            # psycopg 3 has no text form here: SQLAlchemy hands it a Jsonb wrapper, so a dict would reach the
            # CSV as a dict - Python's repr without pyarrow, a struct column (and a silent fall back to row by
            # row) with it. Serialized the way the row by row INSERT stores it: JSON null for None, unless the
            # type says none_as_null.
            serializer = getattr(dialect, '_json_serializer', None) or json.dumps
            def processor(v, serializer=serializer, none_as_null=column_type.none_as_null):
                if v is JSON.NULL:          # the JSON null, always
                    return serializer(None)
                if isinstance(v, Null):     # sqlalchemy.null(): SQL NULL, always
                    return None
                if v is None:
                    return None if none_as_null else serializer(None)
                return serializer(v)
        processors[f] = processor
    return processors


def _mssql_python_bulkcopy(merge: dbmerge):
    # The bulk copy of TDS (what SqlBulkCopy is in .NET): with pyarrow, the Arrow buffers go into the TDS
    # packets directly, without a Python object per value; without it, the rows go as tuples. mssql-python
    # runs it on a connection of its own, opened from the connection string - the staging table, a regular
    # one on MS SQL and already committed, is there for it. Should that connection fail (an authentication
    # it can not repeat, say), the chunk goes row by row.
    from ._chunks import arrow_table, python_columns

    start_time = time.perf_counter()
    fields = list(merge.data_fields)
    staging = merge.conn.dialect.identifier_preparer.format_table(merge.temp_table)
    processors = _json_processors(merge)
    use_arrow = _has_pyarrow()
    # timeout=0: the default of 30 seconds would cut a large load short. keep_nulls: a NULL stays NULL
    # rather than taking the default of its column.
    options = dict(column_mappings=fields, keep_nulls=True, timeout=0)
    chunk_size = merge.chunk_size if merge.chunk_size_given else MSSQL_PYTHON_CHUNK_SIZE

    offset_columns = _datetimeoffset_columns(merge)

    cursor = merge.conn.connection.dbapi_connection.cursor()
    try:
        for begin in range(0, merge.total_row_count, chunk_size):
            end = min(begin + chunk_size, merge.total_row_count)
            if offset_columns and _has_non_utc_offsets(merge, begin, end, offset_columns):
                # A datetimeoffset keeps the offset a value came with, and the bulk copy of mssql-python can not
                # repeat that: from Arrow it writes every value in UTC (+00:00), from tuples it keeps the offset
                # but shifts the moment by it. Such rows go row by row, which stores them exactly.
                _warn_once('mssql-datetimeoffset',
                           'datetimeoffset values with an offset other than UTC are inserted row by row: the bulk '
                           'copy of mssql-python can not keep their offset')
                merge._insert_rows_to_temp(begin, end)
                continue
            try:
                if use_arrow:
                    cursor.bulkcopy_arrow(staging, arrow_table(merge, begin, end, processors=processors), **options)
                else:
                    columns = python_columns(merge, begin, end, processors=processors)
                    cursor.bulkcopy(staging, list(zip(*columns.values())), **options)
            except Exception as e:
                logger.debug(f'Bulk copy of rows {begin}..{end} failed ({e}), inserting them row by row')
                merge._insert_rows_to_temp(begin, end)
    finally:
        cursor.close()

    merge.temp_insert_time = time.perf_counter() - start_time


def _datetimeoffset_columns(merge: dbmerge):
    from sqlalchemy import DateTime
    from sqlalchemy.dialects.mssql import DATETIMEOFFSET
    return [f for f in merge.data_fields
            if isinstance(merge.temp_table.c[f].type, DATETIMEOFFSET)
            or (isinstance(merge.temp_table.c[f].type, DateTime) and merge.temp_table.c[f].type.timezone)]


def _has_non_utc_offsets(merge: dbmerge, begin, end, fields):
    from datetime import timedelta
    from ._chunks import column_values
    for f in fields:
        for value in column_values(merge, f, begin, end):
            offset = value.utcoffset() if hasattr(value, 'utcoffset') else None
            if offset is not None and offset != timedelta(0):
                return True
    return False


def _pyodbc_fast_executemany(merge: dbmerge):
    # fast_executemany: pyodbc sends a whole chunk as one array of parameters instead of a round trip per
    # row. A chunk that fails goes row by row. The server may have taken the rows before the failing one by
    # then, so every chunk runs in a savepoint and is rolled back to it first: otherwise the row by row
    # INSERT would hit the primary key of the staging table, and report that in place of the real error.
    from ._chunks import python_columns

    start_time = time.perf_counter()
    preparer = merge.conn.dialect.identifier_preparer
    fields = list(merge.data_fields)
    sql = (f'INSERT INTO {preparer.format_table(merge.temp_table)} ({", ".join(preparer.quote(f) for f in fields)}) '
           f'VALUES ({", ".join("?" * len(fields))})')

    processors = _json_processors(merge)
    for f in _datetimeoffset_columns(merge):
        # fast_executemany writes an aware datetime as its wall clock with the offset of the time zone of this
        # machine. As text with its own offset, the server reads it right.
        processors[f] = lambda v: v.isoformat(sep=' ') if getattr(v, 'tzinfo', None) is not None else v

    if not merge.conn.in_transaction():
        merge.conn.begin()
    cursor = merge.conn.connection.dbapi_connection.cursor()
    cursor.fast_executemany = True
    try:
        for begin in range(0, merge.total_row_count, merge.chunk_size):
            end = min(begin + merge.chunk_size, merge.total_row_count)
            savepoint = merge.conn.begin_nested()
            try:
                columns = python_columns(merge, begin, end, processors=processors)
                cursor.executemany(sql, list(zip(*columns.values())))
            except Exception as e:
                savepoint.rollback()
                logger.debug(f'Rows {begin}..{end} could not be sent with fast_executemany ({e}), inserting them row by row')
                merge._insert_rows_to_temp(begin, end)
            else:
                savepoint.commit()
    finally:
        cursor.close()

    merge.temp_insert_time = time.perf_counter() - start_time
