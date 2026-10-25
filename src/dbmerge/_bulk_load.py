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
import sys
import time

from .dbmerge import logger

if TYPE_CHECKING:
    from .dbmerge import dbmerge


# The PostgreSQL drivers that take COPY ... FROM STDIN, each in its own way (see _copy).
COPY_DRIVERS = ('psycopg2', 'psycopg', 'pg8000')


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
        connection.register(view, arrow_table(merge, begin, end, processors=processors))
        try:
            merge.conn.exec_driver_sql(f'INSERT INTO {staging} ({columns}) SELECT {columns} FROM {view}')
        finally:
            connection.unregister(view)

    merge.temp_insert_time = time.perf_counter() - start_time


def _postgres_copy(merge: dbmerge):
    # COPY ... FROM STDIN through the connection the merge already holds, so the staging table - a TEMPORARY
    # one, seen only by its own session - is there, and the load belongs to the same transaction.
    from sqlalchemy import DateTime, LargeBinary
    from zoneinfo import ZoneInfo

    start_time = time.perf_counter()
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

    to_csv = _csv_by_arrow if _has_pyarrow() else _csv_by_csv_module
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
    driver = merge.conn.dialect.driver
    cursor = merge.conn.connection.dbapi_connection.cursor()
    try:
        if driver == 'psycopg2':
            cursor.copy_expert(sql, io.BytesIO(data))
        elif driver == 'psycopg':
            with cursor.copy(sql) as copy:
                copy.write(data)
        else:  # pg8000
            cursor.execute(sql, stream=io.BytesIO(data))
    finally:
        cursor.close()


def _json_processors(merge: dbmerge):
    # A JSON column gets its values through its own bind processor, as the row by row INSERT does: the
    # serializer of the engine, and None as the JSON literal null unless the type says none_as_null.
    from sqlalchemy import JSON
    processors = {}
    for f in merge.data_fields:
        column_type = merge.temp_table.c[f].type
        if isinstance(column_type, JSON):
            processors[f] = column_type.bind_processor(merge.conn.dialect) or (lambda v: v)
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
    cursor = merge.conn.connection.dbapi_connection.cursor()
    # timeout=0: the default of 30 seconds would cut a large load short. keep_nulls: a NULL stays NULL
    # rather than taking the default of its column.
    options = dict(column_mappings=fields, keep_nulls=True, timeout=0)

    offset_columns = _datetimeoffset_columns(merge)

    for begin in range(0, merge.total_row_count, merge.chunk_size):
        end = min(begin + merge.chunk_size, merge.total_row_count)
        if offset_columns and _has_non_utc_offsets(merge, begin, end, offset_columns):
            # A datetimeoffset keeps the offset a value came with, and the bulk copy of mssql-python can not
            # repeat that: from Arrow it writes every value in UTC (+00:00), from tuples it keeps the offset
            # but shifts the moment by it. Such rows go row by row, which stores them exactly.
            _warn_once('mssql-datetimeoffset',
                       'datetimeoffset values with an offset other than UTC are inserted row by row: the bulk copy '
                       'of mssql-python can not keep their offset')
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
    # row. A chunk that fails goes row by row; a failure part way through can not double rows silently,
    # since the staging table on MS SQL has a primary key.
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

    cursor = merge.conn.connection.dbapi_connection.cursor()
    cursor.fast_executemany = True
    try:
        for begin in range(0, merge.total_row_count, merge.chunk_size):
            end = min(begin + merge.chunk_size, merge.total_row_count)
            try:
                columns = python_columns(merge, begin, end, processors=processors)
                cursor.executemany(sql, list(zip(*columns.values())))
            except Exception as e:
                logger.debug(f'Rows {begin}..{end} could not be sent with fast_executemany ({e}), inserting them row by row')
                merge._insert_rows_to_temp(begin, end)
    finally:
        cursor.close()

    merge.temp_insert_time = time.perf_counter() - start_time
