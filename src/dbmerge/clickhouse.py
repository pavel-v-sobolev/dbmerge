"""
ClickHouse support for dbmerge.

ClickHouse does not fit the generic SQLAlchemy flow of dbmerge.py: CREATE TABLE needs an engine, a column
is not nullable unless declared so, no driver keeps a TEMPORARY table alive between the statements of
SQLAlchemy, and there are no transactions. So the generic flow hands the steps it can not do over to the
functions of this module: each step of dbmerge.py asks for its function by name (dbmerge._dialect_hook)
and goes the generic way only if the module has none. Each function gets the dbmerge instance and works
on its attributes, just like the method it stands in for.

The module is imported only when a ClickHouse engine or client is actually used, so clickhouse-connect
stays an optional dependency. ClickHouse is reached through clickhouse-connect only: its client runs every
statement here, whether dbmerge was given a "clickhousedb://" engine or the client itself.

The target table has to be a ReplacingMergeTree (plain, Replicated or Shared): a merge writes a changed
row as a new version of it, and this engine keeps only the last version of every key.

The data goes to the staging table in Arrow format, whatever form it came in: ClickHouse reads Arrow
columns as they are, while the generic row by row INSERT of SQLAlchemy is some 20 times slower there.
Without pyarrow it goes through the native protocol of clickhouse-connect, a few times slower than Arrow.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import re
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, Table, Column, func, literal, literal_column, tuple_, and_, or_
from sqlalchemy import select, insert, delete
from sqlalchemy import Boolean, SmallInteger, Integer, BigInteger, Float, Numeric, String, DateTime, Date
from sqlalchemy import Uuid, LargeBinary, JSON
from sqlalchemy.sql.util import ClauseAdapter

from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType, sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.sql import final

try:
    import pyarrow as pa
    import pyarrow.compute as pc
except ImportError:
    # Without pyarrow the staging table is loaded through the native protocol of the client instead,
    # a few times slower (see insert_data_to_temp).
    pa = pc = None

from .dbmerge import logger, IncorrectParameter, IncorrectDataError, NoKeyError
from ._chunks import arrow_table, python_columns
from ._bulk_load import warn_without_pyarrow

if TYPE_CHECKING:
    from .dbmerge import dbmerge


# The oldest server the merge works on: lightweight DELETE, Date32, Bool and FINAL in a subquery are all
# there by 23.8, the LTS release it is tested with.
MIN_SERVER_VERSION = (23, 8)

# Engines that keep one row per sorting key. Replicated and Shared (ClickHouse Cloud) are the same engine
# with the data kept on several replicas.
REPLACING_ENGINES = ('ReplacingMergeTree', 'ReplicatedReplacingMergeTree', 'SharedReplacingMergeTree')

# Columns whose value ClickHouse produces itself, so an INSERT can not write them.
COMPUTED_COLUMN_KINDS = ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')

# Types ClickHouse does not wrap in Nullable(): they have an empty value of their own instead.
NOT_NULLABLE_TYPE_PREFIXES = ('Array(', 'Map(', 'Tuple(', 'Nested(', 'JSON', 'Object(', 'Variant(', 'Dynamic')

# Rows per insert into the staging table, unless exec() is given a chunk_size. Every insert creates a part
# of the table, which is then merged in the background, so ClickHouse wants few large inserts: a million
# rows load in 0.12s as one insert and in 0.44s as inserts of 10,000 (the generic default), and a large
# load in small inserts piles up parts faster than they are merged, until the server refuses the insert.
DEFAULT_CHUNK_SIZE = 1_000_000

# Settings of every insert into the staging table.
# input_format_null_as_default=0: otherwise a NULL arriving for a non-nullable column is silently stored as
# 0, '' or 1970-01-01, while the other engines refuse it.
# async_insert=0: newer servers buffer inserts by default and answer only once the buffer is flushed, which
# makes a series of inserts many times slower. The data comes in large inserts already.
LOAD_SETTINGS = {'input_format_null_as_default': 0, 'async_insert': 0}


class _ClientConnection:
    """
    Stands in for the SQLAlchemy connection when dbmerge is given a clickhouse-connect client instead of
    an engine. The generic flow commits, rolls back and closes its connection at fixed points. ClickHouse
    has no transactions, so there is nothing to commit or roll back, and the client belongs to the caller,
    so it is not closed either.
    """
    closed = False

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def open_session(merge: dbmerge):
    if isinstance(merge.engine, Engine):
        # A pooled connection of the engine is held for the whole merge, so that closing it gives it back
        # to the pool. The statements themselves run through the clickhouse-connect client inside it.
        merge.conn = merge.engine.connect()
        merge.client = merge.conn.connection.dbapi_connection.client
        merge.ch_dialect = merge.engine.dialect
    else:
        merge.conn = _ClientConnection()
        merge.client = merge.engine
        # Only compiles statements here: SQLAlchemy expressions and types still need a dialect to render.
        # Without a DB-API behind it, the dialect would render parameters as :name, while the client binds
        # %(name)s - the paramstyle of its own DB-API, which an engine's dialect takes over from it.
        merge.ch_dialect = ClickHouseDialect(paramstyle='pyformat')

    server_version = _query(merge, 'SELECT version()')[0][0]
    if tuple(int(part) for part in server_version.split('.')[:2]) < MIN_SERVER_VERSION:
        raise IncorrectParameter(f'ClickHouse {server_version} is not supported: dbmerge needs '
                                 f'{".".join(map(str, MIN_SERVER_VERSION))} or newer.')

    # A schema is a database in ClickHouse. The current one is written out, so that the catalog can be
    # queried by name and every statement names its table in full.
    if merge.schema is None or (merge.source_table_name is not None and merge.source_schema is None):
        current_database = _query(merge, 'SELECT currentDatabase()')[0][0]
        merge.schema = merge.schema or current_database
        if merge.source_table_name is not None:
            merge.source_schema = merge.source_schema or current_database
    # A TEMPORARY staging table belongs to the session of the client and to no database (see
    # create_temp_table), so temp_schema is dropped quietly then, as on PostgreSQL. A client made without a
    # session (autogenerate_session_id=False, e.g. to be shared between threads) would lose such a table
    # right after creating it, so it gets a regular one, as delete_mode='delete' does.
    has_session = merge.client.get_client_setting('session_id') is not None
    if merge.delete_mode != 'delete' and has_session:
        merge.temp_schema = None
    elif merge.temp_schema is None:
        merge.temp_schema = merge.schema


def create_schema_if_not_exists(merge: dbmerge, schema_name):
    exists = _query(merge, 'SELECT count() FROM system.databases WHERE name = {name:String}',
                    name=schema_name)[0][0]
    if exists:
        return
    if not merge.can_create_schemas:
        raise IncorrectParameter(f'Database "{schema_name}" does not exist and can_create_schemas=False')
    logger.info(f'Creating database "{schema_name}".')
    merge.client.command(f'CREATE DATABASE IF NOT EXISTS {_quote(merge, schema_name)}')


def reflect_table(merge: dbmerge, table_name, schema):
    rows = _query(merge, 'SELECT engine, engine_full, sorting_key FROM system.tables '
                         'WHERE database = {db:String} AND name = {name:String}', db=schema, name=table_name)
    if not rows:
        return None
    engine, engine_full, sorting_key = rows[0]

    columns = []
    partition_columns = []
    computed_columns = []
    for name, type_name, default_kind, in_sorting_key, in_partition_key in _query(
            merge, 'SELECT name, type, default_kind, is_in_sorting_key, is_in_partition_key '
                   'FROM system.columns WHERE database = {db:String} AND table = {name:String} '
                   'ORDER BY position', db=schema, name=table_name):
        if in_partition_key:
            partition_columns.append(name)
        if default_kind in COMPUTED_COLUMN_KINDS:
            computed_columns.append(name)
            continue
        column_type = sqla_type_from_name(type_name)
        # The sorting key is what ReplacingMergeTree deduplicates by, so it plays the part of the primary
        # key: with no "key" argument, the generic flow takes the merge key from here.
        columns.append(Column(name, column_type, primary_key=bool(in_sorting_key),
                              nullable=column_type.nullable))

    # Built anew on every load, so that a column added in the meantime is picked up.
    existing = merge.metadata.tables.get(f'{schema}.{table_name}')
    if existing is not None:
        merge.metadata.remove(existing)
    table = Table(table_name, merge.metadata, *columns, schema=schema)

    # The version column is the first identifier among the engine arguments: the quoted ones are the
    # ZooKeeper path and replica name of a Replicated engine.
    identifiers = [a for a in _engine_arguments(engine_full, engine) if not a.startswith("'")]
    table.info['clickhouse'] = {
        'engine': engine,
        'sorting_key': sorting_key,
        'partition_columns': partition_columns,
        'computed_columns': computed_columns,
        'version_column': _unquote(identifiers[0]) if identifiers else None,
    }
    return table


def check_table(merge: dbmerge):
    # A table dbmerge is about to create gets the right layout by construction.
    if merge.table is None:
        return

    info = merge.table.info['clickhouse']
    table_name = merge.table_full_name

    if info['engine'] not in REPLACING_ENGINES:
        raise IncorrectParameter(
            f'Table "{table_name}" has engine {info["engine"]}, but dbmerge merges into ClickHouse only through '
            f'ReplacingMergeTree: a changed row is written as a new version of it, and other engines would keep '
            f'the old version next to it for good.')

    # ReplacingMergeTree keeps one row per value of the whole ORDER BY. A column there that is not in the
    # key splits one key into several rows, and a key column missing there merges different keys into one.
    sorting_key = [_unquote(_strip_parentheses(e))
                   for e in _split_top_level(_strip_parentheses(info['sorting_key']))]
    expressions = [e for e in sorting_key if e not in merge.table.c]
    if expressions:
        raise NoKeyError(f'ORDER BY of table "{table_name}" is ({info["sorting_key"]}), and {", ".join(expressions)} '
                         f'is not a column. It has to list exactly the merge key columns.')
    if set(sorting_key) != set(merge.key):
        raise NoKeyError(f'ORDER BY of table "{table_name}" is ({info["sorting_key"]}), but the merge key is '
                         f'{merge.key}. ReplacingMergeTree keeps one row per value of ORDER BY, so it has to '
                         f'list exactly the merge key columns.')

    # Rows are replaced only within a partition, so a key whose rows land in two partitions is never merged.
    outside_key = [c for c in info['partition_columns'] if c not in merge.key]
    if outside_key:
        raise IncorrectParameter(f'Table "{table_name}" is partitioned by {outside_key}, which are not in the merge '
                                 f'key: the versions of one key could land in different partitions and would never '
                                 f'be merged into one row.')

    # With a version column, the row with the greatest version wins, not the one written last. A new
    # version written without it would get the default 0 and lose to the row it replaces.
    version = info['version_column']
    if version is not None and version not in merge.data_fields and version != merge.merged_on_field:
        raise IncorrectParameter(f'Table "{table_name}" keeps the row with the greatest "{version}", but the data '
                                 f'does not have this column: every change would lose to the row it replaces. '
                                 f'Pass "{version}" in the data, or make it the merged_on_field.')

    # Columns ClickHouse computes itself can not be written. A source table built like the target has them
    # too, and their values are what the target computes anyway, so they are left out, as on the other
    # engines; data that brings them is refused.
    computed = [f for f in merge.new_fields if f in info['computed_columns']]
    if computed and merge.source_table is not None:
        logger.info(f'Columns {computed} are computed by ClickHouse in table "{table_name}", '
                    f'they are not merged from the source table.')
        for f in computed:
            del merge.new_fields[f]
            merge.data_fields.pop(f, None)
    elif computed:
        raise IncorrectParameter(f'Columns {computed} of table "{table_name}" are computed by ClickHouse '
                                 f'(MATERIALIZED, ALIAS or EPHEMERAL) and can not be written.')


def adapt_types(merge: dbmerge):
    for f in merge.new_fields:
        field_type = merge.new_fields[f]
        if isinstance(field_type, ChSqlaType):
            # A ClickHouse type given in data_types says everything about the column, nullability included.
            # One that comes from a source table is taken as it is too, but a key column is made not
            # nullable: ORDER BY refuses a nullable column, and a source such as a PostgreSQL table
            # declares every column nullable that is not NOT NULL there.
            if f in merge.key and f not in merge.given_data_types:
                merge._set_new_field_type(f, sqla_type_from_name(_not_nullable(field_type.name)))
            continue
        type_name = _clickhouse_type_name(f, field_type, detected=f not in merge.given_data_types)
        # A ClickHouse column is not nullable unless declared so, and a NULL written into it silently turns
        # into 0, '' or 1970-01-01. The key columns stay as they are: ORDER BY refuses nullable columns.
        if f not in merge.key:
            type_name = _nullable(type_name)
        merge._set_new_field_type(f, sqla_type_from_name(type_name))


def create_table(merge: dbmerge):
    fields = dict(merge.data_fields)
    fields.update({f: t for f, t in merge.new_fields.items() if f not in merge.data_fields})

    columns = []
    for f, field_type in fields.items():
        if f in merge.key:
            logger.info(f'Table field "{f}" - {field_type.name}, sorting key')
        else:
            logger.info(f'Table field "{f}" - {field_type.name}')
        columns.append(f'{_quote(merge, f)} {field_type.name}')

    order_by = ', '.join(_quote(merge, k) for k in merge.key)
    merge.client.command(f'CREATE TABLE {_table_name(merge, merge.schema, merge.table_name)} '
                         f'({", ".join(columns)}) ENGINE = ReplacingMergeTree ORDER BY ({order_by})')
    merge.table = reflect_table(merge, merge.table_name, merge.schema)


def add_column(merge: dbmerge, field_name, field_type):
    # Only the table metadata changes, the existing parts are not rewritten: the rows already in the table
    # read the new column as NULL.
    merge.client.command(f'ALTER TABLE {_table_name(merge, merge.schema, merge.table_name)} '
                         f'ADD COLUMN {_quote(merge, field_name)} {field_type.name}')


def now():
    # now() has whole seconds only, the audit timestamps keep microseconds like on the other engines.
    return func.now64(literal_column('6'))


def database_time(merge: dbmerge):
    return _scalar(merge, select(now()))


def create_temp_table(merge: dbmerge, temp_table_name):
    # A TEMPORARY table lives in the session of the clickhouse-connect client, which runs every statement
    # here. It is in no database, so nobody else sees it, and the server drops it with the session, so a
    # crashed process leaves nothing behind. (Through SQLAlchemy it would not live: clickhouse-connect
    # drops the TEMPORARY prefix from its DDL, and clickhouse-driver opens a new session per statement.)
    #
    # With delete_mode='delete' it has to be a regular table: DELETE runs as a mutation, which does not see
    # temporary tables. A leftover of a crashed process is then recognized by its name (TEMP_TABLE_PREFIX).
    #
    # The columns are those of the data, typed as in the target, so that a value the target can not hold
    # is refused already on the load (see LOAD_SETTINGS).
    #
    # ORDER BY tuple(): the staging rows are only scanned and joined, sorting them on insert costs time
    # for nothing.
    columns = [Column(f, merge.table.c[f].type) for f in merge.data_fields]
    columns_sql = ', '.join(f'{_quote(merge, c.name)} {c.type.name}' for c in columns)
    if merge.temp_schema is None:
        merge.client.command(f'CREATE TEMPORARY TABLE {_quote(merge, temp_table_name)} '
                             f'({columns_sql}) ENGINE = MergeTree ORDER BY tuple()')
    else:
        merge.client.command(f'CREATE TABLE {_table_name(merge, merge.temp_schema, temp_table_name)} '
                             f'({columns_sql}) ENGINE = MergeTree ORDER BY tuple()')
    merge.temp_table = Table(temp_table_name, merge.metadata, *columns, schema=merge.temp_schema)


def insert_data_to_temp(merge: dbmerge):
    start_time = time.perf_counter()
    fields = list(merge.data_fields)
    # dicts and lists headed for a string column go there as JSON text
    json_fields = {f for f in fields if _base_type_name(merge.temp_table.c[f].type.name) == 'String'}

    if pa is not None:
        time_zones = _naive_time_zones(merge)
        for begin in range(0, merge.total_row_count, merge.chunk_size):
            end = min(begin + merge.chunk_size, merge.total_row_count)
            chunk = _localize_naive_timestamps(arrow_table(merge, begin, end, nested_as_json=json_fields), time_zones)
            chunk = _nulls_without_nan(merge, chunk)
            merge.client.insert_arrow(merge.temp_table.name, chunk, database=merge.temp_schema, settings=LOAD_SETTINGS)
    else:
        # The native protocol refuses a None for a non-nullable column on the client side. A naive datetime it
        # converts by the time zone of the client, so it is pinned to that of the column first, as above.
        warn_without_pyarrow(merge.dialect_name)
        processors = {f: _localizer(zone) for f, zone in _naive_time_zones(merge).items()}
        for begin in range(0, merge.total_row_count, merge.chunk_size):
            end = min(begin + merge.chunk_size, merge.total_row_count)
            columns = python_columns(merge, begin, end, nested_as_json=json_fields, processors=processors)
            merge.client.insert(merge.temp_table.name, [columns[f] for f in fields], column_names=fields,
                                column_oriented=True, database=merge.temp_schema, settings=LOAD_SETTINGS)
    merge.temp_insert_time = time.perf_counter() - start_time


def insert_source_table_to_temp(merge: dbmerge):
    # The source is any table or view ClickHouse can read, a table of a PostgreSQL or MySQL database engine
    # among them: the server copies it into the staging table itself.
    start_time = time.perf_counter()
    source = merge.source_table
    rows = select(*[source.c[f] for f in merge.data_fields])
    if merge.source_condition is not None:
        rows = rows.where(merge.source_condition)
    # A ReplacingMergeTree source still holds the replaced versions of a row until its parts are merged:
    # only the current one is read.
    if source.info['clickhouse']['engine'] in REPLACING_ENGINES:
        rows = final(rows, source)
    statement = insert(merge.temp_table).from_select(list(merge.data_fields), rows)
    # insert_null_as_default=0: a NULL for a column that is not Nullable is refused, as it is in data
    # (LOAD_SETTINGS); INSERT ... SELECT would store 0, '' or 1970-01-01 in its place otherwise.
    _, merge.total_row_count = _insert(merge, statement, settings={'insert_null_as_default': 0})
    merge.temp_insert_time = time.perf_counter() - start_time


def drop_table(merge: dbmerge, table):
    if table.schema is None:
        merge.client.command(f'DROP TEMPORARY TABLE IF EXISTS {_quote(merge, table.name)}')
    else:
        # SYNC: an Atomic database keeps a dropped table on disk for 8 more minutes by default, and a
        # staging table is dropped after every merge.
        merge.client.command(f'DROP TABLE IF EXISTS {_table_name(merge, table.schema, table.name)} SYNC')


def insert_changed_rows(merge: dbmerge):
    # Stands in for the UPDATE of the generic flow. ClickHouse has no UPDATE fit for this: a changed row is
    # written as a new version of itself, and ReplacingMergeTree keeps only the last version of a key.
    # It is an insert, so its time is counted as insert time (insert_new_rows adds its own to it).
    start_time = time.perf_counter()
    merge.update_time = 0
    merge.updated_row_count = 0

    non_key_cols = [c for c in merge.data_fields if c not in merge.key and c not in merge.skip_update_fields]
    manage_mark_reset = merge.delete_mark_field is not None and not merge.delete_mark_from_data

    staging = merge.temp_table.alias('s')
    current = _current_rows(merge)

    # The same rules as in the generic flow: skip_compare_fields are written but not compared, and a row
    # marked as deleted that came back is picked up to reset the mark.
    differences = [_differs(current.c[c], staging.c[c]) for c in non_key_cols if c not in merge.skip_compare_fields]
    if manage_mark_reset:
        mark = current.c[merge.delete_mark_field]
        differences.append(or_(mark != literal(merge.delete_mark_active_value), mark.is_(None)))
    if not differences:
        merge.insert_time = time.perf_counter() - start_time
        return

    where = or_(*differences)
    if merge.update_condition is not None:
        where = and_(where, _adapt(merge.update_condition, current, staging))

    values = []
    for column in merge.table.c:
        name = column.name
        if name in merge.key or name in non_key_cols:
            value = staging.c[name]
        elif name == merge.merged_on_field:
            value = now()
        elif manage_mark_reset and name == merge.delete_mark_field:
            value = literal(merge.delete_mark_active_value)
        else:
            # The whole row is written anew, so what the update leaves alone keeps its current value:
            # skip_update_fields, the inserted_on stamp, the columns the data does not carry.
            value = current.c[name]
        values.append(value)

    rows = select(*values).select_from(staging.join(current, _same_key(merge, staging, current))).where(where)
    statement = insert(merge.table).from_select([c.name for c in merge.table.c], rows)
    merge.update_sql, merge.updated_row_count = _insert(merge, statement)
    merge.insert_time = time.perf_counter() - start_time


def insert_new_rows(merge: dbmerge):
    start_time = time.perf_counter()
    staging = merge.temp_table.alias('s')

    # A key is new when the target has no version of it at all, so the target is read without FINAL and
    # only by its key columns.
    target = merge.table.alias('e')
    existing_keys = select(*[target.c[k] for k in merge.key]).where(
        _key(merge, target).in_(_staging_keys(merge)))
    where = _key(merge, staging).not_in(existing_keys)
    if merge.insert_condition is not None:
        where = and_(where, _adapt(merge.insert_condition, None, staging))

    columns = list(merge.data_fields)
    values = [staging.c[f] for f in merge.data_fields]
    if merge.merged_on_field is not None:
        columns.append(merge.merged_on_field)
        values.append(now())
    if merge.inserted_on_field is not None:
        columns.append(merge.inserted_on_field)
        values.append(now())
    if merge.delete_mark_field is not None and not merge.delete_mark_from_data:
        columns.append(merge.delete_mark_field)
        values.append(literal(merge.delete_mark_active_value))

    statement = insert(merge.table).from_select(columns, select(*values).where(where))
    merge.insert_sql, merge.inserted_row_count = _insert(merge, statement)
    merge.insert_time += time.perf_counter() - start_time


def insert_marked_rows(merge: dbmerge):
    # Marking is an insert too: the current version of a row missing from the source is written again with
    # the mark set, and replaces the unmarked one.
    start_time = time.perf_counter()
    target = merge.table.alias('t')
    mark = target.c[merge.delete_mark_field]

    # Rows already marked are left alone, so a repeated merge neither counts them again nor moves their
    # merged_on - as in the generic flow.
    where = [_key(merge, target).not_in(_staging_keys(merge)),
             or_(mark != literal(merge.delete_mark_deleted_value), mark.is_(None))]
    if merge.delete_condition is not None:
        where.append(_adapt(merge.delete_condition, target, None))

    values = []
    for column in merge.table.c:
        name = column.name
        if name == merge.delete_mark_field:
            value = literal(merge.delete_mark_deleted_value)
        elif name == merge.merged_on_field:
            value = now()
        elif name in merge.delete_mark_values:
            value = literal(merge.delete_mark_values[name])
        else:
            value = target.c[name]
        values.append(value)

    rows = final(select(*values).where(and_(*where)), target)
    statement = insert(merge.table).from_select([c.name for c in merge.table.c], rows)
    merge.delete_sql, merge.deleted_row_count = _insert(merge, statement)
    merge.delete_time = time.perf_counter() - start_time


def delete_rows_missing_in_source(merge: dbmerge):
    # A lightweight DELETE: it masks the rows at once, and they are gone physically with the next merges
    # of the parts. Being a mutation, it does not see temporary tables, which is why the staging table is a
    # regular one in this mode (see create_temp_table).
    start_time = time.perf_counter()
    target = merge.table.alias('t')
    where = [_key(merge, target).not_in(_staging_keys(merge))]
    if merge.delete_condition is not None:
        where.append(_adapt(merge.delete_condition, target, None))

    # The keys are taken from the current versions (FINAL), and every version of them is deleted. A
    # condition put on the rows themselves would delete only the versions it matches, and an older
    # version of the same key that it does not match would come back in place of the deleted one.
    doomed_keys = final(select(*[target.c[k] for k in merge.key]).where(and_(*where)), target)

    # DELETE reports no row count, so the keys are counted first. With others writing into the table at
    # the same time, this is an estimate rather than an exact figure.
    merge.deleted_row_count = _scalar(merge, select(func.count()).select_from(doomed_keys.subquery()))
    statement = delete(merge.table).where(_key(merge, merge.table).in_(doomed_keys))
    merge.delete_sql, _ = _execute(merge, statement)
    merge.delete_time = time.perf_counter() - start_time


def _current_rows(merge: dbmerge):
    """The current version (FINAL) of every target row whose key is in the staging table, as a subquery."""
    # FINAL goes into a subquery: on servers before 24.x it is silently ignored on the right side of a JOIN.
    target = merge.table.alias('tf')
    rows = select(*target.c).where(_key(merge, target).in_(_staging_keys(merge)))
    return final(rows, target).subquery('t')


def _staging_keys(merge: dbmerge):
    staging = merge.temp_table.alias('sk')
    return select(*[staging.c[k] for k in merge.key])


def _key(merge: dbmerge, table):
    return tuple_(*[table.c[k] for k in merge.key])


def _same_key(merge: dbmerge, left, right):
    return and_(*[left.c[k] == right.c[k] for k in merge.key])


def _differs(left, right):
    # IS DISTINCT FROM appeared only in recent servers, and <=> worked only in JOIN ON before, so it is
    # spelled out: either the values differ, or exactly one of them is NULL.
    return or_(left != right, func.isNull(left) != func.isNull(right))


def _adapt(condition, target, staging):
    # The conditions of exec() are built on merge.table and merge.temp_table, while the statements here
    # read those tables under aliases, so the condition is moved onto the aliases.
    if target is not None:
        condition = ClauseAdapter(target).traverse(condition)
    if staging is not None:
        condition = ClauseAdapter(staging).traverse(condition)
    return condition


def _compile(merge: dbmerge, statement):
    # render_postcompile: the value list of an in_() in a condition is expanded into its parameters only when
    # SQLAlchemy executes a statement itself; the client here would get a __[POSTCOMPILE_...] placeholder.
    return statement.compile(dialect=merge.ch_dialect, compile_kwargs={'render_postcompile': True})


def _execute(merge: dbmerge, statement, settings=None):
    compiled = _compile(merge, statement)
    sql = str(compiled)
    return sql, merge.client.command(sql, parameters=compiled.params, settings=settings)


def _scalar(merge: dbmerge, statement):
    compiled = _compile(merge, statement)
    return merge.client.query(str(compiled), parameters=compiled.params).result_rows[0][0]


# The SELECT of every INSERT ... SELECT here gives no aliases to its columns: INSERT takes them by position,
# and ClickHouse sees a SELECT alias also in WHERE - before 24.x it would take "1 AS Deleted" for the column
# t.Deleted of the condition and fail.


def _insert(merge: dbmerge, statement, settings=None):
    # A token of its own for every insert: a Replicated or Shared table drops an insert whose data repeats
    # one of the recent ones, which would lose a value that returned to what it once was.
    settings = {**(settings or {}), 'insert_deduplication_token': uuid.uuid4().hex}
    sql, summary = _execute(merge, statement, settings=settings)
    return sql, summary.written_rows


def _query(merge: dbmerge, sql, **parameters):
    return merge.client.query(sql, parameters=parameters).result_rows


def _quote(merge: dbmerge, name):
    return merge.ch_dialect.identifier_preparer.quote_identifier(name)


def _table_name(merge: dbmerge, schema, name):
    return f'{_quote(merge, schema)}.{_quote(merge, name)}'


def _clickhouse_type_name(field, field_type, detected):
    # A type detected from the data is created so that no value changes on the way in. A type given in
    # data_types is the caller's decision, so it gets its closest ClickHouse counterpart and nothing more,
    # just as the other engines never substitute it.
    if isinstance(field_type, Boolean):
        return 'Bool'
    if isinstance(field_type, SmallInteger):
        return 'Int16'
    if isinstance(field_type, BigInteger):
        return 'Int64'
    if isinstance(field_type, Integer):
        return 'Int32'
    if isinstance(field_type, Float):
        return 'Float64'
    if isinstance(field_type, Numeric):
        # Decimal needs a precision. Without one, a double is what the value already is in Python.
        if field_type.precision is None:
            return 'Float64'
        return f'Decimal({field_type.precision}, {field_type.scale or 0})'
    if isinstance(field_type, String):
        # A ClickHouse string has no length, and FixedString(n) would pad every value with zero bytes.
        return 'String'
    if isinstance(field_type, DateTime):
        # DateTime keeps whole seconds only. An aware value is stored as UTC, a naive one as is.
        if detected:
            return "DateTime64(6, 'UTC')" if field_type.timezone else 'DateTime64(6)'
        return "DateTime('UTC')" if field_type.timezone else 'DateTime'
    if isinstance(field_type, Date):
        # Date covers 1970-2149 and silently turns an earlier day into 1970-01-01, Date32 covers 1900-2299.
        return 'Date32' if detected else 'Date'
    if isinstance(field_type, Uuid):
        return 'UUID'
    if isinstance(field_type, (LargeBinary, JSON)):
        return 'String'
    raise IncorrectDataError(f'Column "{field}" has type {field_type!r}, which has no ClickHouse counterpart here. '
                             f'Give it a ClickHouse type in data_types, one of '
                             f'clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes.')


def _nullable(type_name):
    if type_name.startswith('Nullable(') or type_name.startswith(NOT_NULLABLE_TYPE_PREFIXES):
        return type_name
    # Nullable goes inside LowCardinality, never around it.
    if type_name.startswith('LowCardinality('):
        inner = type_name[len('LowCardinality('):-1]
        return type_name if inner.startswith('Nullable(') else f'LowCardinality(Nullable({inner}))'
    return f'Nullable({type_name})'


def _not_nullable(type_name):
    # The type name without its Nullable wrapper: Nullable(Int32) -> Int32,
    # LowCardinality(Nullable(String)) -> LowCardinality(String). Only the outer level is
    # unwrapped, Array(Nullable(Int32)) stays as it is.
    if type_name.startswith('Nullable('):
        return type_name[len('Nullable('):-1]
    if type_name.startswith('LowCardinality(Nullable('):
        return 'LowCardinality(' + type_name[len('LowCardinality(Nullable('):-2] + ')'
    return type_name


def _base_type_name(type_name):
    # Nullable and LowCardinality only wrap the type that holds the values.
    while type_name.startswith(('Nullable(', 'LowCardinality(')):
        type_name = type_name[type_name.index('(') + 1:-1]
    return type_name


def _naive_time_zones(merge: dbmerge):
    """The time zone a datetime without one means, for every date and time column of the staging table."""
    zones = {}
    session_zone = None
    for column in merge.temp_table.c:
        base = _base_type_name(column.type.name)
        if not base.startswith('DateTime'):
            continue
        # The column's own time zone, as in DateTime64(6, 'UTC'), or else the one of the session.
        match = re.search(r"'([^']+)'", base)
        if match is not None:
            zones[column.name] = match.group(1)
        else:
            if session_zone is None:
                session_zone = _query(merge, 'SELECT timezone()')[0][0]
            zones[column.name] = session_zone
    return zones


def _nulls_without_nan(merge: dbmerge, table):
    # pandas keeps a missing integer as NaN in a float column. Arrow marks it null, but the NaN stays in the
    # data buffer under the mark, and servers before 25.x convert that buffer to the integer column without
    # looking at the marks: "Unexpected inf or nan to integer conversion". Such a column is cast to the
    # integer type here, truncating as ClickHouse itself does. A NaN or an infinity that is a real value,
    # not a null, is left for ClickHouse to refuse.
    integer_types = {'Int8': pa.int8(), 'Int16': pa.int16(), 'Int32': pa.int32(), 'Int64': pa.int64(),
                     'UInt8': pa.uint8(), 'UInt16': pa.uint16(), 'UInt32': pa.uint32(), 'UInt64': pa.uint64()}
    for i, name in enumerate(table.column_names):
        column = table.column(i)
        target = integer_types.get(_base_type_name(merge.temp_table.c[name].type.name))
        if target is None or not pa.types.is_floating(column.type) or column.null_count == 0:
            continue
        if pc.any(pc.invert(pc.is_finite(column))).as_py():
            continue
        table = table.set_column(i, name, column.cast(target, safe=False))
    return table


def _localizer(zone):
    tz = ZoneInfo(zone)
    return lambda v: v.replace(tzinfo=tz) if isinstance(v, datetime) and v.tzinfo is None else v


def _localize_naive_timestamps(table, time_zones):
    # A timestamp without a time zone reaches ClickHouse from Arrow as UTC: 12:00 would be stored as
    # 12:00 UTC and read back as 15:00 in Moscow. A naive value means the wall clock of the column - which
    # is also how ClickHouse reads it from text - so it is pinned to the column's time zone first.
    for i, name in enumerate(table.column_names):
        column = table.column(i)
        if name in time_zones and pa.types.is_timestamp(column.type) and column.type.tz is None:
            table = table.set_column(i, name, pc.assume_timezone(column, time_zones[name]))
    return table


def _unquoted_chars(text):
    """Yields (position, character, bracket depth) for the characters of an expression outside quotes."""
    depth = 0
    quote = None
    escaped = False
    for i, ch in enumerate(text):
        if quote is not None:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ("'", '`', '"'):
            quote = ch
            continue
        if ch in '([':
            depth += 1
        elif ch in ')]':
            depth -= 1
        yield i, ch, depth


def _split_top_level(text):
    """Splits a ClickHouse expression list, such as a sorting key, at the commas outside brackets and quotes."""
    parts = []
    start = 0
    for i, ch, depth in _unquoted_chars(text):
        if ch == ',' and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    last = text[start:].strip()
    if last or parts:
        parts.append(last)
    return parts


def _engine_arguments(engine_full, engine):
    # engine_full starts with the engine itself: "ReplacingMergeTree(ver) ORDER BY ...", or with no
    # parentheses at all when the engine has no arguments.
    text = engine_full[len(engine):] if engine_full.startswith(engine) else ''
    if not text.startswith('('):
        return []
    return _split_top_level(text[1:_closing_parenthesis(text)])


def _closing_parenthesis(text):
    """Position of the parenthesis that closes the one text starts with, -1 if there is none."""
    for i, ch, depth in _unquoted_chars(text):
        if ch == ')' and depth == 0:
            return i
    return -1


def _strip_parentheses(expression):
    # ClickHouse keeps the parentheses of a one-column ORDER BY (k) in sorting_key, while it shows a
    # tuple (a, b) without them.
    while expression.startswith('(') and _closing_parenthesis(expression) == len(expression) - 1:
        expression = expression[1:-1].strip()
    return expression


def _unquote(identifier):
    if len(identifier) >= 2 and identifier[0] in ('`', '"') and identifier[-1] == identifier[0]:
        quote = identifier[0]
        return identifier[1:-1].replace('\\' + quote, quote).replace(quote * 2, quote)
    return identifier
