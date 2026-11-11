"""Oracle hooks: correlated updates, LOB comparison and session-local staging data.

The usual INSERT/DELETE/mark flow is retained. A global temporary staging table
keeps its rows across step commits; its definition is explicitly dropped on close.
Schemas are existing Oracle users, never users created implicitly by dbmerge.
"""

import time
from contextlib import nullcontext

import sqlalchemy
from sqlalchemy import Column, Table, Text, DateTime, JSON, LargeBinary, text
from sqlalchemy import and_, or_, select, update, func
from sqlalchemy.dialects import oracle as _oracle_dialect
from sqlalchemy.dialects.oracle import TIMESTAMP, CLOB, NCLOB, BLOB, LONG, BINARY_DOUBLE

from .dbmerge import IncorrectParameter, _is_string_without_length, _is_numeric_without_precision


def _json_available():
    # SQLAlchemy grows an Oracle JSON type only in 2.1. On 2.0 a native JSON
    # column reflects as NullType, from which a staging table can not be built.
    return getattr(_oracle_dialect, 'JSON', None) is not None


def _unsupported_json_message(column):
    return (f'Oracle JSON column "{column}" needs SQLAlchemy 2.1 or newer '
            f'(found {sqlalchemy.__version__}). Upgrade, or serialize the values '
            'to JSON strings and declare Text/CLOB instead.')


def create_schema_if_not_exists(merge, schema_name):
    exists = merge.conn.dialect.has_schema(merge.conn, schema_name)
    merge.conn.commit()
    if not exists:
        raise IncorrectParameter(
            f'Oracle schema "{schema_name}" does not exist. Oracle schemas are users; '
            'ask the DBA to create the user and grant table privileges first. '
            'dbmerge does not create Oracle users, even with can_create_schemas=True.')


def adapt_types(merge):
    for name, field_type in list(merge.new_fields.items()):
        if name in merge.key and isinstance(field_type, (Text, CLOB, NCLOB, LargeBinary, BLOB, LONG, JSON)):
            raise IncorrectParameter(f'Oracle merge key "{name}" must not be a LOB, LONG or JSON column.')
        if isinstance(field_type, JSON):
            if not _json_available():
                raise IncorrectParameter(_unsupported_json_message(name))
            # SQLAlchemy 2.1 creates and reflects native JSON itself.
            continue
        if name in merge.given_data_types:
            continue
        if _is_string_without_length(field_type):
            if name in merge.key:
                raise IncorrectParameter(
                    f'Oracle merge key "{name}" needs an explicit length: '
                    f'data_types={{"{name}": String(255)}}.')
            merge._set_new_field_type(name, CLOB())
        elif isinstance(field_type, DateTime):
            # Generic DateTime becomes Oracle DATE, which loses fractional seconds.
            merge._set_new_field_type(name, TIMESTAMP(timezone=field_type.timezone))
        elif _is_numeric_without_precision(field_type):
            # NUMERIC without a scale is an integer on Oracle. An inferred
            # Python float needs a real 8-byte floating-point column instead.
            merge._set_new_field_type(name, BINARY_DOUBLE())


def check_table(merge):
    if merge.table is None:
        return
    for name in merge.key:
        if isinstance(merge.table.c[name].type, (Text, CLOB, NCLOB, LargeBinary, BLOB, LONG, JSON)):
            raise IncorrectParameter(f'Oracle merge key "{name}" must not be a LOB, LONG or JSON column.')
    for name in merge.data_fields:
        if name not in merge.table.c:
            continue
        field_type = merge.table.c[name].type
        if isinstance(field_type, LONG):
            raise IncorrectParameter(f'Oracle column "{name}" has unsupported type {field_type}.')
        if isinstance(field_type, JSON) and not _json_available():
            raise IncorrectParameter(_unsupported_json_message(name))


def reflect_table(merge, table_name, schema):
    # Native JSON columns reflect as NullType on SQLAlchemy 2.0 and as a JSON
    # type from 2.1 on. The staging table copies every target column, so on 2.0
    # any table with a native JSON column can not be merged; the columns are
    # named in a clear error instead of a cryptic NullType DDL failure.
    table_exists = merge.inspector.has_table(table_name, schema)
    if not table_exists:
        return None
    json_columns = _native_json_columns(merge.conn, table_name, schema)
    if json_columns and not _json_available():
        raise IncorrectParameter(
            f'Oracle table "{schema}.{table_name}" has native JSON column(s) '
            f'{sorted(json_columns)}, which need SQLAlchemy 2.1 or newer '
            f'(found {sqlalchemy.__version__}). Upgrade, or serialize the values '
            'to JSON strings and use Text/CLOB columns instead.')
    return Table(table_name, merge.metadata, autoload_with=merge.engine, schema=schema,
                 extend_existing=True)


def _native_json_columns(conn, table_name, schema):
    # ALL_TAB_COLUMNS carries the physical column type: only a native JSON column
    # is 'JSON'. ALL_JSON_COLUMNS would also list text columns that merely have an
    # IS JSON check constraint, so it is not used here.
    if schema:
        owner = schema.upper()
    else:
        owner = conn.execute(text("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual")).scalar()
    return list(conn.execute(text(
        "SELECT column_name FROM all_tab_columns "
        "WHERE owner = :owner AND table_name = :table_name AND data_type = 'JSON' "
        "ORDER BY column_id"), {'owner': owner, 'table_name': table_name.upper()}).scalars())


def create_temp_table(merge, table_name):
    # Oracle 12.1 and earlier have a 30-byte identifier limit.
    limit = merge.conn.dialect.max_identifier_length
    if len(table_name.encode('utf-8')) > limit:
        table_name = merge._truncate_to_bytes(table_name, limit - 9) + '_' + merge.unique_id
    cols = [Column(c.name, c.type, primary_key=c.name in merge.key, autoincrement=False)
            for c in merge.table.c]
    merge.conn.commit()
    merge.temp_table = Table(table_name, merge.metadata, *cols, schema=merge.temp_schema,
                             prefixes=['GLOBAL TEMPORARY'], oracle_on_commit='PRESERVE ROWS')
    merge.temp_table.create(merge.conn, checkfirst=False)
    merge.conn.commit()


def drop_table(merge, table):
    # PRESERVE ROWS binds the session to this GTT even after COMMIT. TRUNCATE
    # releases that binding before DROP (otherwise Oracle raises ORA-14452).
    with merge.engine.connect() if merge.conn.closed else nullcontext(merge.conn) as conn:
        conn.rollback()
        name = conn.dialect.identifier_preparer.format_table(table)
        conn.exec_driver_sql(f'TRUNCATE TABLE {name}')
        table.drop(conn, checkfirst=False)
        conn.commit()


def _different(left, right):
    if isinstance(left.type, (Text, CLOB, NCLOB, LargeBinary, BLOB)):
        return or_(func.dbms_lob.compare(left, right) != 0,
                   and_(left.is_(None), right.is_not(None)),
                   and_(left.is_not(None), right.is_(None)))
    if isinstance(left.type, JSON):
        # JSON_EQUAL compares documents semantically, where the stored binary
        # form may differ by key order or whitespace; SQL NULL is not a JSON
        # document, so it is handled on its own.
        return or_(and_(left.is_not(None), right.is_not(None), func.json_equal(left, right) == 0),
                   and_(left.is_(None), right.is_not(None)),
                   and_(left.is_not(None), right.is_(None)))
    return left.is_distinct_from(right)


def insert_changed_rows(merge):
    """UPDATE with correlated scalar SELECTs and EXISTS, not UPDATE ... FROM.

    The staging primary key ensures each scalar subquery returns at most one row.
    EXISTS also prevents unmatched target rows from being overwritten with NULL.
    """
    start = time.perf_counter()
    target, source = merge.table, merge.temp_table
    fields = [f for f in merge.data_fields if f not in merge.key and f not in merge.skip_update_fields]
    join = and_(*(target.c[f] == source.c[f] for f in merge.key))
    changes = [_different(target.c[f], source.c[f]) for f in fields if f not in merge.skip_compare_fields]
    managed_mark = merge.delete_mark_field is not None and not merge.delete_mark_from_data
    if managed_mark:
        changes.append(target.c[merge.delete_mark_field].is_distinct_from(merge.delete_mark_active_value))
    if not changes:
        merge.updated_row_count = 0
        merge.update_time = 0
        return
    guard = [join, or_(*changes)]
    if merge.update_condition is not None:
        guard.append(merge.update_condition)
    match = select(1).select_from(source).where(*guard).correlate(target).exists()
    values = {target.c[f]: select(source.c[f]).where(join).correlate(target).scalar_subquery()
              for f in fields}
    if managed_mark:
        values[target.c[merge.delete_mark_field]] = merge.delete_mark_active_value
    if merge.merged_on_field is not None:
        values[target.c[merge.merged_on_field]] = merge._now()
    statement = update(target).values(values).where(match)
    merge.update_sql = str(statement.compile(dialect=merge.conn.dialect))
    merge.updated_row_count = merge._execute_dml(statement)
    merge.update_time = time.perf_counter() - start