"""
DuckDB support for dbmerge: reading a table from the catalog, and the time of the database.

DuckDB goes through the generic flow of dbmerge.py, all but two steps (see dbmerge._dialect_hook):
database_time reads now() without its time zone, and reflect_table stands in for the SQLAlchemy reflection.

duckdb_engine, the SQLAlchemy dialect of DuckDB, is built on the PostgreSQL one and reflects a table through
the pg_catalog views DuckDB emulates, which falls short:
- under SQLAlchemy 2.1 it fails outright: the reflection reads pg_catalog.pg_collation and the REGCLASS
  type, and DuckDB has neither (duckdb/duckdb#27168);
- the primary key is never found, so the merge key could not be taken from the table;
- DOUBLE comes back as FLOAT, which DuckDB takes for a 4-byte float: the staging table, typed after the
  target, would cut every value of a DOUBLE column to single precision. TIMESTAMP WITH TIME ZONE comes
  back without the time zone, JSON as a string, lists, structs and maps as NullType.
So the table is read from the catalog functions of DuckDB itself, duckdb_columns() and duckdb_constraints().

The module is imported only when a DuckDB engine is used.
"""

import re

from sqlalchemy import Table, Column, MetaData, PrimaryKeyConstraint, text, select, cast
from sqlalchemy import Boolean, SmallInteger, Integer, BigInteger, REAL, Double, Numeric, String, LargeBinary
from sqlalchemy import Date, Time, DateTime, Interval, Uuid, JSON
from sqlalchemy.types import UserDefinedType


# DuckDB type names, as duckdb_columns() spells them, and the SQLAlchemy types that create the same column.
# JSON is a type of its own, so that a dict or a list is written as JSON text, as into a JSON column that
# dbmerge creates.
TYPES = {
    'BOOLEAN': Boolean,
    'SMALLINT': SmallInteger,
    'INTEGER': Integer,
    'BIGINT': BigInteger,
    'FLOAT': REAL,
    'DOUBLE': Double,
    'VARCHAR': String,
    'BLOB': LargeBinary,
    'DATE': Date,
    'TIME': Time,
    'TIMESTAMP': DateTime,
    'TIMESTAMP WITH TIME ZONE': lambda: DateTime(timezone=True),
    'INTERVAL': Interval,
    'UUID': Uuid,
    'JSON': JSON,
}


class DuckDBType(UserDefinedType):
    """A DuckDB type with no SQLAlchemy counterpart (LIST, STRUCT, MAP, ENUM, TIMESTAMP_NS...), created as DuckDB names it."""
    cache_ok = True

    def __init__(self, spec):
        self.spec = spec

    def get_col_spec(self, **kw):
        return self.spec


def reflect_table(merge, table_name, schema):
    return read_table(merge.conn, merge.metadata, table_name, schema)


def database_time(merge):
    # DuckDB's now() is a TIMESTAMP WITH TIME ZONE, and turning that into Python needs pytz, which DuckDB
    # does not install. dbmerge reads this time only to name the staging table, and the local time will do.
    return merge.conn.scalar(select(cast(merge._now(), DateTime)))


def read_table(connection, metadata: MetaData, table_name, schema):
    """The table or view as a SQLAlchemy Table in metadata, read anew; None if there is none."""
    params = {'schema': schema, 'table': table_name}
    columns = connection.execute(text(
        "SELECT column_name, data_type, is_nullable FROM duckdb_columns() "
        "WHERE database_name = current_database() AND schema_name = COALESCE(:schema, current_schema()) "
        "AND table_name = :table ORDER BY column_index"), params).all()
    if not columns:
        return None
    key = connection.execute(text(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE database_name = current_database() AND schema_name = COALESCE(:schema, current_schema()) "
        "AND table_name = :table AND constraint_type = 'PRIMARY KEY'"), params).scalar() or []

    # A table already in metadata is replaced, so that a column added since the last read is picked up.
    existing = metadata.tables.get(table_name if schema is None else f'{schema}.{table_name}')
    if existing is not None:
        metadata.remove(existing)
    return Table(table_name, metadata,
                 *[Column(name, _column_type(connection.dialect, data_type), nullable=nullable)
                   for name, data_type, nullable in columns],
                 *([PrimaryKeyConstraint(*key)] if key else []),
                 schema=schema)


def _column_type(dialect, data_type):
    if data_type in TYPES:
        return TYPES[data_type]()
    decimal = re.fullmatch(r'DECIMAL\((\d+),(\d+)\)', data_type)
    if decimal:
        return Numeric(int(decimal.group(1)), int(decimal.group(2)))
    # TINYINT, HUGEINT and the unsigned integers: duckdb_engine has types of its own for them, and as
    # integers they serve as a delete_mark_field too.
    integer = dialect.ischema_names.get(data_type.lower())
    if integer is not None and issubclass(integer, Integer):
        return integer()
    return DuckDBType(data_type)
