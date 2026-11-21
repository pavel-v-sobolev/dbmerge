"""Support PostgreSQL's built-in planar point (not PostGIS geometry)."""

import warnings

from sqlalchemy import Table, cast, func, text
from sqlalchemy.exc import SAWarning
from sqlalchemy.types import NullType, UserDefinedType


class Point(UserDefinedType):
    """PostgreSQL's planar ``point`` type, without an SRID or PostGIS dependency.

    Bind an ``(x, y)`` pair, PostgreSQL point input text, or ``None`` for SQL NULL.
    """

    cache_ok = True

    def get_col_spec(self, **kw):
        return 'POINT'

    def bind_expression(self, bindvalue):
        # psycopg 3 and pg8000 can bind strings as text rather than unknown.
        return cast(bindvalue, self)

    def bind_processor(self, dialect):
        return self.to_text

    @staticmethod
    def to_text(value):
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, (tuple, list)) and len(value) == 2:
            return f'({float(value[0])!r},{float(value[1])!r})'
        raise ValueError('PostgreSQL Point requires (x, y) or point input text.')


def differs(left, right):
    if isinstance(left.type, Point):
        # POINT has no equality operator. ~= uses a tolerance and would miss
        # small updates; point_send preserves the exact coordinates and NULLs.
        return func.pg_catalog.point_send(left).is_distinct_from(func.pg_catalog.point_send(right))
    return left.is_distinct_from(right)


def check_table(merge):
    from .dbmerge import NoKeyError

    for name in merge.key:
        kind = merge.data_fields[name]
        if name in merge.new_fields:
            kind = merge.given_data_types.get(name, kind)
        if isinstance(kind, Point):
            raise NoKeyError(f'PostgreSQL Point column "{name}" cannot be a merge key.')


def reflect_table(merge, name, schema):
    if not merge.inspector.has_table(name, schema):
        return None

    def reflect_point(inspector, table, info):
        if not isinstance(info['type'], NullType):
            return
        # Resolve the relation as PostgreSQL does (including the search path).
        relation = merge.conn.dialect.identifier_preparer.format_table(table)
        is_point = merge.conn.scalar(text('''
            SELECT atttypid = 'pg_catalog.point'::regtype
            FROM pg_catalog.pg_attribute
            WHERE attrelid = pg_catalog.to_regclass(:relation)
              AND attname = :column_name AND NOT attisdropped
        '''), {'relation': relation, 'column_name': info['name']})
        if is_point:
            info['type'] = Point()

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message=r"Did not recognize type 'point' of column", category=SAWarning)
        return Table(name, merge.metadata, autoload_with=merge.conn, schema=schema,
                     extend_existing=True, listeners=[('column_reflect', reflect_point)])