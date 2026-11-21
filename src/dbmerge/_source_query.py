"""Turn a caller-supplied query into a named, column-addressable SQL source."""

import re
from datetime import date, datetime, time
from uuid import UUID

from sqlalchemy import column, false, literal_column, select, text, types
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.selectable import SelectBase

from ._source_select import validate_source_select

_SAMPLE_TYPES = {bool: types.Boolean, int: types.BigInteger, float: types.Double,
                 str: types.String, bytes: types.LargeBinary, date: types.Date, UUID: types.Uuid}


def normalize(query):
    from .dbmerge import IncorrectParameter

    if isinstance(query, str):
        query = text(query.strip().removesuffix(';').rstrip())
    if isinstance(query, TextClause):
        # Text is trusted SQL, not parsed or sanitized here. This check catches
        # accidental non-query inputs before any merge setup runs.
        sql = re.sub(r'\A(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*', '', query.text, flags=re.S)
        if not re.match(r'(?:SELECT|WITH)\b', sql, re.I):
            raise IncorrectParameter('"source_query" must be a SELECT query.')
        return query.columns()
    if not isinstance(query, SelectBase):
        raise IncorrectParameter('"source_query" must be SQL text or a SQLAlchemy SELECT query.')
    return query


def as_source(merge, query):
    from .dbmerge import IncorrectDataError

    source = query.subquery('dbmerge_source')
    if not source.c:
        statement = select(literal_column('*')).select_from(source)
        if describe := merge._dialect_hook('describe_source_query'):
            fields = describe(merge, statement)
        else:
            with merge.conn.execute(statement.where(false())) as result:
                fields = [(name, types.NullType()) for name in result.keys()]
        source = query.element.columns(*(column(name, kind) for name, kind in fields)).subquery('dbmerge_source')

    names = [c.name for c in source.c]
    if not names or any(not name or name.startswith('%(') for name in names) or len(names) != len(set(names)):
        raise IncorrectDataError('"source_query" must return uniquely named columns. Label its output columns explicitly.')
    return source


def select_rows(merge):
    fields = {c.name: c for c in merge.source_table.c}
    statement = select(*(fields[name] for name in merge.data_fields))
    if merge.source_condition is not None:
        statement = statement.where(merge.source_condition)
        dialect = merge.ch_dialect if hasattr(merge, 'ch_dialect') else merge.conn.dialect
        validate_source_select(statement, dialect=dialect)
    return statement


def resolve_types(merge):
    """Fill unknown types only for columns that are actually about to be created."""
    from .dbmerge import IncorrectDataError

    missing = {name for name, kind in merge.new_fields.items() if isinstance(kind, types.NullType)}
    if not missing:
        return

    fields = [c for c in merge.source_table.c if c.name in missing]
    statement = select(*fields)
    if describe := merge._dialect_hook('describe_source_query'):
        for name, kind in describe(merge, statement):
            if name in missing:
                merge._set_new_field_type(name, kind)
                missing.remove(name)
    else:
        # Explicit types and existing target columns never require reading rows
        # into Python. Some DBAPIs (e.g. SQLite) don't expose SQL result types.
        with merge.conn.execute(statement.limit(100)) as result:
            for row in result:
                for field, value in zip(fields, row):
                    if field.name in missing and (kind := _sample_type(value)) is not None:
                        merge._set_new_field_type(field.name, kind)
                        missing.remove(field.name)
                if not missing:
                    break

    if missing:
        raise IncorrectDataError(f'Could not detect source_query types for {sorted(missing)}. '
                                 'Specify them in data_types or type the SQLAlchemy output columns.')


def _sample_type(value):
    if isinstance(value, datetime):
        return types.DateTime(timezone=value.utcoffset() is not None)
    if isinstance(value, time):
        return types.Time(timezone=value.utcoffset() is not None)
    # A sample cannot establish the precision/scale of a Decimal result column.
    kind = _SAMPLE_TYPES.get(type(value))
    return kind() if kind is not None else None