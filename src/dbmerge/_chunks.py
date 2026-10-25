"""
The incoming data in chunks shaped for bulk loading: as an Arrow table, which ClickHouse and DuckDB read as
it is, or as plain Python columns for the loaders that take those. Both carry the same values, converted the
same way. pyarrow is needed for the Arrow table only, and imported only then.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from .dbmerge import dbmerge


def arrow_table(merge: dbmerge, begin, end, nested_as_json=(), processors=None, wall_clock=None):
    """
    Rows begin..end of the incoming data as an Arrow table of its data fields.

    nested_as_json: the fields whose dicts and lists go to a string column as JSON text.
    processors: {field: function} applied to every value of the field, None included - the bind processor
        of a column type that SQLAlchemy runs before it sends a value, such as the JSON serializer.
    wall_clock: {field: time zone name} for a column without a time zone: a datetime that has one is
        written as the wall clock of that zone.
    """
    import pyarrow as pa

    processors = processors or {}
    wall_clock = wall_clock or {}
    fields = list(merge.data_fields)

    if merge.type_of_data in ('list of dict', 'dict of list'):
        return pa.Table.from_pydict(python_columns(merge, begin, end, nested_as_json, processors, wall_clock))

    if merge.type_of_data == 'pandas':
        import pandas as pd
        frame = merge.data.iloc[begin:end][fields]
        for f, zone in wall_clock.items():
            if isinstance(frame[f].dtype, pd.DatetimeTZDtype):
                frame = frame.assign(**{f: frame[f].dt.tz_convert(zone).dt.tz_localize(None)})
            elif frame[f].dtype == object:
                frame = frame.assign(**{f: _wall_clock_values(frame[f].tolist(), zone)})
        converted = {}
        for f in fields:
            if frame[f].dtype == object or f in processors:
                values = _converted_values(frame[f].tolist(), f in nested_as_json, processors.get(f))
                if values is not None:
                    converted[f] = values
        if converted:
            frame = frame.assign(**converted)
        return pa.Table.from_pandas(frame, preserve_index=False)

    # polars, which is already loaded when its DataFrame is passed
    import polars as pl
    frame = merge.data.slice(begin, end - begin).select(fields)
    for f, zone in wall_clock.items():
        dtype = frame.schema[f]
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
            frame = frame.with_columns(pl.col(f).dt.convert_time_zone(zone).dt.replace_time_zone(None))
    converted = []
    for f in fields:
        if frame.schema[f].is_nested() or frame.schema[f] == pl.Object or f in processors:
            values = _converted_values(frame[f].to_list(), f in nested_as_json, processors.get(f))
            if values is not None:
                converted.append(pl.Series(f, values, dtype=pl.String))
    if converted:
        frame = frame.with_columns(converted)
    return frame.to_arrow()


def python_columns(merge: dbmerge, begin, end, nested_as_json=(), processors=None, wall_clock=None):
    """
    Rows begin..end of the incoming data as {field: list of values}, with None for every missing value.
    The arguments are those of arrow_table.
    """
    processors = processors or {}
    wall_clock = wall_clock or {}
    fields = list(merge.data_fields)
    data = merge.data

    if merge.type_of_data == 'list of dict':
        rows = data[begin:end]
        columns = {f: [row.get(f) for row in rows] for f in fields}
    elif merge.type_of_data == 'dict of list':
        columns = {f: data[f][begin:end] for f in fields}
    elif merge.type_of_data == 'pandas':
        import pandas as pd   # already loaded when its DataFrame is passed
        frame = data.iloc[begin:end]
        columns = {}
        for f in fields:
            series = frame[f]
            values = series.tolist()
            if pd.api.types.is_datetime64_any_dtype(series):
                # pandas hands out Timestamps, which a driver may take for nanoseconds since the epoch - that
                # is, for UTC - whatever wall clock they show. A plain datetime carries the wall clock as it is.
                values = [v.to_pydatetime() if isinstance(v, pd.Timestamp) else v for v in values]
            # NaN, NaT and NA all stand for a missing value there, and none of them is None.
            missing = series.isna().tolist()
            columns[f] = [None if m else v for v, m in zip(values, missing)]
    else:
        frame = data.slice(begin, end - begin)
        columns = {f: frame[f].to_list() for f in fields}

    for f, zone in wall_clock.items():
        columns[f] = _wall_clock_values(columns[f], zone)
    for f in fields:
        values = _converted_values(columns[f], f in nested_as_json, processors.get(f))
        if values is not None:
            columns[f] = values
    return columns


def column_values(merge: dbmerge, field, begin, end):
    """The values of one field in rows begin..end of the incoming data, as Python objects."""
    data = merge.data
    if merge.type_of_data == 'list of dict':
        return [row.get(field) for row in data[begin:end]]
    if merge.type_of_data == 'dict of list':
        return data[field][begin:end]
    if merge.type_of_data == 'pandas':
        series = data[field].iloc[begin:end]
        return [None if m else v for v, m in zip(series.tolist(), series.isna().tolist())]
    return data[field].slice(begin, end - begin).to_list()


def _wall_clock_values(values, zone):
    tz = ZoneInfo(zone)
    return [v.astimezone(tz).replace(tzinfo=None) if isinstance(v, datetime) and v.tzinfo is not None else v
            for v in values]


def _converted_values(values, nested_as_json, processor=None):
    """
    The values of a column that can not go to the target column as they are, converted, or None when the
    column needs no conversion. As everywhere in dbmerge, the first value present tells what the column
    holds (see dbmerge._detect_missing_data_types).
    """
    if processor is not None:
        # Exactly what the row by row INSERT sends, None included: a JSON column of SQLAlchemy writes it as
        # the JSON literal null unless its type says none_as_null.
        return [processor(None if _is_missing(v) else v) for v in values]
    first = next((v for v in values if not _is_missing(v)), None)
    if isinstance(first, uuid.UUID):
        return [None if _is_missing(v) else str(v) for v in values]
    # Left as it is, a dict or a list would become a struct or a list column, and its column would get
    # whatever text the database makes of one - '([1,2])' in ClickHouse - instead of JSON.
    if nested_as_json and isinstance(first, (dict, list, tuple)):
        return [None if _is_missing(v) else json.dumps(v) for v in values]
    return None


def _is_missing(value):
    # None, or the NaN that pandas puts for a missing value in a column of Python objects.
    return value is None or (isinstance(value, float) and value != value)
