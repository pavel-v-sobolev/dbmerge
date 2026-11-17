"""Source filters must not silently multiply staging through an implicit FROM."""

from types import SimpleNamespace
from unittest.mock import Mock
import warnings

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, event, exists, func, select

from dbmerge import DBMerge, IncorrectParameter


@pytest.fixture
def tables():
    engine = create_engine('sqlite://')
    metadata = MetaData()
    source = Table('source_rows', metadata, Column('id', Integer, primary_key=True),
                   Column('period', Integer), info={'clickhouse': {'engine': 'MergeTree'}})
    target = Table('target_rows', metadata, Column('id', Integer, primary_key=True), Column('period', Integer))
    staging = Table('staging', metadata, Column('id', Integer), Column('period', Integer))
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(source.insert(), [{'id': 1, 'period': 1}, {'id': 2, 'period': 2}])
    yield engine, source, target, staging
    engine.dispose()


@pytest.mark.parametrize('target_count', [0, 1, 3])
@pytest.mark.parametrize('path', ['generic', 'clickhouse'])
def test_target_filter_rejected_before_any_insert(tables, monkeypatch, target_count, path):
    engine, source, target, staging = tables
    with engine.begin() as conn:
        if target_count:
            conn.execute(target.insert(), [{'id': i, 'period': 1} for i in range(target_count)])
    if path == 'generic':
        with DBMerge(engine, table_name=target.name, source_table_name=source.name, key=['id']) as merge:
            writes = []

            @event.listens_for(engine, 'before_cursor_execute')
            def record(conn, cursor, statement, parameters, context, executemany):
                if statement.lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    writes.append(statement)

            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with pytest.raises(IncorrectParameter, match='source_condition.*FROM'):
                    merge.exec(source_condition=merge.table.c.period == 1)
            assert not writes
    else:
        pytest.importorskip('clickhouse_connect')
        from dbmerge import clickhouse
        execute = Mock()
        monkeypatch.setattr(clickhouse, '_insert', execute)
        merge = SimpleNamespace(source_table=source, temp_table=staging,
                                data_fields=['id', 'period'], source_condition=target.c.period == 1)
        with pytest.raises(IncorrectParameter, match='source_condition.*FROM'):
            clickhouse.insert_source_table_to_temp(merge)
        execute.assert_not_called()


@pytest.mark.parametrize('kind', ['source', 'in', 'exists', 'correlated', 'alias', 'linked'])
def test_valid_source_filters(tables, kind):
    engine, source, target, _ = tables
    with engine.begin() as conn:
        conn.execute(target.insert(), [{'id': 1, 'period': 1}])
    with DBMerge(engine, table_name=target.name, source_table_name=source.name, key=['id']) as merge:
        s, t = merge.source_table, merge.table
        alias = t.alias('allowed')
        conditions = {
            'source': s.c.period == 1,
            'in': s.c.id.in_(select(t.c.id)),
            'exists': exists(select(t.c.id).where(t.c.period == 1)),
            'correlated': exists(select(t.c.id).where(t.c.id == s.c.id)),
            'alias': s.c.id.in_(select(alias.c.id)),
            'linked': (s.c.id == t.c.id) & (t.c.period == 1),
        }
        result = merge.exec(source_condition=conditions[kind])
        assert result.total_row_count == (2 if kind == 'exists' else 1)


def test_correct_source_filter_loads_empty_target(tables):
    engine, source, target, _ = tables
    with DBMerge(engine, table_name=target.name, source_table_name=source.name, key=['id']) as merge:
        result = merge.exec(source_condition=merge.source_table.c.period == 1)
        assert result.inserted_row_count == result.total_row_count == 1


def test_same_table_name_in_different_schemas_is_not_identity():
    from dbmerge._source_select import validate_source_select
    metadata = MetaData()
    source = Table('rows', metadata, Column('id', Integer), schema='input')
    target = Table('rows', metadata, Column('id', Integer), schema='output')
    with pytest.raises(IncorrectParameter):
        validate_source_select(select(source.c.id).where(target.c.id == 1))
    validate_source_select(select(source.c.id).where(source.c.id == target.c.id))


def test_explicit_join_and_nested_subquery_are_allowed():
    from dbmerge._source_select import validate_source_select
    metadata = MetaData()
    source = Table('source', metadata, Column('id', Integer))
    other = Table('other', metadata, Column('id', Integer))
    validate_source_select(select(source.c.id).select_from(source.join(other, source.c.id == other.c.id)))
    validate_source_select(select(source.c.id).where(source.c.id.in_(select(other.c.id))))


def test_clickhouse_valid_filter_stores_and_counts_selected_rows(tables, monkeypatch):
    pytest.importorskip('clickhouse_connect')
    from dbmerge import clickhouse
    engine, source, _, staging = tables

    def execute(merge, statement, **kwargs):
        with engine.begin() as conn:
            return None, conn.execute(statement).rowcount

    monkeypatch.setattr(clickhouse, '_insert', execute)
    merge = SimpleNamespace(source_table=source, temp_table=staging, data_fields=['id', 'period'],
                            source_condition=source.c.period == 1)
    clickhouse.insert_source_table_to_temp(merge)
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(staging)) == merge.total_row_count == 1


def test_clickhouse_replacing_source_is_validated_with_final(tables, monkeypatch):
    pytest.importorskip('clickhouse_connect')
    from dbmerge import clickhouse
    _, source, target, staging = tables
    source.info['clickhouse']['engine'] = 'ReplacingMergeTree'
    execute = Mock(return_value=(None, 1))
    monkeypatch.setattr(clickhouse, '_insert', execute)
    merge = SimpleNamespace(source_table=source, temp_table=staging, data_fields=['id', 'period'],
                            source_condition=source.c.period == 1)
    clickhouse.insert_source_table_to_temp(merge)
    execute.assert_called_once()
    merge.source_condition = target.c.period == 1
    with pytest.raises(IncorrectParameter):
        clickhouse.insert_source_table_to_temp(merge)
    assert execute.call_count == 1


@pytest.mark.parametrize('materialized_view', [False, True])
def test_live_clickhouse_staging_count_matches_query_log(monkeypatch, materialized_view):
    """Requires SELECT system.query_log and SYSTEM FLUSH LOGS on a disposable server."""
    import time
    import uuid
    from conftest import engines, make_engine
    if 'clickhouse' not in engines:
        pytest.skip('requires a live ClickHouse test engine')
    pytest.importorskip('clickhouse_connect')
    from dbmerge import clickhouse
    from clickhouse_connect.driver.exceptions import DatabaseError
    from sqlalchemy.exc import DatabaseError as SQLDatabaseError

    engine = make_engine('clickhouse')
    database = 'source_check_' + uuid.uuid4().hex[:12]
    query_ids = []
    original = clickhouse._execute

    def capture(merge, statement, **kwargs):
        sql, summary = original(merge, statement, **kwargs)
        if (sql.lstrip().upper().startswith('INSERT INTO')
                and merge.temp_table.name in sql.split('SELECT', 1)[0]):
            query_ids.append(summary.query_id())
        return sql, summary

    monkeypatch.setattr(clickhouse, '_execute', capture)
    with engine.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE {database}')
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(f'CREATE TABLE {database}.source (id Int64, period Int64) '
                                 'ENGINE = MergeTree ORDER BY id')
            conn.exec_driver_sql(f'INSERT INTO {database}.source VALUES (1, 1), (2, 2)')
        with DBMerge(engine, schema=database, table_name='target', source_schema=database,
                     source_table_name='source', temp_schema=database, delete_mode='delete', key=['id']) as merge:
            if materialized_view:
                with engine.connect() as conn:
                    conn.exec_driver_sql(f'CREATE TABLE {database}.mirror (id Int64, period Int64) '
                                         'ENGINE = MergeTree ORDER BY id')
                    # View side effects are intentionally separate from physical staging count.
                    conn.exec_driver_sql(f'CREATE MATERIALIZED VIEW {database}.copy_stage '
                                         f'TO {database}.mirror AS SELECT id, period '
                                         f'FROM {database}.`{merge.temp_table.name}`')
            load = merge._insert_source_table_to_temp
            observed = []

            def inspect_staging():
                load()
                with engine.connect() as conn:
                    count = conn.exec_driver_sql(
                        f'SELECT count() FROM {database}.`{merge.temp_table.name}`').scalar()
                observed.append((count, merge.total_row_count))

            monkeypatch.setattr(merge, '_insert_source_table_to_temp', inspect_staging)
            merge.exec(source_condition=merge.source_table.c.period == 1)
            assert len(query_ids) == 1 and query_ids[0]
            assert observed[0][0] == 1
            try:
                with engine.connect() as conn:
                    conn.exec_driver_sql('SYSTEM FLUSH LOGS')
                deadline = time.monotonic() + 10
                while True:
                    with engine.connect() as conn:
                        row = conn.exec_driver_sql(
                            'SELECT written_rows FROM system.query_log '
                            'WHERE query_id = %(id)s AND type = 2', {'id': query_ids[0]}).first()
                    if row is not None:
                        break
                    if time.monotonic() >= deadline:
                        pytest.fail('ClickHouse query_log did not record the staging INSERT')
                    time.sleep(.1)
            except (DatabaseError, SQLDatabaseError) as error:
                if 'ACCESS_DENIED' in str(error) or 'UNKNOWN_TABLE' in str(error):
                    pytest.skip('requires system.query_log access and SYSTEM FLUSH LOGS permission')
                raise
            assert observed[0][1] == row[0]
            if not materialized_view:
                assert observed[0][0] == observed[0][1]
            else:
                with engine.connect() as conn:
                    assert conn.exec_driver_sql(f'SELECT count() FROM {database}.mirror').scalar() == 1
    finally:
        with engine.connect() as conn:
            conn.exec_driver_sql(f'DROP DATABASE IF EXISTS {database}')