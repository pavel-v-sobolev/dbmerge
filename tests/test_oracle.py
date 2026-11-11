"""Oracle integration tests. Set DBMERGE_TEST_URL_ORACLE to enable live tests.

Use a dedicated test user with CREATE TABLE and a tablespace quota, not SYSTEM.
"""

import os
import uuid
from datetime import datetime

import pytest
from sqlalchemy import create_engine, select, text, String, Integer, LargeBinary

from dbmerge import dbmerge, drop_table_if_exists, IncorrectParameter


@pytest.fixture
def engine():
    url = os.environ.get('DBMERGE_TEST_URL_ORACLE')
    if not url:
        pytest.skip('set DBMERGE_TEST_URL_ORACLE for live Oracle tests')
    pytest.importorskip('oracledb')
    engine = create_engine(url)
    yield engine
    engine.dispose()


@pytest.fixture
def table_name(engine):
    name = 'dm_test_' + uuid.uuid4().hex[:12]
    yield name
    drop_table_if_exists(engine, name)


def run_merge(engine, table_name, rows, **options):
    with dbmerge(engine=engine, table_name=table_name, data=rows, key=['id'], **options) as merge:
        table = merge.table
        staging_name = merge.temp_table.name
        result = merge.exec()
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM user_tables WHERE table_name = :name'),
                           {'name': staging_name.upper()}) == 0
        stored = conn.execute(select(table).order_by(table.c.id)).mappings().all()
    return result, stored


@pytest.mark.parametrize('kind', ['list', 'dict', 'pandas', 'polars'])
@pytest.mark.parametrize('step_commits', [False, True])
@pytest.mark.parametrize('bulk', [False, True])
def test_merge_modes_and_lob_null_comparison(engine, table_name, kind, step_commits, bulk):
    stamp = datetime(2026, 1, 2, 3, 4, 5, 123456)

    def convert(rows):
        if kind == 'dict':
            return {k: [row[k] for row in rows] for k in rows[0]}
        if kind == 'pandas':
            return pytest.importorskip('pandas').DataFrame(rows)
        if kind == 'polars':
            return pytest.importorskip('polars').DataFrame(rows)
        return rows

    def run(rows, **options):
        with dbmerge(engine=engine, table_name=table_name, data=convert(rows), key=['id'],
                     merged_on_field='merged', inserted_on_field='inserted', **options) as merge:
            table = merge.table
            staging_name = merge.temp_table.name
            result = merge.exec(commit_all_steps=step_commits, bulk_load=bulk)
        with engine.connect() as conn:
            assert not conn.scalar(text('SELECT count(*) FROM user_tables WHERE table_name = :name'),
                                   {'name': staging_name.upper()})
            stored = conn.execute(select(table).order_by(table.c.id)).mappings().all()
        return result, stored

    rows = [{'id': 1, 'value': 'a' * 5000, 'stamp': stamp},
            {'id': 2, 'value': None, 'stamp': stamp},
            {'id': 3, 'value': 'obsolete', 'stamp': stamp}]
    result, stored = run(rows)
    assert result.inserted_row_count == 3
    assert result.updated_row_count == 0
    assert stored[0]['stamp'] == stamp
    assert stored[0]['merged'] is not None and stored[0]['inserted'] is not None
    result, _ = run(rows)
    assert result.updated_row_count == result.inserted_row_count == 0
    rows = [{'id': 1, 'value': None, 'stamp': stamp},
            {'id': 2, 'value': 'new', 'stamp': stamp},
            {'id': 4, 'value': 'inserted', 'stamp': stamp}]
    result, stored = run(rows, delete_mode='delete')
    assert (result.updated_row_count, result.inserted_row_count, result.deleted_row_count) == (2, 1, 1)
    assert [row['id'] for row in stored] == [1, 2, 4]
    result, _ = run(rows, delete_mode='delete')
    assert result.updated_row_count == result.inserted_row_count == result.deleted_row_count == 0


def test_mark_and_resurrection(engine, table_name):
    options = dict(delete_mode='mark', delete_mark_field='deleted')
    run_merge(engine, table_name, [{'id': 1}, {'id': 2}], **options)
    result, _ = run_merge(engine, table_name, [{'id': 1}], **options)
    assert result.deleted_row_count == 1
    result, rows = run_merge(engine, table_name, [{'id': 1}, {'id': 2}], **options)
    assert result.updated_row_count == 1
    assert all(row['deleted'] == 0 for row in rows)


def test_conditions_and_skipped_fields(engine, table_name):
    types = {'value': String(100), 'version': Integer()}
    run_merge(engine, table_name, [{'id': 1, 'value': 'old', 'version': 2}], data_types=types)

    def guarded(rows):
        with dbmerge(engine=engine, table_name=table_name, data=rows, key=['id'],
                     skip_compare_fields=['version']) as merge:
            return merge.exec(update_condition=merge.temp_table.c.version > merge.table.c.version,
                              insert_condition=merge.temp_table.c.version > 0)

    result = guarded([{'id': 1, 'value': 'stale', 'version': 1}, {'id': 2, 'value': 'no', 'version': 0}])
    assert result.updated_row_count == result.inserted_row_count == 0
    assert guarded([{'id': 1, 'value': 'old', 'version': 3}]).updated_row_count == 0
    assert guarded([{'id': 1, 'value': 'fresh', 'version': 3}]).updated_row_count == 1


def test_failed_merge_rolls_back_target_and_cleans_staging(engine, table_name, monkeypatch):
    run_merge(engine, table_name, [{'id': 1, 'value': 'original'}])
    with dbmerge(engine=engine, table_name=table_name, data=[{'id': 1, 'value': 'changed'}], key=['id']) as merge:
        table = merge.table
        staging_name = merge.temp_table.name

        def fail():
            raise RuntimeError('failure after update')

        monkeypatch.setattr(merge, '_insert_missing_data', fail)
        with pytest.raises(RuntimeError, match='failure after update'):
            merge.exec(commit_all_steps=False)
    with engine.connect() as conn:
        assert conn.scalar(select(table.c.value)) == 'original'
        assert not conn.scalar(text('SELECT count(*) FROM user_tables WHERE table_name = :name'),
                               {'name': staging_name.upper()})


def test_source_table(engine, table_name):
    source_name = table_name + '_src'
    try:
        run_merge(engine, source_name, [{'id': 1, 'value': 'one'}, {'id': 2, 'value': 'two'}])
        with dbmerge(engine=engine, table_name=table_name, source_table_name=source_name, key=['id']) as merge:
            result = merge.exec(source_condition=merge.source_table.c.id == 2)
        assert result.total_row_count == result.inserted_row_count == 1
    finally:
        drop_table_if_exists(engine, source_name)


def test_missing_schema_and_unbounded_string_key(engine, table_name):
    with pytest.raises(IncorrectParameter, match='schemas are users'):
        dbmerge(engine=engine, table_name=table_name, schema='dm_missing_' + uuid.uuid4().hex[:8],
                data=[{'id': 1}], key=['id'])
    with pytest.raises(IncorrectParameter, match='explicit length'):
        dbmerge(engine=engine, table_name=table_name, data=[{'id': 'key'}], key=['id'])


def test_blob_and_added_column(engine, table_name):
    result, _ = run_merge(engine, table_name, [{'id': 1, 'payload': b'abc'}],
                          data_types={'payload': LargeBinary()})
    assert result.inserted_row_count == 1
    result, rows = run_merge(engine, table_name, [{'id': 1, 'payload': b'xyz', 'extra': 'added'}])
    assert result.updated_row_count == 1
    assert 'extra' in result.added_fields
    assert rows[0]['payload'] == b'xyz' and rows[0]['extra'] == 'added'
    result, _ = run_merge(engine, table_name, [{'id': 1, 'payload': b'xyz', 'extra': 'added'}])
    assert result.updated_row_count == 0
    result, rows = run_merge(engine, table_name, [], delete_mode='delete')
    assert result.deleted_row_count == 1 and rows == []


def test_bulk_load_drives_array_dml_and_stores_the_same(engine, table_name, monkeypatch):
    # bulk_load=True loads the staging table through the Oracle array-DML loader, not through the generic
    # executemany; bulk_load=False must not touch it. Both must store the same values, microseconds and
    # LOBs included.
    from dbmerge import _bulk_load

    stamp = datetime(2026, 3, 4, 5, 6, 7, 654321)
    rows = [{'id': 1, 'value': 'a' * 5000, 'payload': b'abc', 'stamp': stamp},
            {'id': 2, 'value': None, 'payload': None, 'stamp': stamp}]

    calls = []
    real_loader = _bulk_load._oracle_executemany

    def spy(merge):
        calls.append(merge.total_row_count)
        return real_loader(merge)

    monkeypatch.setattr(_bulk_load, '_oracle_executemany', spy)

    stored = {}
    for label, bulk in (('row by row', False), ('bulk', True)):
        with dbmerge(engine=engine, table_name=table_name, data=rows, key=['id'],
                     data_types={'payload': LargeBinary()}) as merge:
            table = merge.table
            merge.exec(bulk_load=bulk)
        with engine.connect() as conn:
            stored[label] = conn.execute(select(table).order_by(table.c.id)).mappings().all()

    assert calls == [2], 'the Oracle array-DML loader must run exactly once, for bulk_load=True'
    assert [(r['id'], r['value'], r['payload'], r['stamp']) for r in stored['bulk']] == \
           [(r['id'], r['value'], r['payload'], r['stamp']) for r in stored['row by row']]