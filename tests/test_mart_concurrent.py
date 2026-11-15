"""PostgreSQL integration tests for the composite-key concurrent examples."""

import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest
from sqlalchemy import (Boolean, Column, Date, DateTime, Integer, MetaData, Numeric, Table,
                        delete, func, insert, inspect, select, text, update)
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT

from dbmerge import MergeTracker, request_rebuild, reset_stop
from conftest import engines, make_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from examples.orders_by_customer import OrdersByCustomer

DAY = date(2026, 1, 15)


def test_sql_mart_has_no_public_scope(postgres_engine):
    with MergeTracker(postgres_engine) as tracker:
        handler = OrdersByCustomer(postgres_engine, tracker=tracker)
        assert not hasattr(handler, 'scope')
        assert handler.destination == OrdersByCustomer.destination


@pytest.fixture(params=[name for name in engines if name.startswith('postgres')])
def postgres_engine(request):
    return make_engine(request.param)


@pytest.fixture
def concurrent(request, postgres_engine):
    reset_stop()
    engine = postgres_engine
    schema = 'concurrent_' + uuid.uuid4().hex[:12]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    tracker = MergeTracker(engine, schema=schema)
    orders = Table('orders', MetaData(), Column('id', Integer, primary_key=True),
                   Column('order_date', Date, nullable=False),
                   Column('customer_id', Integer, nullable=False), Column('amount', Numeric),
                    Column('is_deleted', Boolean, nullable=False, default=False),
                    Column('merged_on', DateTime(timezone=True), nullable=False,
                           server_default=func.clock_timestamp(), onupdate=func.clock_timestamp()),
                    schema=schema)
    orders.create(engine)
    changes = OrdersByCustomer(engine, tracker=tracker, sources=[(schema, 'orders')],
                               destination=(schema, 'orders_by_customer_changes'))
    full = OrdersByCustomer(engine, tracker=tracker, sources=[(schema, 'orders')],
                            destination=(schema, 'orders_by_customer_rebuild'))
    change_loop, full_loop = changes.loop(), full.loop()

    def write(statement, scope='changes'):
        def operation():
            with engine.begin() as conn:
                return conn.execute(statement).rowcount
        tracker.exec(operation, destination=(schema, 'orders'), scope=scope)

    try:
        yield orders, changes, full, change_loop, full_loop, tracker, write
    finally:
        change_loop.close()
        full_loop.close()
        tracker.close()
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        reset_stop()


def totals(handler):
    if isinstance(handler, OrdersByCustomer):
        schema = handler.engine.dialect.identifier_preparer.quote_identifier(next(iter(handler.sources))[0])
        with handler.engine.connect() as conn:
            return {(row.order_date, row.customer_id): (row.total_amount, row.is_deleted)
                    for row in conn.execute(text(f'SELECT * FROM {schema}.orders_by_customer'))}
    with handler.engine.connect() as conn:
        return {(row.order_date, row.customer_id): (row.total_amount, row.is_deleted)
                for row in conn.execute(select(handler.table))}


@pytest.mark.parametrize('action', ['update', 'delete', 'soft_delete', 'move', 'date_move', 'insert'])
def test_changes_finish_before_full_and_stale_snapshot_is_skipped(concurrent, action):
    orders, changes, full, change_loop, full_loop, tracker, write = concurrent
    if isinstance(full, OrdersByCustomer):
        pytest.skip('Direct-view SQL merge has no external snapshot to pause')
    write(insert(orders).values(id=1, order_date=DAY, customer_id=1, amount=10), 'full_load')
    assert not change_loop.run_if_pending()
    read, release = Event(), Event()
    original = full._read_month

    def paused(ctx, starts, ends):
        result = original(ctx, starts, ends)
        read.set()
        assert release.wait(15)
        return result

    with patch.object(full, '_read_month', side_effect=paused):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(full_loop.run_if_pending)
            try:
                assert read.wait(15)
                expected = {(DAY, 1): (10, False)}
                if action == 'update':
                    write(update(orders).values(amount=25))
                    expected[(DAY, 1)] = (25, False)
                elif action in ('delete', 'soft_delete'):
                    write(delete(orders) if action == 'delete' else
                          update(orders).values(is_deleted=True))
                    expected[(DAY, 1)] = (0, True)
                elif action in ('move', 'date_move'):
                    day, customer = (DAY, 2) if action == 'move' else (date(2026, 2, 15), 1)
                    write(update(orders).values(order_date=day, customer_id=customer))
                    expected = {(DAY, 1): (0, True), (day, customer): (10, False)}
                else:
                    write(insert(orders).values(id=2, order_date=DAY, customer_id=2, amount=7))
                    expected[(DAY, 2)] = (7, False)
                assert change_loop.run_if_pending()
                assert totals(changes) == expected
            finally:
                release.set()
            assert future.result(timeout=15)
    assert totals(full) == expected


def test_full_resumes_months_and_preserves_changes_progress(concurrent):
    orders, changes, full, change_loop, full_loop, tracker, write = concurrent
    write(insert(orders).values([
        dict(id=1, order_date=DAY, customer_id=1, amount=10),
        dict(id=2, order_date=date(2026, 2, 1), customer_id=1, amount=20)]))
    assert change_loop.run_if_pending()
    before = tracker.watermark(changes.destination[1], schema=changes.destination[0])
    request_rebuild(full.engine, full.destination, schema=tracker.schema)
    if isinstance(full, OrdersByCustomer):
        original = full.query

        def fail_second(*args, **kwargs):
            for row in original(*args, **kwargs):
                if row[0].month == 2:
                    raise RuntimeError('interrupted')
                yield row

        interruption = patch.object(full, 'query', side_effect=fail_second)
    else:
        original = full._read_month

        def fail_second(ctx, starts, ends):
            if starts.month == 2:
                raise RuntimeError('interrupted')
            return original(ctx, starts, ends)

        interruption = patch.object(full, '_read_month', side_effect=fail_second)
    with interruption:
        with pytest.raises(RuntimeError, match='interrupted'):
            full_loop.run_if_pending()
    assert full_loop._rebuild_cursor() == '2026-01'
    assert full_loop.run_if_pending()
    assert full_loop._rebuild_cursor() is None
    assert tracker.watermark(changes.destination[1], schema=changes.destination[0]) == before


def test_equal_total_renews_protection_against_intermediate_full(concurrent):
    from types import SimpleNamespace

    orders, changes, full, change_loop, _, tracker, write = concurrent
    if isinstance(full, OrdersByCustomer):
        pytest.skip('SQL full reads the current view directly, not an intermediate external snapshot')
    write(insert(orders).values(id=1, order_date=DAY, customer_id=1, amount=10))
    assert change_loop.run_if_pending()
    write(update(orders).values(amount=50), 'full_load')
    ctx = SimpleNamespace(engine=full.engine, tracker=tracker, lease_is_mine=None)
    full.setup(ctx)
    rows, boundary = full._read_month(ctx, date(2026, 1, 1), date(2026, 2, 1))
    write(update(orders).values(amount=10))
    assert change_loop.run_if_pending()
    with full.engine.begin() as conn:
        full._lock(conn)
        assert full._apply(conn, ctx, rows, boundary) == 0
    assert totals(full) == {(DAY, 1): (10, False)}


def test_sql_variant_writes_through_dbmerge_with_full_guard(concurrent):
    orders, changes, full, change_loop, full_loop, tracker, write = concurrent
    if not isinstance(full, OrdersByCustomer):
        pytest.skip('SQL-specific conditions contract')
    write(insert(orders).values(id=1, order_date=DAY, customer_id=1, amount=10), 'full_load')
    with patch.object(tracker, 'exec_merge', wraps=tracker.exec_merge) as execute:
        assert full_loop.run_if_pending()
        assert execute.call_count == 1
        merge = execute.call_args.args[0]
        kwargs = execute.call_args.kwargs
        assert merge.delete_mode == 'no'
        assert merge.source_table_name == 'orders_by_customer_view'
        assert kwargs['source_condition'] is not None
        assert merge.merged_on_field == 'merged_on'
        assert merge.inserted_on_field == 'inserted_on'
        assert 'scope' not in kwargs
        assert tracker._mart_scope.get() is None
        assert 'merged_on' in str(kwargs['update_condition'])
        assert kwargs['insert_condition'] is not None
    write(update(orders).values(amount=20))
    # Initialize this consumer's watermark before testing its incremental path.
    tracker.mark_watermark(changes.destination[1], full_loop.tracker.watermark(
        full.destination[1], schema=full.destination[0]), schema=changes.destination[0])
    with patch.object(tracker, 'exec_merge', wraps=tracker.exec_merge) as execute:
        assert change_loop.run_if_pending()
        assert 'scope' not in execute.call_args.kwargs
        assert execute.call_args.args[0].source_table_name.endswith('_view')
        assert execute.call_args.kwargs['source_condition'] is not None
        # Changes has only the lease guard, not full's timestamp restriction.
        assert 'merged_on' not in str(execute.call_args.kwargs['update_condition'])


def test_sql_changes_do_not_touch_unrelated_groups(concurrent):
    orders, changes, full, change_loop, _, _, write = concurrent
    if not isinstance(changes, OrdersByCustomer):
        pytest.skip('SQL-specific changed-group selection')
    write(insert(orders).values([
        dict(id=1, order_date=DAY, customer_id=1, amount=10),
        dict(id=2, order_date=DAY, customer_id=2, amount=20)]))
    assert change_loop.run_if_pending()

    def timestamps():
        schema = changes.engine.dialect.identifier_preparer.quote_identifier(next(iter(changes.sources))[0])
        with changes.engine.connect() as conn:
            return dict(conn.execute(text(
                f'SELECT customer_id, merged_on FROM {schema}.orders_by_customer')).all())

    before = timestamps()
    write(update(orders).where(orders.c.id == 1).values(amount=15))
    assert change_loop.run_if_pending()
    after = timestamps()
    assert after[1] > before[1]
    assert after[2] == before[2]
    assert totals(full) == {(DAY, 1): (15, False), (DAY, 2): (20, False)}


def test_sql_order_ids_are_indexed_arrays(concurrent):
    orders, changes, _, change_loop, _, _, write = concurrent
    if not isinstance(changes, OrdersByCustomer):
        pytest.skip('SQL-specific array contract')
    write(insert(orders).values(id=1, order_date=DAY, customer_id=1, amount=10))
    assert change_loop.run_if_pending()
    schema = next(iter(changes.sources))[0]
    inspector = inspect(changes.engine)
    columns = {column['name']: column['type'] for column in inspector.get_columns(
        'orders_by_customer', schema=schema)}
    assert isinstance(columns['order_ids'], ARRAY)
    assert isinstance(columns['order_ids'].item_type, BIGINT)
    indexes = inspector.get_indexes('orders_by_customer', schema=schema)
    assert any(index['column_names'] == ['order_ids']
               and index['dialect_options'].get('postgresql_using') == 'gin'
               for index in indexes)


@pytest.mark.parametrize('action', ['update', 'delete', 'soft_delete', 'move', 'date_move', 'insert'])
def test_sql_direct_view_rebuild_preserves_applied_changes(concurrent, action):
    orders, changes, full, change_loop, full_loop, tracker, write = concurrent
    if not isinstance(full, OrdersByCustomer):
        pytest.skip('SQL-specific direct-view rebuild')
    write(insert(orders).values(id=1, order_date=DAY, customer_id=1, amount=10))
    assert change_loop.run_if_pending()
    if action == 'update':
        statement = update(orders).values(amount=25)
    elif action == 'delete':
        statement = delete(orders)
    elif action == 'soft_delete':
        statement = update(orders).values(is_deleted=True)
    elif action == 'move':
        statement = update(orders).values(customer_id=2)
    elif action == 'date_move':
        statement = update(orders).values(order_date=date(2026, 2, 15))
    else:
        statement = insert(orders).values(id=2, order_date=DAY, customer_id=2, amount=7)
    write(statement)
    assert change_loop.run_if_pending()
    expected = totals(changes)
    request_rebuild(full.engine, full.destination, schema=tracker.schema)
    assert full_loop.run_if_pending()
    assert totals(full) == expected
