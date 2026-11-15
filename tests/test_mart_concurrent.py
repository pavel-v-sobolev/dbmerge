"""PostgreSQL integration tests for the composite-key concurrent examples."""

import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest
from sqlalchemy import (Boolean, Column, Date, Integer, MetaData, Numeric, Table,
                        delete, insert, select, text, update)

from dbmerge import MergeTracker, request_rebuild, reset_stop
from conftest import engines, make_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from examples.orders_by_customer_concurrent import OrdersByCustomerChanges, OrdersByCustomerFull

DAY = date(2026, 1, 15)


@pytest.fixture(params=[name for name in engines if name.startswith('postgres')])
def concurrent(request):
    reset_stop()
    engine = make_engine(request.param)
    schema = 'concurrent_' + uuid.uuid4().hex[:12]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    tracker = MergeTracker(engine, schema=schema)
    orders = Table('orders', MetaData(), Column('id', Integer, primary_key=True),
                   Column('order_date', Date, nullable=False),
                   Column('customer_id', Integer, nullable=False), Column('amount', Numeric),
                   Column('is_deleted', Boolean, nullable=False, default=False), schema=schema)
    orders.create(engine)
    changes = OrdersByCustomerChanges(engine, schema=schema, source_schema=schema)
    full = OrdersByCustomerFull(engine, schema=schema, source_schema=schema)
    change_loop, full_loop = changes.loop(tracker), full.loop(tracker)

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
    with handler.engine.connect() as conn:
        return {(row.order_date, row.customer_id): (row.total_amount, row.is_deleted)
                for row in conn.execute(select(handler.table))}


@pytest.mark.parametrize('action', ['update', 'delete', 'soft_delete', 'move', 'date_move', 'insert'])
def test_changes_finish_before_full_and_stale_snapshot_is_skipped(concurrent, action):
    orders, changes, full, change_loop, full_loop, tracker, write = concurrent
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
    before = tracker.watermark(changes.destination[1], schema=changes.target[0])
    request_rebuild(full.engine, full.destination, schema=tracker.schema)
    original = full._read_month

    def fail_second(ctx, starts, ends):
        if starts.month == 2:
            raise RuntimeError('interrupted')
        return original(ctx, starts, ends)

    with patch.object(full, '_read_month', side_effect=fail_second):
        with pytest.raises(RuntimeError, match='interrupted'):
            full_loop.run_if_pending()
    assert full_loop._rebuild_cursor() == '2026-01'
    assert full_loop.run_if_pending()
    assert full_loop._rebuild_cursor() is None
    assert tracker.watermark(changes.destination[1], schema=changes.target[0]) == before


def test_equal_total_renews_protection_against_intermediate_full(concurrent):
    from types import SimpleNamespace

    orders, changes, full, change_loop, _, tracker, write = concurrent
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


def test_concurrent_runner_import_does_not_start_workers(monkeypatch):
    import runpy

    directory = Path(__file__).resolve().parents[1] / 'examples'
    monkeypatch.syspath_prepend(str(directory))
    with patch('sqlalchemy.create_engine') as create, patch('signal.signal') as signals:
        namespace = runpy.run_path(str(directory / 'runner_marts_concurrent.py'))
    create.assert_not_called()
    signals.assert_not_called()
    assert callable(namespace['main'])