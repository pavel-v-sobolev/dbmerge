"""End-to-end mart with ClickHouse data and PostgreSQL coordination state."""

import os
import uuid

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from dbmerge import MartLoop, MergeTracker, dbmerge, request_rebuild
from conftest import all_engine_urls, engines, make_engine


def test_clickhouse_tracker_clock_and_safe_border():
    from datetime import timedelta
    from unittest.mock import patch
    from sqlalchemy import select
    from dbmerge import LeaseOptions

    if 'clickhouse' not in engines:
        pytest.skip('needs ClickHouse')
    engine = make_engine('clickhouse')
    pg = create_engine(all_engine_urls['postgres'])
    name = 'clock_' + uuid.uuid4().hex[:8]
    tracker = MergeTracker(pg, table_name='tracking_' + name)
    try:
        stamp = tracker._db_now().replace(microsecond=123456)
        options = dict(tracker=tracker, lease_options=LeaseOptions(scope='changes'),
                       table_name=name, key=['id'], merged_on_field='merged_on',
                       inserted_on_field='inserted_on', delete_mark_field='deleted')
        with dbmerge(engine, data=[{'id': 1, 'value': 10}], **options) as merge:
            with patch.object(tracker, '_db_now', return_value=stamp):
                merge.exec()
            with engine.connect() as conn:
                row = conn.execute(select(merge.table)).first()
            assert row.merged_on.replace(tzinfo=None) == stamp.replace(tzinfo=None)
            assert row.inserted_on.replace(tzinfo=None) == stamp.replace(tzinfo=None)
        for mode in ('no', 'mark'):
            with dbmerge(engine, data=[{'id': 1, 'value': 20}, {'id': 2, 'value': 30}],
                         delete_mode=mode, use_safe_border=True, **options) as merge:
                with patch.object(tracker, 'safe_boundary', return_value=stamp - timedelta(seconds=1)):
                    result = merge.exec()
                assert result.updated_row_count == result.deleted_row_count == 0
    finally:
        tracker.close()
        with engine.connect() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS {name}')
        tracker.table.drop(pg, checkfirst=True)
        pg.dispose()


def test_product_daily_sales_example():
    """Exercise the shipped handler, not a substitute aggregation callback."""
    from datetime import date
    from pathlib import Path
    import sys
    from unittest.mock import patch

    from dbmerge import DBMerge

    address = os.environ.get('DBMERGE_TEST_CLICKHOUSE_POSTGRES')
    if 'clickhouse' not in engines or not address:
        pytest.skip('needs ClickHouse and DBMERGE_TEST_CLICKHOUSE_POSTGRES')
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from examples.product_daily_sales_clickhouse import ProductDailySalesClickHouse
    from examples.seed_product_daily_sales import ROWS

    admin = make_engine('clickhouse')
    pg = create_engine(all_engine_urls['postgres'])
    suffix = uuid.uuid4().hex[:8]
    source, destination, state = (f'sales_source_{suffix}', f'sales_dest_{suffix}', f'sales_state_{suffix}')
    url = make_url(all_engine_urls['postgres'])
    engine = create_engine(admin.url.set(database=destination))
    tracker = loop = None

    def ch(sql):
        with admin.connect() as conn:
            conn.exec_driver_sql(sql)

    def read():
        with engine.connect() as conn:
            return [tuple(row) for row in conn.exec_driver_sql('''
                SELECT "Date", "Product", "Amount" FROM product_daily_sales FINAL
                WHERE NOT is_deleted ORDER BY "Date", "Product"
            ''')]

    try:
        with pg.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA {source}'))
            conn.execute(text(f'''CREATE TABLE {source}.orders (
                "Order" BIGINT PRIMARY KEY, "Date" DATE NOT NULL, "Product" INTEGER NOT NULL,
                "Shop" INTEGER NOT NULL, "Amount" NUMERIC(18, 2) NOT NULL,
                is_deleted BOOLEAN NOT NULL DEFAULT false, merged_on TIMESTAMPTZ NOT NULL
            )'''))
        ch(f"CREATE DATABASE {source} ENGINE = PostgreSQL('{address}', '{url.database}', "
           f"'{url.username}', '{url.password}', '{source}')")
        ch(f'CREATE DATABASE {destination}')
        tracker = MergeTracker(pg, schema=state)
        handler = ProductDailySalesClickHouse(engine, tracker=tracker,
            destination=(destination, 'product_daily_sales'), sources=[(source, 'orders')])
        original = handler.execute

        def execute(ctx, sql, **params):
            return original(ctx, sql.replace('source_pg.orders', f'{source}.orders'), **params)

        with DBMerge(pg, tracker=tracker, schema=source, table_name='orders', key=['Order'],
                     data=[dict(row, is_deleted=False) for row in ROWS],
                     merged_on_field='merged_on') as merge:
            merge.exec()
        loop = handler.loop()
        with patch.object(handler, 'execute', side_effect=execute):
            assert loop.run_if_pending()
        assert read() == [(date(2026, 11, 16), 10, 150), (date(2026, 11, 16), 20, 30),
                          (date(2026, 11, 17), 10, 70)]
        assert not loop.run_if_pending()

        def change(sql):
            def operation():
                with pg.begin() as conn:
                    return conn.execute(text(sql)).rowcount
            tracker.exec(operation, destination=(source, 'orders'))

        change(f'''UPDATE {source}.orders SET "Amount" = 80, "Shop" = 3,
                   merged_on = clock_timestamp() WHERE "Order" = 2''')
        assert loop.run_if_pending()
        assert read()[0] == (date(2026, 11, 16), 10, 180)
        change(f'DELETE FROM {source}.orders WHERE "Order" IN (1, 2)')
        assert loop.run_if_pending()
        assert read() == [(date(2026, 11, 16), 20, 30), (date(2026, 11, 17), 10, 70)]
        request_rebuild(pg, handler.destination, schema=state)
        assert loop.run_if_pending()
        assert read() == [(date(2026, 11, 16), 20, 30), (date(2026, 11, 17), 10, 70)]
    finally:
        if loop is not None:
            loop.close()
        if tracker is not None:
            tracker.close()
        engine.dispose()
        ch(f'DROP DATABASE IF EXISTS {destination}')
        ch(f'DROP DATABASE IF EXISTS {source}')
        with pg.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS {source} CASCADE'))
            conn.execute(text(f'DROP SCHEMA IF EXISTS {state} CASCADE'))
        pg.dispose()


def test_clickhouse_mart_with_postgresql_tracker():
    address = os.environ.get("DBMERGE_TEST_CLICKHOUSE_POSTGRES")
    if "clickhouse" not in engines or not address:
        pytest.skip("needs ClickHouse and DBMERGE_TEST_CLICKHOUSE_POSTGRES")
    engine = make_engine("clickhouse")
    pg = create_engine(all_engine_urls["postgres"])
    suffix = uuid.uuid4().hex[:8]
    source_schema, destination_schema = f"mart_source_{suffix}", f"mart_dest_{suffix}"
    tracking_table = f"tracking_{suffix}"
    state_schema = f"mart_state_{suffix}"
    url = make_url(all_engine_urls["postgres"])
    tracker = loop = None
    phases = []

    def execute(sql):
        with engine.connect() as conn:
            conn.exec_driver_sql(sql)

    def read():
        with engine.connect() as conn:
            return [tuple(row) for row in conn.exec_driver_sql(
                f"SELECT customer_id, total FROM {destination_schema}.totals FINAL ORDER BY customer_id")]

    try:
        with pg.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA {source_schema}")
            conn.exec_driver_sql(f"CREATE TABLE {source_schema}.orders "
                                 "(id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER, "
                                 "is_deleted BOOLEAN NOT NULL DEFAULT false)")
        execute(f"CREATE DATABASE {source_schema} ENGINE = PostgreSQL('{address}', '{url.database}', "
                f"'{url.username}', '{url.password}', '{source_schema}')")
        execute(f"CREATE DATABASE {destination_schema}")
        execute(f"CREATE VIEW {destination_schema}.totals_view AS SELECT customer_id, "
                f"sum(if(is_deleted, 0, amount)) AS total FROM {source_schema}.orders GROUP BY customer_id")
        tracker = MergeTracker(pg, table_name=tracking_table, schema=state_schema)
        # The source view is in the destination database, not the read-only PostgreSQL database.
        def callback(ctx):
            phases.append(ctx.full_rebuild)
            with dbmerge(engine, schema=destination_schema, table_name="totals",
                         source_schema=destination_schema, source_table_name="totals_view",
                         key=["customer_id"], tracker=ctx.tracker,
                         lease_handle=ctx.lease_handle) as merge:
                merge.exec()

        loop = MartLoop(engine, tracker, (destination_schema, "totals"),
                        [(source_schema, "orders")], callback)

        def source_write(sql):
            def write():
                with pg.begin() as conn:
                    return conn.execute(text(sql)).rowcount
            return tracker.exec(write, destination=(source_schema, "orders"))

        source_write(f"INSERT INTO {source_schema}.orders (id, customer_id, amount) "
                     "VALUES (1, 10, 5), (2, 10, 7), (3, 20, 3)")
        assert loop.run_if_pending()
        assert read() == [(10, 12), (20, 3)]
        watermark = tracker.watermark("totals", schema=destination_schema)
        assert watermark is not None
        assert not loop.run_if_pending()
        for amount in (11, 13):
            source_write(f"UPDATE {source_schema}.orders SET amount = {amount} WHERE id = 1")
            assert loop.run_if_pending()
            assert read() == [(10, amount + 7), (20, 3)]
        source_write(f"UPDATE {source_schema}.orders SET is_deleted = true WHERE id = 3")
        assert loop.run_if_pending()
        assert read() == [(10, 20), (20, 0)]
        assert tracker.watermark("totals", schema=destination_schema) >= watermark
        request_rebuild(pg, (destination_schema, "totals"), schema=state_schema)
        assert loop.run_if_pending()
        assert read() == [(10, 20), (20, 0)]
        assert phases == [True, False, False, False, True]
        assert inspect(pg).has_table("dbmerge_marts", schema=state_schema)
        assert not inspect(engine).has_table("dbmerge_marts", schema=destination_schema)
    finally:
        if loop is not None:
            loop.close()
        if tracker is not None:
            tracker.close()
            tracker.table.drop(pg)
        execute(f"DROP DATABASE IF EXISTS {destination_schema}")
        execute(f"DROP DATABASE IF EXISTS {source_schema}")
        with pg.begin() as conn:
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {source_schema} CASCADE")
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {state_schema} CASCADE")
        pg.dispose()