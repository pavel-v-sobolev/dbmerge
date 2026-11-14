"""End-to-end mart with ClickHouse data and PostgreSQL coordination state."""

import os
import uuid

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from dbmerge import MartLoop, MergeTracker, dbmerge, request_rebuild
from conftest import all_engine_urls, engines, make_engine


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
                         key=["customer_id"]) as merge:
                ctx.tracker.exec_merge(merge)

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