"""ClickHouse totals with PostgreSQL sources and a caller-owned PostgreSQL tracker.

Create ClickHouse database source_pg using the PostgreSQL database engine mapped
onto PostgreSQL schema target. Source orders needs Order, Date, Product,
Shop, Amount, is_deleted and merged_on (see seed_product_daily_sales.py).
The mart groups by Date and Product, summing Amount across all shops.
Journal source writes through the same tracker.
Read the result with FINAL. Retained IDs and tombstones detect moves/deletions.

ReplacingMergeTree(version) prefers newer staged snapshots, even if an older
writer finishes later. This is not transactional fencing: source reads, ClickHouse
writes and PostgreSQL leases are not atomic. Wall-clock versions require synchronized
clocks; equal versions can tie. Use soft deletes for durable source evidence.
A single worker processes changes between rebuild blocks, not in a background thread.
"""

from datetime import date
from time import time_ns

from sqlalchemy import and_, tuple_

from dbmerge import MartHandler, MergeTracker, DBMerge


class ProductDailySalesClickHouse(MartHandler):
    destination = (None, 'product_daily_sales')
    sources = [('target', 'orders')]

    def setup(self, ctx):
        for statement in (
            """CREATE TABLE IF NOT EXISTS product_daily_sales (
                "Date" Date, "Product" UInt64, "Amount" Nullable(Decimal(18, 2)),
                is_deleted UInt8, order_ids Array(Int64), version UInt64,
                merged_on DateTime64(6, 'UTC'), inserted_on DateTime64(6, 'UTC')
            ) ENGINE = ReplacingMergeTree(version) ORDER BY ("Date", "Product")""",
            """CREATE OR REPLACE VIEW product_daily_sales_rows_view AS
                SELECT toInt64(assumeNotNull("Order")) AS "Order",
                       toDate(assumeNotNull("Date")) AS "Date",
                       toUInt64(assumeNotNull("Product")) AS "Product",
                       assumeNotNull("Amount") AS "Amount",
                       assumeNotNull(is_deleted) AS is_deleted,
                       assumeNotNull(merged_on) AS merged_on
                FROM source_pg.orders""",
            """CREATE OR REPLACE VIEW product_daily_sales_view AS
                SELECT "Date", "Product", sum("Amount") AS "Amount",
                       toUInt8(0) AS is_deleted,
                       arraySort(groupArray("Order")) AS order_ids,
                       toUInt64(toUnixTimestamp64Micro(now64(6))) AS version
                FROM product_daily_sales_rows_view
                WHERE NOT is_deleted GROUP BY "Date", "Product"
            """,
        ):
            self.execute(ctx, statement)

    def groups_to_handle_sql(self, ctx):
        return """
            WITH changed_orders AS (
                SELECT "Order" FROM product_daily_sales_rows_view
                WHERE merged_on > :watermark
                UNION DISTINCT
                SELECT old_id FROM (
                    SELECT arrayJoin(order_ids) AS old_id FROM product_daily_sales FINAL
                ) WHERE old_id NOT IN (SELECT "Order" FROM product_daily_sales_rows_view)
            )
            SELECT "Date", "Product" FROM product_daily_sales FINAL
            WHERE hasAny(order_ids, (SELECT groupUniqArray("Order") FROM changed_orders))
            UNION DISTINCT
            SELECT "Date", "Product" FROM product_daily_sales_rows_view
            WHERE "Order" IN (SELECT "Order" FROM changed_orders)
        """

    def process_changes(self, ctx):
        # Materialize affected keys once: embedding this query in the aggregate
        # SELECT repeatedly opens PostgreSQL reads and exhausts the 23.8 pool.
        groups = self.query(ctx, self.groups_to_handle_sql(ctx), watermark=ctx.watermark)
        if not groups:
            return
        with DBMerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                     table_name='product_daily_sales',
                     source_table_name='product_daily_sales_view',
                     key=['Date', 'Product'], delete_mode='mark', delete_mark_field='is_deleted',
                     delete_mark_values={'Amount': None, 'version': time_ns() // 1000},
                     merged_on_field='merged_on', inserted_on_field='inserted_on') as merge:
            key = tuple_(merge.source_table.c.Date, merge.source_table.c.Product)
            # PostgreSQL's lease predicate cannot be evaluated inside ClickHouse SQL.
            target_key = tuple_(merge.table.c.Date, merge.table.c.Product)
            merge.exec(source_condition=key.in_(groups), delete_condition=target_key.in_(groups))

    def rebuild(self, ctx):
        periods = self.query(ctx, """
            SELECT DISTINCT toStartOfMonth("Date") AS period FROM (
                SELECT "Date" FROM product_daily_sales_rows_view
                UNION DISTINCT
                SELECT "Date" FROM product_daily_sales FINAL
            ) ORDER BY period
        """)
        for (period,) in periods:
            label = period.strftime('%Y-%m')
            if ctx.rebuild_from and label <= ctx.rebuild_from:
                continue
            ends = date(period.year + 1, 1, 1) if period.month == 12 else date(
                period.year, period.month + 1, 1)
            with DBMerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                         table_name='product_daily_sales',
                         source_table_name='product_daily_sales_view',
                         key=['Date', 'Product'], delete_mode='mark', delete_mark_field='is_deleted',
                         delete_mark_values={'Amount': None, 'version': time_ns() // 1000},
                         merged_on_field='merged_on', inserted_on_field='inserted_on',
                         use_safe_border=True) as merge:
                day = merge.source_table.c.Date
                merge.exec(source_condition=and_(day >= period, day < ends),
                           delete_condition=and_(merge.table.c.Date >= period,
                                                 merge.table.c.Date < ends))
            yield label


if __name__ == '__main__':
    from sqlalchemy import create_engine

    engine = create_engine('clickhousedb://default:@localhost:8123/default')
    tracking_engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    try:
        with MergeTracker(tracking_engine) as tracker:
            mart = ProductDailySalesClickHouse(engine, tracker=tracker)
            mart.run_once()  # For polling see runner_marts.py; for an explicit rebuild use rebuild=True.
    finally:
        engine.dispose()
        tracking_engine.dispose()
