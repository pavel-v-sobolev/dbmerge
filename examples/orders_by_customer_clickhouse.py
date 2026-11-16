"""ClickHouse totals with PostgreSQL sources and a caller-owned PostgreSQL tracker.

Create ClickHouse database source_pg using the PostgreSQL database engine mapped
onto PostgreSQL schema target. Source orders needs id, order_date, customer_id,
amount, is_deleted and merged_on. Journal source writes through the same tracker.
Read the result with FINAL. Retained IDs and tombstones detect moves/deletions.

ReplacingMergeTree(version) prefers newer staged snapshots, even if an older
writer finishes later. This is not transactional fencing: source reads, ClickHouse
writes and PostgreSQL leases are not atomic. Wall-clock versions require synchronized
clocks; equal versions can tie. Use soft deletes for durable source evidence.
A single worker processes changes between rebuild blocks, not in a background thread.
"""

from datetime import date

from sqlalchemy import and_, text, tuple_

from dbmerge import MartHandler, MergeTracker, LeaseBusy, dbmerge


class OrdersByCustomerClickHouse(MartHandler):
    destination = (None, 'orders_by_customer')
    sources = [('target', 'orders')]

    def setup(self, ctx):
        for statement in (
            """CREATE TABLE IF NOT EXISTS orders_by_customer (
                order_date Date, customer_id UInt64, total_amount Decimal(18, 2),
                is_deleted UInt8, order_ids Array(Int64), version UInt64,
                merged_on DateTime64(6, 'UTC'), inserted_on DateTime64(6, 'UTC')
            ) ENGINE = ReplacingMergeTree(version) ORDER BY (order_date, customer_id)""",
            """CREATE OR REPLACE VIEW orders_by_customer_rows_view AS
                SELECT toInt64(assumeNotNull(id)) AS id,
                       toDate(assumeNotNull(order_date)) AS order_date,
                       toUInt64(assumeNotNull(customer_id)) AS customer_id,
                       assumeNotNull(amount) AS amount,
                       assumeNotNull(is_deleted) AS is_deleted,
                       assumeNotNull(merged_on) AS merged_on
                FROM source_pg.orders""",
            """CREATE OR REPLACE VIEW orders_by_customer_view AS
                WITH groups AS (
                    SELECT order_date, customer_id FROM orders_by_customer_rows_view
                    UNION DISTINCT
                    SELECT order_date, customer_id FROM orders_by_customer FINAL
                ), totals AS (
                    SELECT order_date, customer_id,
                           sumIf(amount, NOT is_deleted) AS total_amount,
                           countIf(NOT is_deleted) AS active_count,
                           groupUniqArray(toInt64(id)) AS order_ids
                    FROM orders_by_customer_rows_view GROUP BY order_date, customer_id
                )
                SELECT g.order_date, g.customer_id,
                       CAST(ifNull(t.total_amount, 0) AS Decimal(18, 2)) AS total_amount,
                       toUInt8(ifNull(t.active_count, 0) = 0) AS is_deleted,
                       ifNull(t.order_ids, CAST([] AS Array(Int64))) AS order_ids,
                       toUInt64(toUnixTimestamp64Micro(now64(6))) AS version
                FROM groups g LEFT JOIN totals t USING (order_date, customer_id)""",
        ):
            self.execute(ctx, statement)

    def groups_to_handle_sql(self, ctx):
        return """
            WITH changed_orders AS (
                SELECT toInt64(id) AS id FROM orders_by_customer_rows_view
                WHERE merged_on > :watermark
                UNION DISTINCT
                SELECT old_id FROM (
                    SELECT arrayJoin(order_ids) AS old_id FROM orders_by_customer FINAL
                ) WHERE old_id NOT IN (SELECT toInt64(id) FROM orders_by_customer_rows_view)
            )
            SELECT order_date, customer_id FROM orders_by_customer FINAL
            WHERE hasAny(order_ids, (SELECT groupUniqArray(id) FROM changed_orders))
            UNION DISTINCT
            SELECT order_date, customer_id FROM orders_by_customer_rows_view
            WHERE toInt64(id) IN (SELECT id FROM changed_orders)
        """

    def process_changes(self, ctx):
        groups = text(self.groups_to_handle_sql(ctx)).bindparams(watermark=ctx.watermark)
        with dbmerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                     table_name='orders_by_customer',
                     source_table_name='orders_by_customer_view',
                     key=['order_date', 'customer_id'], delete_mode='no',
                     merged_on_field='merged_on', inserted_on_field='inserted_on') as merge:
            key = tuple_(merge.source_table.c.order_date, merge.source_table.c.customer_id)
            # PostgreSQL's lease predicate cannot be evaluated inside ClickHouse SQL.
            if merge.exec(source_condition=key.in_(groups)) is None:
                raise LeaseBusy('Changes were not written')

    def rebuild(self, ctx):
        periods = self.query(ctx, """
            SELECT DISTINCT toStartOfMonth(order_date) AS period FROM (
                SELECT order_date FROM orders_by_customer_rows_view
                UNION DISTINCT
                SELECT order_date FROM orders_by_customer FINAL
            ) ORDER BY period
        """)
        for (period,) in periods:
            label = period.strftime('%Y-%m')
            if ctx.rebuild_from and label <= ctx.rebuild_from:
                continue
            ends = date(period.year + 1, 1, 1) if period.month == 12 else date(
                period.year, period.month + 1, 1)
            with dbmerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                         table_name='orders_by_customer',
                         source_table_name='orders_by_customer_view',
                         key=['order_date', 'customer_id'], delete_mode='no',
                         merged_on_field='merged_on', inserted_on_field='inserted_on',
                         use_safe_border=True) as merge:
                day = merge.source_table.c.order_date
                if merge.exec(source_condition=and_(day >= period, day < ends)) is None:
                    raise LeaseBusy('Rebuild block was not written')
            yield label


if __name__ == '__main__':
    from sqlalchemy import create_engine

    engine = create_engine('clickhousedb://default:@localhost:8123/default')
    tracking_engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    tracker = MergeTracker(tracking_engine)
    mart = OrdersByCustomerClickHouse(engine, tracker=tracker)
    mart.run_once()  # Use run_once(rebuild=True) for a full rebuild; see runner_marts.py for polling.
