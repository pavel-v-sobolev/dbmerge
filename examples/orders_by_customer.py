"""
An example mart: order totals per customer per day, with soft deletion.

The source table `orders` is keyed by a GUID `id`; the mart groups it by the composite key
(order_date, customer_id). Deleted source rows are not removed — they carry `is_deleted` and are
excluded from the sum, so the deletion reaches the mart as a zero (or reduced) total without any
delete_condition.

Change detection reads the changed composite keys with a subquery over merged_on > ctx.watermark
(which uses the merged_on index on the source) and passes them as source_condition.

The full rebuild walks monthly intervals computed from the source range; an interrupted rebuild
resumes from ctx.rebuild_from.

The source table `orders` (id, order_date, customer_id, amount, is_deleted, merged_on) is assumed
to exist and to be filled by the loader through the same MergeTracker.
"""

from datetime import date, datetime

from sqlalchemy import and_, text, tuple_

from dbmerge import dbmerge
from dbmerge.mart import MartContext, MartHandler


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class OrdersByCustomer(MartHandler):
    name = "orders_by_customer"        # unique mart name (for the lease)
    mart_table = "orders_by_customer"  # the result table
    sources = ["orders"]               # read tables: a change in any of them triggers a run

    def setup(self, ctx: MartContext) -> None:
        # One-time DDL: the aggregate view and the mart table. Deleted rows are excluded from the
        # sum, so a soft deletion updates the total in place.
        self.execute(ctx, "DROP VIEW IF EXISTS orders_by_customer_view")
        self.execute(ctx, """
            CREATE VIEW orders_by_customer_view AS
            SELECT order_date,
                   customer_id,
                   SUM(CASE WHEN is_deleted THEN 0 ELSE amount END) AS total_amount
              FROM orders
             GROUP BY order_date, customer_id
        """)
        self.execute(ctx, """
            CREATE TABLE IF NOT EXISTS orders_by_customer (
                order_date DATE,
                customer_id INTEGER,
                total_amount NUMERIC,
                PRIMARY KEY (order_date, customer_id)
            )
        """)

    def _changed_keys(self, ctx: MartContext):
        # The subquery reads the changed composite keys from the raw source table, so the predicate
        # merged_on > :watermark can use the merged_on index.
        return text("""
            SELECT order_date, customer_id
              FROM orders
             WHERE merged_on > :watermark
        """).bindparams(watermark=ctx.watermark)

    def handle(self, ctx: MartContext) -> None:
        # Incremental work: upsert only the changed (order_date, customer_id) keys. No
        # delete_condition is needed — a deleted order drops out of the sum in place.
        changed = self._changed_keys(ctx)
        with dbmerge(ctx.engine, table_name=self.mart_table, schema=ctx.schema,
                     source_table_name="orders_by_customer_view", source_schema=ctx.schema,
                     key=["order_date", "customer_id"]) as merge:
            source_key = tuple_(merge.source_table.c["order_date"],
                                merge.source_table.c["customer_id"])
            ctx.tracker.exec(
                merge,
                source_condition=source_key.in_(changed),
                next_watermark=ctx.next_watermark,
                with_lease=True, wait_lease=False,
            )

    def rebuild(self, ctx: MartContext):
        # Full rebuild by monthly intervals computed from the source range. An interrupted rebuild
        # resumes from ctx.rebuild_from (the label of the last completed month).
        row = self.query(ctx, "SELECT MIN(order_date), MAX(order_date) FROM orders")[0]
        first, last = row[0], row[1]
        if first is None or last is None:
            return
        first, last = _as_date(first), _as_date(last)
        year, month = first.year, first.month
        end_year, end_month = last.year, last.month
        while (year, month) <= (end_year, end_month):
            label = f"{year:04d}-{month:02d}"
            starts = date(year, month, 1)
            next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
            ends = date(next_year, next_month, 1)
            if not (ctx.rebuild_from and label <= ctx.rebuild_from):
                with dbmerge(ctx.engine, table_name=self.mart_table, schema=ctx.schema,
                             source_table_name="orders_by_customer_view", source_schema=ctx.schema,
                             key=["order_date", "customer_id"]) as merge:
                    d = merge.source_table.c["order_date"]
                    merge.exec(source_condition=and_(d >= starts, d < ends))
                yield label
            year, month = next_year, next_month
