"""
An example mart: order totals per customer per day, with the SOURCE in PostgreSQL and the MART in
ClickHouse (the cross-database case).

The calculation is the same as orders_by_customer.py; only the databases differ:

* the source table `orders` (and the aggregate view) live in PostgreSQL;
* the mart table `orders_by_customer` lives in ClickHouse (a ReplacingMergeTree, keyed by its
  ORDER BY columns);
* the journal (`dbmerge_tracking`) lives in PostgreSQL — MergeTracker rejects ClickHouse, so the
  tracker's engine is the PostgreSQL one.

Wiring:

    pg = create_engine("postgresql://.../dwh")        # source + journal
    ch = create_engine("clickhousedb://.../dwh")      # mart target

    tracker = MergeTracker(pg)                        # journal in PostgreSQL

    OrdersByCustomerClickHouse().run_forever(
        engine=ch,          # where the mart table lives
        tracker=tracker,    # the journal
        schema=None,        # ClickHouse has no schemas
    )

The identity is `schema` + `table_name` only (no server/database name). Tables are declared as
`(schema, table)` tuples: the source `("target", "orders")` carries the PostgreSQL schema "target",
while the mart `(None, "orders_by_customer")` has no schema (ClickHouse). MartLoop uses the tuple,
so the mart's watermark is read by schema "" and the sources are watched by schema "target".

The source table `orders` (id, order_date, customer_id, amount, is_deleted, merged_on) in
PostgreSQL is assumed to exist and to be filled by the loader through the same MergeTracker. The
aggregate view `orders_by_customer_view` is created here in ClickHouse (its SELECT reads the
PostgreSQL table through a ClickHouse database engine, set up once by an administrator — see
onecdc's config/handlers/zakazy_klientov_clickhouse.py for the pattern).
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


class OrdersByCustomerClickHouse(MartHandler):
    name = "orders_by_customer_clickhouse"   # unique mart name (for the lease)
    mart_table = (None, "orders_by_customer")  # the result table: (schema=None, table) in ClickHouse
    sources = [("target", "orders")]           # read table: (schema, table) in PostgreSQL

    def setup(self, ctx: MartContext) -> None:
        # One-time DDL in ClickHouse. The merge key comes from the table's ORDER BY columns.
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
                order_date Date,
                customer_id UInt64,
                total_amount Decimal(18, 2)
            ) ENGINE = ReplacingMergeTree
            ORDER BY (order_date, customer_id)
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
        # Incremental work: upsert only the changed (order_date, customer_id) keys.
        changed = self._changed_keys(ctx)
        mart_schema, mart_table = ctx.mart_table
        with dbmerge(ctx.engine, table_name=mart_table, schema=mart_schema,
                     source_table_name="orders_by_customer_view", source_schema=mart_schema,
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
        # Full rebuild by monthly intervals computed from the source range.
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
                mart_schema, mart_table = ctx.mart_table
                with dbmerge(ctx.engine, table_name=mart_table, schema=mart_schema,
                             source_table_name="orders_by_customer_view", source_schema=mart_schema,
                             key=["order_date", "customer_id"]) as merge:
                    d = merge.source_table.c["order_date"]
                    merge.exec(source_condition=and_(d >= starts, d < ends))
                yield label
            year, month = next_year, next_month
