"""
An example mart: order totals per customer.

A mart is rebuilt when its sources change. The change is detected through the merge journal
(dbmerge_tracking) kept by MergeTracker, not through a signal; the sources must therefore be
loaded through tracker.exec(merge) — see runner_marts.py and TRACKING_AND_MARTS.md.

The window is (last_run_at, ...): rows are selected by merged_on > last_run_at, and the first run
reads everything since the epoch. The mart key differs from the source key (customer_id vs id),
so the read aggregates with GROUP BY; no deletion is needed here.

The source table `orders` (with customer_id, amount and the service merged_on column) is assumed
to exist and to be filled by the loader.
"""

from dbmerge import dbmerge
from dbmerge.mart import MartContext, MartHandler


class OrdersByCustomer(MartHandler):
    name = "orders_by_customer"        # unique mart name (for the lease)
    mart_table = "orders_by_customer"  # the result table
    sources = ["orders"]               # read tables: a change in any of them triggers a run
    min_interval = 5.0                 # minimum pause between runs, seconds

    # Optional hooks:
    #   setup(ctx)   — one-time DDL (views, indexes) before the first run;
    #   rebuild(ctx) — a full rebuild; by default the same as handle (ctx.full_rebuild=True).

    def handle(self, ctx: MartContext) -> None:
        # Incremental work. Read rows newer than the watermark and merge them into the mart;
        # the read is unbounded, and the watermark moves to ctx.boundary for the next run.
        rows = self.query(ctx, """
            SELECT customer_id, SUM(amount) AS total
              FROM orders
             WHERE merged_on > :last_run_at
             GROUP BY customer_id
        """, last_run_at=ctx.last_run_at)
        with dbmerge(ctx.engine, table_name=self.mart_table,
                     data=[dict(row._mapping) for row in rows],
                     key=["customer_id"]) as merge:
            ctx.tracker.exec(merge, boundary=ctx.boundary)
