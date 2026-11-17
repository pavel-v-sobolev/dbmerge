"""PostgreSQL daily product sales, summed across shops.

Source business columns: Order, Date, Product, Shop, Amount. Order is a unique
row identifier declared as BIGINT. Sales across shops collapse into Date,
Product, SUM(Amount); Shop is deliberately not part of the mart key.
Run seed_product_daily_sales.py first. The loader renews merged_on on every write.
The mart also retains operational columns for incremental refresh and tombstones;
read SELECT "Date", "Product", "Amount" ... WHERE NOT is_deleted for business data.
The loop assigns operation scopes internally. Tombstones retain deleted groups.
"""

from datetime import date

from sqlalchemy import DateTime, and_, text, tuple_

from dbmerge import MartHandler, MergeTracker, DBMerge


class ProductDailySales(MartHandler):
    destination = ('target', 'product_daily_sales')
    sources = [('target', 'orders')]

    def ddl(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            CREATE TABLE IF NOT EXISTS {schema}.product_daily_sales (
                "Date" DATE NOT NULL,
                "Product" INTEGER NOT NULL,
                "Amount" NUMERIC NOT NULL,
                is_deleted BOOLEAN NOT NULL,
                merged_on TIMESTAMPTZ,
                inserted_on TIMESTAMPTZ,
                order_ids BIGINT[] NOT NULL DEFAULT ARRAY[]::bigint[],
                PRIMARY KEY ("Date", "Product")
            );
            CREATE INDEX IF NOT EXISTS ix_product_daily_sales_order_ids
                ON {schema}.product_daily_sales USING gin (order_ids);
            CREATE INDEX IF NOT EXISTS ix_product_daily_sales_merged_on
                ON {schema}.product_daily_sales (merged_on);
            CREATE INDEX IF NOT EXISTS ix_orders_merged_on ON {schema}.orders (merged_on);

            CREATE OR REPLACE VIEW {schema}.product_daily_sales_view AS
            WITH groups AS (
                SELECT "Date", "Product" FROM {schema}.orders
                UNION
                SELECT "Date", "Product" FROM {schema}.product_daily_sales
            ), totals AS (
                SELECT "Date", "Product",
                       SUM("Amount") FILTER (WHERE NOT is_deleted) AS "Amount",
                       COUNT(*) FILTER (WHERE NOT is_deleted) AS active_count,
                       array_agg("Order" ORDER BY "Order") AS order_ids
                  FROM {schema}.orders
                 GROUP BY "Date", "Product"
            )
            SELECT g."Date", g."Product",
                   COALESCE(t."Amount", 0) AS "Amount",
                   COALESCE(t.active_count, 0) = 0 AS is_deleted,
                   COALESCE(t.order_ids, ARRAY[]::bigint[]) AS order_ids
              FROM groups g LEFT JOIN totals t USING ("Date", "Product");
        """

    def groups_to_handle_sql(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            WITH changed_orders AS (
                SELECT "Order" FROM {schema}.orders WHERE merged_on > :watermark
                UNION
                -- Hard deletes have no source row; their IDs remain in the mart.
                SELECT old_id FROM {schema}.product_daily_sales,
                                   unnest(order_ids) AS old_id
                 WHERE NOT EXISTS (SELECT 1 FROM {schema}.orders WHERE "Order" = old_id)
            )
            -- PREVIOUS groups, located through the GIN-indexed array overlap.
            SELECT "Date", "Product" FROM {schema}.product_daily_sales
             WHERE order_ids && ARRAY(SELECT "Order" FROM changed_orders)
            UNION
            -- CURRENT groups.
            SELECT "Date", "Product" FROM {schema}.orders
             WHERE "Order" IN (SELECT "Order" FROM changed_orders)
        """

    def rebuild_blocks_sql(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            SELECT DISTINCT date_trunc('month', "Date")::date AS period
              FROM (
                SELECT "Date" FROM {schema}.orders
                UNION
                SELECT "Date" FROM {schema}.product_daily_sales
              ) dates ORDER BY period
        """

    def setup(self, ctx):
        self.execute(ctx, self.ddl(ctx))

    def process_changes(self, ctx):
        schema = next(iter(self.sources))[0]
        groups_to_handle = text(self.groups_to_handle_sql(ctx)).bindparams(watermark=ctx.watermark)
        with DBMerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                     table_name='product_daily_sales', schema=schema,
                     source_table_name='product_daily_sales_view', source_schema=schema,
                     key=['Date', 'Product'], delete_mode='no',
                     data_types={'merged_on': DateTime(timezone=True),
                                 'inserted_on': DateTime(timezone=True)},
                     merged_on_field='merged_on', inserted_on_field='inserted_on') as merge:
            source_key = tuple_(merge.source_table.c.Date, merge.source_table.c.Product)
            merge.exec(source_condition=source_key.in_(groups_to_handle))

    def rebuild(self, ctx):
        schema = next(iter(self.sources))[0]
        for (period,) in self.query(ctx, self.rebuild_blocks_sql(ctx)):
            label = period.strftime('%Y-%m')
            if ctx.rebuild_from and label <= ctx.rebuild_from:
                continue
            ends = date(period.year + 1, 1, 1) if period.month == 12 else date(
                period.year, period.month + 1, 1)
            with DBMerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                         table_name='product_daily_sales', schema=schema,
                         source_table_name='product_daily_sales_view', source_schema=schema,
                         key=['Date', 'Product'], delete_mode='no',
                         data_types={'merged_on': DateTime(timezone=True),
                                     'inserted_on': DateTime(timezone=True)},
                         merged_on_field='merged_on', inserted_on_field='inserted_on',
                         use_safe_border=True) as merge:
                condition = and_(merge.table.c.Date >= period,
                                 merge.table.c.Date < ends)
                merge.exec(
                    update_condition=condition,
                    source_condition=and_(merge.source_table.c.Date >= period,
                                          merge.source_table.c.Date < ends))
            yield label

if __name__ == '__main__':
    from sqlalchemy import create_engine

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    try:
        with MergeTracker(engine) as tracker:
            mart = ProductDailySales(engine, tracker=tracker)
            mart.run_once()  # For polling see runner_marts.py; for an explicit rebuild use rebuild=True.
    finally:
        engine.dispose()
