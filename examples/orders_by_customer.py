"""PostgreSQL-only concurrent totals by (order_date, customer_id).

orders.id must fit BIGINT; the loader renews orders.merged_on on every write.
The loop assigns operation scopes internally. Tombstones protect against stale rebuild writes.
"""

from datetime import date

from sqlalchemy import DateTime, and_, text, tuple_

from dbmerge import MartHandler, MergeTracker, LeaseBusy, dbmerge


class OrdersByCustomer(MartHandler):
    destination = ('target', 'orders_by_customer')
    sources = [('target', 'orders')]

    def ddl(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            CREATE TABLE IF NOT EXISTS {schema}.orders_by_customer (
                order_date DATE NOT NULL,
                customer_id INTEGER NOT NULL,
                total_amount NUMERIC NOT NULL,
                is_deleted BOOLEAN NOT NULL,
                merged_on TIMESTAMPTZ,
                inserted_on TIMESTAMPTZ,
                write_token VARCHAR(32),
                order_ids BIGINT[] NOT NULL DEFAULT ARRAY[]::bigint[],
                PRIMARY KEY (order_date, customer_id)
            );
            CREATE INDEX IF NOT EXISTS ix_orders_by_customer_order_ids
                ON {schema}.orders_by_customer USING gin (order_ids);
            CREATE INDEX IF NOT EXISTS ix_orders_by_customer_merged_on
                ON {schema}.orders_by_customer (merged_on);
            CREATE INDEX IF NOT EXISTS ix_orders_merged_on ON {schema}.orders (merged_on);

            CREATE OR REPLACE VIEW {schema}.orders_by_customer_rows_view AS
            SELECT id, order_date, customer_id, amount, is_deleted, merged_on
              FROM {schema}.orders;

            CREATE OR REPLACE VIEW {schema}.orders_by_customer_view AS
            WITH groups AS (
                SELECT order_date, customer_id FROM {schema}.orders_by_customer_rows_view
                UNION
                SELECT order_date, customer_id FROM {schema}.orders_by_customer
            ), totals AS (
                SELECT order_date, customer_id,
                       SUM(amount) FILTER (WHERE NOT is_deleted) AS total_amount,
                       COUNT(*) FILTER (WHERE NOT is_deleted) AS active_count,
                       array_agg(DISTINCT id::bigint ORDER BY id::bigint) AS order_ids
                  FROM {schema}.orders_by_customer_rows_view
                 GROUP BY order_date, customer_id
            )
            SELECT g.order_date, g.customer_id,
                   COALESCE(t.total_amount, 0) AS total_amount,
                   COALESCE(t.active_count, 0) = 0 AS is_deleted,
                   COALESCE(t.order_ids, ARRAY[]::bigint[]) AS order_ids,
                   -- Renew protection even if the selected group's total is unchanged.
                   md5(random()::text || clock_timestamp()::text)::varchar(32) AS write_token
              FROM groups g LEFT JOIN totals t USING (order_date, customer_id);
        """

    def groups_to_handle_sql(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            WITH changed_orders AS (
                SELECT id::bigint FROM {schema}.orders WHERE merged_on > :watermark
                UNION
                -- Hard deletes have no source row; their IDs remain in the mart.
                SELECT old_id FROM {schema}.orders_by_customer,
                                   unnest(order_ids) AS old_id
                 WHERE NOT EXISTS (SELECT 1 FROM {schema}.orders WHERE id = old_id)
            )
            -- PREVIOUS groups, located through the GIN-indexed array overlap.
            SELECT order_date, customer_id FROM {schema}.orders_by_customer
             WHERE order_ids && ARRAY(SELECT id FROM changed_orders)
            UNION
            -- CURRENT groups.
            SELECT order_date, customer_id FROM {schema}.orders_by_customer_rows_view
             WHERE id = ANY(ARRAY(SELECT id FROM changed_orders))
        """

    def rebuild_blocks_sql(self, ctx):
        schema = ctx.engine.dialect.identifier_preparer.quote_identifier(next(iter(self.sources))[0])
        return f"""
            SELECT DISTINCT date_trunc('month', order_date)::date AS period
              FROM (
                SELECT order_date FROM {schema}.orders
                UNION
                SELECT order_date FROM {schema}.orders_by_customer
              ) dates ORDER BY period
        """

    def setup(self, ctx):
        self.execute(ctx, self.ddl(ctx))

    def process_changes(self, ctx):
        schema = next(iter(self.sources))[0]
        groups_to_handle = text(self.groups_to_handle_sql(ctx)).bindparams(watermark=ctx.watermark)
        with dbmerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                     table_name='orders_by_customer', schema=schema,
                     source_table_name='orders_by_customer_view', source_schema=schema,
                     key=['order_date', 'customer_id'], delete_mode='no',
                     data_types={'merged_on': DateTime(timezone=True),
                                 'inserted_on': DateTime(timezone=True)},
                     merged_on_field='merged_on', inserted_on_field='inserted_on') as merge:
            source_key = tuple_(merge.source_table.c.order_date, merge.source_table.c.customer_id)
            if merge.exec(source_condition=source_key.in_(groups_to_handle)) is None:
                raise LeaseBusy('Changes were not written')

    def rebuild(self, ctx):
        schema = next(iter(self.sources))[0]
        for (period,) in self.query(ctx, self.rebuild_blocks_sql(ctx)):
            label = period.strftime('%Y-%m')
            if ctx.rebuild_from and label <= ctx.rebuild_from:
                continue
            ends = date(period.year + 1, 1, 1) if period.month == 12 else date(
                period.year, period.month + 1, 1)
            with dbmerge(ctx.engine, tracker=ctx.tracker, lease_handle=ctx.lease_handle,
                         table_name='orders_by_customer', schema=schema,
                         source_table_name='orders_by_customer_view', source_schema=schema,
                         key=['order_date', 'customer_id'], delete_mode='no',
                         data_types={'merged_on': DateTime(timezone=True),
                                     'inserted_on': DateTime(timezone=True)},
                         merged_on_field='merged_on', inserted_on_field='inserted_on',
                         use_safe_border=True) as merge:
                condition = and_(merge.table.c.order_date >= period,
                                 merge.table.c.order_date < ends)
                result = merge.exec(
                    update_condition=condition,
                    source_condition=and_(merge.source_table.c.order_date >= period,
                                          merge.source_table.c.order_date < ends))
                if result is None:
                    raise LeaseBusy('Rebuild block was not written')
            yield label

if __name__ == '__main__':
    from sqlalchemy import create_engine

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    tracker = MergeTracker(engine)
    mart = OrdersByCustomer(engine, tracker=tracker)
    mart.run_once()  # Use run_once(rebuild=True) for a full rebuild; see runner_marts.py for polling.
