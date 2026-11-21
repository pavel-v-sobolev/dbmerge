# DBMerge recipes

Choose the merge mode based on what a missing row means in your input:

| Your input contains… | Use… |
|---|---|
| Only new and changed rows | `delete_mode='no'` (the default). Missing rows stay as they are. See the [quick start](README.md#quick-start-example). |
| Every row that should exist in the target | [Full snapshot](#full-snapshot) |
| Every row that should exist within one snapshot (e.g. month or shop) | [Scoped snapshot](#scoped-snapshot) |

These choices apply whether rows come from Python or from a [SQL source](#sql-sources). Use a key that uniquely identifies each row in both source and target.

## Full snapshot

Use this when your input defines the whole target table: for example, a nightly export of the entire product catalog. In this SQLite example, product 1 changes, product 2 disappears, and product 3 is added.

```python
from sqlalchemy import create_engine
from dbmerge import DBMerge

engine = create_engine("sqlite://")
before = [{"id": 1, "name": "Coffee"}, {"id": 2, "name": "Tea"}]
snapshot = [{"id": 1, "name": "Dark roast"}, {"id": 3, "name": "Cocoa"}]

with DBMerge(engine, "products", data=before, key=["id"]) as merge:
    merge.exec()

with DBMerge(engine, "products", data=snapshot, key=["id"], delete_mode="delete") as merge:
    result = merge.exec()

# products now contains (1, "Dark roast") and (3, "Cocoa").
```

To keep the old rows but flag them, use `delete_mode='mark'` with `delete_mark_field='is_deleted'` instead. Filter marked rows out in readers that should see only active products.

Confirm that the export completed and contains the full catalog before enabling deletion or marking. An empty snapshot that reaches the merge can remove or mark every row; a failed export must not be treated as an intentionally empty catalog.

## Scoped snapshot

Use this when the source is complete **within a known slice**, not across the whole table. For example, a complete February sales snapshot replaces the February rows; January stays untouched. If you only have the rows that changed in February, use the default `delete_mode='no'` instead.

Here `source_condition` selects the requested month from `incoming_sales`. `delete_condition` limits deletion on `sales`; it does not filter what can be inserted or updated. Build both conditions from the month requested by the job, not from whichever rows happen to be returned.

```python
from sqlalchemy import create_engine, text
from dbmerge import DBMerge

engine = create_engine("sqlite://")
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE sales (month INTEGER, product_id INTEGER, qty INTEGER,
                            PRIMARY KEY (month, product_id))
    """))
    conn.execute(text("INSERT INTO sales VALUES (202501, 1, 4), (202502, 1, 5), (202502, 2, 6)"))
    conn.execute(text("CREATE TABLE incoming_sales (month INTEGER, product_id INTEGER, qty INTEGER)"))
    conn.execute(text("INSERT INTO incoming_sales VALUES (202501, 1, 99), (202502, 1, 8), (202502, 3, 2)"))

month = 202502
with DBMerge(
    engine, "sales", source_table_name="incoming_sales",
    key=["month", "product_id"], delete_mode="delete",
) as merge:
    result = merge.exec(
        source_condition=merge.source_table.c.month == month,
        delete_condition=merge.table.c.month == month,
    )

# January keeps product 1 at qty 4.
# February updates product 1 to qty 8, removes product 2, and adds product 3 at qty 2.
```

With Python `data`, pass only rows for the requested slice and keep the target `delete_condition`. For a shop or tenant, use its ID instead of `month`. If a successful SQL snapshot returns no rows, the scope still comes from the job parameters, so only that slice can be cleared. If the source failed or is incomplete, skip the destructive merge.

## SQL sources

Use a SQL source when your database can already read the rows. DBMerge loads staging on the server; you do not need to fetch the result into a Python list or DataFrame.

`source_query` requires DBMerge 2.7.0 or newer. With earlier versions, use a database view and pass it as `source_table_name` instead.

- Use `source_table_name` for an existing table or view. Set `source_schema` if it is not in the default schema, and use `source_condition` to filter rows as in the scoped example above.
- Use `source_query` when a `SELECT` should define the input, for example with an aggregation or join. Give its output columns unique names that match the target, including the merge key. Bind runtime values instead of interpolating them.

This example aggregates paid orders by customer. It keeps summary rows for customers absent from the query result.

```python
from sqlalchemy import text
from dbmerge import DBMerge

# `engine` is connected to the database containing orders and the target.
query = text("""
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    WHERE status = :status
    GROUP BY customer_id
""").bindparams(status="paid")

with DBMerge(engine, "customer_order_counts", source_query=query, key=["customer_id"]) as merge:
    result = merge.exec()
```

If the query returns a complete snapshot, use the [full](#full-snapshot) or [scoped](#scoped-snapshot) recipe to reconcile missing rows. For query types and schema inference, see the [source query reference](DOCUMENTATION.md#source-queries). For an operational PostgreSQL source attached to ClickHouse, see [PostgreSQL to ClickHouse](POSTGRESQL_TO_CLICKHOUSE.md).