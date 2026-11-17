"""Seed the PostgreSQL mart demo: python examples/seed_product_daily_sales.py.

Uses a disposable target.orders table. Rerunning upserts the same four orders;
it does not delete any other orders. Share this tracker with the mart workers.
"""

from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine, text

from dbmerge import DBMerge, MergeTracker


ROWS = [
    {'Order': 1, 'Date': date(2026, 11, 16), 'Product': 10, 'Shop': 1, 'Amount': Decimal('100.00')},
    {'Order': 2, 'Date': date(2026, 11, 16), 'Product': 10, 'Shop': 2, 'Amount': Decimal('50.00')},
    {'Order': 3, 'Date': date(2026, 11, 16), 'Product': 20, 'Shop': 1, 'Amount': Decimal('30.00')},
    {'Order': 4, 'Date': date(2026, 11, 17), 'Product': 10, 'Shop': 1, 'Amount': Decimal('70.00')},
]


def seed(engine, tracker):
    with engine.begin() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS target'))
        conn.execute(text('''CREATE TABLE IF NOT EXISTS target.orders (
            "Order" BIGINT PRIMARY KEY, "Date" DATE NOT NULL,
            "Product" INTEGER NOT NULL, "Shop" INTEGER NOT NULL, "Amount" NUMERIC(18, 2) NOT NULL,
            is_deleted BOOLEAN NOT NULL DEFAULT false, merged_on TIMESTAMPTZ NOT NULL
        )'''))
    with DBMerge(engine, tracker=tracker, schema='target', table_name='orders',
                 key=['Order'], data=[dict(row, is_deleted=False) for row in ROWS],
                 merged_on_field='merged_on', delete_mode='no') as merge:
        return merge.exec()


if __name__ == '__main__':
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    try:
        with MergeTracker(engine) as tracker:
            print(seed(engine, tracker))
    finally:
        engine.dispose()