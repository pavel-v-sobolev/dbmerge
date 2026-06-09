"""
Mini smoke-test for CockroachDB support in dbmerge.

Bring the database up first:
    cd db/cockroach && docker compose up -d

Then run from the repo root (so `dbmerge` is importable):
    python db/cockroach/cockroach_test.py

CockroachDB speaks the PostgreSQL wire protocol, so the official
`sqlalchemy-cockroachdb` dialect (driver: psycopg2) is used. The dialect name
reported to dbmerge is "cockroachdb" (NOT "postgresql"), so:
  - the temp table is created as a plain table via the generic branch in
    _create_temp_table (CockroachDB does not support UNLOGGED);
  - dict/list columns are auto-detected as JSONB because "cockroachdb" is
    listed in dbmerge.JSONB_DIALECTS (CockroachDB natively supports JSONB and,
    like postgres, cannot compare plain JSON with IS DISTINCT FROM).
"""

from datetime import date

from sqlalchemy import create_engine, MetaData, Table, select, func

from dbmerge import dbmerge, drop_table_if_exists

# root user, insecure single-node cluster, default database.
engine = create_engine("cockroachdb://root@localhost:26257/defaultdb?sslmode=disable")

key = ['Shop', 'Product', 'Date']


def count_rows(table_name):
    tbl = Table(table_name, MetaData(), autoload_with=engine)
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(tbl)).scalar()


def main():
    drop_table_if_exists(engine, 'Facts')

    # --- Pass 1: create the table from data and insert three rows ---
    # 'Data' is a dict -> auto-detected as JSONB (verifies the JSONB_DIALECTS path).
    data = [
        {'Shop': '123', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 2,    'Price': 50.10, 'Data': {'a': 1}},
        {'Shop': '124', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 1,    'Price': 100.50, 'Data': {'b': [1, 2]}},
        {'Shop': '124', 'Product': 'B2', 'Date': date(2025, 1, 1), 'Qty': None, 'Price': 1.20, 'Data': None},
    ]
    with dbmerge(engine=engine, data=data, table_name="Facts", key=key) as merge:
        r = merge.exec()
    assert r.inserted_row_count == 3, r.inserted_row_count
    assert count_rows('Facts') == 3

    # --- Pass 2: one updated row, one new row, one row missing -> hard delete ---
    # ('123','A1') changes only its JSONB value -> must be detected as an update.
    modified = [
        {'Shop': '123', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 2,    'Price': 50.10, 'Data': {'a': 2}},   # update JSONB
        {'Shop': '124', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 1,    'Price': 100.50, 'Data': {'b': [1, 2]}},  # unchanged
        {'Shop': '125', 'Product': 'C3', 'Date': date(2025, 1, 1), 'Qty': 5,    'Price': 7.00, 'Data': {'c': 3}},    # new
    ]                                                                                                                 # ('124','B2') gone
    with dbmerge(engine=engine, data=modified, table_name="Facts", key=key, delete_mode='delete') as merge:
        r = merge.exec()
    assert r.updated_row_count == 1, r.updated_row_count
    assert r.inserted_row_count == 1, r.inserted_row_count
    assert r.deleted_row_count == 1, r.deleted_row_count
    assert count_rows('Facts') == 3

    print("CockroachDB smoke-test passed.")


if __name__ == "__main__":
    main()
