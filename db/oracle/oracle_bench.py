"""
Benchmark of the Oracle staging load: bulk_load=True (array DML) vs bulk_load=False.

Bring the database up and create the test user first:
    docker compose -f db/oracle/docker-compose.yml up -d
    # then, as an administrator in XEPDB1:
    #   CREATE USER dbmerge_test IDENTIFIED BY oracle;
    #   GRANT CREATE SESSION, CREATE TABLE TO dbmerge_test;
    #   ALTER USER dbmerge_test QUOTA 100M ON USERS;

Then run from the repo root (so `dbmerge` is importable):
    python db/oracle/oracle_bench.py [rows]

The staging table is a GLOBAL TEMPORARY table, so it is loaded with python-oracledb's
array DML (cursor.executemany) either way. bulk_load=True drives the cursor directly;
bulk_load=False goes through SQLAlchemy's executemany, which compiles the statement and
sets the input sizes on every call. On Oracle XE 21.3 / python-oracledb Thin the direct
path is faster - about twice as fast for simple columns, less when LOB and TIMESTAMP
columns dominate the load.
"""

import sys
import time
from datetime import datetime

from sqlalchemy import create_engine, String

from dbmerge import dbmerge, drop_table_if_exists

URL = 'oracle+oracledb://dbmerge_test:oracle@localhost:1521/?service_name=XEPDB1'


def make_rows(n):
    stamp = datetime(2026, 1, 2, 3, 4, 5, 123456)
    return [{'id': i, 'name': 'name-%d' % i, 'note': 'note-%d' % i, 'stamp': stamp} for i in range(n)]


def run(engine, table_name, rows, bulk_load):
    drop_table_if_exists(engine, table_name)
    with dbmerge(engine=engine, table_name=table_name, data=rows, key=['id'],
                 data_types={'name': String(100)}) as merge:
        result = merge.exec(bulk_load=bulk_load)
    drop_table_if_exists(engine, table_name)
    return result


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    engine = create_engine(URL)
    rows = make_rows(n)

    print(f'Loading {n} rows into the staging table (id, name, note CLOB, stamp TIMESTAMP)')
    for bulk_load in (False, True):
        t0 = time.perf_counter()
        result = run(engine, 'BenchRows', rows, bulk_load)
        wall = time.perf_counter() - t0
        label = 'bulk_load=True ' if bulk_load else 'bulk_load=False'
        print(f'{label}  staging {result.temp_insert_time:6.3f}s  '
              f'total merge {result.total_time:6.3f}s  wall {wall:6.3f}s  '
              f'inserted {result.inserted_row_count}')


if __name__ == '__main__':
    main()
