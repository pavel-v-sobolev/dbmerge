"""Run independent full/changes loops: python examples/runner_marts_concurrent.py.

Load orders through the same journal, using exec_merge(..., scope='changes')
or scope='full_load'. For existing unjournaled data request an initial rebuild:
request_rebuild(engine, full.destination, schema=TRACKING_SCHEMA).
"""

import signal
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine

from dbmerge import request_stop, run_mart
from orders_by_customer_concurrent import OrdersByCustomerChanges, OrdersByCustomerFull

DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/dbmerge"
SOURCE_SCHEMA = "target"
MART_SCHEMA = "target"
TRACKING_SCHEMA = None  # same service schema as the loader; create schemas beforehand
POLL_INTERVAL = 2.0


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=0)
    try:
        handlers = [handler(engine, schema=MART_SCHEMA, source_schema=SOURCE_SCHEMA)
                    for handler in (OrdersByCustomerChanges, OrdersByCustomerFull)]
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        with ThreadPoolExecutor(max_workers=len(handlers), thread_name_prefix='mart') as executor:
            futures = [executor.submit(run_mart, handler, schema=TRACKING_SCHEMA,
                                       poll_interval=POLL_INTERVAL) for handler in handlers]
            try:
                for future in as_completed(futures):
                    future.result()
            finally:
                request_stop()
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()