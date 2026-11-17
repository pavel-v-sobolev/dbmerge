"""Continuous mart runner: python examples/runner_marts.py.

Configure the connection string and MARTS entries below. Each configured bound
method may use its own poll interval. A shared tracker stays open until all
workers stop. Source writes must use the same journal. Importing opens no connections.
Enable loops only after sources are ready: a missing watermark triggers an initial
rebuild even when the source-change journal is empty.
"""

import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from sqlalchemy import create_engine

from dbmerge import MergeTracker, request_stop
from product_daily_sales import ProductDailySales


DB_URL = 'postgresql://postgres:postgres@localhost:5432/dbmerge'
TRACKING_SCHEMA = None


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    try:
        tracker = MergeTracker(engine, schema=TRACKING_SCHEMA)
        try:
            MARTS = [
                partial(ProductDailySales(engine, tracker=tracker).run_forever, poll_interval=60),
                # Add configured mart methods here; [] is a harmless no-op.
            ]
            if not MARTS:
                return
            previous_handlers = {}
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    previous_handlers[signum] = signal.signal(signum, request_stop)
                with ThreadPoolExecutor(max_workers=len(MARTS), thread_name_prefix='mart') as executor:
                    try:
                        # Start every worker before waiting for any of them.
                        futures = [executor.submit(mart) for mart in MARTS]
                        for future in as_completed(futures):
                            future.result()
                    finally:
                        # Runs before the executor waits for workers on exit.
                        request_stop()
            finally:
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)
        finally:
            tracker.close()
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()