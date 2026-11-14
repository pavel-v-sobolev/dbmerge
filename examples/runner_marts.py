"""
A ready-to-run template for dbmerge marts (MartLoop / MartHandler from dbmerge.mart).

Run:  python examples/runner_marts.py

This is the dbmerge counterpart of onecdc's config/runner.py: instead of the replicator's signals
it runs one MartLoop per mart, and the "should I run" decision is made from the merge journal
(dbmerge_tracking) kept by a MergeTracker. See TRACKING_AND_MARTS.md at the repo root for the
full story.

Key requirement: the loads that write the source tables must go through the same MergeTracker
(tracker.exec_merge(merge)), otherwise the marts never see a change. This runner only wires the mart
side.

Each mart runs in its own thread. The order is undefined: if one mart depends on another,
coordinate their updates in your own code. To spread the marts across processes, run this script
several times — the mart name is claimed through a Lease, so a second process skips the pass.

Settings are plain constants below; replace the connection string with yours.
"""

import signal
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

from dbmerge.mart import request_stop, run_mart

# Marts to run (the example lives next to this file); add more imports here.
from orders_by_customer import OrdersByCustomer

# --- Settings ------------------------------------------------------------------------------------

DB_URL = "postgresql://user:password@host:5432/db"  # replace with your connection string
POLL_INTERVAL = 60          # seconds between checks

def main() -> None:
    # Allow headroom in the pool for merge and lease heartbeat connections.
    engine = create_engine(DB_URL, pool_pre_ping=True)
    try:
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        # One line per mart; each runs with its own automatically closed tracker.
        with ThreadPoolExecutor(thread_name_prefix='mart') as executor:
            futures = [executor.submit(run_mart, OrdersByCustomer(engine),
                                       poll_interval=POLL_INTERVAL)]
            for future in futures:
                try:
                    future.result()
                except BaseException:
                    request_stop()
                    raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
