"""One-off full rebuild: python examples/rebuild_mart.py.

Configure the connection first. Sources must already exist. An interrupted rebuild
resumes its saved cursor. If another worker owns the lease, the request stays
pending; this script does not wait for that worker to finish.
"""

from sqlalchemy import create_engine

from dbmerge import MergeTracker
from orders_by_customer import OrdersByCustomer


if __name__ == '__main__':
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    tracker = MergeTracker(engine)
    mart = OrdersByCustomer(engine, tracker=tracker)
    if not mart.run_once(rebuild=True):
        print('No pass ran here; another worker may be handling the rebuild request.')