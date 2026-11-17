"""One-off full rebuild: python examples/rebuild_mart.py.

Configure the connection first. Sources must already exist. An interrupted rebuild
resumes its saved cursor. If another worker owns the lease, the request stays
pending; this script does not wait for that worker to finish.
"""

from sqlalchemy import create_engine

from dbmerge import MergeTracker
from product_daily_sales import ProductDailySales


if __name__ == '__main__':
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbmerge')
    try:
        with MergeTracker(engine) as tracker:
            mart = ProductDailySales(engine, tracker=tracker)
            if not mart.run_once(rebuild=True):
                print('No pass ran here; another worker may be handling the rebuild request.')
    finally:
        engine.dispose()