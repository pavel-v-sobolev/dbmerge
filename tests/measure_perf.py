"""
Measures a merge for the benchmark in README.md.

A table is loaded with half of the rows first, then merged with a modified full set: about 65% of the rows
are inserted, 7% updated and 15% deleted. The time of that second merge is what is reported.

    python tests/measure_perf.py                                  every engine of test_all, 100k and 1M rows
    python tests/measure_perf.py postgres postgres-pg8000 --sizes 100000

The engines and their URLs are those of test_all.py, DBMERGE_TEST_URL_<NAME> included. The sample data comes
from data/sample_data.sqlite (tests/sample_data_in_sqlite.py creates it). Every result is also appended as a
line of JSON to data/perf_results.jsonl.

Measure CockroachDB on a fresh cluster: one that has just been through the test suite, which drops and creates
large tables over and over, merged 3-5 times slower in the same benchmark.
"""

import argparse
import json
import logging
import os
import platform
from datetime import datetime

import sqlalchemy
from sqlalchemy import create_engine, String

from sample_data_in_sqlite import get_data, get_modified_data
from test_all import engines
from dbmerge import dbmerge, drop_table_if_exists

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger()

RESULTS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'perf_results.jsonl')


def prepare_and_clean_data(engine):
    drop_table_if_exists(engine,'Facts',schema='target')
    drop_table_if_exists(engine,'Facts_source',schema='source')


def measure_performance(engine_name, size):

    logger.info(f'MEASURE PERFORMANCE {engine_name} {size}')
    # The engine as a user gets it, without the settings of the tests (READ COMMITTED on CockroachDB).
    engine = create_engine(engines[engine_name])

    key = ['Shop','Product','Date']
    data_types = {'Shop':String(100),'Product':String(100)}

    prepare_and_clean_data(engine)

    logger.info('Create source table')
    data = get_data(limit = size//2)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                  inserted_on_field='Inserted On',merged_on_field='Merged On',key=key, data_types=data_types) as merge:
        merge.exec()

    logger.info('Now modify some data and load to Facts table')
    data = get_modified_data(limit = size)

    with dbmerge(engine=engine, data=data, table_name="Facts", schema='target', temp_schema='tmp',
                 delete_mode='delete', inserted_on_field='Inserted On',merged_on_field='Merged On',
                 key=key, data_types=data_types) as merge:
        result = merge.exec()
        logger.info('SHARES: '
        f'inserted  {round(result.inserted_row_count/merge.total_row_count*100)}% '
        f'updated  {round(result.updated_row_count/merge.total_row_count*100)}% '
        f'deleted  {round(result.deleted_row_count/merge.total_row_count*100)}%')

    prepare_and_clean_data(engine)
    engine.dispose()
    return result


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Measure the merge time for the README benchmark.')
    parser.add_argument('engines', nargs='*', default=list(engines), help='engine names of test_all.py')
    parser.add_argument('--sizes', nargs='+', type=int, default=[100000, 1000000])
    args = parser.parse_args()

    for size in args.sizes:
        for engine_name in args.engines:
            try:
                result = measure_performance(engine_name, size)
            except Exception as e:
                # A missing driver or any other failure is reported, and the run goes on with the next engine.
                logger.error(f'{engine_name} {size}: {type(e).__name__}: {e}')
                continue
            line = {'engine': engine_name, 'size': size, 'total_time': round(result.total_time, 2),
                    'temp_insert_time': round(result.temp_insert_time, 2),
                    'sqlalchemy': sqlalchemy.__version__, 'python': platform.python_version(),
                    'measured_at': datetime.now().isoformat(timespec='seconds')}
            logger.info(json.dumps(line))
            with open(RESULTS_FILE, 'a') as f:
                f.write(json.dumps(line) + '\n')
