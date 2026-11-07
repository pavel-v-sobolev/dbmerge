"""CockroachDB-specific transaction retry support."""

import random
import time

from sqlalchemy.exc import DBAPIError

from .dbmerge import logger

MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 0.05
MAX_BACKOFF_SECONDS = 1.0


def _is_retryable(error: BaseException) -> bool:
    """CockroachDB reports transaction restarts as SQLSTATE 40001."""
    current = error
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, 'sqlstate', None) == '40001' or getattr(current, 'pgcode', None) == '40001':
            return True
        current = current.__cause__ or current.__context__ or getattr(current, 'orig', None)
    return False


def _reset_statistics(merge):
    merge.total_row_count = 0
    merge.inserted_row_count = 0
    merge.updated_row_count = 0
    merge.deleted_row_count = 0
    merge.total_time = 0
    merge.temp_insert_time = 0
    merge.insert_time = 0
    merge.update_time = 0
    merge.delete_time = 0


def execute_merge(merge, run_steps):
    """Retry the whole merge after a CockroachDB serialization failure.

    On retry, use a fresh session and staging table. Previously committed stages (when
    commit_all_steps=True) are idempotent merge operations and are safely recomputed.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            return run_steps()
        except DBAPIError as error:
            if not _is_retryable(error) or attempt >= MAX_RETRIES:
                raise
            try:
                merge.conn.rollback()
            except Exception:
                pass
            # Cockroach staging tables are regular tables, not session-local TEMP tables.
            # Remove the failed attempt's table before creating the replacement.
            try:
                merge._drop_temp_table()
            except Exception as cleanup_error:
                logger.warning(f'Could not drop failed CockroachDB staging table before retry: {cleanup_error}')
            merge._close_connection()
            merge.conn = merge.engine.connect()
            _reset_statistics(merge)
            merge._create_temp_table()
            delay = min(INITIAL_BACKOFF_SECONDS * (2 ** attempt), MAX_BACKOFF_SECONDS)
            delay *= random.uniform(0.8, 1.2)
            logger.warning(f'CockroachDB transaction retry {attempt + 1}/{MAX_RETRIES} '
                           f'after SQLSTATE 40001; waiting {delay:.2f}s')
            time.sleep(delay)