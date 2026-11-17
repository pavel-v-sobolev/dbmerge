"""Reject unsafe scheduler settings before opening connections or starting threads."""

from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine

from dbmerge import IncorrectParameter, Lease, MartLoop, MergeTracker


INVALID_POSITIVE = [True, False, 0, -1, float('nan'), float('inf'), -float('inf'), '1', None]


@pytest.mark.parametrize('field', ['heartbeat_period', 'heartbeat_ttl'])
@pytest.mark.parametrize('value', INVALID_POSITIVE)
def test_tracker_rejects_invalid_timing(field, value):
    engine = create_engine('sqlite://')
    try:
        with pytest.raises(IncorrectParameter, match=field):
            with MergeTracker(engine, **{field: value}):
                pass
    finally:
        engine.dispose()


@pytest.mark.parametrize('field', ['period', 'ttl', 'retry_period'])
@pytest.mark.parametrize('value', INVALID_POSITIVE)
def test_lease_rejects_invalid_timing(field, value):
    engine = create_engine('sqlite://')
    try:
        with pytest.raises(IncorrectParameter, match=field):
            lease = Lease(engine, Mock(), 'test', **{field: value})
            lease.close()
    finally:
        engine.dispose()


@pytest.mark.parametrize('field', ['poll_interval', 'min_interval'])
@pytest.mark.parametrize('value', INVALID_POSITIVE)
def test_mart_rejects_invalid_timing(field, value, tmp_path):
    if field == 'min_interval' and value == 0 and not isinstance(value, bool):
        return  # zero intentionally disables throttling
    engine = create_engine(f'sqlite:///{tmp_path / "mart.sqlite"}')
    try:
        with MergeTracker(engine) as tracker:
            with pytest.raises(IncorrectParameter, match=field):
                loop = MartLoop(engine, tracker, 'mart', ['source'], lambda ctx: None,
                                lease_options=None, **{field: value})
                loop.close()
    finally:
        engine.dispose()