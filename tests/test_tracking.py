"""Tests for dbmerge.tracking: the merge journal and its readers."""

import time
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select, update
from sqlalchemy.exc import NoSuchModuleError

from dbmerge import (MergeTracker, dbmerge, IncorrectParameter, Database, database_of,
                     drop_table_if_exists)

from conftest import engines, make_engine

# Tracking needs a transactional shared store: ClickHouse and DuckDB are rejected by MergeTracker.
_TRACKING_ENGINES = [name for name in engines if name not in ("clickhouse", "duckdb")]


def _schema(engine):
    # dbmerge requires a database name on MySQL/MariaDB; use the same schema as test_all.py.
    return None if engine.dialect.name == "sqlite" else "target"


def _dbmerge(engine, **kwargs):
    return dbmerge(engine, schema=_schema(engine), **kwargs)


@pytest.fixture(params=_TRACKING_ENGINES)
def engine(request):
    eng = make_engine(request.param)
    # The data tables are shared across tests on a real database; drop them for isolation.
    drop_table_if_exists(eng, "orders", schema=_schema(eng))
    drop_table_if_exists(eng, "mart", schema=_schema(eng))
    return eng


@pytest.fixture()
def tracker(engine):
    # One tracking table per test, so the shared database stays isolated.
    tr = MergeTracker(engine, table=f"tracking_{uuid.uuid4().hex[:8]}",
                      schema=_schema(engine), heartbeat_period=0.05, heartbeat_ttl=1.0)
    yield tr
    tr.close()


def _rows(name="a"):
    return [{"id": 1, "name": name}, {"id": 2, "name": name}]


def test_track_records_finish_and_changed(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, result = tracker.track(merge)

    assert result.inserted_row_count == 2
    assert record.table_name == "orders"
    assert record.finished_at is not None
    assert record.changed is True

    last = tracker.last_merge("orders")
    assert last is not None and last.changed is True


def test_track_noop_is_not_changed(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.track(merge)
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, _ = tracker.track(merge)

    assert record.changed is False


def test_changed_since(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.track(merge)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        second, _ = tracker.track(merge)

    assert second.changed is True
    # A change happened after this moment (well before the two merges).
    before = second.started_at - timedelta(seconds=10)
    assert tracker.changed_since(["orders"], since=before) is True
    # Nothing changed after the second merge finished.
    assert tracker.changed_since(["orders"], since=second.finished_at) is False


def test_one_finished_row_per_table(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.track(merge)
    with _dbmerge(engine, table_name="orders", data=[{"id": 1, "name": "x"}], key=["id"]) as merge:
        tracker.track(merge)

    t = tracker.table
    with engine.connect() as conn:
        count = conn.scalar(
            select(func.count()).select_from(t)
            .where(t.c.table_name == "orders", t.c.finished_at.is_not(None)))
    assert count == 1


def test_track_failure_leaves_unfinished(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.track(merge)

    assert tracker.last_merge("orders") is None


def test_recover_abandoned_marks_changed(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.track(merge)

    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))

    assert tracker.recover_abandoned() == 1
    assert tracker.changed_since(["orders"], since=datetime(2000, 1, 1)) is True


def test_boundary_holds_in_progress_merge(tracker):
    seen = {}

    class SlowMerge:
        table_name = "orders"
        inserted_row_count = updated_row_count = deleted_row_count = 0
        merge_finished = False

        def exec(self, **kwargs):
            seen["boundary"] = tracker.boundary(["orders"])
            return None

    record, _ = tracker.track(SlowMerge(), changed=True)
    # While the merge ran, the boundary was held at its start (engine precision may vary).
    assert seen["boundary"] <= record.started_at
    # Once finished, it no longer holds the boundary.
    assert tracker.boundary(["orders"]) >= record.started_at


def test_boundary_ignores_abandoned_merge(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.track(merge)

    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))
    # Sleep past the merge's start tick: a held boundary would sit at started_at, a stale one at
    # "now", and on a second-resolution engine the two are only distinguishable one tick apart.
    time.sleep(1.1)
    with engine.connect() as conn:
        started_at = conn.scalar(
            select(tracker.table.c.started_at).where(tracker.table.c.finished_at.is_(None)))
    assert tracker.boundary(["orders"]) > started_at


def test_watermark_and_reset(engine, tracker):
    with _dbmerge(engine, table_name="mart", data=_rows(), key=["id"]) as merge:
        tracker.track(merge, boundary=datetime(2026, 1, 1, 12, 0, 0))

    assert tracker.watermark("mart") is not None
    tracker.reset("mart")
    assert tracker.watermark("mart") is None


def test_constructor_exec_defaults_used(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"],
                 chunk_size=1, bulk_load=False, commit_all_steps=False) as merge:
        tracker.track(merge)
        assert merge.chunk_size == 1
        assert merge.bulk_load is False


def test_exec_overrides_constructor_defaults(engine):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"],
                 chunk_size=1) as merge:
        merge.exec(chunk_size=3)
        assert merge.chunk_size == 3


def test_constructor_rejects_bad_exec_defaults(engine):
    with pytest.raises(IncorrectParameter):
        _dbmerge(engine, table_name="orders", data=_rows(), key=["id"],
                commit_all_steps="false")


def test_tracker_rejects_non_engine():
    with pytest.raises(IncorrectParameter):
        MergeTracker(object())


def test_tracker_rejects_untransactable_engine():
    try:
        eng = create_engine("duckdb:///:memory:")
    except (NoSuchModuleError, ImportError):
        pytest.skip("duckdb_engine not installed")
    with pytest.raises(IncorrectParameter):
        MergeTracker(eng)


def test_database_identity_recorded(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, _ = tracker.track(merge)

    db = database_of(engine)
    assert record.database_type == db.type
    assert record.server == db.server
    assert record.database_name == db.database

    last = tracker.last_merge("orders")
    assert last.database_type == db.type


def test_database_filter(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.track(merge)

    db = database_of(engine)
    other = Database("postgresql", "other:5432", "otherdb")
    assert tracker.last_merge("orders", database=db) is not None
    assert tracker.last_merge("orders", database=other) is None

