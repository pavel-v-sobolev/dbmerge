"""Tests for dbmerge.tracking: the merge journal and its readers."""

import threading
import time
import uuid
from decimal import Decimal
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select, update
from sqlalchemy.exc import NoSuchModuleError

from dbmerge import (MergeTracker, dbmerge, IncorrectParameter, drop_table_if_exists)

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
    tr = MergeTracker(engine, table_name=f"tracking_{uuid.uuid4().hex[:8]}",
                      schema=_schema(engine), heartbeat_period=0.05, heartbeat_ttl=1.0)
    yield tr
    tr.close()


def _rows(name="a"):
    return [{"id": 1, "name": name}, {"id": 2, "name": name}]


def test_process_crash_after_commit_recovers_durable_change(engine, tracker):
    import subprocess
    import sys

    destination = f"crashed_{uuid.uuid4().hex[:8]}"
    script = """
import os, sys
from sqlalchemy import create_engine, text
from dbmerge import MergeTracker
engine = create_engine(sys.argv[1])
tracker = MergeTracker(engine, table_name=sys.argv[2], schema=sys.argv[3] or None)
def write():
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE ' + sys.argv[4] + ' (id INTEGER)'))
        conn.execute(text('INSERT INTO ' + sys.argv[4] + ' VALUES (1)'))
    os._exit(73)
tracker.exec(write, destination=sys.argv[5], scope='changes')
"""
    qualified = f"{tracker.schema}.{destination}" if tracker.schema else destination
    try:
        result = subprocess.run([sys.executable, "-c", script,
                                 engine.url.render_as_string(hide_password=False),
                                 tracker.table.name, tracker.schema or "", qualified, destination],
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 73, result.stderr
        with engine.connect() as conn:
            assert conn.exec_driver_sql(f"SELECT id FROM {qualified}").scalar_one() == 1
        with engine.connect() as conn:
            operation = conn.execute(select(tracker.table).where(
                tracker.table.c.table_name == destination,
                tracker.table.c.row_kind == "operation")).one()
            assert operation.finished_at is None
        # Deterministically expire the dead process; do not wait for a production TTL.
        with engine.begin() as conn:
            conn.execute(update(tracker.table).where(tracker.table.c.table_name == destination)
                         .values(heartbeat_at=datetime(2000, 1, 1)))
        assert tracker.recover_abandoned() == 1
        tracker.exec(lambda: False, destination=destination, scope="changes")
        assert tracker.is_changed_since(destination, datetime(2000, 1, 1), scope="changes")
        assert tracker.recover_abandoned() == 0
    finally:
        drop_table_if_exists(engine, destination, schema=tracker.schema)


@pytest.mark.parametrize("kind", ["tracker", "lease"])
def test_heartbeat_retries_after_failure(engine, tracker, monkeypatch, caplog, kind):
    from dbmerge import Lease

    target = tracker if kind == "tracker" else Lease(engine, tracker.table, uuid.uuid4().hex,
                                                     period=0.01)
    failed, retried = threading.Event(), threading.Event()

    def heartbeat():
        if not failed.is_set():
            failed.set()
            raise RuntimeError("injected heartbeat failure")
        retried.set()

    monkeypatch.setattr(target, "heartbeat", heartbeat)
    if kind == "tracker":
        tracker.heartbeat_period = 0.01
        tracker._in_flight.add("test-retry")
    else:
        target._held.add("test-retry")
    try:
        target._ensure_heartbeat()
        assert retried.wait(5)
        assert "injected heartbeat failure" in caplog.text
    finally:
        if kind == "tracker":
            tracker._in_flight.clear()
        else:
            target._held.clear()
        target.close()


@pytest.mark.parametrize("kind", ["tracker", "lease"])
def test_close_timeout_keeps_heartbeat_engine_alive(engine, tracker, monkeypatch, kind):
    import importlib
    from unittest.mock import Mock
    from dbmerge import Lease

    target = tracker if kind == "tracker" else Lease(engine, tracker.table, uuid.uuid4().hex)
    module = importlib.import_module(f"dbmerge.{'tracking' if kind == 'tracker' else 'lease'}")
    monkeypatch.setattr(module, "HEARTBEAT_JOIN_TIMEOUT" if kind == "tracker" else "LEASE_JOIN_TIMEOUT", 0.01)
    entered, release = threading.Event(), threading.Event()

    def heartbeat():
        entered.set()
        release.wait(10)

    monkeypatch.setattr(target, "heartbeat", heartbeat)
    dispose = Mock(wraps=target.heartbeat_engine.dispose)
    monkeypatch.setattr(target.heartbeat_engine, "dispose", dispose)
    try:
        target._ensure_heartbeat()
        assert entered.wait(5)
        with pytest.raises(RuntimeError, match="heartbeat did not stop"):
            target.close()
        dispose.assert_not_called()
    finally:
        release.set()
        target._heartbeat_thread.join(5)
        target.close()
    dispose.assert_called_once()


def test_exec_records_finish_and_changed(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, result = tracker.exec_merge(merge)

    assert result.inserted_row_count == 2
    assert record.table_name == "orders"
    assert record.finished_at is not None
    assert record.changed is True

    last = tracker.last_merge("orders")
    assert last is not None and last.changed is True


def test_exec_noop_is_not_changed(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, _ = tracker.exec_merge(merge)

    assert record.changed is False


def test_is_changed_since(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        second, _ = tracker.exec_merge(merge)

    assert second.changed is True
    # A change happened after this moment (well before the two merges).
    before = second.started_at - timedelta(seconds=10)
    assert tracker.is_changed_since(["orders"], since=before) is True
    # Nothing changed after the second merge finished.
    assert tracker.is_changed_since(["orders"], since=second.finished_at) is False


def test_one_finished_row_per_table(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    with _dbmerge(engine, table_name="orders", data=[{"id": 1, "name": "x"}], key=["id"]) as merge:
        tracker.exec_merge(merge)

    t = tracker.table
    with engine.connect() as conn:
        count = conn.scalar(
            select(func.count()).select_from(t)
            .where(t.c.table_name == "orders", t.c.finished_at.is_not(None)))
    assert count == 1


def test_exec_failure_leaves_unfinished(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.exec_merge(merge)

    assert tracker.last_merge("orders") is None


def test_recover_abandoned_marks_changed(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.exec_merge(merge)

    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))

    assert tracker.recover_abandoned() == 1
    assert tracker.is_changed_since(["orders"], since=datetime(2000, 1, 1)) is True


def test_safe_boundary_holds_in_progress_merge(tracker):
    seen = {}

    class SlowMerge:
        table_name = "orders"
        inserted_row_count = updated_row_count = deleted_row_count = 0
        merge_finished = False

        def exec(self, **kwargs):
            seen["safe_boundary"] = tracker.safe_boundary(["orders"])
            return None

    record, _ = tracker.exec_merge(SlowMerge())
    # While the merge ran, the boundary was held at its start (engine precision may vary).
    assert seen["safe_boundary"] <= record.started_at
    # Once finished, it no longer holds the boundary.
    assert tracker.safe_boundary(["orders"]) >= record.started_at


def test_safe_boundary_ignores_abandoned_merge(engine, tracker):
    class Boom(Exception):
        pass

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        merge.exec = lambda **kw: (_ for _ in ()).throw(Boom())
        with pytest.raises(Boom):
            tracker.exec_merge(merge)

    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))
    # Sleep past the merge's start tick: a held boundary would sit at started_at, a stale one at
    # "now", and on a second-resolution engine the two are only distinguishable one tick apart.
    time.sleep(1.1)
    with engine.connect() as conn:
        started_at = conn.scalar(
            select(tracker.table.c.started_at).where(tracker.table.c.finished_at.is_(None)))
    assert tracker.safe_boundary(["orders"]) > started_at


def test_watermark_and_reset(engine, tracker):
    with _dbmerge(engine, table_name="mart", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
        tracker.mark_watermark("mart", datetime(2026, 1, 1, 12, 0, 0), schema=_schema(engine))

    assert tracker.watermark("mart") is not None
    tracker.reset("mart")
    assert tracker.watermark("mart") is None


def test_constructor_exec_defaults_used(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"],
                 chunk_size=1, bulk_load=False, commit_all_steps=False) as merge:
        tracker.exec_merge(merge)
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


def test_schema_identity_recorded(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, _ = tracker.exec_merge(merge)

    assert record.table_name == "orders"
    assert record.schema == (_schema(engine) or "")

    last = tracker.last_merge("orders")
    assert last.schema == (_schema(engine) or "")


def test_schema_filter(engine, tracker):
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)

    schema = _schema(engine) or ""
    assert tracker.last_merge("orders", schema=schema) is not None
    assert tracker.last_merge("orders", schema="other") is None


def test_exec_with_lease_skips_and_waits(engine):
    table_name = f"tracking_{uuid.uuid4().hex[:8]}"
    schema = _schema(engine)
    tracker_a = MergeTracker(engine, table_name=table_name, schema=schema,
                             owner="a", heartbeat_period=0.05, heartbeat_ttl=1.0)
    tracker_b = MergeTracker(engine, table_name=table_name, schema=schema,
                             owner="b", heartbeat_period=0.05, heartbeat_ttl=1.0)

    a_started = threading.Event()

    class SlowMerge:
        table_name = "orders"
        inserted_row_count = updated_row_count = deleted_row_count = 0
        merge_finished = False

        def exec(self, **kwargs):
            a_started.set()
            time.sleep(0.4)
            self.inserted_row_count = 1
            return None

    class FastMerge:
        table_name = "orders"
        inserted_row_count = 1
        updated_row_count = deleted_row_count = 0
        merge_finished = False

        def exec(self, **kwargs):
            return None

    thread = threading.Thread(target=tracker_a.exec_merge, args=(SlowMerge(),),
                              kwargs={"with_lease": True}, daemon=True)
    thread.start()
    assert a_started.wait(timeout=5)

    # Without wait_lease the held lease skips the merge entirely.
    record, result = tracker_b.exec_merge(FastMerge(), with_lease=True, wait_lease=False)
    assert record is None and result is None

    # With wait_lease it polls until the holder releases it, then runs.
    record, result = tracker_b.exec_merge(FastMerge(), with_lease=True, wait_lease=True,
                                    wait_interval=0.05, wait_timeout=10)
    assert record is not None and record.changed is True

    thread.join(timeout=5)
    assert not thread.is_alive()

    tracker_a.close()
    tracker_b.close()


def test_finished_rows_and_watermarks_are_schema_isolated(tracker):
    class Merge:
        table_name = "same_name"
        inserted_row_count = 1
        updated_row_count = deleted_row_count = 0

        def __init__(self, schema):
            self.schema = schema

        def exec(self):
            return None

    for schema in ("first", "second"):
        tracker.exec_merge(Merge(schema))
        tracker.mark_watermark("same_name", datetime(2026, 1, 1), schema=schema)
    assert tracker.last_merge("same_name", schema="first") is not None
    assert tracker.last_merge("same_name", schema="second") is not None
    tracker.mark_watermark("same_name", datetime(2026, 2, 1), schema="first")
    assert tracker.watermark("same_name", schema="second") is not None
    tracker.reset("same_name", schema="first")
    assert tracker.last_merge("same_name", schema="first") is None
    assert tracker.last_merge("same_name", schema="second") is not None


def test_recovery_does_not_finish_lease_rows(engine, tracker):
    from dbmerge import Lease

    lease = Lease(engine, tracker.table, "test-owner")
    lease.ensure("expired")
    with engine.begin() as conn:
        conn.execute(update(tracker.table).where(
            tracker.table.c.table_name == lease.key("expired"))
            .values(heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 0
    with engine.connect() as conn:
        assert conn.scalar(select(tracker.table.c.finished_at).where(
            tracker.table.c.table_name == lease.key("expired"))) is None
    lease.close()


@pytest.mark.parametrize("value, expected", [
    (False, False), (0, False), (0.0, False), (0j, False), (Decimal("0"), False),
    (True, True), (1, True), (-1, True), (None, True), ([], True), ({}, True),
    ("", True), ("0", True),
])
def test_exec_callable_result_semantics(tracker, value, expected):
    record, result = tracker.exec(lambda: value, destination=("target", "sql_write"),
                                  scope="full_load")
    assert result is value
    assert record.changed is expected
    assert record.scope == "full_load"
    last = tracker.last_merge("sql_write", schema="target")
    assert last.changed is expected and last.scope == "full_load"


def test_exec_registers_before_call_and_recovers_failure(engine, tracker):
    def operation():
        with engine.connect() as conn:
            row = conn.execute(select(tracker.table).where(
                tracker.table.c.table_name == "sql_write",
                tracker.table.c.row_kind == "operation")).one()
        assert row.finished_at is None
        assert row.scope == "changes"
        assert row.id in tracker._in_flight
        raise RuntimeError("write may have committed")

    with pytest.raises(RuntimeError, match="write may have committed"):
        tracker.exec(operation, destination="sql_write", scope="changes")
    assert not tracker._in_flight
    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 1
    last = tracker.last_merge("sql_write")
    assert last.changed and last.scope == "changes"


def test_exec_tracks_committed_sql(engine, tracker):
    from sqlalchemy import Column, Integer, MetaData, Table, insert

    table = Table(f"write_{uuid.uuid4().hex[:8]}", MetaData(), Column("id", Integer),
                  schema=_schema(engine))
    table.create(engine)
    try:
        driver_results = []

        def operation():
            with engine.begin() as conn:
                count = conn.execute(insert(table).values(id=1)).rowcount
                driver_results.append(count)
                return count

        record, count = tracker.exec(operation, destination=(table.schema, table.name))
        # Some drivers report -1 (unknown); exec must preserve it, not normalize it to 1.
        assert driver_results == [count]
        assert count != 0 and record.changed
        with engine.connect() as conn:
            assert conn.scalar(select(table.c.id)) == 1
    finally:
        table.drop(engine)


@pytest.mark.parametrize("destination", [None, (), ("schema",), (123, "table"), "", "lease:reserved"])
def test_exec_rejects_invalid_destination(tracker, destination):
    with pytest.raises(IncorrectParameter):
        tracker.exec(lambda: 1, destination=destination)
    assert not tracker._in_flight


@pytest.mark.parametrize("scope", [False, 0, "", " ", "x" * 256])
def test_exec_rejects_invalid_scope(tracker, scope):
    with pytest.raises(IncorrectParameter):
        tracker.exec(lambda: 1, destination="write", scope=scope)


def test_exec_merge_delegates_and_preserves_result(engine, tracker):
    from unittest.mock import patch

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        with patch.object(tracker, "exec", wraps=tracker.exec) as execute:
            record, result = tracker.exec_merge(merge, scope="changes", chunk_size=1)
        execute.assert_called_once()
        assert result.inserted_row_count == 2 and record.changed
        assert record.scope == "changes" and merge.chunk_size == 1
    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        record, result = tracker.exec_merge(merge, scope="full_load")
        assert not record.changed and result.inserted_row_count == 0


def test_dbmerge_optional_tracker_and_noop(engine, tracker):
    from unittest.mock import patch

    for expected_changed in (True, False):
        with _dbmerge(engine, tracker=tracker, table_name='orders', data=_rows(), key=['id']) as merge:
            with patch.object(tracker, 'exec', wraps=tracker.exec) as execute, \
                    patch.object(tracker, 'close') as close:
                result = merge.exec(chunk_size=1)
                execute.assert_called_once()
                close.assert_not_called()
            assert merge.tracking_record.changed is expected_changed
            assert result.inserted_row_count == (2 if expected_changed else 0)
            assert merge.chunk_size == 1
            with pytest.raises(IncorrectParameter, match='already finished'):
                merge.exec()
    with _dbmerge(engine, table_name='orders', data=_rows(), key=['id']) as merge:
        merge.exec()
        assert merge.tracker is None and merge.tracking_record is None


def test_dbmerge_tracker_failure_and_validation(engine, tracker):
    from unittest.mock import patch

    with pytest.raises(IncorrectParameter, match='tracker'):
        _dbmerge(engine, tracker=object(), table_name='orders', data=_rows(), key=['id'])
    with _dbmerge(engine, tracker=tracker, table_name='orders', data=_rows(), key=['id']) as merge:
        with patch.object(merge, '_execute_steps', side_effect=RuntimeError('write failed')):
            with pytest.raises(RuntimeError, match='write failed'):
                merge.exec()
        assert merge.merge_finished
        assert not tracker._in_flight
        assert not tracker._closed.is_set()
    tracker.close()
    with pytest.raises(IncorrectParameter, match='open'):
        _dbmerge(engine, tracker=tracker, table_name='orders', data=_rows(), key=['id'])


def test_bound_tracker_legacy_adapter_does_not_track_twice(engine, tracker):
    from unittest.mock import patch

    with _dbmerge(engine, tracker=tracker, table_name='orders', data=_rows(), key=['id']) as merge:
        with patch.object(tracker, 'exec', wraps=tracker.exec) as execute:
            record, result = tracker.exec_merge(merge)
        execute.assert_called_once()
        assert record.changed and result.inserted_row_count == 2


def test_existing_tracking_table_gets_scope_column(engine):
    from sqlalchemy import Column, MetaData, String, Table, inspect

    table = Table(f"old_tracking_{uuid.uuid4().hex[:8]}", MetaData(),
                  Column("id", String(64), primary_key=True), schema=_schema(engine))
    table.create(engine)
    try:
        with MergeTracker(engine, table_name=table.name, schema=table.schema):
            assert "scope" in {column["name"] for column in inspect(engine).get_columns(
                table.name, schema=table.schema)}
    finally:
        table.drop(engine)


def test_exec_merge_preserves_exact_result_and_detects_updated_deleted_counts(tracker):
    sentinel = object()

    class Merge:
        table_name = "counter_write"
        inserted_row_count = 0
        updated_row_count = 1
        deleted_row_count = 2

        def exec(self):
            return sentinel

    record, result = tracker.exec_merge(Merge(), scope="changes")
    assert record.changed is True and result is sentinel


def test_safe_boundary_includes_active_operations_of_every_scope(tracker):
    # This tests scope selection, not heartbeat expiry. Oracle stores heartbeat
    # timestamps at second precision; a one-second TTL can expire at a boundary
    # before the first background heartbeat runs on a busy CI runner.
    tracker.heartbeat_ttl = 60.0
    for scope in ("full_load", "changes"):
        def operation():
            with tracker.engine.connect() as conn:
                started_at = conn.scalar(select(tracker.table.c.started_at).where(
                    tracker.table.c.table_name == "scoped_write",
                    tracker.table.c.row_kind == "operation",
                    tracker.table.c.finished_at.is_(None)))
            assert tracker.safe_boundary(["scoped_write"]) == started_at
            return False

        tracker.exec(operation, destination="scoped_write", scope=scope)


def test_noop_and_watermark_do_not_erase_change(tracker):
    changed, _ = tracker.exec(lambda: True, destination="write", scope="changes")
    before = changed.started_at - timedelta(seconds=10)
    tracker.exec(lambda: False, destination="write", scope="changes")
    tracker.exec(lambda: 0, destination="write", scope="full_load")
    tracker.mark_watermark("write", changed.finished_at)
    assert tracker.is_changed_since("write", since=before)
    assert tracker.is_changed_since("write", since=before, scope="changes")
    assert not tracker.is_changed_since("write", since=before, scope="full_load")
    assert not tracker.is_changed_since("write", since=changed.finished_at)
    assert not tracker.last_merge("write").changed


def test_change_marks_are_destination_and_scope_isolated(tracker):
    record, _ = tracker.exec(lambda: 1, destination=("first", "same"), scope="changes")
    before = record.started_at - timedelta(seconds=10)
    tracker.exec(lambda: False, destination=("second", "same"), scope="changes")
    tracker.exec(lambda: False, destination=("first", "other"), scope="changes")
    tracker.exec(lambda: False, destination=("first", "same"), scope="full_load")
    assert tracker.is_changed_since("same", before, schema="first", scope="changes")
    assert not tracker.is_changed_since("same", before, schema="second")
    assert not tracker.is_changed_since("other", before, schema="first")
    assert not tracker.is_changed_since("same", before, schema="first", scope="full_load")


def test_recovery_signal_survives_noop_and_restart(engine, tracker):
    row_id, started = tracker._begin("write", "target", "full_load")
    tracker._abandon(row_id)
    tracker.close()  # Drain an already-running heartbeat before simulating expiration.
    with engine.begin() as conn:
        conn.execute(update(tracker.table).where(tracker.table.c.id == row_id)
                     .values(heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 1
    assert tracker.recover_abandoned() == 0
    with MergeTracker(engine, table_name=tracker.table.name, schema=tracker.schema) as restarted:
        restarted.exec(lambda: False, destination=("target", "write"), scope="full_load")
        assert restarted.is_changed_since("write", started - timedelta(seconds=10),
                                          schema="target", scope="full_load")


def test_change_publication_rolls_back_with_failed_finish(engine, tracker):
    from unittest.mock import patch

    publish = tracker._publish_change

    def fail_after_publication(*args):
        publish(*args)
        raise RuntimeError("completion failed")

    with patch.object(tracker, "_publish_change", side_effect=fail_after_publication):
        with pytest.raises(RuntimeError, match="completion failed"):
            tracker.exec(lambda: 1, destination="write", scope="changes")
    assert not tracker.is_changed_since("write", datetime(2000, 1, 1))
    with engine.connect() as conn:
        assert conn.execute(select(tracker.table.c.finished_at).where(
            tracker.table.c.row_kind == "operation")).one().finished_at is None
    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 1
    assert tracker.is_changed_since("write", datetime(2000, 1, 1), scope="changes")


def test_out_of_order_completion_preserves_change(tracker):
    first, started = tracker._begin("write", "", "changes")
    second, _ = tracker._begin("write", "", "changes")
    tracker._finish(second, "write", True, None)
    tracker._finish(first, "write", False, None)
    assert not tracker.last_merge("write").changed
    assert tracker.is_changed_since("write", started - timedelta(seconds=10), scope="changes")


def test_change_timestamp_never_moves_backwards(engine, tracker):
    tracker._ensure_change_row("write", "", "changes")
    newer, older = datetime(2026, 2, 1), datetime(2026, 1, 1)
    for stamp in (newer, older):
        with engine.begin() as conn:
            tracker._publish_change(conn, "write", "", "changes", stamp)
    assert tracker.is_changed_since("write", older, scope="changes")
    assert not tracker.is_changed_since("write", newer, scope="changes")


def test_backfills_legacy_changed_rows(engine, tracker):
    from sqlalchemy import insert

    stamp = tracker._db_now()
    with engine.begin() as conn:
        conn.execute(insert(tracker.table).values(
            id=uuid.uuid4().hex, owner="legacy", table_name="write", schema="target",
            scope="changes", started_at=stamp, heartbeat_at=stamp, finished_at=stamp, changed=True))
    with MergeTracker(engine, table_name=tracker.table.name, schema=tracker.schema) as migrated:
        migrated.exec(lambda: False, destination=("target", "write"), scope="changes")
        assert migrated.is_changed_since("write", stamp - timedelta(seconds=10),
                                         schema="target", scope="changes")


def test_reset_clears_marks_but_active_operation_can_publish(tracker):
    record, _ = tracker.exec(lambda: 1, destination=("first", "write"), scope="changes")
    tracker.exec(lambda: 1, destination=("second", "write"), scope="changes")
    before = record.started_at - timedelta(seconds=10)
    active, _ = tracker._begin("write", "first", "changes")
    tracker.reset("write", schema="first")
    assert not tracker.is_changed_since("write", before, schema="first")
    assert tracker.is_changed_since("write", before, schema="second")
    tracker._finish(active, "write", True, None, "first")
    assert tracker.is_changed_since("write", before, schema="first")


def test_concurrent_writers_share_durable_change_mark(engine, tracker):
    ready = threading.Barrier(2)
    release_noop = threading.Event()
    failures = []
    before = tracker._db_now() - timedelta(seconds=10)
    with MergeTracker(engine, table_name=tracker.table.name, schema=tracker.schema,
                      heartbeat_period=0.05, heartbeat_ttl=10) as other:
        def run(current, changed):
            def operation():
                ready.wait(timeout=10)
                if not changed:
                    assert release_noop.wait(timeout=10)
                return changed

            try:
                current.exec(operation, destination="concurrent", scope="changes")
            except BaseException as exc:
                failures.append(exc)
            finally:
                if changed:
                    release_noop.set()

        workers = [threading.Thread(target=run, args=(current, changed), daemon=True)
                   for current, changed in ((tracker, True), (other, False))]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=20)
        assert not any(worker.is_alive() for worker in workers)
        assert failures == []
        assert tracker.is_changed_since("concurrent", before, scope="changes")
        assert other.is_changed_since("concurrent", before, scope="changes")
        assert not tracker.last_merge("concurrent").changed


def test_recovery_rechecks_refreshed_heartbeat_before_publishing(engine, tracker):
    from unittest.mock import patch

    row_id, _ = tracker._begin("write", "", "changes")
    tracker._abandon(row_id)
    with engine.begin() as conn:
        conn.execute(update(tracker.table).values(heartbeat_at=datetime(2000, 1, 1)))
    ensure = tracker._ensure_change_row

    def refresh_before_recovery(*args):
        ensure(*args)
        with engine.begin() as conn:
            conn.execute(update(tracker.table).where(tracker.table.c.id == row_id)
                         .values(heartbeat_at=func.now()))

    with patch.object(tracker, "_ensure_change_row", side_effect=refresh_before_recovery):
        assert tracker.recover_abandoned() == 0
    assert not tracker.is_changed_since("write", datetime(2000, 1, 1), scope="changes")


def test_single_tracking_table_contains_operations_and_change_state(engine, tracker):
    from sqlalchemy import inspect

    tracker.exec(lambda: 1, destination="single_table", scope="changes")
    tracker.exec(lambda: 0, destination="single_table", scope="changes")
    tracker.exec(lambda: False, destination="single_table", scope="full_load")
    assert not inspect(engine).has_table(f"{tracker.table.name}_changes", schema=tracker.schema)
    assert list(tracker.metadata.tables.values()) == [tracker.table]
    with engine.connect() as conn:
        rows = conn.execute(select(tracker.table).where(
            tracker.table.c.table_name == "single_table")).all()
    assert sum(row.row_kind == "operation" for row in rows) == 1
    assert sum(row.row_kind == "change" for row in rows) == 2
    changes = {row.scope: row for row in rows if row.row_kind == "change"}
    assert changes["changes"].changed_at is not None
    assert changes["full_load"].changed_at is None
    assert all(row.id.startswith("change:") for row in changes.values())


def test_lease_nonreentrant_and_renewal_contract(engine, tracker):
    from dbmerge import Lease
    from unittest.mock import patch
    lease = Lease(engine, tracker.table, "same-owner", period=30, ttl=10)
    other = Lease(engine, tracker.table, "same-owner", period=30, ttl=10)
    lease.ensure("exclusive")
    try:
        with patch.object(lease, "_ensure_heartbeat"):
            assert lease.acquire("exclusive")
            assert not lease.acquire("exclusive")
            assert not other.acquire("exclusive")
            assert lease.still_mine("exclusive")
            assert lease.renew("exclusive")
            with engine.begin() as conn:
                conn.execute(update(tracker.table).where(
                    tracker.table.c.table_name == lease.key("exclusive"))
                    .values(heartbeat_at=datetime(2000, 1, 1)))
            assert not lease.still_mine("exclusive")
            assert not lease.renew("exclusive")
            assert other.acquire("exclusive")
    finally:
        lease.close()
        other.close()


def test_heartbeat_uses_independent_pool(engine, tracker):
    from sqlalchemy import create_engine
    from dbmerge import Lease
    limited = create_engine(engine.url, pool_size=1, max_overflow=0, pool_timeout=0.2)
    try:
        with MergeTracker(limited, table_name=tracker.table.name, schema=tracker.schema) as current:
            row_id, _ = current._begin("pool_source", "", "changes")
            lease = Lease(limited, current.table, "pool-owner", heartbeat_engine=current.heartbeat_engine)
            lease.ensure("pool-lease")
            assert lease.acquire("pool-lease")
            try:
                with limited.connect():  # Consume the only working connection.
                    current.heartbeat()
                    assert lease.renew("pool-lease")
                    assert lease.still_mine("pool-lease")
            finally:
                current._abandon(row_id)
                lease.close()
    finally:
        limited.dispose()


def test_tracker_close_waits_for_operation(engine, tracker):
    started, finish, closed = threading.Event(), threading.Event(), threading.Event()
    failures = []

    def operation():
        started.set()
        assert finish.wait(timeout=10)
        return 1

    def execute():
        try:
            tracker.exec(operation, destination="closing")
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=execute, daemon=True)
    worker.start()
    assert started.wait(timeout=5)
    closer = threading.Thread(target=lambda: (tracker.close(), closed.set()), daemon=True)
    closer.start()
    try:
        assert not closed.wait(timeout=0.1)
        assert tracker._heartbeat_thread.is_alive()
    finally:
        finish.set()
        worker.join(timeout=10)
        closer.join(timeout=10)
    assert closed.is_set() and failures == []
    assert tracker.last_merge("closing").changed
    with pytest.raises(IncorrectParameter):
        tracker.exec(lambda: 1, destination="closing")


def test_caller_heartbeat_engine_is_not_disposed(engine, tracker):
    from unittest.mock import patch
    with patch.object(engine, "dispose") as dispose:
        current = MergeTracker(engine, table_name=tracker.table.name, schema=tracker.schema,
                               heartbeat_engine=engine)
        current.close()
        dispose.assert_not_called()


def test_watermark_survives_later_writes_and_recovery_compacts(engine, tracker):
    tracker.mark_watermark("progress", datetime(2026, 1, 1))
    tracker.exec(lambda: 1, destination="progress")
    tracker.exec(lambda: False, destination="progress")
    assert tracker.watermark("progress").year == 2026
    first, _ = tracker._begin("progress", "", "changes")
    second, _ = tracker._begin("progress", "", "full_load")
    tracker._abandon(first)
    tracker._abandon(second)
    tracker.close()
    with engine.begin() as conn:
        conn.execute(update(tracker.table).where(tracker.table.c.id.in_([first, second]))
                     .values(heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 2
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(tracker.table).where(
            tracker.table.c.table_name == "progress", tracker.table.c.row_kind == "operation",
            tracker.table.c.finished_at.is_not(None))) == 1
    assert tracker.watermark("progress").year == 2026


def test_change_rows_never_act_as_operations(engine, tracker):
    tracker._ensure_change_row("state_only", "", "changes")
    t = tracker.table
    with engine.begin() as conn:
        conn.execute(update(t).where(t.c.row_kind == "change").values(
            started_at=datetime(2000, 1, 1), heartbeat_at=func.now()))
    assert tracker.safe_boundary("state_only").year > 2000
    with engine.begin() as conn:
        conn.execute(update(t).where(t.c.row_kind == "change").values(
            heartbeat_at=datetime(2000, 1, 1)))
    assert tracker.recover_abandoned() == 0
    assert not tracker.is_changed_since("state_only", datetime(2000, 1, 1))
    # Even a state row with operation-like fields cannot leak into operation readers or pruning.
    with engine.begin() as conn:
        conn.execute(update(t).where(t.c.row_kind == "change").values(
            finished_at=func.now(), watermark=datetime(2099, 1, 1)))
    assert tracker.last_merge("state_only") is None
    assert tracker.watermark("state_only") is None
    assert tracker.reset("state_only") == 0
    tracker.mark_watermark("state_only", datetime(2026, 1, 1))
    tracker.exec(lambda: True, destination="state_only", scope="changes")
    assert tracker.is_changed_since("state_only", datetime(2000, 1, 1), scope="changes")


def test_legacy_tracking_columns_are_extended_without_another_table(engine):
    from sqlalchemy import MetaData, Table, insert, inspect

    name = f"legacy_{uuid.uuid4().hex[:8]}"
    # Use the actual previous journal definition minus the newly introduced columns.
    from dbmerge.tracking import _tracking_table
    current = _tracking_table(MetaData(), name, _schema(engine))
    legacy = Table(name, MetaData(), *(column._copy() for column in current.columns
                                     if column.name not in ("row_kind", "changed_at")),
                   schema=_schema(engine))
    legacy.create(engine)
    stamp = datetime(2026, 1, 1)
    try:
        with engine.begin() as conn:
            conn.execute(insert(legacy).values(id=uuid.uuid4().hex, table_name="write",
                         scope="changes", schema="target", started_at=stamp,
                         heartbeat_at=stamp, finished_at=stamp, changed=True))
        with MergeTracker(engine, table_name=name, schema=legacy.schema) as migrated:
            assert migrated.is_changed_since("write", datetime(2000, 1, 1),
                                             schema="target", scope="changes")
            migrated.exec(lambda: False, destination=("target", "write"), scope="changes")
            assert migrated.is_changed_since("write", datetime(2000, 1, 1))
            assert not inspect(engine).has_table(f"{name}_changes", schema=legacy.schema)
    finally:
        legacy.drop(engine)


def test_shared_tracker_initializes_one_lease(engine, tracker):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    barrier = Barrier(8)

    def get_lease():
        barrier.wait(timeout=10)
        return tracker._get_lease()

    with ThreadPoolExecutor(max_workers=8) as executor:
        leases = list(executor.map(lambda _: get_lease(), range(8)))
    assert all(lease is leases[0] for lease in leases)
