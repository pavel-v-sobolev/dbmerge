"""Tests for dbmerge.mart: the MartLoop and its lease."""

import threading
import time
import uuid

import pytest

from dbmerge import (IncorrectParameter, Lease, MartLoop, MartHandler, MergeTracker, dbmerge,
                     drop_table_if_exists, rebuild_requested, request_rebuild,
                     request_stop)

from conftest import engines, make_engine


@pytest.fixture(autouse=True)
def reset_process_stop():
    from dbmerge import reset_stop
    reset_stop()
    yield
    reset_stop()


@pytest.mark.parametrize("dialect_name", ["mssql", "oracle"])
def test_marts_boolean_default_uses_dialect_literal(dialect_name):
    import importlib
    from sqlalchemy import MetaData
    from sqlalchemy.schema import CreateTable
    from dbmerge.mart import _marts_table

    dialect = importlib.import_module(f"sqlalchemy.dialects.{dialect_name}").dialect()
    ddl = str(CreateTable(_marts_table(MetaData(), None)).compile(dialect=dialect))
    assert "DEFAULT 0" in ddl
    assert "DEFAULT false" not in ddl


def test_whole_handler_owns_watermark(engine, tracker):
    from datetime import datetime
    destination = (None, f"progress_{uuid.uuid4().hex[:8]}")
    tracker.mark_watermark(destination[1], datetime(2000, 1, 1))
    tracker.exec(lambda: 1, destination="source")
    fail = [True]

    def process_changes(ctx):
        tracker.exec(lambda: 1, destination=destination)
        assert tracker.watermark(destination[1]).year == 2000
        if fail[0]:
            raise RuntimeError("second write failed")

    loop = MartLoop(engine, tracker, destination, ["source"], process_changes)
    try:
        with pytest.raises(RuntimeError, match="second write failed"):
            loop.run_if_pending()
        assert tracker.watermark(destination[1]).year == 2000
        fail[0] = False
        assert loop.run_if_pending()
        assert tracker.watermark(destination[1]).year > 2000
        assert not loop._lease.still_mine(loop.name)
    finally:
        loop.close()


def test_rebuild_request_at_start_boundary_is_retained(engine, tracker, monkeypatch):
    destination = (None, f"starting_{uuid.uuid4().hex[:8]}")
    loop = MartLoop(engine, tracker, destination, ["source"], lambda ctx: None)
    request_rebuild(engine, destination, schema=tracker.schema)
    guard = loop._guard_progress
    requested = threading.Event()
    errors = []

    def request():
        try:
            request_rebuild(engine, destination, schema=tracker.schema)
        except Exception as exc:
            errors.append(exc)
        finally:
            requested.set()

    def guard_with_concurrent_request(conn):
        # The old token has been observed, but the rebuild-start UPDATE has not run.
        if not requested.is_set():
            thread = threading.Thread(target=request)
            thread.start()
            thread.join(10)
            assert not thread.is_alive()
            assert not errors
        guard(conn)

    monkeypatch.setattr(loop, "_guard_progress", guard_with_concurrent_request)
    try:
        assert loop.run_if_pending()
        assert rebuild_requested(engine, destination, schema=tracker.schema)
        assert loop.run_if_pending()
        assert not rebuild_requested(engine, destination, schema=tracker.schema)
    finally:
        loop.close()


def test_legacy_marts_table_migration_preserves_rebuild_state(engine, tracker, monkeypatch):
    import importlib
    from sqlalchemy import Boolean, Column, MetaData, String, Table, inspect, select
    from dbmerge.mart import _ensure_marts_table

    name = f"legacy_marts_{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(importlib.import_module("dbmerge.mart"), "MARTS_TABLE", name)
    legacy = Table(name, MetaData(),
                   Column("table_name", String(255), primary_key=True),
                   Column("schema", String(255), primary_key=True),
                   Column("full_rebuild_is_required", Boolean, nullable=False),
                   Column("rebuild_cursor", String(255)), schema=tracker.schema)
    try:
        legacy.create(engine)
        with engine.begin() as conn:
            conn.execute(legacy.insert().values(table_name="mart", schema="business", rebuild_cursor="block-1",
                                                full_rebuild_is_required=True))
        table = _ensure_marts_table(engine, tracker.schema)
        _ensure_marts_table(engine, tracker.schema)  # Repeated startup must be safe.
        assert {"rebuild_request", "rebuild_boundary"} <= {
            column["name"] for column in inspect(engine).get_columns(name, schema=tracker.schema)}
        with engine.connect() as conn:
            row = conn.execute(select(table)).one()
            assert row.rebuild_cursor == "block-1"
            assert row.full_rebuild_is_required
            assert row.rebuild_request is None and row.rebuild_boundary is None
        request_rebuild(engine, ("business", "mart"), schema=tracker.schema)
        assert rebuild_requested(engine, ("business", "mart"), schema=tracker.schema)
    finally:
        # Keep reflection (checkfirst's SELECT) outside the DDL transaction on CockroachDB.
        legacy.drop(engine, checkfirst=False)


def test_rebuild_resumes_in_new_process_after_crash(engine, tracker):
    import subprocess
    import sys
    from datetime import datetime
    from sqlalchemy import select, update
    from dbmerge.mart import _ensure_marts_table

    destination = f"restart_{uuid.uuid4().hex[:8]}"
    script = """
import os, sys
from sqlalchemy import create_engine
from dbmerge import MergeTracker, MartLoop
engine = create_engine(sys.argv[1])
tracker = MergeTracker(engine, table_name=sys.argv[2], schema=sys.argv[3] or None)
def rebuild(ctx):
    print('cursor=' + str(ctx.rebuild_from), flush=True)
    if sys.argv[5] == 'crash':
        yield 'block-1'
        os._exit(74)
    assert ctx.rebuild_from == 'block-1'
    yield 'block-2'
loop = MartLoop(engine, tracker, sys.argv[4], ['source'], lambda ctx: None, rebuild=rebuild)
assert loop.run_if_pending()
loop.close()
tracker.close()
"""
    request_rebuild(engine, destination, schema=tracker.schema)
    args = [sys.executable, "-c", script, engine.url.render_as_string(hide_password=False),
            tracker.table.name, tracker.schema or "", destination]
    first = subprocess.run(args + ["crash"], capture_output=True, text=True, timeout=30)
    assert first.returncode == 74, first.stderr
    table = _ensure_marts_table(engine, tracker.schema)
    with engine.connect() as conn:
        row = conn.execute(select(table).where(table.c.table_name == destination)).one()
        assert row.rebuild_cursor == "block-1"
        boundary = row.rebuild_boundary
        assert boundary is not None
    assert tracker.watermark(destination) is None
    with engine.begin() as conn:
        conn.execute(update(tracker.table).where(tracker.table.c.row_kind == "lease")
                     .values(heartbeat_at=datetime(2000, 1, 1)))
    second = subprocess.run(args + ["resume"], capture_output=True, text=True, timeout=30)
    assert second.returncode == 0, second.stderr
    assert "cursor=block-1" in second.stdout
    assert tracker.watermark(destination).replace(tzinfo=None) == boundary.replace(tzinfo=None)
    with engine.connect() as conn:
        assert conn.scalar(select(table.c.rebuild_cursor).where(table.c.table_name == destination)) is None


def test_disabled_scoped_consumer_catches_up(engine, tracker):
    from datetime import datetime
    destination = (None, f"scoped_{uuid.uuid4().hex[:8]}")
    tracker.mark_watermark(destination[1], datetime(2000, 1, 1))
    calls = []
    loop = MartLoop(engine, tracker, destination, ["scoped_source"], calls.append,
                    enabled=False)
    try:
        tracker.exec(lambda: 1, destination="scoped_source", scope="full_load")
        assert not loop.run_if_pending()
        loop.enabled = True
        assert loop.run_if_pending()
        calls.clear()
        loop.enabled = False
        if engine.dialect.name in ("sqlite", "mysql", "mariadb", "oracle"):
            time.sleep(1.1)
        tracker.exec(lambda: 1, destination="scoped_source", scope="changes")
        tracker.exec(lambda: False, destination="scoped_source", scope="changes")
        assert not loop.run_if_pending()
        loop.enabled = True
        assert loop.run_if_pending()
        assert len(calls) == 1
    finally:
        loop.close()


def test_rebuild_request_during_active_empty_cursor_is_retained(engine, tracker):
    destination = (None, f"repeat_{uuid.uuid4().hex[:8]}")
    fail = [True]
    starts = []

    def rebuild(ctx):
        starts.append(ctx.rebuild_from)
        if fail[0]:
            fail[0] = False
            request_rebuild(engine, destination, schema=tracker.schema)
            raise RuntimeError("before first block")
        yield "block"

    loop = MartLoop(engine, tracker, destination, ["source"], lambda ctx: None, rebuild=rebuild)
    try:
        request_rebuild(engine, destination, schema=tracker.schema)
        with pytest.raises(RuntimeError, match="before first block"):
            loop.run_if_pending()
        assert loop._rebuild_cursor() == ""
        assert rebuild_requested(engine, destination, schema=tracker.schema)
        assert loop.run_if_pending()  # Resume the old rebuild, not consume the new request.
        assert rebuild_requested(engine, destination, schema=tracker.schema)
        assert loop.run_if_pending()  # New requested rebuild.
        assert not rebuild_requested(engine, destination, schema=tracker.schema)
        assert starts == [None, "", None]
    finally:
        loop.close()


def test_lease_loss_does_not_commit_watermark(engine, tracker):
    from datetime import datetime
    from sqlalchemy import update
    destination = (None, f"lost_{uuid.uuid4().hex[:8]}")
    tracker.mark_watermark(destination[1], datetime(2000, 1, 1))
    tracker.exec(lambda: 1, destination="source")

    def process_changes(ctx):
        with engine.begin() as conn:
            conn.execute(update(tracker.table).where(
                tracker.table.c.table_name == loop._lease.key(loop.name)).values(owner="successor"))

    loop = MartLoop(engine, tracker, destination, ["source"], process_changes)
    try:
        with pytest.raises(RuntimeError, match="Lease lost"):
            loop.run_if_pending()
        assert tracker.watermark(destination[1]).year == 2000
    finally:
        loop.close()


def test_process_stop_reaches_direct_and_future_loops(engine, tracker):
    from dbmerge import reset_stop
    loop = RecordingHandler(engine, tracker=tracker).loop()
    request_stop()
    future = RecordingHandler(engine, tracker=tracker).loop()
    try:
        assert not loop.run_if_pending()
        assert not future.run_if_pending()
        reset_stop()
        assert loop._stop_requested() and future._stop_requested()
    finally:
        loop.close()
        future.close()


def test_scope_filter_does_not_exclude_full_load_from_boundary(engine, tracker):
    from datetime import datetime
    # This tests scope, not expiry. SQLite's second-resolution clock can make a
    # one-second test TTL expire across a second boundary before its next heartbeat.
    tracker.heartbeat_ttl = 60.0
    destination = (None, f"boundary_{uuid.uuid4().hex[:8]}")
    tracker.mark_watermark(destination[1], datetime(2000, 1, 1))
    active, started = tracker._begin("source", "", "full_load")
    # Compare against the stored value: Oracle DATE rounds away fractional seconds.
    from sqlalchemy import select
    with engine.connect() as conn:
        started = conn.scalar(select(tracker.table.c.started_at).where(tracker.table.c.id == active))
    tracker.exec(lambda: 1, destination="source", scope="changes")
    seen = []
    loop = MartLoop(engine, tracker, destination, ["source"], seen.append)
    try:
        assert loop.run_if_pending()
        assert seen[0].next_watermark == started
    finally:
        tracker._abandon(active)
        loop.close()


def test_plain_handler_consumes_explicit_rebuild(engine, tracker):
    destination = (None, f"plain_{uuid.uuid4().hex[:8]}")
    seen = []
    loop = MartLoop(engine, tracker, destination, ["source"], seen.append)
    try:
        request_rebuild(engine, destination, schema=tracker.schema)
        assert loop.run_if_pending()
        assert seen[0].full_rebuild
        assert not rebuild_requested(engine, destination, schema=tracker.schema)
        assert not loop.run_if_pending()
    finally:
        loop.close()


def test_close_waits_for_pass_without_releasing_lease_early(engine, tracker):
    destination = (None, f"close_{uuid.uuid4().hex[:8]}")
    started, finish, closed = threading.Event(), threading.Event(), threading.Event()
    errors = []
    tracker.exec(lambda: 1, destination="source")

    def process_changes(ctx):
        started.set()
        assert finish.wait(timeout=10)

    loop = MartLoop(engine, tracker, destination, ["source"], process_changes)

    def run():
        try:
            loop.run_if_pending()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    closer = threading.Thread(target=lambda: (loop.close(), closed.set()), daemon=True)
    worker.start()
    assert started.wait(timeout=5)
    closer.start()
    try:
        assert not closed.wait(timeout=0.1)
        assert loop._lease.still_mine(loop.name)
    finally:
        finish.set()
        worker.join(timeout=10)
        closer.join(timeout=10)
    assert closed.is_set() and errors == []
    assert not loop.run_if_pending()

_TRACKING_ENGINES = [name for name in engines if name not in ("clickhouse", "duckdb")]


def _schema(engine):
    return None if engine.dialect.name == "sqlite" else "target"


def _dbmerge(engine, **kwargs):
    return dbmerge(engine, schema=_schema(engine), **kwargs)


@pytest.fixture(params=_TRACKING_ENGINES)
def engine(request):
    eng = make_engine(request.param)
    drop_table_if_exists(eng, "orders", schema=_schema(eng))
    drop_table_if_exists(eng, "mart", schema=_schema(eng))
    return eng


@pytest.fixture()
def tracker(engine):
    tr = MergeTracker(engine, table_name=f"tracking_{uuid.uuid4().hex[:8]}",
                      schema=_schema(engine), heartbeat_period=0.05, heartbeat_ttl=1.0)
    yield tr
    tr.close()


def _rows(name="a"):
    return [{"id": 1, "name": name}, {"id": 2, "name": name}]


def test_mart_runs_when_source_changed(engine, tracker):
    seen = []

    def process_changes(ctx):
        seen.append(ctx)
        with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                     data=[{"id": 1}], key=["id"]) as merge:
            ctx.tracker.exec_merge(merge)

    mart = MartLoop(engine, tracker, destination=(_schema(engine), "mart"),
                    sources=[(_schema(engine), "orders")], process_changes=process_changes)

    assert mart.run_if_pending() is False          # no source change yet
    assert seen == []

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True           # first run: full rebuild
    assert len(seen) == 1 and seen[0].full_rebuild is True

    assert mart.run_if_pending() is False          # nothing new since
    assert len(seen) == 1

    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec_merge(merge)
    # A subsequent no-op must not hide the change before the mart observes it.
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        record, _ = tracker.exec_merge(merge)
    assert not record.changed
    time.sleep(1.1)
    assert mart.run_if_pending() is True           # a real change: incremental
    assert len(seen) == 2 and seen[1].full_rebuild is False

    mart.close()


def test_mart_lease_exclusivity(engine, tracker):
    lease1 = Lease(engine, tracker.table, "owner1")
    lease2 = Lease(engine, tracker.table, "owner2")
    ran = []

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)

    m1 = MartLoop(engine, tracker, (_schema(engine), "mart"), [(_schema(engine), "orders")],
                  lambda ctx: ran.append("m1"), lease=lease1)
    m2 = MartLoop(engine, tracker, (_schema(engine), "mart"), [(_schema(engine), "orders")],
                  lambda ctx: ran.append("m2"), lease=lease2)

    assert m1.run_if_pending() is True
    assert m2.run_if_pending() is False
    assert ran == ["m1"]

    m1.close()
    m2.close()
    lease1.close()
    lease2.close()


class RecordingHandler(MartHandler):
    destination = "mart"
    sources = ["orders"]

    def __init__(self, engine, *, tracker):
        super().__init__(engine, tracker=tracker, destination=(_schema(engine), "mart"),
                         sources=[(_schema(engine), "orders")])
        self.setup_calls = 0
        self.process_changes_calls = 0
        self.rebuild_calls = 0

    def setup(self, ctx):
        self.setup_calls += 1

    def process_changes(self, ctx):
        self.process_changes_calls += 1
        with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                     data=[{"id": 1}], key=["id"]) as merge:
            ctx.tracker.exec_merge(merge)

    def rebuild(self, ctx):
        self.rebuild_calls += 1
        self.process_changes(ctx)


def test_handler_setup_and_rebuild(engine, tracker):
    h = RecordingHandler(engine, tracker=tracker)
    mart = h.loop()

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    time.sleep(1.1)

    assert mart.run_if_pending() is True
    assert (h.setup_calls, h.rebuild_calls, h.process_changes_calls) == (1, 1, 1)

    assert mart.run_if_pending() is False                 # no new change
    assert (h.setup_calls, h.rebuild_calls, h.process_changes_calls) == (1, 1, 1)

    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec_merge(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True                  # incremental: process_changes, not rebuild
    assert h.setup_calls == 1
    assert h.rebuild_calls == 1
    assert h.process_changes_calls == 2
    mart.close()


@pytest.mark.parametrize("kwargs", [
    {}, {"destination": "mart"}, {"sources": ["orders"]},
    {"destination": "", "sources": ["orders"]},
    {"destination": "mart", "sources": []},
    {"destination": "mart", "sources": "orders"},
    {"destination": ("invalid",), "sources": ["orders"]},
])
def test_handler_requires_destination_and_sources(engine, tracker, kwargs):
    with pytest.raises(IncorrectParameter):
        MartHandler(engine, tracker=tracker, **kwargs)


def test_handler_requires_engine():
    with pytest.raises(TypeError):
        MartHandler()
    with pytest.raises(IncorrectParameter):
        MartHandler(None, tracker=None, destination="mart", sources=["orders"])


def test_sources_are_isolated_per_instance(engine, tracker):
    first = RecordingHandler(engine, tracker=tracker)
    second = RecordingHandler(engine, tracker=tracker)
    assert first.sources == second.sources == frozenset({(_schema(engine), "orders")})
    assert first.destination == (_schema(engine), "mart")
    assert not hasattr(first, "name")


def test_default_lease_is_destination_scoped(engine, tracker):
    first = MartLoop(engine, tracker, ("a", "mart"), ["orders"], lambda ctx: None)
    second = MartLoop(engine, tracker, ("a", "mart"), ["orders"], lambda ctx: None)
    other = MartLoop(engine, tracker, ("b", "mart"), ["orders"], lambda ctx: None)
    try:
        assert first._lease.acquire(first.name)
        assert not second._lease.acquire(second.name)
        assert other._lease.acquire(other.name)
    finally:
        first.close()
        second.close()
        other.close()


def test_handler_constructor_no_lease(engine, tracker):
    from sqlalchemy import select, true

    seen = []

    class M(MartHandler):
        destination = 'without_lease'
        sources = ['source_without_lease']

        def process_changes(self, ctx):
            with ctx.engine.connect() as conn:
                assert conn.scalar(select(ctx.lease_is_mine))
            assert str(ctx.lease_is_mine) == str(true())
            seen.append(ctx.full_rebuild)

    handler = M(engine, tracker=tracker, with_lease=False)
    loop = handler.loop()
    try:
        assert not hasattr(handler, 'scope')
        assert not hasattr(loop, 'scope')
        assert loop._lease is None
        tracker.exec(lambda: 1, destination='source_without_lease', scope='changes')
        assert loop.run_if_pending()
        assert seen == [True]
        assert not loop.run_if_pending()
        if engine.dialect.name in ('sqlite', 'mysql', 'mariadb', 'oracle'):
            time.sleep(1.1)  # These tracking clocks/columns have second precision.
        tracker.exec(lambda: 1, destination='source_without_lease', scope='changes')
        assert loop.run_if_pending()
        assert seen == [True, False]
    finally:
        loop.close()


def test_operation_scopes_follow_callbacks_and_restore_on_failure(engine, tracker):
    records = []

    class M(MartHandler):
        destination = f'auto_scope_{uuid.uuid4().hex[:8]}'
        sources = ['auto_scope_source']

        def process_changes(self, ctx):
            record, _ = ctx.tracker.exec(lambda: 1, destination='auto_scope_output', scope='ignored')
            records.append(record.scope)

        def rebuild(self, ctx):
            record, _ = ctx.tracker.exec(lambda: 1, destination='auto_scope_output')
            records.append(record.scope)
            self.process_changes(ctx)  # Direct helper call remains part of this rebuild.
            if engine.dialect.name in ('sqlite', 'mysql', 'mariadb', 'oracle'):
                time.sleep(1.1)
            ctx.tracker.exec(lambda: 1, destination='auto_scope_source')
            yield 'first'
            record, _ = ctx.tracker.exec(lambda: 1, destination='auto_scope_output')
            records.append(record.scope)
            raise RuntimeError('scope failure')

    loop = M(engine, tracker=tracker).loop()
    try:
        request_rebuild(tracker.engine, loop.destination, schema=tracker.schema)
        with pytest.raises(RuntimeError, match='scope failure'):
            loop.run_if_pending()
        assert records == ['rebuild', 'rebuild', 'changes', 'rebuild']
        record, _ = tracker.exec(lambda: 1, destination='outside', scope='custom')
        assert record.scope == 'custom'
        assert tracker._mart_scope.get() is None
    finally:
        loop.close()


def test_rebuild_and_changes_use_separate_leases(engine, tracker):
    names = []

    class M(MartHandler):
        destination = 'operation_leases'
        sources = ['operation_source']

        def process_changes(self, ctx):
            names.append(loop.name)
            assert loop._lease.still_mine(loop.name)

        def rebuild(self, ctx):
            names.append(loop.name)
            if engine.dialect.name in ('sqlite', 'mysql', 'mariadb', 'oracle'):
                time.sleep(1.1)
            tracker.exec(lambda: 1, destination='operation_source', scope='changes')
            yield 'block'

    handler = M(engine, tracker=tracker)
    loop = handler.loop()
    try:
        tracker.exec(lambda: 1, destination='operation_source', scope='changes')
        assert loop.run_if_pending()
        # An explicit incremental pass uses a different key than the rebuild.
        if engine.dialect.name in ('sqlite', 'mysql', 'mariadb', 'oracle'):
            time.sleep(1.1)
        tracker.exec(lambda: 1, destination='operation_source', scope='changes')
        assert loop.run_if_pending()
        assert names[0].endswith(':rebuild')
        assert names[1].endswith(':changes')  # Increment between rebuild blocks.
        assert names[-1].endswith(':changes')
        assert names[0] != names[-1]
        assert not loop._lease.still_mine(names[0])
        assert not loop._lease.still_mine(names[-1])
    finally:
        loop.close()


def test_handler_rejects_invalid_lease_configuration(engine, tracker):
    with pytest.raises(IncorrectParameter, match='with_lease'):
        MartHandler(engine, tracker=tracker, destination='mart', sources=['source'], with_lease='yes')
    with pytest.raises(TypeError, match='scope'):
        MartHandler(engine, tracker=tracker, destination='mart', sources=['source'], scope=' ')
    lease = Lease(engine, tracker.table, 'invalid_combination')
    try:
        with pytest.raises(IncorrectParameter, match='with_lease=False'):
            MartLoop(engine, tracker, 'mart', ['source'], lambda ctx: None,
                     lease=lease, with_lease=False)
    finally:
        lease.close()


def test_sources_in_multiple_schemas(engine, tracker):
    from datetime import datetime, timedelta
    from unittest.mock import patch

    loop = MartLoop(engine, tracker, "mart", [(None, "orders"), ("other", "items")],
                    lambda ctx: None)
    now = datetime(2026, 1, 1)
    try:
        with patch.object(tracker, "safe_boundary", side_effect=lambda names, schema: (
                now if schema == "" else now - timedelta(seconds=10))) as boundary:
            assert loop._safe_boundary() == now - timedelta(seconds=10)
            assert {call.kwargs["schema"] for call in boundary.call_args_list} == {"", "other"}
        with patch.object(tracker, "is_changed_since", side_effect=lambda names, since, schema: (
                schema == "other")):
            assert loop._sources_changed(now)
    finally:
        loop.close()


def test_rebuild_state_uses_tracking_engine(engine, tracker, tmp_path):
    from sqlalchemy import create_engine, inspect

    destination_engine = create_engine(f"sqlite:///{tmp_path / 'destination.sqlite'}")
    destination = (None, f"logical_export_{uuid.uuid4().hex[:8]}")
    loop = MartLoop(destination_engine, tracker, destination, ["orders"], lambda ctx: None)
    try:
        request_rebuild(tracker.engine, destination, schema=tracker.schema)
        assert rebuild_requested(tracker.engine, destination, schema=tracker.schema)
        assert loop._rebuild_requested()
        assert loop._lease.acquire(loop.name)
        loop._write_rebuild_cursor("block-1")
        assert loop._rebuild_cursor() == "block-1"
        loop._observed_request = loop._request_token()
        loop._clear_rebuild_request()
        assert not rebuild_requested(tracker.engine, destination, schema=tracker.schema)
        assert not inspect(destination_engine).has_table("dbmerge_marts")
    finally:
        loop.close()
        destination_engine.dispose()


def test_stop_requested_before_run_is_not_lost(engine, tracker):
    from unittest.mock import patch

    loop = RecordingHandler(engine, tracker=tracker).loop()
    loop.request_stop()
    with patch.object(loop, "run_if_pending") as run:
        loop.run_forever()
        run.assert_not_called()


def test_handler_uses_caller_tracker(engine, tracker):
    from unittest.mock import patch

    handler = RecordingHandler(engine, tracker=tracker)
    with patch('dbmerge.mart.MergeTracker') as create, \
            patch('dbmerge.mart.MartLoop.run_forever'), patch.object(tracker, 'close') as close:
        handler.run_forever(poll_interval=0.5)
        create.assert_not_called()
        close.assert_not_called()
    assert handler.tracker is tracker
    loop = handler.loop()
    try:
        assert loop._lease.period == tracker.heartbeat_period
        assert loop._lease.ttl == tracker.heartbeat_ttl
    finally:
        loop.close()


def test_shared_tracker_parallel_marts_keep_scopes_and_ownership(engine, tracker):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from datetime import datetime
    from unittest.mock import patch

    barrier = Barrier(2)
    seen = []

    class M(MartHandler):
        sources = ['shared_source']

        def process_changes(self, ctx):
            barrier.wait(timeout=10)
            record, _ = ctx.tracker.exec(lambda: 1, destination=self.destination)
            seen.append(record.scope)

        def rebuild(self, ctx):
            barrier.wait(timeout=10)
            record, _ = ctx.tracker.exec(lambda: 1, destination=self.destination)
            seen.append(record.scope)
            yield 'all'

    rebuild = M(engine, tracker=tracker, destination=f'parallel_rebuild_{uuid.uuid4().hex[:8]}')
    changes = M(engine, tracker=tracker, destination=f'parallel_changes_{uuid.uuid4().hex[:8]}')
    tracker.mark_watermark(changes.destination[1], datetime(2000, 1, 1))
    tracker.exec(lambda: 1, destination='shared_source')
    with patch.object(tracker, 'close', wraps=tracker.close) as close:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(rebuild.run_once, rebuild=True), executor.submit(changes.run_once)]
            assert all(future.result(timeout=15) for future in futures)
        close.assert_not_called()
    assert sorted(seen) == ['changes', 'rebuild']
    assert tracker._mart_scope.get() is None
    assert tracker.exec(lambda: 1, destination='after_parallel')[0].scope is None


def test_handler_requires_open_tracker(engine, tracker):
    with pytest.raises(TypeError, match='tracker'):
        MartHandler(engine, destination='mart', sources=['orders'])
    for value in (None, object()):
        with pytest.raises(IncorrectParameter, match='tracker='):
            MartHandler(engine, tracker=value, destination='mart', sources=['orders'])
    tracker.close()
    with pytest.raises(IncorrectParameter, match='open tracker'):
        RecordingHandler(engine, tracker=tracker)


def test_handler_run_methods(engine, tracker):
    from unittest.mock import patch

    handler = RecordingHandler(engine, tracker=tracker)
    with patch.object(handler, '_run', return_value=True) as run:
        handler.run_forever(poll_interval=0.5)
        run.assert_called_once_with(0.5, continuous=True)
        run.reset_mock()
        assert handler.run_once(rebuild=True)
        run.assert_called_once_with(continuous=False, rebuild=True)


def test_handler_keeps_tracker_when_loop_creation_fails(engine, tracker):
    from unittest.mock import patch

    handler = RecordingHandler(engine, tracker=tracker)
    with patch.object(handler, 'loop', side_effect=RuntimeError('cannot create loop')), \
            patch.object(tracker, 'close') as close:
        with pytest.raises(RuntimeError, match='cannot create loop'):
            handler.run_once()
        close.assert_not_called()


@pytest.mark.parametrize('separate_tracking', [False, True])
def test_handler_run_once_rebuild_without_source_changes(engine, tracker, separate_tracking, tmp_path):
    from sqlalchemy import create_engine

    seen = []

    class RebuildMart(MartHandler):
        destination = f"manual_{uuid.uuid4().hex[:8]}"
        sources = ['unchanged_source']

        def rebuild(self, ctx):
            assert ctx.full_rebuild
            assert ctx.engine is engine
            seen.append('first')
            yield 'first'
            seen.append('second')
            yield 'second'

    tracking_engine = create_engine(f"sqlite:///{tmp_path / 'rebuild.sqlite'}") if separate_tracking else engine
    tracking_schema = None if separate_tracking else tracker.schema
    tracking = MergeTracker(tracking_engine, schema=tracking_schema)
    try:
        mart = RebuildMart(engine, tracker=tracking, with_lease=False)
        assert mart.run_once(rebuild=True)
        assert seen == ['first', 'second']
        assert not rebuild_requested(tracking_engine, mart.destination, schema=tracking_schema)
        assert not mart.run_once()
    finally:
        tracking.close()
        if separate_tracking:
            tracking_engine.dispose()


def test_handler_rebuild_busy_lease_keeps_request(engine, tracker):
    from unittest.mock import patch

    handler = RecordingHandler(engine, tracker=tracker)
    loop = handler.loop()
    name = loop._lease_prefix + ':rebuild'
    try:
        assert loop._lease.acquire(name)
        with patch('dbmerge.mart.MergeTracker', return_value=tracker):
            assert handler.run_once(rebuild=True) is False
        assert rebuild_requested(tracker.engine, handler.destination, schema=tracker.schema)
    finally:
        loop._lease.release(name)
        loop.close()


@pytest.mark.parametrize("example", ["orders_by_customer", "orders_by_customer_clickhouse", "rebuild_mart"])
def test_example_entry_point_one_pass(example, tmp_path, monkeypatch):
    import runpy
    from pathlib import Path
    from unittest.mock import patch

    from sqlalchemy import create_engine

    path = Path(__file__).resolve().parents[1] / "examples" / f"{example}.py"
    monkeypatch.syspath_prepend(str(path.parent))
    destination_engine = create_engine(f"sqlite:///{tmp_path / 'destination.sqlite'}")
    example_engines = [destination_engine]
    if example.endswith("clickhouse"):
        example_engines.append(create_engine(f"sqlite:///{tmp_path / 'tracking.sqlite'}"))
    try:
        with patch("sqlalchemy.create_engine", side_effect=example_engines) as create, \
                patch.object(MartLoop, "run_if_pending", autospec=True, return_value=False) as run:
            runpy.run_path(str(path), run_name="__main__")
            assert create.call_count == len(example_engines)
            run.assert_called_once()
            assert run.call_args.args[0].engine is destination_engine
            if example == 'rebuild_mart':
                assert rebuild_requested(destination_engine, ('target', 'orders_by_customer'))
    finally:
        for engine in example_engines:
            engine.dispose()


@pytest.mark.parametrize("outcome", [False, True, RuntimeError("debug failure")])
def test_handler_single_pass_cleanup(engine, tracker, outcome):
    from unittest.mock import patch
    from dbmerge import mart as mart_module

    handler = RecordingHandler(engine, tracker=tracker)
    loop = handler.loop()
    with patch("dbmerge.mart.MergeTracker", return_value=tracker), \
            patch.object(handler, "loop", return_value=loop), \
            patch.object(loop, "run_if_pending", side_effect=(
                outcome if isinstance(outcome, Exception) else None), return_value=outcome) as run, \
            patch.object(loop, "run_forever") as forever, \
            patch.object(loop, "close", wraps=loop.close) as close_loop, \
            patch.object(tracker, "close", wraps=tracker.close) as close_tracker:
        if isinstance(outcome, Exception):
            with pytest.raises(RuntimeError, match="debug failure"):
                handler.run_once()
        else:
            assert handler.run_once() is outcome
        run.assert_called_once()
        forever.assert_not_called()
        close_loop.assert_called_once()
        close_tracker.assert_not_called()
        assert loop not in mart_module._running_loops
        assert not loop._lease.still_mine(loop.name)


def test_handler_min_interval_throttles(engine, tracker):
    h = RecordingHandler(engine, tracker=tracker)
    h.min_interval = 10.0
    mart = h.loop()

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True

    # A new change arrives, but min_interval has not elapsed → throttled.
    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec_merge(merge)
    assert mart.run_if_pending() is False
    assert h.process_changes_calls == 1
    mart.close()



def test_process_changes_api_has_no_handle_alias(engine, tracker):
    from inspect import signature

    assert "process_changes" in signature(MartLoop).parameters
    assert "handle" not in signature(MartLoop).parameters
    assert not hasattr(MartHandler, "handle")
    calls = []
    loop = MartLoop(engine, tracker, destination="renamed", sources=["source"],
                    process_changes=calls.append)
    try:
        tracker.exec(lambda: 1, destination="source")
        assert loop.run_if_pending()
        assert calls[-1].full_rebuild
    finally:
        loop.close()


def test_handler_and_request_stop(engine, tracker):
    class M(MartHandler):
        destination = "mart"
        sources = ["orders"]

        def process_changes(self, ctx):
            with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                         data=[{"id": 1}], key=["id"]) as merge:
                ctx.tracker.exec_merge(merge)

    thread = threading.Thread(target=M(engine, tracker=tracker).run_forever,
                              kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    time.sleep(0.2)          # let the handler register its loop before stopping it
    request_stop()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_rebuild_blocks_seal_and_resume(engine, tracker):
    seen = []

    class M(MartHandler):
        destination = "mart"
        sources = ["orders"]
        crash = True

        def process_changes(self, ctx):
            seen.append(("process_changes", ctx.rebuild_from))
            with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                         data=[{"id": 1}], key=["id"]) as merge:
                ctx.tracker.exec_merge(merge)

        def rebuild(self, ctx):
            seen.append(("rebuild", ctx.rebuild_from))
            if not ctx.rebuild_from:
                with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                             data=[{"id": 1}], key=["id"]) as merge:
                    merge.exec()
                yield "b1"
                if self.crash:
                    self.crash = False
                    raise RuntimeError("simulated crash")
            with _dbmerge(ctx.engine, table_name=ctx.destination[1],
                         data=[{"id": 2}], key=["id"]) as merge:
                merge.exec()
            yield "b2"

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec_merge(merge)

    mart = M(engine, tracker=tracker, destination=(_schema(engine), "mart"),
             sources=[(_schema(engine), "orders")]).loop(poll_interval=0.05)
    with pytest.raises(RuntimeError):
        mart.run_if_pending()

    # The watermark was sealed and the cursor survived the crash.
    assert tracker.watermark("mart") is None  # Failed rebuild cannot publish consumer progress.

    # Resume: the already-done "b1" block is skipped, then the rebuild finishes.
    seen.clear()
    assert mart.run_if_pending() is True
    assert ("rebuild", "b1") in seen

    # A finished rebuild leaves nothing pending.
    assert mart.run_if_pending() is False
    mart.close()
