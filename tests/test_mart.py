"""Tests for dbmerge.mart: the MartLoop and its lease."""

import threading
import time
import uuid

import pytest

from dbmerge import (Lease, MartLoop, MartHandler, MergeTracker, dbmerge, as_handler,
                     drop_table_if_exists, request_stop, run_mart)

from conftest import engines, make_engine

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

    def handle(ctx):
        seen.append(ctx)
        with _dbmerge(ctx.engine, table_name=ctx.mart_table,
                     data=[{"id": 1}], key=["id"]) as merge:
            ctx.tracker.exec(merge, boundary=ctx.boundary)

    mart = MartLoop(engine, tracker, name="m", mart_table="mart",
                    sources=["orders"], handle=handle)

    assert mart.run_if_pending() is False          # no source change yet
    assert seen == []

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True           # first run: full rebuild
    assert len(seen) == 1 and seen[0].full_rebuild is True

    assert mart.run_if_pending() is False          # nothing new since
    assert len(seen) == 1

    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True           # a real change: incremental
    assert len(seen) == 2 and seen[1].full_rebuild is False

    mart.close()


def test_mart_lease_exclusivity(engine, tracker):
    lease1 = Lease(engine, tracker.table, "owner1")
    lease2 = Lease(engine, tracker.table, "owner2")
    ran = []

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec(merge)

    m1 = MartLoop(engine, tracker, "m", "mart", ["orders"],
                  lambda ctx: ran.append("m1"), lease=lease1)
    m2 = MartLoop(engine, tracker, "m", "mart", ["orders"],
                  lambda ctx: ran.append("m2"), lease=lease2)

    assert m1.run_if_pending() is True
    assert m2.run_if_pending() is False
    assert ran == ["m1"]

    m1.close()
    m2.close()


class RecordingHandler(MartHandler):
    name = "h"
    mart_table = "mart"
    sources = ["orders"]

    def __init__(self):
        self.setup_calls = 0
        self.handle_calls = 0
        self.rebuild_calls = 0

    def setup(self, ctx):
        self.setup_calls += 1

    def handle(self, ctx):
        self.handle_calls += 1
        with _dbmerge(ctx.engine, table_name=ctx.mart_table,
                     data=[{"id": 1}], key=["id"]) as merge:
            ctx.tracker.exec(merge, boundary=ctx.boundary)

    def rebuild(self, ctx):
        self.rebuild_calls += 1
        self.handle(ctx)


def test_handler_setup_and_rebuild(engine, tracker):
    h = RecordingHandler()
    mart = h.loop(engine, tracker)

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec(merge)
    time.sleep(1.1)

    assert mart.run_if_pending() is True
    assert (h.setup_calls, h.rebuild_calls, h.handle_calls) == (1, 1, 1)

    assert mart.run_if_pending() is False                 # no new change
    assert (h.setup_calls, h.rebuild_calls, h.handle_calls) == (1, 1, 1)

    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True                  # incremental: handle, not rebuild
    assert h.setup_calls == 1
    assert h.rebuild_calls == 1
    assert h.handle_calls == 2
    mart.close()


def test_handler_min_interval_throttles(engine, tracker):
    h = RecordingHandler()
    h.min_interval = 10.0
    mart = h.loop(engine, tracker)

    with _dbmerge(engine, table_name="orders", data=_rows(), key=["id"]) as merge:
        tracker.exec(merge)
    time.sleep(1.1)
    assert mart.run_if_pending() is True

    # A new change arrives, but min_interval has not elapsed → throttled.
    time.sleep(1.1)
    with _dbmerge(engine, table_name="orders", data=_rows("changed"), key=["id"]) as merge:
        tracker.exec(merge)
    assert mart.run_if_pending() is False
    assert h.handle_calls == 1
    mart.close()


def test_as_handler_normalizes():
    h = RecordingHandler()
    assert as_handler(h) is h

    class Mod:
        name = "m"
        mart_table = "mart"
        sources = ["orders"]

        def handle(self, ctx):
            pass

    norm = as_handler(Mod())
    assert norm.name == "m" and norm.sources == ["orders"] and callable(norm.handle)

    def fn(ctx):
        pass

    fn.name = "f"
    fn.mart_table = "mart"
    fn.sources = ["orders"]
    norm2 = as_handler(fn)
    assert norm2.name == "f" and norm2.handle is fn


def test_run_mart_and_request_stop(engine, tracker):
    class M(MartHandler):
        name = "m"
        mart_table = "mart"
        sources = ["orders"]

        def handle(self, ctx):
            with _dbmerge(ctx.engine, table_name=ctx.mart_table,
                         data=[{"id": 1}], key=["id"]) as merge:
                ctx.tracker.exec(merge, boundary=ctx.boundary)

    thread = threading.Thread(target=run_mart, args=(M(), engine, tracker),
                              kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    time.sleep(0.2)          # let run_mart register its loop before stopping it
    request_stop()
    thread.join(timeout=5)
    assert not thread.is_alive()

