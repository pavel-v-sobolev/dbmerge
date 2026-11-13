"""A mart loop: re-run a derived table when its sources change.

A mart is a table built from one or more source tables. ``MartLoop`` owns the whole "when to run"
machinery and leaves only the calculation to a ``handle`` callable:

* it recovers abandoned merges (``recover_abandoned``);
* it claims the mart name through a lease, so two processes never build the same mart;
* it reads its watermark (the boundary of its last successful run) and the safe upper boundary of
  its sources;
* it runs ``handle`` only when the sources really changed since the watermark.

The decision is made from timestamps, not from a signal: a change that was missed (a crash, a mart
that was switched off) is seen by the next pass through ``is_changed_since``. ``handle`` must therefore
be idempotent — it may run again. The handler records the new watermark itself by passing
``boundary=ctx.boundary`` to ``tracker.exec``.

``run_mart`` is the convenience entry point for a process that runs several marts: it wraps a handler
into a ``MartLoop`` with a lease and runs it until :func:`request_stop` is called.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from sqlalchemy import exists, func, or_, text
from sqlalchemy.engine import Engine

from .dbmerge import logger
from .lease import Lease
from .tracking import Database, MergeTracker

# The first run reads from the beginning of time rather than from a missing watermark.
EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class MartContext:
    """What a mart handler receives. Data is not here: the handler does its own SELECT."""

    engine: Engine
    tracker: MergeTracker
    name: str
    mart_table: str
    sources: frozenset[str]
    last_run_at: datetime      # where to read changes from
    boundary: datetime         # up to where it is safe to read
    full_rebuild: bool         # True on the first run / after reset
    lease_is_mine: Any = None  # SQL condition "the mart name is still ours"
    schema: str | None = None  # where the mart table lives (for setup/merge)
    logger: Any = logger       # the dbmerge logger, for the handler's own messages


class MartLoop:
    """Polls the tracking table and runs a handler when a mart's sources changed."""

    def __init__(self, engine: Engine, tracker: MergeTracker, name: str, mart_table: str,
                 sources: Iterable[str], handle: Callable[[MartContext], None],
                 poll_interval: float = 2.0, lease: Lease | None = None,
                 database: Database | None = None,
                 setup: Callable[[MartContext], None] | None = None,
                 rebuild: Callable[[MartContext], None] | None = None,
                 min_interval: float = 0.0, schema: str | None = None):
        self.engine = engine
        self.tracker = tracker
        self.name = name
        self.mart_table = mart_table
        self.sources = frozenset(sources)
        self.handle = handle
        self.poll_interval = poll_interval
        # Where the sources (and the mart) live. None reads across all databases.
        self.database = database
        self.schema = schema
        self.setup = setup
        self.rebuild = rebuild
        self.min_interval = min_interval

        self._lease = lease
        if lease is not None:
            lease.ensure(name)

        self._stop_signal: threading.Event | None = None
        self._setup_done = False
        self._next_allowed = 0.0

    def run_forever(self) -> None:
        """Blocking poll loop. Stops on request_stop() and then releases the lease."""
        stop = threading.Event()
        self._stop_signal = stop
        try:
            while not stop.is_set():
                try:
                    self.run_if_pending()
                except Exception:
                    logger.exception("Mart %s dispatch failed", self.name)
                stop.wait(self.poll_interval)
        finally:
            self.close()

    def request_stop(self) -> None:
        if self._stop_signal is not None:
            self._stop_signal.set()

    def close(self) -> None:
        """Release the name so a successor can take it without waiting out the lease TTL."""
        if self._lease is not None:
            self._lease.release(self.name)
            self._lease.close()

    def run_if_pending(self) -> bool:
        """One pass: run the handler if there is work, otherwise do nothing. Returns whether it ran."""
        if time.monotonic() < self._next_allowed:
            return False

        if self._lease is not None and not self._lease.acquire(self.name):
            return False

        self.tracker.recover_abandoned()

        watermark = self.tracker.watermark(self.mart_table, database=self.database)
        full_rebuild = watermark is None
        last_run_at = watermark if watermark is not None else EPOCH

        boundary = self.tracker.boundary(self.sources, database=self.database)
        if not full_rebuild and boundary <= last_run_at:
            # A merge that started before our watermark is still running: nothing is settled yet.
            return False

        if not self.tracker.is_changed_since(self.sources, since=last_run_at,
                                          database=self.database):
            return False

        context = MartContext(
            engine=self.engine, tracker=self.tracker, name=self.name,
            mart_table=self.mart_table, sources=self.sources,
            last_run_at=last_run_at, boundary=boundary, full_rebuild=full_rebuild,
            lease_is_mine=self._lease_is_mine(), schema=self.schema,
        )

        if self.setup is not None and not self._setup_done:
            self.setup(context)
            self._setup_done = True

        if full_rebuild and self.rebuild is not None:
            self.rebuild(context)
        else:
            self.handle(context)

        self._next_allowed = time.monotonic() + self.min_interval
        return True

    def _lease_is_mine(self):
        """A SQL condition "the mart name is still ours", for guards inside the handler's merge.

        The expression is meant for timezone-capable engines (PostgreSQL/CockroachDB), where
        ``now() - interval`` works.
        """
        if self._lease is None:
            return None
        t = self.tracker.table
        return exists().where(
            t.c.table_name == self._lease.key(self.name),
            t.c.owner == self._lease.owner,
            t.c.heartbeat_at >= func.now() - timedelta(seconds=self._lease.ttl))


class MartHandler:
    """Base class for a mart: declare sources and the work, and let MartLoop drive the rest.

    Subclass and set ``name``, ``mart_table`` and ``sources``, then implement ``handle`` (and
    optionally ``setup`` for one-time DDL and ``rebuild`` for a full rebuild; the default rebuild is
    ``handle`` with ``full_rebuild=True``). ``loop()`` builds a MartLoop; ``run_forever`` runs it.
    """

    name: str = ""
    mart_table: str = ""
    sources: list[str] = []
    min_interval: float = 0.0

    def setup(self, ctx: MartContext) -> None:
        """One-time preparation before the first run (e.g. create the mart table)."""

    def handle(self, ctx: MartContext) -> None:
        """The incremental work. Records the watermark via tracker.exec(..., boundary=ctx.boundary)."""
        raise NotImplementedError(f"{type(self).__name__}.handle(ctx) is not implemented")

    def rebuild(self, ctx: MartContext) -> None:
        """A full rebuild. By default the same as handle: ctx.full_rebuild is True."""
        self.handle(ctx)

    def changed_since(self, ctx: MartContext, *columns) -> Any:
        """An OR of ``column > ctx.last_run_at`` — rows of the mart changed since the watermark."""
        return or_(*[column > ctx.last_run_at for column in columns])

    def execute(self, ctx: MartContext, sql: str, **params) -> None:
        """Run a SQL statement in its own transaction."""
        with ctx.engine.begin() as conn:
            conn.execute(text(sql), params)

    def query(self, ctx: MartContext, sql: str, **params) -> list:
        """Run a SELECT and return all rows; the connection is released before returning."""
        with ctx.engine.connect() as conn:
            return conn.execute(text(sql), params).all()

    def loop(self, engine: Engine, tracker: MergeTracker, *, poll_interval: float = 2.0,
             lease: Lease | None = None, database: Database | None = None,
             schema: str | None = None) -> MartLoop:
        return MartLoop(engine, tracker, self.name, self.mart_table, self.sources,
                        self.handle, poll_interval=poll_interval, lease=lease,
                        database=database, setup=self.setup, rebuild=self.rebuild,
                        min_interval=self.min_interval, schema=schema)

    def run_forever(self, engine: Engine, tracker: MergeTracker, **kwargs) -> None:
        self.loop(engine, tracker, **kwargs).run_forever()


# Backward-compatible alias; prefer MartHandler in new code.
Handler = MartHandler


def as_handler(obj) -> MartHandler:
    """Normalize a handler declared as a MartHandler instance, a module or a callable.

    A module or a callable is read for the same attributes a MartHandler declares; a callable
    without a separate ``handle`` attribute is the handle itself.
    """
    if isinstance(obj, MartHandler):
        return obj
    handler = MartHandler()
    for attr in ("name", "mart_table", "sources", "setup", "rebuild", "min_interval"):
        if hasattr(obj, attr):
            setattr(handler, attr, getattr(obj, attr))
    if hasattr(obj, "handle"):
        handler.handle = obj.handle
    elif callable(obj):
        handler.handle = obj
    return handler


_running_loops: list["MartLoop"] = []


def run_mart(mart, engine: Engine, tracker: MergeTracker | None = None, *,
             poll_interval: float = 2.0, schema: str | None = None,
             database: Database | None = None, lease: Lease | None = None) -> None:
    """Run one mart until :func:`request_stop` is called.

    ``mart`` is a ``MartHandler`` instance, a module or a callable (see :func:`as_handler`). The
    ``MergeTracker`` is created automatically when not given. Pass a shared ``Lease`` to enforce
    one process per mart; without one the mart may run in two processes if this is started twice.
    """
    handler = as_handler(mart)
    if tracker is None:
        tracker = MergeTracker(engine, schema=schema)
    loop = handler.loop(engine, tracker, poll_interval=poll_interval, lease=lease,
                        schema=schema, database=database)
    _running_loops.append(loop)
    try:
        loop.run_forever()
    finally:
        _running_loops.remove(loop)


def request_stop(*_args) -> None:
    """Ask every loop started by :func:`run_mart` to stop after its current pass.

    Accepts the two arguments a signal handler receives, so it can be passed directly to
    ``signal.signal``.
    """
    for loop in list(_running_loops):
        loop.request_stop()
