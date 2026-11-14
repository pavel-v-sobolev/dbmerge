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
``next_watermark=ctx.next_watermark`` to ``tracker.exec``.

``run_mart`` is the convenience entry point for a process that runs several marts: it wraps a handler
into a ``MartLoop`` with a lease and runs it until :func:`request_stop` is called.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator

from sqlalchemy import (Boolean, Column, MetaData, String, Table, exists, func, inspect, insert,
                        or_, schema, select, text, update)
from sqlalchemy.engine import Engine

from .dbmerge import logger
from .lease import Lease
from .tracking import Database, MergeTracker

# The first run reads from the beginning of time rather than from a missing watermark.
EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)

# Per-mart state: a rebuild request flag and the cursor of an in-progress block rebuild.
MARTS_TABLE = "dbmerge_marts"


def _marts_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        MARTS_TABLE, metadata,
        Column("table_name", String(255), primary_key=True),
        Column("full_rebuild_is_required", Boolean, nullable=False, server_default=text("false")),
        Column("rebuild_cursor", String(255), nullable=True),
        schema=schema_name,
    )


def _ensure_marts_table(engine: Engine, schema_name: str | None) -> Table:
    table = _marts_table(MetaData(), schema_name)
    inspector = inspect(engine)
    if inspector.has_table(table.name, table.schema):
        return table
    if schema_name is not None and engine.dialect.name != "sqlite":
        with engine.connect() as conn:
            if not engine.dialect.has_schema(conn, schema_name):
                conn.execute(schema.CreateSchema(schema_name))
                conn.commit()
    table.create(engine, checkfirst=False)
    return table


@dataclass(frozen=True)
class MartContext:
    """What a mart handler receives. Data is not here: the handler does its own SELECT."""

    engine: Engine
    tracker: MergeTracker
    name: str
    mart_table: str
    sources: frozenset[str]
    watermark: datetime        # where to read changes from (EPOCH on the first run)
    next_watermark: datetime   # the watermark to record on success (the sources' safe upper boundary)
    full_rebuild: bool         # True on the first run / after reset
    rebuild_from: str | None = None  # last completed rebuild block, None when not resuming
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
        self._marts_table: Table | None = None

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

        cursor = self._rebuild_cursor()
        rebuild_requested = self._rebuild_requested()
        explicit_rebuild = rebuild_requested or cursor is not None

        stored_watermark = self.tracker.watermark(self.mart_table, database=self.database)
        full_rebuild = stored_watermark is None or explicit_rebuild
        watermark = stored_watermark if stored_watermark is not None else EPOCH

        next_watermark = self.tracker.boundary(self.sources, database=self.database)
        if not explicit_rebuild:
            if not full_rebuild and next_watermark <= watermark:
                # A merge that started before our watermark is still running: nothing is settled yet.
                return False
            if not self.tracker.is_changed_since(self.sources, since=watermark,
                                                 database=self.database):
                return False

        context = MartContext(
            engine=self.engine, tracker=self.tracker, name=self.name,
            mart_table=self.mart_table, sources=self.sources,
            watermark=watermark, next_watermark=next_watermark, full_rebuild=full_rebuild,
            lease_is_mine=self._lease_is_mine(), schema=self.schema,
            rebuild_from=cursor,
        )

        if self.setup is not None and not self._setup_done:
            self.setup(context)
            self._setup_done = True

        if full_rebuild and self.rebuild is not None:
            self._run_rebuild(context, stored_watermark)
        else:
            self.handle(context)

        self._next_allowed = time.monotonic() + self.min_interval
        return True

    def _stop_requested(self) -> bool:
        return self._stop_signal is not None and self._stop_signal.is_set()

    def _get_marts_table(self):
        if self._marts_table is None:
            self._marts_table = _ensure_marts_table(self.engine, self.schema)
        return self._marts_table

    def _ensure_marts_row(self) -> None:
        table = self._get_marts_table()
        with self.engine.begin() as conn:
            if conn.scalar(select(table.c.table_name).where(table.c.table_name == self.mart_table)) is None:
                conn.execute(insert(table).values(table_name=self.mart_table))

    def _rebuild_cursor(self):
        table = self._get_marts_table()
        with self.engine.connect() as conn:
            return conn.scalar(select(table.c.rebuild_cursor)
                               .where(table.c.table_name == self.mart_table))

    def _write_rebuild_cursor(self, cursor) -> None:
        self._ensure_marts_row()
        table = self._marts_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.table_name == self.mart_table)
                         .values(rebuild_cursor=cursor))

    def _rebuild_requested(self) -> bool:
        table = self._get_marts_table()
        with self.engine.connect() as conn:
            return bool(conn.scalar(select(table.c.full_rebuild_is_required)
                                    .where(table.c.table_name == self.mart_table)))

    def _clear_rebuild_request(self) -> None:
        self._ensure_marts_row()
        table = self._marts_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.table_name == self.mart_table)
                         .values(full_rebuild_is_required=False))

    def _run_rebuild(self, context: MartContext, watermark: datetime | None) -> None:
        """Drive a resumable, block-by-block rebuild; apply increments between blocks."""
        cursor = context.rebuild_from
        if not cursor:  # None or "" — a fresh rebuild, or restarted before any block was done
            cursor = ""
            self._write_rebuild_cursor(cursor)
            self._clear_rebuild_request()
            if watermark is None:
                # First rebuild: seed the watermark so increments between blocks start from the
                # current boundary instead of re-reading the whole history.
                self.tracker.mark_watermark(self.mart_table, context.next_watermark,
                                            database=self.database)

        started = time.monotonic()
        blocks = self.rebuild(context)
        if blocks is None:
            blocks = ()
        try:
            for label in blocks:
                if self._lease is not None and not self._lease.still_mine(self.name):
                    logger.error("Mart %s lost its lease during the rebuild — stopping at block %s, "
                                 "the new owner will continue", self.name, label)
                    return
                self._write_rebuild_cursor(str(label))
                logger.info("Mart %s: rebuild block %s done", self.name, label)
                if self._stop_requested():
                    logger.info("Mart %s: rebuild paused at block %s, will resume on start",
                                self.name, label)
                    return
                self._run_increment()
        finally:
            close = getattr(blocks, "close", None)
            if close is not None:
                close()

        self._write_rebuild_cursor(None)
        logger.info("Mart %s: rebuild finished in %.1fs", self.name, time.monotonic() - started)

    def _run_increment(self) -> None:
        """Apply changes that arrived since the mart's watermark (between rebuild blocks)."""
        watermark = self.tracker.watermark(self.mart_table, database=self.database)
        if watermark is None:
            watermark = EPOCH
        next_watermark = self.tracker.boundary(self.sources, database=self.database)
        if next_watermark <= watermark:
            return
        if not self.tracker.is_changed_since(self.sources, since=watermark,
                                             database=self.database):
            return
        context = MartContext(
            engine=self.engine, tracker=self.tracker, name=self.name,
            mart_table=self.mart_table, sources=self.sources,
            watermark=watermark, next_watermark=next_watermark, full_rebuild=False,
            lease_is_mine=self._lease_is_mine(), schema=self.schema,
        )
        self.handle(context)

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
        """The incremental work. Records the watermark via tracker.exec(..., next_watermark=ctx.next_watermark)."""
        raise NotImplementedError(f"{type(self).__name__}.handle(ctx) is not implemented")

    def rebuild(self, ctx: MartContext) -> Iterator[str]:
        """A full rebuild, block by block: commit a block, then yield its label.

        The default is a single block: it runs handle() and yields "all". Subclasses that rebuild
        in chunks yield one label per committed block; MartLoop saves the label so an interrupted
        rebuild resumes from the next block.
        """
        self.handle(ctx)
        yield "all"

    def changed_since(self, ctx: MartContext, *columns) -> Any:
        """An OR of ``column > ctx.watermark`` — rows of the mart changed since the watermark."""
        return or_(*[column > ctx.watermark for column in columns])

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


def rebuild_requested(engine: Engine, schema: str | None, table_name: str) -> bool:
    """Whether a full rebuild was requested for a mart; its next pass will perform it."""
    table = _ensure_marts_table(engine, schema)
    with engine.connect() as conn:
        return bool(conn.scalar(select(table.c.full_rebuild_is_required)
                                .where(table.c.table_name == table_name)))


def request_rebuild(engine: Engine, schema: str | None, table_name: str) -> None:
    """Request a full rebuild of a mart; the next pass of its MartLoop performs it."""
    table = _ensure_marts_table(engine, schema)
    with engine.begin() as conn:
        conn.execute(update(table).where(table.c.table_name == table_name)
                     .values(full_rebuild_is_required=True))
        if conn.scalar(select(table.c.table_name).where(table.c.table_name == table_name)) is None:
            conn.execute(insert(table).values(table_name=table_name, full_rebuild_is_required=True))
