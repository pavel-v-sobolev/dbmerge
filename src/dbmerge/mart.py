"""A mart loop: re-run a derived table when its sources change.

A mart is a table built from one or more source tables. ``MartLoop`` owns the whole "when to run"
machinery and leaves only the calculation to a ``process_changes`` callable:

* it recovers abandoned merges (``recover_abandoned``);
* it claims the destination through a lease to coordinate competing processes;
* it reads its watermark (the boundary of its last successful run) and the safe upper boundary of
  its sources;
* it runs ``process_changes`` only when the sources really changed since the watermark.

The decision is made from durable last-change timestamps, not from an ephemeral signal.
A later no-op does not erase an earlier change. ``process_changes`` must be
idempotent — it may run again. The loop saves progress only after the whole handler succeeds,
with a lease check in the progress transaction.

``run_mart`` is the convenience entry point for a process that runs several marts: it wraps a handler
into a ``MartLoop`` with a lease and runs it until :func:`request_stop` is called.
"""

from __future__ import annotations

import threading
import time
import hashlib
import json
import uuid
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator

from sqlalchemy import (Boolean, Column, DateTime, MetaData, String, Table, and_, exists, func, inspect,
                        case, false, insert, or_, schema, select, text, update)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, IntegrityError

from .dbmerge import IncorrectParameter, logger
from .lease import Lease
from .tracking import MergeTracker

# The first run reads from the beginning of time rather than from a missing watermark.
EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)

# Per-mart state: a rebuild request flag and the cursor of an in-progress block rebuild.
MARTS_TABLE = "dbmerge_marts"


def _marts_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        MARTS_TABLE, metadata,
        Column("table_name", String(255), primary_key=True),
        Column("schema", String(255), primary_key=True),
        Column("full_rebuild_is_required", Boolean, nullable=False, server_default=false()),
        Column("rebuild_cursor", String(255), nullable=True),
        Column("rebuild_request", String(36), nullable=True),
        Column("rebuild_boundary", DateTime(timezone=True), nullable=True),
        schema=schema_name,
    )


def _ensure_marts_table(engine: Engine, schema_name: str | None) -> Table:
    table = _marts_table(MetaData(), schema_name)
    inspector = inspect(engine)
    if inspector.has_table(table.name, table.schema):
        columns = {column["name"] for column in inspector.get_columns(table.name, schema=table.schema)}
        compiler = engine.dialect.ddl_compiler(engine.dialect, None)
        for name in ("rebuild_request", "rebuild_boundary"):
            if name in columns:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {compiler.preparer.format_table(table)} "
                                      f"ADD {compiler.get_column_specification(table.c[name])}"))
            except DBAPIError:
                if name not in {column["name"] for column in inspect(engine).get_columns(
                        table.name, schema=table.schema)}:
                    raise
        return table
    if schema_name is not None and engine.dialect.name != "sqlite":
        with engine.connect() as conn:
            if not engine.dialect.has_schema(conn, schema_name):
                conn.execute(schema.CreateSchema(schema_name))
                conn.commit()
    table.create(engine, checkfirst=False)
    return table


def _split_name(name) -> tuple[str | None, str]:
    """Normalize a table reference to ``(schema, table)``.

    Accepts a ``(schema, table)`` tuple, or a bare ``"table"`` / ``"schema.table"`` string.
    """
    if isinstance(name, tuple):
        if len(name) != 2:
            raise IncorrectParameter("A reference must be a (schema, identifier) tuple")
        schema, table = name
    elif isinstance(name, str) and "." in name:
        schema, _, table = name.rpartition(".")
    else:
        schema, table = None, name
    if (schema is not None and not isinstance(schema, str)) or not isinstance(table, str) or not table.strip():
        raise IncorrectParameter("A reference requires a non-empty identifier and an optional string schema")
    return (schema or None, table)


def _sources(sources) -> frozenset[tuple[str | None, str]]:
    if sources is None or isinstance(sources, str):
        raise IncorrectParameter("sources is required and must be a non-empty iterable of references")
    try:
        result = frozenset(_split_name(source) for source in sources)
    except TypeError as exc:
        raise IncorrectParameter("sources must be an iterable of references") from exc
    if not result:
        raise IncorrectParameter("sources must not be empty")
    return result


def _marts_row_where(table, schema: str | None, table_name: str):
    # Oracle treats '' as NULL, which cannot be part of this composite primary key.
    # Accept the old empty-string representation as well as the nonempty sentinel.
    schema_condition = (table.c.schema == schema if schema else
                        or_(table.c.schema == " ", table.c.schema == "", table.c.schema.is_(None)))
    return and_(table.c.table_name == table_name, schema_condition)


@dataclass(frozen=True)
class MartContext:
    """What a mart handler receives. Data is not here: the handler does its own SELECT."""

    engine: Engine
    tracker: MergeTracker
    destination: tuple[str | None, str]          # logical destination identity, not necessarily a table
    sources: frozenset[tuple[str | None, str]]    # (schema, table) of each source
    watermark: datetime        # where to read changes from (EPOCH on the first run)
    next_watermark: datetime   # the watermark to record on success (the sources' safe upper boundary)
    full_rebuild: bool         # True on the first run / after reset
    rebuild_from: str | None = None  # last completed rebuild block, None when not resuming
    lease_is_mine: Any = None  # SQL condition "the mart name is still ours"
    logger: Any = logger       # the dbmerge logger, for the handler's own messages


class MartLoop:
    """Polls the tracking table and runs a handler when a mart's sources changed."""

    def __init__(self, engine: Engine, tracker: MergeTracker, destination: str | tuple[str | None, str],
                 sources: Iterable[str], process_changes: Callable[[MartContext], None],
                 poll_interval: float = 2.0, lease: Lease | None = None,
                 setup: Callable[[MartContext], None] | None = None,
                 rebuild: Callable[[MartContext], None] | None = None,
                 min_interval: float = 0.0, schema: str | None = None,
                 scope: str | None = None, enabled: bool = True):
        if not isinstance(engine, Engine):
            raise IncorrectParameter("MartLoop requires a SQLAlchemy Engine")
        if poll_interval <= 0 or min_interval < 0:
            raise IncorrectParameter("poll_interval must be positive and min_interval non-negative")
        if scope is not None and (not isinstance(scope, str) or not scope.strip()):
            raise IncorrectParameter("scope must be a non-empty string or None")
        self.scope = scope
        self.enabled = enabled
        self._pass_lock = threading.RLock()
        self._closed = False
        self._pass_thread = None
        self.engine = engine
        self.tracker = tracker
        self.destination = _split_name(destination)
        self.sources = _sources(sources)
        # A bounded, unambiguous lease key derived solely from the destination identity.
        self.name = "mart:" + hashlib.sha256(json.dumps(self.destination).encode()).hexdigest()[:48]
        self.process_changes = process_changes
        self.poll_interval = poll_interval
        # The schema where the dbmerge_marts table lives (a service detail); the business
        # schemas of the mart and each source live in the tuples above.
        self.schema = tracker.schema if schema is None else schema
        self.setup = setup
        self.rebuild = rebuild
        self.min_interval = min_interval

        # Split once for the tracking reads.
        self._mart_schema, self._mart_name = self.destination
        self._source_groups: dict[str, list[str]] = {}
        for source_schema, source_name in self.sources:
            self._source_groups.setdefault(source_schema or "", []).append(source_name)

        self._owns_lease = lease is None
        self._lease = lease if lease is not None else Lease(
            tracker.engine, tracker.table, f"mart:{uuid.uuid4().hex}",
            heartbeat_engine=tracker.heartbeat_engine)
        self._lease.ensure(self.name)

        self._stop_signal = threading.Event()
        if _stop_requested.is_set():
            self._stop_signal.set()
        _all_loops.add(self)
        self._setup_done = False
        self._next_allowed = 0.0
        self._marts_table: Table | None = None

    def run_forever(self) -> None:
        """Blocking poll loop. Stops on request_stop() and then releases the lease."""
        stop = self._stop_signal
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
        """Wait for an active pass before releasing its resources."""
        self.request_stop()
        if self._pass_thread == threading.get_ident():
            raise IncorrectParameter("Use request_stop(), not close(), inside a mart handler")
        with self._pass_lock:
            if self._closed:
                return
            self._closed = True
            self._lease.release(self.name)
            if self._owns_lease:
                self._lease.close()
            _all_loops.discard(self)

    def run_if_pending(self) -> bool:
        """One exclusive pass. Disabled/stopped consumers keep their watermark for catch-up."""
        with self._pass_lock:
            if self._closed or self._stop_requested() or not self.enabled:
                return False
            if time.monotonic() < self._next_allowed or not self._lease.acquire(self.name):
                return False
            self._pass_thread = threading.get_ident()
            try:
                return self._run_pass()
            finally:
                try:
                    self._lease.release(self.name)
                finally:
                    self._pass_thread = None

    def _run_pass(self) -> bool:
        self.tracker.recover_abandoned()

        cursor = self._rebuild_cursor()
        rebuild_requested = self._rebuild_requested()
        self._observed_request = self._request_token()
        explicit_rebuild = rebuild_requested or cursor is not None

        stored_watermark = self.tracker.watermark(self._mart_name, schema=self._mart_schema or "")
        full_rebuild = stored_watermark is None or explicit_rebuild
        watermark = stored_watermark if stored_watermark is not None else EPOCH

        next_watermark = self._safe_boundary()
        if not explicit_rebuild:
            if not full_rebuild and next_watermark <= watermark:
                # A merge that started before our watermark is still running: nothing is settled yet.
                return False
            if not self._sources_changed(watermark):
                return False

        context = MartContext(
            engine=self.engine, tracker=self.tracker,
            destination=self.destination, sources=self.sources,
            watermark=watermark, next_watermark=next_watermark, full_rebuild=full_rebuild,
            lease_is_mine=self._lease_is_mine(),
            rebuild_from=cursor,
        )

        if self.setup is not None and not self._setup_done:
            self.setup(context)
            self._setup_done = True

        if full_rebuild:
            self._run_rebuild(context, stored_watermark)
        else:
            self.process_changes(context)
            self._commit_watermark(context.next_watermark)

        self._next_allowed = time.monotonic() + self.min_interval
        return True

    def _stop_requested(self) -> bool:
        return self._stop_signal is not None and self._stop_signal.is_set()

    def _get_marts_table(self):
        if self._marts_table is None:
            self._marts_table = _ensure_marts_table(self.tracker.engine, self.schema)
        return self._marts_table

    def _ensure_marts_row(self) -> None:
        table = self._get_marts_table()
        _ensure_state_row(self.tracker.engine, table, self._mart_schema, self._mart_name)

    def _rebuild_cursor(self):
        table = self._get_marts_table()
        with self.tracker.engine.connect() as conn:
            cursor = conn.scalar(select(table.c.rebuild_cursor)
                                 .where(_marts_row_where(table, self._mart_schema, self._mart_name)))
        return "" if cursor == " " and self.tracker.engine.dialect.name == "oracle" else cursor

    def _write_rebuild_cursor(self, cursor) -> None:
        self._ensure_marts_row()
        table = self._marts_table
        with self.tracker.engine.begin() as conn:
            self._guard_progress(conn)
            conn.execute(update(table)
                         .where(_marts_row_where(table, self._mart_schema, self._mart_name))
                         .values(rebuild_cursor=cursor))

    def _rebuild_requested(self) -> bool:
        table = self._get_marts_table()
        with self.tracker.engine.connect() as conn:
            return bool(conn.scalar(select(table.c.full_rebuild_is_required)
                                    .where(_marts_row_where(table, self._mart_schema, self._mart_name))))

    def _clear_rebuild_request(self) -> None:
        self._ensure_marts_row()
        table = self._marts_table
        with self.tracker.engine.begin() as conn:
            conn.execute(update(table)
                         .where(_marts_row_where(table, self._mart_schema, self._mart_name))
                         .where(table.c.rebuild_request == getattr(self, "_observed_request", None))
                         .values(full_rebuild_is_required=False))

    def _run_rebuild(self, context: MartContext, watermark: datetime | None) -> None:
        """Drive a resumable, block-by-block rebuild; apply increments between blocks."""
        cursor = context.rebuild_from
        if cursor is None:  # An empty cursor is already an active rebuild, not a new request.
            cursor = ""
            self._ensure_marts_row()
            table = self._get_marts_table()
            with self.tracker.engine.begin() as conn:
                self._guard_progress(conn)
                conn.execute(update(table).where(_marts_row_where(
                    table, self._mart_schema, self._mart_name))
                    .values(rebuild_boundary=context.next_watermark,
                            rebuild_cursor=" " if self.tracker.engine.dialect.name == "oracle" else "",
                            full_rebuild_is_required=case(
                                (table.c.rebuild_request == self._observed_request, False),
                                else_=table.c.full_rebuild_is_required)))

        started = time.monotonic()
        blocks = self.rebuild(context) if self.rebuild is not None else self.process_changes(context)
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

        self._commit_watermark(self._rebuild_boundary() or context.next_watermark)
        self._write_rebuild_cursor(None)
        logger.info("Mart %s: rebuild finished in %.1fs", self.name, time.monotonic() - started)

    def _run_increment(self) -> None:
        """Apply changes that arrived since the mart's watermark (between rebuild blocks)."""
        watermark = self.tracker.watermark(self._mart_name, schema=self._mart_schema or "")
        boundary = self._rebuild_boundary()
        if boundary is not None:
            watermark = max(watermark, boundary) if watermark is not None else boundary
        if watermark is None:
            watermark = EPOCH
        next_watermark = self._safe_boundary()
        if next_watermark <= watermark:
            return
        if not self._sources_changed(watermark):
            return
        context = MartContext(
            engine=self.engine, tracker=self.tracker,
            destination=self.destination, sources=self.sources,
            watermark=watermark, next_watermark=next_watermark, full_rebuild=False,
            lease_is_mine=self._lease_is_mine(),
        )
        self.process_changes(context)
        self._commit_watermark(context.next_watermark)

    def _request_token(self):
        table = self._get_marts_table()
        with self.tracker.engine.connect() as conn:
            return conn.scalar(select(table.c.rebuild_request).where(
                _marts_row_where(table, self._mart_schema, self._mart_name)))

    def _rebuild_boundary(self):
        table = self._get_marts_table()
        with self.tracker.engine.connect() as conn:
            return conn.scalar(select(table.c.rebuild_boundary).where(
                _marts_row_where(table, self._mart_schema, self._mart_name)))

    def _commit_watermark(self, value):
        self.tracker.mark_watermark(self._mart_name, value, schema=self._mart_schema,
                                   lease=self._lease, lease_name=self.name)

    def _guard_progress(self, conn):
        from .tracking import _coerce_datetime, _now_expr
        t = self.tracker.table
        now = _coerce_datetime(conn.scalar(select(_now_expr(self.tracker.engine))))
        result = conn.execute(update(t).where(
            t.c.table_name == self._lease.key(self.name), t.c.owner == self._lease.owner,
            t.c.heartbeat_at >= now - timedelta(seconds=self._lease.ttl))
            .values(heartbeat_at=now))
        if not result.rowcount:
            raise RuntimeError("Lease lost before committing rebuild progress")

    def _safe_boundary(self) -> datetime:
        return min(self.tracker.safe_boundary(names, schema=schema)
                   for schema, names in self._source_groups.items())

    def _sources_changed(self, watermark: datetime) -> bool:
        kwargs = {} if self.scope is None else {"scope": self.scope}
        return any(self.tracker.is_changed_since(names, since=watermark, schema=schema, **kwargs)
                   for schema, names in self._source_groups.items())

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

    Subclass and set the required ``destination`` and ``sources``, then implement ``process_changes`` (and
    optionally ``setup`` for one-time DDL and ``rebuild`` for a full rebuild; the default rebuild is
    ``process_changes`` with ``full_rebuild=True``). ``loop()`` builds a MartLoop; ``run_forever`` runs it.
    """

    destination: str | tuple[str | None, str]
    sources: Iterable[str | tuple[str | None, str]]
    min_interval: float = 0.0
    scope: str | None = None
    enabled: bool = True

    def __init__(self, engine: Engine, *, destination=None, sources=None):
        if not isinstance(engine, Engine):
            raise IncorrectParameter("MartHandler requires a SQLAlchemy Engine")
        self.engine = engine
        self.destination = _split_name(
            destination if destination is not None else getattr(self, "destination", None))
        self.sources = _sources(sources if sources is not None else getattr(self, "sources", None))

    def setup(self, ctx: MartContext) -> None:
        """One-time preparation before the first run (e.g. create the mart table)."""

    def process_changes(self, ctx: MartContext) -> None:
        """Perform committed, idempotent work; the loop saves progress after successful return."""
        raise NotImplementedError(f"{type(self).__name__}.process_changes(ctx) is not implemented")

    def rebuild(self, ctx: MartContext) -> Iterator[str]:
        """A full rebuild, block by block: commit a block, then yield its label.

        The default is a single block: it runs process_changes() and yields "all". Subclasses that rebuild
        in chunks yield one label per committed block; MartLoop saves the label so an interrupted
        rebuild resumes from the next block.
        """
        self.process_changes(ctx)
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

    def loop(self, tracker: MergeTracker, *, poll_interval: float = 2.0,
             lease: Lease | None = None, schema: str | None = None) -> MartLoop:
        return MartLoop(self.engine, tracker, self.destination, self.sources,
                        self.process_changes, poll_interval=poll_interval, lease=lease,
                        setup=self.setup, rebuild=self.rebuild,
                        min_interval=self.min_interval, schema=schema,
                        scope=self.scope, enabled=self.enabled)

    def run_forever(self, tracker: MergeTracker | None = None, **kwargs) -> None:
        run_mart(self, tracker=tracker, **kwargs)


# Backward-compatible alias; prefer MartHandler in new code.
Handler = MartHandler


def as_handler(obj, engine: Engine | None = None) -> MartHandler:
    """Normalize a handler declared as a MartHandler instance, a module or a callable.

    A module or a callable is read for the same attributes a MartHandler declares; a callable
    without a separate ``process_changes`` attribute is the process_changes itself.
    """
    if isinstance(obj, MartHandler):
        if engine is not None and engine is not obj.engine:
            raise IncorrectParameter("engine differs from the handler's constructor engine")
        return obj
    handler = MartHandler(engine if engine is not None else getattr(obj, "engine", None),
                          destination=getattr(obj, "destination", None),
                          sources=getattr(obj, "sources", None))
    for attr in ("setup", "rebuild", "min_interval", "scope", "enabled"):
        if hasattr(obj, attr):
            setattr(handler, attr, getattr(obj, attr))
    if hasattr(obj, "process_changes"):
        handler.process_changes = obj.process_changes
    elif callable(obj):
        handler.process_changes = obj
    else:
        raise IncorrectParameter("A mart requires a callable process_changes(ctx)")
    return handler


_running_loops: list["MartLoop"] = []
_all_loops = weakref.WeakSet()
_stop_requested = threading.Event()


def run_mart(mart, engine: Engine | None = None, tracker: MergeTracker | None = None, *,
             poll_interval: float = 2.0, schema: str | None = None,
             lease: Lease | None = None, run_forever: bool = True) -> bool | None:
    """Poll a mart, or perform one pending pass with ``run_forever=False``.

    ``mart`` is a ``MartHandler`` instance, a module or a callable (see :func:`as_handler`). The
    ``MergeTracker`` is created automatically when not given and closed on exit. Each loop gets
    a destination-derived lease by default. ``engine`` is only needed for modules/callables.
    A single pass returns whether work ran and propagates errors to the caller; continuous mode
    runs until :func:`request_stop` and logs pass errors. Both modes close their owned resources.
    """
    handler = as_handler(mart, engine)
    owns_tracker = tracker is None
    if tracker is None:
        tracker = MergeTracker(handler.engine, schema=schema)
    try:
        loop = handler.loop(tracker, poll_interval=poll_interval, lease=lease, schema=schema)
        _running_loops.append(loop)
        try:
            if run_forever:
                loop.run_forever()
            else:
                return loop.run_if_pending()
        finally:
            try:
                loop.close()
            finally:
                _running_loops.remove(loop)
    finally:
        if owns_tracker:
            tracker.close()


def request_stop(*_args) -> None:
    """Ask every loop started by :func:`run_mart` to stop after its current pass.

    Accepts the two arguments a signal handler receives, so it can be passed directly to
    ``signal.signal``.
    """
    _stop_requested.set()
    for loop in list(_all_loops):
        loop.request_stop()


def reset_stop() -> None:
    """Allow new loops to start after a process-wide stop; existing stopped loops stay stopped."""
    _stop_requested.clear()


def rebuild_requested(engine: Engine, destination, *, schema: str | None = None) -> bool:
    """Whether a full rebuild was requested for a mart; its next pass will perform it."""
    table = _ensure_marts_table(engine, schema)
    destination_schema, table_name = _split_name(destination)
    with engine.connect() as conn:
        return bool(conn.scalar(select(table.c.full_rebuild_is_required)
                                .where(_marts_row_where(table, destination_schema, table_name))))


def request_rebuild(engine: Engine, destination, *, schema: str | None = None) -> None:
    """Request a full rebuild of a mart; the next pass of its MartLoop performs it."""
    table = _ensure_marts_table(engine, schema)
    destination_schema, table_name = _split_name(destination)
    _ensure_state_row(engine, table, destination_schema, table_name)
    with engine.begin() as conn:
        conn.execute(update(table).where(_marts_row_where(table, destination_schema, table_name))
                     .values(full_rebuild_is_required=True, rebuild_request=str(uuid.uuid4())))


def _ensure_state_row(engine, table, destination_schema, table_name):
    condition = _marts_row_where(table, destination_schema, table_name)
    with engine.connect() as conn:
        if conn.scalar(select(table.c.table_name).where(condition)) is not None:
            return
        try:
            conn.execute(insert(table).values(table_name=table_name, schema=destination_schema or " "))
            conn.commit()
        except IntegrityError:
            conn.rollback()
            if conn.scalar(select(table.c.table_name).where(condition)) is None:
                raise
