"""
Change tracking for dbmerge merges.

The module answers the two questions a downstream consumer (a data mart built on top of merged
tables) asks about a table:

* "Has this table really changed since a given moment?" — the ``changed`` flag, recorded when a
  merge finishes, distinguishes a merge that wrote rows from a no-op merge that found nothing to
  change.
* "Is a merge running right now, so the table may be half-written?" — a merge records a row before
  it starts and marks it finished afterwards. While it runs, a background thread refreshes the row's
  heartbeat, and ``safe_boundary`` returns the earliest start of a live merge.

Operations live in a journal (one row per merge). For every table name the journal keeps the last
finished row and any number of unfinished rows: a finished merge replaces earlier finished rows for
the same table, so history does not grow. A merge that died mid-way is left unfinished with a stale
heartbeat; it stops holding the boundary once its heartbeat expires, and ``recover_abandoned`` marks
it changed so marts that missed it wake up. Dedicated change-state rows in the same journal retain a
monotonic last-change timestamp per destination and scope. Completion and change publication
commit together; a later no-op cannot erase an earlier change signal.

The tracking table is created on first use, so a project that never opts into tracking gets no extra
tables. The feature is built around timezone-aware timestamps and is meant for engines that support
them (PostgreSQL/CockroachDB first); on other engines recording works, and the heartbeat/boundary
semantics follow the engine's datetime handling.
"""

from __future__ import annotations

from contextvars import ContextVar

import os
import hashlib
import json
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from numbers import Number
from typing import Any, Callable, Iterable

from sqlalchemy import (Boolean, Column, DateTime, Engine, MetaData, String, Table,
                         case, cast, create_engine, delete, func, inspect, insert, literal, or_, schema,
                         select, text, update)
from sqlalchemy.exc import DBAPIError, IntegrityError

from .dbmerge import IncorrectParameter, logger

# The default name of the tracking table. Overridable; created lazily on first use.
TRACKING_TABLE = "dbmerge_tracking"

# How often a running merge refreshes the heartbeat of its row, and how long a heartbeat may stay
# stale before the merge is considered abandoned. The TTL is deliberately several times the period:
# a single delayed refresh must not look like a death, but a dead process must not freeze the
# boundary forever (see boundary/recover_abandoned).
HEARTBEAT_PERIOD = 20.0
HEARTBEAT_TTL = 90.0
# How long close() waits for the heartbeat thread to finish its current statement.
HEARTBEAT_JOIN_TIMEOUT = 30.0

# Engines that can not be the tracking store: no real transactions (ClickHouse) or no cross-process
# shared store (DuckDB). A merge into such an engine is tracked through a transactional one
# (PostgreSQL/CockroachDB), passed as the tracker's engine.
TRACKING_UNSUPPORTED_DIALECTS = ("clickhousedb", "duckdb")


def _heartbeat_engine(engine: Engine) -> Engine:
    """Independent pool, preserving the original pool's connection creator/configuration."""
    if engine.dialect.name == "sqlite" and engine.url.database in (None, "", ":memory:"):
        raise IncorrectParameter("Background tracking requires file-backed SQLite, not :memory:")
    return create_engine(engine.url, pool=engine.pool.recreate())


def _instance_owner(name: str) -> str:
    """An owner string that distinguishes processes and restarts, for diagnostics only."""
    return f"{name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _coerce_datetime(value: Any) -> datetime:
    # sqlite has no datetime type: CURRENT_TIMESTAMP comes back as text there. Everywhere else it is
    # a datetime. Either form compares consistently within its own engine.
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _now_expr(engine: Engine):
    # The database clock, typed to match the tracking columns. MSSQL's CURRENT_TIMESTAMP is a naive
    # DATETIME while DateTime(timezone=True) is a DATETIMEOFFSET; sysdatetimeoffset() returns the
    # aware value the column stores, so "now" stays comparable with stored values.
    if engine.dialect.name == "mssql":
        return func.sysdatetimeoffset()
    return func.now()


@dataclass(frozen=True)
class MergeRecord:
    """What the tracking table recorded about one merge."""

    table_name: str
    schema: str
    started_at: datetime
    finished_at: datetime | None
    changed: bool
    watermark: datetime | None
    scope: str | None = None


def _matches_column(column, value):
    # Oracle stores an empty string as NULL, so an empty value matches both a NULL and an empty
    # string. The COALESCE(column, '') trick does not work there: '' itself is bound as NULL.
    if value == "":
        return or_(column.is_(None), column == "")
    return column == value


def _as_names(table_names) -> list[str]:
    """A single table name or an iterable of names, normalized to a list.

    A bare string is one name, not an iterable of characters, so it is wrapped.
    """
    if isinstance(table_names, str):
        return [table_names]
    return list(table_names)


def _operation_rows(table):
    # NULL is a pre-row_kind operation (or legacy lease, excluded by its reserved name).
    return (or_(table.c.row_kind.is_(None), table.c.row_kind == "operation")
            & ~table.c.table_name.startswith("lease:", autoescape=True))


def _tracking_table(metadata: MetaData, name: str, schema: str | None) -> Table:
    return Table(
        name, metadata,
        Column("id", String(64), primary_key=True),
        Column("row_kind", String(16), nullable=True),
        Column("changed_at", DateTime(timezone=True), nullable=True),
        # NULL only for lease rows ("free"); a merge row always names its process owner.
        Column("owner", String(255), nullable=True),
        Column("table_name", String(255), nullable=False),
        # The schema of the merged table ("" when there is none). Together with table_name it
        # is the identity; two databases with a conflicting name need separate tracking tables.
        Column("schema", String(255), nullable=True),
        # Operation category, independent of the destination's table/schema identity.
        Column("scope", String(255), nullable=True),
        # All marks are by the database clock: the one clock the writer and the reader share.
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("finished_at", DateTime(timezone=True), nullable=True),
        Column("heartbeat_at", DateTime(timezone=True), nullable=False),
        # Whether the merge actually wrote rows (inserted + updated + deleted > 0). Set on finish.
        Column("changed", Boolean, nullable=True),
        # For a mart merge: the watermark recorded on success — the upper bound of the window it
        # processed, and where the next mart update starts from. A recovered row never has one.
        Column("watermark", DateTime(timezone=True), nullable=True),
        schema=schema,
    )

def _lease_name(schema: str, table_name: str, scope: str | None = None) -> str:
    """A stable per-(schema, table, scope) key; preserve the legacy unscoped key."""
    base = f"{schema}|{table_name}"
    if scope is None:
        return base
    return 'scoped:' + hashlib.sha256(json.dumps([schema, table_name, scope]).encode()).hexdigest()[:48]


class MergeTracker:
    """A database-backed journal of merges: run a merge, then ask when and whether a table changed.

    The tracker is used as a context manager so its heartbeat thread is stopped on exit, and the
    merge itself is the standard dbmerge context::

        with MergeTracker(engine) as tracker:
            with dbmerge(engine, table_name="orders", data=rows, key=["id"]) as merge:
                record, result = tracker.exec_merge(merge)

    ``exec`` inserts a row before the callable, runs it, and marks the row
    finished afterwards. A separate daemon thread keeps the row's heartbeat fresh while the merge
    runs, on its own connection, so a long merge is not mistaken for a dead one.
    """

    def __init__(self, engine: Engine, table_name: str = TRACKING_TABLE,
                 schema: str | None = None, owner: str | None = None,
                 heartbeat_period: float = HEARTBEAT_PERIOD,
                 heartbeat_ttl: float = HEARTBEAT_TTL,
                 heartbeat_engine: Engine | None = None):
        """Create a tracker writing its journal to a table of a transactional database.

        Args:
            engine: A SQLAlchemy ``Engine`` for the transactional database that holds the journal.
                ClickHouse and DuckDB are rejected — they have no cross-process transactional
                store; track merges into them by passing a PostgreSQL/CockroachDB engine here.
            table_name: The name of the journal table. It is created lazily on first use, so a
                project that never opts into tracking gets no extra table.
            schema: The schema (or database on MySQL/MariaDB) holding the journal table. Created if
                it does not exist; ignored on SQLite, which has no schemas.
            owner: A string identifying this process in the journal's ``owner`` column, for
                diagnostics only. Defaults to ``host:pid:random``.
            heartbeat_period: How often a running merge refreshes its row's heartbeat, in seconds.
            heartbeat_ttl: How long a heartbeat may stay stale before a merge is considered
                abandoned, in seconds.
        """
        if not isinstance(engine, Engine):
            raise IncorrectParameter(f"MergeTracker needs a SQLAlchemy Engine to write its journal to, "
                                     f"got {type(engine).__name__}.")
        if engine.dialect.name in TRACKING_UNSUPPORTED_DIALECTS:
            raise IncorrectParameter(f"MergeTracker can not journal on \"{engine.dialect.name}\": it has no "
                                     f"transactional shared store. Track through PostgreSQL/CockroachDB "
                                     f"and pass that engine here.")
        self.engine = engine
        self.owner = owner or _instance_owner("merge")
        self.heartbeat_period = heartbeat_period
        self.heartbeat_ttl = heartbeat_ttl

        if engine.dialect.name == "sqlite" and schema is not None:
            logger.warning('"sqlite" engine does not support schemas. Omitting tracking schema = "%s"',
                           schema)
            schema = None
        self.schema = schema

        self.metadata = MetaData()
        self.table = _tracking_table(self.metadata, table_name, schema)
        self._ensure_schema()
        self._ensure_table()
        self._backfill_changes()

        self._in_flight: set[str] = set()
        # RLock: _begin holds the lock while _ensure_heartbeat takes it again to check-and-create
        # the thread atomically.
        self._lock = threading.RLock()
        self._idle = threading.Condition(self._lock)
        self._active: dict[int, int] = {}
        self._closing = False
        self._close_lock = threading.Lock()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._lease = None
        self._owns_heartbeat_engine = heartbeat_engine is None
        self._mart_scope = ContextVar('dbmerge_mart_scope', default=None)
        self.heartbeat_engine = heartbeat_engine or _heartbeat_engine(engine)

    # ------------------------------------------------------------------ context manager

    def __enter__(self) -> "MergeTracker":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ tracking

    def exec(self, operation: Callable[[], Any], *, destination,
             scope: str | None = None,
             lease_options=None, lease_handle=None):
        """Track a committed zero-argument write; only False/numeric zero means no change.

        Returns (record, original_result). Consumer watermarks belong to MartLoop, not writes.
        New operations are rejected once close() starts; close waits for active operations.
        """
        with self._idle:
            if self._closing or self._closed.is_set():
                raise IncorrectParameter("Cannot execute an operation with a closing tracker")
            thread_id = threading.get_ident()
            self._active[thread_id] = self._active.get(thread_id, 0) + 1
        try:
            return self._exec(operation, destination=destination, scope=scope,
                              lease_options=lease_options, lease_handle=lease_handle)
        finally:
            with self._idle:
                self._active[thread_id] -= 1
                if not self._active[thread_id]:
                    del self._active[thread_id]
                self._idle.notify_all()

    def _exec(self, operation, *, destination, scope, lease_options, lease_handle):
        """Run a callable and journal the write to ``destination``.

        ``destination`` is a bare/schema-qualified string or a ``(schema, identifier)`` tuple.
        The callable receives no arguments and must commit its data before returning. Only an
        explicit ``False`` or numeric zero means no change; None and empty containers mean change.
        ``scope`` is an optional operation category, not a source table or destination identity.
        Returns ``(MergeRecord, original_result)`` or ``(None, None)`` if a requested lease is busy.
        Exceptions propagate and leave an unfinished operation for ``recover_abandoned``.
        """
        if not callable(operation):
            raise IncorrectParameter("exec() expects a callable; use exec_merge() for a dbmerge instance")
        if isinstance(destination, tuple) and len(destination) == 2:
            destination_schema, table_name = destination
        elif isinstance(destination, str):
            destination_schema, separator, table_name = destination.rpartition(".")
            if not separator:
                destination_schema, table_name = None, destination
        else:
            raise IncorrectParameter("destination must be a string or a (schema, identifier) tuple")
        if (destination_schema is not None and not isinstance(destination_schema, str)
                or not isinstance(table_name, str) or not table_name.strip()
                or len(table_name) > 255 or len(destination_schema or "") > 255):
            raise IncorrectParameter("destination requires a non-empty identifier and optional string schema (max 255 characters)")
        if table_name.startswith("lease:"):
            raise IncorrectParameter("destination identifiers starting with 'lease:' are reserved")
        # Mart callbacks own their operation category; do not leak it to other
        # threads or retain it after returning to standalone tracking calls.
        from .lease import validate_lease_arguments
        validate_lease_arguments(lease_options, lease_handle, self)
        scope = self._mart_scope.get() or scope
        if scope is not None and (not isinstance(scope, str) or not scope.strip() or len(scope) > 255):
            raise IncorrectParameter("scope must be None or a non-empty string of at most 255 characters")
        if self._closed.is_set():
            raise IncorrectParameter("Cannot execute an operation with a closed tracker")

        destination_schema = destination_schema or ""
        lease_name = None
        if lease_options is not None:
            lease_name = _lease_name(destination_schema, table_name, lease_options.scope)
            if not self._acquire_lease(lease_name, wait=lease_options.wait_lease,
                                       timeout=lease_options.wait_timeout,
                                       interval=lease_options.wait_interval):
                logger.info('Skipping write to "%s": its lease is held by another process', table_name)
                if self._mart_scope.get() is not None:
                    from .lease import LeaseBusy
                    raise LeaseBusy('Tracked mart write skipped because its lease is busy')
                return (None, None)
        try:
            if lease_name is not None:
                lease_handle = self._get_lease().handle(lease_name)
            if lease_handle is not None:
                lease_handle.check()
            row_id, started_at = self._begin(table_name, destination_schema, scope)
            try:
                result = operation()
                if lease_handle is not None:
                    lease_handle.check()
                changed = not (result is False or isinstance(result, Number) and result == 0)
                finished_at = self._finish(row_id, table_name, changed, None,
                                           destination_schema)
            except BaseException:
                self._abandon(row_id)
                raise
            return (MergeRecord(table_name=table_name, schema=destination_schema,
                                started_at=started_at, finished_at=finished_at,
                                changed=changed, watermark=None, scope=scope), result)
        finally:
            if lease_name is not None:
                self._release_lease(lease_name)

    def exec_merge(self, merge, *, scope: str | None = None,
                   lease_options=None, lease_handle=None, **exec_kwargs):
        """Run a dbmerge instance through :meth:`exec`, returning its original merge result.

        Destination is inferred from the merge; change detection uses its inserted, updated and
        deleted row counters, not the truth value of the returned mergeResult. Additional keyword
        arguments are forwarded to ``merge.exec``. There is no ``changed`` override.
        Bound merges execute without recursive journaling. Configure lease_options/lease_handle
        on the merge or adapter, not both. Omitted adapter arguments use merge configuration.
        """
        table_name = getattr(merge, "table_name", None)
        if not table_name or not callable(getattr(merge, "exec", None)):
            raise IncorrectParameter("exec_merge() expects a dbmerge instance with table_name and exec()")
        if getattr(merge, "merge_finished", False):
            raise IncorrectParameter(f'Merge of table "{table_name}" is already finished')
        bound_tracker = getattr(merge, 'tracker', None)
        if bound_tracker is not None and bound_tracker is not self:
            raise IncorrectParameter('Merge is configured with a different tracker')
        bound_options = getattr(merge, 'lease_options', None)
        bound_handle = getattr(merge, 'lease_handle', None)
        if lease_options is not None or lease_handle is not None:
            if bound_options is not None or bound_handle is not None:
                raise IncorrectParameter('Configure lease_options/lease_handle on merge or adapter, not both')
        else:
            lease_options, lease_handle = bound_options, bound_handle
        from .lease import validate_lease_arguments
        validate_lease_arguments(lease_options, lease_handle, self)
        if "next_watermark" in exec_kwargs:
            raise IncorrectParameter("Watermarks belong to MartLoop, not individual writes")
        if "changed" in exec_kwargs:
            raise IncorrectParameter("exec_merge() determines changes from merge counters; changed is not supported")
        merge_result = None

        def execute_merge():
            nonlocal merge_result
            execute = merge._exec if bound_tracker is not None else merge.exec
            active_handle = lease_handle
            if lease_options is not None:
                active_handle = self._get_lease().handle(_lease_name(
                    getattr(merge, 'schema', None) or '', table_name, lease_options.scope))
            if hasattr(merge, '_execution_tracker'):
                merge._execution_tracker = self
                merge._execution_lease_handle = active_handle
            try:
                merge_result = execute(**exec_kwargs)
            finally:
                if hasattr(merge, '_execution_tracker'):
                    merge._execution_tracker = None
                    merge._execution_lease_handle = None
            return (getattr(merge, "inserted_row_count", 0)
                    + getattr(merge, "updated_row_count", 0)
                    + getattr(merge, "deleted_row_count", 0)) > 0

        record, _ = self.exec(execute_merge, destination=(getattr(merge, "schema", None), table_name),
                              scope=scope, lease_options=lease_options, lease_handle=lease_handle)
        if bound_tracker is not None:
            merge.tracking_record = record
        return record, merge_result

    def _begin(self, table_name: str, schema: str, scope: str | None = None):
        """Insert the row before the merge and register it for heartbeat. Returns (id, started_at)."""
        self._ensure_change_row(table_name, schema, scope)
        row_id = str(uuid.uuid4())
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            conn.execute(insert(self.table).values(
                id=row_id, row_kind="operation", owner=self.owner, table_name=table_name,
                schema=schema, scope=scope,
                started_at=now, heartbeat_at=now))
            conn.commit()
        with self._lock:
            self._in_flight.add(row_id)
            self._ensure_heartbeat()
        return row_id, now

    def _finish(self, row_id: str, table_name: str, changed: bool,
                watermark: datetime | None, schema: str = "") -> datetime:
        # On SQL Server/MySQL pruning scans can deadlock with another destination's finish
        # even though the operation rows differ. Only retry this rolled-back journal
        # transaction: the user's write has already committed and must not run again.
        for attempt in range(5):
            try:
                return self._finish_transaction(row_id, table_name, changed, watermark, schema)
            except DBAPIError as exc:
                message = str(exc.orig).lower()
                deadlock = (self.engine.dialect.name == "mssql"
                            and ("deadlock victim" in message or "was deadlocked" in message)
                            or self.engine.dialect.name in ("mysql", "mariadb")
                            and "deadlock found when trying to get lock" in message)
                if not deadlock or attempt == 4:
                    raise
                logger.warning('Retrying journal completion after %s deadlock (attempt %s)',
                               self.engine.dialect.name, attempt + 1)
                time.sleep(0.05 * 2 ** attempt)

    def _finish_transaction(self, row_id: str, table_name: str, changed: bool,
                            watermark: datetime | None, schema: str = "") -> datetime:
        """Mark the row finished, replace earlier finished rows of the table, stop its heartbeat."""
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            row = conn.execute(select(t.c.schema, t.c.table_name, t.c.scope)
                               .where(t.c.id == row_id)).one()
            conn.execute(update(t).where(t.c.id == row_id)
                         .values(finished_at=now, changed=changed, watermark=watermark))
            if changed:
                self._publish_change(conn, row.table_name, row.schema, row.scope, now)
            # One finished row per table: the just-finished merge replaces every earlier finished
            # row. Unfinished rows are left alone.
            conn.execute(delete(t).where(t.c.table_name == table_name,
                                         _operation_rows(t),
                                         _matches_column(t.c.schema, schema),
                                         t.c.finished_at.is_not(None),
                                         t.c.id != row_id))
            conn.commit()
        with self._lock:
            self._in_flight.discard(row_id)
        return now

    @staticmethod
    def _change_key(table_name, schema, scope):
        identity = json.dumps([schema or "", table_name, scope], ensure_ascii=True)
        return "change:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:57]

    def _ensure_change_row(self, table_name, schema, scope):
        """Create a stable update target outside the operation completion transaction."""
        c = self.table
        key = self._change_key(table_name, schema, scope)
        with self.engine.connect() as conn:
            if conn.scalar(select(c.c.id).where(c.c.id == key)) is not None:
                return
            try:
                conn.execute(insert(c).values(id=key, row_kind="change", table_name=table_name,
                                              schema=schema or "", scope=scope,
                                              started_at=func.now(), heartbeat_at=func.now()))
                conn.commit()
            except IntegrityError:
                conn.rollback()
                if conn.scalar(select(c.c.id).where(c.c.id == key)) is None:
                    raise

    def _publish_change(self, conn, table_name, schema, scope, changed_at):
        """Monotonic, row-locked update, in the journal completion transaction.

        CASE is evaluated by the database after acquiring the update lock: competing finishes
        cannot replace a newer mark with an older timestamp, even when commits arrive out of order.
        """
        c = self.table
        # CASE arms need the column's SQL type even when a caller supplies a naive datetime.
        # CockroachDB does not implicitly unify TIMESTAMP and TIMESTAMPTZ here.
        stamp = literal(changed_at, type_=c.c.changed_at.type)
        if self.engine.dialect.name == "cockroachdb":
            stamp = cast(stamp, c.c.changed_at.type)
        conn.execute(update(c).where(c.c.id == self._change_key(table_name, schema, scope))
                     .values(changed_at=case(
                         (or_(c.c.changed_at.is_(None), c.c.changed_at < stamp), stamp),
                         else_=c.c.changed_at)))

    def _backfill_changes(self):
        """Preserve all change signals still present in a pre-upgrade journal."""
        t = self.table
        # Also tolerate the minimal legacy table used by the scope-migration check.
        columns = {column["name"] for column in inspect(self.engine).get_columns(
            t.name, schema=t.schema)}
        if not {"table_name", "schema", "scope", "changed", "finished_at"} <= columns:
            return
        with self.engine.connect() as conn:
            rows = conn.execute(select(t.c.table_name, t.c.schema, t.c.scope,
                                       func.max(t.c.finished_at).label("changed_at"))
                                .where(_operation_rows(t), t.c.changed == True,
                                       t.c.finished_at.is_not(None))
                                .group_by(t.c.table_name, t.c.schema, t.c.scope)).all()
        for row in rows:
            self._ensure_change_row(row.table_name, row.schema, row.scope)
            with self.engine.begin() as conn:
                self._publish_change(conn, row.table_name, row.schema, row.scope, row.changed_at)
        if "watermark" in columns:
            with self.engine.connect() as conn:
                progress = conn.execute(select(t.c.table_name, t.c.schema,
                    func.max(t.c.watermark).label("watermark"))
                    .where(_operation_rows(t), t.c.watermark.is_not(None))
                    .group_by(t.c.table_name, t.c.schema)).all()
            for row in progress:
                self.mark_watermark(row.table_name, row.watermark, schema=row.schema)

    def _abandon(self, row_id: str) -> None:
        """Stop refreshing the row after a failed merge. The row stays, finished_at stays NULL."""
        with self._lock:
            self._in_flight.discard(row_id)

    # ------------------------------------------------------------------ heartbeat

    def _ensure_heartbeat(self) -> None:
        with self._lock:
            if self._heartbeat_thread is not None or self._closed.is_set():
                return
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name=f"dbmerge-heartbeat:{self.owner}", daemon=True)
            self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._closed.is_set():
            try:
                self.heartbeat()
            except Exception:
                logger.exception("Merge heartbeat failed")
            self._closed.wait(self.heartbeat_period)
            with self._lock:
                if not self._in_flight:
                    self._heartbeat_thread = None
                    return

    def heartbeat(self) -> None:
        """Refresh the heartbeat of every merge currently being tracked by this process."""
        with self._lock:
            in_flight = list(self._in_flight)
        if not in_flight:
            return
        with self.heartbeat_engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id.in_(in_flight))
                         .values(heartbeat_at=func.now()))

    def close(self) -> None:
        """Drain active writes with heartbeats alive, then close only owned resources."""
        with self._idle:
            if threading.get_ident() in self._active:
                raise IncorrectParameter("Cannot close tracker from inside its active operation")
            self._closing = True
            while self._active:
                self._idle.wait()
        with self._close_lock:
            self._closed.set()
            thread = self._heartbeat_thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT)
                if thread.is_alive():
                    raise RuntimeError("Tracker heartbeat did not stop; heartbeat engine remains open")
            try:
                if self._lease is not None:
                    self._lease.close()
            finally:
                if self._owns_heartbeat_engine:
                    self.heartbeat_engine.dispose()

    # ------------------------------------------------------------------ reading

    def safe_boundary(self, table_names: str | Iterable[str],
                      schema: str | None = None) -> datetime:
        """The upper bound of safe reading: min(now, earliest start of a live merge on these tables.

        ``table_names`` is a single name or an iterable of names. An unfinished merge whose
        heartbeat is stale does not hold the boundary, so a dead process blocks readers for at
        most heartbeat_ttl. The time and the minimum come from one connection, so "now" for the
        cutoff and "now" for the bound are the same clock reading.
        """
        names = _as_names(table_names)
        if not names:
            return self._db_now()
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            cutoff = now - timedelta(seconds=self.heartbeat_ttl)
            conds = [_operation_rows(t), t.c.table_name.in_(names), t.c.finished_at.is_(None),
                     t.c.heartbeat_at > cutoff]
            if schema is not None:
                conds.append(_matches_column(t.c.schema, schema))
            earliest = conn.scalar(select(func.min(t.c.started_at)).where(*conds))
        return min(now, earliest) if earliest is not None else now

    def last_merge(self, table_name: str, schema: str | None = None) -> MergeRecord | None:
        """The last finished merge of a table, or None if it has none."""
        t = self.table
        conds = [_operation_rows(t), t.c.table_name == table_name, t.c.finished_at.is_not(None)]
        if schema is not None:
            conds.append(_matches_column(t.c.schema, schema))
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.table_name, t.c.schema, t.c.started_at, t.c.finished_at,
                       t.c.changed, t.c.watermark, t.c.scope)
                .where(*conds)
                .order_by(t.c.finished_at.desc())
                .limit(1)).first()
        if row is None:
            return None
        return MergeRecord(table_name=row.table_name, schema=row.schema,
                           started_at=row.started_at, finished_at=row.finished_at,
                           changed=bool(row.changed), watermark=row.watermark, scope=row.scope)

    def watermark(self, table_names: str | Iterable[str],
                  schema: str | None = None) -> datetime | None:
        """The watermark of the last successful merge over ``table_names`` — where the next update starts.

        ``table_names`` is a single name or an iterable of names (for a mart, its one table). Only
        successful finishes carry a watermark; a recovered (crashed) row has none, so a crash never
        advances the watermark. Returns None when there is no successful run yet.
        """
        names = _as_names(table_names)
        if not names:
            return None
        t = self.table
        conds = [t.c.table_name.in_(names), t.c.watermark.is_not(None),
                 or_(_operation_rows(t), t.c.row_kind == "watermark")]
        if schema is not None:
            conds.append(_matches_column(t.c.schema, schema))
        with self.engine.connect() as conn:
            return conn.scalar(select(func.max(t.c.watermark)).where(*conds))

    def is_changed_since(self, table_names: str | Iterable[str], since: datetime,
                         schema: str | None = None, *, scope: str | None = None) -> bool:
        """Whether any durable change mark is after ``since``, including before a later no-op.

        ``table_names`` is a single name or an iterable of names. ``scope=None`` reads all
        scopes; a string filters wakeup signals only, never the active-operation boundary.
        """
        names = _as_names(table_names)
        if not names:
            return False
        t = self.table
        conds = [t.c.row_kind == "change", t.c.table_name.in_(names), t.c.changed_at > since]
        if scope is not None:
            conds.append(t.c.scope == scope)
        if schema is not None:
            conds.append(_matches_column(t.c.schema, schema))
        with self.engine.connect() as conn:
            found = conn.scalar(select(t.c.id).where(*conds).limit(1))
        return found is not None

    def recover_abandoned(self) -> int:
        """Mark merges with an expired heartbeat as finished and changed, and return how many.

        A merge that died may have committed its data, so marts that missed it are woken
        unconditionally (their own query over the window is idempotent). Recovered rows carry no
        watermark, so a crashed mart never advances its own watermark. Their durable change marks
        survive subsequent operations even when the recovered journal row is pruned.
        """
        t = self.table
        with self.engine.connect() as conn:
            cutoff = _coerce_datetime(conn.scalar(select(_now_expr(self.engine)))) \
                - timedelta(seconds=self.heartbeat_ttl)
            ids = conn.execute(
                select(t.c.id, t.c.table_name, t.c.schema, t.c.scope)
                .where(_operation_rows(t), t.c.finished_at.is_(None), t.c.heartbeat_at <= cutoff,
                       ~t.c.table_name.startswith("lease:", autoescape=True))).all()
        recovered = 0
        for row in ids:
            self._ensure_change_row(row.table_name, row.schema, row.scope)
            with self.engine.begin() as conn:
                now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
                result = conn.execute(
                    update(t)
                    .where(t.c.id == row.id, t.c.finished_at.is_(None), t.c.heartbeat_at <= cutoff)
                    .values(finished_at=now, changed=True))
                if result.rowcount:
                    self._publish_change(conn, row.table_name, row.schema, row.scope, now)
                    conn.execute(delete(t).where(_operation_rows(t),
                        t.c.table_name == row.table_name, _matches_column(t.c.schema, row.schema or ""),
                        t.c.finished_at.is_not(None), t.c.id != row.id))
                    recovered += 1
        if recovered:
            logger.warning("Recovered %s abandoned merge(s) as changed", recovered)
        return recovered

    def reset(self, table_name: str, schema: str | None = None) -> int:
        """Drop the finished row(s) of a table, so its next mart update starts from the beginning."""
        t = self.table
        with self.engine.begin() as conn:
            conds = [or_(_operation_rows(t), t.c.row_kind == "watermark"),
                     t.c.table_name == table_name, t.c.finished_at.is_not(None)]
            if schema is not None:
                conds.append(_matches_column(t.c.schema, schema))
            result = conn.execute(delete(t).where(*conds))
            c = self.table
            change_conds = [c.c.row_kind == "change", c.c.table_name == table_name]
            if schema is not None:
                change_conds.append(_matches_column(c.c.schema, schema))
            # Keep stable rows: active operations may still need to publish into them.
            conn.execute(update(c).where(*change_conds).values(changed_at=None))
        return result.rowcount

    def mark_watermark(self, table_name: str, watermark: datetime,
                       schema: str | None = None, *, lease_handle=None) -> None:
        """Persist consumer progress independently of individual operation rows.

        When a lease is supplied, lock/check/renew its row in the same transaction. This guards
        progress publication, not arbitrary destination writes (which still require idempotence).
        """
        key = "watermark:" + self._change_key(table_name, schema, None)[7:61]
        t = self.table
        with self.engine.begin() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            if lease_handle is not None:
                lease_handle.validate(self)
                lease_handle.guard_progress(conn)
            previous = conn.scalar(select(t.c.watermark).where(t.c.id == key))
            if previous is not None:
                stamp = literal(watermark, type_=t.c.watermark.type)
                if self.engine.dialect.name == "cockroachdb":
                    stamp = cast(stamp, t.c.watermark.type)
                conn.execute(update(t).where(t.c.id == key).values(watermark=case(
                    (t.c.watermark < stamp, stamp), else_=t.c.watermark)))
            else:
                conn.execute(insert(t).values(id=key, row_kind="watermark", owner=self.owner,
                    table_name=table_name, schema=schema or "", started_at=now,
                    heartbeat_at=now, finished_at=now, changed=False, watermark=watermark))

    # ------------------------------------------------------------------ lease

    def _get_lease(self):
        """The lease manager behind lease_options, created lazily to avoid a circular import."""
        with self._lock:
            if self._lease is None:
                from .lease import Lease
                self._lease = Lease(self.engine, self.table, self.owner,
                                    period=self.heartbeat_period, ttl=self.heartbeat_ttl,
                                    heartbeat_engine=self.heartbeat_engine)
            return self._lease

    def _acquire_lease(self, name, *, wait, timeout, interval) -> bool:
        """Claim the per-table lease, optionally waiting for another holder to release it."""
        lease = self._get_lease()
        lease.ensure(name)
        if lease.acquire(name):
            return True
        if not wait:
            return False
        logger.info('Waiting for the lease of "%s" (held by %s); checking every %.1fs',
                    name, self._lease_owner(name) or "an unknown process", interval)
        started = time.monotonic()
        while True:
            next_check = time.monotonic() + interval
            if timeout is not None:
                next_check = min(next_check, started + timeout)
            while time.monotonic() < next_check:
                if self._closing or self._closed.wait(max(0, min(0.1, next_check - time.monotonic()))):
                    return False
            if self._closing:
                return False
            if lease.acquire(name):
                logger.info('Lease of "%s" acquired after %.1fs', name,
                            time.monotonic() - started)
                return True
            if timeout is not None and time.monotonic() - started >= timeout:
                logger.warning('Timed out waiting for the lease of "%s" after %.1fs',
                               name, time.monotonic() - started)
                return False

    def _lease_owner(self, name):
        """The current holder of a lease key, for diagnostics."""
        key = self._get_lease().key(name)
        with self.engine.connect() as conn:
            return conn.scalar(select(self.table.c.owner)
                               .where(self.table.c.table_name == key))

    def _release_lease(self, name) -> None:
        self._get_lease().release(name)

    # ------------------------------------------------------------------ internals

    def _ensure_schema(self) -> None:
        # The tracking table may live in a schema/database that does not exist yet (MySQL/MariaDB
        # require one). Create it the way dbmerge creates its own schemas.
        if self.schema is None or self.engine.dialect.name == "sqlite":
            return
        with self.engine.connect() as conn:
            exists = self.engine.dialect.has_schema(conn, self.schema)
            conn.commit()
            if not exists:
                conn.execute(schema.CreateSchema(self.schema))
                conn.commit()

    def _ensure_table(self) -> None:
        inspector = inspect(self.engine)
        if inspector.has_table(self.table.name, self.table.schema):
            # Nullable additions preserve existing operation and lease rows.
            columns = {column["name"] for column in inspector.get_columns(
                self.table.name, schema=self.table.schema)}
            for name in ("scope", "row_kind", "changed_at"):
                if name in columns:
                    continue
                compiler = self.engine.dialect.ddl_compiler(self.engine.dialect, None)
                statement = (f"ALTER TABLE {compiler.preparer.format_table(self.table)} "
                             f"ADD {compiler.get_column_specification(self.table.c[name])}")
                try:
                    with self.engine.begin() as conn:
                        conn.execute(text(statement))
                except DBAPIError:
                    # A concurrent startup may have added it. Never hide an unrelated DDL error.
                    if name not in {column["name"] for column in inspect(self.engine).get_columns(
                            self.table.name, schema=self.table.schema)}:
                        raise
            return
        # The existence check and the CREATE stay in separate transactions: the table is known to be
        # missing, and a CREATE alone in its transaction is what CockroachDB requires.
        self.table.create(self.engine, checkfirst=False)

    def _db_now(self) -> datetime:
        with self.engine.connect() as conn:
            return _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
