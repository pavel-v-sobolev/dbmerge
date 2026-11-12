"""
Change tracking for dbmerge merges.

The module answers the two questions a downstream consumer (a data mart built on top of merged
tables) asks about a table:

* "Has this table really changed since a given moment?" — the ``changed`` flag, recorded when a
  merge finishes, distinguishes a merge that wrote rows from a no-op merge that found nothing to
  change.
* "Is a merge running right now, so the table may be half-written?" — a merge records a row before
  it starts and marks it finished afterwards. While it runs, a background thread refreshes the row's
  heartbeat, and ``boundary`` returns the earliest start of a live merge.

All of it lives in one table (one row per merge). For every table name the table keeps the last
finished row and any number of unfinished rows: a finished merge replaces earlier finished rows for
the same table, so history does not grow. A merge that died mid-way is left unfinished with a stale
heartbeat; it stops holding the boundary once its heartbeat expires, and ``recover_abandoned`` marks
it changed so marts that missed it wake up.

The tracking table is created on first use, so a project that never opts into tracking gets no extra
tables. The feature is built around timezone-aware timestamps and is meant for engines that support
them (PostgreSQL/CockroachDB first); on other engines recording works, and the heartbeat/boundary
semantics follow the engine's datetime handling.
"""

from __future__ import annotations

import os
import socket
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from sqlalchemy import (Boolean, Column, DateTime, Engine, MetaData, String, Table, and_,
                        delete, func, inspect, insert, schema, select, update)

from .dbmerge import IncorrectParameter, _is_clickhouse_client, logger, mergeResult

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


def _instance_owner(name: str) -> str:
    """An owner string that distinguishes processes and restarts, for diagnostics only."""
    return f"{name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _coerce_datetime(value: Any) -> datetime:
    # sqlite has no datetime type: CURRENT_TIMESTAMP comes back as text there. Everywhere else it is
    # a datetime. Either form compares consistently within its own engine.
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


@dataclass(frozen=True)
class MergeRecord:
    """What the tracking table recorded about one merge."""

    table_name: str
    database_type: str
    server: str
    database_name: str
    started_at: datetime
    finished_at: datetime | None
    changed: bool
    boundary: datetime | None


@dataclass(frozen=True)
class Database:
    """Where a merged table lives: the dialect, the server and the database name.

    One tracking table can record merges from several databases (e.g. source tables in PostgreSQL
    and a mart in ClickHouse). These three values keep rows of equal table names apart.
    """

    type: str
    server: str
    database: str


def database_of(engine) -> Database:
    """The identity of the database an engine points at."""
    if _is_clickhouse_client(engine):
        host = getattr(engine, "host", "") or ""
        port = getattr(engine, "port", None)
        server = f"{host}:{port}" if port else host
        return Database(type="clickhousedb", server=server,
                        database=getattr(engine, "database", "") or "")
    url = engine.url
    server = url.host or ""
    if url.port is not None:
        server = f"{server}:{url.port}"
    return Database(type=engine.dialect.name, server=server, database=url.database or "")


def _tracking_table(metadata: MetaData, name: str, schema: str | None) -> Table:
    return Table(
        name, metadata,
        Column("id", String(64), primary_key=True),
        # NULL only for lease rows ("free"); a merge row always names its process owner.
        Column("owner", String(255), nullable=True),
        Column("table_name", String(255), nullable=False),
        # Where the merged table lives, so equal table names in different databases stay apart.
        Column("database_type", String(64), nullable=True),
        Column("server", String(255), nullable=True),
        Column("database_name", String(255), nullable=True),
        # All marks are by the database clock: the one clock the writer and the reader share.
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("finished_at", DateTime(timezone=True), nullable=True),
        Column("heartbeat_at", DateTime(timezone=True), nullable=False),
        # Whether the merge actually wrote rows (inserted + updated + deleted > 0). Set on finish.
        Column("changed", Boolean, nullable=True),
        # For a mart merge: the upper bound of the window it processed, recorded on success. It is
        # the watermark the next mart update starts from. A recovered row never has one.
        Column("boundary", DateTime(timezone=True), nullable=True),
        schema=schema,
    )

class MergeTracker:
    """A database-backed journal of merges: track a merge, then ask when and whether a table changed.

    The tracker is used as a context manager so its heartbeat thread is stopped on exit, and the
    merge itself is the standard dbmerge context::

        with MergeTracker(engine) as tracker:
            with dbmerge(engine, table_name="orders", data=rows, key=["id"]) as merge:
                record, result = tracker.track(merge)

    ``track`` inserts a row before the merge, runs ``merge.exec()`` itself, and marks the row
    finished afterwards. A separate daemon thread keeps the row's heartbeat fresh while the merge
    runs, on its own connection, so a long merge is not mistaken for a dead one.
    """

    def __init__(self, engine: Engine, table: str = TRACKING_TABLE,
                 schema: str | None = None, owner: str | None = None,
                 heartbeat_period: float = HEARTBEAT_PERIOD,
                 heartbeat_ttl: float = HEARTBEAT_TTL):
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
        self.table = _tracking_table(self.metadata, table, schema)
        self._ensure_schema()
        self._ensure_table()

        self._in_flight: set[str] = set()
        # RLock: _begin holds the lock while _ensure_heartbeat takes it again to check-and-create
        # the thread atomically.
        self._lock = threading.RLock()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    # ------------------------------------------------------------------ context manager

    def __enter__(self) -> "MergeTracker":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ tracking

    def track(self, merge, *, boundary: datetime | None = None,
              changed: bool | None = None, **exec_kwargs):
        """Run ``merge.exec(**exec_kwargs)`` and record it in the tracking table.

        ``merge`` is a dbmerge instance (or anything with ``table_name``, ``exec`` and the
        inserted/updated/deleted row counters). The table name comes from the merge itself, and
        ``changed`` is derived from its counters unless it is passed explicitly. ``boundary`` is
        recorded on success and is the watermark the next mart update starts from.

        Returns ``(MergeRecord, mergeResult)``. On failure the row is left unfinished with a stale
        heartbeat: it stops holding the boundary once its heartbeat expires and can be recovered by
        ``recover_abandoned``.
        """
        table_name = getattr(merge, "table_name", None)
        if not table_name:
            raise IncorrectParameter("track() expects a dbmerge instance; its table_name is empty")
        if not callable(getattr(merge, "exec", None)):
            raise IncorrectParameter("track() expects a dbmerge instance with an exec() method")
        if getattr(merge, "merge_finished", False):
            raise IncorrectParameter(f"Merge of table \"{table_name}\" is already finished; "
                                     f"track it before it runs")
        if changed is not None and not isinstance(changed, bool):
            raise IncorrectParameter(f"changed must be True, False or None, got {changed!r}")
        if boundary is not None and not isinstance(boundary, datetime):
            raise IncorrectParameter(f"boundary must be a datetime or None, got {boundary!r}")

        merge_engine = getattr(merge, "engine", None)
        database = database_of(merge_engine) if merge_engine is not None else Database("", "", "")

        row_id, started_at = self._begin(table_name, database)
        try:
            merge_result = merge.exec(**exec_kwargs)
        except BaseException:
            self._abandon(row_id)
            raise

        if changed is None:
            changed = (getattr(merge, "inserted_row_count", 0)
                       + getattr(merge, "updated_row_count", 0)
                       + getattr(merge, "deleted_row_count", 0)) > 0
        finished_at = self._finish(row_id, table_name, changed, boundary)
        return (MergeRecord(table_name=table_name, database_type=database.type,
                            server=database.server, database_name=database.database,
                            started_at=started_at, finished_at=finished_at,
                            changed=changed, boundary=boundary),
                merge_result)

    def _begin(self, table_name: str, database: Database):
        """Insert the row before the merge and register it for heartbeat. Returns (id, started_at)."""
        row_id = str(uuid.uuid4())
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(func.now())))
            conn.execute(insert(self.table).values(
                id=row_id, owner=self.owner, table_name=table_name,
                database_type=database.type, server=database.server,
                database_name=database.database,
                started_at=now, heartbeat_at=now))
            conn.commit()
        with self._lock:
            self._in_flight.add(row_id)
            self._ensure_heartbeat()
        return row_id, now

    def _finish(self, row_id: str, table_name: str, changed: bool,
                boundary: datetime | None) -> datetime:
        """Mark the row finished, replace earlier finished rows of the table, stop its heartbeat."""
        with self._lock:
            self._in_flight.discard(row_id)
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(func.now())))
            conn.execute(update(t).where(t.c.id == row_id)
                         .values(finished_at=now, changed=changed, boundary=boundary))
            # One finished row per table: the just-finished merge replaces every earlier finished
            # row. Unfinished rows are left alone.
            conn.execute(delete(t).where(t.c.table_name == table_name,
                                         t.c.finished_at.is_not(None),
                                         t.c.id != row_id))
            conn.commit()
        return now

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
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id.in_(in_flight))
                         .values(heartbeat_at=func.now()))

    def close(self) -> None:
        """Stop the heartbeat thread and wait up to HEARTBEAT_JOIN_TIMEOUT for it."""
        self._closed.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT)

    # ------------------------------------------------------------------ reading

    def boundary(self, table_names: Iterable[str], database: Database | None = None) -> datetime:
        """The upper bound of safe reading: min(now, earliest start of a live merge on these tables.

        An unfinished merge whose heartbeat is stale does not hold the boundary, so a dead process
        blocks readers for at most heartbeat_ttl. The time and the minimum come from one connection,
        so "now" for the cutoff and "now" for the bound are the same clock reading.
        """
        names = list(table_names)
        if not names:
            return self._db_now()
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(func.now())))
            cutoff = now - timedelta(seconds=self.heartbeat_ttl)
            conds = [t.c.table_name.in_(names), t.c.finished_at.is_(None),
                     t.c.heartbeat_at > cutoff]
            if database is not None:
                conds.append(self._database_where(database))
            earliest = conn.scalar(select(func.min(t.c.started_at)).where(*conds))
        return min(now, earliest) if earliest is not None else now

    def last_merge(self, table_name: str, database: Database | None = None) -> MergeRecord | None:
        """The last finished merge of a table, or None if it has none."""
        t = self.table
        conds = [t.c.table_name == table_name, t.c.finished_at.is_not(None)]
        if database is not None:
            conds.append(self._database_where(database))
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.table_name, t.c.database_type, t.c.server, t.c.database_name,
                       t.c.started_at, t.c.finished_at, t.c.changed, t.c.boundary)
                .where(*conds)
                .order_by(t.c.finished_at.desc())
                .limit(1)).first()
        if row is None:
            return None
        return MergeRecord(table_name=row.table_name, database_type=row.database_type,
                           server=row.server, database_name=row.database_name,
                           started_at=row.started_at, finished_at=row.finished_at,
                           changed=bool(row.changed), boundary=row.boundary)

    def watermark(self, table_name: str, database: Database | None = None) -> datetime | None:
        """The boundary of the last successful merge of a mart, i.e. where its next update starts.

        Only successful finishes carry a boundary; a recovered (crashed) row has none, so a crash
        never advances the watermark. Returns None when the mart has no successful run yet.
        """
        t = self.table
        conds = [t.c.table_name == table_name, t.c.boundary.is_not(None)]
        if database is not None:
            conds.append(self._database_where(database))
        with self.engine.connect() as conn:
            return conn.scalar(select(func.max(t.c.boundary)).where(*conds))

    def changed_since(self, table_names: Iterable[str], since: datetime,
                      database: Database | None = None) -> bool:
        """Whether any of the tables really changed (a finished, changed merge) after ``since``."""
        names = list(table_names)
        if not names:
            return False
        t = self.table
        conds = [t.c.table_name.in_(names), t.c.changed == True, t.c.finished_at > since]
        if database is not None:
            conds.append(self._database_where(database))
        with self.engine.connect() as conn:
            found = conn.scalar(select(t.c.id).where(*conds).limit(1))
        return found is not None

    def recover_abandoned(self) -> int:
        """Mark merges with an expired heartbeat as finished and changed, and return how many.

        A merge that died may have committed its data, so marts that missed it are woken
        unconditionally (their own query over the window is idempotent). Recovered rows carry no
        boundary, so a crashed mart never advances its own watermark. They are left until the next
        successful merge of the table replaces them.
        """
        t = self.table
        with self.engine.connect() as conn:
            cutoff = _coerce_datetime(conn.scalar(select(func.now()))) \
                - timedelta(seconds=self.heartbeat_ttl)
            ids = conn.execute(
                select(t.c.id)
                .where(t.c.finished_at.is_(None), t.c.heartbeat_at <= cutoff)).scalars().all()
        recovered = 0
        for row_id in ids:
            with self.engine.begin() as conn:
                result = conn.execute(
                    update(t)
                    .where(t.c.id == row_id, t.c.finished_at.is_(None))
                    .values(finished_at=func.now(), changed=True))
                if result.rowcount:
                    recovered += 1
        if recovered:
            logger.warning("Recovered %s abandoned merge(s) as changed", recovered)
        return recovered

    def reset(self, table_name: str) -> int:
        """Drop the finished row(s) of a table, so its next mart update starts from the beginning."""
        t = self.table
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(t).where(t.c.table_name == table_name, t.c.finished_at.is_not(None)))
        return result.rowcount

    # ------------------------------------------------------------------ internals

    def _database_where(self, database: Database):
        t = self.table
        # Oracle stores an empty string as NULL, so empty values match through COALESCE.
        return and_(t.c.database_type == database.type,
                    func.coalesce(t.c.server, "") == database.server,
                    func.coalesce(t.c.database_name, "") == database.database)

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
            return
        # The existence check and the CREATE stay in separate transactions: the table is known to be
        # missing, and a CREATE alone in its transaction is what CockroachDB requires.
        self.table.create(self.engine, checkfirst=False)

    def _db_now(self) -> datetime:
        with self.engine.connect() as conn:
            return _coerce_datetime(conn.scalar(select(func.now())))
