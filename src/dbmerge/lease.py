"""A named lease stored in the tracking table: one process owns a key at a time.

The lease reuses the tracking table instead of adding another system table. Ordinary leases are
addressed by their reserved ``lease:<name>`` in ``table_name``; mart leases use their primary key
and store a readable name in ``table_name``. For either kind, "free" is ``owner IS NULL``, "held"
is ``owner`` set with a fresh ``heartbeat_at``, and ``finished_at`` stays NULL.

Acquisition is a nonreentrant conditional UPDATE: the row must be free or have
an expired heartbeat. The check and the change are one statement, so two processes can not both win.
The expiry does not prove the owner died — a long pause or a lost connection can also cost the lease,
so the caller checks ownership before critical steps.
"""

from __future__ import annotations

import threading
import uuid
import hashlib
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import exists, func, insert, literal_column, or_, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from .tracking import _coerce_datetime, _now_expr, _heartbeat_engine
from .dbmerge import IncorrectParameter
from ._validation import validate_interval

LEASE_HEARTBEAT_PERIOD = 20.0
LEASE_TTL = 900.0
LEASE_HEARTBEAT_RETRY_PERIOD = 5.0
LEASE_JOIN_TIMEOUT = 30.0

LEASE_PREFIX = "lease:"
_DEFAULT_LEASE_OPTIONS = object()


class LeaseLost(RuntimeError):
    """A supplied lease is no longer owned by this acquisition."""


class LeaseBusy(RuntimeError):
    """A required write was skipped because its lease is busy."""


@dataclass(frozen=True)
class LeaseHandle:
    """Borrowed reference to one acquisition. It never acquires or releases a lease."""

    _lease: Lease
    name: str
    token: str

    def validate(self, tracker):
        lease = self._lease
        if (lease.engine is not tracker.engine and lease.engine.url != tracker.engine.url
                or lease.table.name != tracker.table.name
                or lease.table.schema != tracker.table.schema):
            raise IncorrectParameter('lease_handle must use the tracker storage')
        self.check()

    def check(self):
        if not self._lease.still_mine(self.name, token=self.token):
            raise LeaseLost('Lease lost or lease_handle is stale')

    def sql_condition(self):
        return self._lease.sql_condition(self.name, token=self.token)

    def guard_progress(self, conn):
        """Check/renew this exact acquisition in the progress transaction."""
        lease = self._lease
        t = lease.table
        now = _coerce_datetime(conn.scalar(select(_now_expr(lease.engine))))
        result = conn.execute(update(t).where(
            lease._row_condition(self.name), t.c.owner == lease.owner,
            t.c.scope == self.token,
            t.c.heartbeat_at >= now - timedelta(seconds=lease.ttl)).values(heartbeat_at=now))
        if not result.rowcount:
            raise LeaseLost('Lease lost before committing mart progress')


def validate_lease_arguments(lease_options, lease_handle, tracker):
    from .dbmerge import LeaseOptions
    if lease_options is not None and not isinstance(lease_options, LeaseOptions):
        raise IncorrectParameter('lease_options must be LeaseOptions or None')
    if lease_handle is not None and not isinstance(lease_handle, LeaseHandle):
        raise IncorrectParameter('lease_handle must be LeaseHandle or None')
    if lease_options is not None and lease_handle is not None:
        raise IncorrectParameter('lease_options and lease_handle are mutually exclusive')
    if (lease_options is not None or lease_handle is not None) and tracker is None:
        raise IncorrectParameter('lease_options/lease_handle require tracker')
    if lease_handle is not None:
        lease_handle.validate(tracker)


def _mart_lease_options(lease_options, lease_handle, tracker):
    from .dbmerge import LeaseOptions

    if lease_options is _DEFAULT_LEASE_OPTIONS:
        lease_options = LeaseOptions() if lease_handle is None else None
    validate_lease_arguments(lease_options, lease_handle, tracker)
    if lease_options is not None and lease_options.scope is not None:
        raise IncorrectParameter('Mart lease_options.scope is managed internally (rebuild/changes)')
    return lease_options


class Lease:
    """Owns a set of named keys with a heartbeat, in the tracking table."""

    def __init__(self, engine: Engine, table, owner: str, *,
                 period: float = LEASE_HEARTBEAT_PERIOD,
                 ttl: float = LEASE_TTL,
                 retry_period: float = LEASE_HEARTBEAT_RETRY_PERIOD,
                 heartbeat_engine: Engine | None = None):
        for name, value in (('period', period), ('ttl', ttl), ('retry_period', retry_period)):
            validate_interval(name, value)
        self._owns_heartbeat_engine = heartbeat_engine is None
        self.heartbeat_engine = heartbeat_engine or _heartbeat_engine(engine)
        self.engine = engine
        self.table = table
        self.owner = owner
        self.period = period
        self.ttl = ttl
        self.retry_period = retry_period

        self._held: set[str] = set()
        self._tokens: dict[str, str] = {}
        self._lock = threading.RLock()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    @staticmethod
    def key(name: str) -> str:
        return LEASE_PREFIX + name

    def _row_id(self, name: str) -> str:
        key = self.key(name)
        # Keep existing short IDs, but do not truncate distinct long names to one PK.
        return key if len(key) <= 64 else LEASE_PREFIX + hashlib.sha256(key.encode()).hexdigest()[:58]

    def _row_condition(self, name: str):
        return self.table.c.table_name == self.key(name)

    def ensure(self, name: str) -> None:
        """Create the lease row for a name if it does not exist yet. Idempotent."""
        key = self.key(name)
        t = self.table
        with self.engine.connect() as conn:
            if len(key) > 64 and conn.scalar(select(t.c.id).where(self._row_condition(name))) is not None:
                # A pre-upgrade row may use the truncated ID. Reuse it, even while held.
                return
            try:
                conn.execute(insert(t).values(
                    id=self._row_id(name), row_kind="lease", owner=None, table_name=key,
                    started_at=func.now(), heartbeat_at=func.now()))
                conn.commit()
            except IntegrityError:
                # Another process created it first. Roll the failed INSERT back: on PostgreSQL the
                # transaction is now aborted, and the connection must not be returned in that state.
                conn.rollback()

    def acquire(self, name: str) -> bool:
        """Nonreentrant acquisition. Use renew() explicitly to extend an existing hold."""
        with self._lock:
            if self._closed.is_set():
                raise IncorrectParameter("Cannot acquire a closed lease")
            if name in self._held:
                return False
            t = self.table
            token = uuid.uuid4().hex
            with self.engine.begin() as conn:
                now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
                result = conn.execute(update(t).where(
                    self._row_condition(name),
                    or_(t.c.owner.is_(None), t.c.heartbeat_at < now - timedelta(seconds=self.ttl)))
                    .values(owner=self.owner, heartbeat_at=now, scope=token))
            if result.rowcount:
                self._held.add(name)
                self._tokens[name] = token
                self._ensure_heartbeat()
                return True
            return False

    def handle(self, name: str) -> LeaseHandle:
        with self._lock:
            token = self._tokens.get(name)
            if name not in self._held or token is None:
                raise LeaseLost('Cannot reference a lease that is not held')
            handle = LeaseHandle(self, name, token)
            handle.check()
            return handle

    def renew(self, name: str) -> bool:
        """Extend a fresh hold; never resurrect an expired or replaced hold."""
        with self._lock:
            if self._closed.is_set() or name not in self._held:
                return False
            t = self.table
            with self.heartbeat_engine.begin() as conn:
                now = _coerce_datetime(conn.scalar(select(_now_expr(self.heartbeat_engine))))
                result = conn.execute(update(t).where(
                    self._row_condition(name), t.c.owner == self.owner,
                    t.c.scope == self._tokens.get(name),
                    t.c.heartbeat_at >= now - timedelta(seconds=self.ttl))
                    .values(heartbeat_at=now))
            if not result.rowcount:
                self._held.discard(name)
            return bool(result.rowcount)

    def release(self, name: str) -> None:
        """Release this acquisition without freeing a newer holder's lease."""
        with self._lock:
            self._held.discard(name)
            token = self._tokens.pop(name, None)
        if token is None:
            return
        t = self.table
        with self.engine.begin() as conn:
            # Free = owner IS NULL; heartbeat_at stays (it is NOT NULL) and no longer matters.
            conn.execute(update(t)
                         .where(self._row_condition(name), t.c.owner == self.owner, t.c.scope == token)
                         .values(owner=None))

    def still_mine(self, name: str, *, token: str | None = None) -> bool:
        """Check that the held acquisition is still current and unexpired."""
        if self._closed.is_set():
            return False
        with self._lock:
            if name not in self._held:
                return False
            token = token or self._tokens.get(name)
            if token != self._tokens.get(name):
                return False
        t = self.table
        with self.heartbeat_engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            cutoff = now - timedelta(seconds=self.ttl)
            found = conn.scalar(
                select(t.c.id).where(self._row_condition(name),
                                     t.c.owner == self.owner,
                                     t.c.scope == token,
                                     t.c.heartbeat_at >= cutoff))
        return found is not None

    def sql_condition(self, name: str, *, token: str | None = None):
        """Statement-time ownership predicate, only for SQL executed in this database."""
        dialect = self.engine.dialect.name
        now = _now_expr(self.engine)
        if dialect == 'postgresql':
            now = func.clock_timestamp()
        if dialect == 'sqlite':
            cutoff = func.datetime('now', f'-{self.ttl} seconds')
        elif dialect in ('mysql', 'mariadb'):
            cutoff = func.timestampadd(literal_column('MICROSECOND'), -int(self.ttl * 1000000), now)
        elif dialect == 'mssql':
            cutoff = func.dateadd(literal_column('millisecond'), -int(self.ttl * 1000), now)
        elif dialect == 'oracle':
            cutoff = now - func.numtodsinterval(self.ttl, 'SECOND')
        else:
            cutoff = now - timedelta(seconds=self.ttl)
        t = self.table
        return exists().where(self._row_condition(name), t.c.owner == self.owner,
                              t.c.scope == (token or self._tokens.get(name)),
                              t.c.heartbeat_at >= cutoff)

    # ------------------------------------------------------------------ heartbeat

    def _ensure_heartbeat(self) -> None:
        with self._lock:
            if self._heartbeat_thread is not None or self._closed.is_set():
                return
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name=f"dbmerge-lease:{self.owner}", daemon=True)
            self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._closed.is_set():
            try:
                self.heartbeat()
            except Exception:
                import logging
                logging.getLogger("dbmerge").exception("Lease heartbeat failed")
            self._closed.wait(self.period)
            with self._lock:
                if not self._held:
                    self._heartbeat_thread = None
                    return

    def heartbeat(self) -> None:
        with self._lock:
            held = list(self._held)
        for name in held:
            self.renew(name)

    def close(self) -> None:
        """Stop renewal and release every owned key. Caller engines remain caller-owned."""
        self._closed.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=LEASE_JOIN_TIMEOUT)
            if thread.is_alive():
                raise RuntimeError("Lease heartbeat did not stop; heartbeat engine remains open")
        with self._lock:
            held = list(self._held)
        try:
            for name in held:
                self.release(name)
        finally:
            if self._owns_heartbeat_engine:
                self.heartbeat_engine.dispose()
