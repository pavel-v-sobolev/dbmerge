"""A named lease stored in the tracking table: one process owns a key at a time.

The lease reuses the tracking table instead of adding another system table. A lease row is an
ordinary row whose ``table_name`` is a reserved ``lease:<name>`` key: "free" is ``owner IS NULL``,
"held" is ``owner`` set with a fresh ``heartbeat_at``. ``finished_at`` stays NULL for lease rows, so
they never look like a finished merge to the tracker's own readers (which filter by real table names
anyway).

Acquisition is a nonreentrant conditional UPDATE: the row must be free or have
an expired heartbeat. The check and the change are one statement, so two processes can not both win.
The expiry does not prove the owner died — a long pause or a lost connection can also cost the lease,
so the caller checks ownership before critical steps.
"""

from __future__ import annotations

import threading
from datetime import timedelta

from sqlalchemy import func, insert, or_, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from .tracking import _coerce_datetime, _now_expr, _heartbeat_engine
from .dbmerge import IncorrectParameter

LEASE_HEARTBEAT_PERIOD = 20.0
LEASE_TTL = 900.0
LEASE_HEARTBEAT_RETRY_PERIOD = 5.0
LEASE_JOIN_TIMEOUT = 30.0

LEASE_PREFIX = "lease:"


class Lease:
    """Owns a set of named keys with a heartbeat, in the tracking table."""

    def __init__(self, engine: Engine, table, owner: str, *,
                 period: float = LEASE_HEARTBEAT_PERIOD,
                 ttl: float = LEASE_TTL,
                 retry_period: float = LEASE_HEARTBEAT_RETRY_PERIOD,
                 heartbeat_engine: Engine | None = None):
        self._owns_heartbeat_engine = heartbeat_engine is None
        self.heartbeat_engine = heartbeat_engine or _heartbeat_engine(engine)
        self.engine = engine
        self.table = table
        self.owner = owner
        self.period = period
        self.ttl = ttl
        self.retry_period = retry_period

        self._held: set[str] = set()
        self._lock = threading.RLock()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    @staticmethod
    def key(name: str) -> str:
        return LEASE_PREFIX + name

    def ensure(self, name: str) -> None:
        """Create the lease row for a name if it does not exist yet. Idempotent."""
        key = self.key(name)
        t = self.table
        with self.engine.connect() as conn:
            try:
                conn.execute(insert(t).values(
                    id=key[:64], row_kind="lease", owner=None, table_name=key,
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
            key = self.key(name)
            t = self.table
            with self.engine.begin() as conn:
                now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
                result = conn.execute(update(t).where(
                    t.c.table_name == key,
                    or_(t.c.owner.is_(None), t.c.heartbeat_at < now - timedelta(seconds=self.ttl)))
                    .values(owner=self.owner, heartbeat_at=now))
            if result.rowcount:
                self._held.add(name)
                self._ensure_heartbeat()
                return True
            return False

    def renew(self, name: str) -> bool:
        """Extend a fresh hold; never resurrect an expired or replaced hold."""
        with self._lock:
            if self._closed.is_set() or name not in self._held:
                return False
            t = self.table
            with self.heartbeat_engine.begin() as conn:
                now = _coerce_datetime(conn.scalar(select(_now_expr(self.heartbeat_engine))))
                result = conn.execute(update(t).where(
                    t.c.table_name == self.key(name), t.c.owner == self.owner,
                    t.c.heartbeat_at >= now - timedelta(seconds=self.ttl))
                    .values(heartbeat_at=now))
            if not result.rowcount:
                self._held.discard(name)
            return bool(result.rowcount)

    def release(self, name: str) -> None:
        """Free the name — only if this process still owns it."""
        key = self.key(name)
        with self._lock:
            self._held.discard(name)
        t = self.table
        with self.engine.begin() as conn:
            # Free = owner IS NULL; heartbeat_at stays (it is NOT NULL) and no longer matters.
            conn.execute(update(t)
                         .where(t.c.table_name == key, t.c.owner == self.owner)
                         .values(owner=None))

    def still_mine(self, name: str) -> bool:
        """Whether this process still owns the name with a fresh heartbeat."""
        if self._closed.is_set():
            return False
        key = self.key(name)
        t = self.table
        with self.heartbeat_engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(_now_expr(self.engine))))
            cutoff = now - timedelta(seconds=self.ttl)
            found = conn.scalar(
                select(t.c.id).where(t.c.table_name == key,
                                     t.c.owner == self.owner,
                                     t.c.heartbeat_at >= cutoff))
        return found is not None

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
