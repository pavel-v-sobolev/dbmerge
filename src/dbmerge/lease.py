"""A named lease stored in the tracking table: one process owns a key at a time.

The lease reuses the tracking table instead of adding another system table. A lease row is an
ordinary row whose ``table_name`` is a reserved ``lease:<name>`` key: "free" is ``owner IS NULL``,
"held" is ``owner`` set with a fresh ``heartbeat_at``. ``finished_at`` stays NULL for lease rows, so
they never look like a finished merge to the tracker's own readers (which filter by real table names
anyway).

Acquisition is a conditional UPDATE: the row must be free, already owned by the same owner, or have
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

from .tracking import _coerce_datetime

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
                 retry_period: float = LEASE_HEARTBEAT_RETRY_PERIOD):
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
        with self.engine.begin() as conn:
            try:
                conn.execute(insert(t).values(
                    id=key[:64], owner=None, table_name=key,
                    started_at=func.now(), heartbeat_at=func.now()))
            except IntegrityError:
                # Another process created it first. The row is there; nothing else to do.
                pass

    def acquire(self, name: str) -> bool:
        """Claim the name, returning True if this process owns it now."""
        key = self.key(name)
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(func.now())))
            cutoff = now - timedelta(seconds=self.ttl)
            result = conn.execute(
                update(t)
                .where(t.c.table_name == key,
                       or_(t.c.owner.is_(None),
                           t.c.owner == self.owner,
                           t.c.heartbeat_at < cutoff))
                .values(owner=self.owner, heartbeat_at=now))
            conn.commit()
        if result.rowcount:
            with self._lock:
                self._held.add(name)
                self._ensure_heartbeat()
            return True
        return False

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
        key = self.key(name)
        t = self.table
        with self.engine.connect() as conn:
            now = _coerce_datetime(conn.scalar(select(func.now())))
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
        if not held:
            return
        keys = [self.key(name) for name in held]
        t = self.table
        with self.engine.begin() as conn:
            conn.execute(update(t)
                         .where(t.c.table_name.in_(keys), t.c.owner == self.owner)
                         .values(heartbeat_at=func.now()))

    def close(self) -> None:
        self._closed.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=LEASE_JOIN_TIMEOUT)
