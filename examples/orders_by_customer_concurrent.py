"""PostgreSQL experiment: independent full/changes loops, one daily customer mart.

Based on orders_by_customer.py: orders(id, order_date, customer_id, amount,
is_deleted, merged_on), grouped by (order_date, customer_id). All writers use
the same MergeTracker. Full reads monthly snapshots outside the write lock;
changes wins over an older snapshot via touched_at <= snapshot boundary.
Tombstones retain old groups after a move or physical deletion.

This is not a ClickHouse recipe. Changes currently reconciles ALL groups;
scope filters journal signals, not source rows. See TRACKING_AND_MARTS.md.
"""

import hashlib
import json
from datetime import date, datetime, timezone

from sqlalchemy import (Boolean, Column, Date, DateTime, Integer, MetaData, Numeric,
                        Table, and_, func, select, text, tuple_, union)
from sqlalchemy.dialects.postgresql import insert

from dbmerge import MartHandler


class _OrdersWriter(MartHandler):
    def __init__(self, engine, *, schema="target", source_schema="target",
                 table_name="orders_by_customer_concurrent", source_table="orders"):
        if engine.dialect.name != "postgresql":
            raise ValueError("The concurrent example requires PostgreSQL")
        # Canonicalize the default schema so None and 'public' share journal/lock identities.
        schema = schema or engine.dialect.default_schema_name or "public"
        source_schema = source_schema or engine.dialect.default_schema_name or "public"
        self.target = (schema, table_name)
        self.source = (source_schema, source_table)
        super().__init__(engine, destination=(schema, table_name + self.suffix),
                         sources=[self.source])
        identity = json.dumps(self.target).encode()
        self._lock_key = int.from_bytes(hashlib.sha256(identity).digest()[:8],
                                        'big', signed=True)

    def _lock(self, conn):
        conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': self._lock_key})

    def _check_lease(self, conn, ctx):
        if ctx.lease_is_mine is not None and not conn.scalar(select(ctx.lease_is_mine)):
            raise RuntimeError('Mart lease lost before applying totals')

    def setup(self, ctx):
        self.table = Table(self.target[1], MetaData(),
                           Column('order_date', Date, primary_key=True),
                           Column('customer_id', Integer, primary_key=True),
                           Column('total_amount', Numeric, nullable=False),
                           Column('is_deleted', Boolean, nullable=False),
                           Column('touched_at', DateTime(timezone=True), nullable=False),
                           schema=self.target[0])
        # checkfirst alone does not serialize concurrent CREATE TABLE calls.
        with ctx.engine.begin() as conn:
            self._lock(conn)
            self.table.create(conn, checkfirst=True)
        self.orders = Table(self.source[1], MetaData(), schema=self.source[0],
                            autoload_with=ctx.engine)

    def _keys(self):
        return union(select(self.orders.c.order_date, self.orders.c.customer_id).where(
            self.orders.c.order_date.is_not(None), self.orders.c.customer_id.is_not(None)),
            select(self.table.c.order_date, self.table.c.customer_id))

    def _snapshot(self, conn, keys):
        if not keys:
            return []
        active = select(self.orders.c.order_date, self.orders.c.customer_id,
                        func.sum(self.orders.c.amount).label('total_amount')).where(
            self.orders.c.is_deleted.is_(False)).group_by(
                self.orders.c.order_date, self.orders.c.customer_id).subquery()
        groups = self._keys().subquery()
        rows = conn.execute(select(groups.c.order_date, groups.c.customer_id,
                                   func.coalesce(active.c.total_amount, 0).label('total_amount'),
                                   active.c.customer_id.is_(None).label('is_deleted'))
                            .select_from(groups.outerjoin(active, and_(
                                groups.c.order_date == active.c.order_date,
                                groups.c.customer_id == active.c.customer_id)))
                            .where(tuple_(groups.c.order_date, groups.c.customer_id).in_(keys))
                            .order_by(groups.c.order_date, groups.c.customer_id)).mappings()
        return [dict(row) for row in rows]

    def _apply(self, conn, ctx, rows, boundary=None):
        self._check_lease(conn, ctx)
        if not rows:
            return 0
        # now() would report transaction start, possibly before waiting for the lock.
        touched_at = conn.scalar(select(func.clock_timestamp()))
        statement = insert(self.table).values([dict(row, touched_at=touched_at) for row in rows])
        statement = statement.on_conflict_do_update(
            index_elements=[self.table.c.order_date, self.table.c.customer_id],
            set_={name: getattr(statement.excluded, name)
                  for name in ('total_amount', 'is_deleted', 'touched_at')},
            where=self.table.c.touched_at <= boundary if boundary is not None else None)
        # psycopg may report rowcount=-1 for a multi-values statement. RETURNING
        # counts actual writes, including zero when every stale row was skipped.
        return len(conn.execute(statement.returning(self.table.c.customer_id)).all())


class OrdersByCustomerChanges(_OrdersWriter):
    """Authoritative changes; even equal totals must renew the protection timestamp."""

    suffix = '__changes'
    scope = 'changes'

    def process_changes(self, ctx):
        def operation():
            with ctx.engine.begin() as conn:
                self._lock(conn)
                self._check_lease(conn, ctx)
                keys = [tuple(row) for row in conn.execute(self._keys())]
                return self._apply(conn, ctx, self._snapshot(conn, keys))

        ctx.tracker.exec(operation, destination=self.target, scope=self.scope)


class OrdersByCustomerFull(_OrdersWriter):
    """Monthly full reconciliation; stale snapshots cannot overwrite changes."""

    suffix = '__full'
    scope = 'full_load'

    def _read_month(self, ctx, starts, ends):
        boundary = ctx.tracker.safe_boundary(self.target[1], schema=self.target[0])
        groups = self._keys().subquery()
        with ctx.engine.begin() as conn:
            self._lock(conn)
            self._check_lease(conn, ctx)
            keys = [tuple(row) for row in conn.execute(select(groups).where(
                groups.c.order_date >= starts, groups.c.order_date < ends))]
            # Publish keys before reading amounts. A concurrent delete of the last
            # order must still find the old group, even on the first full pass.
            if keys:
                conn.execute(insert(self.table).values([
                    dict(order_date=day, customer_id=customer, total_amount=0,
                         is_deleted=True, touched_at=datetime(1900, 1, 1, tzinfo=timezone.utc))
                    for day, customer in keys]).on_conflict_do_nothing())
        with ctx.engine.connect() as conn:
            rows = self._snapshot(conn, keys)
        return rows, boundary

    def rebuild(self, ctx):
        groups = self._keys().subquery()
        with ctx.engine.connect() as conn:
            first, last = conn.execute(select(func.min(groups.c.order_date),
                                              func.max(groups.c.order_date))).one()
        if first is None:
            return
        starts = first.replace(day=1)
        while starts <= last:
            ends = date(starts.year + 1, 1, 1) if starts.month == 12 else date(
                starts.year, starts.month + 1, 1)
            label = starts.strftime('%Y-%m')
            if not ctx.rebuild_from or label > ctx.rebuild_from:
                rows, boundary = self._read_month(ctx, starts, ends)

                def operation():
                    with ctx.engine.begin() as conn:
                        self._lock(conn)
                        return self._apply(conn, ctx, rows, boundary)

                ctx.tracker.exec(operation, destination=self.target, scope=self.scope)
                yield label
            starts = ends

    def process_changes(self, ctx):
        # A subsequent full_load signal reconciles again; MartLoop only persists
        # the block cursor when it drives rebuild(), not this ordinary pass.
        for _ in self.rebuild(ctx):
            pass