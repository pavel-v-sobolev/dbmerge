"""Distinct long lease names must not collide in the journal primary key."""

from sqlalchemy import create_engine, select, update

from dbmerge import Lease, MergeTracker


def test_long_names_with_common_prefix_are_independent(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "leases.sqlite"}')
    try:
        with MergeTracker(engine) as tracker:
            lease = Lease(engine, tracker.table, 'test', heartbeat_engine=tracker.heartbeat_engine)
            first, second = 'shared-' + 'a' * 80 + '-one', 'shared-' + 'a' * 80 + '-two'
            try:
                lease.ensure(first)
                lease.ensure(second)
                with engine.connect() as conn:
                    rows = conn.execute(select(tracker.table.c.id).where(
                        tracker.table.c.table_name.in_([lease.key(first), lease.key(second)]))).all()
                assert len(rows) == 2
                assert lease.acquire(first)
                assert lease.acquire(second)
                # Simulate a pre-upgrade row without changing its ownership or lookup name.
                with engine.begin() as conn:
                    conn.execute(update(tracker.table).where(
                        tracker.table.c.table_name == lease.key(first)).values(id=lease.key(first)[:64]))
                lease.ensure(first)
                with engine.connect() as conn:
                    assert len(conn.execute(select(tracker.table.c.id).where(
                        tracker.table.c.table_name == lease.key(first))).all()) == 1
                assert lease.still_mine(first)
            finally:
                lease.close()
    finally:
        engine.dispose()