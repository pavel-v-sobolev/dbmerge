"""Concurrent startup must tolerate another process creating service storage."""

import importlib

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.ddl import CreateTable

from dbmerge import MergeTracker
from dbmerge.mart import _ensure_marts_table


@pytest.mark.parametrize('component', ['tracker', 'mart'])
@pytest.mark.parametrize('concurrent', [True, False])
def test_service_table_creation_race(monkeypatch, component, concurrent, tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "startup.sqlite"}')
    module = importlib.import_module('dbmerge.tracking' if component == 'tracker' else 'dbmerge.mart')
    original_inspect = module.inspect
    checked = []

    class StaleInspector:
        def __init__(self, bind):
            self.actual = original_inspect(bind)

        def has_table(self, *args, **kwargs):
            if not checked:
                checked.append(True)
                return False  # process B observed absence before process A committed CREATE
            return self.actual.has_table(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.actual, name)

    table_name = 'dbmerge_tracking' if component == 'tracker' else 'dbmerge_marts'
    try:
        if concurrent:
            if component == 'tracker':
                MergeTracker(engine).close()
            else:
                _ensure_marts_table(engine, None)
        monkeypatch.setattr(module, 'inspect', StaleInspector)
        if not concurrent:
            from sqlalchemy import event

            @event.listens_for(engine, 'before_execute')
            def fail_create(conn, clauseelement, multiparams, params, execution_options):
                if isinstance(clauseelement, CreateTable):
                    raise OperationalError('CREATE TABLE', {}, RuntimeError('permission denied'))

        def start():
            if component == 'tracker':
                MergeTracker(engine).close()
            else:
                _ensure_marts_table(engine, None)

        if concurrent:
            start()
            assert original_inspect(engine).has_table(table_name)
        else:
            with pytest.raises(OperationalError, match='permission denied'):
                start()
    finally:
        engine.dispose()