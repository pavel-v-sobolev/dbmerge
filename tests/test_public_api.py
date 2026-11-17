"""Canonical class name and backwards-compatible public imports."""

import importlib

import pytest
from sqlalchemy import create_engine, text

from dbmerge import DBMerge, dbmerge


def test_merge_class_names_are_identical():
    package = importlib.import_module('dbmerge')
    module = importlib.import_module('dbmerge.dbmerge')
    assert DBMerge is dbmerge is module.DBMerge is module.dbmerge
    assert DBMerge.__name__ == 'DBMerge'
    assert {'DBMerge', 'dbmerge'} <= set(package.__all__)


@pytest.mark.parametrize('merge_class', [DBMerge, dbmerge], ids=['DBMerge', 'legacy'])
def test_both_class_names_execute_merges(merge_class):
    engine = create_engine('sqlite:///:memory:')
    try:
        for cls, value in ((merge_class, 'first'), (DBMerge, 'updated')):
            with cls(engine, table_name='orders', data=[{'id': 1, 'value': value}],
                     key=['id']) as merge:
                assert isinstance(merge, DBMerge)
                assert isinstance(merge, dbmerge)
                merge.exec()
        with engine.connect() as conn:
            assert conn.execute(text('SELECT id, value FROM orders')).all() == [(1, 'updated')]
    finally:
        engine.dispose()