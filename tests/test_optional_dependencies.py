"""The base install must remain usable without any DataFrame/Arrow extras."""

import subprocess
import sys


def test_sqlite_merge_without_optional_imports():
    script = '''
import importlib.abc
import sys

class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pandas', 'polars', 'pyarrow'}:
            raise ModuleNotFoundError('Blocked optional dependency: ' + fullname)

sys.meta_path.insert(0, BlockOptional())
from sqlalchemy import create_engine, text
from dbmerge import DBMerge, dbmerge
assert DBMerge is dbmerge
engine = create_engine('sqlite://')
try:
    with DBMerge(engine, 'items', data=[{'id': 1, 'value': 'ok'}], key=['id']) as merge:
        assert merge.exec().inserted_row_count == 1
    with engine.connect() as connection:
        assert connection.execute(text('SELECT value FROM items')).scalar_one() == 'ok'
finally:
    engine.dispose()
assert not {'pandas', 'polars', 'pyarrow'} & set(sys.modules)
'''
    completed = subprocess.run([sys.executable, '-c', script], capture_output=True,
                               text=True, timeout=30)
    assert completed.returncode == 0, completed.stdout + completed.stderr