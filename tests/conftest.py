"""Shared engine matrix and per-test fixtures for the test suite.

A test that needs a live database is parametrized with ``engine_name`` (a key of ``engines``) and
calls ``make_engine(engine_name)``. The matrix is narrowed by the ``DBMERGE_TEST_ENGINES``
environment variable, and individual URLs by ``DBMERGE_TEST_URL_<NAME>``; the CI workflows set
these and run ``pytest tests/test_all.py tests/test_tracking.py tests/test_mart.py``.
"""

import os
import urllib.parse

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError

from db_hooks import dialect_hook

mssql_settings = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=master;"
    "UID=sa;"
    "PWD=MSSQL.test_pass;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

engines = {
    'sqlite': """sqlite:///data/data.sqlite""",
    'postgres': """postgresql+psycopg2://postgres:postgres@localhost:5432/dbmerge""",
    'postgres-psycopg': "postgresql+psycopg://postgres:postgres@localhost:5432/dbmerge",
    'postgres-pg8000': "postgresql+pg8000://postgres:postgres@localhost:5432/dbmerge",
    'mariadb': """mariadb+mariadbconnector://root:root@127.0.0.1:3306""",
    'mysql-pymysql': "mysql+pymysql://root:root@127.0.0.1:3307",
    'mysql-connector': "mysql+mysqlconnector://root:root@127.0.0.1:3307",
    'mysql-mysqlclient': "mysql+mysqldb://root:root@127.0.0.1:3307",
    'mssql': f"mssql+pyodbc:///?odbc_connect={mssql_settings}",
    'mssql-pymssql': "mssql+pymssql://sa:MSSQL.test_pass@localhost:1433/master",
    'mssql-python': "mssql+mssqlpython://sa:MSSQL.test_pass@localhost:1433/master?TrustServerCertificate=yes",
    'cockroachdb': "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable",
    'cockroachdb-psycopg': "cockroachdb+psycopg://root@localhost:26257/defaultdb?sslmode=disable",
    'clickhouse': "clickhousedb://default:@localhost:8123/default",
    # A file, not :memory: — every connection to :memory: is a database of its own, so the checks
    # below would not see what the merge wrote.
    'duckdb': 'duckdb:///data/data.ddb',
    'oracle': "oracle+oracledb://system:oracle@localhost:1521/?service_name=XEPDB1",
}

# Any URL above can be replaced from the environment, e.g. DBMERGE_TEST_URL_CLICKHOUSE for another
# server version, or DBMERGE_TEST_URL_MYSQL_PYMYSQL for another port.
for _name in engines:
    _override = os.environ.get('DBMERGE_TEST_URL_' + _name.upper().replace('-', '_'))
    if _override:
        engines[_name] = _override

# DBMERGE_TEST_ENGINES=postgres,postgres-pg8000 runs the tests of these engines only. Every URL,
# before DBMERGE_TEST_ENGINES narrows engines down: a test of one engine may need another as a source.
all_engine_urls = dict(engines)

if os.environ.get('DBMERGE_TEST_ENGINES'):
    _selected = os.environ['DBMERGE_TEST_ENGINES'].split(',')
    _unknown = [name for name in _selected if name not in engines]
    if _unknown:
        raise ValueError(f'DBMERGE_TEST_ENGINES: unknown engines {_unknown}, known are {list(engines)}')
    engines = {name: url for name, url in engines.items() if name in _selected}


def make_engine(engine_name):
    # CockroachDB runs SERIALIZABLE by default; a merge step can stay open long enough for an
    # unrelated commit to fail COMMIT with a transient RETRY_SERIALIZABLE, so the tests ask for
    # READ COMMITTED. An engine whose dialect or driver is not installed here is skipped.
    try:
        if engine_name.startswith('cockroachdb'):
            engine = create_engine(engines[engine_name], isolation_level='READ COMMITTED')
        else:
            engine = create_engine(engines[engine_name])
    except (NoSuchModuleError, ImportError) as e:
        pytest.skip(f'{engine_name} is not available here: {e}')
    _made_engines.append(engine)
    if hook := dialect_hook(engine.dialect.name, 'prepare_engine'):
        # Prepare each database URL once, not on every short-lived test engine.
        url = engine.url.render_as_string(hide_password=False)
        if url not in _prepared_databases:
            hook(engine)
            _prepared_databases.add(url)
    return engine


# The engines make_engine made, disposed after every test: a pooled connection stays open until its
# engine is disposed or collected, and a whole run once used up the 151 connections of MariaDB.
_made_engines = []
_prepared_databases = set()


@pytest.fixture(autouse=True)
def _prepare_database_test(request):
    engine_name = getattr(request.node, 'callspec', None)
    if engine_name is None:
        return
    engine_name = engine_name.params.get('engine_name')
    if engine_name is None:
        return
    # Resolve through the URL, so overrides and driver aliases keep working.
    dialect_name = make_url(engines[engine_name]).get_backend_name()
    if hook := dialect_hook(dialect_name, 'prepare_test'):
        hook(request)


@pytest.fixture(autouse=True)
def _dispose_engines():
    yield
    while _made_engines:
        _made_engines.pop().dispose()
