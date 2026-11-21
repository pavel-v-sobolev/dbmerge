"""Keep the copyable documentation aligned with the public API and package contents."""

import ast
import inspect
import re
import runpy
import textwrap
from pathlib import Path
from urllib.parse import unquote

import pytest
from sqlalchemy import create_engine

from dbmerge import DBMerge


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = 'https://github.com/pavel-v-sobolev/dbmerge/blob/main/'
GUIDES = ['README.md', 'DOCUMENTATION.md', 'TRACKING_AND_MARTS.md',
          'CLICKHOUSE.md', 'CLICKHOUSE_RU.md', 'GUIDES.md',
          'POSTGRESQL_TO_CLICKHOUSE.md', 'POSTGRESQL_TO_CLICKHOUSE_RU.md',
          'ORACLE.md', 'COMPATIBILITY.md', 'CONTRIBUTING.md', 'CHANGELOG.md', 'llms.txt']


def _heading_anchors(content):
    # ATX headings used by our guides; ignore headings in code and handle repeats.
    seen = set()
    content = re.sub(r'^```[^\n]*\n.*?^```[^\n]*$', '', content, flags=re.M | re.S)
    for heading in re.findall(r'^#{1,6} (.+)$', content, re.M):
        base = re.sub(r'[^\w -]', '', heading.lower()).replace(' ', '-')
        anchor, suffix = base, 0
        while anchor in seen:
            suffix += 1
            anchor = f'{base}-{suffix}'
        seen.add(anchor)
    return seen


@pytest.mark.parametrize('name', GUIDES)
def test_python_examples_and_local_links(name):
    path = ROOT / name
    content = path.read_text()
    for number, block in enumerate(re.findall(r'```python\n(.*?)```', content, re.S), 1):
        ast.parse(textwrap.dedent(block), filename=f'{name}:python-block-{number}')
    for target in re.findall(r'\]\(([^)]+)\)', content):
        if target.startswith(REPOSITORY):
            base, target = ROOT, target.removeprefix(REPOSITORY)
        elif target.startswith(('http:', 'https:', 'mailto:')):
            continue
        else:
            base = path.parent
        filename, _, anchor = target.partition('#')
        destination = base / unquote(filename) if filename else path
        assert destination.exists(), f'{name}: {target}'
        if anchor and destination.suffix == '.md':
            assert unquote(anchor) in _heading_anchors(destination.read_text()), f'{name}: {target}'


def test_constructor_parameters_are_documented():
    content = (ROOT / 'DOCUMENTATION.md').read_text()
    documented = set(re.findall(r'^- \*\*`(\w+)`\*\*', content, re.M))
    assert set(inspect.signature(DBMerge).parameters) <= documented


def test_readme_links_work_outside_the_repository():
    content = (ROOT / 'README.md').read_text()
    targets = re.findall(r'\]\(([^)]+)\)', content)
    targets += re.findall(r'(?:href|src|srcset)="([^"]+)"', content)
    for target in targets:
        assert target.startswith('https://'), f'README link must be absolute: {target}'
        if target.startswith(REPOSITORY):
            local = target[len(REPOSITORY):].split('#', 1)[0]
            assert (ROOT / local).is_file(), f'README links to a missing repository file: {target}'


def _recipe_snippet(heading):
    guide = (ROOT / 'GUIDES.md').read_text()
    section = re.search(rf'^## {re.escape(heading)}\n(.*?)(?=^## |\Z)', guide, re.M | re.S)
    assert section, heading
    return re.search(r'```python\n(.*?)```', section[1], re.S)[1]


@pytest.mark.parametrize('heading, query, expected', [
    ('Full snapshot', 'SELECT * FROM products ORDER BY id', [(1, 'Dark roast'), (3, 'Cocoa')]),
    ('Scoped snapshot', 'SELECT * FROM sales ORDER BY month, product_id',
     [(202501, 1, 4), (202502, 1, 8), (202502, 3, 2)]),
])
def test_snapshot_recipes_execute(heading, query, expected):
    namespace = {}
    try:
        exec(_recipe_snippet(heading), namespace)
        with namespace['engine'].connect() as conn:
            assert conn.exec_driver_sql(query).all() == expected
    finally:
        if 'engine' in namespace:
            namespace['engine'].dispose()


def test_sql_sources_recipe_preserves_missing_rows():
    engine = create_engine('sqlite://')
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql('CREATE TABLE orders (customer_id INTEGER, status VARCHAR(10))')
            conn.exec_driver_sql("INSERT INTO orders VALUES (1, 'paid'), (1, 'paid'), (2, 'new')")
            conn.exec_driver_sql('CREATE TABLE customer_order_counts '
                                 '(customer_id INTEGER PRIMARY KEY, order_count INTEGER)')
            conn.exec_driver_sql('INSERT INTO customer_order_counts VALUES (2, 5)')
        exec(_recipe_snippet('SQL sources'), {'engine': engine})
        with engine.connect() as conn:
            assert conn.exec_driver_sql('SELECT * FROM customer_order_counts ORDER BY customer_id').all() == [(1, 2), (2, 5)]
    finally:
        engine.dispose()


def test_owned_runtime_templates_do_not_contain_cyrillic():
    # Inspect only owned literal templates, never formatted user data or external
    # error messages. This enforces the observed language regression, not ASCII
    # output or a claim to recognize every non-English language.
    methods = {'debug', 'info', 'warning', 'warn', 'error', 'exception', 'critical', 'log', 'print'}
    paths = list((ROOT / 'src' / 'dbmerge').glob('*.py')) + list((ROOT / 'examples').glob('*.py'))
    for path in paths:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            expressions = []
            if isinstance(node, ast.Call):
                name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, 'id', '')
                if name in methods:
                    # logger.log takes the level before the message.
                    index = 1 if name == 'log' else 0
                    expressions = node.args[index:index + 1]
            elif isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
                expressions = node.exc.args
            for expression in expressions:
                literals = ([expression] if isinstance(expression, ast.Constant) else
                            [part for part in expression.values if isinstance(part, ast.Constant)]
                            if isinstance(expression, ast.JoinedStr) else [])
                for literal in literals:
                    if isinstance(literal.value, str):
                        assert not re.search('[А-Яа-яЁё]', literal.value), f'{path}:{node.lineno}'


def test_quick_start_executes(capsys):
    runpy.run_path(str(ROOT / 'quick_start.py'))
    assert 'Inserted: 2; updated: 0' in capsys.readouterr().out


def test_user_guide_executes():
    namespace = runpy.run_path(str(ROOT / 'user_guide.py'))
    try:
        assert namespace['result'].inserted_row_count == 4
    finally:
        namespace['engine'].dispose()


def test_sdist_includes_all_guides():
    # Works on the project's oldest supported Python (no tomllib dependency).
    project = (ROOT / 'pyproject.toml').read_text()
    include = project.split('[tool.hatch.build.targets.sdist]', 1)[1].split(
        '[project.optional-dependencies]', 1)[0]
    for name in GUIDES + ['TRACKING_AND_MARTS.md', 'llms.txt']:
        assert f'"/{name}"' in include, f'{name} missing from source distribution'


def test_product_daily_sales_replaces_previous_mart_examples():
    obsolete = 'orders_' + 'by_customer'
    paths = [ROOT / name for name in GUIDES]
    paths += list((ROOT / 'examples').glob('*.py'))
    for path in paths:
        assert obsolete not in path.name
        assert obsolete not in path.read_text(), str(path)
    for name in ('product_daily_sales.py', 'product_daily_sales_clickhouse.py',
                 'seed_product_daily_sales.py'):
        assert (ROOT / 'examples' / name).is_file()


def test_release_version_is_consistent():
    project = (ROOT / 'pyproject.toml').read_text()
    version = re.search(r'^version = "([^"]+)"', project, re.M).group(1)
    lock = (ROOT / 'uv.lock').read_text()
    assert f'name = "dbmerge"\nversion = "{version}"' in lock
    assert f'## [{version}] - ' in (ROOT / 'CHANGELOG.md').read_text()