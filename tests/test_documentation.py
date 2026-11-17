"""Keep the copyable documentation aligned with the public API and package contents."""

import ast
import inspect
import re
import runpy
import textwrap
from pathlib import Path

import pytest

from dbmerge import DBMerge


ROOT = Path(__file__).resolve().parents[1]
GUIDES = ['README.md', 'DOCUMENTATION.md', 'TRACKING_AND_MARTS.md',
          'CLICKHOUSE.md', 'CLICKHOUSE_RU.md', 'ORACLE.md', 'COMPATIBILITY.md', 'llms.txt']


@pytest.mark.parametrize('name', GUIDES)
def test_python_examples_and_local_links(name):
    path = ROOT / name
    content = path.read_text()
    for number, block in enumerate(re.findall(r'```python\n(.*?)```', content, re.S), 1):
        ast.parse(textwrap.dedent(block), filename=f'{name}:python-block-{number}')
    for target in re.findall(r'\]\(([^)]+)\)', content):
        if target.startswith(('http:', 'https:', '#', 'mailto:')):
            continue
        assert (path.parent / target.split('#')[0]).exists(), f'{name}: {target}'


def test_constructor_parameters_are_documented():
    content = (ROOT / 'DOCUMENTATION.md').read_text()
    documented = set(re.findall(r'^- \*\*`(\w+)`\*\*', content, re.M))
    assert set(inspect.signature(DBMerge).parameters) <= documented


def test_readme_links_work_outside_the_repository():
    content = (ROOT / 'README.md').read_text()
    targets = re.findall(r'\]\(([^)]+)\)', content)
    targets += re.findall(r'(?:href|src|srcset)="([^"]+)"', content)
    repository = 'https://github.com/pavel-v-sobolev/dbmerge/blob/main/'
    for target in targets:
        assert target.startswith('https://'), f'README link must be absolute: {target}'
        if target.startswith(repository):
            local = target[len(repository):].split('#', 1)[0]
            assert (ROOT / local).is_file(), f'README links to a missing repository file: {target}'


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