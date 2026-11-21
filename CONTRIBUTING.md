# Development and verification

## Environment

Use Python 3.10 or newer. From a source checkout, install the project and the
dependencies needed by the shared tests:

```bash
python -m venv .venv
.venv/bin/python -m pip install -e . pytest pandas polars numpy
```

The commands below use a Linux/macOS shell; on Windows activate the virtual
environment and use `python` instead of `.venv/bin/python`.
Database-specific suites also need their drivers and running database servers.
See the workflow files in `.github/workflows/` for the exact installation and
service configuration used by each job. The broad `dev` extra includes native
drivers and may require OS build dependencies; it is not needed for SQLite tests.

## Fast checks (no external server)

```bash
DBMERGE_TEST_ENGINES=sqlite .venv/bin/python -m pytest \
  tests/test_public_api.py tests/test_validation.py tests/test_startup.py \
  tests/test_lease_keys.py tests/test_optional_dependencies.py \
  tests/test_documentation.py tests/test_mart_runner.py tests/test_db_hooks.py -q
```

Documentation checks parse Python snippets, check local file links and Markdown
heading anchors, and verify API parameter coverage and source-distribution
inclusion. They execute the SQLite quick start, user guide and merge recipes.
They do not fetch external URLs or execute server-dependent examples.

## Full SQLite checks

The shared merge suite needs sample data. Its initialization creates/uses a
large fixture in `data/`; this is a separate input database, not the target below.

```bash
mkdir -p data
.venv/bin/python tests/sample_data_in_sqlite.py
DBMERGE_TEST_ENGINES=sqlite \
DBMERGE_TEST_URL_SQLITE=sqlite:////tmp/dbmerge-local-tests.sqlite \
.venv/bin/python -m pytest tests -v --tb=short --durations=20
```

**Use disposable databases only.** Tests create and drop tables and schemas.
Do not run two test processes against the same SQLite target file. Give each
process a unique absolute path in `DBMERGE_TEST_URL_SQLITE`. Initialize the sample
fixture before parallel runs, not while another process reads it. A full merge
run can take several minutes. Tracking requires file-backed SQLite, not an
in-memory connection shared between background workers.

Select server suites with `DBMERGE_TEST_ENGINES` and the corresponding
`DBMERGE_TEST_URL_<ENGINE>` variables defined in `tests/conftest.py`. Never use
production credentials. A passing SQLite run does not establish PostgreSQL,
Oracle, SQL Server or ClickHouse correctness. Some concurrency checks deliberately
skip unless their supported server engine is selected.

## Package and release checks

```bash
.venv/bin/python -m pip install build twine
.venv/bin/python -m build
.venv/bin/python -m twine check dist/*
.venv/bin/python -m compileall -q src tests examples quick_start.py user_guide.py
git diff --check
```

Before publishing: review the changelog and version, run the server CI matrix,
install the built wheel in a clean environment, and verify both `DBMerge` and
the legacy `dbmerge` alias. Keep the logger name `dbmerge`. Update feature-version
notes in the README and `llms.txt` when moving changes out of `Unreleased`.
Do not publish a build carrying an already released version number.