# Oracle support

Oracle uses the existing public API with hooks in `src/dbmerge/oracle.py`.
Live integration tests cover Oracle XE 21.3, SQLAlchemy 2.0 and 2.1, and
python-oracledb Thin mode. Other Oracle versions and Thick mode have not been verified.

## Installation and connection

```sh
pip install 'dbmerge[oracle]'
```

```python
from sqlalchemy import create_engine, String
from dbmerge import dbmerge

engine = create_engine(
    'oracle+oracledb://dbmerge_test:oracle@localhost:1521/?service_name=XEPDB1'
)
with dbmerge(engine=engine, table_name='facts', key=['code'],
             data=[{'code': 'A', 'description': 'new value'}],
             data_types={'code': String(100)}) as merge:
    result = merge.exec()
```

Use an application user, not SYSTEM. Start the existing Docker XE service with
`docker compose -f db/oracle/docker-compose.yml up -d`, wait for readiness,
then create a test user in XEPDB1 as an administrator:

```sql
CREATE USER dbmerge_test IDENTIFIED BY oracle;
GRANT CREATE SESSION, CREATE TABLE TO dbmerge_test;
ALTER USER dbmerge_test QUOTA 100M ON USERS;
```

The application-user environment variables in that compose file do not replace
explicit user provisioning for the Oracle registry image.

## Implementation

- Updates use correlated scalar SELECTs guarded by EXISTS, rather than UPDATE
  with a joined FROM clause. The staging primary key guarantees a single source
  row per target key. Update conditions and skip-update/skip-compare lists apply.
- Staging uses GLOBAL TEMPORARY tables with ON COMMIT PRESERVE ROWS so both
  `commit_all_steps` modes work. Cleanup rolls back pending work, truncates the
  GTT to release the session binding and drops its definition. A killed process
  can leave the definition behind; normal cleanup removes it.
- New inferred non-key strings become CLOB; string keys need an explicit
  `String(length)`. CLOB/NCLOB/BLOB comparisons use DBMS_LOB.COMPARE plus NULL
  checks. Explicit data types are not replaced.
- Inferred datetimes become TIMESTAMP to retain microseconds. Explicit
  `DateTime()` retains Oracle's normal DATE mapping.
- Inferred floating-point numbers become BINARY_DOUBLE: an unqualified NUMERIC
  would round the fractional part. Explicit numeric types are not replaced.
- Inserts, deletion, marking, resurrection, audit fields and source-table
  loading retain the generic flow. Oracle INSERT ... SELECT is executed as
  compiled driver SQL to obtain the actual row count.
- `bulk_load=True` loads the staging table with python-oracledb's array DML:
  the cursor's `executemany` is driven directly, one call per chunk, with
  `setinputsizes` taken from the same column types SQLAlchemy uses (a TIMESTAMP
  keeps its microseconds, CLOB/BLOB keep their length). Each chunk runs in a
  savepoint; a chunk the array DML refuses goes back to the generic INSERT, so
  the error it raises is the one the row-by-row path raises. The direct path is
  about twice as fast for simple columns.

## Limitations

- Native JSON and LONG input columns are not supported. Serialize structured
  values yourself and declare Text/CLOB. LOB keys are not supported.
- Oracle schemas are users and must already exist; dbmerge never creates users,
  even with `can_create_schemas=True`. For a different target/staging schema the
  DBA must grant the necessary cross-schema privileges. These are not needed
  when all tables belong to the connecting user and schemas are omitted.
- Oracle DDL commits independently of merge DML. Target creation and added
  columns are not undone by a later failed merge. With
  `commit_all_steps=False`, target DML is rolled back on failure before cleanup.
  With step commits enabled, already committed phases remain committed.
- `bulk_load` uses array DML (`cursor.executemany`) for the staging table.
  There is no direct-path (`INSERT /*+ APPEND */`) loader: the staging table is
  a global temporary table, and a direct-path insert is not meaningful there.
- Oracle's empty-string, DATE/time and timezone semantics remain Oracle's own.
  Do not assume SQL TIME, empty-string preservation or timezone offset
  preservation without testing your explicit column types and driver.

## Tests

Oracle is part of the shared database matrix. Run against a **disposable test
database**: the Oracle preparation hook creates missing locked schema owners
`TARGET`, `SOURCE`, `TMP` and sets each owner's USERS tablespace quota to 1 GB.
The default SYSTEM URL is only for that local test container. Custom URLs need
the equivalent administrative permissions (including cross-schema DDL/DML).
Do not point this setup at a production database or schemas used by applications.

```sh
DBMERGE_TEST_ENGINES=oracle pytest tests/test_all.py -v
```

Override the shared suite's connection with `DBMERGE_TEST_URL_ORACLE` if needed.
The test hooks are in `tests/db_hooks`: optional hooks prepare the engine once
per URL, handle unsupported scenarios, adapt parity input and select computed
column options. No hook means the existing generic behavior. Shared assertions
and merge scenarios remain in `tests/test_all.py`.

On Oracle XE 21.3 / SQLAlchemy 2.0 and 2.1 the shared suite passes 79 tests and
skips 17: four native-JSON inference cases and thirteen tests specific to other
databases. Loader parity serializes structured values as explicit Text/CLOB because
native JSON is unsupported on 2.0 (from 2.1 the dialect has a native `JSON` type,
but the shared suite still skips that scenario). Oracle computed columns are tested
as virtual, rather than requesting persisted computed columns.

Additional Oracle-specific tests can run under a minimally privileged user:

```sh
DBMERGE_TEST_URL_ORACLE='oracle+oracledb://dbmerge_test:oracle@localhost:1521/?service_name=XEPDB1' \
    pytest tests/test_oracle.py -v
```

Without the environment variable the additional Oracle integration tests are skipped.
That suite covers list/dict/pandas/polars input, both step-commit and bulk flags,
CLOB changes and NULL transitions, insert/update/delete counts, marking and
resurrection, guards, source filtering, microseconds, cleanup and failed-merge
rollback, and asserts that `bulk_load=True` actually drives the array-DML loader
while storing exactly what the row-by-row path stores. The shared suite
additionally tests the full source-table scenario, real cross-schema operations
and all supported generic merge scenarios.

A staging-load benchmark lives in `db/oracle/oracle_bench.py`:

```sh
python db/oracle/oracle_bench.py 100000
```

Reference documentation: SQLAlchemy's Oracle dialect (temporary-table options,
DATE/TIMESTAMP types) and Oracle's CREATE TABLE / temporary-table documentation.