# DBMerge Compatibility

The databases and drivers DBMerge is tested with, how each of them loads the staging table, and where they store the same data differently. A combination marked ✅ passes the full test suite: every merge mode, with each of the four kinds of input data (list of dicts, dict of lists, pandas and polars DataFrames).

---

## Databases and Drivers

| Database | Driver | Engine URL | SQLAlchemy 2.0 | SQLAlchemy 2.1 | Bulk staging load |
|---|---|---|---|---|---|
| PostgreSQL | psycopg2 | `postgresql+psycopg2://` | ✅ | ✅ | `COPY FROM STDIN` |
| PostgreSQL | psycopg 3 | `postgresql+psycopg://` | ✅ | ✅ | `COPY FROM STDIN` |
| PostgreSQL | pg8000 | `postgresql+pg8000://` | ✅ | ✅ | `COPY FROM STDIN` |
| CockroachDB | psycopg2 + sqlalchemy-cockroachdb | `cockroachdb://` | ✅ | ✅ ³ | `COPY FROM STDIN` |
| CockroachDB | psycopg 3 + sqlalchemy-cockroachdb | `cockroachdb+psycopg://` | ✅ | ✅ ³ | `COPY FROM STDIN` |
| MariaDB | MariaDB Connector/Python | `mariadb+mariadbconnector://` | ✅ | ✅ | — |
| MySQL | PyMySQL | `mysql+pymysql://` | ✅ | ✅ | — |
| MySQL | mysql-connector-python | `mysql+mysqlconnector://` | ✅ | ✅ | — |
| MySQL | mysqlclient | `mysql+mysqldb://` | ✅ | ✅ | — |
| MS SQL Server | pyodbc + ODBC Driver 18 | `mssql+pyodbc://` | ✅ | ✅ | `fast_executemany` |
| MS SQL Server | mssql-python | `mssql+mssqlpython://` | — ¹ | ✅ | bulk copy |
| MS SQL Server | pymssql | `mssql+pymssql://` | ⚠️ ² | ⚠️ ² | — |
| SQLite | sqlite3 (standard library) | `sqlite://` | ✅ | ✅ | — |
| DuckDB | duckdb_engine | `duckdb://` | ✅ | ✅ | Arrow |
| ClickHouse | clickhouse-connect | `clickhousedb://`, or a client | ✅ | ✅ | Arrow |

✅ passes every test  ·  ⚠️ works, with the limitation in the note  ·  — no such combination

1. SQLAlchemy has the `mssql+mssqlpython` dialect only from 2.1 on.
2. pymssql truncates microseconds to milliseconds, on every path: a `datetime` of `12:00:00.123456` is stored as `12:00:00.123`. Everything else works. It has no bulk load in DBMerge either — see [MS SQL Server](#ms-sql-server). For SQL Server, prefer pyodbc or mssql-python.
3. sqlalchemy-cockroachdb 2.0.4 declares `SQLAlchemy<2.1`, so with SQLAlchemy 2.1 it has to be 2.0.3, the last release without that limit. The tests ran with 2.0.3 on SQLAlchemy 2.1 and with 2.0.4 on SQLAlchemy 2.0.

**Not supported:**

- **ClickHouse through clickhouse-sqlalchemy** (`clickhouse://`, `clickhouse+native://`). Its driver opens a new session for every statement, so a temporary staging table does not survive between them. DBMerge refuses such an engine with `IncorrectParameter`; use clickhouse-connect.
- **Oracle**: `UPDATE` with a `JOIN` is not available in its SQLAlchemy dialect.

**Versions in the tests:** PostgreSQL 18, CockroachDB 24.3, MariaDB 10.11, MySQL 8.4, SQL Server 2022, SQLite 3.50, DuckDB 1.5 and 2.0, ClickHouse 23.8, 24.8 and 26.9; Python 3.14 and 3.15 with pandas 3, polars and pyarrow, and Python 3.10 and 3.12 without pyarrow (SQLite, DuckDB, PostgreSQL). The oldest ClickHouse DBMerge accepts is 23.8. SQLAlchemy 2.1 needs Python 3.11 or newer.

---

## Bulk Staging Load

Every merge first loads the data into a staging table (see [The Staging Table](DOCUMENTATION.md#the-staging-table)). By default this is SQLAlchemy's `INSERT` with many parameter sets, which most drivers send row by row. Where the database and its driver offer a faster way, `exec()` uses it, and stores exactly the same values as the row by row `INSERT` would — with one exception, values that do not fit their column on CockroachDB (see [CockroachDB](#cockroachdb)). `exec(bulk_load=False)` turns it off.

| Database and driver | How the staging table is loaded | Measured: row by row → bulk |
|---|---|---|
| PostgreSQL, CockroachDB — psycopg2, psycopg 3, pg8000 | `COPY … FROM STDIN` in CSV, through the merge's own connection | 1M rows: psycopg2 24 s → 5.5 s; psycopg 3 15.6 s → 5.3 s; pg8000 192 s → 5.4 s |
| MS SQL Server — mssql-python | TDS bulk copy (`cursor.bulkcopy`, from Arrow when pyarrow is installed) | 200k rows: 121.6 s → 2.5 s |
| MS SQL Server — pyodbc | `fast_executemany`: a chunk is sent as one array of parameters | 100k rows: 61.7 s → 3.1 s |
| DuckDB | an Arrow table, read by DuckDB in place with one `INSERT … SELECT` per chunk | the test suite: 10 min → 3.7 min |
| ClickHouse | `insert_arrow` of clickhouse-connect | 1M rows: 20 s through SQLAlchemy → 1.3 s |

**pyarrow** is optional everywhere, including ClickHouse. Without it:

- ClickHouse loads through the native protocol of clickhouse-connect, a few times slower than Arrow.
- DuckDB loads row by row.
- PostgreSQL and CockroachDB write the CSV with Python's `csv` module on Python 3.12+, and load row by row on older Pythons (the `csv` module tells `NULL` from an empty string only since 3.12).

In these cases DBMerge logs a warning once per process: install pyarrow to load faster. mssql-python without pyarrow sends the bulk copy as tuples, with no warning.

**ClickHouse always loads in bulk**, `bulk_load` has no effect there.

Some data goes row by row even with `bulk_load=True`, so that what is stored does not change:

- PostgreSQL and CockroachDB: a staging table with an `ARRAY` or `Interval` column; a chunk the CSV can not carry as the driver would send it (a column of mixed types, an integer beyond 64 bits); a timestamp column without time zone when the session `TimeZone` is unknown to Python.
- mssql-python: a chunk with a `datetimeoffset` value whose offset is not UTC — see [MS SQL Server](#ms-sql-server) — and a chunk the bulk copy fails on. Bulk copy runs on a second connection that mssql-python opens from the connection string; if that connection fails, the data goes row by row.
- pyodbc: a chunk `fast_executemany` fails on. It is rolled back to a savepoint first, so the row by row `INSERT` reports the real error rather than the primary key of the staging table.
- DuckDB: a chunk Arrow can not put in a table (a column of mixed types).

**Chunk size.** `chunk_size` is the number of rows per insert. It defaults to 10,000; to 100,000 for the bulk copy of mssql-python, where every chunk opens a connection of its own (a million rows load in 5.4s in chunks of 10,000, in 1.5s in chunks of 100,000); and to 1,000,000 on ClickHouse, where every insert creates a new part of the table.

---

## Differences Between Databases and Drivers

### PostgreSQL

**pg8000 and scramp 1.5.0.** pg8000 1.31.5 does not connect with scramp 1.5.0, released on 2026-10-25: `AttributeError: module 'scramp' has no attribute 'make_channel_binding'`. Install `scramp<1.5` until a pg8000 release fixes it.

**A timezone-aware `datetime` in a `timestamp` column without time zone** is stored differently by different drivers. psycopg2 and psycopg 3 convert it to the session `TimeZone`, pg8000 converts it to UTC. With the session in `Europe/Moscow`:

```
value sent:   2025-06-01 12:00:00+00:00
psycopg2:     2025-06-01 15:00:00
psycopg 3:    2025-06-01 15:00:00
pg8000:       2025-06-01 12:00:00
```

The bulk load repeats what its driver does, so switching `bulk_load` changes nothing, but switching the driver does. A `timestamptz` column (`DateTime(timezone=True)`) keeps the moment with every driver.

**A float in a `NUMERIC` column without precision** may be stored with a different scale by the bulk load and by the row by row `INSERT`: `1.0` against `1`. The values are equal as numbers, so the next merge does not see a change.

### CockroachDB

CockroachDB runs `SERIALIZABLE` transactions by default. A merge step over many rows stays open long enough for an unrelated commit to break it, and `COMMIT` fails with a transient `RETRY_SERIALIZABLE` error that the client is expected to retry. DBMerge does not retry. The tests run with `create_engine(url, isolation_level='READ COMMITTED')`, where CockroachDB retries such statements itself.

**COPY does not apply the limits of a column type.** The COPY of CockroachDB, which loads the staging table with `bulk_load=True`, stores a value that does not fit its column without an error: a string longer than `VARCHAR(n)` or `CHAR(n)` is cut to `n` characters, a number beyond the precision of `DECIMAL(p,s)` is stored as it is, and extra decimal places are not rounded. The row by row `INSERT` refuses or rounds such values, as PostgreSQL does with COPY too. Checked on CockroachDB 24.3 and 26.3. If your data may not fit the column types, pass `exec(bulk_load=False)` on CockroachDB.

### MySQL and MariaDB

- **The staging table is a regular table on MySQL** and a `TEMPORARY` one on MariaDB. MySQL can not refer to the same temporary table twice in one statement (`Can't reopen table`), which an `update_condition` or `insert_condition` built on `merge.temp_table` needs. A regular table can be left over by a process killed mid-merge — see [The Staging Table](DOCUMENTATION.md#the-staging-table).
- **`Uuid` on MariaDB with SQLAlchemy 2.1** creates a column of MariaDB's native `UUID` type, where SQLAlchemy 2.0 created `CHAR(32)`. The native type refuses values that are not standard UUIDs, such as `87654321-4321-8765-4321-876543218765`; `uuid.uuid4()` and other standard UUIDs are fine.

### MS SQL Server

- **`datetimeoffset` with mssql-python.** A `datetimeoffset` column keeps the offset each value came with: `12:00+03:00` is stored as `12:00+03:00`, not as `09:00+00:00`. The bulk copy of mssql-python can not keep it: from Arrow it stores every value in UTC, and from tuples it keeps the offset but moves the time by it. A chunk where such a column has a value with an offset other than UTC is therefore loaded row by row, with a warning once per process. Values in UTC, and columns of other types, go through bulk copy.
- **pyodbc** with `fast_executemany` writes an aware `datetime` into a `datetimeoffset` column as its wall-clock time with the offset of the client machine. DBMerge sends such values as text with their own offset instead, so `bulk_load` stores the same as the row by row `INSERT`.
- **pyodbc on Python 3.15** has no ready-made wheel yet (5.3.0): pip builds it from source, which needs a C++ compiler and the unixODBC headers (`unixodbc-dev` on Debian and Ubuntu).
- **pymssql** truncates microseconds to milliseconds. Its own bulk copy (`bulk_copy`) is not used: it refuses `date` values, drops the offset of an aware `datetime` and stores an empty string as `NULL`.

### DuckDB

- `duckdb:///:memory:` gives every connection a database of its own, so whatever DBMerge writes is invisible to the other connections of the engine. Use a database file.
- DBMerge reads the table definition from DuckDB's own catalog rather than through SQLAlchemy reflection, which fails under SQLAlchemy 2.1 ([duckdb#27168](https://github.com/duckdb/duckdb/issues/27168)) and misreads some types under 2.0.

### ClickHouse

ClickHouse merges work differently from every other database; see [ClickHouse](DOCUMENTATION.md#clickhouse) in the documentation.

- **23.8** is the oldest version DBMerge accepts; every merge works from there on.
- `source_table_name` is not supported on ClickHouse.
