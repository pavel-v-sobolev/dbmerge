# Changelog

All notable changes to DBMerge. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). Releases before 2.0.0 are described in [GitHub Releases](https://github.com/pavel-v-sobolev/dbmerge/releases).

## [2.1.1] - 2026-10-29

### Fixed

- **A view of a PostgreSQL or MySQL database attached to ClickHouse** works as `source_table_name`. It was not found: `system.tables` lists only the tables of such a database.
- **`source_condition` with an `IN` or `NOT IN` of one column over a subquery, on a source of such a database**, reads only the rows it picks. ClickHouse sends to the other server only the filters that compare columns with constants, so it read the whole source and filtered it itself. DBMerge now runs the subquery first and writes its rows into the condition. An `IN` of several columns and a subquery correlated with the source are still left to ClickHouse.
- **A `datetime` in a condition on ClickHouse is compared as the moment it names.** clickhouse-connect bound it as text with whole seconds, in the time zone of the server, which moved the bound of a condition such as `merged_on > last_run` to the start of its second, and missed the moment in a column of a time zone of its own, such as `DateTime64(6, 'UTC')`, on a server outside UTC.
- **A long `in_()` list in a condition on ClickHouse** no longer fails with `Max query size exceeded`: `max_query_size` is raised for the statement.

## [2.1.0] - 2026-10-28

### Added

- **`source_table_name` on ClickHouse.** The source can be any table or view ClickHouse reads, including a PostgreSQL or MySQL database attached to ClickHouse with its database engine; the server copies it into the staging table itself. A `ReplacingMergeTree` source is read with `FINAL`. A target created from a source takes its column types, with the key columns made not nullable. See [Source tables on ClickHouse](DOCUMENTATION.md#source-tables-on-clickhouse).

### Fixed

- **`in_()` with a list of values in a condition.** On ClickHouse every condition with it failed, and so did `insert_condition` and `source_condition` with DuckDB, psycopg 3 and mysql-connector-python: the list reached the database as an unexpanded `__[POSTCOMPILE_…]` placeholder.

## [2.0.1] - 2026-10-26

### Fixed

- **PostgreSQL and CockroachDB, bulk load:** a value that COPY refuses is reported as the same SQLAlchemy exception that the row by row `INSERT` raises (`DataError` and so on). It used to be hidden behind `current transaction is aborted` from the cleanup; with a naive `datetime` column in the data, it arrived as the driver's own exception, which `except sqlalchemy.exc.DataError` does not catch.
- **psycopg 3 with SQLAlchemy 2.1:** JSON columns go through COPY. With pyarrow, their chunks used to fall back to the row by row `INSERT` silently; without pyarrow, the merge failed.
- **PostgreSQL and CockroachDB without pyarrow (Python 3.12+):** a pandas integer column with a missing value, which pandas holds as floats, no longer fails COPY on values such as `1.0`.
- **MS SQL Server with pyodbc:** a bad value in a chunk is reported as itself, not as a violation of the primary key of the staging table.
- **DuckDB:** a chunk that Arrow can not hold, such as a column of mixed types, goes row by row, so a bad value is reported by DuckDB rather than as a pyarrow error.
- **ClickHouse:** a value that does not fit its column raises `IncorrectDataError` naming the column and the row, rather than a bare pyarrow error.
- **A failure to drop the staging table** no longer hides the error of the merge, and the connection is always closed. After a merge that succeeded, the failure is logged as a warning with the name of the table.
- **`source_table_name` with a computed target column** (`GENERATED ALWAYS AS …`): the column is left out and computed by the target, instead of failing the update.

### Changed

- **Every row of a list of dicts must have the same keys, and a pandas DataFrame must not have two columns of one name.** Both raise `IncorrectDataError`. Before, the values of a key missing from the first row were dropped, a key missing from a later row failed the row by row `INSERT` but became `NULL` through COPY, and of two columns of one name only the last one was stored.
- **A computed target column in `data`** raises `IncorrectParameter` before anything is written.
- **mssql-python** loads the staging table in chunks of 100,000 rows unless `chunk_size` is given. Every bulk copy opens a connection of its own; a million rows load in about half the time.

### Added

- The exceptions can be imported from the package root: `from dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists`. The old path `dbmerge.dbmerge` still works.

### Known issues

- **CockroachDB:** its COPY, which loads the staging table with `bulk_load=True`, does not apply the limits of column types. A string longer than `VARCHAR(n)` or `CHAR(n)` is cut, a number beyond `DECIMAL(p,s)` is stored as it is, and extra decimal places are not rounded, where the row by row `INSERT` refuses or rounds. Pass `exec(bulk_load=False)` if your data may not fit the column types. This was so in 2.0.0 as well.
- **pg8000 1.31.5** does not connect with scramp 1.5.0, released on 2026-10-25. Install `scramp<1.5` until pg8000 fixes it.

## [2.0.0] - 2026-10-25

### Added

- **ClickHouse**, through clickhouse-connect (`pip install dbmerge[clickhouse]`): merges into `ReplacingMergeTree` tables, from a `clickhousedb://` engine or a clickhouse-connect client.
- **DuckDB**, under SQLAlchemy 2.0 and 2.1.
- **Bulk load of the staging table**, `exec(bulk_load=True)` by default: `COPY FROM STDIN` on PostgreSQL and CockroachDB, bulk copy with mssql-python, `fast_executemany` with pyodbc, Arrow on DuckDB and ClickHouse.
- **Tested drivers:** psycopg 3 and pg8000 on PostgreSQL, psycopg 3 on CockroachDB, PyMySQL, mysql-connector-python and mysqlclient on MySQL, mssql-python (SQLAlchemy 2.1 only) and pymssql on MS SQL Server. **SQLAlchemy 2.1** and **Python 3.15** are tested alongside SQLAlchemy 2.0 and earlier Pythons.
- [COMPATIBILITY.md](COMPATIBILITY.md): the tested databases, drivers and versions, the bulk load of each driver, and where drivers store the same data differently.

### Changed

- `exec(chunk_size=None)` by default: 10,000 rows, 1,000,000 on ClickHouse.
- **MySQL:** the staging table is a regular table instead of a `TEMPORARY` one, since MySQL can not refer to one temporary table twice in a statement. MariaDB keeps the `TEMPORARY` table.
- **Without pyarrow:** PostgreSQL and CockroachDB write the CSV for COPY with the `csv` module on Python 3.12+, and load row by row with a warning on older Pythons; DuckDB and ClickHouse warn and load without Arrow.
- **PostgreSQL:** a float in a `NUMERIC` column without precision may be stored with another scale (`1.0` against `1`); the values are equal as numbers.
- **DuckDB:** a column for numbers of unknown precision is created as `DOUBLE`. As a generic `NUMERIC` it would be `DECIMAL(18,3)` there, rounded to three decimal places.

### Fixed

- Correct `inserted_row_count` with psycopg 3, mysql-connector-python and DuckDB, which reported no count for `INSERT … SELECT`.
- DuckDB: the table definition is read from DuckDB's own catalog. SQLAlchemy reflection fails on DuckDB under SQLAlchemy 2.1, and under 2.0 read `DOUBLE` as a 4-byte float (cutting a staging table to single precision) and missed the primary key.

### Upgrading from 1.x

- **Bulk load is on by default.** Pass `exec(bulk_load=False)` to load the staging table row by row as before.
- **MySQL:** the staging table is a regular table now, and a process killed mid-merge can leave it behind; see [The Staging Table](DOCUMENTATION.md#the-staging-table).
- **CockroachDB:** with the bulk load, values that do not fit their column types are not refused (see Known issues of 2.0.1).
- **DuckDB:** a float column created by an earlier version is `DECIMAL(18,3)` and keeps rounding. Change it to `DOUBLE` and reload the data if the decimals matter.
- **pymssql** keeps milliseconds only, a limitation of the driver, and has no bulk load. Prefer pyodbc or mssql-python on MS SQL Server.

[2.1.1]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.0.1...2.1.0
[2.0.1]: https://github.com/pavel-v-sobolev/dbmerge/compare/2.0.0...2.0.1
[2.0.0]: https://github.com/pavel-v-sobolev/dbmerge/compare/1.0.24...2.0.0
